use futures::prelude::*;
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tarpc::{
    client::{self, RpcError},
    context,
    mux::Mux,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Json,
};

#[tarpc::service]
trait Counter {
    async fn add(n: u32) -> u32;
}

#[derive(Clone, Default)]
struct CounterServer(Arc<AtomicU32>);

impl Counter for CounterServer {
    async fn add(self, _: context::Context, n: u32) -> u32 {
        self.0.fetch_add(n, Ordering::SeqCst) + n
    }
}

/// Served by the side that initiated the connection, and called back by the other side.
#[tarpc::service]
trait Greeter {
    async fn name() -> String;
}

#[derive(Clone)]
struct GreeterServer(String);

impl Greeter for GreeterServer {
    async fn name(self, _: context::Context) -> String {
        self.0
    }
}

/// Served by the side that accepted the connection; each call calls back into the caller.
#[tarpc::service]
trait Hello {
    async fn hello() -> String;
}

#[derive(Clone)]
struct HelloServer(GreeterClient);

impl Hello for HelloServer {
    async fn hello(self, _: context::Context) -> String {
        let name = self.0.name(context::current()).await.unwrap();
        format!("Hello, {name}!")
    }
}

/// Serves every stream opened to `service` with `serve`.
macro_rules! listen {
    ($mux:expr, $service:expr, $serve:expr) => {{
        let serve = $serve;
        let incoming = $mux.listen($service, Json::default)?;
        tokio::spawn(incoming.for_each(move |transport| {
            let serve = serve.clone();
            async move {
                tokio::spawn(
                    BaseChannel::with_defaults(transport)
                        .execute(serve)
                        .for_each(|response| async move {
                            tokio::spawn(response);
                        }),
                );
            }
        }));
    }};
}

fn pair() -> (Mux, Mux) {
    let (a, b) = tokio::io::duplex(1024);
    (Mux::spawn(a), Mux::spawn(b))
}

#[tokio::test]
async fn bidirectional_callback() -> anyhow::Result<()> {
    let (client_side, server_side) = pair();

    // The initiating side serves Greeter...
    listen!(
        &client_side,
        "greeter",
        GreeterServer("Alice".into()).serve()
    );

    // ...and the accepting side calls it back while handling Hello.
    let greeter = GreeterClient::new(
        client::Config::default(),
        server_side.connect("greeter", Json::default())?,
    )
    .spawn();
    listen!(&server_side, "hello", HelloServer(greeter).serve());

    let hello = HelloClient::new(
        client::Config::default(),
        client_side.connect("hello", Json::default())?,
    )
    .spawn();
    assert_eq!(hello.hello(context::current()).await?, "Hello, Alice!");
    Ok(())
}

#[tokio::test]
async fn multiple_instances() -> anyhow::Result<()> {
    let (a, b) = pair();
    listen!(&b, "counter/1", CounterServer::default().serve());
    listen!(&b, "counter/2", CounterServer::default().serve());

    let one = CounterClient::new(
        client::Config::default(),
        a.connect("counter/1", Json::default())?,
    )
    .spawn();
    let two = CounterClient::new(
        client::Config::default(),
        a.connect("counter/2", Json::default())?,
    )
    .spawn();
    // Another client of the same instance shares its state but not its stream.
    let one_again = CounterClient::new(
        client::Config::default(),
        a.connect("counter/1", Json::default())?,
    )
    .spawn();

    assert_eq!(one.add(context::current(), 1).await?, 1);
    assert_eq!(two.add(context::current(), 10).await?, 10);
    assert_eq!(one_again.add(context::current(), 1).await?, 2);
    assert_eq!(two.add(context::current(), 10).await?, 20);

    // Concurrent requests on several streams.
    let results = future::try_join_all((0..100).map(|_| one.add(context::current(), 1))).await?;
    assert_eq!(results.into_iter().max(), Some(102));
    Ok(())
}

#[tokio::test]
async fn both_sides_serve_the_same_name() -> anyhow::Result<()> {
    let (a, b) = pair();
    listen!(&a, "greeter", GreeterServer("a".into()).serve());
    listen!(&b, "greeter", GreeterServer("b".into()).serve());

    let a_to_b = GreeterClient::new(
        client::Config::default(),
        a.connect("greeter", Json::default())?,
    )
    .spawn();
    let b_to_a = GreeterClient::new(
        client::Config::default(),
        b.connect("greeter", Json::default())?,
    )
    .spawn();
    assert_eq!(a_to_b.name(context::current()).await?, "b");
    assert_eq!(b_to_a.name(context::current()).await?, "a");
    Ok(())
}

#[tokio::test]
async fn unknown_service_fails_fast() -> anyhow::Result<()> {
    let (a, _b) = pair();
    let client = CounterClient::new(
        client::Config::default(),
        a.connect("nope", Json::default())?,
    )
    .spawn();

    let result = tokio::time::timeout(Duration::from_secs(5), client.add(context::current(), 1))
        .await
        .expect("should fail before the deadline");
    let err = result.unwrap_err();
    assert!(matches!(err, RpcError::Channel(_)), "{err:?}");
    let debug = format!("{err:?}");
    assert!(
        debug.contains("NotFound") && debug.contains("nope"),
        "{debug}"
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_listener() -> anyhow::Result<()> {
    let (a, _b) = pair();
    let first = a.listen::<(), (), _, _>("svc", Json::<(), ()>::default)?;
    let err = a
        .listen::<(), (), _, _>("svc", Json::<(), ()>::default)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    // The name is freed when the listener is dropped.
    drop(first);
    let _second = a.listen::<(), (), _, _>("svc", Json::<(), ()>::default)?;
    Ok(())
}

#[tokio::test]
async fn connection_closed() -> anyhow::Result<()> {
    let (a, b) = tokio::io::duplex(1024);
    let a = Mux::spawn(a);
    let (b, b_driver) = Mux::new(b);
    let b_driver = tokio::spawn(b_driver);
    listen!(&b, "counter", CounterServer::default().serve());

    let client = CounterClient::new(
        client::Config::default(),
        a.connect("counter", Json::default())?,
    )
    .spawn();
    assert_eq!(client.add(context::current(), 1).await?, 1);

    b_driver.abort();
    let _ = b_driver.await;

    let result = tokio::time::timeout(Duration::from_secs(5), client.add(context::current(), 1))
        .await
        .expect("should fail before the deadline");
    assert!(result.is_err(), "{result:?}");
    Ok(())
}
