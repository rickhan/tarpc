// Copyright 2026 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Bidirectional RPC and multiple service instances over one TCP connection.
//!
//! The server listens on two instances of `Counter` and on `Hello`. While handling `Hello`, the
//! server calls back into `Greeter`, which is served by the client on the same connection.

use futures::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tarpc::{
    client, context,
    mux::Mux,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Json,
};
use tokio::net::{TcpListener, TcpStream};

#[tarpc::service]
trait Counter {
    /// Adds `n` to the counter and returns the new total.
    async fn add(n: u64) -> u64;
}

#[tarpc::service]
trait Hello {
    /// Greets the caller by the name the caller's own `Greeter` service reports.
    async fn hello() -> String;
}

/// Served by the client, called by the server.
#[tarpc::service]
trait Greeter {
    async fn name() -> String;
}

#[derive(Clone, Default)]
struct CounterServer(Arc<AtomicU64>);

impl Counter for CounterServer {
    async fn add(self, _: context::Context, n: u64) -> u64 {
        self.0.fetch_add(n, Ordering::SeqCst) + n
    }
}

#[derive(Clone)]
struct HelloServer {
    /// A client for the peer's `Greeter`, on the same connection the request arrived on.
    peer: GreeterClient,
}

impl Hello for HelloServer {
    async fn hello(self, _: context::Context) -> String {
        // context::current() propagates the incoming request's deadline and trace.
        match self.peer.name(context::current()).await {
            Ok(name) => format!("Hello, {name}!"),
            Err(e) => format!("Hello, whoever you are ({e})!"),
        }
    }
}

#[derive(Clone)]
struct GreeterServer;

impl Greeter for GreeterServer {
    async fn name(self, _: context::Context) -> String {
        "Alice".into()
    }
}

/// Serves every stream the peer opens to `service` on `mux`.
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

async fn serve_connection(stream: TcpStream) -> anyhow::Result<()> {
    let mux = Mux::spawn(stream);

    // Two independent instances of the same service.
    listen!(mux, "counter/a", CounterServer::default().serve());
    listen!(mux, "counter/b", CounterServer::default().serve());

    // A client for the service the peer provides, used while serving the peer's requests.
    let peer = GreeterClient::new(
        client::Config::default(),
        mux.connect("greeter", Json::default())?,
    )
    .spawn();
    listen!(mux, "hello", HelloServer { peer }.serve());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("localhost:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            if let Err(e) = serve_connection(stream).await {
                eprintln!("Failed to serve connection: {e}");
            }
        }
    });

    let mux = Mux::spawn(TcpStream::connect(addr).await?);
    listen!(mux, "greeter", GreeterServer.serve());

    let hello = HelloClient::new(
        client::Config::default(),
        mux.connect("hello", Json::default())?,
    )
    .spawn();
    let a = CounterClient::new(
        client::Config::default(),
        mux.connect("counter/a", Json::default())?,
    )
    .spawn();
    let b = CounterClient::new(
        client::Config::default(),
        mux.connect("counter/b", Json::default())?,
    )
    .spawn();

    println!("{}", hello.hello(context::current()).await?);
    println!("counter/a = {}", a.add(context::current(), 1).await?);
    println!("counter/a = {}", a.add(context::current(), 1).await?);
    println!("counter/b = {}", b.add(context::current(), 10).await?);
    Ok(())
}
