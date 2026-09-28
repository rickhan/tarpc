// Copyright 2026 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Multiplexes any number of RPC services, in both directions, over a single connection.
//!
//! A plain tarpc [`Transport`](crate::Transport) connects exactly one client to exactly one
//! server. A [`Mux`] instead wraps one byte stream (e.g. a `TcpStream`) and carves it into many
//! independent virtual transports ("streams"). Both ends of the connection are symmetric: each
//! side can
//!
//! - [`listen`](Mux::listen) on any number of named services, acting as their server, and
//! - [`connect`](Mux::connect) to any number of named services on the peer, acting as a client.
//!
//! This makes it possible for the side that accepted a connection to call RPCs on the side that
//! initiated it, and vice versa, all on the same connection. Multiple instances of the same
//! service are simply listened on under different names, e.g. `"counter/a"` and `"counter/b"`.
//!
//! Every call to [`connect`](Mux::connect) opens a new stream, which the peer receives as a new
//! transport from the [`Incoming`] stream of the listener for that service name. Each stream is a
//! normal tarpc transport: wrap client streams with a generated `Client::new`, and server
//! streams with a [`BaseChannel`](crate::server::BaseChannel).
//!
//! If the peer has no listener for the requested service name, the stream fails with an error of
//! kind [`io::ErrorKind::NotFound`], failing any pending requests.
//!
//! The connection stays open as long as the [`Driver`] is polled and some local [`Mux`] handle,
//! [`Incoming`] listener, or stream is alive, and until the peer closes it.
//!
//! Buffers between streams and the driver are unbounded, so that a slow stream can never block
//! others on the same connection. Flow control comes from tarpc itself: a client caps its in-flight
//! requests via [`client::Config`](crate::client::Config), and a server sends at most one response
//! per request.
//!
//! # Example
//!
//! ```rust
//! use futures::prelude::*;
//! use tarpc::{
//!     client, context,
//!     mux::Mux,
//!     server::{BaseChannel, Channel},
//!     tokio_serde::formats::Json,
//! };
//!
//! #[tarpc::service]
//! trait Ping {
//!     async fn ping(n: u32) -> u32;
//! }
//!
//! #[derive(Clone)]
//! struct PingServer;
//!
//! impl Ping for PingServer {
//!     async fn ping(self, _: context::Context, n: u32) -> u32 {
//!         n + 1
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Any AsyncRead + AsyncWrite works, e.g. a TcpStream.
//!     let (a, b) = tokio::io::duplex(4096);
//!     let a = Mux::spawn(a);
//!     let b = Mux::spawn(b);
//!
//!     // Both sides serve "ping"...
//!     for mux in [&a, &b] {
//!         let incoming = mux.listen("ping", Json::default)?;
//!         tokio::spawn(incoming.for_each(|transport| async move {
//!             tokio::spawn(
//!                 BaseChannel::with_defaults(transport)
//!                     .execute(PingServer.serve())
//!                     .for_each(|response| async move {
//!                         tokio::spawn(response);
//!                     }),
//!             );
//!         }));
//!     }
//!
//!     // ...and both sides call the other.
//!     let a_to_b = PingClient::new(client::Config::default(), a.connect("ping", Json::default())?).spawn();
//!     let b_to_a = PingClient::new(client::Config::default(), b.connect("ping", Json::default())?).spawn();
//!     assert_eq!(a_to_b.ping(context::current(), 1).await.unwrap(), 2);
//!     assert_eq!(b_to_a.ping(context::current(), 2).await.unwrap(), 3);
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]

use futures::{channel::mpsc, prelude::*, ready, task::*};
use pin_project::pin_project;
use std::{
    collections::{HashMap, VecDeque, hash_map::Entry},
    error::Error,
    fmt, io,
    marker::PhantomData,
    mem,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serde::{Deserializer, Framed as SerdeFramed, Serializer};
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::{Decoder, Encoder, Framed, LengthDelimitedCodec},
};

/// Maximum number of frames read per poll of the driver before yielding to writes.
const READ_BUDGET: usize = 64;

/// A handle to a multiplexed connection.
///
/// Cloning the handle is cheap; all clones refer to the same connection.
#[derive(Clone)]
pub struct Mux {
    shared: Arc<Shared>,
    outbound: mpsc::UnboundedSender<Frame<Bytes>>,
}

impl fmt::Debug for Mux {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mux").finish_non_exhaustive()
    }
}

impl Mux {
    /// Wraps a byte stream in a multiplexed connection.
    ///
    /// Returns a handle to the connection and the [`Driver`] future that performs all I/O. The
    /// driver must be polled (usually spawned) for any stream on the connection to make progress.
    pub fn new<S>(io: S) -> (Mux, Driver<S>)
    where
        S: AsyncRead + AsyncWrite,
    {
        Self::with_codec(io, LengthDelimitedCodec::new())
    }

    /// Like [`new`](Mux::new), but frames the byte stream with the given length-delimited codec,
    /// e.g. to raise the maximum frame length.
    pub fn with_codec<S>(io: S, codec: LengthDelimitedCodec) -> (Mux, Driver<S>)
    where
        S: AsyncRead + AsyncWrite,
    {
        let shared = Arc::new(Shared::default());
        let (outbound, frames) = mpsc::unbounded();
        let driver = Driver {
            framed: Framed::new(io, FrameCodec { inner: codec }),
            frames,
            rejects: VecDeque::new(),
            guard: ShutdownGuard(shared.clone()),
        };
        (Mux { shared, outbound }, driver)
    }

    /// Wraps a byte stream in a multiplexed connection and spawns its [`Driver`] on the tokio
    /// runtime. Driver errors are logged.
    pub fn spawn<S>(io: S) -> Mux
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (mux, driver) = Mux::new(io);
        tokio::spawn(async move {
            if let Err(e) = driver.await {
                tracing::warn!("Multiplexed connection failed: {}", e);
            }
        });
        mux
    }

    /// Opens a new stream to the peer's listener for `service`.
    ///
    /// The returned transport is typically passed to a generated `Client::new`. Nothing is sent
    /// to the peer until the first message is written.
    pub fn connect<Item, SinkItem, Codec>(
        &self,
        service: impl Into<String>,
        codec: Codec,
    ) -> io::Result<Transport<Item, SinkItem, Codec>> {
        let service = service.into();
        check_service_name(&service)?;
        let stream = self.shared.next_stream.fetch_add(1, Ordering::Relaxed);
        let (tx, inbox) = mpsc::unbounded();
        {
            let mut state = self.shared.state();
            // If the connection is already closed, the inbox sender is dropped here, so the
            // stream ends immediately.
            if !state.closed {
                state.client_streams.insert(
                    stream,
                    ClientStream {
                        service: service.clone(),
                        inbox: tx,
                    },
                );
            }
        }
        let raw = RawStream {
            shared: self.shared.clone(),
            outbound: self.outbound.clone(),
            inbox,
            stream,
            role: Role::Client {
                unopened: Some(service),
            },
        };
        Ok(Transport::new(raw, codec))
    }

    /// Listens for streams the peer opens to `service`.
    ///
    /// Each stream is yielded by the returned [`Incoming`] as a new transport, typically wrapped
    /// in a [`BaseChannel`](crate::server::BaseChannel). Only one listener per service name may
    /// exist at a time; the name becomes available again when the listener is dropped. Streams
    /// that were already accepted are unaffected by dropping the listener.
    pub fn listen<Item, SinkItem, Codec, CodecFn>(
        &self,
        service: impl Into<String>,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, CodecFn>>
    where
        CodecFn: FnMut() -> Codec,
    {
        let service = service.into();
        check_service_name(&service)?;
        let id = self.shared.next_listener.fetch_add(1, Ordering::Relaxed);
        let (tx, streams) = mpsc::unbounded();
        {
            let mut state = self.shared.state();
            // If the connection is already closed, the sender is dropped here, so the listener
            // ends immediately.
            if !state.closed {
                match state.listeners.entry(service.clone()) {
                    Entry::Occupied(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            format!("a listener for service {service:?} already exists"),
                        ));
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Listener {
                            id,
                            streams: tx,
                            outbound: self.outbound.clone(),
                        });
                    }
                }
            }
        }
        Ok(Incoming {
            streams,
            codec_fn,
            guard: ListenerGuard {
                shared: self.shared.clone(),
                service,
                id,
            },
            ghost: PhantomData,
        })
    }
}

fn check_service_name(service: &str) -> io::Result<()> {
    if service.len() > u16::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "service name is {} bytes long; the maximum is {}",
                service.len(),
                u16::MAX
            ),
        ));
    }
    Ok(())
}

/// Performs all I/O for a multiplexed connection.
///
/// Resolves when the peer closes the connection, when an I/O error occurs, or when all local
/// [`Mux`] handles, [`Incoming`] listeners, and streams have been dropped. When it resolves or is
/// dropped, all streams on the connection end.
#[must_use = "the driver must be polled (e.g. spawned) for the connection to make progress"]
#[pin_project]
pub struct Driver<S> {
    #[pin]
    framed: Framed<S, FrameCodec>,
    frames: mpsc::UnboundedReceiver<Frame<Bytes>>,
    /// Frames generated by the driver itself, in response to frames from the peer.
    rejects: VecDeque<Frame<Bytes>>,
    guard: ShutdownGuard,
}

impl<S> fmt::Debug for Driver<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver").finish_non_exhaustive()
    }
}

impl<S> Driver<S>
where
    S: AsyncRead + AsyncWrite,
{
    /// Routes incoming frames to their streams. Resolves when the peer closes the connection.
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        for _ in 0..READ_BUDGET {
            match ready!(this.framed.as_mut().poll_next(cx)) {
                Some(frame) => this.guard.0.route(frame?, this.rejects),
                None => return Poll::Ready(Ok(())),
            }
        }
        // Let writes make progress before reading more.
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Writes outgoing frames. Resolves when no local handles to the connection remain.
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            ready!(this.framed.as_mut().poll_ready(cx))?;
            let frame = match this.rejects.pop_front() {
                Some(frame) => frame,
                None => match this.frames.poll_next_unpin(cx) {
                    Poll::Ready(Some(frame)) => frame,
                    Poll::Ready(None) => {
                        ready!(this.framed.as_mut().poll_close(cx))?;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => {
                        ready!(this.framed.as_mut().poll_flush(cx))?;
                        return Poll::Pending;
                    }
                },
            };
            this.framed.as_mut().start_send(frame)?;
        }
    }
}

impl<S> Future for Driver<S>
where
    S: AsyncRead + AsyncWrite,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = match self.as_mut().poll_read(cx) {
            Poll::Ready(result) => result,
            Poll::Pending => ready!(self.as_mut().poll_write(cx)),
        };
        self.guard.0.shutdown(result.as_ref().err());
        Poll::Ready(result)
    }
}

/// Ends all streams when the driver is dropped.
struct ShutdownGuard(Arc<Shared>);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.shutdown(None);
    }
}

/// A stream of transports opened by the peer to a service, returned by [`Mux::listen`].
#[must_use = "streams do nothing unless polled"]
#[pin_project]
pub struct Incoming<Item, SinkItem, CodecFn> {
    streams: mpsc::UnboundedReceiver<RawStream>,
    codec_fn: CodecFn,
    guard: ListenerGuard,
    ghost: PhantomData<(fn(SinkItem), fn() -> Item)>,
}

impl<Item, SinkItem, CodecFn> Incoming<Item, SinkItem, CodecFn> {
    /// Returns the name of the service being listened on.
    pub fn service(&self) -> &str {
        &self.guard.service
    }
}

impl<Item, SinkItem, CodecFn> fmt::Debug for Incoming<Item, SinkItem, CodecFn> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("service", &self.guard.service)
            .finish_non_exhaustive()
    }
}

impl<Item, SinkItem, Codec, CodecFn> Stream for Incoming<Item, SinkItem, CodecFn>
where
    CodecFn: FnMut() -> Codec,
{
    type Item = Transport<Item, SinkItem, Codec>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let raw = ready!(this.streams.poll_next_unpin(cx));
        Poll::Ready(raw.map(|raw| Transport::new(raw, (this.codec_fn)())))
    }
}

/// Unregisters a listener when its [`Incoming`] is dropped.
struct ListenerGuard {
    shared: Arc<Shared>,
    service: String,
    id: u64,
}

impl Drop for ListenerGuard {
    fn drop(&mut self) {
        let mut state = self.shared.state();
        if state.listeners.get(&self.service).map(|l| l.id) == Some(self.id) {
            state.listeners.remove(&self.service);
        }
    }
}

/// One stream of a multiplexed connection, serializing messages with a
/// [`tokio_serde`] codec.
///
/// A client-side transport (from [`Mux::connect`]) sends [`ClientMessage`](crate::ClientMessage)s
/// and receives [`Response`](crate::Response)s; a server-side transport (from [`Incoming`]) does
/// the opposite.
#[pin_project]
pub struct Transport<Item, SinkItem, Codec> {
    #[pin]
    inner: SerdeFramed<RawStream, Item, SinkItem, IoCodec<Codec>>,
}

impl<Item, SinkItem, Codec> Transport<Item, SinkItem, Codec> {
    fn new(raw: RawStream, codec: Codec) -> Self {
        Transport {
            inner: SerdeFramed::new(raw, IoCodec(codec)),
        }
    }
}

impl<Item, SinkItem, Codec> fmt::Debug for Transport<Item, SinkItem, Codec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw = self.inner.get_ref();
        f.debug_struct("Transport")
            .field("stream", &raw.stream)
            .field("role", &raw.role)
            .finish()
    }
}

impl<Item, SinkItem, Codec> Stream for Transport<Item, SinkItem, Codec>
where
    Codec: Deserializer<Item>,
    Codec::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Item = io::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<io::Result<Item>>> {
        self.project().inner.poll_next(cx)
    }
}

impl<Item, SinkItem, Codec> Sink<SinkItem> for Transport<Item, SinkItem, Codec>
where
    Codec: Serializer<SinkItem>,
    Codec::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> io::Result<()> {
        self.project().inner.start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_close(cx)
    }
}

/// Adapts a codec's errors to [`io::Error`], the error type of [`RawStream`].
#[pin_project]
struct IoCodec<C>(#[pin] C);

impl<T, C> Serializer<T> for IoCodec<C>
where
    C: Serializer<T>,
    C::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Error = io::Error;

    fn serialize(self: Pin<&mut Self>, item: &T) -> io::Result<Bytes> {
        self.project().0.serialize(item).map_err(io::Error::other)
    }
}

impl<T, C> Deserializer<T> for IoCodec<C>
where
    C: Deserializer<T>,
    C::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Error = io::Error;

    fn deserialize(self: Pin<&mut Self>, src: &BytesMut) -> io::Result<T> {
        self.project().0.deserialize(src).map_err(io::Error::other)
    }
}

/// One stream of a multiplexed connection, carrying serialized messages.
struct RawStream {
    shared: Arc<Shared>,
    outbound: mpsc::UnboundedSender<Frame<Bytes>>,
    inbox: mpsc::UnboundedReceiver<io::Result<BytesMut>>,
    stream: u64,
    role: Role,
}

#[derive(Debug)]
enum Role {
    /// This side opened the stream. The service name is sent with the first message.
    Client { unopened: Option<String> },
    /// The peer opened the stream.
    Server,
}

impl Stream for RawStream {
    type Item = io::Result<BytesMut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inbox.poll_next_unpin(cx)
    }
}

impl Sink<Bytes> for RawStream {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.outbound.is_closed() {
            return Poll::Ready(Err(connection_closed()));
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, payload: Bytes) -> io::Result<()> {
        let stream = self.stream;
        let frame = match &mut self.role {
            Role::Client { unopened } => match unopened.take() {
                Some(service) => Frame::Open {
                    stream,
                    service,
                    payload,
                },
                None => Frame::ClientData { stream, payload },
            },
            Role::Server => Frame::ServerData { stream, payload },
        };
        self.outbound
            .unbounded_send(frame)
            .map_err(|_| connection_closed())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for RawStream {
    fn drop(&mut self) {
        let stream = self.stream;
        let frame = match &self.role {
            Role::Client { unopened } => {
                self.shared.state().client_streams.remove(&stream);
                // The peer never heard of a stream that sent nothing.
                unopened.is_none().then_some(Frame::ClientClose { stream })
            }
            Role::Server => {
                self.shared.state().server_streams.remove(&stream);
                Some(Frame::ServerClose { stream })
            }
        };
        if let Some(frame) = frame {
            let _ = self.outbound.unbounded_send(frame);
        }
    }
}

fn connection_closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the multiplexed connection is closed",
    )
}

/// State shared between a connection's driver, handles, listeners, and streams.
#[derive(Default)]
struct Shared {
    next_stream: AtomicU64,
    next_listener: AtomicU64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Set once the driver has finished; nothing new is registered afterwards.
    closed: bool,
    listeners: HashMap<String, Listener>,
    /// Streams opened by this side, keyed by this side's stream IDs.
    client_streams: HashMap<u64, ClientStream>,
    /// Streams opened by the peer, keyed by the peer's stream IDs.
    server_streams: HashMap<u64, mpsc::UnboundedSender<io::Result<BytesMut>>>,
}

struct Listener {
    id: u64,
    streams: mpsc::UnboundedSender<RawStream>,
    /// Used by streams accepted on behalf of this listener.
    outbound: mpsc::UnboundedSender<Frame<Bytes>>,
}

struct ClientStream {
    service: String,
    inbox: mpsc::UnboundedSender<io::Result<BytesMut>>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        // State is never left inconsistent by a panic, so poisoning can be ignored.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Delivers a frame received from the peer.
    fn route(self: &Arc<Self>, frame: Frame<BytesMut>, rejects: &mut VecDeque<Frame<Bytes>>) {
        let mut state = self.state();
        match frame {
            Frame::Open {
                stream,
                service,
                payload,
            } => {
                if state.server_streams.contains_key(&stream) {
                    tracing::warn!(stream, "Peer reopened a stream that is already open.");
                    return;
                }
                let Some(listener) = state.listeners.get(&service) else {
                    tracing::debug!(stream, service, "Rejecting stream to unknown service.");
                    rejects.push_back(Frame::Reject { stream });
                    return;
                };
                let (tx, inbox) = mpsc::unbounded();
                let _ = tx.unbounded_send(Ok(payload));
                let raw = RawStream {
                    shared: self.clone(),
                    outbound: listener.outbound.clone(),
                    inbox,
                    stream,
                    role: Role::Server,
                };
                let listener = listener.streams.clone();
                state.server_streams.insert(stream, tx);
                // The stream unregisters itself on drop, which requires the lock.
                drop(state);
                // If the listener is gone, dropping the stream closes it.
                let _ = listener.unbounded_send(raw);
            }
            Frame::ClientData { stream, payload } => {
                if let Some(inbox) = state.server_streams.get(&stream) {
                    let _ = inbox.unbounded_send(Ok(payload));
                }
            }
            Frame::ClientClose { stream } => {
                state.server_streams.remove(&stream);
            }
            Frame::ServerData { stream, payload } => {
                if let Some(client) = state.client_streams.get(&stream) {
                    let _ = client.inbox.unbounded_send(Ok(payload));
                }
            }
            Frame::ServerClose { stream } => {
                state.client_streams.remove(&stream);
            }
            Frame::Reject { stream } => {
                if let Some(client) = state.client_streams.remove(&stream) {
                    let _ = client.inbox.unbounded_send(Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("peer is not listening on service {:?}", client.service),
                    )));
                }
            }
        }
    }

    /// Ends all streams and listeners, first failing streams with `error`, if any.
    fn shutdown(&self, error: Option<&io::Error>) {
        let (client_streams, server_streams, listeners) = {
            let mut state = self.state();
            if state.closed {
                return;
            }
            state.closed = true;
            (
                mem::take(&mut state.client_streams),
                mem::take(&mut state.server_streams),
                mem::take(&mut state.listeners),
            )
        };
        if let Some(error) = error {
            let inboxes = client_streams
                .values()
                .map(|client| &client.inbox)
                .chain(server_streams.values());
            for inbox in inboxes {
                let _ = inbox.unbounded_send(Err(io::Error::new(error.kind(), error.to_string())));
            }
        }
        drop(listeners);
    }
}

/// The unit of data sent over a multiplexed connection.
///
/// Stream IDs are allocated by the side that opens a stream, so the two sides' IDs overlap. The
/// frame kind disambiguates: `Open`, `ClientData`, and `ClientClose` refer to a stream opened by the
/// sender, while `ServerData`, `ServerClose`, and `Reject` refer to one opened by the receiver.
#[derive(Debug)]
enum Frame<P> {
    /// Opens a stream to a service and carries its first message.
    Open {
        stream: u64,
        service: String,
        payload: P,
    },
    ClientData {
        stream: u64,
        payload: P,
    },
    ClientClose {
        stream: u64,
    },
    ServerData {
        stream: u64,
        payload: P,
    },
    ServerClose {
        stream: u64,
    },
    /// The service named when opening the stream has no listener.
    Reject {
        stream: u64,
    },
}

const OPEN: u8 = 0;
const CLIENT_DATA: u8 = 1;
const CLIENT_CLOSE: u8 = 2;
const SERVER_DATA: u8 = 3;
const SERVER_CLOSE: u8 = 4;
const REJECT: u8 = 5;

/// The length of the kind and stream ID that start every frame.
const HEADER_LEN: usize = 1 + 8;

/// Encodes frames as `kind: u8, stream: u64, [service_len: u16, service], payload`, inside a
/// length-delimited frame. The service name is only present in `Open` frames.
struct FrameCodec {
    inner: LengthDelimitedCodec,
}

impl Encoder<Frame<Bytes>> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame<Bytes>, dst: &mut BytesMut) -> io::Result<()> {
        let (kind, stream, service, payload) = match frame {
            Frame::Open {
                stream,
                service,
                payload,
            } => (OPEN, stream, Some(service), payload),
            Frame::ClientData { stream, payload } => (CLIENT_DATA, stream, None, payload),
            Frame::ClientClose { stream } => (CLIENT_CLOSE, stream, None, Bytes::new()),
            Frame::ServerData { stream, payload } => (SERVER_DATA, stream, None, payload),
            Frame::ServerClose { stream } => (SERVER_CLOSE, stream, None, Bytes::new()),
            Frame::Reject { stream } => (REJECT, stream, None, Bytes::new()),
        };
        let service_len = service.as_ref().map_or(0, |service| 2 + service.len());
        let mut buf = BytesMut::with_capacity(HEADER_LEN + service_len + payload.len());
        buf.put_u8(kind);
        buf.put_u64(stream);
        if let Some(service) = service {
            // Service name lengths are checked when streams are created.
            buf.put_u16(service.len() as u16);
            buf.put_slice(service.as_bytes());
        }
        buf.put_slice(&payload);
        self.inner.encode(buf.freeze(), dst)
    }
}

impl Decoder for FrameCodec {
    type Item = Frame<BytesMut>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame<BytesMut>>> {
        let Some(mut buf) = self.inner.decode(src)? else {
            return Ok(None);
        };
        if buf.len() < HEADER_LEN {
            return Err(invalid_frame("frame is shorter than its header"));
        }
        let kind = buf.get_u8();
        let stream = buf.get_u64();
        let frame = match kind {
            OPEN => {
                if buf.len() < 2 {
                    return Err(invalid_frame("open frame is missing its service name"));
                }
                let len = buf.get_u16() as usize;
                if buf.len() < len {
                    return Err(invalid_frame("open frame is missing its service name"));
                }
                let service = buf.split_to(len);
                let service = std::str::from_utf8(&service)
                    .map_err(|_| invalid_frame("service name is not valid UTF-8"))?
                    .to_owned();
                Frame::Open {
                    stream,
                    service,
                    payload: buf,
                }
            }
            CLIENT_DATA => Frame::ClientData {
                stream,
                payload: buf,
            },
            CLIENT_CLOSE => Frame::ClientClose { stream },
            SERVER_DATA => Frame::ServerData {
                stream,
                payload: buf,
            },
            SERVER_CLOSE => Frame::ServerClose { stream },
            REJECT => Frame::Reject { stream },
            kind => return Err(invalid_frame(&format!("unknown frame kind {kind}"))),
        };
        Ok(Some(frame))
    }
}

fn invalid_frame(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(frame: Frame<Bytes>) -> Frame<BytesMut> {
        let mut codec = FrameCodec {
            inner: LengthDelimitedCodec::new(),
        };
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(buf.is_empty());
        decoded
    }

    #[test]
    fn frame_roundtrip() {
        let frame = roundtrip(Frame::Open {
            stream: 7,
            service: "counter/a".into(),
            payload: Bytes::from_static(b"hello"),
        });
        assert!(matches!(
            frame,
            Frame::Open { stream: 7, ref service, ref payload }
                if service == "counter/a" && &payload[..] == b"hello"
        ));

        let frame = roundtrip(Frame::ServerData {
            stream: u64::MAX,
            payload: Bytes::from_static(b"world"),
        });
        assert!(matches!(
            frame,
            Frame::ServerData { stream: u64::MAX, ref payload } if &payload[..] == b"world"
        ));

        assert!(matches!(
            roundtrip(Frame::Reject { stream: 3 }),
            Frame::Reject { stream: 3 }
        ));
    }

    #[test]
    fn decode_rejects_bad_frames() {
        let mut codec = FrameCodec {
            inner: LengthDelimitedCodec::new(),
        };
        for body in [&b"\x00"[..], b"\x09\x00\x00\x00\x00\x00\x00\x00\x00"] {
            let mut buf = BytesMut::new();
            LengthDelimitedCodec::new()
                .encode(Bytes::copy_from_slice(body), &mut buf)
                .unwrap();
            let err = codec.decode(&mut buf).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn long_service_name() {
        assert!(check_service_name(&"x".repeat(u16::MAX as usize)).is_ok());
        assert!(check_service_name(&"x".repeat(u16::MAX as usize + 1)).is_err());
    }
}
