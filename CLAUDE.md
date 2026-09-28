# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace

Cargo workspace (edition 2024, MSRV 1.85) with three members:

- `tarpc/` — the RPC framework library.
- `plugins/` — the `tarpc-plugins` proc-macro crate providing `#[tarpc::service]` and `#[tarpc::derive_serde]`. Re-exported from `tarpc`.
- `example-service/` — a client/server binary pair demonstrating TCP + OpenTelemetry (OTLP/Jaeger) tracing.

## Commands

Most tests, examples, and code paths are gated behind Cargo features, so plain `cargo test` skips a lot. Use `--all-features` (or `--features full`) for broad coverage.

```bash
cargo test --manifest-path tarpc/Cargo.toml --all-features     # full library test suite
cargo test -p tarpc --all-features --test service_functional   # one integration test file
cargo test -p tarpc --all-features <test_name>                 # single test by name
cargo test -p tarpc-plugins                                    # proc-macro tests
cargo run -p tarpc --example readme --features full            # run an example (see required-features in tarpc/Cargo.toml)

cargo fmt --all -- --check                                     # CI formatting check
cargo clippy --all-features --all-targets -- -D warnings       # CI lint (warnings are errors)
```

CI (`.github/workflows/main.yml`) runs `cargo test --manifest-path tarpc/Cargo.toml` across a matrix of feature combinations (`serde1`, `tokio1`, `serde-transport`, `serde-transport-json`, `serde-transport-bincode`, `tcp`, `unix`), including with no features. Code must compile and pass tests under each valid combination, so guard feature-specific code with `#[cfg(feature = ...)]` carefully. CI also runs every example in `tarpc/examples`.

Feature dependencies: `serde-transport` implies `serde1` + `tokio1`; `serde-transport-{json,bincode}` imply `serde-transport`; `tcp`/`unix` are only meaningful with `serde-transport`.

### Compile-fail (trybuild) tests

`tarpc/tests/compile_fail.rs` runs trybuild against `tarpc/tests/compile_fail/**`. Subdirectories are selected by feature: `serde1/`, `no_serde1/`, `serde_transport/` (needs `serde-transport` + `tcp`). If compiler diagnostics change, regenerate the `.stderr` snapshots with `TRYBUILD=overwrite cargo test -p tarpc --all-features --test compile_fail` (and again without `serde1` for `no_serde1/`), then review the diff.

### Git hooks

`hooks/pre-commit` (ASCII filenames, whitespace, `rustfmt --edition 2024 --check` on staged `.rs` files) and `hooks/pre-push` (clean working copy) are opt-in; copy them into `.git/hooks`.

## Architecture

### Service macro (`plugins/src/lib.rs`)

`#[tarpc::service] trait Foo { async fn bar(x: X) -> Y; }` expands to:
- trait `Foo` whose methods take `self, context::Context, ...args` (implementors are `Clone` and consumed per request);
- `FooRequest` / `FooResponse` enums (one variant per RPC; serde derives when `serde1` is on, or custom via `#[tarpc::service(derive = [...])]`);
- `ServeFoo<S>` implementing `server::Serve` (dispatches request enum → trait method), obtained via `Foo::serve(self)`;
- `FooClient<Stub>` with `FooClient::new(client::Config, transport)` returning a `client::NewClient` (call `.spawn()`, which requires `tokio1`, or drive the dispatch future manually) and one async method per RPC taking a `Context` first.

Changes to generated code affect `plugins/tests`, `tarpc/tests/proc_macro_hygene.rs`, and the trybuild snapshots.

### Core library (`tarpc/src`)

- `lib.rs` — wire types: `ClientMessage` (`Request` or `Cancel`), `Request<T>`, `Response<T>`, `ServerError`, `ChannelError`. Its crate-level docs are the source of `README.md` (between the `cargo-sync-readme` markers); edit `lib.rs` docs and keep README in sync.
- `transport.rs` — `Transport` is any `Stream<Item = Result<In, E>> + Sink<Out>`; `transport::channel` provides in-memory transports used heavily in tests.
- `serde_transport.rs` — framed codec transport over any `AsyncRead + AsyncWrite` using `tokio-serde`, plus `tcp` and `unix` connect/listen helpers.
- `mux.rs` — multiplexes many named-service streams, in both directions, over one `AsyncRead + AsyncWrite` connection. A `Driver` future owns the socket and routes frames (`Open`/`ClientData`/`ClientClose`/`ServerData`/`ServerClose`/`Reject`, keyed by stream ID + direction) to per-stream virtual transports, which are then used with the normal client and `BaseChannel`.
- `context.rs` — per-request `Context` carrying the deadline (`Instant`; serialized as a relative duration) and the OpenTelemetry trace context. `context::current()` derives from the active tracing span, which is how deadlines and traces propagate across chained RPCs.
- `client.rs` — `Channel` (cheap cloneable handle) sends requests over an mpsc to `RequestDispatch`, a future that owns the transport, multiplexes in-flight requests (`client/in_flight_requests.rs`), enforces deadlines, and sends `Cancel` messages when a caller drops its response future. `client/stub.rs` defines the `Stub` trait with `load_balance`, `retry`, and `mock` implementations.
- `server.rs` — `BaseChannel` wraps a transport and tracks in-flight requests (`server/in_flight_requests.rs`), handling cancellation and deadline expiry. The `Channel` trait adds combinators; `Channel::requests()` yields `InFlightRequest`s and `.execute(serve)` yields response futures the caller spawns. `server/incoming.rs` handles streams of channels; `server/limits/` provides per-channel request limits and channels-per-key throttling; `server/request_hook/` provides before/after hooks via the `RequestHook` extension trait on `Serve`.
- `cancellations.rs` — internal channel used to signal cancelled request IDs between request guards and the dispatch/channel futures.
- `trace.rs` — `TraceId`/`SpanId` and conversions to/from OpenTelemetry span context.

Everything is poll-based: `RequestDispatch`, `BaseChannel`, and friends are hand-written `Future`/`Stream`/`Sink` implementations using `pin-project`, so changes usually involve careful `poll_*` state handling rather than `async fn`.

## Release notes

User-visible and especially breaking changes are recorded in `RELEASES.md`.
