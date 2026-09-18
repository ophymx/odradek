# odradek-web-ws

WebSocket transport over
[`odradek-web-core`](https://crates.io/crates/odradek-web-core): the
same Kafka subscriptions as
[`odradek-web-sse`](https://crates.io/crates/odradek-web-sse), delivered
as JSON text frames.

- `GET /topics/{topic}/partitions/{p}/ws` — one partition, resumable
  via `from=<offset>`.
- `GET /topics/{topic}/ws` — every partition merged into one stream,
  resumable via a multi-partition cursor.
- Parameter errors are rejected as plain HTTP 400s *before* the
  upgrade, so misconfigured clients get a readable error instead of a
  dropped socket — as are `403` for a topic the hub's gate denies or an
  `Origin` the state's `OriginPolicy` does not, `404` for topics *and
  partitions* the source does not have, `503` after shutdown or at pump
  capacity, and `502` for other source trouble.
- A stream that fails mid-flight closes the socket with code `1008`
  (auth) or `1011` (anything else) and the kind and message as the
  close reason; a clean end closes with `1001` ("going away").
- Graceful shutdown: call `WsState::shutdown()` (e.g. from axum's
  `with_graceful_shutdown`) to stop every pump, close open sockets
  with `1001`, and refuse new upgrades. Idle pumps also exit on their
  own after `PumpConfig::idle_shutdown` (default 30s) and respawn on
  demand.

Like the SSE crate, it is an embeddable axum `Router` — mount it in
your own service and layer your own auth. Both transports share one
JSON mapping (`odradek_web_core::json`), so the same consumer code
works against either.

## Security

**Cross-origin (CSWSH).** CORS does not apply to WebSockets. A browser
lets any page open a socket to any host and attaches the victim's
cookies to the handshake, so an embedder's cookie authentication
authenticates the *attacker's* connection. The only defence is the
`Origin` header, and checking it is this crate's job:

```rust
use odradek_web_ws::{OriginPolicy, PumpConfig, WsState};
use odradek_web_ws::web_core::Hub;

let hub = Hub::new(factory, PumpConfig::default())
    .with_topic_gate(|topic| topic.starts_with("public."));
let state = WsState::from_hub_with_origins(
    hub,
    OriginPolicy::allow(["https://app.example.com"]),
);
```

`WsState::new` and `WsState::from_hub` **deny cross-origin handshakes**
— name your origins to let browsers in. A handshake carrying *no*
`Origin` is allowed by those policies: it is not a browser, and the
attack is a browser attack. That holds only while `Origin`-less
requests are not privileged; if your embedder authorizes by ambient
credential (a cookie, a client certificate, a source IP allowlist), add
`OriginPolicy::require_origin()` and have those clients send one.
`OriginPolicy::any()` exists for a bridge already fronted by something
that authenticates each handshake — it is the explicit opt-out, not a
shortcut.

**Topic authorization.** `WsState::new` takes the hub's gate for the
same reason SSE's does: the alternative default serves every topic on
the cluster, `__consumer_offsets` included, plus enumeration by `404`
probing. Per-user rules belong in a middleware layer over the router,
which can see the request; the gate is per process.

**CORS.** Do not put a permissive CORS layer over these routes to make
anything work — it does nothing for the WebSocket handshake, and the
response bodies are your Kafka data.

**Client frames.** The protocol is send-only, so the upgrade caps
incoming messages and frames at 4 KiB. Without that cap, tungstenite's
defaults let each socket buffer 64 MiB of data the server then throws
away.

**Connection limits and memory.** The hub caps *pumps*
(`Hub::with_max_pumps`, 1024 by default), not sockets; bound concurrent
connections in your server or proxy, since each holds a queue of up to
`PumpConfig::queue_capacity` events. Worst-case memory is
`max_pumps x ring_capacity x <max fetch bytes>` — the ring is bounded
by event *count*, not bytes, and one event can pin the fetch buffer it
came from.

Close reasons are classified, not narrated: the `kind` plus a fixed
message. The upstream's own error text goes to `tracing`.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
