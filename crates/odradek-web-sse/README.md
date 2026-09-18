# odradek-web-sse

Server-Sent Events transport over
[`odradek-web-core`](https://crates.io/crates/odradek-web-core): an
embeddable axum `Router` that streams Kafka topics to browsers.

- `GET /topics/{topic}/partitions/{p}/events` — one partition; each
  event's id is its resume token, so a reconnecting `EventSource`
  resumes via `Last-Event-ID` without missing or repeating a record.
- `GET /topics/{topic}/events` — every partition merged into one
  stream (order holds within partitions), with a multi-partition
  cursor as the resume token.
- Resume tokens are opaque: echo back the id the server sent, do not
  parse or increment it. (The `data` object's `offset` is the
  *record's* position, not the one to resume from.)
- `from=` start positions and key/header filters as query parameters;
  UTF-8 payloads as strings, binary as base64.
- Typed failures: `403` for topics the hub's gate denies, `404` for
  topics *and partitions* the source does not have, `503` after
  shutdown or at pump capacity, `502` for other source trouble. A
  stream that fails mid-flight ends with one `event: error` frame
  carrying `{"kind", "message"}`.
- Graceful shutdown: call `SseState::shutdown()` (e.g. from axum's
  `with_graceful_shutdown`) to stop every pump, end open streams
  cleanly, and refuse new subscribes. Idle pumps also exit on their own
  after `PumpConfig::idle_shutdown` (default 30s) and respawn on demand.

It is a `Router`, not a server: mount it under your own routes and
layer your own auth.

```rust
use odradek_web_sse::{router, PumpConfig, SseState};

// The topic gate is an argument, not an option: these routes are as
// reachable as the port they are mounted on.
let state = SseState::new(factory, PumpConfig::default(), |topic| {
    topic.starts_with("public.")
});

let app = axum::Router::new()
    .nest("/kafka", router(state))
    .route_layer(my_auth_layer);
```

## Security

The response bodies are your Kafka records. Five things to get right:

**Topic authorization.** `SseState::new` takes the hub's gate because
the alternative default is serving every topic on the cluster —
`__consumer_offsets` included — plus topic enumeration by probing for
which names answer `404`. The gate is per *process*; for per-user rules
("this reader may see topic X"), add a middleware layer over the router
— it can see the path, the session, and the headers, which the gate
deliberately cannot:

```rust
let app = axum::Router::new()
    .nest("/kafka", router(state))
    .route_layer(axum::middleware::from_fn(authorize_topic_for_user));
```

**CORS.** Do not put a permissive CORS layer over these routes.
`EventSource` is same-origin by default, and that same-origin policy is
the only thing keeping a page the user happens to visit from reading
your topics; `Access-Control-Allow-Origin: *` hands them to everyone.
If browsers on another origin genuinely need the stream, name that
origin — never `*`, never a reflected `Origin`.

**Authentication.** Layer it yourself (`route_layer`). Nothing here
authenticates anybody.

**Connection limits.** One connection costs a queue of up to
`PumpConfig::queue_capacity` events; the hub caps *pumps*
(`Hub::with_max_pumps`, 1024 by default), not connections. Bound
concurrency in your server or proxy.

**Memory.** Worst case is `max_pumps x ring_capacity x <max fetch
bytes>`: the ring is bounded by event *count*, not bytes, and one event
can pin the whole fetch buffer it came from. With Kafka's
`partition_max_bytes` at 1 MiB, a pump's default 1024-event ring is a
megabyte-scale number.

Errors sent to clients are classified, not narrated: the `kind` plus a
fixed message. The upstream's own error text — broker hostnames, ports,
ACL state — goes to `tracing`.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
