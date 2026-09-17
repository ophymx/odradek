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
  dropped socket — as are `403` for topics the hub's gate denies
  (`Hub::with_topic_gate`), `404` for topics the source does not have,
  `503` after shutdown, and `502` for other source trouble.
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

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
