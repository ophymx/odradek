# odradek-web-sse

Server-Sent Events transport over
[`odradek-web-core`](https://crates.io/crates/odradek-web-core): an
embeddable axum `Router` that streams Kafka topics to browsers.

- `GET /topics/{topic}/partitions/{p}/events` — one partition; each
  event's id is its resume token (the next offset), so a reconnecting
  `EventSource` resumes via `Last-Event-ID` without missing or
  repeating a record.
- `GET /topics/{topic}/events` — every partition merged into one
  stream (order holds within partitions), with a multi-partition
  cursor (`partition:next_offset,...`) as the resume token.
- `from=` start positions and key/header filters as query parameters;
  UTF-8 payloads as strings, binary as base64.

It is a `Router`, not a server: mount it under your own routes and
layer your own auth.

```rust
use odradek_web_sse::{router, SseState};

let app = axum::Router::new()
    .nest("/kafka", router(state))
    .route_layer(my_auth_layer);
```

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
