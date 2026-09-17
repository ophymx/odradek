# odradek-web-core

Transport-agnostic bridge from Kafka partitions to web-shaped
subscribers.

One pump per (topic, partition) fans out to any number of subscribers,
each starting from earliest, latest, or a specific offset, with
per-subscriber filters (key prefix, header match). Backpressure is
self-healing: a slow subscriber falls out of the live path into
catch-up — served from an in-memory ring, or from Kafka past it — and
rejoins as it drains. Offset order, no gaps, no duplicates, at any
subscriber speed.

This crate is the engine; transports are thin layers on top:

- [`odradek-web-sse`](https://crates.io/crates/odradek-web-sse) —
  Server-Sent Events with `Last-Event-ID` resume
- [`odradek-web-ws`](https://crates.io/crates/odradek-web-ws) —
  WebSocket JSON frames

The source is pluggable through the `RecordSource` / `SourceFactory`
traits: Kafka via
[`odradek-client`](https://crates.io/crates/odradek-client) in
production, and an in-memory log (published as
`odradek_web_core::memory`) for deterministic downstream tests.

## Features

`kafka` (default) pulls in `odradek-client` for the real-cluster
`KafkaSource`. With `--no-default-features` the engine — pump, hub,
filters, cursors, in-memory source — builds without any Kafka client
at all; bring your own `RecordSource`.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
