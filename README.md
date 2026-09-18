# odradek

A constellation of Rust crates for Kafka-protocol integrations.

Kafka is becoming a protocol beyond the Apache implementation — multiple
brokers, proxies, and clients now speak the same wire format. odradek treats
the protocol as the first-class artifact and builds outward from it.

## Crates

| Crate | Purpose |
|---|---|
| [`odradek-protocol`](crates/odradek-protocol) | Sans-I/O wire protocol: primitive codecs (varints, compact strings, tagged fields), the API key registry, versioned message types generated from the upstream schemas, and the record-batch codec with the byte-identical proxy guarantee. |
| [`odradek-client`](crates/odradek-client) | Async, Rust-native Kafka client built on tokio: connections, metadata routing, producer, consumer. TLS, SASL, and each compression codec are default-on cargo features you can opt out of. |
| [`odradek-acceptance`](crates/odradek-acceptance) | Acceptance suite that validates *either side* of the protocol: run it against a server (suite acts as client) or against a client (suite acts as server harness). |
| [`odradek-web-core`](crates/odradek-web-core) | Transport-agnostic bridge from Kafka partitions to web-shaped subscribers: fan-out, replay from offsets, filtering, self-healing backpressure. |
| [`odradek-web-sse`](crates/odradek-web-sse) | Server-Sent Events transport over the bridge: an embeddable axum router with `Last-Event-ID` resume — a reconnecting `EventSource` never misses or repeats a record. |
| [`odradek-web-ws`](crates/odradek-web-ws) | WebSocket transport over the bridge: the same streams as JSON text frames, offset-resumable via `from=`. |

With TLS and SASL in the client, the whole constellation — client,
bridge, and transports — can face real-world clusters.

## Design principles

- **Sans-I/O core.** `odradek-protocol` does encoding/decoding only. The
  client, the acceptance suite (which must impersonate both sides), and the
  proxies all reuse the same codec, so a protocol fix lands once.
- **Unknown data round-trips.** Tagged fields and unrecognized extensions are
  preserved as raw bytes, which proxying and forward-compatibility both
  require.
- **Malformed input never panics.** Decoders return typed errors; the
  acceptance suite depends on being able to feed hostile bytes to the codec.
- **Checks are data.** Acceptance checks carry stable ids and the requirement
  they verify, so conformance reports are citable and diffable across
  implementations.

## Status

Early but functional end to end:

- `odradek-protocol`: wire primitives; message types generated from the
  Kafka 4.1.0 schemas vendored in `crates/odradek-protocol/schemas/`
  (headers, ApiVersions, Metadata, Produce, Fetch, CreateTopics,
  ListOffsets, FindCoordinator, OffsetCommit/Fetch) via
  `cargo xtask codegen`;
  header-version selection including the ApiVersions response-header quirk;
  record batch (v2) encoding with CRC-32C validation — compressed and
  unknown-codec payloads stay raw and re-encode byte-identically (the
  proxy guarantee), verified against a golden segment produced by a real
  Kafka 4.1 broker. Known tagged fields are materialized as typed
  `Option` struct fields (`None` = absent on the wire, present-null
  distinguished for nullable ones); unknown tags still round-trip raw
  through `unknown_tagged_fields`.
- `odradek-client`: framed connection with correlation-id pipelining and
  ApiVersions negotiation (including the `UNSUPPORTED_VERSION` downgrade
  path), over plaintext or TLS (rustls; Mozilla roots, custom CA, or a
  caller-built config), optionally mutual — a client certificate
  authenticates the connection itself against a cluster running
  `ssl.client.auth=required`, verified live against one — with optional
  SASL — PLAIN and
  SCRAM-SHA-256/512 per RFC 5802, server signature verified, checked
  against the RFC 7677 vector — authenticated on every connection; the cluster layer: metadata discovery, a per-broker connection
  pool with per-broker version ranges, and partition-leader routing; and
  a producer and consumer: the producer batches records per partition
  (size-triggered or explicit flush), compresses with gzip, lz4,
  snappy (xerial framing), or zstd (compression is a client concern —
  the protocol crate carries payloads raw; interop for all four codecs
  is verified in both directions against Kafka's Java tools), and retries through leadership changes by
  invalidating stale metadata; the consumer fetches, decompresses, and
  materializes records with absolute offsets,
  skipping control batches and pre-offset records, with earliest/latest
  lookup via ListOffsets and durable positions as a simple (non-member)
  consumer — coordinator discovery plus offset commit/fetch under a
  group id; classic consumer-group membership (join/sync/heartbeat/
  leave, caller-driven, with leader-side range assignment matching
  Kafka's RangeAssignor); and KIP-848 next-generation membership
  (one ConsumerGroupHeartbeat API, broker-side assignment addressed
  by topic id, member-epoch fencing on heartbeats and commits) —
  tested against in-process fake single- and multi-broker clusters,
  with real-broker smoke examples: `group_consume` and
  `group848_consume` assemble the whole thing — join a group, resume
  from committed offsets, read the assigned partitions, commit, and
  prove a second member picks up exactly where the first stopped —
  alongside `produce_consume`, `group_join`, and `group848_join`, which
  exercise the pieces on their own (the last one covering live
  incremental reconciliation on Kafka 4.1's new coordinator).
- `odradek-acceptance`: conformance checks for **both roles** over raw
  connections, independent of the client crate. 27 server checks across
  13 APIs plus 7 client checks, each one proven by an injected fault to
  detect what it claims — a check nothing can trip fails calibration.
  `cargo xtask conformance` runs them against Apache Kafka and Redpanda
  in Docker and enforces the baselines committed in
  [`conformance/`](conformance/). See the
  [crate README](crates/odradek-acceptance) for what is covered, how
  calibration works, and what the baselines currently record.

- `odradek-web-core`: the bridging engine, transport-agnostic — one
  pump per (topic, partition) fans out to any number of subscribers,
  each starting earliest/latest/at-an-offset with per-subscriber
  filters (key prefix, header match). Backpressure is self-healing: a
  slow subscriber falls out of the live path into catch-up (from the
  in-memory ring, or from Kafka past it) and rejoins as it drains —
  offset order, no gaps, no duplicates, at any speed. Engine-tested
  over an in-memory source (published as `odradek_web_core::memory` for
  downstream tests); the `tail` example replays and live-tails a real
  broker.
- `odradek-web-sse`: the first transport — an embeddable axum `Router`
  (mount it in your own service, layer your own auth) streaming
  `GET /topics/{topic}/partitions/{p}/events` as SSE with resume
  tokens as event ids (opaque — echoed back, not parsed),
  `Last-Event-ID` reconnect resume, `from=` positions, and
  key/header filters; UTF-8 payloads as strings, binary as base64.
  Tested over a real listener with a raw HTTP client; the `serve`
  example bridges a real broker to `curl -N`.
- `odradek-web-ws`: the WebSocket transport — the same subscriptions
  as JSON text frames (shared JSON mapping in
  `odradek_web_core::json`), parameter errors rejected before the
  upgrade, resume via `from=<offset>`. Tested with a hand-rolled
  WebSocket client (upgrade handshake + frame parser); its `serve`
  example mounts both transports on one router.
- Topic-level subscriptions on all of the above: `GET /topics/{t}/events`
  (SSE) and `/topics/{t}/ws` merge every partition into one stream —
  order holds within partitions — with a multi-partition cursor
  (`partition:next_offset,...`) as the resume token; SSE carries it as
  every event's id, so `Last-Event-ID` reconnects resume all
  partitions loss-free (unseen partitions replay from earliest).

TLS and SASL are validated live (`secure_smoke` example) against
Kafka's SASL_PLAINTEXT, SSL, and SASL_SSL listeners: PLAIN and both
SCRAM variants authenticate, wrong passwords and untrusted
certificates fail cleanly.

Next: crates.io publication.

```sh
cargo test --workspace       # everything
cargo xtask codegen          # regenerate message types from schemas
cargo xtask conformance      # real-broker conformance runs (needs docker)
cargo xtask conformance --record   # refresh baselines from a run
```

## License

Copyright © 2026 Jeffrey T. Peckham (Ophymx).

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

The message schemas vendored in `crates/odradek-protocol/schemas/` are
copied from [Apache Kafka](https://github.com/apache/kafka) and remain
licensed to the Apache Software Foundation under the Apache License,
Version 2.0, as noted in each file's header.
