# odradek

A constellation of Rust crates for Kafka-protocol integrations.

Kafka is becoming a protocol beyond the Apache implementation — multiple
brokers, proxies, and clients now speak the same wire format. odradek treats
the protocol as the first-class artifact and builds outward from it.

## Crates

| Crate | Purpose |
|---|---|
| [`odradek-protocol`](crates/odradek-protocol) | Sans-I/O wire protocol: primitive codecs (varints, compact strings, tagged fields), API key registry, and — next — versioned message types generated from the upstream schemas. |
| [`odradek-client`](crates/odradek-client) | Async, Rust-native Kafka client built on tokio: connections, metadata routing, producer, consumer. |
| [`odradek-acceptance`](crates/odradek-acceptance) | Acceptance suite that validates *either side* of the protocol: run it against a server (suite acts as client) or against a client (suite acts as server harness). |

Planned once the above are solid: `odradek-web-*` proxy crates providing
building blocks that bridge Kafka to web clients — fan-out, filtering,
replay, and friends — over WebSocket/SSE.

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
  (headers, ApiVersions, Metadata, Produce, Fetch) via `cargo xtask codegen`;
  header-version selection including the ApiVersions response-header quirk;
  record batch (v2) encoding with CRC-32C validation — compressed and
  unknown-codec payloads stay raw and re-encode byte-identically (the
  proxy guarantee), verified against a golden segment produced by a real
  Kafka 4.1 broker. Known limitation: tagged fields are not yet
  materialized as struct fields — they round-trip losslessly through
  `unknown_tagged_fields`.
- `odradek-client`: framed connection with correlation-id pipelining and
  ApiVersions negotiation (including the `UNSUPPORTED_VERSION` downgrade
  path), plus the cluster layer: metadata discovery, a per-broker
  connection pool with per-broker version ranges, and partition-leader
  routing — tested against in-process fake single- and multi-broker
  clusters.
- `odradek-acceptance`: conformance checks for **both roles** over raw
  connections independent of the client crate — `api-versions/*`,
  `metadata/*`, `produce/*`, and `fetch/*` server checks (including a
  create → produce → fetch flow that asserts the broker returns the
  produced batch byte-identical in the crc-covered region) and `client/*`
  client checks, where the harness impersonates a three-broker cluster so
  partition-leader routing is observable — with JSON reports and
  per-implementation baselines:

  ```sh
  # validate a server
  odradek-accept --server localhost:9092 [--json]
  # validate a client: listen, point its bootstrap here
  odradek-accept --client-listen 127.0.0.1:19092
  # record / enforce expected results per implementation
  odradek-accept --server ... --write-baseline conformance/kafka.json
  odradek-accept --server ... --baseline conformance/kafka.json
  ```

  The suite is calibrated in three directions: a fault-injectable reference
  subject (`odradek_acceptance::subject`) proves each check detects exactly
  the violation it claims to (sensitivity) and that a conformant subject
  trips nothing (specificity); `odradek-client` itself passes the client
  checks (dogfooding); and real brokers are ground truth — `cargo xtask
  conformance` runs the suite against Apache Kafka and Redpanda in Docker
  and enforces the per-implementation baselines committed in
  [`conformance/`](conformance/). Nothing fails today, and the baselines
  already record real behavioral differences: Redpanda 25.2 advertises
  Metadata only to v8, Produce to v7, and Fetch to v11 — so the flexible
  metadata header and topic-id-addressed produce/fetch checks (which
  Kafka 4.1 passes) skip there, on record. The fault ↔ check registry is
  enforced by test: a check no fault can trip, or a fault no check
  detects, fails calibration.

Next: producer/consumer machinery on the client's cluster layer,
harness fault modes (NOT_LEADER redirects to observe client retries),
materialize known tagged fields in codegen, and vendor more schemas
(consumer groups, offsets).

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
