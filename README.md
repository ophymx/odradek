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
  header-version selection including the ApiVersions response-header quirk.
  Known limitation: tagged fields are not yet materialized as struct fields —
  they round-trip losslessly through `unknown_tagged_fields`.
- `odradek-client`: framed connection with correlation-id pipelining and
  ApiVersions negotiation (including the `UNSUPPORTED_VERSION` downgrade
  path), tested against an in-process fake broker.
- `odradek-acceptance`: conformance checks for **both roles** over raw
  connections independent of the client crate — `api-versions/*` server
  checks and `client/*` client checks — with JSON reports and per-
  implementation baselines:

  ```sh
  # validate a server
  odradek-accept --server localhost:9092 [--json]
  # validate a client: listen, point its bootstrap here
  odradek-accept --client-listen 127.0.0.1:19092
  # record / enforce expected results per implementation
  odradek-accept --server ... --write-baseline conformance/kafka.json
  odradek-accept --server ... --baseline conformance/kafka.json
  ```

  The suite is calibrated in both directions: a fault-injectable reference
  subject (`odradek_acceptance::subject`) proves each check detects exactly
  the violation it claims to (sensitivity) and that a conformant subject
  trips nothing (specificity); `odradek-client` itself passes the client
  checks (dogfooding). Still missing: runs against real brokers (needs a
  container runtime or JVM) to calibrate against ground truth.

Next: run the suite against Apache Kafka/Redpanda/etc. and commit
per-implementation baselines, materialize known tagged fields in codegen,
vendor more schemas (consumer groups, offsets), the client's
cluster/metadata layer, and record batch encoding.

```sh
cargo test --workspace       # everything
cargo xtask codegen          # regenerate message types from schemas
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
