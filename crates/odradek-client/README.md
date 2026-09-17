# odradek-client

Async, Rust-native Kafka client built on
[`odradek-protocol`](https://crates.io/crates/odradek-protocol) and
tokio. No C bindings anywhere in the tree: TLS is rustls, crypto is
RustCrypto, codecs are pure Rust (zstd via the libzstd binding is the
one exception).

- **Connections**: framed, correlation-id pipelined, ApiVersions
  negotiation with the `UNSUPPORTED_VERSION` downgrade path; plaintext
  or TLS (Mozilla roots, custom CA, or caller-built config); optional
  SASL — PLAIN and SCRAM-SHA-256/512 with server-signature
  verification — authenticated on every connection.
- **Cluster layer**: metadata discovery, per-broker connection pool and
  version ranges, partition-leader routing, coordinator discovery.
- **Producer**: per-partition batching (size-triggered or explicit
  flush), gzip / lz4 / snappy / zstd compression, retries through
  leadership changes.
- **Consumer**: fetch with decompression and absolute offsets,
  earliest/latest lookup, committed offsets under a group id, and
  classic consumer-group membership (join/sync/heartbeat/leave with
  Kafka-compatible range assignment).

```rust
use odradek_client::{ClientConfig, Cluster, Producer};

let mut config = ClientConfig::default();
config.bootstrap_servers = vec!["localhost:9092".into()];
config.client_id = "my-service".into();
let cluster = Cluster::connect(config).await?;
let mut producer = Producer::new(cluster);
```

See [`examples/`](examples/) for produce/consume, consumer groups, and
TLS/SASL smoke tests against a real broker.

## Features

All on by default: `tls`, `sasl`, `gzip`, `lz4`, `snappy`, `zstd`.
Opting out slims the build: `--no-default-features` is a plaintext,
unauthenticated client whose only codec is uncompressed (a fetched
batch in a disabled codec is a typed runtime error, not a crash).
Skipping `zstd` drops the one C dependency — `tls,sasl,gzip,lz4,snappy`
is an all-Rust build.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
