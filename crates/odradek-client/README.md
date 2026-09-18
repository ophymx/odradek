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
  verification — authenticated on every connection; client-side
  connect and per-request timeouts, so a hung broker cannot hang the
  caller.
- **Cluster layer**: metadata discovery, per-broker connection pool and
  version ranges, partition-leader routing, coordinator discovery, a
  control-plane connection that fails over across bootstrap servers
  and known brokers, and topic creation (`Cluster::create_topic`).
- **Producer**: per-partition batching (size-triggered or explicit
  flush), keyed produce with Kafka's default partitioner (murmur2,
  Java-compatible) and round-robin for keyless records, gzip / lz4 /
  snappy / zstd compression, retries through leadership changes.
- **Consumer**: fetch with decompression and absolute offsets,
  earliest/latest lookup, committed offsets under a group id, and
  classic consumer-group membership (join/sync/heartbeat/leave with
  Kafka-compatible range assignment); group members commit offsets
  under their live generation, so the coordinator fences zombies.

`Cluster` is a cheap clonable handle: clone it per component and they
share one authenticated connection pool and metadata cache.

```rust
use odradek_client::{ClientConfig, Cluster, Consumer, Producer};

let mut config = ClientConfig::default();
config.bootstrap_servers = vec!["localhost:9092".into()];
config.client_id = "my-service".into();
let cluster = Cluster::connect(config).await?;

let mut producer = Producer::new(cluster.clone());
let consumer = Consumer::new(cluster);   // shares the producer's pool
```

See [`examples/`](examples/) for produce/consume, consumer groups, and
TLS/SASL smoke tests against a real broker.

## Security defaults

**Connections are plaintext TCP unless configured otherwise.**
`ClientConfig::default()` sets no transport encryption, which is fine
for a loopback broker and wrong for any network you do not own; build
with the `tls` feature and set `ClientConfig::tls`.

Because that default is unsafe for credentials, SASL PLAIN over an
unencrypted connection is refused before the handshake starts
(`ClientError::InsecureCredentials`) unless you opt in with
`ClientConfig::allow_plaintext_credentials`. SCRAM is permitted over
plaintext — it never sends the password — but an observer still
collects the username, salt, iteration count, and client proof, which
is everything an offline dictionary attack needs.

Work a broker can make this client do is bounded on both paths where it
is worth doing: SCRAM iteration counts are clamped to
`[4096, ClientConfig::scram_max_iterations]` and derived on a blocking
thread, and one `fetch()` materializes at most
`ConsumerConfig::max_fetch_records` records over at most 64 MiB of
decompressed record bytes.

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
