# odradek-client

Async, Rust-native Kafka client built on
[`odradek-protocol`](https://crates.io/crates/odradek-protocol) and
tokio. No C bindings anywhere in the tree: TLS is rustls, crypto is
RustCrypto, codecs are pure Rust (zstd via the libzstd binding is the
one exception).

- **Admin**: list the cluster's consumer groups, describe them down to
  members and assignments, read a topic's or broker's configuration,
  and delete topics. Each routes where the protocol says it must — a
  listing asks *every* broker, since each answers only for the groups
  it coordinates; a description asks each group's coordinator; a
  deletion goes to the controller and retries when that moves.
- **Idempotent produce** (on by default): the producer takes an id
  from the broker and numbers every batch per partition, so a produce
  whose *acknowledgement* was lost gets retried without being appended
  twice. A retry carries the same sequence — that is the whole
  mechanism — and a failed batch rewinds rather than leaving a gap the
  broker would reject everything after. It requires `acks = -1`, and a
  configuration that asks for both idempotence and weaker acks is
  refused rather than silently given neither.
- **Transactions**: a set of writes across partitions that a
  `read_committed` reader sees all of or none of, plus
  `send_offsets_to_transaction` to commit consumed positions in the
  same transaction as the output they produced — the exactly-once
  consume-transform-produce loop. `init_transactions` fences whoever
  last held the transactional id and rolls back what they left open,
  which is what makes a crashed producer recoverable by its
  successor. A failure inside a transaction moves the producer to
  `TransactionState::Abortable` rather than letting it commit
  something it cannot guarantee.
- **Read isolation**: `IsolationLevel::ReadCommitted` filters out the
  records of aborted transactions, which the broker still returns —
  applying the abort list is the client's job, and a client that skips
  it looks correct until someone aborts.
- **Connections**: framed, correlation-id pipelined, ApiVersions
  negotiation with the `UNSUPPORTED_VERSION` downgrade path; plaintext
  or TLS (Mozilla roots, custom CA, or caller-built config), with
  optional mutual TLS — a client certificate and key for a cluster
  running `ssl.client.auth=required`; optional SASL — PLAIN and
  SCRAM-SHA-256/512 with server-signature verification — authenticated
  on every connection; client-side
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

Group membership and reading are separate halves: `GroupMember` (or
`ConsumerGroupMember`, for KIP-848) decides *which* partitions are
yours, and `Consumer` reads one. The loop between them is yours to
write, because a poll loop encodes application decisions — when to
commit, what a failed record does, whether a rebalance discards
in-flight work — that a library would have to guess at. It is about
forty lines, and
[`examples/group_consume.rs`](examples/group_consume.rs) is those
forty lines, assembled and running against a real broker:
join, resume from the group's committed offsets, read, commit,
rebalance. [`examples/group848_consume.rs`](examples/group848_consume.rs)
is the same loop on the newer protocol, with a header on what changes.

[`examples/transactions.rs`](examples/transactions.rs) does the same
for exactly-once: it aborts a transaction and shows the records still
in the log under `read_uncommitted` and gone under `read_committed`,
commits across two partitions, then runs a consume-transform-produce
loop that commits its input position inside its output transaction.

See [`examples/`](examples/) for those, plus produce/consume,
group-membership mechanics on their own, and TLS/SASL smoke tests.

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

## What is tested against a real broker

`cargo xtask conformance` runs the examples in `examples/` — unmodified,
as a caller would — against every subject in the workspace's broker
matrix: Apache Kafka 4.1 and 3.7, Redpanda 25.2, each as one broker and
as a three-node cluster, plus TLS and SCRAM. Produce and consume in all
four codecs, admin, classic consumer groups, KIP-848, and transactions
with `read_committed`, per subject.

That this is written in the present tense is recent. The examples were
the validation every feature here shipped on, and they were run by hand;
nothing ran them in CI, so the whole list above rested on somebody
remembering. The first automated run found that this client could not
create a topic against a multi-node Redpanda at all — it read
`NOT_CONTROLLER` as a refusal rather than as the redirect it is, which
Kafka hides by forwarding controller work internally and Redpanda does
not. Every example failed on its first line against a cluster nobody had
pointed it at.

The matrix also records what a subject cannot do: KIP-848 is
`unsupported` on Kafka 3.7 (advertised, answered `UNSUPPORTED_VERSION`)
and on Redpanda 25.2 (not advertised at all), and both are checked in
that direction too — a scenario that starts working is reported as
loudly as one that stops.

What it does not reach yet, so that the list above is not read as more
than it is: of the SASL mechanisms only SCRAM-SHA-256 meets a real
broker here, because that is the credential the authenticating subject
is provisioned with — PLAIN and SCRAM-SHA-512 are covered against the
RFC vectors in `odradek-sasl` and by hand, not by this. Mutual TLS is
the same: verified live against a cluster requiring client certificates,
once, by hand; no subject in the matrix asks for one.

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
