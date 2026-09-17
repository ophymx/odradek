# odradek-acceptance

Acceptance suite for the Kafka *protocol* — not any one implementation.

Kafka has outgrown the Apache broker: Redpanda, WarpStream, Bufstream,
and a growing set of proxies and clients all speak the same wire format.
This suite validates **either side** of that conversation: run it
against a server (the suite acts as a client) or against a client (the
suite impersonates a multi-broker cluster and can stage faults like
leadership moves).

```sh
# validate a server
odradek-accept --server localhost:9092 [--json]
# validate a client: listen, point its bootstrap here
odradek-accept --client-listen 127.0.0.1:19092
# record / enforce expected results per implementation
odradek-accept --server ... --write-baseline conformance/kafka.json
odradek-accept --server ... --baseline conformance/kafka.json
```

Checks are data: each carries a stable id and the requirement it
verifies, so conformance reports are citable and diffable across
implementations. Baselines make it a **regression** tool — differences
between implementations are recorded honestly, not scored.

The suite is calibrated in three directions: a fault-injectable
reference subject proves each check detects exactly the violation it
claims (and that a conformant subject trips nothing); the
[odradek client](https://crates.io/crates/odradek-client) passes the
client checks; and real brokers are ground truth — the workspace CI
runs the suite against Apache Kafka and Redpanda in Docker on every
push.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
