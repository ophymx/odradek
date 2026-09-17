# odradek-acceptance

Acceptance suite for the Kafka *protocol* — not any one implementation.

Kafka has outgrown the Apache broker: Redpanda, WarpStream, Bufstream,
and a growing set of proxies and clients all speak the same wire format.
This suite validates **either side** of that conversation: run it
against a server (the suite acts as a client) or against a client (the
suite impersonates a multi-broker cluster and can stage faults like
leadership moves).

```sh
# every catalogued check: id, subject role, requirement
odradek-accept --list
# validate a server
odradek-accept --server localhost:9092 [--json]
# validate a client: listen, point its bootstrap here
odradek-accept --client-listen 127.0.0.1:19092
# record / enforce expected results per implementation
odradek-accept --server ... --write-baseline conformance/kafka.json
odradek-accept --server ... --baseline conformance/kafka.json
```

Checks are data: every check is an entry in a static catalog carrying a
stable id, the requirement it verifies, and its subject role, so
conformance reports are citable and diffable across implementations —
`--list` prints the whole catalog. Baselines make it a **regression**
tool — differences between implementations are recorded honestly, not
scored.

Reports and baselines are JSON with a versioned envelope —
`{"format": 1, "suite": "<version>", ...}` — so stored files stay
interpretable as the suite evolves, and reports round-trip (a stored
run can be re-parsed and re-diffed later). Baseline comparison keeps
three cases apart: a regression (a check's outcome changed), a new
check the baseline predates (re-record to adopt it), and a baseline
entry for a check that no longer exists.

A check has four possible outcomes: `pass`, `fail` (the subject
violated the requirement), `skipped` (the check does not apply), and
`error` — the check *could not run* (connection refused, timeout,
harness setup failure). `error` is an infrastructure finding, kept
strictly apart from `fail`: it never satisfies a baseline and never
passes a run, but a flaky broker is reported as infrastructure trouble,
not nonconformance.

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
