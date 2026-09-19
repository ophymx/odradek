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

The fault registry is exhaustive in both directions, enforced by test:
a check no fault can trip is unproven, a fault no check detects is dead
weight, and either fails the build. This is the property that makes the
catalog worth anything — a check that cannot fail passes everywhere.

## What is covered

`api-versions/*`, `metadata/*`, `produce/*`, `fetch/*`,
`list-offsets/*`, `find-coordinator/*`, `offsets/*`, `create-topics/*`,
`groups/*`, `consumer-group/*` (KIP-848), and `sasl/*` — everything a
consumer needs, plus the group protocols and the SASL handshake
sequence.

Success paths are the easy half. The checks that earn their keep are
the ones where the *wrong* answer is plausible:

- A fetch past the high watermark must answer `OFFSET_OUT_OF_RANGE`,
  not the empty batch set a caught-up consumer sees — the wrong answer
  is silent, and leaves a client polling a position that will never
  exist.
- An unknown topic must be *named* in the Metadata response carrying
  `UNKNOWN_TOPIC_OR_PARTITION`, not omitted: omitted, a client cannot
  tell "no such topic" from "you ignored my question".
- A partition a group never committed reads as offset `-1`, not `0` —
  `0` is a valid offset, so the wrong answer sends a resuming consumer
  back to the start of the log.
- `validate_only` must answer without creating, checked by creating for
  real afterwards and watching for the give-away.
- A SASL token arriving before any mechanism was negotiated is
  `ILLEGAL_SASL_STATE`: "your sequence is wrong", not "your credentials
  are wrong". A client told the latter retries the same broken sequence
  forever.
- A SCRAM server nonce must *extend* the client's, not replace it. The
  client's nonce is its only evidence that an answer is not a recording
  of an older exchange.
- The iteration count a SCRAM server states is one the client must spend
  before it learns anything, and cannot refuse without failing to
  connect, so it has a floor (RFC 7677: 4096).
- A SCRAM exchange ends with a server signature, or the client has
  authenticated itself to whatever answered the socket and cannot tell.

The `client/*` checks run the same idea from the other side: the
harness plays broker, and the fault it injects is a refusal shaped like
a success. RFC 7628 has an OAUTHBEARER server report a bad token in a
*successful* response carrying `{"status":"invalid_token"}`, so a
client that reads only the error code sees zero, believes it
authenticated, and sends data on a connection nobody authorized. This
crate's own client shipped exactly that bug and a live broker caught
it; the check is here so the next one is caught in CI. Its twin
watches for the lone `\x01` the RFC has the client send back, without
which the broker is left mid-exchange and reports a timeout instead of
the reason it already knows.

Five checks sweep **every version the subject advertises** rather than
negotiating one and stopping. Against Kafka 4.1 that is 13 Metadata
exchanges, 12 fetches of a single produced batch, and 7
FindCoordinator versions straddling the v4 shape change. A fault that
misbehaves only at the lowest version keeps it honest: it is
undetectable by a suite that negotiates once, so calibration fails if a
sweep is ever removed.

The group and KIP-848 checks came out of *implementing* those protocols
in the reference subject rather than out of reading the schemas, which
is where the underspecified parts surface. A join carrying no member id
must be refused with `MEMBER_ID_REQUIRED` *and* handed an id to rejoin
with. The assignment bytes a leader supplies must reach their member
unexamined — the same opacity the record-batch codec promises. In
KIP-848, an absent assignment means "unchanged" while an empty one
means "revoked", which is how a steady-state heartbeat is told apart
from an unsubscribe.

## What the baselines record

Nothing fails today. The differences are capability and version ones,
recorded rather than scored:

- Redpanda 25.2 advertises Metadata only to v8, Produce to v7, and
  Fetch to v11, so the flexible-metadata-header and topic-id-addressed
  produce/fetch checks skip there.
- Redpanda 25.2 does not implement KIP-848 at all: those four checks
  skip, which is the one place the two implementations genuinely part
  company rather than agreeing.
- Mechanism negotiation cannot be observed from a listener with no SASL
  configured: every SASL request there is `ILLEGAL_SASL_STATE`, which is
  correct rather than nonconformant. The harness therefore gives Kafka a
  second, SASL-configured listener and passes it with `--sasl-server`.
  Redpanda deliberately gets none: its SASL is switched on cluster-wide,
  and once it is, the listener configured for no authentication starts
  refusing anonymous callers, taking 13 unrelated checks down with it.
  Kafka scopes SASL per listener and has no such coupling. So that check
  passes on Kafka and skips on Redpanda, and the skip reason says which
  of the two situations it is.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
