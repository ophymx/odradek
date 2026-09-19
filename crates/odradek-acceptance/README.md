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
`groups/*`, `consumer-group/*` (KIP-848), `sasl/*`, `txn/*`, and
`admin/*` — everything a producer or consumer needs, plus the group
protocols, the SASL handshake sequence, transactions, and topic
deletion.

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
- A batch sent twice under one producer id, epoch and sequence is
  stored *once*. This is the whole of idempotent produce: the producer
  cannot tell a lost request from a lost acknowledgement, so it
  retries, and a broker that appends the retry leaves the log holding
  the records twice with nothing downstream able to say which
  duplicates were meant.
- A stamped sequence that skips ahead is refused with
  `OUT_OF_ORDER_SEQUENCE_NUMBER`. The broker cannot tell "the batch you
  skipped never existed" from "it is still in flight and will arrive
  out of order", so accepting the gap abandons the ordering the
  producer was promised without saying so.
- Seeking by time lands on the first record at or after the timestamp,
  and answers `-1` for a time after the last one. Both wrong answers
  are quiet: the log end reads as "you are caught up", the log start as
  "read everything again".
- A topic reported deleted stops existing — the mirror of
  `validate_only`, checked by asking again rather than by trusting the
  acknowledgement.
- A replication factor the cluster cannot satisfy is *refused*. The
  plausible wrong answer here is not an error but a success: creating
  the topic with however many replicas are available, so a caller who
  asked for a durability level is told it got one and finds out
  otherwise during the broker failure the topic was meant to survive.
- Commit metadata comes back exactly as it was given. The string is the
  client's — processing state, a schema version, a shard id — and the
  offset beside it reads back fine either way, so a broker that drops
  it loses something nothing looks wrong about.
- A fetch waits out `max_wait_ms` for data that is not there, and does
  not spend it on data that is. Both halves fail quietly: a broker that
  answers an empty long poll immediately turns every caught-up consumer
  into a busy loop — correct data, burned CPU, no error anywhere — and
  one that sits on a fetch it could already answer adds its whole wait
  to the latency of every record.
- Every partition leader is a broker the same response names. A leader
  id a client cannot resolve leaves it with nowhere to send and no
  error to explain it: a healthy-looking topic it simply cannot write
  to.
- A live group is described with the members it has. That is the only
  view an operator or an admin client gets of who holds what, and a
  group that reads as empty reads as safe to delete.
- A member that has left stops being one. A coordinator that keeps
  honouring a departed member's heartbeats believes it still owns its
  partitions, so they are never reassigned and simply go unread, with
  every request involved succeeding.
- Re-taking a transactional id must hand out a *higher* epoch, and the
  superseded one must then be refused. Either half alone is worthless:
  an epoch nobody enforces fences nothing, and enforcement without a
  bump fences the wrong producer.
- While a transaction is open the last stable offset stays below the
  high watermark. A broker that lets them meet shows `read_committed`
  consumers records that may still be aborted — the one thing they
  asked not to see.
- An aborted transaction is *named* in a `read_committed` fetch over
  its records. The records are returned either way, because aborting
  does not unwrite anything; the list is the only thing that tells a
  client which of them to drop.

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

The group, KIP-848 and transaction checks came out of *implementing*
those protocols in the reference subject rather than out of reading the
schemas, which is where the underspecified parts surface. A join carrying no member id
must be refused with `MEMBER_ID_REQUIRED` *and* handed an id to rejoin
with. The assignment bytes a leader supplies must reach their member
unexamined — the same opacity the record-batch codec promises. In
KIP-848, an absent assignment means "unchanged" while an empty one
means "revoked", which is how a steady-state heartbeat is told apart
from an unsubscribe.

`groups/leave-unregisters-the-member` caught the reference subject
reading only LeaveGroup's pre-v3 `member_id` field — v3 moved the
departing member into a `members` array and the old field left the
wire, so every leave was acknowledged and ignored. That is the shape of
bug these checks exist for, and it was in the suite's own code.

Transactions surfaced two more. A client must ask FindCoordinator for
its transactional id before InitProducerId — obvious in a multi-broker
cluster, but it is also what makes brokers materialize their
transaction log, and Redpanda 25.2 answers InitProducerId with
*silence* until it has. And a transactional write to a partition the
client never announced is not refused by Kafka 4.1: the partition
leader verifies membership with the coordinator and adds what is
missing, so the records are in the transaction after all. The check
was written expecting a refusal, the broker said otherwise, and the
requirement it now states is the one that actually holds — the records
must not end up outside the transaction, by either route.

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
