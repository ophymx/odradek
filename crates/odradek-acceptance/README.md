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
# ... one that will not answer an anonymous caller
odradek-accept --server localhost:9092 --authenticate user:password
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
`groups/*`, `consumer-group/*` (KIP-848), `sasl/*`, `txn/*`, `admin/*`,
and `cluster/*` — everything a producer or consumer needs, plus the
group protocols, the SASL handshake sequence, transactions, topic
deletion, and what the brokers of a cluster must agree on.

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
- A compressed batch comes back exactly as it was produced, for each of
  gzip, snappy, lz4 and zstd. The record set is the producer's bytes and
  a broker storing a topic at the default `compression.type=producer`
  has no business in them — recompressing, even to the same codec,
  rewrites the batch and breaks every consumer that verified the crc it
  was given, which includes anything proxying or mirroring the log. The
  give-away is not an error: the records decode fine, they are simply
  not the bytes anybody wrote.

  One codec per check, rather than one check sweeping all four, because
  which codecs an implementation accepts is exactly the sort of
  difference the baselines exist to record — an aggregate would hide a
  missing one behind three working ones.

  These also pin the *framing*, which is where the interoperability
  traps live. Kafka's snappy is not the snappy project's framing format
  but the one Java's `SnappyOutputStream` happened to use, and lz4 is
  the frame format rather than a raw block. A producer that reaches for
  its snappy library's stream encoder writes something no Kafka consumer
  can read: same codec name, different bytes. That the check tests this
  and not merely passthrough was worth establishing rather than
  assuming — framed the wrong way, a batch is refused outright (Apache
  Kafka 4.1 answers `UNKNOWN_SERVER_ERROR`), so the brokers really do
  look inside, and a suite that framed it wrongly would find out rather
  than quietly pass its own bytes back and forth.
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
- `acks=0` means *no response at all* — not an empty one, and not one
  the client can ignore. A broker that answers puts a frame on the wire
  nobody has a correlation id outstanding for, so the client reads it
  as the reply to its next request and every reply after that is
  matched to the wrong one. Silent, and it corrupts everything
  downstream rather than failing.
- A topic id does not change under a topic that never went away. Ids
  exist so a client can tell a recreated topic from the one it meant;
  reminting one fails every id-addressed request already in flight
  with `UNKNOWN_TOPIC_ID`, which reads as "that topic is gone".
- A fenced producer cannot *write*. Refusing its bookkeeping calls at
  the coordinator is only inconvenient for a zombie; the partition
  leader is different code and the one that matters, because records it
  accepts land inside a transaction the live producer is about to
  commit. The successor then commits work it never did, leaving no
  trace anywhere.
- A committed transaction becomes readable and is *not* named in the
  aborted list. Two quiet ways to get it wrong: a stable offset that
  never moves past the records leaves a consumer blocked on a
  transaction that finished, and naming a committed producer as aborted
  has every client throw its records away on purpose.
- Offsets committed inside a transaction are held back until it
  commits. This is exactly-once from the broker's side — a broker that
  publishes them immediately has the input marked processed while the
  output can still be thrown away, which is the duplicate-work window
  transactions exist to close, and it closes silently because every
  request succeeds.
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

`client/tolerates-unknown-tagged-fields` checks the other direction of
the same promise the protocol crate makes internally: a response
arrives carrying a tag no schema defines — the way one from a newer
broker does — and the client is expected to keep it and carry on. One
that refuses instead breaks against every broker newer than itself, and
breaks on upgrade day in somebody's cluster rather than in a test suite.

`client/honours-throttle-time` is the one that has been wrong twice,
in both directions. It found a gap in this crate's own client, which
read `throttle_time_ms` from nothing at all — Quota
enforcement is not advice a client can decline: the broker answers,
sets the field, and then stops reading that connection for that long,
so a client that ignores it does not get its next request in sooner —
it gets it in later, sitting in a socket buffer while its own request
timeout runs down. The failure looks like an unreliable broker from
the inside, which is why nobody goes looking for it in the client.

Then Apache's own Java client failed it, and the check was what was
wrong. It asked whether the *next* request waited, which assumes a
client that speaks in turns; a pipelining client has requests on the
wire before it reads the answer, and Kafka's producer had an
InitProducerId out 2ms later. That is not a violation, it crossed in
flight. The check now looks at the *middle* of the pause instead —
the front belongs to requests already sent, the tail to a client
resuming a rounding-error early, and what nobody has an excuse for is
still talking in between. Pointing a foreign client at the harness is
the only thing that could have shown this: every client check here was
written, calibrated, and dogfooded by the same author against the same
client, which is a closed loop no amount of care escapes.

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

Five subjects: Apache Kafka 4.1 and Redpanda 25.2, each as a single
broker and again as a three-node cluster, plus Redpanda with SASL
required on every connection.

The matrix overlaps by *brokers* rather than by subjects. Eight at once
on a two-core runner is enough load to make a broker take seconds over
work it usually does in milliseconds, and every wait in the suite is
for work a broker does asynchronously — so an oversubscribed machine
produces failures that blame the subject for the harness's choice of
how much to run at the same time.

Nothing fails today. The differences are capability, version and
topology ones, recorded rather than scored:

- The seven `cluster/*` checks skip on both single-broker subjects and
  pass on both clusters. That is a property of the deployment, not of
  the implementation: there is nothing for one broker to agree with,
  and nothing to fail over to.
- Redpanda 25.2 advertises Metadata only to v8, Produce to v7, and
  Fetch to v11, so the flexible-metadata-header and topic-id-addressed
  produce/fetch checks skip there.
- Redpanda 25.2 advertises Metadata only to v8, which is below where
  topic ids reach the topics array, so the id-stability check skips
  there too.
- Redpanda 25.2 does not implement KIP-848 at all: those four checks
  skip, which is the one place the two implementations genuinely part
  company rather than agreeing.
- Mechanism negotiation cannot be observed from a listener with no SASL
  configured: every SASL request there is `ILLEGAL_SASL_STATE`, which is
  correct rather than nonconformant. Kafka scopes SASL to a listener, so
  that subject gets a second one and `--sasl-server` points at it while
  everything else uses the plaintext one. Redpanda switches SASL on for
  the whole cluster, so there is no plaintext listener to fall back to —
  which is why it gets a subject of its own where the suite
  authenticates *every* connection, and where the `sasl/*` checks point
  at the same listener as everything else.

  That subject is the only one that exercises the suite against a broker
  which will not answer an anonymous caller at all — which is how
  brokers are actually run, and which the suite could not do before.

## The `cluster/*` checks

Every other check can be asked of a single broker. These cannot. On one
node, every partition's leader and every group's coordinator *is* the
broker you are already connected to, so "do all the brokers agree" and
"does the wrong broker refuse" have no content — and a client that never
consults Metadata is indistinguishable from one that does. Against a
single-broker subject they skip and say so; against a cluster they ask:

- every broker names the same leader for a partition, and the same
  coordinator for a group — disagreement gives two producers two places
  to write, or splits a group's offsets across brokers by which one each
  member happened to ask;
- a topic asked for n replicas lands on n *distinct* brokers, the leader
  among them, the in-sync set drawn from them;
- a broker that does not lead a partition refuses the write rather than
  appending to a log the leader knows nothing about;
- a broker that does not coordinate a group refuses the offset commit
  rather than storing it where the coordinator will never read it.

Those do not kill a broker. They are about a cluster's answers being
consistent while everything works, which is the precondition for
anything about failure meaning something. Two more then take that step:

- leadership moves off a stopped broker to one of its replicas, and the
  new leader accepts writes — otherwise the replicas were decoration;
- a group gets a new coordinator when its own stops, still reporting the
  offset it acknowledged. A commit is only as durable as whatever
  answers after the failure; if the offsets die with the broker, a
  consumer resuming after an outage reprocesses everything since, and
  finds out on the worst day.

Stopping a broker is not something the protocol can express, so the
suite does not guess how. It takes `--cluster-control <cmd>` and runs
`<cmd> <stop|start> <node-id> <host:port>`, leaving `docker stop`,
`kubectl delete pod` or an ssh hop to whoever knows the deployment —
the same bargain `--sasl-server` strikes. Without one, these checks
skip. Both names are passed because not every implementation lets you
choose node ids: Kafka takes a configured `node.id`, Redpanda assigns
its own.

They are the only checks that change the subject rather than observe
it, so each restores the cluster on every path out, including the paths
where it has already decided the subject is wrong. A check that left a
broker down would hand its failure to whatever ran next, and the report
would blame the wrong thing.

Adding them was not the expensive part. Pointing the existing suite at
three brokers was: it failed 31 of 51 checks, and failed different ones
each run, because it had assumed throughout that the broker it
bootstrapped from led every partition and coordinated every group. On a
cluster of one that is true by construction, which is how the assumption
survived 51 checks and two implementations. The harness routes now — to
the partition leader for writes, to the group's coordinator for group
calls, to the transaction coordinator for transactions, re-reading
Metadata when a broker says it is the wrong one to ask.

The reference subject is a three-node cluster for the same reason: a
fault like "every broker names itself the leader" cannot be injected
into a subject that has only one broker to name. Its brokers are tasks
rather than containers, so its `ClusterControl` stops them in-process —
and the checks cannot tell, which is the point of taking one rather
than assuming how a broker is stopped.

Adding Redpanda as a second cluster found two more suite bugs
immediately, both of the same family as the first. Topic deletion
belongs to the *controller*: Kafka forwards an admin request sent
elsewhere, Redpanda answers `NOT_CONTROLLER` and expects the client to
consult Metadata, which names the controller in every response. Both
are within their rights, and the suite had been relying on the first.
And a coordinator that has just taken a group over has to load its
state before it can answer about it — until then the group's partitions
are absent from its reply, which the suite read as offsets lost rather
than as a wait.

## The proxied pass

`cargo xtask conformance` runs every subject twice: once against the
broker, and once with
[`odradek-protocol`'s proxy example](../odradek-protocol/examples/proxy.rs)
between the suite and the broker. Both passes are enforced against the
same baseline — the proxied one gets no baseline of its own, because the
claim being tested is precisely that there is nothing to record: a proxy
built on the protocol crate should be indistinguishable from the broker
it stands in front of.

That is where the crate's design claims stop being assertions. Unknown
tagged fields round-trip raw, record batches re-encode byte-identically,
response header versions are recoverable from the request — each is
justified in the source with "a proxy needs this", and a proxy that got
any of them wrong would make some check answer differently than the
broker behind it. The diff then names which one.

It earned its place immediately. Its first run reported four group
checks erroring through the proxy that had passed directly, and the
proxy turned out to be innocent: the suite was reusing fixed group names
across runs, so a second run against a live cluster parked behind a
rebalance waiting for the first run's members. Throwaway containers had
hidden it from CI, and anyone pointing the suite twice at their own
cluster would have hit it. Group names are stamped per run now.

Use `--no-proxy` to skip the second pass.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
