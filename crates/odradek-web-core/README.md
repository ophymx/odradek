# odradek-web-core

Transport-agnostic bridge from Kafka partitions to web-shaped
subscribers.

One pump per (topic, partition) fans out to any number of subscribers,
each starting from earliest, latest, or after a specific offset, with
per-subscriber filters (key prefix, header match). Fan-out is by shared
handle: an event is read once, rendered as JSON at most once, and
delivered to every subscriber as a refcount bump. Backpressure is
self-healing: a slow subscriber falls out of the live path into
catch-up — served from an in-memory ring, or from Kafka past it — and
rejoins as it drains. Offset order, no gaps, no duplicates, at any
subscriber speed.

Catch-up past the ring is scheduled, at most one fetch per pump
iteration, taking turns: laggards cannot put an unbounded queue of
broker round trips in front of the live path, at the cost of replaying
more slowly the more of them there are.

This crate is the engine; transports are thin layers on top:

- [`odradek-web-sse`](https://crates.io/crates/odradek-web-sse) —
  Server-Sent Events with `Last-Event-ID` resume
- [`odradek-web-ws`](https://crates.io/crates/odradek-web-ws) —
  WebSocket JSON frames

The source is pluggable through the `RecordSource` / `SourceFactory`
traits: Kafka via
[`odradek-client`](https://crates.io/crates/odradek-client) in
production, and an in-memory log (published as
`odradek_web_core::memory`) for deterministic downstream tests.

Positions are exclusive: every one names a record already seen, and
asks for what follows. A resume token is therefore the offset a client
last received, echoed back unchanged, and the engine never computes one
position from another — it says "after the event I just delivered" and
hands back whatever the source gave it. A source is free to number its
records sparsely, or in strides, or with gaps; only an adapter over a
dense log like Kafka's does the `+ 1`, where the numbering is a known
fact about the store rather than a guess about positions in general.

Source errors are typed (`SourceErrorKind`: not-found, auth,
unavailable, other): permanent failures stop a pump at once, transient
ones retry with backoff, and either way subscribers receive one final
`StreamError` explaining why before their stream closes. Lifecycle is
managed: dead subscribers are noticed even on quiet topics, a pump with
no subscribers exits after `PumpConfig::idle_shutdown` (default 30s,
respawned on the next subscribe), pumps that have exited are evicted
from the hub's map rather than retained, and `Hub::shutdown` stops
everything cleanly.

## Security

This engine sits behind transports that are reachable by whoever can
reach the port. What it guarantees, and what is yours to configure:

**The topic gate.** A `Hub` serves *nothing* until you say what it is
for — `Hub::with_topic_gate(|topic| ...)` to name the topics, or
`Hub::allow_all_topics()` to say you mean all of them. Deny is the
default because the other default is every topic on the cluster:
`__consumer_offsets` included, every topic created after the code was
written included, plus topic enumeration (a name that exists answers
differently from one that does not). The gate is a property of the
*process* — "this bridge serves these topics". Per-user rules ("this
reader may see topic X") belong in a middleware layer over the
transport's router, which can see the whole request; the gate is the
floor under it.

**What an anonymous request can allocate.** The pump map is keyed by
(topic, partition), both from the request path, so its growth is the
exposure. Three bounds, each covering a dimension the others cannot:

1. the gate, which refuses before any source call at all — but it
   only bounds the topic, and the partition index is a free `i32`;
2. an existence check, before anything is inserted: a subscribe to a
   topic or partition the source does not have is refused
   (`404`-shaped `NotFound`) without creating an entry, spawning a
   task, or making a broker round trip beyond the one cached metadata
   lookup per topic;
3. `Hub::with_max_pumps` (1024 by default), which bounds what is left
   — real partitions of allowed topics. Entries whose pump has exited
   are evicted before the limit is applied, so a dead pump never holds
   a slot.

**Memory.** Budget `max_pumps x ring_capacity x <source's max fetch
bytes>` for the worst case. The ring is bounded by event *count*, not
bytes, and an event can pin the whole fetch buffer it was read from, so
with Kafka's `partition_max_bytes` at 1 MiB a single pump's ring of
1024 is a megabyte-scale number, not a kilobyte-scale one. Lower
`ring_capacity`, `max_pumps`, or both for a tighter ceiling. Note also
that the ceiling is on *pumps*, not connections: limit concurrent
connections in your server (a `tower` concurrency layer, or your
proxy), because each one holds a queue of up to `queue_capacity`
events.

**Error text.** `SourceError::message` is the upstream's own words — on
a real cluster that means bootstrap hostnames and ports, leader and ACL
state, TLS and SASL detail. It is for `tracing`. What a client is told
is the classification (`SourceErrorKind` / `RejectionKind`) plus a
fixed sentence, which is what `Rejection` and `StreamError::public_message`
produce.

## Non-goals

One rule decides most of what belongs here, and it is worth stating
before the arguments rather than after:

> **The engine never decodes a record, never sees a request, and never
> keeps state a client could keep instead.**

Each clause refuses a different category, and each refusal has
somewhere better to go.

**Never decodes.** A record's value is opaque `Bytes` from `fetch` to
frame. So: no JSONPath or SQL-ish filters, no schema registry, no
Avro/Protobuf decoding, no field projection, no format conversion.
`Filter` compares bytes — key prefix, header value — because it runs on
the pump loop once per event per subscriber, and anything on that loop
must be O(1) in record size. The line is *no decoding*, not "do not
touch the value". Decode in the subscriber, where the cost is yours.

**Never sees a request.** The hub takes a topic gate, not a principal.
So: no per-user authorization, rate limiting, quotas, sessions, or
CORS. Those need the whole request, and they belong in middleware over
the transport's `Router` — which is why the transports hand you a
`Router` instead of a server. One pattern is worth knowing before you
conclude the gap is fatal: a per-subscription `Filter` *is* row-level
authorization when authority lines up with the key. Authenticate in
your middleware, derive `key_prefix = "tenant-7/"` from the identity,
and pass it to `subscribe`. That works today, and it is a reason to put
the tenant in the key when you design the topic.

**Never keeps state a client could keep.** The resume token lives in
the client. So: no server-side durable cursors and no consumer-group
membership in this crate (`odradek-client` has both, for programs that
want them). What the refusal buys is worth more than the feature: a
reader that reconnects to a *different process* resumes exactly, with
zero coordination, because its cursor arrived in the request. Running N
replicas behind a load balancer is correct by construction rather than
by a clustering mode.

Two more, refused deliberately:

- **No total order across partitions.** A topic subscription holds
  order *within* each partition and interleaves them. A global order
  would mean buffering every partition to the pace of the slowest, and
  one quiet partition would stall the stream indefinitely.
- **No write path.** `RecordSource` has no `append`, so no adapter has
  to implement one it cannot honour. Produce with the store's own
  client; this side is a reader.

## Features

`kafka` (default) pulls in `odradek-client` for the real-cluster
`KafkaSource`. With `--no-default-features` the engine — pump, hub,
filters, cursors, in-memory source — builds without any Kafka client
at all; bring your own `RecordSource`.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
