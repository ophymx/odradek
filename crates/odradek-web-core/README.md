# odradek-web-core

Transport-agnostic bridge from Kafka partitions to web-shaped
subscribers.

One pump per (topic, partition) fans out to any number of subscribers,
each starting from earliest, latest, or a specific offset, with
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

## Features

`kafka` (default) pulls in `odradek-client` for the real-cluster
`KafkaSource`. With `--no-default-features` the engine — pump, hub,
filters, cursors, in-memory source — builds without any Kafka client
at all; bring your own `RecordSource`.

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
