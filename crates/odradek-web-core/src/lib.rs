//! Transport-agnostic bridge from Kafka partitions to web-shaped
//! subscribers.
//!
//! Web clients cannot speak the Kafka wire protocol; this crate is the
//! engine that translates between the two worlds, independent of any
//! particular transport. An SSE or WebSocket server is a thin loop over
//! a [`Subscription`]; the hard parts live here:
//!
//! - **Fan-out**: one [`pump`](pump::PumpHandle) per (topic, partition)
//!   owns the Kafka connection; any number of subscribers share it —
//!   and share the events themselves, as [`SharedEvent`] handles that
//!   carry the record's JSON rendering with them, computed once however
//!   many subscribers ask for it.
//! - **Replay**: subscribers start [`Earliest`](Position::Earliest),
//!   [`Latest`](Position::Latest), or at an exact offset — and every
//!   [`Event`] carries its offset, so `offset + 1` is a natural resume
//!   token (`Last-Event-ID`, in SSE terms).
//! - **Filtering**: per-subscriber [`Filter`]s (key prefix, header
//!   match) applied before anything is queued.
//! - **Self-healing backpressure**: a slow subscriber falls out of the
//!   live path into catch-up (from the in-memory ring, or from Kafka
//!   itself past the ring) and rejoins as it drains. Delivery is in
//!   offset order with no gaps and no duplicates, at every speed.
//!
//! - **Bounded by default**: a [`Hub`] serves no topic until its gate
//!   says which ([`Hub::with_topic_gate`] or the explicit
//!   [`Hub::allow_all_topics`]), a subscribe to a topic or partition
//!   the source does not have is refused before anything is allocated
//!   for it, pumps whose task has exited are evicted rather than
//!   retained, and [`Hub::with_max_pumps`] caps how many can run at
//!   once. See the [`hub`] module docs for what that bounds and what
//!   it costs.
//!
//! The engine reads through the [`RecordSource`] trait, so it tests
//! against an in-memory log; [`KafkaSource`] adapts
//! [`odradek_client::Consumer`] for production.
//!
//! This crate is the engine plus the shared web wire contract — the
//! query grammar ([`StreamParams`]), the JSON event shape
//! ([`event_json`]), and the resume cursors ([`cursor`]) — so the
//! transports cannot drift apart.

pub mod cursor;
pub mod event;
pub mod hub;
pub mod json;
pub mod memory;
pub mod params;
pub mod pump;
pub mod source;

pub use event::{Event, Filter, Position, SharedEvent, TopicPosition};
pub use hub::{DEFAULT_MAX_PUMPS, Hub, Rejection, SharedHub, TopicSubscription};
pub use json::{event_json, event_json_bytes};
pub use memory::{MemoryFactory, MemoryLog};
pub use params::StreamParams;
pub use pump::{
    HubError, PumpConfig, PumpHandle, RejectionKind, StreamError, StreamItem, Subscription,
};
#[cfg(feature = "kafka")]
pub use source::{KafkaSource, KafkaSourceFactory};
pub use source::{RecordSource, SourceBatch, SourceError, SourceErrorKind, SourceFactory};

/// The Kafka client behind [`KafkaSource`], re-exported so integrators
/// configure it without naming another crate.
#[cfg(feature = "kafka")]
pub use odradek_client as client;
#[cfg(feature = "kafka")]
pub use odradek_client::ClientConfig;
