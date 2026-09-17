//! Transport-agnostic bridge from Kafka partitions to web-shaped
//! subscribers.
//!
//! Web clients cannot speak the Kafka wire protocol; this crate is the
//! engine that translates between the two worlds, independent of any
//! particular transport. An SSE or WebSocket server is a thin loop over
//! a [`Subscription`]; the hard parts live here:
//!
//! - **Fan-out**: one [`pump`](pump::PumpHandle) per (topic, partition)
//!   owns the Kafka connection; any number of subscribers share it.
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
//! The engine reads through the [`RecordSource`] trait, so it tests
//! against an in-memory log; [`KafkaSource`] adapts
//! [`odradek_client::Consumer`] for production.

pub mod cursor;
pub mod event;
pub mod hub;
pub mod json;
pub mod memory;
pub mod pump;
pub mod source;

pub use event::{Event, Filter, Position, TopicPosition};
pub use hub::{Hub, TopicSubscription};
pub use pump::{HubError, PumpConfig, PumpHandle, Subscription};
#[cfg(feature = "kafka")]
pub use source::{KafkaSource, KafkaSourceFactory};
pub use source::{RecordSource, SourceBatch, SourceError, SourceFactory};
