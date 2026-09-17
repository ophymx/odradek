//! Async, Rust-native Kafka client.
//!
//! This crate owns everything the sans-I/O [`odradek_protocol`] layer
//! deliberately does not: connections, API version negotiation, metadata
//! discovery and routing, and — above that — producer, consumer, and
//! consumer-group machinery. It is explicit and caller-driven: every
//! network interaction happens on your task, inside your `.await`, with
//! no hidden background work.
//!
//! Layering, bottom up:
//! 1. [`conn`]: a single broker connection — framing (i32 length prefix),
//!    correlation-id matching, in-flight request pipelining, ApiVersions
//!    negotiation on connect, optional TLS and SASL.
//! 2. [`cluster`]: a [`Cluster`] handle over a connection pool keyed by
//!    broker id, a metadata cache, partition-leader routing, and
//!    coordinator discovery.
//! 3. [`producer`], [`consumer`], [`group`]: batching and compression on
//!    the produce side; fetch, offset lookup, and durable offsets on the
//!    consume side; classic join/sync/heartbeat/leave membership.
//!
//! # One cluster, many components
//!
//! [`Cluster`] is a cheap clonable handle. Clone it once per component —
//! `Producer::new(cluster.clone())`, `Consumer::new(cluster.clone())` —
//! and they share one authenticated connection pool and one metadata
//! cache instead of each dialing their own. A `Producer` is a per-task
//! object (it holds mutable batch buffers), so the idiom for a service
//! that produces from many tasks is one shared `Cluster` and a cheap
//! `Producer` per task. See [`cluster`] for how blocking requests
//! (long-poll fetch, parked join) interact with a shared connection.

pub mod cluster;
mod compression;
pub mod conn;
pub mod consumer;
pub mod error;
pub mod group;
pub mod negotiate;
mod offsets;
pub mod producer;
#[cfg(feature = "sasl")]
pub mod sasl;
#[cfg(feature = "tls")]
pub mod tls;

pub use cluster::Cluster;
pub use conn::Connection;
pub use consumer::{ConsumedRecord, Consumer, ConsumerConfig, FetchResult};
pub use error::ClientError;
pub use group::{GroupConfig, GroupMember, HeartbeatStatus};
pub use negotiate::ApiVersionRanges;
pub use odradek_protocol as protocol;
/// The record vocabulary users hand to [`Producer`] and get back from
/// [`Consumer`], re-exported from the protocol crate.
pub use odradek_protocol::records::{Compression, Record, RecordHeader};
pub use producer::{Delivery, Producer, ProducerConfig};
#[cfg(feature = "sasl")]
pub use sasl::{Mechanism, SaslConfig};
#[cfg(feature = "tls")]
pub use tls::Tls;

/// Configuration shared by every entry point of the client.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientConfig {
    /// Initial brokers used to bootstrap cluster metadata, as `host:port`.
    pub bootstrap_servers: Vec<String>,
    /// Client id reported to the broker in every request header.
    pub client_id: String,
    /// Ceiling on establishing one broker connection end to end: TCP
    /// connect, TLS handshake, ApiVersions negotiation, and SASL
    /// authentication together (default: 10s).
    pub connect_timeout: std::time::Duration,
    /// Ceiling on each request's wait for its response (default: 30s).
    /// A request that blows it poisons its connection — the broker
    /// processes a connection's requests in order, so everything queued
    /// behind a hung request is hung too — and the next use redials.
    /// Long-polling requests must fit under it; see
    /// [`ConsumerConfig::max_wait_ms`].
    pub request_timeout: std::time::Duration,
    /// TLS for every broker connection (default: plaintext).
    #[cfg(feature = "tls")]
    pub tls: Tls,
    /// SASL credentials, authenticated on every connection right after
    /// version negotiation (default: none).
    #[cfg(feature = "sasl")]
    pub sasl: Option<SaslConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            client_id: "odradek".to_owned(),
            connect_timeout: std::time::Duration::from_secs(10),
            request_timeout: std::time::Duration::from_secs(30),
            #[cfg(feature = "tls")]
            tls: Tls::None,
            #[cfg(feature = "sasl")]
            sasl: None,
        }
    }
}
