//! Async, Rust-native Kafka client.
//!
//! This crate owns everything the sans-I/O [`odradek_protocol`] layer
//! deliberately does not: connections, API version negotiation, metadata
//! discovery and routing, and — above that — producer and consumer
//! machinery.
//!
//! Planned layering, bottom up:
//! 1. `conn`: a single broker connection — framing (i32 length prefix),
//!    correlation-id matching, in-flight request pipelining, ApiVersions
//!    negotiation on connect.
//! 2. `cluster`: connection pool keyed by broker id, metadata cache,
//!    partition-leader routing, retry/backoff policy.
//! 3. `producer` / `consumer`: batching, compression, consumer groups.
//!
//! Current state: `conn` (framing, correlation-id pipelining, ApiVersions
//! negotiation) and `cluster` (metadata cache, per-broker connections,
//! partition-leader routing) are implemented; `producer` and `consumer`
//! are next.

pub mod cluster;
pub mod conn;
pub mod error;
pub mod negotiate;

pub use cluster::Cluster;
pub use conn::Connection;
pub use error::ClientError;
pub use negotiate::ApiVersionRanges;
pub use odradek_protocol as protocol;

/// Configuration shared by every entry point of the client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Initial brokers used to bootstrap cluster metadata, as `host:port`.
    pub bootstrap_servers: Vec<String>,
    /// Client id reported to the broker in every request header.
    pub client_id: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            client_id: "odradek".to_owned(),
        }
    }
}
