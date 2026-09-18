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
//! 3. [`producer`], [`consumer`], [`group`], [`consumer_group`]:
//!    batching and compression on the produce side; fetch, offset
//!    lookup, and durable offsets on the consume side; classic
//!    join/sync/heartbeat/leave membership and KIP-848 next-generation
//!    membership.
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
//!
//! # Security defaults
//!
//! **Connections are plaintext TCP unless you say otherwise.**
//! [`ClientConfig::default`] uses no transport encryption, so every
//! request, response, and record body crosses the network in the clear
//! and any on-path party can read or rewrite it. Turn on TLS with the
//! `tls` feature and `ClientConfig::tls` (see `Tls::system` or
//! `Tls::with_ca_pem`) for any network you do not own.
//!
//! Because that default is unsafe for credentials, the client refuses to
//! put a SASL PLAIN password on an unencrypted socket: it fails with
//! [`ClientError::InsecureCredentials`] before the handshake starts,
//! unless you explicitly set `ClientConfig::allow_plaintext_credentials`.
//! SCRAM is allowed over plaintext (it never transmits the password), but
//! a passive observer still learns the username, the salt, the iteration
//! count, and the client proof — enough to mount an offline dictionary
//! attack — so TLS is the right answer there too.
//!
//! Broker-controlled work is bounded on the paths where a hostile or
//! compromised broker could otherwise spend the client's CPU and memory:
//! see `ClientConfig::scram_max_iterations` for the SCRAM key-derivation
//! bound and [`consumer::ConsumerConfig::max_fetch_records`] for the
//! fetch materialization bound.

pub mod cluster;
mod compression;
pub mod conn;
pub mod consumer;
pub mod consumer_group;
pub mod error;
pub mod group;
mod join;
pub mod negotiate;
mod offsets;
pub mod producer;
mod retry;
#[cfg(feature = "sasl")]
pub mod sasl;
#[cfg(feature = "tls")]
pub mod tls;

pub use cluster::Cluster;
pub use conn::Connection;
pub use consumer::{ConsumedRecord, Consumer, ConsumerConfig, FetchResult};
pub use consumer_group::{ConsumerGroupConfig, ConsumerGroupMember, GroupEvent};
pub use error::{ClientError, ErrorCategory};
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
///
/// # Security defaults
///
/// **The default is plaintext TCP.** [`ClientConfig::default`] sets no
/// transport encryption, so unless you build with the `tls` feature and
/// set `ClientConfig::tls`, everything — records, request headers, and
/// any SCRAM handshake material — crosses the network readable and
/// modifiable by anything on the path. That default suits a loopback
/// broker in a test; it does not suit any network you do not own.
///
/// Credentials get a stronger default: SASL PLAIN over an unencrypted
/// connection is refused with [`ClientError::InsecureCredentials`]
/// rather than silently sending the password in the clear. Set
/// [`ClientConfig::allow_plaintext_credentials`] to accept that exposure
/// deliberately.
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
    /// Ceiling on idle *blocking* connections kept per broker
    /// (default: 256).
    ///
    /// Blocking requests — long-poll fetches, parked group joins — run
    /// on leased connections out of a per-broker pool (see [`cluster`]).
    /// The pool sizes itself to the peak number of leases a broker has
    /// had out at once, so a workload with 200 concurrent long polls
    /// against one broker keeps 200 connections warm rather than
    /// redialing TCP+TLS+ApiVersions+SASL for each one. This is the
    /// backstop on that: a workload that spikes once will not pin more
    /// than this many idle sockets per broker afterwards. Lower it for
    /// fd-tight environments; raise it past your peak fan-out if you
    /// run more concurrent long polls than this per broker.
    pub blocking_idle_max: usize,
    /// Permit mechanisms that transmit the password — SASL PLAIN — on
    /// connections with no transport encryption (default: `false`).
    ///
    /// Left `false`, a PLAIN authentication over plaintext TCP fails
    /// with [`ClientError::InsecureCredentials`] *before* anything is
    /// sent, because the alternative is putting the password on the wire
    /// where every hop can read it. Set it to `true` only when you have
    /// decided the exposure is acceptable — a loopback broker, a
    /// container network you own — or when encryption is terminated
    /// somewhere this client cannot see (a sidecar proxy, a VPN).
    ///
    /// It does not affect SCRAM, which never transmits the password;
    /// see the `sasl` module docs for what SCRAM does leak over
    /// plaintext.
    pub allow_plaintext_credentials: bool,
    /// TLS for every broker connection (default: plaintext).
    #[cfg(feature = "tls")]
    pub tls: Tls,
    /// SASL credentials, authenticated on every connection right after
    /// version negotiation (default: none).
    #[cfg(feature = "sasl")]
    pub sasl: Option<SaslConfig>,
    /// Ceiling on the SCRAM iteration count this client will honour
    /// (default: 1_000_000).
    ///
    /// The iteration count in a SCRAM exchange is chosen by the
    /// *broker*, and the client must run that many PBKDF2 rounds before
    /// it can tell whether the broker even knows the password. At
    /// roughly 2.8s per million rounds, an unbounded count is a remote
    /// CPU-burn primitive: `i=2^31` is about 1.6 hours of work per
    /// connection. The derivation runs on a blocking thread so it cannot
    /// wedge the async runtime, but it still costs a thread and a core,
    /// so it is bounded here too.
    ///
    /// The default is about two orders of magnitude above what brokers
    /// configure in practice (Kafka's own default is 4096). Raise it if
    /// your cluster deliberately runs a harder KDF; there is no reason
    /// to raise it past the work you are willing to spend per dial.
    ///
    /// The floor is not configurable: counts below RFC 7677's minimum of
    /// 4096 are always rejected, since accepting one lets a rogue broker
    /// downgrade the KDF to a single HMAC and harvest a proof that is
    /// cheap to attack offline.
    #[cfg(feature = "sasl")]
    pub scram_max_iterations: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            client_id: "odradek".to_owned(),
            connect_timeout: std::time::Duration::from_secs(10),
            request_timeout: std::time::Duration::from_secs(30),
            blocking_idle_max: 256,
            allow_plaintext_credentials: false,
            #[cfg(feature = "tls")]
            tls: Tls::None,
            #[cfg(feature = "sasl")]
            sasl: None,
            #[cfg(feature = "sasl")]
            scram_max_iterations: sasl::DEFAULT_MAX_SCRAM_ITERATIONS,
        }
    }
}
