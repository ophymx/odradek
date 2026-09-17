//! Client error types.

use odradek_protocol::{DecodeError, EncodeError, ErrorCode};

/// Errors surfaced by the client.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode error: {0}")]
    Encode(#[from] EncodeError),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    /// The api key has no generated message support in `odradek-protocol`.
    #[error("api key {0} is not supported by this client")]
    UnsupportedApi(i16),
    /// The peer and this client share no usable version of an API.
    #[error("no mutually supported version for api key {0}")]
    NoCommonVersion(i16),
    /// The connection is closed; in-flight and future requests fail with this.
    #[error("connection closed")]
    ConnectionClosed,
    /// A client-side deadline elapsed: [`crate::ClientConfig::connect_timeout`]
    /// over a whole dial, or [`crate::ClientConfig::request_timeout`] over one
    /// request's response wait. The payload names which.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    /// The peer broke the protocol (bad correlation id, oversized frame, ...).
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    /// The broker answered with an error code.
    #[error("broker error: {0}")]
    Broker(ErrorCode),
    /// No bootstrap server could be reached.
    #[error("no bootstrap server reachable: {0}")]
    Bootstrap(String),
    /// The cluster metadata names no live leader for the partition.
    #[error("no known leader for {topic}[{partition}]")]
    UnknownLeader { topic: String, partition: i32 },
    /// The fetched data uses a compression codec this client cannot
    /// materialize yet.
    #[error("compressed batches are not supported yet ({0})")]
    UnsupportedCompression(&'static str),
    /// TLS configuration or handshake failure.
    #[error("tls: {0}")]
    Tls(String),
    /// SASL authentication failure (bad credentials, unsupported
    /// mechanism, or a malformed exchange).
    #[error("sasl: {0}")]
    Sasl(String),
}

impl ClientError {
    /// True when retrying after refreshed metadata could plausibly
    /// succeed: leadership moved, a topic is still materializing, or the
    /// connection died under us.
    pub fn is_retriable(&self) -> bool {
        match self {
            ClientError::ConnectionClosed | ClientError::Io(_) | ClientError::Timeout(_) => true,
            ClientError::Broker(code) => {
                *code == ErrorCode::NOT_LEADER_OR_FOLLOWER
                    || *code == ErrorCode::LEADER_NOT_AVAILABLE
                    || *code == ErrorCode::UNKNOWN_TOPIC_OR_PARTITION
                    || *code == ErrorCode::UNKNOWN_TOPIC_ID
                    || *code == ErrorCode::COORDINATOR_LOAD_IN_PROGRESS
                    || *code == ErrorCode::COORDINATOR_NOT_AVAILABLE
                    || *code == ErrorCode::NOT_COORDINATOR
                    || *code == ErrorCode::REQUEST_TIMED_OUT
                    || *code == ErrorCode::KAFKA_STORAGE_ERROR
            }
            _ => false,
        }
    }
}
