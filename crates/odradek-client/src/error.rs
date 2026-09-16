//! Client error types.

use odradek_protocol::{DecodeError, EncodeError, ErrorCode};

/// Errors surfaced by the client.
#[derive(Debug, thiserror::Error)]
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
    /// The peer broke the protocol (bad correlation id, oversized frame, ...).
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    /// The broker answered with an error code.
    #[error("broker error: {0}")]
    Broker(ErrorCode),
}
