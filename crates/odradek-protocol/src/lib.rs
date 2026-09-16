//! Sans-I/O implementation of the Kafka wire protocol.
//!
//! Kafka is increasingly a *protocol* with multiple independent server and
//! client implementations, not just the Apache broker. This crate is the
//! shared foundation of the odradek constellation: it knows how to encode and
//! decode the protocol, and nothing about sockets, async runtimes, or broker
//! semantics. That keeps it reusable from the client, the acceptance suite
//! (which must impersonate both sides of the wire), and the web proxies.
//!
//! Layout:
//! - [`wire`]: primitive codecs — fixed-width big-endian integers, varints,
//!   zigzag varints, (compact/nullable) strings and bytes, array length
//!   prefixes, and tagged fields (flexible versions / KIP-482).
//! - [`api_key`]: the API key registry identifying each request type.
//! - [`error`]: encode/decode error types.
//!
//! Versioned message types (request/response structs generated from the
//! upstream message schemas) are the next layer to land here.

pub mod api_key;
pub mod error;
pub mod error_code;
pub mod header;
pub mod messages;
pub mod wire;

pub use api_key::ApiKey;
pub use error::{DecodeError, EncodeError};
pub use error_code::ErrorCode;
