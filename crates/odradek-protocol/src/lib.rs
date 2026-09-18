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
//! - [`frame`]: the i32 length-prefixed frame envelope around every
//!   request and response.
//! - [`api_key`]: the API key registry identifying each request type;
//!   [`message::Message`] is the generic spine over every generated
//!   message, and `messages::supported_versions` maps keys to the
//!   schema snapshot's version ranges.
//! - [`messages`]: versioned request/response structs generated from the
//!   vendored upstream schemas (`cargo xtask codegen`); unknown tagged
//!   fields round-trip raw.
//! - [`header`]: request/response header codecs and header-version
//!   selection, including the ApiVersions response-header quirk.
//! - [`records`]: the record batch (v2) codec with CRC-32C validation;
//!   compressed and unknown-codec payloads stay raw and re-encode
//!   byte-identically (the proxy guarantee).
//! - [`error_code`]: the open-world Kafka error code registry.
//! - [`error`]: encode/decode error types.
//!
//! # Zero-copy decoding
//!
//! Decoding a message costs O(fields) rather than O(payload bytes) —
//! but only when the source is a [`Bytes`](bytes::Bytes). Payload fields
//! (a fetch response's `records`, and the keys and values inside them)
//! are then refcounted slices of the caller's buffer, never copies.
//! The property comes from `Bytes`'s override of `Buf::copy_to_bytes`;
//! every other `Buf` implementation — slices, `Cursor`, `Chain` — falls
//! back to the default, which allocates and copies each payload,
//! measured ~370x slower for a 1 MiB fetch response. Decode from a
//! `Bytes` on any path that carries records. `tests/zero_copy.rs` holds
//! the guarantee to it, down to asserting the decoded payloads alias the
//! input buffer.

pub mod api_key;
pub mod consumer_protocol;
pub mod error;
pub mod error_code;
pub mod frame;
pub mod header;
pub mod message;
pub mod messages;
pub mod records;
pub mod wire;

pub use api_key::ApiKey;
pub use error::{DecodeError, EncodeError};
pub use error_code::ErrorCode;
pub use message::Message;
