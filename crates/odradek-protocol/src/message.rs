//! The trait every generated request/response message implements.
//!
//! Generated inherent methods remain the primary API; this trait is the
//! generic spine over them, so dispatch loops (a proxy's forwarding
//! core, the acceptance suite's exchange helpers) can be written once
//! instead of per message type. Implementations are emitted by
//! `cargo xtask codegen` for every top-level message.

use bytes::{Buf, BufMut};

use crate::budget::Limits;
use crate::error::{DecodeError, EncodeError};

/// A versioned Kafka message: encode/decode plus its identity in the
/// api-key registry.
pub trait Message: Sized {
    /// The api key identifying this message's request type.
    const API_KEY: i16;
    /// The lowest schema version this snapshot can speak.
    const MIN_VERSION: i16;
    /// The highest schema version this snapshot can speak.
    const MAX_VERSION: i16;

    fn encode(&self, buf: &mut impl BufMut, version: i16) -> Result<(), EncodeError>;

    /// Decode one message of `version`.
    ///
    /// Decoding costs O(fields), not O(payload bytes), but only when
    /// `buf` is a [`Bytes`](bytes::Bytes): payload fields (a fetch
    /// response's `records`, and the keys and values inside them) are
    /// then refcounted slices of the caller's buffer rather than copies.
    /// That comes from `Bytes`'s override of `Buf::copy_to_bytes`; other
    /// `Buf` implementations — slices, `Cursor`, `Chain` — get the
    /// default, which allocates and copies every payload, measured ~370x
    /// slower for a 1 MiB fetch response. Decode from a `Bytes` on any
    /// path that carries records. `tests/zero_copy.rs` holds the
    /// property to it.
    ///
    /// Allocation is bounded by the default
    /// [`Limits`], derived from `buf`'s remaining length; see
    /// [`Message::decode_with_limits`].
    fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, DecodeError>;

    /// Decode one message of `version` under `limits`.
    ///
    /// Every generated message overrides this. The default is here so
    /// that adding it broke no hand-written implementation: it ignores
    /// `limits` and decodes under that type's own default bound, which
    /// is never *less* safe than [`Message::decode`].
    fn decode_with_limits(
        buf: &mut impl Buf,
        version: i16,
        limits: Limits,
    ) -> Result<Self, DecodeError> {
        let _ = limits;
        Self::decode(buf, version)
    }
}
