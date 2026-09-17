//! The trait every generated request/response message implements.
//!
//! Generated inherent methods remain the primary API; this trait is the
//! generic spine over them, so dispatch loops (a proxy's forwarding
//! core, the acceptance suite's exchange helpers) can be written once
//! instead of per message type. Implementations are emitted by
//! `cargo xtask codegen` for every top-level message.

use bytes::{Buf, BufMut};

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
    fn decode(buf: &mut impl Buf, version: i16) -> Result<Self, DecodeError>;
}
