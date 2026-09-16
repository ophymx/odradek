//! Error types shared by the wire codecs.

/// A value could not be encoded into the Kafka wire format.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EncodeError {
    /// A string or byte sequence exceeds the maximum length its wire
    /// representation can carry (e.g. a `STRING` longer than `i16::MAX`).
    #[error("value of length {len} exceeds the wire format limit of {max}")]
    TooLong { len: usize, max: usize },
    /// A field was `None` but the message version being encoded does not
    /// permit null for it.
    #[error("field {0} may not be null in this message version")]
    NullField(&'static str),
}

/// Bytes on the wire could not be decoded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    /// The buffer ended before the value was complete.
    #[error("buffer truncated: needed {needed} more byte(s)")]
    Truncated { needed: usize },
    /// A varint ran past the maximum width for its type.
    #[error("varint overflows a 64-bit value")]
    VarintOverflow,
    /// A length prefix was negative where null is not permitted, or does not
    /// fit in memory on this platform.
    #[error("invalid length prefix: {0}")]
    InvalidLength(i64),
    /// A `STRING` value was not valid UTF-8.
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    /// An API key, error code, or other enum discriminant is unknown.
    #[error("unknown {kind} discriminant: {value}")]
    UnknownDiscriminant { kind: &'static str, value: i64 },
}
