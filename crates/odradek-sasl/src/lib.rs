//! Sans-I/O SASL for the odradek crates: SCRAM-SHA-256/512 and PLAIN,
//! both the client and the server role.
//!
//! This is not a general SASL framework, and the name says so: it is the
//! mechanisms the [odradek](https://github.com/ophymx/odradek) Kafka
//! crates need, in the shape they need them. It exists because three
//! places in that workspace were deriving the same keys — a client, a
//! reference server, and the acceptance suite that checks both — and a
//! security exchange implemented three times is implemented zero times
//! well.
//!
//! Sans-I/O, like the protocol crate: every type here produces and
//! consumes strings, and the caller carries them over whatever
//! transport it has. That is also what makes both roles testable
//! against each other and against the RFC's worked examples without a
//! socket.
//!
//! ```
//! use odradek_sasl::{Limits, Mechanism, ScramClient, ScramServer};
//!
//! let mut client = ScramClient::new(
//!     Mechanism::ScramSha256, "user", "pencil", Limits::default(),
//! )?;
//! let mut server = ScramServer::new(
//!     Mechanism::ScramSha256, "user", "pencil",
//!     b"salt".to_vec(), 4096, Limits::default(),
//! )?;
//!
//! let first = client.client_first();
//! let challenge = server.server_first(&first)?;
//! let final_message = client.client_final(&challenge)?;
//! let signature = server.server_final(&final_message)?;
//! // The step that authenticates the *server*; skipping it makes the
//! // exchange one-way.
//! client.verify_server_final(&signature)?;
//! # Ok::<(), odradek_sasl::SaslError>(())
//! ```
//!
//! # Scope, and what is deliberately absent
//!
//! - **No channel binding.** The client advertises `n` (does not
//!   support it) rather than `y` (supports it, server did not offer),
//!   so a server requiring channel binding refuses instead of silently
//!   downgrading.
//! - **No credential store.** [`ScramServer`] holds one account, because
//!   its consumers are a reference implementation and a test double.
//!   Storing derived keys rather than a password is what a real server
//!   should do, and this deliberately does not pretend to be one.
//! - **No GSSAPI, OAUTHBEARER, or DIGEST-MD5.** [`Mechanism`] is
//!   `#[non_exhaustive]` so adding one is not a breaking change.
//!
//! # What SASL protects, and what it does not
//!
//! SASL authenticates; it does not encrypt. **PLAIN** puts the password
//! on the wire and is refused by this crate's consumers on an
//! unencrypted connection. **SCRAM** never sends the password, but a
//! passive observer of a plaintext connection still collects the
//! username, salt, iteration count, nonces and proof — exactly the
//! inputs to an offline dictionary attack, at the cost the iteration
//! count sets. SCRAM over plaintext protects a password from being
//! *read*, not from being *cracked*. Use TLS.

#![forbid(unsafe_code)]

mod mechanism;
mod plain;
mod prep;
mod scram;

pub use mechanism::Mechanism;
pub use plain::{plain_token, verify_plain_token};
pub use prep::saslprep;
pub use scram::{GS2_HEADER, ScramClient, ScramServer, fresh_nonce};

/// Bounds on the iteration count, which one side names and the other
/// pays.
///
/// Both directions matter and for different reasons. A count *below*
/// the floor is a downgrade: PBKDF2 with one round is a single HMAC,
/// and a client that computes a proof over that key has handed a
/// near-unprotected credential to whoever asked. A count *above* the
/// ceiling is a denial of service: the client must spend it before it
/// has learned anything about the server, so an unbounded count is a
/// server-controlled CPU burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Limits {
    /// RFC 7677 §4 makes 4096 the minimum for SCRAM-SHA-256.
    pub min_iterations: u32,
    pub max_iterations: u32,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            min_iterations: 4096,
            max_iterations: 1_000_000,
        }
    }
}

impl Limits {
    /// Bounds of your own. The floor should not go below RFC 7677's
    /// 4096 without a reason you can state.
    pub fn new(min_iterations: u32, max_iterations: u32) -> Limits {
        Limits {
            min_iterations,
            max_iterations,
        }
    }

    fn check(&self, iterations: u32) -> Result<(), SaslError> {
        if iterations < self.min_iterations {
            return Err(SaslError::IterationsTooLow {
                asked: iterations,
                floor: self.min_iterations,
            });
        }
        if iterations > self.max_iterations {
            return Err(SaslError::IterationsTooHigh {
                asked: iterations,
                ceiling: self.max_iterations,
            });
        }
        Ok(())
    }
}

/// What went wrong in an exchange.
///
/// No variant carries a password, a key, or a proof: an error type is
/// the most likely thing in a program to end up in a log.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SaslError {
    /// A credential could not be normalized (RFC 4013), so it cannot be
    /// used as a key input at all.
    Unpreparable,
    /// A message was missing an attribute or had the wrong shape.
    Malformed(&'static str),
    /// A step was taken before the one it depends on.
    OutOfSequence,
    /// The server's nonce does not extend the client's, so this answer
    /// could be a recording of an earlier exchange.
    NonceNotExtended,
    /// The server named an iteration count below the floor, which would
    /// weaken the key this client derives.
    IterationsTooLow { asked: u32, floor: u32 },
    /// The server named an iteration count above the ceiling, which is
    /// a CPU burn the client must pay before learning anything.
    IterationsTooHigh { asked: u32, ceiling: u32 },
    /// The final message carried no signature, so the server proved
    /// nothing about itself.
    NoServerSignature,
    /// The server's signature does not verify: it does not hold this
    /// account's key material.
    ServerSignatureMismatch,
    /// The client's proof does not verify.
    BadClientProof,
    /// The channel-binding field does not match the GS2 header the
    /// exchange began with: something rewrote it in flight.
    ChannelBindingMismatch,
    /// The server named an account it does not have.
    UnknownUser,
    /// The server answered with an `e=` error.
    ServerRejected(String),
    /// A SCRAM operation was asked of a non-SCRAM mechanism.
    NotScram,
    /// The system could not supply randomness for a nonce.
    Entropy,
}

impl std::fmt::Display for SaslError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslError::Unpreparable => {
                write!(f, "credential cannot be normalized (RFC 4013 saslprep)")
            }
            SaslError::Malformed(what) => write!(f, "malformed SASL message: bad {what}"),
            SaslError::OutOfSequence => write!(f, "SASL steps taken out of order"),
            SaslError::NonceNotExtended => write!(
                f,
                "server nonce does not extend the client's, so the answer may be a replay"
            ),
            SaslError::IterationsTooLow { asked, floor } => write!(
                f,
                "server asks for {asked} iterations, below this client's floor of {floor}"
            ),
            SaslError::IterationsTooHigh { asked, ceiling } => write!(
                f,
                "server asks for {asked} iterations, above this client's ceiling of {ceiling}"
            ),
            SaslError::NoServerSignature => {
                write!(f, "server did not sign the exchange")
            }
            SaslError::ServerSignatureMismatch => write!(
                f,
                "server signature mismatch: it does not know this password"
            ),
            SaslError::BadClientProof => write!(f, "client proof does not verify"),
            SaslError::ChannelBindingMismatch => write!(
                f,
                "channel-binding field does not match the header this exchange began with"
            ),
            SaslError::UnknownUser => write!(f, "no such user"),
            SaslError::ServerRejected(e) => write!(f, "server rejected the exchange: {e}"),
            SaslError::NotScram => write!(f, "not a SCRAM mechanism"),
            SaslError::Entropy => write!(f, "could not generate a nonce"),
        }
    }
}

impl std::error::Error for SaslError {}

/// Base64, the encoding every SCRAM field that is not text uses.
mod b64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    use crate::SaslError;

    pub(crate) fn encode(raw: &[u8]) -> String {
        STANDARD.encode(raw)
    }

    pub(crate) fn decode(value: &str) -> Result<Vec<u8>, SaslError> {
        STANDARD
            .decode(value)
            .map_err(|_| SaslError::Malformed("base64"))
    }
}
