//! The mechanisms this crate speaks.

/// A SASL mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mechanism {
    /// Username and password, in the clear. Authenticates only if the
    /// transport is encrypted.
    Plain,
    /// SCRAM with SHA-256 (RFC 7677).
    ScramSha256,
    /// SCRAM with SHA-512.
    ScramSha512,
}

impl Mechanism {
    /// The name this mechanism has on the wire.
    pub fn name(self) -> &'static str {
        match self {
            Mechanism::Plain => "PLAIN",
            Mechanism::ScramSha256 => "SCRAM-SHA-256",
            Mechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Parse a wire name.
    pub fn from_name(name: &str) -> Option<Mechanism> {
        match name {
            "PLAIN" => Some(Mechanism::Plain),
            "SCRAM-SHA-256" => Some(Mechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(Mechanism::ScramSha512),
            _ => None,
        }
    }

    /// Whether the mechanism puts the password itself on the wire.
    ///
    /// The one question a caller must ask before choosing a transport:
    /// a mechanism that sends the password needs an encrypted one, and
    /// no amount of care elsewhere substitutes.
    pub fn sends_password(self) -> bool {
        match self {
            Mechanism::Plain => true,
            // SCRAM sends a proof derived from the password, never the
            // password.
            Mechanism::ScramSha256 | Mechanism::ScramSha512 => false,
        }
    }
}

impl std::fmt::Display for Mechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
