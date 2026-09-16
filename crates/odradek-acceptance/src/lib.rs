//! Acceptance suite for the Kafka *protocol* — not any one implementation.
//!
//! Kafka has outgrown the Apache broker: Redpanda, WarpStream, Bufstream,
//! and various proxies all speak the same wire format. This crate validates
//! either side of that conversation:
//!
//! - **Server under test**: the suite acts as a client, dials the subject,
//!   and drives checks against it (version negotiation, header conformance,
//!   error-code semantics, flexible-version handling, ...).
//! - **Client under test**: the suite acts as a server, accepts the
//!   subject's connections, and validates what the client puts on the wire
//!   (well-formed headers, correct compact encodings, sane retry behavior on
//!   injected errors, ...).
//!
//! Checks are data: each one carries an id, the protocol requirement it
//! verifies, and which subject roles it applies to, so reports can cite
//! exactly what an implementation got wrong.

use std::fmt;

pub mod checks;
pub mod raw;
pub mod report;

pub use report::{CheckOutcome, Report};

/// Which side of the wire the subject implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectRole {
    /// The subject accepts connections and answers requests (a broker or
    /// broker-compatible proxy).
    Server,
    /// The subject dials the suite's harness server and issues requests.
    Client,
}

/// Stable identifier for a check, e.g. `api-versions/flexible-header`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CheckId(pub String);

impl fmt::Display for CheckId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Result of running a single check against a subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    /// The subject violated the protocol; `details` explains the observed
    /// behavior versus the requirement.
    Fail {
        details: String,
    },
    /// The check does not apply (e.g. the subject does not advertise the
    /// API version the check exercises).
    Skipped {
        reason: String,
    },
}
