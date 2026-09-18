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
//! Checks are data: every check lives in a static catalog
//! ([`checks::catalog`]) carrying its stable id, the protocol requirement
//! it verifies, and its [`SubjectRole`], so runs execute exactly the
//! catalog and reports can cite exactly what an implementation got wrong.

// This crate carries no `unsafe` block and has never needed one:
// forbid rather than deny, so the decision cannot be reversed by a
// local `allow` in a module nobody re-reads.
#![forbid(unsafe_code)]

use std::fmt;

pub mod checks;
pub mod raw;
pub mod report;
pub mod subject;

/// The wire codec the suite speaks, re-exported for embedders writing
/// their own checks against [`raw`] connections or the [`subject`].
pub use odradek_protocol as protocol;
pub use report::{CheckOutcome, Report};

/// Which side of the wire the subject implements. Every catalogued
/// [`checks::Check`] carries its role; a run executes exactly the checks
/// whose role matches the subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectRole {
    /// The subject accepts connections and answers requests (a broker or
    /// broker-compatible proxy).
    Server,
    /// The subject dials the suite's harness server and issues requests.
    Client,
}

impl fmt::Display for SubjectRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubjectRole::Server => write!(f, "server"),
            SubjectRole::Client => write!(f, "client"),
        }
    }
}

/// Stable identifier for a check, e.g. `api-versions/flexible-header`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CheckId(pub String);

impl fmt::Display for CheckId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Result of running a single check against a subject.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
#[non_exhaustive]
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
    /// The check could not run at all — connection refused, timeout,
    /// harness setup failure. This is an infrastructure finding about the
    /// run, never evidence that the subject violated the requirement.
    Error {
        details: String,
    },
}
