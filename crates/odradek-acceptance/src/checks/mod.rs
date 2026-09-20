//! The check catalog, organized by subject role.
//!
//! Checks are data: every check the suite can run is an entry in
//! [`server::SERVER_CHECKS`] or [`client::CLIENT_CHECKS`], carrying its
//! stable id, the requirement it verifies, and the runner that verifies
//! it. The catalog is the single source of truth — `run()` on either side
//! iterates it, reports cite its ids verbatim, and the calibration tests
//! derive their expectations from it.

use std::future::Future;
use std::pin::Pin;

use crate::{SubjectRole, Verdict};

pub mod client;
pub mod server;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a catalogued check executes. The variant *is* the check's subject
/// role: a server check drives a connection to the subject, a client
/// check evaluates a recorded harness session.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Runner {
    Server(for<'a> fn(&'a server::ServerCtx) -> BoxFuture<'a, Verdict>),
    Client(fn(&client::Session) -> Verdict),
}

/// A catalogued check: a stable, citable id, the protocol requirement it
/// verifies, and (crate-internally) the runner that verifies it.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct Check {
    /// Stable identifier, e.g. `api-versions/v0-basic`. Baselines and
    /// reports key on this.
    pub id: &'static str,
    /// The requirement, phrased as a claim about the subject.
    pub requirement: &'static str,
    pub(crate) runner: Runner,
}

impl Check {
    /// Which side of the wire this check subjects to scrutiny. Derived
    /// from the runner, so a check cannot claim one role and run as the
    /// other.
    pub fn role(&self) -> SubjectRole {
        match self.runner {
            Runner::Server(_) => SubjectRole::Server,
            Runner::Client(_) => SubjectRole::Client,
        }
    }
}

/// Every check the suite knows, server checks first, in run order.
pub fn catalog() -> impl Iterator<Item = &'static Check> {
    server::SERVER_CHECKS.iter().chain(client::CLIENT_CHECKS)
}
