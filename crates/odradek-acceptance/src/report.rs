//! Conformance reports.

use std::fmt;

use crate::{CheckId, Verdict};

/// The result of one check, together with what it verified.
#[derive(Debug, Clone)]
pub struct CheckOutcome {
    pub id: CheckId,
    /// The protocol requirement this check verifies, phrased as a claim
    /// about the subject.
    pub requirement: &'static str,
    pub verdict: Verdict,
}

/// All outcomes from running a suite against one subject.
#[derive(Debug, Clone)]
pub struct Report {
    /// The subject, e.g. `server localhost:9092`.
    pub subject: String,
    pub outcomes: Vec<CheckOutcome>,
}

impl Report {
    pub fn passed(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Pass))
    }

    pub fn failed(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Fail { .. }))
    }

    pub fn skipped(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Skipped { .. }))
    }

    fn count(&self, pred: impl Fn(&Verdict) -> bool) -> usize {
        self.outcomes.iter().filter(|o| pred(&o.verdict)).count()
    }

    /// True when nothing failed (skips do not fail a run).
    pub fn is_conformant(&self) -> bool {
        self.failed() == 0
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "conformance report for {}", self.subject)?;
        for outcome in &self.outcomes {
            match &outcome.verdict {
                Verdict::Pass => writeln!(f, "  PASS {}", outcome.id)?,
                Verdict::Fail { details } => {
                    writeln!(f, "  FAIL {}", outcome.id)?;
                    writeln!(f, "       requirement: {}", outcome.requirement)?;
                    writeln!(f, "       observed: {details}")?;
                }
                Verdict::Skipped { reason } => writeln!(f, "  SKIP {} ({reason})", outcome.id)?,
            }
        }
        write!(
            f,
            "{} passed, {} failed, {} skipped",
            self.passed(),
            self.failed(),
            self.skipped()
        )
    }
}
