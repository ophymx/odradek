//! Conformance reports and baselines.
//!
//! A baseline records the expected status of every check for a given
//! subject implementation. Committing baselines per implementation turns
//! conformance runs into regression tests: a new failure is a defect (in
//! the subject or in the suite), and an unexpected pass means a known
//! deviation was fixed and the baseline should be updated.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CheckId, Verdict};

/// The result of one check, together with what it verified.
#[derive(Debug, Clone, Serialize)]
pub struct CheckOutcome {
    pub id: CheckId,
    /// The protocol requirement this check verifies, phrased as a claim
    /// about the subject.
    pub requirement: &'static str,
    #[serde(flatten)]
    pub verdict: Verdict,
}

/// All outcomes from running a suite against one subject.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    /// The subject, e.g. `server localhost:9092`.
    pub subject: String,
    pub outcomes: Vec<CheckOutcome>,
}

/// The status of a check, stripped of details, as stored in baselines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BaselineStatus {
    Pass,
    Fail,
    Skipped,
}

impl fmt::Display for BaselineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaselineStatus::Pass => write!(f, "pass"),
            BaselineStatus::Fail => write!(f, "fail"),
            BaselineStatus::Skipped => write!(f, "skipped"),
        }
    }
}

impl From<&Verdict> for BaselineStatus {
    fn from(v: &Verdict) -> Self {
        match v {
            Verdict::Pass => BaselineStatus::Pass,
            Verdict::Fail { .. } => BaselineStatus::Fail,
            Verdict::Skipped { .. } => BaselineStatus::Skipped,
        }
    }
}

/// Expected per-check statuses for one subject implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Baseline {
    pub checks: BTreeMap<String, BaselineStatus>,
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

    /// Look up one check's verdict by id.
    pub fn verdict(&self, id: &str) -> Option<&Verdict> {
        self.outcomes
            .iter()
            .find(|o| o.id.0 == id)
            .map(|o| &o.verdict)
    }

    /// Serialize the full report (including details) as pretty JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("report serializes")
    }

    /// Reduce this run to a baseline.
    pub fn to_baseline(&self) -> Baseline {
        Baseline {
            checks: self
                .outcomes
                .iter()
                .map(|o| (o.id.0.clone(), BaselineStatus::from(&o.verdict)))
                .collect(),
        }
    }

    /// Compare this run against a baseline. Returns one human-readable line
    /// per discrepancy; empty means the run matches expectations exactly —
    /// including that known deviations still deviate.
    pub fn diff_against(&self, baseline: &Baseline) -> Vec<String> {
        let mut diffs = Vec::new();
        let current = self.to_baseline();
        for (id, expected) in &baseline.checks {
            match current.checks.get(id) {
                None => diffs.push(format!("{id}: in baseline ({expected}) but not run")),
                Some(actual) if actual != expected => {
                    diffs.push(format!("{id}: expected {expected}, got {actual}"))
                }
                Some(_) => {}
            }
        }
        for (id, actual) in &current.checks {
            if !baseline.checks.contains_key(id) {
                diffs.push(format!("{id}: ran ({actual}) but missing from baseline"));
            }
        }
        diffs
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
