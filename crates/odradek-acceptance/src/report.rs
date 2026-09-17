//! Conformance reports and baselines.
//!
//! A baseline records the expected status of every check for a given
//! subject implementation. Committing baselines per implementation turns
//! conformance runs into regression tests: a new failure is a defect (in
//! the subject or in the suite), and an unexpected pass means a known
//! deviation was fixed and the baseline should be updated.
//!
//! Both JSON shapes carry a versioned envelope so files remain
//! interpretable after the suite evolves: `format` is the schema version
//! (currently [`FORMAT`]) and `suite` records which suite version
//! produced the file. Reports round-trip through JSON, so a stored run
//! can be re-diffed later.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CheckId, Verdict};

/// The JSON schema version this suite writes (and the only one it reads).
pub const FORMAT: u32 = 1;

/// The suite version stamped into reports and baselines.
const SUITE: &str = env!("CARGO_PKG_VERSION");

/// The result of one check, together with what it verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CheckOutcome {
    pub id: CheckId,
    /// The protocol requirement this check verifies, phrased as a claim
    /// about the subject.
    pub requirement: String,
    #[serde(flatten)]
    pub verdict: Verdict,
}

impl CheckOutcome {
    pub fn new(id: CheckId, requirement: impl Into<String>, verdict: Verdict) -> CheckOutcome {
        CheckOutcome {
            id,
            requirement: requirement.into(),
            verdict,
        }
    }
}

/// All outcomes from running a suite against one subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Report {
    /// Envelope schema version; see [`FORMAT`].
    pub format: u32,
    /// The suite version that produced this report.
    pub suite: String,
    /// The subject, e.g. `server localhost:9092`.
    pub subject: String,
    pub outcomes: Vec<CheckOutcome>,
}

/// The status of a check, stripped of details, as stored in baselines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BaselineStatus {
    Pass,
    Fail,
    Skipped,
    /// The check could not run (infrastructure). Never treated as a
    /// match by [`Report::diff_against`]: an `error` in a baseline means
    /// it was recorded during an outage and should be re-recorded.
    Error,
}

impl fmt::Display for BaselineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaselineStatus::Pass => write!(f, "pass"),
            BaselineStatus::Fail => write!(f, "fail"),
            BaselineStatus::Skipped => write!(f, "skipped"),
            BaselineStatus::Error => write!(f, "error"),
        }
    }
}

impl From<&Verdict> for BaselineStatus {
    fn from(v: &Verdict) -> Self {
        match v {
            Verdict::Pass => BaselineStatus::Pass,
            Verdict::Fail { .. } => BaselineStatus::Fail,
            Verdict::Skipped { .. } => BaselineStatus::Skipped,
            Verdict::Error { .. } => BaselineStatus::Error,
        }
    }
}

/// Expected per-check statuses for one subject implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Baseline {
    /// Envelope schema version; see [`FORMAT`].
    pub format: u32,
    /// The suite version that recorded this baseline.
    pub suite: String,
    pub checks: BTreeMap<String, BaselineStatus>,
}

impl Baseline {
    /// Parse a baseline, insisting on a compatible envelope.
    pub fn from_json(json: &str) -> Result<Baseline, String> {
        let baseline: Baseline = serde_json::from_str(json).map_err(explain_parse_error)?;
        check_format(baseline.format)?;
        Ok(baseline)
    }
}

/// One discrepancy between a run and a baseline. The variants keep the
/// three data cases apart — a real regression, a check the baseline does
/// not know yet, and a baseline entry for a check that no longer exists —
/// and set infrastructure trouble apart from all of them.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BaselineDiff {
    /// The check ran in both; its outcome changed.
    Regression {
        id: String,
        expected: BaselineStatus,
        actual: BaselineStatus,
    },
    /// The check ran but the baseline has no entry for it — the suite
    /// grew a check since the baseline was recorded.
    NotInBaseline { id: String, actual: BaselineStatus },
    /// The baseline names a check the suite no longer runs.
    RemovedCheck {
        id: String,
        expected: BaselineStatus,
    },
    /// The check could not run this time (infrastructure). Never counts
    /// as matching the baseline, but is not evidence of nonconformance.
    Infrastructure { id: String, details: String },
}

impl fmt::Display for BaselineDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaselineDiff::Regression {
                id,
                expected,
                actual,
            } => write!(f, "{id}: expected {expected}, got {actual}"),
            BaselineDiff::NotInBaseline { id, actual } => write!(
                f,
                "{id}: ran ({actual}) but the baseline has no entry — \
                 re-record the baseline to adopt the new check"
            ),
            BaselineDiff::RemovedCheck { id, expected } => write!(
                f,
                "{id}: baseline expects {expected}, but no such check exists \
                 any more — re-record the baseline"
            ),
            BaselineDiff::Infrastructure { id, details } => write!(
                f,
                "{id}: infrastructure — the check could not run ({details}); \
                 not evidence of nonconformance"
            ),
        }
    }
}

impl Report {
    /// Assemble a report for one run, stamping the envelope.
    pub fn new(subject: impl Into<String>, outcomes: Vec<CheckOutcome>) -> Report {
        Report {
            format: FORMAT,
            suite: SUITE.into(),
            subject: subject.into(),
            outcomes,
        }
    }

    /// Parse a stored report, insisting on a compatible envelope.
    pub fn from_json(json: &str) -> Result<Report, String> {
        let report: Report = serde_json::from_str(json).map_err(explain_parse_error)?;
        check_format(report.format)?;
        Ok(report)
    }

    pub fn passed(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Pass))
    }

    pub fn failed(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Fail { .. }))
    }

    pub fn skipped(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Skipped { .. }))
    }

    /// Checks that could not run (infrastructure), as distinct from
    /// checks the subject failed.
    pub fn errored(&self) -> usize {
        self.count(|v| matches!(v, Verdict::Error { .. }))
    }

    fn count(&self, pred: impl Fn(&Verdict) -> bool) -> usize {
        self.outcomes.iter().filter(|o| pred(&o.verdict)).count()
    }

    /// True when nothing failed (skips do not fail a run). Says nothing
    /// about [`errored`](Report::errored) checks: a run with errors
    /// proves neither conformance nor nonconformance.
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
            format: self.format,
            suite: self.suite.clone(),
            checks: self
                .outcomes
                .iter()
                .map(|o| (o.id.0.clone(), BaselineStatus::from(&o.verdict)))
                .collect(),
        }
    }

    /// Compare this run against a baseline. Empty means the run matches
    /// expectations exactly — including that known deviations still
    /// deviate. A check that could not run
    /// ([`Verdict::Error`]) never matches: it surfaces as
    /// [`BaselineDiff::Infrastructure`] rather than pretending the
    /// subject regressed.
    pub fn diff_against(&self, baseline: &Baseline) -> Vec<BaselineDiff> {
        let mut diffs = Vec::new();
        for o in &self.outcomes {
            if let Verdict::Error { details } = &o.verdict {
                diffs.push(BaselineDiff::Infrastructure {
                    id: o.id.0.clone(),
                    details: details.clone(),
                });
                continue;
            }
            let actual = BaselineStatus::from(&o.verdict);
            match baseline.checks.get(&o.id.0) {
                None => diffs.push(BaselineDiff::NotInBaseline {
                    id: o.id.0.clone(),
                    actual,
                }),
                Some(&expected) if expected != actual => diffs.push(BaselineDiff::Regression {
                    id: o.id.0.clone(),
                    expected,
                    actual,
                }),
                Some(_) => {}
            }
        }
        for (id, &expected) in &baseline.checks {
            if !self.outcomes.iter().any(|o| &o.id.0 == id) {
                diffs.push(BaselineDiff::RemovedCheck {
                    id: id.clone(),
                    expected,
                });
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
                Verdict::Error { details } => {
                    writeln!(f, "  ERROR {}", outcome.id)?;
                    writeln!(f, "        could not run: {details}")?;
                }
            }
        }
        write!(
            f,
            "{} passed, {} failed, {} skipped, {} errored",
            self.passed(),
            self.failed(),
            self.skipped(),
            self.errored()
        )
    }
}

fn check_format(format: u32) -> Result<(), String> {
    if format == FORMAT {
        Ok(())
    } else {
        Err(format!(
            "format {format} is not supported (this suite reads format {FORMAT}); re-record"
        ))
    }
}

fn explain_parse_error(e: serde_json::Error) -> String {
    let msg = e.to_string();
    if msg.contains("missing field `format`") {
        format!("{msg} — the file predates the versioned envelope (format {FORMAT}); re-record")
    } else {
        msg
    }
}
