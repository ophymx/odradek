//! Calibration of the server-side checks against the fault-injectable
//! subject: specificity (a conformant subject passes everything) and
//! sensitivity (each fault trips the check that claims to detect it).

use odradek_acceptance::Verdict;
use odradek_acceptance::checks;
use odradek_acceptance::subject::{Fault, SubjectServer};

#[tokio::test]
async fn compliant_subject_passes_all_checks() {
    let subject = SubjectServer::spawn(vec![]).await.unwrap();
    let report = checks::server::run(subject.addr()).await;
    assert!(report.is_conformant(), "false positives:\n{report}");
    assert_eq!(
        report.passed(),
        6,
        "expected every check to run and pass:\n{report}"
    );
}

/// fault → the check id that must detect it.
const SENSITIVITY: &[(Fault, &str)] = &[
    (Fault::WrongCorrelationEcho, "api-versions/correlation-echo"),
    (Fault::InvertedVersionRange, "api-versions/v0-basic"),
    (Fault::OmitApiVersionsKey, "api-versions/v0-basic"),
    (Fault::TrailingGarbage, "api-versions/v0-basic"),
    (
        Fault::WrongErrorOnUnsupportedVersion,
        "api-versions/unsupported-version-error",
    ),
    (
        Fault::ErrorBodyNotV0,
        "api-versions/unsupported-version-error",
    ),
    (
        Fault::AdvertiseWrongMaxInError,
        "api-versions/unsupported-version-error",
    ),
    (Fault::FlexibleHeaderOnV3, "api-versions/flexible-v3"),
    (Fault::MetadataEmptyBrokers, "metadata/basic"),
    (Fault::MetadataUnrequestedTopic, "metadata/basic"),
    (
        Fault::MetadataNonFlexibleHeader,
        "metadata/flexible-response-header",
    ),
];

/// The calibration registry is exhaustive in both directions: a check
/// without a fault that trips it is unproven (vacuous until shown
/// otherwise), and a fault no check detects is dead weight.
#[tokio::test]
async fn every_check_has_a_fault_and_every_fault_a_check() {
    let subject = SubjectServer::spawn(vec![]).await.unwrap();
    let report = checks::server::run(subject.addr()).await;
    for outcome in &report.outcomes {
        assert!(
            SENSITIVITY.iter().any(|(_, t)| *t == outcome.id.0),
            "check {} has no fault in SENSITIVITY proving it detects anything",
            outcome.id
        );
    }
    for fault in Fault::ALL {
        assert!(
            SENSITIVITY.iter().any(|(f, _)| f == fault),
            "fault {fault:?} is not mapped to a check in SENSITIVITY"
        );
    }
}

#[tokio::test]
async fn each_fault_trips_its_targeted_check() {
    for &(fault, target) in SENSITIVITY {
        let subject = SubjectServer::spawn(vec![fault]).await.unwrap();
        let report = checks::server::run(subject.addr()).await;
        assert!(
            matches!(report.verdict(target), Some(Verdict::Fail { .. })),
            "fault {fault:?} was not detected by {target}:\n{report}"
        );
    }
}

/// Faults that don't corrupt the discovery exchange must fail ONLY their
/// targeted check — no collateral false positives elsewhere.
#[tokio::test]
async fn isolated_faults_cause_no_collateral_failures() {
    let isolated = [
        Fault::WrongErrorOnUnsupportedVersion,
        Fault::ErrorBodyNotV0,
        Fault::AdvertiseWrongMaxInError,
        Fault::FlexibleHeaderOnV3,
        Fault::MetadataEmptyBrokers,
        Fault::MetadataUnrequestedTopic,
    ];
    for fault in isolated {
        let target = SENSITIVITY
            .iter()
            .find(|(f, _)| *f == fault)
            .map(|(_, t)| *t)
            .unwrap();
        let subject = SubjectServer::spawn(vec![fault]).await.unwrap();
        let report = checks::server::run(subject.addr()).await;
        for outcome in &report.outcomes {
            if outcome.id.0 == target {
                assert!(
                    matches!(outcome.verdict, Verdict::Fail { .. }),
                    "fault {fault:?} not detected:\n{report}"
                );
            } else {
                assert!(
                    matches!(outcome.verdict, Verdict::Pass),
                    "fault {fault:?} caused collateral non-pass in {}:\n{report}",
                    outcome.id
                );
            }
        }
    }
}

#[tokio::test]
async fn baseline_roundtrip_and_diff() {
    let subject = SubjectServer::spawn(vec![Fault::FlexibleHeaderOnV3])
        .await
        .unwrap();
    let report = checks::server::run(subject.addr()).await;

    // A run always matches the baseline derived from itself, even with a
    // known deviation recorded as an expected failure.
    let baseline = report.to_baseline();
    assert!(report.diff_against(&baseline).is_empty());
    let json = serde_json::to_string(&baseline).unwrap();
    let parsed = serde_json::from_str(&json).unwrap();
    assert_eq!(baseline, parsed);

    // Against the compliant subject's baseline the deviation surfaces.
    let compliant = SubjectServer::spawn(vec![]).await.unwrap();
    let compliant_report = checks::server::run(compliant.addr()).await;
    let diffs = report.diff_against(&compliant_report.to_baseline());
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(diffs[0].contains("api-versions/flexible-v3"), "{diffs:?}");
}
