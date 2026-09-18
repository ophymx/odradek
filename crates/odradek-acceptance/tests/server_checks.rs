//! Calibration of the server-side checks against the fault-injectable
//! subject: specificity (a conformant subject passes everything) and
//! sensitivity (each fault trips the check that claims to detect it).
//! The check catalog is the single source of truth: id lists and counts
//! here are derived from it, never restated.

use std::time::Duration;

use odradek_acceptance::checks::catalog;
use odradek_acceptance::checks::server::{self, ProbeConfig};
use odradek_acceptance::report::{BaselineDiff, BaselineStatus, FORMAT, Report};
use odradek_acceptance::subject::{Fault, SubjectServer};
use odradek_acceptance::{SubjectRole, Verdict};

/// Every subject here is in-process and answers instantly, so the default
/// settle budget — five seconds, right for a real broker electing a
/// leader for a fresh topic — is pure latency. Worse, a fault that answers
/// a *permanent* UNKNOWN_TOPIC_ID (which the flow treats as retriable)
/// burns the whole budget before reaching the correct verdict. Three
/// tenths of a second is still an order of magnitude more than these
/// subjects ever need.
async fn run(addr: &str) -> Report {
    let mut config = ProbeConfig::default();
    config.settle_budget = Duration::from_millis(300);
    // The reference subject speaks SASL on its only listener, so it is
    // its own SASL address. A real broker needs two, because a listener
    // without SASL cannot answer a question about mechanisms.
    server::run_with_sasl(addr, Some(addr), &config).await
}

fn server_check_ids() -> Vec<&'static str> {
    catalog()
        .filter(|c| c.role() == SubjectRole::Server)
        .map(|c| c.id)
        .collect()
}

#[tokio::test]
async fn compliant_subject_passes_all_checks() {
    let subject = SubjectServer::spawn(vec![]).await.unwrap();
    let report = run(subject.addr()).await;
    assert!(report.is_conformant(), "false positives:\n{report}");
    let expected = server_check_ids();
    assert_eq!(
        report.passed(),
        expected.len(),
        "expected every catalogued server check to run and pass:\n{report}"
    );
    // The report cites exactly the catalog, in catalog order.
    let ran: Vec<&str> = report.outcomes.iter().map(|o| o.id.0.as_str()).collect();
    assert_eq!(ran, expected);
}

/// fault → the check id that must detect it.
const SENSITIVITY: &[(Fault, &str)] = &[
    (Fault::WrongCorrelationEcho, "api-versions/correlation-echo"),
    (Fault::InvertedVersionRange, "api-versions/v0-basic"),
    (Fault::OmitApiVersionsKey, "api-versions/v0-basic"),
    (Fault::TrailingGarbage, "api-versions/v0-basic"),
    (Fault::FetchTrailingGarbage, "fetch/batch-integrity"),
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
    (Fault::ProduceWrongBaseOffset, "produce/basic"),
    (Fault::FetchCorruptBatch, "fetch/batch-integrity"),
    (Fault::ProduceTopicIdUnknown, "produce/topic-id"),
    (Fault::FetchWrongTopicId, "fetch/topic-id"),
    (
        Fault::ListOffsetsWrongEarliest,
        "list-offsets/earliest-latest",
    ),
    (Fault::FindCoordinatorWrongKey, "find-coordinator/group"),
    (
        Fault::OffsetFetchLosesCommit,
        "offsets/commit-fetch-roundtrip",
    ),
    (Fault::OffsetFetchUnsetIsZero, "offsets/unset-is-sentinel"),
    // Only detectable if the fetch check sweeps the advertised range: a
    // subject that is correct at its maximum version and wrong below it
    // passes any suite that negotiates once and stops.
    (Fault::FetchCorruptOnOldVersions, "fetch/batch-integrity"),
    (Fault::FetchPastEndSucceeds, "fetch/offset-out-of-range"),
    (Fault::MetadataUnknownTopicOmitted, "metadata/unknown-topic"),
    (
        Fault::CreateTopicsDuplicateSucceeds,
        "create-topics/duplicate",
    ),
    (
        Fault::CreateTopicsValidateOnlyCreates,
        "create-topics/validate-only",
    ),
    (
        Fault::JoinGroupAcceptsEmptyMemberId,
        "groups/member-id-required",
    ),
    (
        Fault::SyncGroupRewritesAssignment,
        "groups/assignment-round-trips",
    ),
    (
        Fault::GroupIgnoresGeneration,
        "groups/stale-generation-fenced",
    ),
    (
        Fault::ConsumerGroupEpochStuck,
        "consumer-group/epoch-advances",
    ),
    (
        Fault::ConsumerGroupAssignsNothing,
        "consumer-group/assigns-subscription",
    ),
    (
        Fault::ConsumerGroupNullSubscriptionRevokes,
        "consumer-group/omitted-subscription-is-unchanged",
    ),
    (
        Fault::ConsumerGroupIgnoresEpoch,
        "consumer-group/fenced-epoch",
    ),
    (
        Fault::SaslAuthenticateWithoutHandshake,
        "sasl/authenticate-requires-handshake",
    ),
    (
        Fault::SaslHandshakeHidesMechanisms,
        "sasl/refusal-names-mechanisms",
    ),
    (
        Fault::ScramNonceReplacesClients,
        "sasl/scram-nonce-extends-client",
    ),
    (Fault::ScramWeakIterations, "sasl/scram-iteration-floor"),
    (
        Fault::ScramSkipsServerSignature,
        "sasl/scram-server-proves-itself",
    ),
];

/// The calibration registry is exhaustive in both directions against the
/// catalog: a catalogued check without a fault that trips it is unproven
/// (vacuous until shown otherwise), a fault no check detects is dead
/// weight, and a registry entry naming an uncatalogued check is a typo.
#[test]
fn every_check_has_a_fault_and_every_fault_a_check() {
    let ids = server_check_ids();
    for id in &ids {
        assert!(
            SENSITIVITY.iter().any(|(_, t)| t == id),
            "check {id} has no fault in SENSITIVITY proving it detects anything"
        );
    }
    for fault in Fault::ALL {
        assert!(
            SENSITIVITY.iter().any(|(f, _)| f == fault),
            "fault {fault:?} is not mapped to a check in SENSITIVITY"
        );
    }
    for (fault, target) in SENSITIVITY {
        assert!(
            ids.contains(target),
            "SENSITIVITY maps {fault:?} to {target}, which is not in the catalog"
        );
    }
}

#[tokio::test]
async fn each_fault_trips_its_targeted_check() {
    for &(fault, target) in SENSITIVITY {
        let subject = SubjectServer::spawn(vec![fault]).await.unwrap();
        let report = run(subject.addr()).await;
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
        // The fetch flows deliberately ignore the assigned base offset, so
        // this stays confined to produce/basic. FetchCorruptBatch and
        // FetchTrailingGarbage are NOT isolated: both fetch checks read
        // the same corrupted/garbaged responses.
        Fault::ProduceWrongBaseOffset,
        // Each topic-id fault fires only on its own id-addressed path;
        // the name-addressed checks never see it.
        Fault::ProduceTopicIdUnknown,
        Fault::FetchWrongTopicId,
    ];
    for fault in isolated {
        let target = SENSITIVITY
            .iter()
            .find(|(f, _)| *f == fault)
            .map(|(_, t)| *t)
            .unwrap();
        let subject = SubjectServer::spawn(vec![fault]).await.unwrap();
        let report = run(subject.addr()).await;
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

/// A subject that cannot be reached yields infrastructure errors, not
/// protocol failures — and baseline enforcement labels them as such
/// instead of reporting a regression.
#[tokio::test]
async fn unreachable_subject_errors_instead_of_failing() {
    // Bind then drop: the port is free again, so connects are refused.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let report = run(&addr).await;
    assert_eq!(report.outcomes.len(), server_check_ids().len());
    for outcome in &report.outcomes {
        assert!(
            matches!(outcome.verdict, Verdict::Error { .. }),
            "{} against a dead address must be an infra error, got:\n{report}",
            outcome.id
        );
    }
    assert_eq!(report.errored(), report.outcomes.len());
    // No failures: nothing here says the subject violated the protocol.
    assert!(report.is_conformant());

    let compliant = SubjectServer::spawn(vec![]).await.unwrap();
    let baseline = run(compliant.addr()).await.to_baseline();
    let diffs = report.diff_against(&baseline);
    assert_eq!(diffs.len(), report.outcomes.len(), "{diffs:?}");
    for diff in &diffs {
        assert!(
            matches!(diff, BaselineDiff::Infrastructure { .. }),
            "an errored check must diff as infrastructure, got {diff:?}"
        );
    }
}

#[tokio::test]
async fn baseline_roundtrip_and_diff() {
    let subject = SubjectServer::spawn(vec![Fault::FlexibleHeaderOnV3])
        .await
        .unwrap();
    let report = run(subject.addr()).await;

    // A run always matches the baseline derived from itself, even with a
    // known deviation recorded as an expected failure.
    let baseline = report.to_baseline();
    assert!(report.diff_against(&baseline).is_empty());
    let json = serde_json::to_string(&baseline).unwrap();
    let parsed = serde_json::from_str(&json).unwrap();
    assert_eq!(baseline, parsed);

    // Against the compliant subject's baseline the deviation surfaces as
    // a regression (the check exists in both, its outcome changed).
    let compliant = SubjectServer::spawn(vec![]).await.unwrap();
    let compliant_report = run(compliant.addr()).await;
    let diffs = report.diff_against(&compliant_report.to_baseline());
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    match &diffs[0] {
        BaselineDiff::Regression {
            id,
            expected,
            actual,
        } => {
            assert_eq!(id, "api-versions/flexible-v3");
            assert_eq!(*expected, BaselineStatus::Pass);
            assert_eq!(*actual, BaselineStatus::Fail);
        }
        other => panic!("expected a regression diff, got {other:?}"),
    }
}

/// The three data cases baseline comparison must keep apart: a changed
/// outcome, a check the baseline predates, and a baseline entry for a
/// check that no longer exists.
#[tokio::test]
async fn baseline_diff_distinguishes_new_and_removed_checks() {
    let subject = SubjectServer::spawn(vec![]).await.unwrap();
    let report = run(subject.addr()).await;
    let mut baseline = report.to_baseline();
    let (known_id, _) = baseline.checks.pop_first().unwrap();
    baseline
        .checks
        .insert("ghost/no-such-check".into(), BaselineStatus::Pass);

    let diffs = report.diff_against(&baseline);
    assert_eq!(diffs.len(), 2, "{diffs:?}");
    assert!(
        diffs.iter().any(|d| matches!(
            d,
            BaselineDiff::NotInBaseline { id, actual: BaselineStatus::Pass } if *id == known_id
        )),
        "{diffs:?}"
    );
    assert!(
        diffs.iter().any(|d| matches!(
            d,
            BaselineDiff::RemovedCheck { id, .. } if id == "ghost/no-such-check"
        )),
        "{diffs:?}"
    );
}

/// Reports and baselines carry the versioned envelope, and reports
/// round-trip through their JSON form.
#[tokio::test]
async fn report_envelope_round_trips() {
    let subject = SubjectServer::spawn(vec![]).await.unwrap();
    let report = run(subject.addr()).await;
    assert_eq!(report.format, FORMAT);
    assert_eq!(report.suite, env!("CARGO_PKG_VERSION"));

    let parsed = Report::from_json(&report.to_json()).unwrap();
    assert_eq!(parsed, report);

    let baseline = report.to_baseline();
    assert_eq!(baseline.format, FORMAT);
    assert_eq!(baseline.suite, report.suite);

    // Pre-envelope files are refused with advice, not misread.
    let err = odradek_acceptance::report::Baseline::from_json(r#"{"checks":{}}"#).unwrap_err();
    assert!(err.contains("re-record"), "{err}");
    let err =
        odradek_acceptance::report::Baseline::from_json(r#"{"format":99,"suite":"?","checks":{}}"#)
            .unwrap_err();
    assert!(err.contains("format 99"), "{err}");
}
