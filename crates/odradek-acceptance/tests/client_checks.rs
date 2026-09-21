//! Calibration of the client-side checks: our own client must pass them
//! (dogfooding), and a deliberately misbehaving raw client must be caught.
//! Id lists and counts derive from the check catalog, never restated.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use odradek_acceptance::checks::catalog;
use odradek_acceptance::checks::client::{HarnessFault, ObserveConfig, ROUTING_TOPIC, run};
use odradek_acceptance::{SubjectRole, Verdict};
use odradek_client::{ClientConfig, Cluster, Connection, Consumer, Producer};
use odradek_protocol::header::request_header_version;
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::request_header::RequestHeader;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn client_check_ids() -> Vec<&'static str> {
    catalog()
        .filter(|c| c.role() == SubjectRole::Client)
        .map(|c| c.id)
        .collect()
}

/// The ids this file cites when asserting specific catches; a typo (or a
/// catalog rename this file missed) fails here rather than passing
/// vacuously.
const CITED: &[&str] = &[
    "client/header-well-formed",
    "client/starts-with-api-versions",
    "client/correlation-ids-unique",
    "client/respects-advertised-versions",
    "client/body-decodes",
    "client/routes-to-partition-leader",
    "client/recovers-from-leader-change",
    "client/heeds-sasl-rejection",
    "client/acknowledges-sasl-rejection",
    "client/honours-throttle-time",
    "client/tolerates-unknown-tagged-fields",
];

#[test]
fn cited_ids_are_catalogued() {
    let ids = client_check_ids();
    for id in CITED {
        assert!(ids.contains(id), "{id} is not in the catalog");
    }
}

fn config() -> ObserveConfig {
    let mut config = ObserveConfig::default();
    config.max_requests = 16;
    config.idle_timeout = Duration::from_millis(1500);
    config
}

fn produce_body(partition: i32, version: i16) -> Bytes {
    let mut partition_data = PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(Bytes::new());
    let mut topic_data = TopicProduceData::default();
    topic_data.name = ROUTING_TOPIC.into();
    topic_data.partition_data = vec![partition_data];
    let mut req = ProduceRequest::default();
    req.acks = -1;
    req.timeout_ms = 5_000;
    req.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    body.freeze()
}

/// The full dogfood: our cluster layer discovers the harness's fake
/// brokers, routes a produce to each partition's advertised leader, and
/// the consumer's fetch path routes the same way.
#[tokio::test]
async fn odradek_client_passes_the_client_checks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move { run(&listener, &config()).await });

    let mut client_config = ClientConfig::default();
    client_config.bootstrap_servers = vec![addr];
    client_config.client_id = "odradek".into();
    let cluster = Cluster::connect(client_config).await.unwrap();
    for partition in 0..3 {
        let broker = cluster
            .partition_leader(ROUTING_TOPIC, partition)
            .await
            .unwrap();
        let version = broker.ranges.pick(0, (3, 12)).unwrap();
        let body = produce_body(partition, version);
        broker.conn.request(0, version, &body).await.unwrap();
    }
    // The consumer rides the same cluster; the harness serves empty logs.
    let consumer = Consumer::new(cluster);
    let result = consumer.fetch(ROUTING_TOPIC, 2, 0).await.unwrap();
    assert!(result.records.is_empty());
    assert_eq!(result.next_offset, 0);
    drop(consumer); // close all connections so the observation session ends

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        report.is_conformant(),
        "our client is nonconformant:\n{report}"
    );
    // Every catalogued client check reports, and every one whose fault
    // is not armed here passes. Checking the ids rather than a count
    // means a new check cannot slip in as a silent skip.
    const FAULT_GATED: &[&str] = &[
        "client/recovers-from-leader-change",
        "client/honours-throttle-time",
        "client/heeds-sasl-rejection",
        "client/acknowledges-sasl-rejection",
        "client/tolerates-unknown-tagged-fields",
    ];
    for id in client_check_ids() {
        let verdict = report.verdict(id);
        if FAULT_GATED.contains(&id) {
            assert!(
                matches!(verdict, Some(Verdict::Skipped { .. })),
                "{id} needs its fault armed, so it should skip here:\n{report}"
            );
        } else {
            assert!(
                matches!(verdict, Some(Verdict::Pass)),
                "{id} should pass against our own client:\n{report}"
            );
        }
    }
}

#[tokio::test]
async fn misbehaving_client_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let harness = tokio::spawn(async move { run(&listener, &config()).await });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    // Sin #1: first request is Metadata, not ApiVersions.
    // Sin #2: the body is garbage.
    send_frame(&mut stream, &request_frame(3, 12, 7, &[0xff, 0xff, 0xff])).await;
    // Sin #3: version far outside anything advertised.
    // Sin #4: correlation id 7 reused.
    send_frame(&mut stream, &request_frame(3, 99, 7, &[0xff, 0xff, 0xff])).await;
    // Give the harness time to read both before we vanish.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);

    let report = harness.await.unwrap().expect("harness ran");
    for id in [
        "client/starts-with-api-versions",
        "client/correlation-ids-unique",
        "client/respects-advertised-versions",
        "client/body-decodes",
    ] {
        assert!(
            matches!(report.verdict(id), Some(Verdict::Fail { .. })),
            "{id} did not catch the misbehavior:\n{report}"
        );
    }
    // The headers themselves were well-formed; no false positive there.
    assert!(
        matches!(
            report.verdict("client/header-well-formed"),
            Some(Verdict::Pass)
        ),
        "false positive on header check:\n{report}"
    );
}

/// Recovery dogfood: the harness moves partition 0's leadership on the
/// first produce; our producer must refresh metadata and re-deliver to
/// the new leader.
#[tokio::test]
async fn odradek_client_recovers_from_leader_change() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::LeaderMove);
        run(&listener, &config).await
    });

    let mut client_config = ClientConfig::default();
    client_config.bootstrap_servers = vec![addr];
    client_config.client_id = "odradek".into();
    let cluster = Cluster::connect(client_config).await.unwrap();
    let mut producer = Producer::new(cluster);
    let offset = producer
        .produce(
            ROUTING_TOPIC,
            0,
            vec![odradek_protocol::records::Record {
                value: Some(Bytes::from_static(b"survives the move")),
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    assert_eq!(offset, 0);
    drop(producer);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/recovers-from-leader-change"),
            Some(Verdict::Pass)
        ),
        "recovery not credited:\n{report}"
    );
    assert!(report.is_conformant(), "{report}");
}

/// Recovery sensitivity: a client that gets NOT_LEADER and simply gives
/// up must be caught.
#[tokio::test]
async fn client_that_abandons_after_leader_change_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::LeaderMove);
        run(&listener, &config).await
    });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ranges = conn.negotiate().await.unwrap();
    let version = ranges.pick(0, (3, 12)).unwrap();
    // Correctly routed (partition 0 → broker 0), answered NOT_LEADER by
    // the staged move — and then this client just walks away.
    conn.request(0, version, &produce_body(0, version))
        .await
        .unwrap();
    drop(conn);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/recovers-from-leader-change"),
            Some(Verdict::Fail { .. })
        ),
        "abandonment not caught:\n{report}"
    );
}

/// Routing sensitivity: a client that negotiates properly but sends
/// partition 1's produce to broker 0 (the bootstrap) must be caught.
#[tokio::test]
async fn misrouted_produce_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move { run(&listener, &config()).await });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ranges = conn.negotiate().await.unwrap();
    let version = ranges.pick(0, (3, 12)).unwrap();
    // Partition 1 is led by broker 1, but this goes to the bootstrap.
    conn.request(0, version, &produce_body(1, version))
        .await
        .unwrap();
    drop(conn);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/routes-to-partition-leader"),
            Some(Verdict::Fail { .. })
        ),
        "misrouted produce not caught:\n{report}"
    );
    // Everything else about this client was fine — no collateral.
    for id in [
        "client/header-well-formed",
        "client/starts-with-api-versions",
        "client/correlation-ids-unique",
        "client/respects-advertised-versions",
        "client/body-decodes",
    ] {
        assert!(
            matches!(report.verdict(id), Some(Verdict::Pass)),
            "collateral failure in {id}:\n{report}"
        );
    }
}

/// A client that never dials is an infrastructure finding, not a report:
/// the harness's first accept has a deadline and times out cleanly.
#[tokio::test]
async fn harness_times_out_when_no_client_connects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config();
    config.accept_timeout = Duration::from_millis(200);
    let err = run(&listener, &config).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "{err}");
}

fn request_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut header = RequestHeader::default();
    header.request_api_key = api_key;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;
    header.client_id = Some("misbehaving".into());
    let mut payload = BytesMut::new();
    // The header version is a function of the api and its version, not
    // a constant. This said 2 unconditionally, from when every frame
    // here was a flexible Metadata; a SaslHandshake v1 takes a v1
    // header, and a v2 one puts an extra tagged-field byte in front of
    // the body. Nothing read these bodies, so nothing minded — until
    // the harness started answering the mechanism it was asked for.
    let header_version = request_header_version(api_key, api_version)
        .expect("the tests only send apis the protocol crate knows");
    header.encode(&mut payload, header_version).unwrap();
    payload.extend_from_slice(body);
    payload.to_vec()
}

async fn send_frame(stream: &mut TcpStream, payload: &[u8]) {
    let mut frame = BytesMut::new();
    frame.put_i32(i32::try_from(payload.len()).unwrap());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await.unwrap();
}

/// The odradek client heeds a SASL refusal.
///
/// The harness offers OAUTHBEARER and refuses the token the way RFC 7628
/// says to — a success-shaped response carrying the reason. A client
/// that reads only the error code sees zero, believes it authenticated,
/// and produces into a connection nobody authorized. This crate shipped
/// exactly that bug and caught it against a live broker; the check is
/// here so the next one is caught in CI.
#[tokio::test]
async fn odradek_client_heeds_a_sasl_refusal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::RejectSaslToken);
        run(&listener, &config).await
    });

    let mut client_config = ClientConfig::default();
    client_config.bootstrap_servers = vec![addr];
    client_config.client_id = "odradek".into();
    client_config.sasl = Some(odradek_client::SaslConfig::oauthbearer("a-token"));
    // The harness speaks plaintext; the point here is the refusal, not
    // the transport.
    client_config.allow_plaintext_credentials = true;

    // Connecting must fail: the token was refused.
    let outcome = Cluster::connect(client_config).await;
    assert!(
        outcome.is_err(),
        "a refused token must not produce a usable cluster handle"
    );

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/heeds-sasl-rejection"),
            Some(Verdict::Pass)
        ),
        "{report}"
    );
    assert!(
        matches!(
            report.verdict("client/acknowledges-sasl-rejection"),
            Some(Verdict::Pass)
        ),
        "{report}"
    );
}

/// And a client that ignores the refusal is caught.
///
/// This is the calibration: the checks above are only worth something if
/// a client that barrels on fails them. This one authenticates, is
/// refused, says nothing about it, and produces anyway.
#[tokio::test]
async fn a_client_that_ignores_a_sasl_refusal_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::RejectSaslToken);
        run(&listener, &config).await
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    // ApiVersions, so the harness sees a well-formed opening.
    send_frame(&mut stream, &request_frame(18, 0, 1, &[])).await;
    // SaslHandshake("OAUTHBEARER").
    let mut handshake = Vec::new();
    handshake.extend_from_slice(&(11i16).to_be_bytes());
    handshake.extend_from_slice(b"OAUTHBEARER");
    send_frame(&mut stream, &request_frame(17, 1, 2, &handshake)).await;
    // A token, which the harness refuses.
    let token = b"n,,\x01auth=Bearer nope\x01\x01";
    let mut auth = Vec::new();
    auth.extend_from_slice(&i32::try_from(token.len()).unwrap().to_be_bytes());
    auth.extend_from_slice(token);
    send_frame(&mut stream, &request_frame(36, 1, 3, &auth)).await;
    // Sin: no acknowledgement, and a data api on a connection that was
    // just told no.
    send_frame(
        &mut stream,
        &request_frame(3, 12, 4, &[0x00, 0x00, 0x00, 0x00, 0x00]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/heeds-sasl-rejection"),
            Some(Verdict::Fail { .. })
        ),
        "producing after a refusal must be caught:\n{report}"
    );
    assert!(
        matches!(
            report.verdict("client/acknowledges-sasl-rejection"),
            Some(Verdict::Fail { .. })
        ),
        "an unacknowledged refusal must be caught:\n{report}"
    );
}

/// The odradek client waits out a throttle.
///
/// The check that found this gap: before it, the client read
/// `throttle_time_ms` from exactly nothing and pushed straight on into
/// a broker that had stopped reading.
#[tokio::test]
async fn odradek_client_waits_out_a_throttle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::Throttle);
        run(&listener, &config).await
    });

    let mut client_config = ClientConfig::default();
    client_config.bootstrap_servers = vec![addr];
    client_config.client_id = "odradek".into();
    let cluster = Cluster::connect(client_config).await.unwrap();
    // Produced, not metadata-refreshed. Either drives requests down one
    // connection, but only the produce path carries the thing the check
    // now requires before it will call a gap a pause: a client that is
    // demonstrably mid-stream. A metadata refresh loop settled the old
    // version of this check and would settle nothing now — which is the
    // point, since a client idle between metadata calls looks exactly
    // like one honouring a throttle.
    let mut producer = Producer::new(cluster.clone());
    // Producing in a loop for longer than the throttle window is what
    // makes this observable. A client that waits sends a handful of
    // requests spaced a window apart, and is silent through the middle
    // of each one; a client that does not is talking the whole time,
    // and the check is looking at exactly that middle stretch. Without
    // the loop the test passes either way — its first version did.
    let until = std::time::Instant::now() + Duration::from_millis(900);
    while std::time::Instant::now() < until {
        let record = odradek_protocol::records::Record {
            value: Some(Bytes::from_static(b"throttled")),
            ..Default::default()
        };
        producer
            .produce(ROUTING_TOPIC, 0, vec![record])
            .await
            .unwrap();
    }
    drop(producer);
    drop(cluster);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/honours-throttle-time"),
            Some(Verdict::Pass)
        ),
        "{report}"
    );
}

/// And a client that ignores the throttle is caught.
#[tokio::test]
async fn a_client_that_ignores_a_throttle_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::Throttle);
        run(&listener, &config).await
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &request_frame(18, 0, 1, &[])).await;
    // Metadata v9 naming no topics: an empty compact array, the
    // auto-create flag, and the tagged-field section.
    send_frame(
        &mut stream,
        &request_frame(3, 9, 2, &[0x01, 0x00, 0x00, 0x00]),
    )
    .await;
    // A request while the pause is still running, and late enough that
    // nothing could still be crossing in flight: the sin is talking
    // *during* the window, not having had something already on the
    // wire when it started.
    tokio::time::sleep(Duration::from_millis(250)).await;
    send_frame(
        &mut stream,
        &request_frame(3, 9, 3, &[0x01, 0x00, 0x00, 0x00]),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/honours-throttle-time"),
            Some(Verdict::Fail { .. })
        ),
        "ignoring a throttle must be caught:\n{report}"
    );
}

/// The odradek client carries on past a field from the future.
///
/// The protocol crate keeps unknown tagged fields rather than refusing
/// them, and this is that promise observed from outside: a response
/// arrives carrying a tag no schema defines, and the client goes on
/// working.
#[tokio::test]
async fn odradek_client_tolerates_a_field_from_the_future() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::UnknownTaggedField);
        run(&listener, &config).await
    });

    let mut client_config = ClientConfig::default();
    client_config.bootstrap_servers = vec![addr];
    client_config.client_id = "odradek".into();
    let cluster = Cluster::connect(client_config).await.unwrap();
    let mut producer = Producer::new(cluster);
    producer
        .produce(
            ROUTING_TOPIC,
            0,
            vec![odradek_protocol::records::Record {
                value: Some(Bytes::from_static(b"undeterred")),
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    drop(producer);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/tolerates-unknown-tagged-fields"),
            Some(Verdict::Pass)
        ),
        "{report}"
    );
}

/// And a client that gives up at the unknown field is caught.
#[tokio::test]
async fn a_client_that_stops_at_an_unknown_field_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let harness = tokio::spawn(async move {
        let mut config = config();
        config.fault = Some(HarnessFault::UnknownTaggedField);
        run(&listener, &config).await
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    send_frame(&mut stream, &request_frame(18, 0, 1, &[])).await;
    // Metadata v9 naming no topics; the answer carries the unknown tag.
    send_frame(
        &mut stream,
        &request_frame(3, 9, 2, &[0x01, 0x00, 0x00, 0x00]),
    )
    .await;
    // And then nothing: the client that could not cope.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);

    let report = harness.await.unwrap().expect("harness ran");
    assert!(
        matches!(
            report.verdict("client/tolerates-unknown-tagged-fields"),
            Some(Verdict::Fail { .. })
        ),
        "giving up at an unknown tagged field must be caught:\n{report}"
    );
}
