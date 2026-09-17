//! Calibration of the client-side checks: our own client must pass them
//! (dogfooding), and a deliberately misbehaving raw client must be caught.

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use odradek_acceptance::Verdict;
use odradek_acceptance::checks::client::{HarnessFault, ObserveConfig, ROUTING_TOPIC, run};
use odradek_client::{ClientConfig, Cluster, Connection, Consumer, Producer};
use odradek_protocol::messages::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use odradek_protocol::messages::request_header::RequestHeader;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

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

    let report = harness.await.unwrap();
    assert!(
        report.is_conformant(),
        "our client is nonconformant:\n{report}"
    );
    assert_eq!(
        report.passed(),
        6,
        "expected every client check to pass:\n{report}"
    );
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

    let report = harness.await.unwrap();
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

    let report = harness.await.unwrap();
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

    let report = harness.await.unwrap();
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

    let report = harness.await.unwrap();
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

fn request_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut header = RequestHeader::default();
    header.request_api_key = api_key;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;
    header.client_id = Some("misbehaving".into());
    let mut payload = BytesMut::new();
    // Metadata v9+ is flexible → header v2 for both frames.
    header.encode(&mut payload, 2).unwrap();
    payload.extend_from_slice(body);
    payload.to_vec()
}

async fn send_frame(stream: &mut TcpStream, payload: &[u8]) {
    let mut frame = BytesMut::new();
    frame.put_i32(i32::try_from(payload.len()).unwrap());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await.unwrap();
}
