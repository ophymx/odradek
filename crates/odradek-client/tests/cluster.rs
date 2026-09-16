//! Cluster-layer tests against an in-process fake multi-broker cluster:
//! metadata discovery, per-broker connections, and leader routing.

use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use odradek_client::{ClientConfig, ClientError, Cluster};
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const TOPIC: &str = "routing";

/// Which broker each produced partition arrived at.
type Arrivals = Arc<Mutex<Vec<(i32, i32)>>>; // (broker node_id, partition)

struct FakeCluster {
    /// node_id -> host:port
    endpoints: Vec<(i32, String)>,
    arrivals: Arrivals,
}

/// Spawn `n` fake brokers. Every broker answers ApiVersions and full
/// cluster Metadata (topic `routing`, one partition per broker, partition
/// i led by node i — except leaderless ones listed in `no_leader`), and
/// records which node each Produce partition arrives at.
async fn spawn_fake_cluster(n: i32, no_leader: &'static [i32]) -> FakeCluster {
    let mut listeners = Vec::new();
    let mut endpoints = Vec::new();
    for node_id in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push((node_id, listener.local_addr().unwrap().to_string()));
        listeners.push((node_id, listener));
    }
    let arrivals: Arrivals = Arc::new(Mutex::new(Vec::new()));

    for (node_id, listener) in listeners {
        let endpoints = endpoints.clone();
        let arrivals = Arc::clone(&arrivals);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve_conn(
                    stream,
                    node_id,
                    endpoints.clone(),
                    Arc::clone(&arrivals),
                    no_leader,
                ));
            }
        });
    }
    FakeCluster {
        endpoints,
        arrivals,
    }
}

async fn serve_conn(
    mut stream: TcpStream,
    node_id: i32,
    endpoints: Vec<(i32, String)>,
    arrivals: Arrivals,
    no_leader: &'static [i32],
) {
    loop {
        let mut len_bytes = [0u8; 4];
        if stream.read_exact(&mut len_bytes).await.is_err() {
            return;
        }
        let len = i32::from_be_bytes(len_bytes).max(0) as usize;
        let mut frame = vec![0u8; len];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let mut frame = Bytes::from(frame);
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let hv = request_header_version(api_key, api_version).unwrap();
        let header = RequestHeader::decode(&mut frame, hv).unwrap();

        let body = match api_key {
            18 => {
                let resp = ApiVersionsResponse {
                    api_keys: [(18, 0, 4), (3, 0, 13), (0, 3, 12), (1, 4, 17), (2, 1, 10)]
                        .into_iter()
                        .map(|(api_key, min_version, max_version)| ApiVersion {
                            api_key,
                            min_version,
                            max_version,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            3 => {
                let resp = MetadataResponse {
                    brokers: endpoints
                        .iter()
                        .map(|(id, addr)| {
                            let (host, port) = addr.rsplit_once(':').unwrap();
                            MetadataResponseBroker {
                                node_id: *id,
                                host: host.into(),
                                port: port.parse().unwrap(),
                                ..Default::default()
                            }
                        })
                        .collect(),
                    cluster_id: Some("fake-cluster".into()),
                    controller_id: 0,
                    topics: vec![MetadataResponseTopic {
                        name: Some(TOPIC.into()),
                        partitions: endpoints
                            .iter()
                            .map(|(id, _)| MetadataResponsePartition {
                                partition_index: *id,
                                leader_id: if no_leader.contains(id) { -1 } else { *id },
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            0 => {
                use odradek_protocol::messages::produce_response::{
                    PartitionProduceResponse, TopicProduceResponse,
                };
                let produce = ProduceRequest::decode(&mut frame, api_version).unwrap();
                let mut responses = Vec::new();
                for topic in &produce.topic_data {
                    assert_eq!(topic.name, TOPIC);
                    let mut partitions = Vec::new();
                    for p in &topic.partition_data {
                        arrivals.lock().unwrap().push((node_id, p.index));
                        partitions.push(PartitionProduceResponse {
                            index: p.index,
                            error_code: 0,
                            base_offset: 7,
                            log_append_time_ms: -1,
                            ..Default::default()
                        });
                    }
                    responses.push(TopicProduceResponse {
                        name: topic.name.clone(),
                        partition_responses: partitions,
                        ..Default::default()
                    });
                }
                let mut buf = BytesMut::new();
                ProduceResponse {
                    responses,
                    ..Default::default()
                }
                .encode(&mut buf, api_version)
                .unwrap();
                buf.freeze()
            }
            1 => {
                use odradek_protocol::messages::fetch_request::FetchRequest;
                use odradek_protocol::messages::fetch_response::{
                    FetchResponse, FetchableTopicResponse, PartitionData,
                };
                let fetch = FetchRequest::decode(&mut frame, api_version).unwrap();
                let topic = &fetch.topics[0];
                assert_eq!(topic.topic, TOPIC);
                let resp = FetchResponse {
                    responses: vec![FetchableTopicResponse {
                        topic: TOPIC.into(),
                        partitions: vec![PartitionData {
                            partition_index: topic.partitions[0].partition,
                            error_code: 0,
                            high_watermark: 9,
                            last_stable_offset: 9,
                            log_start_offset: 5,
                            records: Some(fake_log()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            2 => {
                use odradek_protocol::messages::list_offsets_request::ListOffsetsRequest;
                use odradek_protocol::messages::list_offsets_response::{
                    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
                };
                let req = ListOffsetsRequest::decode(&mut frame, api_version).unwrap();
                let partition = &req.topics[0].partitions[0];
                let offset = match partition.timestamp {
                    -2 => 5, // earliest: the log start
                    -1 => 9, // latest: the log end
                    other => panic!("fake broker got list offsets timestamp {other}"),
                };
                let resp = ListOffsetsResponse {
                    topics: vec![ListOffsetsTopicResponse {
                        name: TOPIC.into(),
                        partitions: vec![ListOffsetsPartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: 0,
                            timestamp: -1,
                            offset,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            other => panic!("fake broker got api key {other}"),
        };

        let resp_header = ResponseHeader {
            correlation_id: header.correlation_id,
            unknown_tagged_fields: Vec::new(),
        };
        let mut out = BytesMut::new();
        out.put_i32(0);
        resp_header
            .encode(
                &mut out,
                response_header_version(api_key, api_version).unwrap(),
            )
            .unwrap();
        out.extend_from_slice(&body);
        let frame_len = i32::try_from(out.len() - 4).unwrap();
        out[..4].copy_from_slice(&frame_len.to_be_bytes());
        if stream.write_all(&out).await.is_err() {
            return;
        }
    }
}

fn config_for(cluster: &FakeCluster) -> ClientConfig {
    ClientConfig {
        bootstrap_servers: vec![cluster.endpoints[0].1.clone()],
        client_id: "odradek".into(),
    }
}

fn probe_produce_body(partition: i32, version: i16) -> Bytes {
    use odradek_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    let req = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.into(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(Bytes::new()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    body.freeze()
}

#[tokio::test]
async fn produce_routes_to_each_partition_leader() {
    let fake = spawn_fake_cluster(3, &[]).await;
    let mut cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    // No explicit refresh: the first leader lookup must fetch metadata
    // by itself.
    for partition in 0..3 {
        let broker = cluster.partition_leader(TOPIC, partition).await.unwrap();
        let version = broker.ranges.pick(0, (3, 12)).unwrap();
        let body = probe_produce_body(partition, version);
        broker.conn.request(0, version, &body).await.unwrap();
    }

    let mut arrivals = fake.arrivals.lock().unwrap().clone();
    arrivals.sort_unstable();
    assert_eq!(arrivals, vec![(0, 0), (1, 1), (2, 2)]);

    // The cached view agrees.
    assert_eq!(cluster.brokers().count(), 3);
    assert_eq!(cluster.partitions(TOPIC).unwrap().len(), 3);
    assert_eq!(cluster.leader_id(TOPIC, 2), Some(2));
}

#[tokio::test]
async fn leaderless_partition_is_an_error_not_a_guess() {
    let fake = spawn_fake_cluster(2, &[1]).await;
    let mut cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    // Partition 0 routes; partition 1 has leader -1 and must error.
    assert!(cluster.partition_leader(TOPIC, 0).await.is_ok());
    match cluster.partition_leader(TOPIC, 1).await {
        Err(ClientError::UnknownLeader { topic, partition }) => {
            assert_eq!(topic, TOPIC);
            assert_eq!(partition, 1);
        }
        other => panic!("expected UnknownLeader, got {other:?}"),
    }
}

/// The fake partition log: a control batch at offset 5 (transaction
/// marker — not data), then a data batch with offsets 6-8.
fn fake_log() -> Bytes {
    use odradek_protocol::records::{Record, RecordBatch, Records, encode_set};
    let control = RecordBatch {
        base_offset: 5,
        attributes: 1 << 5, // control
        base_timestamp: 900,
        max_timestamp: 900,
        records: Records::Plain(vec![Record {
            value: Some(Bytes::from_static(b"\0\0\0\0")),
            ..Default::default()
        }]),
        ..Default::default()
    };
    let data = RecordBatch {
        base_offset: 6,
        last_offset_delta: 2,
        base_timestamp: 1_000,
        max_timestamp: 1_002,
        records: Records::Plain(
            [b"a", b"b", b"c"]
                .iter()
                .enumerate()
                .map(|(i, v)| Record {
                    offset_delta: i32::try_from(i).unwrap(),
                    timestamp_delta: i64::try_from(i).unwrap(),
                    value: Some(Bytes::from_static(*v)),
                    ..Default::default()
                })
                .collect(),
        ),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_set(&mut buf, &[control, data]).unwrap();
    buf.freeze()
}

#[tokio::test]
async fn consumer_fetches_from_an_offset_and_skips_noise() {
    use odradek_client::Consumer;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut consumer = Consumer::new(cluster);

    // Fetching from 7: the control batch and record 6 are not data the
    // caller asked for; absolute offsets and timestamps are materialized.
    let result = consumer.fetch(TOPIC, 1, 7).await.unwrap();
    let got: Vec<(i64, i64, &[u8])> = result
        .records
        .iter()
        .map(|r| (r.offset, r.timestamp, r.value.as_deref().unwrap()))
        .collect();
    assert_eq!(got, vec![(7, 1_001, b"b".as_slice()), (8, 1_002, b"c")]);
    assert_eq!(result.next_offset, 9);
    assert_eq!(result.high_watermark, 9);

    assert_eq!(consumer.earliest_offset(TOPIC, 1).await.unwrap(), 5);
    assert_eq!(consumer.latest_offset(TOPIC, 1).await.unwrap(), 9);
}

#[tokio::test]
async fn producer_delivers_a_batch_and_returns_the_offset() {
    use odradek_client::Producer;
    use odradek_protocol::records::Record;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut producer = Producer::new(cluster);
    let offset = producer
        .produce(
            TOPIC,
            2,
            vec![Record {
                key: Some(Bytes::from_static(b"k")),
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    assert_eq!(offset, 7); // the fake's fixed base offset
    assert!(fake.arrivals.lock().unwrap().contains(&(2, 2)));
}

#[tokio::test]
async fn bootstrap_falls_through_dead_servers() {
    let fake = spawn_fake_cluster(1, &[]).await;
    // First server is a dead port (bound then dropped), second is live.
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);
    let config = ClientConfig {
        bootstrap_servers: vec![dead_addr, fake.endpoints[0].1.clone()],
        client_id: "odradek".into(),
    };
    let mut cluster = Cluster::connect(config).await.unwrap();
    cluster.refresh_metadata(&[TOPIC]).await.unwrap();
    assert_eq!(cluster.brokers().count(), 1);
}
