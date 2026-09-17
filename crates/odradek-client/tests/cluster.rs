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

/// Committed offsets: (group, topic, partition) -> (offset, committed at
/// broker node_id).
type Offsets = Arc<Mutex<std::collections::HashMap<(String, String, i32), (i64, i32)>>>;

/// The fake group coordinator's node id.
const COORDINATOR: i32 = 1;

/// Stored record sets per (topic, partition), verbatim as produced.
type Logs = Arc<Mutex<std::collections::HashMap<(String, i32), BytesMut>>>;

/// One consumer group's coordinator state (a single group suffices).
#[derive(Default)]
struct GroupState {
    next_member: u32,
    generation: i32,
    rebalancing: bool,
    /// member id -> subscription metadata, current generation.
    members: Vec<(String, Bytes)>,
    /// member id -> assignment bytes, from the leader's SyncGroup.
    assignments: std::collections::HashMap<String, Bytes>,
}
type Group = Arc<Mutex<GroupState>>;

struct FakeCluster {
    /// node_id -> host:port
    endpoints: Vec<(i32, String)>,
    arrivals: Arrivals,
    offsets: Offsets,
    logs: Logs,
    group: Group,
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
    let offsets: Offsets = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let logs: Logs = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let group: Group = Arc::new(Mutex::new(GroupState::default()));

    for (node_id, listener) in listeners {
        let endpoints = endpoints.clone();
        let arrivals = Arc::clone(&arrivals);
        let offsets = Arc::clone(&offsets);
        let logs = Arc::clone(&logs);
        let group = Arc::clone(&group);
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
                    Arc::clone(&offsets),
                    Arc::clone(&logs),
                    Arc::clone(&group),
                    no_leader,
                ));
            }
        });
    }
    FakeCluster {
        endpoints,
        arrivals,
        offsets,
        logs,
        group,
    }
}

// One parameter per piece of shared broker state; a config struct would
// just rename the problem in a test fake.
#[allow(clippy::too_many_arguments)]
async fn serve_conn(
    mut stream: TcpStream,
    node_id: i32,
    endpoints: Vec<(i32, String)>,
    arrivals: Arrivals,
    offsets: Offsets,
    logs: Logs,
    group: Group,
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
                    api_keys: [
                        (18, 0, 4),
                        (3, 0, 13),
                        (0, 3, 12),
                        (1, 4, 17),
                        (2, 1, 10),
                        (10, 0, 6),
                        (8, 2, 10),
                        (9, 1, 10),
                        (11, 4, 9),
                        (14, 3, 5),
                        (12, 0, 4),
                        (13, 0, 5),
                    ]
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
                        if let Some(records) = &p.records {
                            logs.lock()
                                .unwrap()
                                .entry((topic.name.clone(), p.index))
                                .or_default()
                                .extend_from_slice(records);
                        }
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
                let partition = topic.partitions[0].partition;
                // Serve whatever was produced; the canned log otherwise.
                let stored = logs
                    .lock()
                    .unwrap()
                    .get(&(TOPIC.to_owned(), partition))
                    .map(|b| b.clone().freeze());
                let resp = FetchResponse {
                    responses: vec![FetchableTopicResponse {
                        topic: TOPIC.into(),
                        partitions: vec![PartitionData {
                            partition_index: partition,
                            error_code: 0,
                            high_watermark: 9,
                            last_stable_offset: 9,
                            log_start_offset: 5,
                            records: Some(stored.unwrap_or_else(fake_log)),
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
            10 => {
                use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
                use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
                let req = FindCoordinatorRequest::decode(&mut frame, api_version).unwrap();
                assert!(!req.key.is_empty());
                // Broker COORDINATOR when the cluster has one, else the
                // last broker there is.
                let coord = endpoints
                    .get(COORDINATOR as usize)
                    .unwrap_or_else(|| endpoints.last().unwrap());
                let (host, port) = coord.1.rsplit_once(':').unwrap();
                let resp = FindCoordinatorResponse {
                    error_code: 0,
                    node_id: coord.0,
                    host: host.into(),
                    port: port.parse().unwrap(),
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            8 => {
                use odradek_protocol::messages::offset_commit_request::OffsetCommitRequest;
                use odradek_protocol::messages::offset_commit_response::{
                    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
                };
                let req = OffsetCommitRequest::decode(&mut frame, api_version).unwrap();
                assert_eq!(req.generation_id_or_member_epoch, -1);
                assert!(req.member_id.is_empty());
                let topic = &req.topics[0];
                let p = &topic.partitions[0];
                offsets.lock().unwrap().insert(
                    (req.group_id.clone(), topic.name.clone(), p.partition_index),
                    (p.committed_offset, node_id),
                );
                let resp = OffsetCommitResponse {
                    topics: vec![OffsetCommitResponseTopic {
                        name: topic.name.clone(),
                        partitions: vec![OffsetCommitResponsePartition {
                            partition_index: p.partition_index,
                            error_code: 0,
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
            9 => {
                use odradek_protocol::messages::offset_fetch_request::OffsetFetchRequest;
                use odradek_protocol::messages::offset_fetch_response::{
                    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
                };
                let req = OffsetFetchRequest::decode(&mut frame, api_version).unwrap();
                let topic = &req.topics.as_ref().unwrap()[0];
                let partition = topic.partition_indexes[0];
                let committed = offsets
                    .lock()
                    .unwrap()
                    .get(&(req.group_id.clone(), topic.name.clone(), partition))
                    .map_or(-1, |(offset, _)| *offset);
                let resp = OffsetFetchResponse {
                    topics: vec![OffsetFetchResponseTopic {
                        name: topic.name.clone(),
                        partitions: vec![OffsetFetchResponsePartition {
                            partition_index: partition,
                            committed_offset: committed,
                            committed_leader_epoch: -1,
                            metadata: Some(String::new()),
                            error_code: 0,
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
            11 => {
                use odradek_protocol::messages::join_group_request::JoinGroupRequest;
                use odradek_protocol::messages::join_group_response::{
                    JoinGroupResponse, JoinGroupResponseMember,
                };
                let req = JoinGroupRequest::decode(&mut frame, api_version).unwrap();
                assert_eq!(req.protocol_type, "consumer");
                assert_eq!(req.protocols[0].name, "range");
                let mut state = group.lock().unwrap();
                let resp = if req.member_id.is_empty() {
                    state.next_member += 1;
                    JoinGroupResponse {
                        error_code: 79, // MEMBER_ID_REQUIRED
                        member_id: format!("member-{}", state.next_member),
                        ..Default::default()
                    }
                } else {
                    state.rebalancing = false;
                    state.generation += 1;
                    state.members =
                        vec![(req.member_id.clone(), req.protocols[0].metadata.clone())];
                    JoinGroupResponse {
                        error_code: 0,
                        generation_id: state.generation,
                        protocol_name: Some("range".into()),
                        leader: req.member_id.clone(),
                        member_id: req.member_id.clone(),
                        members: state
                            .members
                            .iter()
                            .map(|(id, meta)| JoinGroupResponseMember {
                                member_id: id.clone(),
                                metadata: meta.clone(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            14 => {
                use odradek_protocol::messages::sync_group_request::SyncGroupRequest;
                use odradek_protocol::messages::sync_group_response::SyncGroupResponse;
                let req = SyncGroupRequest::decode(&mut frame, api_version).unwrap();
                let mut state = group.lock().unwrap();
                assert_eq!(req.generation_id, state.generation);
                for a in &req.assignments {
                    state
                        .assignments
                        .insert(a.member_id.clone(), a.assignment.clone());
                }
                let resp = SyncGroupResponse {
                    error_code: 0,
                    assignment: state
                        .assignments
                        .get(&req.member_id)
                        .cloned()
                        .unwrap_or_default(),
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            12 => {
                use odradek_protocol::messages::heartbeat_request::HeartbeatRequest;
                use odradek_protocol::messages::heartbeat_response::HeartbeatResponse;
                let req = HeartbeatRequest::decode(&mut frame, api_version).unwrap();
                let state = group.lock().unwrap();
                let known = state.members.iter().any(|(id, _)| *id == req.member_id);
                let resp = HeartbeatResponse {
                    error_code: if !known {
                        25 // UNKNOWN_MEMBER_ID
                    } else if state.rebalancing {
                        27 // REBALANCE_IN_PROGRESS
                    } else {
                        0
                    },
                    ..Default::default()
                };
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            13 => {
                use odradek_protocol::messages::leave_group_request::LeaveGroupRequest;
                use odradek_protocol::messages::leave_group_response::LeaveGroupResponse;
                let _ = LeaveGroupRequest::decode(&mut frame, api_version).unwrap();
                group.lock().unwrap().members.clear();
                let mut buf = BytesMut::new();
                LeaveGroupResponse::default()
                    .encode(&mut buf, api_version)
                    .unwrap();
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
        ..Default::default()
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
async fn enqueue_batches_until_flush_or_size_trigger() {
    use odradek_client::{Producer, ProducerConfig};
    use odradek_protocol::records::Record;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut producer = Producer::with_config(
        cluster,
        ProducerConfig {
            batch_max_bytes: 200,
            ..Default::default()
        },
    );
    let record = |v: &'static str| Record {
        value: Some(Bytes::from_static(v.as_bytes())),
        ..Default::default()
    };

    // Two small records buffer without delivering.
    assert!(
        producer
            .enqueue(TOPIC, 0, record("one"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        producer
            .enqueue(TOPIC, 1, record("two"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(producer.buffered(), 2);
    assert!(fake.arrivals.lock().unwrap().is_empty());

    // Flush delivers one batch per partition, to each partition's leader.
    let deliveries = producer.flush().await.unwrap();
    assert_eq!(deliveries.len(), 2);
    assert!(deliveries.iter().all(|d| d.records == 1));
    assert_eq!(producer.buffered(), 0);
    let mut arrivals = fake.arrivals.lock().unwrap().clone();
    arrivals.sort_unstable();
    assert_eq!(arrivals, vec![(0, 0), (1, 1)]);

    // A fat record blows the 200-byte threshold: immediate delivery of
    // the partition's whole buffer as one batch.
    producer.enqueue(TOPIC, 2, record("small")).await.unwrap();
    let fat = Record {
        value: Some(Bytes::from(vec![0x55; 300])),
        ..Default::default()
    };
    let delivery = producer.enqueue(TOPIC, 2, fat).await.unwrap().unwrap();
    assert_eq!(delivery.records, 2);
    assert_eq!(producer.buffered(), 0);
}

#[tokio::test]
async fn compressed_batches_roundtrip_end_to_end() {
    use odradek_client::{Consumer, Producer, ProducerConfig};
    use odradek_protocol::records::{Compression, Record, decode_set};

    for codec in [Compression::Gzip, Compression::Lz4] {
        let fake = spawn_fake_cluster(1, &[]).await;
        let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
        let mut producer = Producer::with_config(
            cluster,
            ProducerConfig {
                compression: codec,
                ..Default::default()
            },
        );
        for i in 0..3 {
            producer
                .enqueue(
                    TOPIC,
                    0,
                    Record {
                        key: Some(Bytes::from(format!("k{i}"))),
                        value: Some(Bytes::from(format!("compressed value {i}"))),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        producer.flush().await.unwrap();

        // The wire really carried a compressed batch...
        let stored = fake.logs.lock().unwrap()[&(TOPIC.to_owned(), 0)]
            .clone()
            .freeze();
        let batches = decode_set(&mut stored.clone()).unwrap();
        assert_eq!(batches.len(), 1, "{codec:?}");
        assert_eq!(batches[0].compression(), codec);

        // ...and the consumer materializes it back into the records.
        let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
        let mut consumer = Consumer::new(cluster);
        let result = consumer.fetch(TOPIC, 0, 0).await.unwrap();
        let values: Vec<String> = result
            .records
            .iter()
            .map(|r| String::from_utf8(r.value.clone().unwrap().to_vec()).unwrap())
            .collect();
        assert_eq!(
            values,
            vec![
                "compressed value 0",
                "compressed value 1",
                "compressed value 2"
            ],
            "{codec:?}"
        );
    }
}

#[tokio::test]
async fn offsets_commit_through_the_coordinator_and_read_back() {
    use odradek_client::Consumer;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut consumer = Consumer::new(cluster);

    // Nothing committed yet.
    assert_eq!(
        consumer.committed_offset("g1", TOPIC, 2).await.unwrap(),
        None
    );

    consumer.commit_offset("g1", TOPIC, 2, 41).await.unwrap();
    assert_eq!(
        consumer.committed_offset("g1", TOPIC, 2).await.unwrap(),
        Some(41)
    );
    // Other groups and partitions stay independent.
    assert_eq!(
        consumer.committed_offset("g2", TOPIC, 2).await.unwrap(),
        None
    );
    assert_eq!(
        consumer.committed_offset("g1", TOPIC, 0).await.unwrap(),
        None
    );

    // The commit went to the coordinator FindCoordinator named — not to
    // the bootstrap broker the consumer happened to be connected to.
    let (offset, committed_at) = fake.offsets.lock().unwrap()[&("g1".into(), TOPIC.into(), 2)];
    assert_eq!(offset, 41);
    assert_eq!(committed_at, COORDINATOR);
}

#[tokio::test]
async fn group_membership_join_heartbeat_rebalance_leave() {
    use odradek_client::{GroupConfig, GroupMember, HeartbeatStatus};

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    // Join runs the MEMBER_ID_REQUIRED dance, elects us leader (sole
    // member), and the leader's range assignment covers every partition.
    let mut member = GroupMember::join(cluster, "g1", &[TOPIC], GroupConfig::default())
        .await
        .unwrap();
    assert_eq!(member.member_id(), "member-1");
    assert!(member.is_leader());
    assert_eq!(member.generation_id(), 1);
    assert_eq!(
        member.assignment(),
        &[(TOPIC.to_owned(), vec![0, 1, 2])],
        "sole member owns all partitions"
    );

    assert_eq!(member.heartbeat().await.unwrap(), HeartbeatStatus::Stable);

    // The coordinator starts a rebalance; the heartbeat reports it and a
    // rejoin lands in the next generation with a fresh assignment.
    fake.group.lock().unwrap().rebalancing = true;
    assert_eq!(
        member.heartbeat().await.unwrap(),
        HeartbeatStatus::RebalanceInProgress
    );
    member.rejoin().await.unwrap();
    assert_eq!(member.generation_id(), 2);
    assert_eq!(member.assignment(), &[(TOPIC.to_owned(), vec![0, 1, 2])]);
    assert_eq!(member.heartbeat().await.unwrap(), HeartbeatStatus::Stable);

    // Leaving hands the cluster back and the coordinator forgets us.
    let _cluster = member.leave().await.unwrap();
    assert!(fake.group.lock().unwrap().members.is_empty());
}

#[tokio::test]
async fn evicted_member_is_told_so() {
    use odradek_client::{GroupConfig, GroupMember, HeartbeatStatus};

    let fake = spawn_fake_cluster(1, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut member = GroupMember::join(cluster, "g1", &[TOPIC], GroupConfig::default())
        .await
        .unwrap();

    // The coordinator forgets the member (session timeout, say).
    fake.group.lock().unwrap().members.clear();
    assert_eq!(member.heartbeat().await.unwrap(), HeartbeatStatus::Evicted);
    // Rejoining starts over: new member id, next generation.
    member.rejoin().await.unwrap();
    assert_eq!(member.member_id(), "member-2");
    assert_eq!(member.heartbeat().await.unwrap(), HeartbeatStatus::Stable);
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
        ..Default::default()
    };
    let mut cluster = Cluster::connect(config).await.unwrap();
    cluster.refresh_metadata(&[TOPIC]).await.unwrap();
    assert_eq!(cluster.brokers().count(), 1);
}
