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

/// Accepted connections per broker node id.
type Accepts = Arc<Mutex<std::collections::HashMap<i32, u32>>>;

/// The fake group coordinator's node id.
const COORDINATOR: i32 = 1;

/// Fetching at this offset makes the fake park the connection a while,
/// like a long-poll waiting out max_wait_ms.
const SLOW_FETCH_OFFSET: i64 = 777_777;

/// Stored record sets per (topic, partition), verbatim as produced.
type Logs = Arc<Mutex<std::collections::HashMap<(String, i32), BytesMut>>>;

/// Topic names accepted by the fake's CreateTopics handler.
type Created = Arc<Mutex<std::collections::HashSet<String>>>;

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
    /// node_id -> the broker's accept-loop task; aborting it drops the
    /// listener and every accepted connection (see `kill_broker`).
    brokers: Vec<(i32, tokio::task::JoinHandle<()>)>,
    arrivals: Arrivals,
    offsets: Offsets,
    created: Created,
    // Read back only by the codec roundtrip test, which needs every codec.
    #[cfg_attr(
        not(all(
            feature = "gzip",
            feature = "lz4",
            feature = "snappy",
            feature = "zstd"
        )),
        allow(dead_code)
    )]
    logs: Logs,
    group: Group,
    accepts: Accepts,
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
    let created: Created = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let accepts: Accepts = Arc::new(Mutex::new(std::collections::HashMap::new()));

    let mut brokers = Vec::new();
    for (node_id, listener) in listeners {
        let endpoints = endpoints.clone();
        let arrivals = Arc::clone(&arrivals);
        let offsets = Arc::clone(&offsets);
        let logs = Arc::clone(&logs);
        let group = Arc::clone(&group);
        let created = Arc::clone(&created);
        let accepts = Arc::clone(&accepts);
        let handle = tokio::spawn(async move {
            // The accept loop owns its connections' tasks: aborting the
            // loop drops the JoinSet, which aborts them all — the whole
            // broker dies at once, listener and live sockets together.
            let mut conns = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { return };
                        *accepts.lock().unwrap().entry(node_id).or_insert(0) += 1;
                        conns.spawn(serve_conn(
                            stream,
                            node_id,
                            endpoints.clone(),
                            Arc::clone(&arrivals),
                            Arc::clone(&offsets),
                            Arc::clone(&logs),
                            Arc::clone(&group),
                            Arc::clone(&created),
                            no_leader,
                        ));
                    }
                    Some(_) = conns.join_next() => {}
                }
            }
        });
        brokers.push((node_id, handle));
    }
    FakeCluster {
        endpoints,
        brokers,
        arrivals,
        offsets,
        created,
        logs,
        group,
        accepts,
    }
}

impl FakeCluster {
    /// Kill one broker: its listener closes and every established
    /// connection to it drops, as if the process died.
    fn kill_broker(&self, node_id: i32) {
        for (id, handle) in &self.brokers {
            if *id == node_id {
                handle.abort();
            }
        }
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
    created: Created,
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
                let mut resp = ApiVersionsResponse::default();
                resp.api_keys = [
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
                    (19, 2, 7),
                ]
                .into_iter()
                .map(|(api_key, min_version, max_version)| {
                    let mut v = ApiVersion::default();
                    v.api_key = api_key;
                    v.min_version = min_version;
                    v.max_version = max_version;
                    v
                })
                .collect();
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            3 => {
                let mut resp = MetadataResponse::default();
                resp.brokers = endpoints
                    .iter()
                    .map(|(id, addr)| {
                        let (host, port) = addr.rsplit_once(':').unwrap();
                        let mut broker = MetadataResponseBroker::default();
                        broker.node_id = *id;
                        broker.host = host.into();
                        broker.port = port.parse().unwrap();
                        broker
                    })
                    .collect();
                resp.cluster_id = Some("fake-cluster".into());
                resp.controller_id = 0;
                let mut topic = MetadataResponseTopic::default();
                topic.name = Some(TOPIC.into());
                topic.partitions = endpoints
                    .iter()
                    .map(|(id, _)| {
                        let mut p = MetadataResponsePartition::default();
                        p.partition_index = *id;
                        p.leader_id = if no_leader.contains(id) { -1 } else { *id };
                        p
                    })
                    .collect();
                resp.topics = vec![topic];
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
                        let mut presp = PartitionProduceResponse::default();
                        presp.index = p.index;
                        presp.error_code = 0;
                        presp.base_offset = 7;
                        presp.log_append_time_ms = -1;
                        partitions.push(presp);
                    }
                    let mut tresp = TopicProduceResponse::default();
                    tresp.name = topic.name.clone();
                    tresp.partition_responses = partitions;
                    responses.push(tresp);
                }
                let mut buf = BytesMut::new();
                let mut resp = ProduceResponse::default();
                resp.responses = responses;
                resp.encode(&mut buf, api_version).unwrap();
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
                if topic.partitions[0].fetch_offset == SLOW_FETCH_OFFSET {
                    // Model a long-poll parked on this connection.
                    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                }
                // Serve whatever was produced; the canned log otherwise.
                let stored = logs
                    .lock()
                    .unwrap()
                    .get(&(TOPIC.to_owned(), partition))
                    .map(|b| b.clone().freeze());
                let mut pdata = PartitionData::default();
                pdata.partition_index = partition;
                pdata.error_code = 0;
                pdata.high_watermark = 9;
                pdata.last_stable_offset = 9;
                pdata.log_start_offset = 5;
                pdata.records = Some(stored.unwrap_or_else(fake_log));
                let mut tresp = FetchableTopicResponse::default();
                tresp.topic = TOPIC.into();
                tresp.partitions = vec![pdata];
                let mut resp = FetchResponse::default();
                resp.responses = vec![tresp];
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
                let mut presp = ListOffsetsPartitionResponse::default();
                presp.partition_index = partition.partition_index;
                presp.error_code = 0;
                presp.timestamp = -1;
                presp.offset = offset;
                let mut tresp = ListOffsetsTopicResponse::default();
                tresp.name = TOPIC.into();
                tresp.partitions = vec![presp];
                let mut resp = ListOffsetsResponse::default();
                resp.topics = vec![tresp];
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
                let mut resp = FindCoordinatorResponse::default();
                resp.error_code = 0;
                resp.node_id = coord.0;
                resp.host = host.into();
                resp.port = port.parse().unwrap();
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
                let topic = &req.topics[0];
                let p = &topic.partitions[0];
                // The simple-consumer path (generation -1, no member id)
                // is unfenced; a member's commit must carry its current
                // identity or be refused, like a real coordinator.
                let error_code =
                    if req.generation_id_or_member_epoch == -1 && req.member_id.is_empty() {
                        0
                    } else {
                        let state = group.lock().unwrap();
                        if !state.members.iter().any(|(id, _)| *id == req.member_id) {
                            25 // UNKNOWN_MEMBER_ID
                        } else if req.generation_id_or_member_epoch != state.generation {
                            22 // ILLEGAL_GENERATION
                        } else {
                            0
                        }
                    };
                if error_code == 0 {
                    offsets.lock().unwrap().insert(
                        (req.group_id.clone(), topic.name.clone(), p.partition_index),
                        (p.committed_offset, node_id),
                    );
                }
                let mut presp = OffsetCommitResponsePartition::default();
                presp.partition_index = p.partition_index;
                presp.error_code = error_code;
                let mut tresp = OffsetCommitResponseTopic::default();
                tresp.name = topic.name.clone();
                tresp.partitions = vec![presp];
                let mut resp = OffsetCommitResponse::default();
                resp.topics = vec![tresp];
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
                let mut presp = OffsetFetchResponsePartition::default();
                presp.partition_index = partition;
                presp.committed_offset = committed;
                presp.committed_leader_epoch = -1;
                presp.metadata = Some(String::new());
                presp.error_code = 0;
                let mut tresp = OffsetFetchResponseTopic::default();
                tresp.name = topic.name.clone();
                tresp.partitions = vec![presp];
                let mut resp = OffsetFetchResponse::default();
                resp.topics = vec![tresp];
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
                    let mut resp = JoinGroupResponse::default();
                    resp.error_code = 79; // MEMBER_ID_REQUIRED
                    resp.member_id = format!("member-{}", state.next_member);
                    resp
                } else {
                    state.rebalancing = false;
                    state.generation += 1;
                    state.members =
                        vec![(req.member_id.clone(), req.protocols[0].metadata.clone())];
                    let mut resp = JoinGroupResponse::default();
                    resp.error_code = 0;
                    resp.generation_id = state.generation;
                    resp.protocol_name = Some("range".into());
                    resp.leader = req.member_id.clone();
                    resp.member_id = req.member_id.clone();
                    resp.members = state
                        .members
                        .iter()
                        .map(|(id, meta)| {
                            let mut member = JoinGroupResponseMember::default();
                            member.member_id = id.clone();
                            member.metadata = meta.clone();
                            member
                        })
                        .collect();
                    resp
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
                let mut resp = SyncGroupResponse::default();
                resp.error_code = 0;
                resp.assignment = state
                    .assignments
                    .get(&req.member_id)
                    .cloned()
                    .unwrap_or_default();
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
                let mut resp = HeartbeatResponse::default();
                resp.error_code = if !known {
                    25 // UNKNOWN_MEMBER_ID
                } else if state.rebalancing {
                    27 // REBALANCE_IN_PROGRESS
                } else {
                    0
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
            19 => {
                use odradek_protocol::messages::create_topics_request::CreateTopicsRequest;
                use odradek_protocol::messages::create_topics_response::{
                    CreatableTopicResult, CreateTopicsResponse,
                };
                let req = CreateTopicsRequest::decode(&mut frame, api_version).unwrap();
                let mut resp = CreateTopicsResponse::default();
                for t in &req.topics {
                    let fresh = created.lock().unwrap().insert(t.name.clone());
                    let mut tresp = CreatableTopicResult::default();
                    tresp.name = t.name.clone();
                    tresp.error_code = if fresh { 0 } else { 36 }; // TOPIC_ALREADY_EXISTS
                    resp.topics.push(tresp);
                }
                let mut buf = BytesMut::new();
                resp.encode(&mut buf, api_version).unwrap();
                buf.freeze()
            }
            other => panic!("fake broker got api key {other}"),
        };

        let mut resp_header = ResponseHeader::default();
        resp_header.correlation_id = header.correlation_id;
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
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![cluster.endpoints[0].1.clone()];
    config.client_id = "odradek".into();
    config
}

fn probe_produce_body(partition: i32, version: i16) -> Bytes {
    use odradek_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    let mut partition_data = PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(Bytes::new());
    let mut topic_data = TopicProduceData::default();
    topic_data.name = TOPIC.into();
    topic_data.partition_data = vec![partition_data];
    let mut req = ProduceRequest::default();
    req.acks = -1;
    req.timeout_ms = 5_000;
    req.topic_data = vec![topic_data];
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    body.freeze()
}

#[tokio::test]
async fn produce_routes_to_each_partition_leader() {
    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

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
    assert_eq!(cluster.brokers().len(), 3);
    assert_eq!(cluster.partitions(TOPIC).unwrap().len(), 3);
    assert_eq!(cluster.leader_id(TOPIC, 2), Some(2));
}

#[tokio::test]
async fn leaderless_partition_is_an_error_not_a_guess() {
    let fake = spawn_fake_cluster(2, &[1]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

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
    let consumer = Consumer::new(cluster);

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
    let mut producer_config = ProducerConfig::default();
    producer_config.batch_max_bytes = 200;
    let mut producer = Producer::with_config(cluster, producer_config);
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
#[cfg(all(
    feature = "gzip",
    feature = "lz4",
    feature = "snappy",
    feature = "zstd"
))]
async fn compressed_batches_roundtrip_end_to_end() {
    use odradek_client::{Consumer, Producer, ProducerConfig};
    use odradek_protocol::records::{Compression, Record, decode_set};

    for codec in [
        Compression::Gzip,
        Compression::Lz4,
        Compression::Snappy,
        Compression::Zstd,
    ] {
        let fake = spawn_fake_cluster(1, &[]).await;
        let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
        let mut producer_config = ProducerConfig::default();
        producer_config.compression = codec;
        let mut producer = Producer::with_config(cluster, producer_config);
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
        let consumer = Consumer::new(cluster);
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
    let consumer = Consumer::new(cluster);

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
    member.leave().await.unwrap();
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
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![dead_addr, fake.endpoints[0].1.clone()];
    config.client_id = "odradek".into();
    let cluster = Cluster::connect(config).await.unwrap();
    cluster.refresh_metadata(&[TOPIC]).await.unwrap();
    assert_eq!(cluster.brokers().len(), 1);
}

#[tokio::test]
async fn one_cluster_handle_is_shared_across_concurrent_tasks() {
    use odradek_client::{Consumer, Producer};
    use odradek_protocol::records::Record;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    // Three producer tasks, one per partition, all over clones of the
    // same handle — the shared metadata cache and connection pool must
    // still route each batch to its partition's leader.
    let mut tasks = tokio::task::JoinSet::new();
    for partition in 0..3 {
        let cluster = cluster.clone();
        tasks.spawn(async move {
            let mut producer = Producer::new(cluster);
            producer
                .produce(
                    TOPIC,
                    partition,
                    vec![Record {
                        value: Some(Bytes::from(format!("task {partition}"))),
                        ..Default::default()
                    }],
                )
                .await
                .unwrap();
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    {
        let arrivals = fake.arrivals.lock().unwrap();
        for partition in 0..3 {
            assert!(
                arrivals.contains(&(partition, partition)),
                "partition {partition} must reach its leader, got {arrivals:?}"
            );
        }
    }

    // A consumer over the same handle reuses what the producers learned.
    let consumer = Consumer::new(cluster);
    assert_eq!(consumer.latest_offset(TOPIC, 1).await.unwrap(), 9);
}

#[tokio::test]
async fn enqueue_keyed_routes_by_key_and_round_robins_keyless() {
    use odradek_client::Producer;
    use odradek_protocol::records::Record;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let mut producer = Producer::new(cluster);

    // Equal keys land in one partition: one delivery of both records.
    // The first call refreshes metadata itself (nothing cached yet).
    for value in ["one", "two"] {
        producer
            .enqueue_keyed(
                TOPIC,
                Record {
                    key: Some(Bytes::from_static(b"stable-key")),
                    value: Some(Bytes::from_static(value.as_bytes())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let deliveries = producer.flush().await.unwrap();
    assert_eq!(deliveries.len(), 1, "equal keys share a partition");
    assert_eq!(deliveries[0].records, 2);

    // Keyless records round-robin across the topic's three partitions.
    for _ in 0..3 {
        producer
            .enqueue_keyed(
                TOPIC,
                Record {
                    value: Some(Bytes::from_static(b"keyless")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let mut partitions: Vec<i32> = producer
        .flush()
        .await
        .unwrap()
        .iter()
        .map(|d| d.partition)
        .collect();
    partitions.sort_unstable();
    assert_eq!(partitions, vec![0, 1, 2]);
}

#[tokio::test]
async fn control_plane_fails_over_when_the_bootstrap_broker_dies() {
    let fake = spawn_fake_cluster(3, &[]).await;
    // Bootstrap through broker 0 only.
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    cluster.refresh_metadata(&[TOPIC]).await.unwrap();
    assert_eq!(cluster.brokers().len(), 3);

    // Broker 0 — the only configured bootstrap server — dies: listener
    // and established connections included.
    fake.kill_broker(0);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The next refresh rides the failover: the dead control connection
    // is dropped and the redial falls through the dead bootstrap address
    // to a known broker.
    cluster.refresh_metadata(&[TOPIC]).await.unwrap();
    assert_eq!(cluster.brokers().len(), 3);

    // Coordinator discovery uses the same control plane.
    use odradek_client::Consumer;
    let consumer = Consumer::new(cluster);
    consumer.commit_offset("g1", TOPIC, 1, 17).await.unwrap();
    assert_eq!(
        consumer.committed_offset("g1", TOPIC, 1).await.unwrap(),
        Some(17)
    );
}

#[tokio::test]
async fn create_topic_speaks_create_topics_and_surfaces_duplicates() {
    use odradek_protocol::ErrorCode;

    let fake = spawn_fake_cluster(1, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    cluster.create_topic("fresh-topic", 3, 1).await.unwrap();
    assert!(fake.created.lock().unwrap().contains("fresh-topic"));

    // Creating it again is an error the caller can match on.
    match cluster.create_topic("fresh-topic", 3, 1).await {
        Err(ClientError::Broker(code)) => assert_eq!(code, ErrorCode::TOPIC_ALREADY_EXISTS),
        other => panic!("expected TOPIC_ALREADY_EXISTS, got {other:?}"),
    }
}

#[tokio::test]
async fn stale_generation_commit_is_fenced() {
    use odradek_client::{GroupConfig, GroupMember};
    use odradek_protocol::ErrorCode;

    let fake = spawn_fake_cluster(3, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let member = GroupMember::join(cluster, "g1", &[TOPIC], GroupConfig::default())
        .await
        .unwrap();

    // A commit carrying the member's live generation lands.
    member.commit_offset(TOPIC, 0, 11).await.unwrap();
    assert_eq!(member.committed_offset(TOPIC, 0).await.unwrap(), Some(11));
    let (offset, committed_at) = fake.offsets.lock().unwrap()[&("g1".into(), TOPIC.into(), 0)];
    assert_eq!(offset, 11);
    assert_eq!(committed_at, COORDINATOR);

    // The group rebalances away from under us; the stale generation's
    // commit is fenced, not silently applied.
    fake.group.lock().unwrap().generation += 1;
    match member.commit_offset(TOPIC, 0, 12).await {
        Err(ClientError::Broker(code)) => assert_eq!(code, ErrorCode::ILLEGAL_GENERATION),
        other => panic!("expected ILLEGAL_GENERATION, got {other:?}"),
    }
    // The fenced commit changed nothing.
    let (offset, _) = fake.offsets.lock().unwrap()[&("g1".into(), TOPIC.into(), 0)];
    assert_eq!(offset, 11);
}

#[tokio::test]
async fn parked_fetch_does_not_block_produce_to_the_same_broker() {
    use odradek_client::{Consumer, Producer};
    use odradek_protocol::records::Record;

    let fake = spawn_fake_cluster(1, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();

    // Park a long fetch on its leased connection...
    let consumer = Consumer::new(cluster.clone());
    let parked = tokio::spawn(async move {
        // The fake sleeps ~1.5s before answering this offset.
        let _ = consumer.fetch(TOPIC, 0, SLOW_FETCH_OFFSET).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // ...and a produce to the same broker must not wait behind it.
    let mut producer = Producer::new(cluster);
    let started = std::time::Instant::now();
    producer
        .produce(
            TOPIC,
            0,
            vec![Record {
                value: Some(Bytes::from_static(b"unblocked")),
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "produce should not queue behind the parked fetch (took {elapsed:?})"
    );
    assert!(!parked.is_finished(), "the fetch must still be parked");
    parked.await.unwrap();
}

#[tokio::test]
async fn released_fetch_leases_are_reused_not_redialed() {
    use odradek_client::Consumer;

    let fake = spawn_fake_cluster(1, &[]).await;
    let cluster = Cluster::connect(config_for(&fake)).await.unwrap();
    let consumer = Consumer::new(cluster);

    consumer.fetch(TOPIC, 0, 0).await.unwrap();
    let after_first = *fake.accepts.lock().unwrap().get(&0).unwrap();
    for _ in 0..3 {
        consumer.fetch(TOPIC, 0, 0).await.unwrap();
    }
    let after_more = *fake.accepts.lock().unwrap().get(&0).unwrap();
    assert_eq!(
        after_first, after_more,
        "sequential fetches must reuse the released lease"
    );
}
