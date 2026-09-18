//! Checks that run against a client under test (the suite acts as server).
//!
//! The harness impersonates a small cluster: the bootstrap listener plus
//! two more ephemeral listeners, presented in Metadata as brokers 0-2.
//! One topic ([`ROUTING_TOPIC`]) spans three partitions, partition `i`
//! led by broker `i`, so leader routing is observable. Every broker
//! answers just enough of the protocol to keep a real client talking
//! (ApiVersions, Metadata, and empty Produce/Fetch successes) and records
//! every frame; the catalogued checks ([`CLIENT_CHECKS`]) are evaluated
//! over the recorded `Session`. A session ends when the subject hangs up
//! (its last connection closes), when it has said enough
//! ([`ObserveConfig::max_requests`]), or when it falls silent
//! ([`ObserveConfig::idle_timeout`]). When the harness itself cannot run —
//! a listener fails to bind, or no client ever connects before the
//! [`ObserveConfig::accept_timeout`] deadline — [`run`] returns an
//! infrastructure error instead of fabricating check outcomes.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::fetch_request::FetchRequest;
use odradek_protocol::messages::fetch_response::FetchResponse;
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::{ErrorCode, Message, frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::checks::{Check, Runner};
use crate::report::{CheckOutcome, Report};
use crate::{CheckId, SubjectRole, Verdict};

/// (api key, min, max) the harness advertises — exactly the apis it can
/// parse and answer, so version discipline is checkable. Produce and
/// fetch are served across the full schema range: the harness resolves
/// the id-addressed (v13+) forms through [`ROUTING_TOPIC_ID`].
const ADVERTISED: &[(i16, i16, i16)] = &[
    (
        ApiVersionsRequest::API_KEY,
        ApiVersionsRequest::MIN_VERSION,
        MAX_API_VERSIONS,
    ),
    (
        ProduceRequest::API_KEY,
        ProduceRequest::MIN_VERSION,
        ProduceRequest::MAX_VERSION,
    ),
    (
        FetchRequest::API_KEY,
        FetchRequest::MIN_VERSION,
        FetchRequest::MAX_VERSION,
    ),
    (
        MetadataRequest::API_KEY,
        MetadataRequest::MIN_VERSION,
        MetadataRequest::MAX_VERSION,
    ),
];

const MAX_API_VERSIONS: i16 = ApiVersionsRequest::MAX_VERSION;

/// How many brokers the harness impersonates.
pub const BROKER_COUNT: i32 = 3;

/// The topic the harness advertises for routing observation: one
/// partition per broker, partition `i` led by broker `i`.
pub const ROUTING_TOPIC: &str = "odradek-routing";

/// The routing topic's id, for id-addressed clients (16 bytes).
pub const ROUTING_TOPIC_ID: [u8; 16] = *b"odradek-routing!";

/// Every client-side check, in run order. Ids are stable; baselines and
/// the calibration tests cite them verbatim.
pub static CLIENT_CHECKS: &[Check] = &[
    Check {
        id: "client/header-well-formed",
        requirement: "every request carries a decodable header at the version \
                      implied by its (api key, api version)",
        runner: Runner::Client(header_well_formed),
    },
    Check {
        id: "client/starts-with-api-versions",
        requirement: "the first request on every connection is ApiVersions, so \
                      versions are negotiated before anything else is sent",
        runner: Runner::Client(starts_with_api_versions),
    },
    Check {
        id: "client/correlation-ids-unique",
        requirement: "correlation ids are not reused within a connection, so \
                      responses are unambiguously attributable",
        runner: Runner::Client(correlation_ids_unique),
    },
    Check {
        id: "client/respects-advertised-versions",
        requirement: "after negotiation the client only sends apis and \
                      versions the server advertised",
        runner: Runner::Client(respects_advertised_versions),
    },
    Check {
        id: "client/body-decodes",
        requirement: "request bodies decode per the message schema at the \
                      claimed version, with no trailing bytes",
        runner: Runner::Client(body_decodes),
    },
    Check {
        id: "client/routes-to-partition-leader",
        requirement: "produce and fetch requests go to the broker the \
                      metadata advertises as the partition's leader",
        runner: Runner::Client(routes_to_partition_leader),
    },
    Check {
        id: "client/recovers-from-leader-change",
        requirement: "after a NOT_LEADER_OR_FOLLOWER answer, the client \
                      refreshes metadata and re-delivers to the newly \
                      advertised leader",
        runner: Runner::Client(recovers_from_leader_change),
    },
];

/// A deliberate misbehavior the harness can stage to observe how the
/// client copes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessFault {
    /// The first produce for each routing-topic partition is answered
    /// with NOT_LEADER_OR_FOLLOWER and leadership moves to the next
    /// broker; later metadata reflects the move. A resilient client
    /// refreshes and re-delivers to the new leader.
    LeaderMove,
}

/// Limits for one observation session.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ObserveConfig {
    /// Stop after this many requests (the checks need finite input).
    pub max_requests: usize,
    /// Stop when every connection goes quiet for this long. This is the
    /// backstop for a client that connects and then says nothing: a
    /// client that hangs up ends the session at once instead.
    pub idle_timeout: Duration,
    /// How long to wait for the client's *first* connection before giving
    /// up with an infrastructure error — the harness must not hang
    /// forever on a client that never dials.
    pub accept_timeout: Duration,
    /// Optional staged misbehavior.
    pub fault: Option<HarnessFault>,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        ObserveConfig {
            max_requests: 32,
            idle_timeout: Duration::from_secs(3),
            accept_timeout: Duration::from_secs(60),
            fault: None,
        }
    }
}

/// What the harness saw in one request frame.
#[derive(Debug)]
struct Observation {
    /// Which impersonated broker received the frame.
    node_id: i32,
    /// Connection ordinal (correlation ids and handshakes are per
    /// connection).
    conn_id: usize,
    /// Position within its connection.
    index: usize,
    api_key: i16,
    api_version: i16,
    /// None when even a salvage parse could not recover a header.
    header: Option<RequestHeader>,
    header_error: Option<String>,
    body_error: Option<String>,
    /// Body checking does not apply (unknown api, or an ApiVersions probe
    /// above our max — the probe dance is legal).
    body_exempt: bool,
    /// (topic, partition) pairs this produce/fetch addressed.
    routes: Vec<(String, i32)>,
}

/// A NOT_LEADER injection that actually fired.
#[derive(Debug, Clone, Copy)]
struct FaultEvent {
    partition: i32,
    to_node: i32,
}

/// One recorded observation session: everything the client-side checks
/// judge.
#[derive(Debug)]
pub(crate) struct Session {
    observations: Vec<Observation>,
    fault: Option<HarnessFault>,
    events: Vec<FaultEvent>,
}

/// Connection bookkeeping for one session: hands out connection ordinals
/// and signals when the last live connection closes.
///
/// A client that has hung up has nothing left to say, so the session can
/// be judged immediately instead of waiting out
/// [`ObserveConfig::idle_timeout`] — which remains the backstop for a
/// client that connects and then falls silent.
#[derive(Debug)]
struct ConnTracker {
    next_id: AtomicUsize,
    live: AtomicUsize,
    ended: mpsc::UnboundedSender<()>,
}

impl ConnTracker {
    /// Register a connection about to be handled and return its ordinal.
    /// Called before the handler is spawned, so the live count never dips
    /// through zero between accept and handling.
    fn open(&self) -> usize {
        self.live.fetch_add(1, Ordering::SeqCst);
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Retire a connection, signalling the session's end when it was the
    /// last one live.
    fn close(&self) {
        if self.live.fetch_sub(1, Ordering::SeqCst) == 1 {
            let _ = self.ended.send(());
        }
    }
}

/// The impersonated cluster: endpoints (broker `i` on `ports[i]`) and
/// mutable leadership for the routing topic.
struct ClusterView {
    ports: Vec<u16>,
    fault: Option<HarnessFault>,
    /// Current leader per routing-topic partition.
    leaders: std::sync::Mutex<Vec<i32>>,
    /// Leader-move injections that fired.
    events: std::sync::Mutex<Vec<FaultEvent>>,
}

impl ClusterView {
    /// The produce verdict for `partition` arriving at `node_id`: the
    /// error code to answer, staging the leader move when armed.
    fn produce_error(&self, partition: i32, node_id: i32) -> i16 {
        let mut leaders = self.leaders.lock().unwrap();
        let Some(slot) = usize::try_from(partition)
            .ok()
            .filter(|&p| p < leaders.len())
        else {
            return ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0;
        };
        if leaders[slot] != node_id {
            return ErrorCode::NOT_LEADER_OR_FOLLOWER.0;
        }
        if self.fault == Some(HarnessFault::LeaderMove) {
            let already_moved = self
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.partition == partition);
            if !already_moved {
                let to_node = (node_id + 1) % BROKER_COUNT;
                leaders[slot] = to_node;
                self.events
                    .lock()
                    .unwrap()
                    .push(FaultEvent { partition, to_node });
                return ErrorCode::NOT_LEADER_OR_FOLLOWER.0;
            }
        }
        ErrorCode::NONE.0
    }

    /// The fetch verdict for `partition` arriving at `node_id`: judged
    /// against current leadership, never staging moves.
    fn fetch_error(&self, partition: i32, node_id: i32) -> i16 {
        let leaders = self.leaders.lock().unwrap();
        match usize::try_from(partition)
            .ok()
            .filter(|&p| p < leaders.len())
        {
            Some(slot) if leaders[slot] == node_id => ErrorCode::NONE.0,
            Some(_) => ErrorCode::NOT_LEADER_OR_FOLLOWER.0,
            None => ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0,
        }
    }
}

/// Accept client connections (bootstrap on `listener`, brokers 1+ on
/// internal listeners), observe every frame, and evaluate the catalogued
/// Client-role checks.
///
/// An `Err` means the harness itself could not run — an infrastructure
/// finding about the run, never a statement about the subject.
pub async fn run(listener: &TcpListener, config: &ObserveConfig) -> io::Result<Report> {
    // Brokers 1..N listen on ephemeral ports next to the bootstrap.
    let mut extra = Vec::new();
    for _ in 1..BROKER_COUNT {
        let l = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| io::Error::new(e.kind(), format!("cannot bind harness broker: {e}")))?;
        extra.push(l);
    }
    let mut ports = vec![
        listener
            .local_addr()
            .map_err(|e| io::Error::new(e.kind(), format!("bootstrap listener: {e}")))?
            .port(),
    ];
    ports.extend(
        extra
            .iter()
            .filter_map(|l| l.local_addr().ok().map(|a| a.port())),
    );
    let view = Arc::new(ClusterView {
        ports,
        fault: config.fault,
        leaders: std::sync::Mutex::new((0..BROKER_COUNT).collect()),
        events: std::sync::Mutex::new(Vec::new()),
    });

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (ended_tx, mut ended_rx) = mpsc::unbounded_channel();
    let conns = Arc::new(ConnTracker {
        next_id: AtomicUsize::new(0),
        live: AtomicUsize::new(0),
        ended: ended_tx,
    });
    let mut accept_tasks = Vec::new();
    for (i, l) in extra.into_iter().enumerate() {
        let node_id = i32::try_from(i).unwrap_or(0) + 1;
        let view = Arc::clone(&view);
        let tx = tx.clone();
        let conns = Arc::clone(&conns);
        accept_tasks.push(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = l.accept().await else {
                    return;
                };
                let conn_id = conns.open();
                tokio::spawn(handle_conn(
                    stream,
                    node_id,
                    conn_id,
                    Arc::clone(&view),
                    tx.clone(),
                    Arc::clone(&conns),
                ));
            }
        }));
    }

    // Wait — with a deadline — for the client's first connection,
    // necessarily to the bootstrap, the only address it has.
    let first = tokio::time::timeout(config.accept_timeout, listener.accept()).await;
    let peer = match first {
        Err(_) => {
            abort_all(&accept_tasks);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("no client connected within {:?}", config.accept_timeout),
            ));
        }
        Ok(Err(e)) => {
            abort_all(&accept_tasks);
            return Err(io::Error::new(e.kind(), format!("accept failed: {e}")));
        }
        Ok(Ok((stream, peer))) => {
            let conn_id = conns.open();
            tokio::spawn(handle_conn(
                stream,
                0,
                conn_id,
                Arc::clone(&view),
                tx.clone(),
                Arc::clone(&conns),
            ));
            peer
        }
    };

    // Collect observations until the client hangs up (or goes idle, or
    // says enough).
    let mut observations = Vec::new();
    while observations.len() < config.max_requests {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((stream, _)) = accepted {
                    let conn_id = conns.open();
                    tokio::spawn(handle_conn(
                        stream, 0, conn_id, Arc::clone(&view), tx.clone(), Arc::clone(&conns),
                    ));
                }
            }
            obs = rx.recv() => match obs {
                Some(obs) => observations.push(obs),
                None => break,
            },
            _ = ended_rx.recv() => break,
            () = tokio::time::sleep(config.idle_timeout) => break,
        }
    }
    abort_all(&accept_tasks);
    // The last connection's closing can win the race against its own
    // final observations, which are already queued: take what is there.
    while observations.len() < config.max_requests {
        match rx.try_recv() {
            Ok(obs) => observations.push(obs),
            Err(_) => break,
        }
    }
    let events = view.events.lock().unwrap().clone();
    let session = Session {
        observations,
        fault: config.fault,
        events,
    };
    Ok(evaluate(&session, &format!("client {peer}")))
}

fn abort_all(tasks: &[JoinHandle<()>]) {
    for t in tasks {
        t.abort();
    }
}

/// Serve one client connection, retiring it with the tracker however it
/// ends — closed, unreadable, or with the collector gone.
async fn handle_conn(
    stream: TcpStream,
    node_id: i32,
    conn_id: usize,
    view: Arc<ClusterView>,
    tx: mpsc::UnboundedSender<Observation>,
    conns: Arc<ConnTracker>,
) {
    serve_conn(stream, node_id, conn_id, view, tx).await;
    conns.close();
}

async fn serve_conn(
    mut stream: TcpStream,
    node_id: i32,
    conn_id: usize,
    view: Arc<ClusterView>,
    tx: mpsc::UnboundedSender<Observation>,
) {
    let mut index = 0;
    loop {
        let Some(frame) = read_frame(&mut stream).await else {
            return;
        };
        let obs = parse_request(node_id, conn_id, index, frame);
        index += 1;
        if let Some(header) = &obs.header {
            respond(&mut stream, header, &view, node_id, &obs.routes).await;
        }
        if tx.send(obs).is_err() {
            return;
        }
    }
}

async fn read_frame(stream: &mut TcpStream) -> Option<Bytes> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.ok()?;
    let len = frame::check_len(len_bytes, frame::DEFAULT_MAX_FRAME).ok()?;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.ok()?;
    Some(Bytes::from(frame))
}

fn parse_request(node_id: i32, conn_id: usize, index: usize, frame: Bytes) -> Observation {
    let mut obs = Observation {
        node_id,
        conn_id,
        index,
        api_key: -1,
        api_version: -1,
        header: None,
        header_error: None,
        body_error: None,
        body_exempt: true,
        routes: Vec::new(),
    };
    if frame.len() < 8 {
        obs.header_error = Some(format!(
            "frame of {} byte(s) cannot hold a header",
            frame.len()
        ));
        return obs;
    }
    obs.api_key = i16::from_be_bytes([frame[0], frame[1]]);
    obs.api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let (api_key, api_version) = (obs.api_key, obs.api_version);

    let known_probe_version = if api_key == ApiVersionsRequest::API_KEY {
        // ApiVersions probes above our max are legal; parse at our newest.
        Some(api_version.min(MAX_API_VERSIONS))
    } else {
        None
    };
    let header_version =
        request_header_version(api_key, known_probe_version.unwrap_or(api_version));

    let mut body = None;
    match header_version {
        Some(hv) => {
            let mut buf = frame.clone();
            match RequestHeader::decode(&mut buf, hv) {
                Ok(h) => {
                    obs.header = Some(h);
                    body = Some(buf);
                }
                Err(e) => {
                    obs.header = salvage_header(&frame);
                    obs.header_error = Some(format!("header (v{hv}): {e}"));
                }
            }
        }
        // Unknown api key: no defined header version. Salvage for the
        // correlation id; the advertised-apis check reports the violation.
        None => obs.header = salvage_header(&frame),
    }

    match (api_key, &mut body) {
        (_, None) => {}
        (ApiVersionsRequest::API_KEY, Some(_)) if api_version > MAX_API_VERSIONS => {}
        (ApiVersionsRequest::API_KEY, Some(buf)) => {
            obs.body_exempt = false;
            obs.body_error = decode_fully::<ApiVersionsRequest>(buf, api_version).err();
        }
        (ProduceRequest::API_KEY, Some(buf)) => {
            obs.body_exempt = false;
            match decode_fully::<ProduceRequest>(buf, api_version) {
                Err(e) => obs.body_error = Some(e),
                Ok(req) => {
                    for topic in &req.topic_data {
                        let name = resolve_topic(&topic.name, topic.topic_id);
                        for p in &topic.partition_data {
                            obs.routes.push((name.clone(), p.index));
                        }
                    }
                }
            }
        }
        (FetchRequest::API_KEY, Some(buf)) => {
            obs.body_exempt = false;
            match decode_fully::<FetchRequest>(buf, api_version) {
                Err(e) => obs.body_error = Some(e),
                Ok(req) => {
                    for topic in &req.topics {
                        let name = resolve_topic(&topic.topic, topic.topic_id);
                        for p in &topic.partitions {
                            obs.routes.push((name.clone(), p.partition));
                        }
                    }
                }
            }
        }
        (MetadataRequest::API_KEY, Some(buf)) => {
            obs.body_exempt = false;
            obs.body_error = decode_fully::<MetadataRequest>(buf, api_version).err();
        }
        _ => {}
    }
    obs
}

/// Map a request's topic reference (name, or id for v13+) back to a name.
fn resolve_topic(name: &str, topic_id: [u8; 16]) -> String {
    if !name.is_empty() {
        name.to_owned()
    } else if topic_id == ROUTING_TOPIC_ID {
        ROUTING_TOPIC.to_owned()
    } else {
        format!("<topic id {topic_id:02x?}>")
    }
}

fn decode_fully<T: Message>(buf: &mut Bytes, version: i16) -> Result<T, String> {
    match T::decode(buf, version) {
        Err(e) => Err(format!("body: {e}")),
        Ok(_) if !buf.is_empty() => Err(format!(
            "body leaves {} undecoded trailing byte(s)",
            buf.len()
        )),
        Ok(v) => Ok(v),
    }
}

/// Best-effort header recovery so the session can continue: try flexible
/// then classic header encodings.
fn salvage_header(frame: &Bytes) -> Option<RequestHeader> {
    for hv in [2, 1] {
        if let Ok(h) = RequestHeader::decode(&mut frame.clone(), hv) {
            return Some(h);
        }
    }
    None
}

async fn respond(
    stream: &mut TcpStream,
    header: &RequestHeader,
    view: &ClusterView,
    node_id: i32,
    routes: &[(String, i32)],
) {
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;

    let (body, header_version) = match api_key {
        ApiVersionsRequest::API_KEY if api_version > MAX_API_VERSIONS => {
            (encode_api_versions(ErrorCode::UNSUPPORTED_VERSION.0, 0), 0)
        }
        ApiVersionsRequest::API_KEY => (encode_api_versions(ErrorCode::NONE.0, api_version), 0),
        MetadataRequest::API_KEY => {
            let v = api_version.clamp(MetadataRequest::MIN_VERSION, MetadataRequest::MAX_VERSION);
            let leaders = view.leaders.lock().unwrap().clone();
            let mut resp = MetadataResponse::default();
            resp.brokers = view
                .ports
                .iter()
                .enumerate()
                .map(|(i, port)| {
                    let mut broker = MetadataResponseBroker::default();
                    broker.node_id = i32::try_from(i).unwrap_or(0);
                    broker.host = "127.0.0.1".into();
                    broker.port = i32::from(*port);
                    broker
                })
                .collect();
            resp.cluster_id = Some("odradek-harness".into());
            resp.controller_id = 0;
            let mut topic = MetadataResponseTopic::default();
            topic.name = Some(ROUTING_TOPIC.into());
            topic.topic_id = ROUTING_TOPIC_ID;
            topic.partitions = leaders
                .iter()
                .enumerate()
                .map(|(i, leader)| {
                    let mut partition = MetadataResponsePartition::default();
                    partition.partition_index = i32::try_from(i).unwrap_or(0);
                    partition.leader_id = *leader;
                    partition.replica_nodes = vec![*leader];
                    partition.isr_nodes = vec![*leader];
                    partition
                })
                .collect();
            resp.topics = vec![topic];
            let mut buf = BytesMut::new();
            let Ok(()) = resp.encode(&mut buf, v) else {
                return;
            };
            (
                buf.freeze(),
                response_header_version(MetadataRequest::API_KEY, v).unwrap_or(0),
            )
        }
        ProduceRequest::API_KEY => {
            use odradek_protocol::messages::produce_response::{
                PartitionProduceResponse, TopicProduceResponse,
            };
            let v = api_version.clamp(ProduceRequest::MIN_VERSION, ProduceRequest::MAX_VERSION);
            // Echo the addressed topics/partitions, judging each against
            // the current (possibly fault-moved) leadership.
            let mut responses: Vec<TopicProduceResponse> = Vec::new();
            for (topic, partition) in routes {
                let error_code = if topic == ROUTING_TOPIC {
                    view.produce_error(*partition, node_id)
                } else {
                    ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
                };
                let mut entry = PartitionProduceResponse::default();
                entry.index = *partition;
                entry.error_code = error_code;
                entry.base_offset = if error_code == 0 { 0 } else { -1 };
                entry.log_append_time_ms = -1;
                match responses.iter_mut().find(|t| &t.name == topic) {
                    Some(t) => t.partition_responses.push(entry),
                    None => {
                        let mut topic_resp = TopicProduceResponse::default();
                        topic_resp.name = topic.clone();
                        topic_resp.topic_id = if topic == ROUTING_TOPIC {
                            ROUTING_TOPIC_ID
                        } else {
                            [0u8; 16]
                        };
                        topic_resp.partition_responses = vec![entry];
                        responses.push(topic_resp);
                    }
                }
            }
            let mut resp = ProduceResponse::default();
            resp.responses = responses;
            let mut buf = BytesMut::new();
            let Ok(()) = resp.encode(&mut buf, v) else {
                return;
            };
            (
                buf.freeze(),
                response_header_version(ProduceRequest::API_KEY, v).unwrap_or(0),
            )
        }
        FetchRequest::API_KEY => {
            use odradek_protocol::messages::fetch_response::{
                FetchableTopicResponse, PartitionData,
            };
            let v = api_version.clamp(FetchRequest::MIN_VERSION, FetchRequest::MAX_VERSION);
            // Echo the addressed topics/partitions (empty logs), judged
            // against current leadership.
            let mut responses: Vec<FetchableTopicResponse> = Vec::new();
            for (topic, partition) in routes {
                let error_code = if topic == ROUTING_TOPIC {
                    view.fetch_error(*partition, node_id)
                } else {
                    ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
                };
                let mut entry = PartitionData::default();
                entry.partition_index = *partition;
                entry.error_code = error_code;
                entry.high_watermark = 0;
                entry.last_stable_offset = 0;
                entry.log_start_offset = 0;
                entry.records = Some(Bytes::new());
                match responses.iter_mut().find(|t| &t.topic == topic) {
                    Some(t) => t.partitions.push(entry),
                    None => {
                        let mut topic_resp = FetchableTopicResponse::default();
                        topic_resp.topic = topic.clone();
                        topic_resp.topic_id = if topic == ROUTING_TOPIC {
                            ROUTING_TOPIC_ID
                        } else {
                            [0u8; 16]
                        };
                        topic_resp.partitions = vec![entry];
                        responses.push(topic_resp);
                    }
                }
            }
            let mut resp = FetchResponse::default();
            resp.responses = responses;
            let mut buf = BytesMut::new();
            let Ok(()) = resp.encode(&mut buf, v) else {
                return;
            };
            (
                buf.freeze(),
                response_header_version(FetchRequest::API_KEY, v).unwrap_or(0),
            )
        }
        // Unknown api: nothing sensible to say; stay silent.
        _ => return,
    };

    let mut resp_header = ResponseHeader::default();
    resp_header.correlation_id = header.correlation_id;
    let mut out = BytesMut::new();
    let Ok(()) = frame::frame(&mut out, |out| {
        resp_header.encode(out, header_version)?;
        out.extend_from_slice(&body);
        Ok(())
    }) else {
        return;
    };
    let _ = stream.write_all(&out).await;
}

fn encode_api_versions(error_code: i16, version: i16) -> Bytes {
    let mut resp = ApiVersionsResponse::default();
    resp.error_code = error_code;
    resp.api_keys = ADVERTISED
        .iter()
        .map(|&(api_key, min_version, max_version)| {
            let mut v = ApiVersion::default();
            v.api_key = api_key;
            v.min_version = min_version;
            v.max_version = max_version;
            v
        })
        .collect();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)
        .expect("infallible for this shape");
    buf.freeze()
}

/// Evaluate the catalogued Client-role checks over a recorded session.
fn evaluate(session: &Session, subject: &str) -> Report {
    let mut outcomes = Vec::new();
    for check in crate::checks::catalog() {
        if check.role() != SubjectRole::Client {
            continue;
        }
        let Runner::Client(runner) = check.runner else {
            continue;
        };
        outcomes.push(CheckOutcome::new(
            CheckId(check.id.into()),
            check.requirement,
            runner(session),
        ));
    }
    Report::new(subject, outcomes)
}

fn skip(reason: &str) -> Verdict {
    Verdict::Skipped {
        reason: reason.into(),
    }
}

fn describe(o: &Observation) -> String {
    format!(
        "conn #{} request #{} to broker {} (api {} v{})",
        o.conn_id, o.index, o.node_id, o.api_key, o.api_version
    )
}

fn header_well_formed(s: &Session) -> Verdict {
    let failures: Vec<String> = s
        .observations
        .iter()
        .filter_map(|o| {
            o.header_error
                .as_ref()
                .map(|e| format!("{}: {e}", describe(o)))
        })
        .collect();
    if s.observations.is_empty() {
        skip("client sent no requests")
    } else if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: failures.join("; "),
        }
    }
}

/// Per connection — negotiation is a per-connection handshake, not a
/// per-session one.
fn starts_with_api_versions(s: &Session) -> Verdict {
    let mut failures = Vec::new();
    for o in s.observations.iter().filter(|o| o.index == 0) {
        if o.api_key != ApiVersionsRequest::API_KEY {
            failures.push(format!(
                "{}: first request on the connection is not ApiVersions",
                describe(o)
            ));
        }
    }
    if s.observations.is_empty() {
        skip("client sent no requests")
    } else if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: failures.join("; "),
        }
    }
}

/// Scoped per connection.
fn correlation_ids_unique(s: &Session) -> Verdict {
    let mut seen: HashMap<usize, HashSet<i32>> = HashMap::new();
    let mut duplicates = Vec::new();
    for o in &s.observations {
        if let Some(h) = &o.header {
            if !seen.entry(o.conn_id).or_default().insert(h.correlation_id) {
                duplicates.push(format!(
                    "{} reuses correlation id {}",
                    describe(o),
                    h.correlation_id
                ));
            }
        }
    }
    if s.observations.is_empty() {
        skip("client sent no requests")
    } else if duplicates.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: duplicates.join("; "),
        }
    }
}

/// ApiVersions itself is exempt: the probe-and-downgrade dance happens
/// before ranges are known.
fn respects_advertised_versions(s: &Session) -> Verdict {
    let mut violations = Vec::new();
    let mut applicable = 0usize;
    for o in s
        .observations
        .iter()
        .filter(|o| o.api_key != ApiVersionsRequest::API_KEY)
    {
        applicable += 1;
        match ADVERTISED.iter().find(|(k, _, _)| *k == o.api_key) {
            None => violations.push(format!(
                "{} uses an api key the harness never advertised",
                describe(o)
            )),
            Some(&(_, min, max)) if o.api_version < min || o.api_version > max => violations.push(
                format!("{} is outside the advertised {min}-{max}", describe(o)),
            ),
            Some(_) => {}
        }
    }
    if applicable == 0 {
        skip("only ApiVersions requests observed")
    } else if violations.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: violations.join("; "),
        }
    }
}

fn body_decodes(s: &Session) -> Verdict {
    let mut failures = Vec::new();
    let mut applicable = 0usize;
    for o in s.observations.iter().filter(|o| !o.body_exempt) {
        applicable += 1;
        if let Some(e) = &o.body_error {
            failures.push(format!("{}: {e}", describe(o)));
        }
    }
    if applicable == 0 {
        skip("no checkable request bodies observed")
    } else if failures.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: failures.join("; "),
        }
    }
}

/// Partition i of the routing topic starts led by broker i; a staged
/// fault may move it. A misroute is an arrival at a broker that was never
/// that partition's advertised leader.
fn routes_to_partition_leader(s: &Session) -> Verdict {
    let ever_led = |partition: i32, node: i32| {
        node == partition
            || s.events
                .iter()
                .any(|e| e.partition == partition && e.to_node == node)
    };
    let mut misroutes = Vec::new();
    let mut routed = 0usize;
    for o in &s.observations {
        for (topic, partition) in &o.routes {
            if topic != ROUTING_TOPIC {
                continue;
            }
            routed += 1;
            if !ever_led(*partition, o.node_id) {
                misroutes.push(format!(
                    "{} addresses {topic}[{partition}], which broker {} \
                     was never advertised as leading",
                    describe(o),
                    o.node_id
                ));
            }
        }
    }
    if routed == 0 {
        skip("no produce/fetch for the routing topic observed")
    } else if misroutes.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: misroutes.join("; "),
        }
    }
}

/// Only meaningful when the leader-move fault is armed and actually
/// fired.
fn recovers_from_leader_change(s: &Session) -> Verdict {
    let mut unrecovered = Vec::new();
    for event in &s.events {
        let redelivered = s.observations.iter().any(|o| {
            o.node_id == event.to_node
                && o.routes
                    .iter()
                    .any(|(t, p)| t == ROUTING_TOPIC && *p == event.partition)
        });
        if !redelivered {
            unrecovered.push(format!(
                "after NOT_LEADER moved {ROUTING_TOPIC}[{}]'s leadership to \
                 broker {}, no produce/fetch ever reached it there",
                event.partition, event.to_node
            ));
        }
    }
    if s.fault != Some(HarnessFault::LeaderMove) {
        skip("leader-move fault not armed")
    } else if s.events.is_empty() {
        skip("the client sent nothing that triggered the leader move")
    } else if unrecovered.is_empty() {
        Verdict::Pass
    } else {
        Verdict::Fail {
            details: unrecovered.join("; "),
        }
    }
}
