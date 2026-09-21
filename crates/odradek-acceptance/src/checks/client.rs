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
use odradek_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use odradek_protocol::messages::list_offsets_request::ListOffsetsRequest;
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use odradek_protocol::messages::produce_request::ProduceRequest;
use odradek_protocol::messages::produce_response::ProduceResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use odradek_protocol::wire::RawTaggedField;
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
    // An idempotent producer asks for an id before it produces
    // anything, so a harness that does not offer this is one no
    // default-configured client can produce to.
    (
        InitProducerIdRequest::API_KEY,
        InitProducerIdRequest::MIN_VERSION,
        InitProducerIdRequest::MAX_VERSION,
    ),
    (
        SaslHandshakeRequest::API_KEY,
        SaslHandshakeRequest::MIN_VERSION,
        SaslHandshakeRequest::MAX_VERSION,
    ),
    (
        SaslAuthenticateRequest::API_KEY,
        SaslAuthenticateRequest::MIN_VERSION,
        SaslAuthenticateRequest::MAX_VERSION,
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
    // A consumer asks where to start before it fetches anything, so a
    // harness that does not offer this is one no consumer can reach
    // the fetch path of — which left half of
    // `client/routes-to-partition-leader` ("produce *and* fetch") with
    // no third-party client able to exercise it.
    (
        ListOffsetsRequest::API_KEY,
        ListOffsetsRequest::MIN_VERSION,
        ListOffsetsRequest::MAX_VERSION,
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
    Check {
        id: "client/tolerates-unknown-tagged-fields",
        requirement: "keeps going after a response carrying a tagged field it \
                      does not know, which is what the tagged-field section is \
                      for",
        runner: Runner::Client(tolerates_unknown_tagged_fields),
    },
    Check {
        id: "client/honours-throttle-time",
        requirement: "waits out a response's throttle_time_ms before sending \
                      again on that connection, instead of pushing into a \
                      broker that has stopped reading",
        runner: Runner::Client(honours_throttle_time),
    },
    Check {
        id: "client/heeds-sasl-rejection",
        requirement: "after a SASL token is refused, the client sends no data \
                      api on that connection — a refusal shaped like success is \
                      still a refusal",
        runner: Runner::Client(heeds_sasl_rejection),
    },
    Check {
        id: "client/acknowledges-sasl-rejection",
        requirement: "after a SASL token is refused, the client answers once \
                      more so the server can close the exchange (RFC 7628 §3.1)",
        runner: Runner::Client(acknowledges_sasl_rejection),
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
    /// Offer OAUTHBEARER and refuse whatever token arrives, the way
    /// RFC 7628 §3.1 says to: a *success-shaped* response carrying a
    /// JSON description of the problem, not an error code.
    ///
    /// The shape is the trap. A client that checks only the error code
    /// sees zero and believes it authenticated; the failure then
    /// surfaces later as a state error naming nothing. That is a bug
    /// this project wrote and caught against a live broker, and these
    /// checks exist to make it unrepeatable.
    RejectSaslToken,

    /// Carry a tagged field no released Kafka version defines, the way
    /// a broker newer than the client does.
    ///
    /// Forward compatibility is the tagged-field section's whole
    /// purpose: a client is supposed to keep what it does not
    /// understand and carry on. One that refuses instead breaks against
    /// every broker newer than itself, and breaks on upgrade day rather
    /// than in anybody's test suite.
    UnknownTaggedField,

    /// Answer with a non-zero `throttle_time_ms`, the way a broker
    /// enforcing a quota does.
    ///
    /// The broker then stops reading this connection for that long, so
    /// a client that ignores the field does not go faster — its next
    /// request sits in a socket buffer until the mute lifts, which is
    /// indistinguishable from a hung broker and spends the request
    /// timeout instead of a backoff.
    Throttle,
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
    /// When the frame arrived, as milliseconds since the session began.
    at_ms: u64,
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
    /// The mechanism a SaslHandshake asked for, if this is one.
    sasl_mechanism: Option<String>,
}

/// The SASL apis the harness offers, so a client configured for
/// OAUTHBEARER has something to negotiate with.
/// The pause a throttled response asks for.
///
/// Long enough that a client which ignores it is unmistakable, short
/// enough not to dominate the suite's own runtime.
const THROTTLE_MS: i32 = 400;

/// Metadata carries `throttle_time_ms` from v3.
const METADATA_THROTTLE_MIN: i16 = 3;

/// Metadata gained a tagged-field section at v9.
const METADATA_FLEXIBLE_MIN: i16 = 9;

/// The first ApiVersions response whose *body* is flexible. Its header
/// stays v0 at every version — the quirk the protocol crate documents —
/// so the tagged-field section lives in the body alone.
const API_VERSIONS_FLEXIBLE_MIN: i16 = 3;

/// A tag far above anything the schemas define, so it can only be
/// something the client has never heard of.
const UNKNOWN_TAG: u32 = 60_000;

const SASL_HANDSHAKE_API: i16 = 17;
const SASL_AUTHENTICATE_API: i16 = 36;

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
    /// `(connection, request index)` where a SASL token was refused.
    sasl_rejections: Vec<(usize, usize)>,
    /// `(connection, request index, when the answer went out)` for every
    /// throttled response.
    throttles: Vec<(usize, usize, u64)>,
    /// `(connection, request index)` for every response that carried an
    /// unknown tagged field.
    tagged: Vec<(usize, usize)>,
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
    /// Where a SASL token was refused: (connection, request index).
    /// What the client does *after* this point is the whole question.
    sasl_rejections: std::sync::Mutex<Vec<(usize, usize)>>,
    /// `(connection, request index, when the answer went out)` for every
    /// response that carried a throttle.
    throttles: std::sync::Mutex<Vec<(usize, usize, u64)>>,
    /// The session's zero point, for `at_ms`.
    started: std::time::Instant,
    /// `(connection, request index)` for every response that carried an
    /// unknown tagged field.
    tagged: std::sync::Mutex<Vec<(usize, usize)>>,
    /// Connections whose SaslHandshake named a mechanism this harness
    /// offers, and which may therefore be spoken to in that mechanism's
    /// terms. See [`OFFERED_MECHANISM`].
    negotiated: std::sync::Mutex<HashSet<usize>>,
}

/// The one mechanism the harness knows how to be wrong in.
///
/// Only OAUTHBEARER, because the refusal
/// [`RejectSaslToken`](HarnessFault::RejectSaslToken) injects is an
/// OAUTHBEARER construct — RFC 7628 §3.1's success-shaped failure
/// challenge — and no other mechanism has one. Under PLAIN a failure is
/// the response's error code and nothing else, so the same bytes mean
/// "authenticated" rather than "refused", and a client that carries on
/// is right to.
const OFFERED_MECHANISM: &str = "OAUTHBEARER";

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
        sasl_rejections: std::sync::Mutex::new(Vec::new()),
        negotiated: std::sync::Mutex::new(HashSet::new()),
        throttles: std::sync::Mutex::new(Vec::new()),
        tagged: std::sync::Mutex::new(Vec::new()),
        started: std::time::Instant::now(),
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
        sasl_rejections: view.sasl_rejections.lock().unwrap().clone(),
        throttles: view.throttles.lock().unwrap().clone(),
        tagged: view.tagged.lock().unwrap().clone(),
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
        let at_ms = u64::try_from(view.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let obs = parse_request(node_id, conn_id, index, at_ms, frame);
        index += 1;
        if let Some(header) = &obs.header {
            respond(
                &mut stream,
                Answering {
                    header,
                    routes: &obs.routes,
                    conn_id,
                    index: obs.index,
                    sasl_mechanism: obs.sasl_mechanism.as_deref(),
                },
                &view,
                node_id,
            )
            .await;
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

fn parse_request(
    node_id: i32,
    conn_id: usize,
    index: usize,
    at_ms: u64,
    frame: Bytes,
) -> Observation {
    let mut obs = Observation {
        node_id,
        conn_id,
        index,
        at_ms,
        api_key: -1,
        api_version: -1,
        header: None,
        header_error: None,
        body_error: None,
        body_exempt: true,
        sasl_mechanism: None,
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
        (ListOffsetsRequest::API_KEY, Some(buf)) => {
            obs.body_exempt = false;
            match decode_fully::<ListOffsetsRequest>(buf, api_version) {
                Err(e) => obs.body_error = Some(e),
                Ok(req) => {
                    for topic in &req.topics {
                        for p in &topic.partitions {
                            obs.routes.push((topic.name.clone(), p.partition_index));
                        }
                    }
                }
            }
        }
        (SASL_HANDSHAKE_API, Some(buf)) => {
            obs.body_exempt = false;
            // The mechanism is carried out of here because `respond` is
            // handed the header and not the body, and answering a
            // handshake correctly means answering the mechanism that
            // was actually asked for.
            match decode_fully::<SaslHandshakeRequest>(buf, api_version) {
                Err(e) => obs.body_error = Some(e),
                Ok(req) => obs.sasl_mechanism = Some(req.mechanism),
            }
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

/// Everything the harness knows about the request it is answering.
///
/// A struct rather than six parameters: they all come from the same
/// [`Observation`] and are only ever passed together, and the list had
/// grown past the point where the call site said anything.
#[derive(Clone, Copy)]
struct Answering<'a> {
    header: &'a RequestHeader,
    /// `(topic, partition)` pairs this request addresses.
    routes: &'a [(String, i32)],
    conn_id: usize,
    index: usize,
    /// The mechanism a SaslHandshake asked for, if this is one.
    sasl_mechanism: Option<&'a str>,
}

async fn respond(stream: &mut TcpStream, req: Answering<'_>, view: &ClusterView, node_id: i32) {
    let Answering {
        header,
        routes,
        conn_id,
        index,
        sasl_mechanism,
    } = req;
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;

    let (body, header_version) = match api_key {
        ApiVersionsRequest::API_KEY if api_version > MAX_API_VERSIONS => {
            // A version the harness cannot speak is refused at v0, and
            // a v0 body has nowhere to put a tagged field anyway.
            (
                encode_api_versions(ErrorCode::UNSUPPORTED_VERSION.0, 0, None),
                0,
            )
        }
        ApiVersionsRequest::API_KEY => (
            encode_api_versions(ErrorCode::NONE.0, api_version, Some((view, conn_id, index))),
            0,
        ),
        MetadataRequest::API_KEY => {
            let v = api_version.clamp(MetadataRequest::MIN_VERSION, MetadataRequest::MAX_VERSION);
            let leaders = view.leaders.lock().unwrap().clone();
            let mut resp = MetadataResponse::default();
            // Metadata carries the throttle from v3 on, and every client
            // asks for it early and often — so it is where a quota pause
            // is most likely to reach one.
            if view.fault == Some(HarnessFault::UnknownTaggedField) && v >= METADATA_FLEXIBLE_MIN {
                resp.unknown_tagged_fields.push(RawTaggedField {
                    tag: UNKNOWN_TAG,
                    data: Bytes::from_static(b"from a newer broker"),
                });
                view.tagged.lock().unwrap().push((conn_id, index));
            }
            if view.fault == Some(HarnessFault::Throttle) && v >= METADATA_THROTTLE_MIN {
                resp.throttle_time_ms = THROTTLE_MS;
                let sent_at = u64::try_from(view.started.elapsed().as_millis()).unwrap_or(u64::MAX);
                view.throttles
                    .lock()
                    .unwrap()
                    .push((conn_id, index, sent_at));
            }
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
        SASL_HANDSHAKE_API => {
            let v = api_version.clamp(
                SaslHandshakeRequest::MIN_VERSION,
                SaslHandshakeRequest::MAX_VERSION,
            );
            let mut resp = SaslHandshakeResponse::default();
            resp.mechanisms = vec![OFFERED_MECHANISM.to_owned()];
            // Answer the mechanism the client actually asked for. This
            // used to say NONE to everything while advertising one
            // mechanism, which told a client asking for PLAIN that
            // PLAIN was agreed and then spoke OAUTHBEARER to it — and
            // failed two checks against a client that had done nothing
            // wrong. A broker answers UNSUPPORTED_SASL_MECHANISM here
            // and the client stops; so does this.
            match sasl_mechanism {
                Some(OFFERED_MECHANISM) => {
                    resp.error_code = ErrorCode::NONE.0;
                    view.negotiated.lock().unwrap().insert(conn_id);
                }
                _ => resp.error_code = ErrorCode::UNSUPPORTED_SASL_MECHANISM.0,
            }
            let mut body = BytesMut::new();
            if resp.encode(&mut body, v).is_err() {
                return;
            }
            (
                body.freeze(),
                response_header_version(SASL_HANDSHAKE_API, v).unwrap_or(0),
            )
        }
        SASL_AUTHENTICATE_API => {
            let v = api_version.clamp(
                SaslAuthenticateRequest::MIN_VERSION,
                SaslAuthenticateRequest::MAX_VERSION,
            );
            let mut resp = SaslAuthenticateResponse::default();
            resp.error_code = ErrorCode::NONE.0;
            // Only where the mechanism was agreed. A client that was
            // refused at the handshake and kept going anyway is not
            // being asked an OAUTHBEARER question.
            let negotiated = view.negotiated.lock().unwrap().contains(&conn_id);
            if negotiated && view.fault == Some(HarnessFault::RejectSaslToken) {
                let already = view
                    .sasl_rejections
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(c, _)| *c == conn_id);
                if !already {
                    // The RFC 7628 failure challenge: success-shaped,
                    // carrying the reason. Recorded so the checks can
                    // ask what the client did next.
                    resp.auth_bytes = Bytes::from_static(b"{\"status\":\"invalid_token\"}");
                    view.sasl_rejections.lock().unwrap().push((conn_id, index));
                }
            }
            let mut body = BytesMut::new();
            if resp.encode(&mut body, v).is_err() {
                return;
            }
            (
                body.freeze(),
                response_header_version(SASL_AUTHENTICATE_API, v).unwrap_or(0),
            )
        }
        InitProducerIdRequest::API_KEY => {
            use odradek_protocol::messages::init_producer_id_response::InitProducerIdResponse;
            let v = api_version.clamp(
                InitProducerIdRequest::MIN_VERSION,
                InitProducerIdRequest::MAX_VERSION,
            );
            // One id for the harness's lifetime. Nothing here checks
            // sequences — that is the broker's job and this is not one —
            // but a client that asks has to be answered, or it cannot
            // produce at all.
            let mut resp = InitProducerIdResponse::default();
            resp.error_code = ErrorCode::NONE.0;
            resp.producer_id = 1000;
            resp.producer_epoch = 0;
            let mut body = BytesMut::new();
            if resp.encode(&mut body, v).is_err() {
                return;
            }
            (
                body.freeze(),
                response_header_version(InitProducerIdRequest::API_KEY, v).unwrap_or(0),
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
        ListOffsetsRequest::API_KEY => {
            use odradek_protocol::messages::list_offsets_response::{
                ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
            };
            let v = api_version.clamp(
                ListOffsetsRequest::MIN_VERSION,
                ListOffsetsRequest::MAX_VERSION,
            );
            // Every log here is empty, so every question about where it
            // starts or ends has the same answer. Judged against
            // current leadership like produce and fetch are, so a
            // consumer asking the wrong broker is told so and the
            // routing check sees it.
            let mut topics: Vec<ListOffsetsTopicResponse> = Vec::new();
            for (topic, partition) in routes {
                let error_code = if topic == ROUTING_TOPIC {
                    view.fetch_error(*partition, node_id)
                } else {
                    ErrorCode::UNKNOWN_TOPIC_OR_PARTITION.0
                };
                let mut entry = ListOffsetsPartitionResponse::default();
                entry.partition_index = *partition;
                entry.error_code = error_code;
                entry.timestamp = -1;
                entry.offset = 0;
                entry.leader_epoch = -1;
                match topics.iter_mut().find(|t| &t.name == topic) {
                    Some(t) => t.partitions.push(entry),
                    None => {
                        let mut topic_resp = ListOffsetsTopicResponse::default();
                        topic_resp.name = topic.clone();
                        topic_resp.partitions = vec![entry];
                        topics.push(topic_resp);
                    }
                }
            }
            let mut resp =
                odradek_protocol::messages::list_offsets_response::ListOffsetsResponse::default();
            resp.topics = topics;
            let mut body = BytesMut::new();
            if resp.encode(&mut body, v).is_err() {
                return;
            }
            (
                body.freeze(),
                response_header_version(ListOffsetsRequest::API_KEY, v).unwrap_or(0),
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

fn encode_api_versions(
    error_code: i16,
    version: i16,
    tag: Option<(&ClusterView, usize, usize)>,
) -> Bytes {
    let mut resp = ApiVersionsResponse::default();
    resp.error_code = error_code;
    // ApiVersions is the *other* place a tagged field can reach a
    // client, and by some distance the better one: its body is
    // flexible from v3, every client sends it, and every client parses
    // the answer before it can do anything else. Metadata alone was
    // not enough — librdkafka asks for Metadata v4 and nothing higher,
    // so the flexible section this fault needs does not exist in its
    // Metadata responses and the check could only ever skip against
    // the most widely deployed client there is.
    if let Some((view, conn_id, index)) = tag {
        if view.fault == Some(HarnessFault::UnknownTaggedField)
            && version >= API_VERSIONS_FLEXIBLE_MIN
        {
            resp.unknown_tagged_fields.push(RawTaggedField {
                tag: UNKNOWN_TAG,
                data: Bytes::from_static(b"from a newer broker"),
            });
            view.tagged.lock().unwrap().push((conn_id, index));
        }
    }
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
/// The apis a client may still send on a connection whose SASL token
/// was refused: another SASL message, and nothing else.
fn is_sasl_api(api_key: i16) -> bool {
    api_key == SASL_HANDSHAKE_API
        || api_key == SASL_AUTHENTICATE_API
        || api_key == ApiVersionsRequest::API_KEY
}

/// A refusal shaped like success is still a refusal.
///
/// The client asked to authenticate and was told no. Everything it does
/// on that connection afterwards is unauthenticated, and a client that
/// produces anyway has not merely failed — it believes it succeeded,
/// which is the state in which data goes somewhere nobody checked.
fn heeds_sasl_rejection(s: &Session) -> Verdict {
    if s.sasl_rejections.is_empty() {
        return Verdict::Skipped {
            reason: "no SASL token was refused in this session".into(),
        };
    }
    for (conn_id, index) in &s.sasl_rejections {
        let after: Vec<&Observation> = s
            .observations
            .iter()
            .filter(|o| o.conn_id == *conn_id && o.index > *index)
            .collect();
        if let Some(bad) = after.iter().find(|o| !is_sasl_api(o.api_key)) {
            return Verdict::Fail {
                details: format!(
                    "token refused on connection {conn_id}, then api {} was sent on it \
                     anyway: the client read a success-shaped refusal as success",
                    bad.api_key
                ),
            };
        }
    }
    Verdict::Pass
}

/// RFC 7628 §3.1: answer the refusal so the server can close.
///
/// One more message, carrying nothing. Skip it and the server is left
/// mid-exchange, and the failure it eventually reports is a timeout
/// rather than the reason it already knows.
fn acknowledges_sasl_rejection(s: &Session) -> Verdict {
    if s.sasl_rejections.is_empty() {
        return Verdict::Skipped {
            reason: "no SASL token was refused in this session".into(),
        };
    }
    for (conn_id, index) in &s.sasl_rejections {
        let acked = s.observations.iter().any(|o| {
            o.conn_id == *conn_id && o.index > *index && o.api_key == SASL_AUTHENTICATE_API
        });
        if !acked {
            return Verdict::Fail {
                details: format!(
                    "token refused on connection {conn_id} and the client said nothing \
                     more, leaving the exchange open"
                ),
            };
        }
    }
    Verdict::Pass
}

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

/// A throttled client must pause before it sends again.
///
/// Quota enforcement is not advice a client can decline. The broker
/// answers, sets `throttle_time_ms`, and then stops reading this
/// connection for that long — so a client that ignores the field does
/// not get its request in sooner, it gets it in *later*, sitting in a
/// socket buffer while its own request timeout runs down. The failure
/// looks like an unreliable broker from the inside.
///
/// Judged on the connection the throttle arrived on: a pause is
/// per connection, and a client with work for another broker is right
/// to keep going there.
fn honours_throttle_time(session: &Session) -> Verdict {
    if session.throttles.is_empty() {
        return Verdict::Skipped {
            reason: "no response carried a throttle in this session".into(),
        };
    }
    // The first throttle the client actually had a chance to observe.
    // Clients open several connections — bootstrap, control, one per
    // partition leader — and a throttle on one the client never speaks
    // to again says nothing, which is not the same as passing.
    let Some((sent_at, spoke)) = session
        .throttles
        .iter()
        .find_map(|&(conn_id, index, sent_at)| {
            let after: Vec<&Observation> = session
                .observations
                .iter()
                .filter(|obs| obs.conn_id == conn_id && obs.index > index)
                .collect();
            (!after.is_empty()).then_some((sent_at, after))
        })
    else {
        return Verdict::Skipped {
            reason: "every throttled connection went quiet afterwards, so there was no \
                     pause to observe"
                .into(),
        };
    };

    // Judged on the *back half* of the window, not on the next request.
    // A client may have requests on the wire already when the throttled
    // answer arrives — Kafka's own producer does — and those are not
    // violations, they crossed in flight. What no client has an excuse
    // for is still talking once those have drained: anything arriving
    // this deep into the window was sent by a client that had read the
    // throttle and kept going.
    // The middle of the window, not the whole of it. The front is left
    // to requests crossing in flight; the tail is left to a client
    // resuming a little early, which is a rounding difference rather
    // than a refusal to wait.
    let window = u64::try_from(THROTTLE_MS).unwrap_or(0);
    let enforced_from = sent_at + window / 2;
    let enforced_until = sent_at + window - window / 8;
    let Some(offender) = spoke
        .iter()
        .find(|obs| obs.at_ms >= enforced_from && obs.at_ms < enforced_until)
    else {
        return Verdict::Pass;
    };
    Verdict::Fail {
        details: format!(
            "answered with throttle_time_ms={THROTTLE_MS} and the client was still sending \
             {}ms into the pause (api {} v{}); requests already in flight are fair, but by \
             now they have drained and the broker is not reading yet — that request waits \
             out the mute instead of the client waiting out the throttle",
            offender.at_ms.saturating_sub(sent_at),
            offender.api_key,
            offender.api_version
        ),
    }
}

/// A client must carry on past a tagged field it does not know.
///
/// This is what the tagged-field section is *for*: brokers add fields
/// without a version bump, and every client older than the addition is
/// expected to keep them and proceed. One that refuses instead breaks
/// against every broker newer than itself — and breaks on upgrade day,
/// in somebody's cluster, rather than in a test suite.
///
/// Judged by whether the client went on working: it asked for
/// something, was answered with a field from the future, and either
/// carried on or did not.
fn tolerates_unknown_tagged_fields(session: &Session) -> Verdict {
    let Some(&(conn_id, index)) = session.tagged.first() else {
        return Verdict::Skipped {
            reason: "no response carried an unknown tagged field in this session".into(),
        };
    };
    let carried_on = session
        .observations
        .iter()
        .any(|obs| obs.conn_id == conn_id && obs.index > index);
    if carried_on {
        return Verdict::Pass;
    }
    // Another connection is enough too: a client that reconnects rather
    // than reusing the connection has still tolerated the field.
    if session
        .observations
        .iter()
        .any(|obs| obs.conn_id != conn_id)
    {
        return Verdict::Pass;
    }
    Verdict::Fail {
        details: format!(
            "answered request {index} on connection {conn_id} with tag {UNKNOWN_TAG}, a field \
             no schema defines, and the client sent nothing further anywhere; a client that \
             stops at a field from the future stops at every broker newer than itself"
        ),
    }
}
