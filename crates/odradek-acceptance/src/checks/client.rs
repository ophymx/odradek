//! Checks that run against a client under test (the suite acts as server).
//!
//! The harness impersonates a small cluster: the bootstrap listener plus
//! two more ephemeral listeners, presented in Metadata as brokers 0-2.
//! One topic ([`ROUTING_TOPIC`]) spans three partitions, partition `i`
//! led by broker `i`, so leader routing is observable. Every broker
//! answers just enough of the protocol to keep a real client talking
//! (ApiVersions, Metadata, and empty Produce/Fetch successes) and records
//! every frame; checks are evaluated over the recorded observations.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::report::{CheckOutcome, Report};
use crate::{CheckId, Verdict};

/// (api key, min, max) the harness advertises — exactly the apis it can
/// parse, so version discipline is checkable.
const ADVERTISED: &[(i16, i16, i16)] = &[(18, 0, 4), (0, 3, 12), (1, 4, 17), (3, 0, 13)];

const MAX_API_VERSIONS: i16 = 4;

/// How many brokers the harness impersonates.
pub const BROKER_COUNT: i32 = 3;

/// The topic the harness advertises for routing observation: one
/// partition per broker, partition `i` led by broker `i`.
pub const ROUTING_TOPIC: &str = "odradek-routing";

/// The routing topic's id, for id-addressed clients (16 bytes).
pub const ROUTING_TOPIC_ID: [u8; 16] = *b"odradek-routing!";

/// Limits for one observation session.
#[derive(Debug, Clone)]
pub struct ObserveConfig {
    /// Stop after this many requests (the checks need finite input).
    pub max_requests: usize,
    /// Stop when every connection goes quiet for this long.
    pub idle_timeout: Duration,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        ObserveConfig {
            max_requests: 32,
            idle_timeout: Duration::from_secs(3),
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

/// The impersonated cluster's endpoints; broker `i` listens on
/// `ports[i]`.
struct ClusterView {
    ports: Vec<u16>,
}

/// Accept client connections (bootstrap on `listener`, brokers 1+ on
/// internal listeners), observe every frame, and evaluate the checks.
pub async fn run(listener: &TcpListener, config: &ObserveConfig) -> Report {
    let fail_report = |details: String| Report {
        subject: "client <none>".into(),
        outcomes: vec![CheckOutcome {
            id: CheckId("client/session".into()),
            requirement: "a client connects to the harness",
            verdict: Verdict::Fail { details },
        }],
    };

    // Brokers 1..N listen on ephemeral ports next to the bootstrap.
    let mut extra = Vec::new();
    for _ in 1..BROKER_COUNT {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(l) => extra.push(l),
            Err(e) => return fail_report(format!("cannot bind harness broker: {e}")),
        }
    }
    let mut ports = vec![match listener.local_addr() {
        Ok(a) => a.port(),
        Err(e) => return fail_report(format!("bootstrap listener: {e}")),
    }];
    ports.extend(
        extra
            .iter()
            .filter_map(|l| l.local_addr().ok().map(|a| a.port())),
    );
    let view = Arc::new(ClusterView { ports });

    let (tx, mut rx) = mpsc::unbounded_channel();
    let conn_counter = Arc::new(AtomicUsize::new(0));
    let mut accept_tasks = Vec::new();
    for (i, l) in extra.into_iter().enumerate() {
        let node_id = i32::try_from(i).unwrap_or(0) + 1;
        let view = Arc::clone(&view);
        let tx = tx.clone();
        let conn_counter = Arc::clone(&conn_counter);
        accept_tasks.push(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = l.accept().await else {
                    return;
                };
                let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(handle_conn(
                    stream,
                    node_id,
                    conn_id,
                    Arc::clone(&view),
                    tx.clone(),
                ));
            }
        }));
    }

    // Wait (without a deadline, as ever) for the client's first
    // connection — necessarily to the bootstrap, the only address it has.
    let peer = match listener.accept().await {
        Ok((stream, peer)) => {
            let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(handle_conn(
                stream,
                0,
                conn_id,
                Arc::clone(&view),
                tx.clone(),
            ));
            peer
        }
        Err(e) => {
            for t in &accept_tasks {
                t.abort();
            }
            return fail_report(format!("accept failed: {e}"));
        }
    };

    // Collect observations until the whole session goes idle.
    let mut observations = Vec::new();
    while observations.len() < config.max_requests {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((stream, _)) = accepted {
                    let conn_id = conn_counter.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(handle_conn(stream, 0, conn_id, Arc::clone(&view), tx.clone()));
                }
            }
            obs = rx.recv() => match obs {
                Some(obs) => observations.push(obs),
                None => break,
            },
            () = tokio::time::sleep(config.idle_timeout) => break,
        }
    }
    for t in &accept_tasks {
        t.abort();
    }
    evaluate(&observations, &format!("client {peer}"))
}

async fn handle_conn(
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
            respond(&mut stream, header, &view).await;
        }
        if tx.send(obs).is_err() {
            return;
        }
    }
}

async fn read_frame(stream: &mut TcpStream) -> Option<Bytes> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.ok()?;
    let len = i32::from_be_bytes(len_bytes);
    if !(0..=crate::raw::MAX_FRAME_SIZE).contains(&len) {
        return None;
    }
    let mut frame = vec![0u8; len as usize];
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

    let known_probe_version = if api_key == 18 {
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
        (18, Some(_)) if api_version > MAX_API_VERSIONS => {}
        (18, Some(buf)) => {
            obs.body_exempt = false;
            obs.body_error = decode_fully::<ApiVersionsRequest>(buf, api_version).err();
        }
        (0, Some(buf)) => {
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
        (1, Some(buf)) => {
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
        (3, Some(buf)) => {
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

trait DecodeBody: Sized {
    fn decode_body(buf: &mut Bytes, version: i16) -> Result<Self, odradek_protocol::DecodeError>;
}

macro_rules! impl_decode_body {
    ($($ty:ty),+) => {
        $(impl DecodeBody for $ty {
            fn decode_body(
                buf: &mut Bytes,
                version: i16,
            ) -> Result<Self, odradek_protocol::DecodeError> {
                Self::decode(buf, version)
            }
        })+
    };
}
impl_decode_body!(
    ApiVersionsRequest,
    ProduceRequest,
    FetchRequest,
    MetadataRequest
);

fn decode_fully<T: DecodeBody>(buf: &mut Bytes, version: i16) -> Result<T, String> {
    match T::decode_body(buf, version) {
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

async fn respond(stream: &mut TcpStream, header: &RequestHeader, view: &ClusterView) {
    let api_key = header.request_api_key;
    let api_version = header.request_api_version;

    let (body, header_version) = match api_key {
        18 if api_version > MAX_API_VERSIONS => (encode_api_versions(35, 0), 0),
        18 => (encode_api_versions(0, api_version), 0),
        3 => {
            let v = api_version.clamp(0, 13);
            let resp = MetadataResponse {
                brokers: view
                    .ports
                    .iter()
                    .enumerate()
                    .map(|(i, port)| MetadataResponseBroker {
                        node_id: i32::try_from(i).unwrap_or(0),
                        host: "127.0.0.1".into(),
                        port: i32::from(*port),
                        ..Default::default()
                    })
                    .collect(),
                cluster_id: Some("odradek-harness".into()),
                controller_id: 0,
                topics: vec![MetadataResponseTopic {
                    name: Some(ROUTING_TOPIC.into()),
                    topic_id: ROUTING_TOPIC_ID,
                    partitions: (0..BROKER_COUNT)
                        .map(|i| MetadataResponsePartition {
                            partition_index: i,
                            leader_id: i,
                            replica_nodes: vec![i],
                            isr_nodes: vec![i],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            let Ok(()) = resp.encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(3, v).unwrap_or(0))
        }
        0 => {
            let v = api_version.clamp(3, 12);
            let mut buf = BytesMut::new();
            let Ok(()) = ProduceResponse::default().encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(0, v).unwrap_or(0))
        }
        1 => {
            let v = api_version.clamp(4, 17);
            let mut buf = BytesMut::new();
            let Ok(()) = FetchResponse::default().encode(&mut buf, v) else {
                return;
            };
            (buf.freeze(), response_header_version(1, v).unwrap_or(0))
        }
        // Unknown api: nothing sensible to say; stay silent.
        _ => return,
    };

    let resp_header = ResponseHeader {
        correlation_id: header.correlation_id,
        unknown_tagged_fields: Vec::new(),
    };
    let mut out = BytesMut::new();
    out.put_i32(0);
    let Ok(()) = resp_header.encode(&mut out, header_version) else {
        return;
    };
    out.extend_from_slice(&body);
    let Ok(len) = i32::try_from(out.len() - 4) else {
        return;
    };
    out[..4].copy_from_slice(&len.to_be_bytes());
    let _ = stream.write_all(&out).await;
}

fn encode_api_versions(error_code: i16, version: i16) -> Bytes {
    let resp = ApiVersionsResponse {
        error_code,
        api_keys: ADVERTISED
            .iter()
            .map(|&(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, version)
        .expect("infallible for this shape");
    buf.freeze()
}

fn evaluate(observations: &[Observation], subject: &str) -> Report {
    let mut outcomes = Vec::new();
    let none_observed = observations.is_empty();
    let skip = |reason: &str| Verdict::Skipped {
        reason: reason.into(),
    };
    let describe = |o: &Observation| {
        format!(
            "conn #{} request #{} to broker {} (api {} v{})",
            o.conn_id, o.index, o.node_id, o.api_key, o.api_version
        )
    };

    // client/header-well-formed
    let header_failures: Vec<String> = observations
        .iter()
        .filter_map(|o| {
            o.header_error
                .as_ref()
                .map(|e| format!("{}: {e}", describe(o)))
        })
        .collect();
    outcomes.push(CheckOutcome {
        id: CheckId("client/header-well-formed".into()),
        requirement: "every request carries a decodable header at the version \
                      implied by its (api key, api version)",
        verdict: if none_observed {
            skip("client sent no requests")
        } else if header_failures.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: header_failures.join("; "),
            }
        },
    });

    // client/starts-with-api-versions: per connection — negotiation is a
    // per-connection handshake, not a per-session one.
    let mut handshake_failures = Vec::new();
    for o in observations.iter().filter(|o| o.index == 0) {
        if o.api_key != 18 {
            handshake_failures.push(format!(
                "{}: first request on the connection is not ApiVersions",
                describe(o)
            ));
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/starts-with-api-versions".into()),
        requirement: "the first request on every connection is ApiVersions, so \
                      versions are negotiated before anything else is sent",
        verdict: if none_observed {
            skip("client sent no requests")
        } else if handshake_failures.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: handshake_failures.join("; "),
            }
        },
    });

    // client/correlation-ids-unique (scoped per connection).
    let mut seen: HashMap<usize, HashSet<i32>> = HashMap::new();
    let mut duplicates = Vec::new();
    for o in observations {
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
    outcomes.push(CheckOutcome {
        id: CheckId("client/correlation-ids-unique".into()),
        requirement: "correlation ids are not reused within a connection, so \
                      responses are unambiguously attributable",
        verdict: if none_observed {
            skip("client sent no requests")
        } else if duplicates.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: duplicates.join("; "),
            }
        },
    });

    // client/respects-advertised-versions (ApiVersions itself exempt: the
    // probe-and-downgrade dance happens before ranges are known).
    let mut range_violations = Vec::new();
    let mut applicable = 0usize;
    for o in observations.iter().filter(|o| o.api_key != 18) {
        applicable += 1;
        match ADVERTISED.iter().find(|(k, _, _)| *k == o.api_key) {
            None => range_violations.push(format!(
                "{} uses an api key the harness never advertised",
                describe(o)
            )),
            Some(&(_, min, max)) if o.api_version < min || o.api_version > max => range_violations
                .push(format!(
                    "{} is outside the advertised {min}-{max}",
                    describe(o)
                )),
            Some(_) => {}
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/respects-advertised-versions".into()),
        requirement: "after negotiation the client only sends apis and \
                      versions the server advertised",
        verdict: if applicable == 0 {
            skip("only ApiVersions requests observed")
        } else if range_violations.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: range_violations.join("; "),
            }
        },
    });

    // client/body-decodes
    let mut body_failures = Vec::new();
    let mut body_applicable = 0usize;
    for o in observations.iter().filter(|o| !o.body_exempt) {
        body_applicable += 1;
        if let Some(e) = &o.body_error {
            body_failures.push(format!("{}: {e}", describe(o)));
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/body-decodes".into()),
        requirement: "request bodies decode per the message schema at the \
                      claimed version, with no trailing bytes",
        verdict: if body_applicable == 0 {
            skip("no checkable request bodies observed")
        } else if body_failures.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: body_failures.join("; "),
            }
        },
    });

    // client/routes-to-partition-leader: partition i of the routing topic
    // is led by broker i; produce/fetch for it must arrive there.
    let mut misroutes = Vec::new();
    let mut routed = 0usize;
    for o in observations {
        for (topic, partition) in &o.routes {
            if topic != ROUTING_TOPIC {
                continue;
            }
            routed += 1;
            if *partition != o.node_id {
                misroutes.push(format!(
                    "{} addresses {topic}[{partition}], whose advertised \
                     leader is broker {partition}",
                    describe(o)
                ));
            }
        }
    }
    outcomes.push(CheckOutcome {
        id: CheckId("client/routes-to-partition-leader".into()),
        requirement: "produce and fetch requests go to the broker the \
                      metadata advertises as the partition's leader",
        verdict: if routed == 0 {
            skip("no produce/fetch for the routing topic observed")
        } else if misroutes.is_empty() {
            Verdict::Pass
        } else {
            Verdict::Fail {
                details: misroutes.join("; "),
            }
        },
    });

    Report {
        subject: subject.into(),
        outcomes,
    }
}
