//! Cluster layer: metadata discovery, broker connection pool, and
//! partition-leader routing.
//!
//! A [`Cluster`] bootstraps through any reachable configured broker,
//! learns the cluster's shape from Metadata, and hands out negotiated
//! [`Connection`]s to the broker a request must go to. Version ranges are
//! kept per broker — brokers in a mixed-version cluster may advertise
//! different ranges.
//!
//! A `Cluster` is a cheap clonable handle: clones share one connection
//! pool, one metadata cache, and one set of discovered coordinators, so
//! a producer, a consumer, and a group membership in the same process
//! ride the same authenticated connections. Handles are `Send + Sync`;
//! internal locks are never held across I/O.
//!
//! # Connection sharing and blocking requests
//!
//! A Kafka broker processes a single connection's requests strictly in
//! order: it will not begin the next request until the current one's
//! response is sent. Fast requests (Produce, Metadata, Heartbeat)
//! pipeline freely on the one shared connection per broker, but a
//! *blocking* request — a long-poll Fetch waiting out `max_wait_ms`, a
//! JoinGroup parked for a whole rebalance — would hold that connection
//! and starve everything queued behind it.
//!
//! Blocking requests therefore run on *leased* connections instead:
//! [`Cluster::blocking_partition_leader`] and
//! [`Cluster::blocking_coordinator`] check a dedicated connection out
//! of a per-broker pool (dialing when the pool is dry), and
//! [`BrokerLease::release`] returns it for reuse on success. The
//! consumer's fetch path and the group join/sync dance use leases, so a
//! parked fetch never delays a produce to the same broker, concurrent
//! bridge pumps do not serialize their long-polls, and two members of
//! one group can share a `Cluster` — heartbeats ride the never-parked
//! fast lane.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_protocol::messages::find_coordinator_request::FindCoordinatorRequest;
use odradek_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use odradek_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use odradek_protocol::messages::metadata_response::MetadataResponse;

use crate::ClientConfig;
use crate::conn;
use crate::conn::Connection;
use crate::error::ClientError;
use crate::negotiate::ApiVersionRanges;

/// Metadata versions this client speaks: v1+ so an empty topics array
/// means "none" rather than v0's "all".
const METADATA_SUPPORTED: (i16, i16) = (1, MetadataRequest::MAX_VERSION);

/// One broker's advertised endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl BrokerInfo {
    fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// One partition's leadership as last reported by metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionInfo {
    pub index: i32,
    pub leader_id: i32,
    pub error: ErrorCode,
}

/// A negotiated connection to one broker, with the version ranges that
/// broker advertised.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Broker {
    pub conn: Connection,
    pub ranges: ApiVersionRanges,
}

/// A dedicated connection checked out of the blocking pool for one
/// blocking request (or one uninterrupted sequence, like join → sync).
///
/// Call [`BrokerLease::release`] after a *successful* exchange to hand
/// the connection back for reuse. Dropping the lease instead discards
/// the connection — the right outcome after an error, when it may be
/// poisoned or mid-frame.
#[derive(Debug)]
pub struct BrokerLease {
    broker: Broker,
    node_id: i32,
    cluster: Cluster,
}

impl BrokerLease {
    /// The leased connection and its negotiated version ranges.
    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// Return the connection to the pool for reuse.
    ///
    /// The connection is kept unless the idle pool already holds as
    /// many as this broker's own peak concurrency ever needed, or as
    /// [`ClientConfig::blocking_idle_max`] allows — whichever is
    /// smaller. Sizing by observed concurrency is the point: it is
    /// exactly when many requests are parked against one broker that
    /// discarding a released connection is most expensive, since each
    /// one then pays a full dial on its next round.
    pub fn release(self) {
        let max = self.cluster.inner.config.blocking_idle_max;
        let mut state = self.cluster.state();
        let pool = state.blocking.entry(self.node_id).or_default();
        if pool.idle.len() < pool.peak.min(max) {
            pool.idle.push(self.broker.clone());
        }
        // The lease's own Drop settles the outstanding count, and it
        // takes the same lock.
        drop(state);
    }
}

impl Drop for BrokerLease {
    fn drop(&mut self) {
        let mut state = self.cluster.state();
        if let Some(pool) = state.blocking.get_mut(&self.node_id) {
            pool.outstanding = pool.outstanding.saturating_sub(1);
        }
    }
}

/// One broker's blocking connections: those idle, and how many the
/// workload has ever held at once.
///
/// The peak is the pool's sizing signal. A hardcoded idle cap gets the
/// common case exactly backwards: it is precisely when concurrency is
/// *high* — 200 bridge pumps parked in long polls against one broker —
/// that dropping a released connection is most expensive, since every
/// one of them then pays a full TCP+TLS+ApiVersions+SASL dial on its
/// next round. Sizing the idle pool by observed peak concurrency
/// instead means the pool never holds more sockets than the workload
/// has already proven it wants — a steady workload dials only while
/// ramping up — and [`ClientConfig::blocking_idle_max`] is the backstop
/// against a one-off spike pinning that many sockets for good.
#[derive(Debug, Default)]
struct BlockingPool {
    idle: Vec<Broker>,
    /// Leases checked out right now.
    outstanding: usize,
    /// The high-water mark of `outstanding`.
    peak: usize,
}

/// The mutable half of a cluster: caches every handle shares.
#[derive(Debug, Default)]
struct State {
    /// The control-plane connection (metadata refresh, FindCoordinator,
    /// admin). `None` after a failure; the next use redials, failing over
    /// from the configured bootstrap servers to any known broker.
    control: Option<Broker>,
    brokers: HashMap<i32, BrokerInfo>,
    topics: HashMap<String, Vec<PartitionInfo>>,
    /// Topic id → name, as of the last metadata refresh (v10+ metadata
    /// carries ids; KIP-848 assignments address topics by id).
    topic_ids: HashMap<[u8; 16], String>,
    conns: HashMap<i32, Broker>,
    /// Blocking-request connections per broker: idle ones plus the
    /// concurrency that sizes the pool.
    blocking: HashMap<i32, BlockingPool>,
    /// The node Metadata last named as the controller, if it named one.
    ///
    /// Separate from [`State::control`], which is only "a broker we can
    /// reach". Most control-plane traffic does not care which broker
    /// answers it; topic creation and deletion do, and asking the wrong
    /// one gets `NOT_CONTROLLER`.
    controller: Option<i32>,
    /// Group id → coordinator node id, as last discovered.
    coordinators: HashMap<String, i32>,
    /// Transactional id → coordinator node id.
    ///
    /// Kept apart from `coordinators` because the two namespaces are
    /// independent: the same string can name a group and a
    /// transactional id, they are hashed onto their own internal topics,
    /// and nothing makes them land on the same broker.
    txn_coordinators: HashMap<String, i32>,
}

#[derive(Debug)]
struct Inner {
    config: ClientConfig,
    /// The connection the cluster bootstrapped through, frozen for
    /// [`Cluster::bootstrap_broker`]. Control-plane traffic uses the
    /// replaceable [`State::control`] slot instead.
    initial: Broker,
    state: Mutex<State>,
}

/// A connected cluster: metadata cache plus per-broker connections.
/// Cloning is cheap and clones share everything.
#[derive(Debug, Clone)]
pub struct Cluster {
    inner: Arc<Inner>,
}

impl Cluster {
    /// Dial the first reachable bootstrap server and negotiate versions.
    /// No metadata is fetched yet; see [`Cluster::refresh_metadata`].
    pub async fn connect(config: ClientConfig) -> Result<Cluster, ClientError> {
        let mut last = String::from("no bootstrap servers configured");
        for addr in &config.bootstrap_servers {
            match dial(addr, &config).await {
                Ok(bootstrap) => {
                    let state = State {
                        control: Some(bootstrap.clone()),
                        ..State::default()
                    };
                    return Ok(Cluster {
                        inner: Arc::new(Inner {
                            config,
                            initial: bootstrap,
                            state: Mutex::new(state),
                        }),
                    });
                }
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        Err(ClientError::Bootstrap(last))
    }

    /// The shared state; the guard must never live across an `.await`.
    /// The shared state, ignoring lock poisoning.
    ///
    /// Poisoning means some thread panicked while holding this lock, not
    /// that the map inside it is unusable — every critical section here
    /// is a few inserts and lookups. Propagating the poison would matter
    /// more than the panic did: [`BrokerLease`] settles its outstanding
    /// count in `Drop`, so a panicking guard would panic again while
    /// unwinding, and a panic during unwind aborts the process.
    fn state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Fetch metadata for `topics` through the control-plane connection
    /// and update the broker and leadership caches. A dead control
    /// connection fails over: the redial tries the configured bootstrap
    /// servers and every currently-known broker.
    pub async fn refresh_metadata(&self, topics: &[&str]) -> Result<(), ClientError> {
        let broker = self.control_broker().await?;
        match self.refresh_metadata_via(&broker, topics).await {
            Err(e) if is_control_failure(&e) => {
                self.forget_control();
                let broker = self.control_broker().await?;
                self.refresh_metadata_via(&broker, topics).await
            }
            other => other,
        }
    }

    async fn refresh_metadata_via(
        &self,
        broker: &Broker,
        topics: &[&str],
    ) -> Result<(), ClientError> {
        let version = broker
            .ranges
            .pick(MetadataRequest::API_KEY, METADATA_SUPPORTED)?;
        let mut request = MetadataRequest::default();
        request.topics = Some(
            topics
                .iter()
                .map(|name| {
                    let mut topic = MetadataRequestTopic::default();
                    topic.name = Some((*name).to_owned());
                    topic
                })
                .collect(),
        );
        request.allow_auto_topic_creation = false;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(MetadataRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<MetadataResponse>(&broker.conn, &mut resp, version)?;

        let brokers: HashMap<i32, BrokerInfo> = resp
            .brokers
            .iter()
            .map(|b| {
                (
                    b.node_id,
                    BrokerInfo {
                        node_id: b.node_id,
                        host: b.host.clone(),
                        port: b.port,
                    },
                )
            })
            .collect();
        let mut refreshed = Vec::new();
        for topic in &resp.topics {
            let Some(name) = &topic.name else { continue };
            let code = ErrorCode(topic.error_code);
            if !code.is_ok() {
                return Err(ClientError::Broker(code));
            }
            refreshed.push((
                name.clone(),
                topic.topic_id,
                topic
                    .partitions
                    .iter()
                    .map(|p| PartitionInfo {
                        index: p.partition_index,
                        leader_id: p.leader_id,
                        error: ErrorCode(p.error_code),
                    })
                    .collect(),
            ));
        }

        // -1 means the answering broker does not know who the
        // controller is, which is a different thing from there not
        // being one; keeping the last known id beats forgetting it.
        let controller = (resp.controller_id >= 0).then_some(resp.controller_id);

        let mut state = self.state();
        state.brokers = brokers;
        if let Some(controller) = controller {
            state.controller = Some(controller);
        }
        for (name, topic_id, partitions) in refreshed {
            if topic_id != [0u8; 16] {
                state.topic_ids.insert(topic_id, name.clone());
            }
            state.topics.insert(name, partitions);
        }
        Ok(())
    }

    /// The connection this cluster originally bootstrapped through.
    ///
    /// Frozen at connect time: if that broker dies this handle stays
    /// dead. Prefer [`Cluster::control_broker`], which redials and fails
    /// over, for admin calls and probes.
    pub fn bootstrap_broker(&self) -> &Broker {
        &self.inner.initial
    }

    /// A live control-plane connection, for requests that need no
    /// routing (admin calls, probes). Redials on first use after a
    /// failure, trying the configured bootstrap servers and every
    /// currently-known broker until one answers.
    pub async fn control_broker(&self) -> Result<Broker, ClientError> {
        if let Some(broker) = self.state().control.clone() {
            return Ok(broker);
        }
        // Candidates: the configured bootstrap list, then everything
        // metadata has taught us since — dedup'd, lock dropped before
        // any dialing.
        let mut candidates = self.inner.config.bootstrap_servers.clone();
        candidates.extend(self.state().brokers.values().map(BrokerInfo::addr));
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|addr| seen.insert(addr.clone()));
        let mut last = String::from("no control-plane candidates");
        for addr in &candidates {
            match dial(addr, &self.inner.config).await {
                Ok(broker) => {
                    // A concurrent redial may have won; keep the winner.
                    let mut state = self.state();
                    return Ok(state.control.get_or_insert(broker).clone());
                }
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
        Err(ClientError::Bootstrap(last))
    }

    /// Drop the control-plane connection; the next use redials.
    pub(crate) fn forget_control(&self) {
        self.state().control = None;
    }

    /// A connection to the broker Metadata last named as the controller.
    ///
    /// Creating and deleting topics is the controller's work, and only
    /// its work. Falls back to [`Cluster::control_broker`] when no
    /// metadata has been fetched yet or the answer named no controller:
    /// that is exactly right on a single-broker cluster and a guess on
    /// a larger one, which is why the callers retry rather than trust
    /// it.
    pub async fn controller(&self) -> Result<Broker, ClientError> {
        // Read out from under the lock before branching: the fallback
        // takes the same lock, and a guard living into the `else` would
        // be a deadlock resting on when a temporary happens to drop.
        let known = self.state().controller;
        let Some(id) = known else {
            return self.control_broker().await;
        };
        match self.broker(id).await {
            Ok(broker) => Ok(broker),
            // Named a node we cannot reach or have never heard of. Any
            // broker beats none; `NOT_CONTROLLER` will say so.
            Err(_) => self.control_broker().await,
        }
    }

    /// Forget who the controller was, ask again, and connect to whoever
    /// the answer names.
    ///
    /// Every Metadata response carries `controller_id`, so relearning it
    /// costs the refresh that a `NOT_CONTROLLER` answer already implies.
    pub(crate) async fn rediscover_controller(&self) -> Result<Broker, ClientError> {
        self.state().controller = None;
        self.forget_control();
        // No topics: this is asked for the broker list and the
        // controller id, both of which every answer carries.
        self.refresh_metadata(&[]).await?;
        self.controller().await
    }

    /// Known brokers, as of the last metadata refresh.
    pub fn brokers(&self) -> Vec<BrokerInfo> {
        self.state().brokers.values().cloned().collect()
    }

    /// A topic's partitions, as of the last metadata refresh.
    ///
    /// Copies the whole partition vector out from under the state lock.
    /// Callers that only need the count want
    /// [`Cluster::partition_count`]; callers that only need one
    /// partition's leader want [`Cluster::leader_id`].
    pub fn partitions(&self, topic: &str) -> Option<Vec<PartitionInfo>> {
        self.state().topics.get(topic).cloned()
    }

    /// How many partitions `topic` has, as of the last metadata refresh.
    ///
    /// The cheap answer for the question the hot path actually asks: a
    /// keyed producer needs the count for every single record, and
    /// cloning a 200-entry partition vector to read `.len()` off it is
    /// a copy — under the shared state lock — per record.
    pub fn partition_count(&self, topic: &str) -> Option<usize> {
        self.state().topics.get(topic).map(Vec::len)
    }

    /// A topic's partitions, refreshing metadata if the topic is not
    /// yet cached. The one call for "what partitions does this topic
    /// have" — producers, group leaders, and bridges all need it.
    pub async fn topic_partitions(&self, topic: &str) -> Result<Vec<PartitionInfo>, ClientError> {
        if self.partitions(topic).is_none() {
            self.refresh_metadata(&[topic]).await?;
        }
        self.partitions(topic)
            .ok_or(ClientError::Broker(ErrorCode::UNKNOWN_TOPIC_OR_PARTITION))
    }

    /// The topic name behind a metadata-reported topic id, if the last
    /// refresh saw it.
    pub fn topic_name_by_id(&self, topic_id: [u8; 16]) -> Option<String> {
        self.state().topic_ids.get(&topic_id).cloned()
    }

    /// The node id currently leading `topic[partition]`.
    pub fn leader_id(&self, topic: &str, partition: i32) -> Option<i32> {
        let state = self.state();
        let info = state
            .topics
            .get(topic)?
            .iter()
            .find(|p| p.index == partition)?;
        (info.leader_id >= 0).then_some(info.leader_id)
    }

    /// A negotiated connection to the broker with `node_id`, dialing on
    /// first use.
    pub async fn broker(&self, node_id: i32) -> Result<Broker, ClientError> {
        let info = {
            let state = self.state();
            if let Some(broker) = state.conns.get(&node_id) {
                return Ok(broker.clone());
            }
            state.brokers.get(&node_id).cloned()
        };
        let info = info.ok_or(ClientError::UnknownLeader {
            topic: format!("<broker {node_id}>"),
            partition: -1,
        })?;
        let broker = dial(&info.addr(), &self.inner.config).await?;
        // A concurrent dial to the same broker may have won the race;
        // keep whichever connection landed first.
        Ok(self.state().conns.entry(node_id).or_insert(broker).clone())
    }

    /// Forget cached leadership for `topic` — e.g. after a metadata
    /// answer stopped making sense — so the next lookup refreshes.
    ///
    /// Blunt: this costs *every* partition of the topic a refresh. For
    /// the common "this partition's leader moved" case reach for
    /// [`Cluster::mark_partition_stale`] instead.
    pub fn mark_stale(&self, topic: &str) {
        self.state().topics.remove(topic);
    }

    /// Forget cached leadership for one partition — e.g. after a
    /// NOT_LEADER_OR_FOLLOWER — so the next lookup for *that* partition
    /// refreshes while the rest of the topic keeps routing.
    pub fn mark_partition_stale(&self, topic: &str, partition: i32) {
        let mut state = self.state();
        let Some(partitions) = state.topics.get_mut(topic) else {
            return;
        };
        match partitions.iter_mut().find(|p| p.index == partition) {
            // -1 is metadata's own "no leader": the next lookup refreshes.
            Some(info) => info.leader_id = -1,
            // A partition the cached view does not even know about means
            // the whole view is behind; refetch it.
            None => {
                state.topics.remove(topic);
            }
        }
    }

    /// Drop the pooled connections to `node_id` (e.g. after it failed);
    /// the next use redials. Leases already checked out are unaffected,
    /// and the pool's record of how much concurrency this broker sees
    /// survives, so a redial does not re-learn it from scratch.
    pub fn forget_broker(&self, node_id: i32) {
        let mut state = self.state();
        state.conns.remove(&node_id);
        if let Some(pool) = state.blocking.get_mut(&node_id) {
            pool.idle.clear();
        }
    }

    /// Check a dedicated connection to `node_id` out of the blocking
    /// pool, dialing if none is idle. Use for requests that hold their
    /// connection (long-poll fetches, parked joins); fast requests
    /// belong on the shared [`Cluster::broker`] connection.
    pub async fn blocking_broker(&self, node_id: i32) -> Result<BrokerLease, ClientError> {
        let idle = {
            let mut state = self.state();
            let idle = state
                .blocking
                .get_mut(&node_id)
                .and_then(|pool| pool.idle.pop());
            if idle.is_none() && !state.brokers.contains_key(&node_id) {
                return Err(ClientError::UnknownLeader {
                    topic: format!("<broker {node_id}>"),
                    partition: -1,
                });
            }
            idle
        };
        let broker = match idle {
            Some(broker) => broker,
            None => {
                let info = self.state().brokers.get(&node_id).cloned().ok_or(
                    ClientError::UnknownLeader {
                        topic: format!("<broker {node_id}>"),
                        partition: -1,
                    },
                )?;
                dial(&info.addr(), &self.inner.config).await?
            }
        };
        {
            // Count the checkout only once the connection is in hand, so
            // a failed dial does not inflate the pool's idea of the
            // workload's concurrency.
            let mut state = self.state();
            let pool = state.blocking.entry(node_id).or_default();
            pool.outstanding += 1;
            pool.peak = pool.peak.max(pool.outstanding);
        }
        Ok(BrokerLease {
            broker,
            node_id,
            cluster: Cluster {
                inner: Arc::clone(&self.inner),
            },
        })
    }

    /// Like [`Cluster::partition_leader`], but checking a dedicated
    /// connection out of the blocking pool.
    pub async fn blocking_partition_leader(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<BrokerLease, ClientError> {
        if self.leader_id(topic, partition).is_none() {
            self.refresh_metadata(&[topic]).await?;
        }
        let leader =
            self.leader_id(topic, partition)
                .ok_or_else(|| ClientError::UnknownLeader {
                    topic: topic.to_owned(),
                    partition,
                })?;
        self.blocking_broker(leader).await
    }

    /// A negotiated connection to the current leader of
    /// `topic[partition]`, refreshing metadata once if leadership is
    /// unknown.
    pub async fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Broker, ClientError> {
        if self.leader_id(topic, partition).is_none() {
            self.refresh_metadata(&[topic]).await?;
        }
        let leader =
            self.leader_id(topic, partition)
                .ok_or_else(|| ClientError::UnknownLeader {
                    topic: topic.to_owned(),
                    partition,
                })?;
        self.broker(leader).await
    }
}

/// FindCoordinator versions this client speaks: the single-key shape
/// (v4+ switches to batched keys).
const FIND_COORDINATOR_SUPPORTED: (i16, i16) = (0, 3);

/// Which coordinator a FindCoordinator is asking about.
///
/// The wire calls this `key_type`, and the two values name two
/// unrelated services that happen to share a request: the group
/// coordinator owns offsets and rebalances, the transaction
/// coordinator owns producer ids and transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorKind {
    Group,
    Transaction,
}

impl CoordinatorKind {
    fn key_type(self) -> i8 {
        match self {
            CoordinatorKind::Group => 0,
            CoordinatorKind::Transaction => 1,
        }
    }

    fn cached(self, state: &State, key: &str) -> Option<i32> {
        match self {
            CoordinatorKind::Group => state.coordinators.get(key).copied(),
            CoordinatorKind::Transaction => state.txn_coordinators.get(key).copied(),
        }
    }

    fn record(self, state: &mut State, key: &str, node_id: i32) {
        match self {
            CoordinatorKind::Group => state.coordinators.insert(key.to_owned(), node_id),
            CoordinatorKind::Transaction => state.txn_coordinators.insert(key.to_owned(), node_id),
        };
    }

    fn forget(self, state: &mut State, key: &str) -> Option<i32> {
        match self {
            CoordinatorKind::Group => state.coordinators.remove(key),
            CoordinatorKind::Transaction => state.txn_coordinators.remove(key),
        }
    }
}

impl Cluster {
    /// A negotiated connection to `group`'s coordinator, discovering it
    /// via FindCoordinator on first use.
    /// The node coordinating `group`, if this handle has looked it up.
    ///
    /// `None` before the first [`Cluster::coordinator`] call — this
    /// reports what is cached rather than asking, so a caller batching
    /// work by coordinator can do so without a round trip per group.
    pub fn coordinator_id(&self, group: &str) -> Option<i32> {
        self.state().coordinators.get(group).copied()
    }

    pub async fn coordinator(&self, group: &str) -> Result<Broker, ClientError> {
        self.coordinator_of(CoordinatorKind::Group, group).await
    }

    /// A negotiated connection to the coordinator for `transactional_id`
    /// — the broker that owns this producer's transaction state, and the
    /// only one InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn,
    /// TxnOffsetCommit and EndTxn may be sent to.
    ///
    /// A different lookup from [`Cluster::coordinator`], not just a
    /// different argument: the transaction log and the offsets log are
    /// separate internal topics, so the same name can coordinate on
    /// different brokers depending on which question is asked.
    pub async fn transaction_coordinator(
        &self,
        transactional_id: &str,
    ) -> Result<Broker, ClientError> {
        self.coordinator_of(CoordinatorKind::Transaction, transactional_id)
            .await
    }

    async fn coordinator_of(
        &self,
        kind: CoordinatorKind,
        key: &str,
    ) -> Result<Broker, ClientError> {
        let cached = kind.cached(&self.state(), key);
        let node_id = match cached {
            Some(node_id) => node_id,
            None => {
                // A dead control connection fails over like metadata does.
                let broker = self.control_broker().await?;
                match self.find_coordinator(&broker, kind, key).await {
                    Err(e) if is_control_failure(&e) => {
                        self.forget_control();
                        let broker = self.control_broker().await?;
                        self.find_coordinator(&broker, kind, key).await?
                    }
                    other => other?,
                }
            }
        };
        self.broker(node_id).await
    }

    async fn find_coordinator(
        &self,
        broker: &Broker,
        kind: CoordinatorKind,
        key: &str,
    ) -> Result<i32, ClientError> {
        let version = broker
            .ranges
            .pick(FindCoordinatorRequest::API_KEY, FIND_COORDINATOR_SUPPORTED)?;
        let mut request = FindCoordinatorRequest::default();
        request.key = key.to_owned();
        request.key_type = kind.key_type();
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(FindCoordinatorRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<FindCoordinatorResponse>(&broker.conn, &mut resp, version)?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        let mut state = self.state();
        // The response names the coordinator's endpoint directly;
        // make it dialable even before any metadata refresh.
        state.brokers.insert(
            resp.node_id,
            BrokerInfo {
                node_id: resp.node_id,
                host: resp.host.clone(),
                port: resp.port,
            },
        );
        kind.record(&mut state, key, resp.node_id);
        Ok(resp.node_id)
    }

    /// Forget `group`'s discovered coordinator — e.g. after
    /// NOT_COORDINATOR — so the next use rediscovers it.
    pub fn forget_coordinator(&self, group: &str) {
        self.forget_coordinator_of(CoordinatorKind::Group, group);
    }

    /// Forget the discovered coordinator for `transactional_id`.
    pub fn forget_transaction_coordinator(&self, transactional_id: &str) {
        self.forget_coordinator_of(CoordinatorKind::Transaction, transactional_id);
    }

    fn forget_coordinator_of(&self, kind: CoordinatorKind, key: &str) {
        let mut state = self.state();
        if let Some(node_id) = kind.forget(&mut state, key) {
            state.conns.remove(&node_id);
            if let Some(pool) = state.blocking.get_mut(&node_id) {
                pool.idle.clear();
            }
        }
    }

    /// Like [`Cluster::coordinator`], but checking a dedicated
    /// connection out of the blocking pool — for the join/sync dance,
    /// which can park for a whole rebalance.
    pub async fn blocking_coordinator(&self, group: &str) -> Result<BrokerLease, ClientError> {
        // Discovery (a fast exchange) rides the shared path; only the
        // blocking work itself needs a lease.
        self.coordinator(group).await?;
        let node_id =
            self.state()
                .coordinators
                .get(group)
                .copied()
                .ok_or(ClientError::ProtocolViolation(
                    "coordinator vanished after discovery".into(),
                ))?;
        self.blocking_broker(node_id).await
    }
}

/// CreateTopics versions this client speaks: v2+ for the per-topic error
/// message, v7 the last before topic-id responses.
const CREATE_TOPICS_SUPPORTED: (i16, i16) = (2, 7);

/// Broker-side deadline for a CreateTopics request.
const CREATE_TOPICS_TIMEOUT_MS: i32 = 30_000;

impl Cluster {
    /// Create `name` with `partitions` partitions at `replication_factor`,
    /// through the control-plane connection.
    ///
    /// Any per-topic error — including `TOPIC_ALREADY_EXISTS` — surfaces
    /// as [`ClientError::Broker`]; callers that tolerate an existing
    /// topic can match on the code.
    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), ClientError> {
        let broker = self.controller().await?;
        match self
            .create_topic_via(&broker, name, partitions, replication_factor)
            .await
        {
            Err(e) if is_control_redirect(&e) => {
                let broker = self.rediscover_controller().await?;
                self.create_topic_via(&broker, name, partitions, replication_factor)
                    .await
            }
            other => other,
        }
    }

    async fn create_topic_via(
        &self,
        broker: &Broker,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), ClientError> {
        let version = broker
            .ranges
            .pick(CreateTopicsRequest::API_KEY, CREATE_TOPICS_SUPPORTED)?;
        let mut creatable = CreatableTopic::default();
        creatable.name = name.to_owned();
        creatable.num_partitions = partitions;
        creatable.replication_factor = replication_factor;
        let mut request = CreateTopicsRequest::default();
        request.topics = vec![creatable];
        request.timeout_ms = CREATE_TOPICS_TIMEOUT_MS;
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp = broker
            .conn
            .request(CreateTopicsRequest::API_KEY, version, &body)
            .await?;
        let resp = conn::decode_body::<CreateTopicsResponse>(&broker.conn, &mut resp, version)?;
        let entry = resp.topics.iter().find(|t| t.name == name).ok_or_else(|| {
            ClientError::ProtocolViolation(format!("create topics response omits {name}"))
        })?;
        let code = ErrorCode(entry.error_code);
        if code.is_ok() {
            Ok(())
        } else {
            Err(ClientError::Broker(code))
        }
    }
}

/// True when the control-plane connection itself failed — closed under
/// us, an I/O error, or a client-side timeout — as opposed to the
/// broker answering with an error. The remedy is dropping the
/// connection and failing over, not resending.
fn is_control_failure(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::ConnectionClosed | ClientError::Io(_) | ClientError::Timeout(_)
    )
}

/// True when a controller-bound request should be asked of a different
/// broker: the connection failed, or whoever answered is not the
/// controller any more.
///
/// `NOT_CONTROLLER` is a redirect, not a refusal, and the difference is
/// not academic — Kafka forwards controller work internally, so a
/// client that reads it as a refusal works there and fails against an
/// implementation that answers honestly. This one did.
pub(crate) fn is_control_redirect(e: &ClientError) -> bool {
    is_control_failure(e)
        || matches!(e, ClientError::Broker(code) if *code == ErrorCode::NOT_CONTROLLER)
}

/// One full connection establishment — TCP, TLS, ApiVersions, SASL —
/// bounded by [`ClientConfig::connect_timeout`].
async fn dial(addr: &str, config: &ClientConfig) -> Result<Broker, ClientError> {
    tokio::time::timeout(config.connect_timeout, async {
        let conn = Connection::connect(addr, config).await?;
        let ranges = conn.negotiate().await?;
        #[cfg(feature = "sasl")]
        crate::sasl::authenticate(&conn, &ranges, config).await?;
        Ok(Broker { conn, ranges })
    })
    .await
    .unwrap_or(Err(ClientError::Timeout("connect")))
}
