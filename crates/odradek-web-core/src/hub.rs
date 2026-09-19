//! The hub: pumps on demand, one per (topic, partition), and
//! topic-level subscriptions merging every partition into one stream.
//!
//! Lifecycle and access control live here: a topic gate
//! ([`Hub::with_topic_gate`]) rejects unwanted topic names before any
//! pump or connection exists, dead or idle-exited pumps are respawned
//! (and their map entries replaced) on the next subscribe, and
//! [`Hub::shutdown`] stops every pump, closes remaining subscriber
//! streams cleanly, and fails later subscribes with
//! [`HubError::ShutDown`].
//!
//! # What an anonymous request can cost
//!
//! The pump map is keyed by (topic, partition), both of which come
//! from the request path, so its growth is the hub's exposure. Three
//! bounds, each covering a dimension the others do not:
//!
//! 1. **The gate** ([`Hub::with_topic_gate`], deny-all until you
//!    choose) bounds the topic dimension, and refuses before any
//!    source call at all.
//! 2. **Existence** — every subscribe checks the pair against the
//!    source's own partition list (cached per topic) *before* the map
//!    grows, so a request naming a topic or partition that does not
//!    exist creates no entry, spawns no task, and makes no broker
//!    round trip beyond the one cached metadata lookup per topic. This
//!    bounds the partition dimension, which the gate cannot see.
//! 3. **Capacity** ([`Hub::with_max_pumps`], 1024 by default) bounds
//!    what is left — real partitions of allowed topics — and dead
//!    entries are evicted ([`PumpHandle::is_dead`]) before the limit is
//!    applied, so a pump that has exited never holds a slot.
//!
//! So `pumps.len()` is bounded by `max_pumps`, and an entry exists
//! only for a partition that exists, is allowed, and has a live task.
//! Memory follows from [`PumpConfig`]: worst case
//! `max_pumps x ring_capacity x <source's max fetch bytes>`, because a
//! ring is bounded by event *count* and an event can pin the buffer it
//! was read from.
//!
//! What is *not* bounded, and the reason to keep the gate narrow: the
//! partition cache only remembers topics that exist, so each request
//! naming a fresh unknown topic the gate allows costs one metadata
//! lookup. A gate written as an exact set bounds that to its size; a
//! prefix gate (`public.*`) leaves a request able to ask about a name
//! nobody has ever used. Those lookups are serialized behind the hub's
//! lock — which caps the concurrent load on the source, at the cost of
//! making a flood of unknown names slow down legitimate first
//! subscribes to other topics. Nothing is retained either way.
//!
//! [`SharedHub`] is the concurrent front door, and it is careful about
//! what it holds its lock across: the map lookup (and, the first time a
//! partition is asked for, the pump spawn) — never the subscribe round
//! trip to a running pump. That matters because a pump on a quiet topic
//! sits in a long poll: holding the lock across its reply would make one
//! subscribe block every other subscribe in the process, which is
//! exactly the reconnect-storm case. Topic-level subscribes go one step
//! further and run their per-partition round trips concurrently.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::event::{Filter, Position, TopicPosition};
use crate::params::StreamParams;
use crate::pump::{HubError, PumpConfig, PumpHandle, RejectionKind, StreamItem, Subscription};
use crate::source::{SourceError, SourceFactory};

type TopicPredicate = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// How many pumps a hub runs before refusing new partitions. Each one
/// is a task, a source connection, and a ring of up to
/// [`PumpConfig::ring_capacity`] events, so this is the hub's memory
/// ceiling; raise it past the partition count of the largest topic you
/// serve, and see [`Hub::with_max_pumps`].
pub const DEFAULT_MAX_PUMPS: usize = 1024;

/// Which topics a hub will serve.
enum Gate {
    /// None at all — the state a hub is born in. Every subscribe is
    /// refused until the embedder chooses
    /// [`Hub::with_topic_gate`] or [`Hub::allow_all_topics`].
    DenyAll,
    /// Every topic the source knows, including the cluster's internal
    /// ones. Chosen explicitly by [`Hub::allow_all_topics`].
    Any,
    Predicate(TopicPredicate),
}

impl Gate {
    fn as_str(&self) -> &'static str {
        match self {
            Gate::DenyAll => "deny-all",
            Gate::Any => "any",
            Gate::Predicate(_) => "predicate",
        }
    }
}

/// Fans partitions out to any number of subscribers, creating a
/// [`PumpHandle`] per (topic, partition) lazily via the factory.
///
/// A new hub serves *nothing*: pick [`Hub::with_topic_gate`] (the
/// answer for anything internet-facing) or [`Hub::allow_all_topics`]
/// before it is useful. See the [module docs](self) for the bounds on
/// what an anonymous request can make one of these allocate.
pub struct Hub<F: SourceFactory> {
    factory: F,
    config: PumpConfig,
    pumps: HashMap<(String, i32), PumpHandle>,
    /// Discovered partitions per topic; also the existence check for
    /// partition-level subscribes.
    topics: HashMap<String, Vec<i32>>,
    /// Topics this hub will serve at all.
    gate: Gate,
    /// The ceiling on `pumps.len()`.
    max_pumps: usize,
    /// So an unconfigured hub says so once, rather than once per
    /// refused request (which an anonymous client could drive).
    ungated_warning: std::sync::Once,
    shut_down: bool,
}

impl<F: SourceFactory + std::fmt::Debug> std::fmt::Debug for Hub<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("factory", &self.factory)
            .field("config", &self.config)
            .field("pumps", &self.pumps)
            .field("topics", &self.topics)
            .field("gate", &self.gate.as_str())
            .field("max_pumps", &self.max_pumps)
            .field("shut_down", &self.shut_down)
            .finish()
    }
}

/// A merged stream over every partition of one topic. Order holds
/// within each partition; partitions interleave.
#[derive(Debug)]
pub struct TopicSubscription {
    pub topic: String,
    partitions: Vec<i32>,
    receiver: mpsc::Receiver<StreamItem>,
}

impl TopicSubscription {
    /// The next event; `Some(Err(_))` reports a partition pump that
    /// failed, `None` means every partition's stream has ended.
    pub async fn recv(&mut self) -> Option<StreamItem> {
        self.receiver.recv().await
    }

    /// The underlying receiver, for `select!`-style composition.
    pub fn into_receiver(self) -> mpsc::Receiver<StreamItem> {
        self.receiver
    }

    /// The partitions this subscription covers.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

impl<F: SourceFactory> Hub<F> {
    /// A hub that serves **no topics yet**: every subscribe is denied
    /// until you call [`Hub::with_topic_gate`] (name what this process
    /// may read) or [`Hub::allow_all_topics`] (say you mean all of it).
    ///
    /// The default is deny rather than allow because the failure modes
    /// are not comparable: a missing gate you notice is a `403` in
    /// development, and a missing gate you do not notice is every
    /// topic on the cluster — `__consumer_offsets` included — readable
    /// by anyone who can reach the port, plus topic enumeration by
    /// probing for which names answer `404`.
    pub fn new(factory: F, config: PumpConfig) -> Hub<F> {
        Hub {
            factory,
            config,
            pumps: HashMap::new(),
            topics: HashMap::new(),
            gate: Gate::DenyAll,
            max_pumps: DEFAULT_MAX_PUMPS,
            ungated_warning: std::sync::Once::new(),
            shut_down: false,
        }
    }

    /// Serve only topics `gate` approves; everything else fails
    /// subscribe with [`HubError::Denied`] *before* any pump, source
    /// connection, or metadata lookup happens.
    ///
    /// The gate is a property of the process, not of the request: it
    /// answers "which topics does this bridge exist to serve", and it
    /// is the only authorization the hub itself performs. Per-user
    /// rules ("this reader may see topic X") belong in a middleware
    /// layer over the transport's router, which can see the whole
    /// request; the gate is the floor under it.
    ///
    /// ```
    /// # use odradek_web_core::{Hub, PumpConfig, memory::{MemoryFactory, MemoryLog}};
    /// let hub = Hub::new(MemoryFactory::new(MemoryLog::new()), PumpConfig::default())
    ///     .with_topic_gate(|topic| topic.starts_with("public."));
    /// ```
    #[must_use]
    pub fn with_topic_gate(mut self, gate: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.gate = Gate::Predicate(Arc::new(gate));
        self
    }

    /// Serve every topic the source knows — the explicit opt-out of
    /// [`Hub::with_topic_gate`].
    ///
    /// Reasonable for a hub behind an authenticating proxy that does
    /// its own per-topic authorization, or for tests. Anywhere else,
    /// remember what "every topic" includes: the cluster's internal
    /// topics (`__consumer_offsets` and friends), every topic created
    /// after this code was written, and — because a name that exists
    /// answers differently from one that does not — the topic list
    /// itself.
    #[must_use]
    pub fn allow_all_topics(mut self) -> Self {
        self.gate = Gate::Any;
        self
    }

    /// The ceiling on concurrently running pumps; past it, subscribes
    /// to *new* partitions fail with [`HubError::AtCapacity`] while
    /// established ones keep streaming. Defaults to
    /// [`DEFAULT_MAX_PUMPS`].
    ///
    /// This is the last bound on an anonymous request's memory cost,
    /// and the one that holds even when the gate is wide and the
    /// partitions are real. Set it above the partition count of the
    /// largest topic you serve (a topic-level subscribe needs one pump
    /// per partition at once), and budget
    /// `max_pumps x ring_capacity x <max fetch bytes>` for the worst
    /// case.
    #[must_use]
    pub fn with_max_pumps(mut self, max_pumps: usize) -> Self {
        self.max_pumps = max_pumps.max(1);
        self
    }

    fn check_topic(&self, topic: &str) -> Result<(), HubError> {
        if self.shut_down {
            return Err(HubError::ShutDown);
        }
        match &self.gate {
            Gate::Any => Ok(()),
            // No let-chain: the crate's MSRV (1.85) predates their
            // stabilization in 1.88.
            Gate::Predicate(gate) if gate(topic) => Ok(()),
            Gate::Predicate(_) => Err(HubError::Denied(topic.to_owned())),
            Gate::DenyAll => {
                // Once, not per request: the refusal itself is the
                // request's answer, and an anonymous client should not
                // be able to drive the log.
                self.ungated_warning.call_once(|| {
                    tracing::warn!(
                        "this hub has no topic gate, so every subscribe is denied; \
                         call Hub::with_topic_gate to name the topics it serves, \
                         or Hub::allow_all_topics to serve all of them"
                    );
                });
                Err(HubError::Denied(topic.to_owned()))
            }
        }
    }

    /// Subscribe to every partition of `topic` as one merged stream.
    ///
    /// Backpressure composes: a slow reader fills the merged queue,
    /// which stalls the per-partition forwarders, whose queues then
    /// demote each partition into the pump's loss-free catch-up.
    pub async fn subscribe_topic(
        &mut self,
        topic: &str,
        position: TopicPosition,
        filter: Filter,
    ) -> Result<TopicSubscription, HubError> {
        let partitions = self.partitions_of(topic).await?;
        // A topic subscribe is all or nothing, so the pumps it starts
        // are its own to clean up: it walks the partitions in order, and
        // one that fails part way — at the pump ceiling, most likely —
        // would otherwise leave the earlier ones holding slots against
        // every other topic until they idle out.
        let before = self.running_partitions(topic, &partitions);
        let mut subscriptions = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let start = partition_position(&position, *partition);
            match self
                .subscribe(topic, *partition, start, filter.clone())
                .await
            {
                Ok(subscription) => subscriptions.push(subscription),
                Err(e) => {
                    drop(subscriptions);
                    self.release_new(topic, &partitions, &before).await;
                    return Err(e);
                }
            }
        }
        Ok(merge(
            topic,
            partitions,
            subscriptions,
            self.config.queue_capacity,
        ))
    }

    /// Which of `partitions` already have a pump running.
    fn running_partitions(&self, topic: &str, partitions: &[i32]) -> HashSet<i32> {
        partitions
            .iter()
            .copied()
            .filter(|partition| self.pumps.contains_key(&(topic.to_owned(), *partition)))
            .collect()
    }

    /// Stop and forget the pumps for `partitions` that were not already
    /// running when `before` was taken.
    ///
    /// Only reached with exclusive access to the map (`&mut self`, or
    /// the shared hub's lock held across the whole acquire loop), so a
    /// pump another subscriber attached to in the meantime cannot be
    /// one of these.
    async fn release_new(&mut self, topic: &str, partitions: &[i32], before: &HashSet<i32>) {
        for partition in partitions {
            if before.contains(partition) {
                continue;
            }
            if let Some(handle) = self.pumps.remove(&(topic.to_owned(), *partition)) {
                handle.shutdown().await;
            }
        }
    }

    /// Subscribe to one partition, starting the pump on first use.
    pub async fn subscribe(
        &mut self,
        topic: &str,
        partition: i32,
        position: Position,
        filter: Filter,
    ) -> Result<Subscription, HubError> {
        let (handle, created) = self.acquire(topic, partition).await?;
        match handle.subscribe(position, filter.clone()).await {
            Ok(sub) => Ok(sub),
            Err(HubError::PumpClosed) if !created => {
                // The pump died (error budget exhausted) or exited
                // idle; replace it once and retry.
                let handle = self.respawn(topic, partition, &handle).await?;
                match handle.subscribe(position, filter).await {
                    Ok(sub) => Ok(sub),
                    Err(e) => {
                        self.forget(topic, partition, &handle);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                if created {
                    // First creation failed outright (e.g. the topic
                    // does not exist): no permanent dead map entry.
                    self.forget(topic, partition, &handle);
                    handle.shutdown().await;
                }
                Err(e)
            }
        }
    }

    /// The partitions of `topic`, discovered once and cached.
    async fn partitions_of(&mut self, topic: &str) -> Result<Vec<i32>, HubError> {
        self.known_partitions(topic).await.cloned()
    }

    /// The cached partition list for `topic`, fetching it the first
    /// time. The cache is what keeps the existence check from being a
    /// broker round trip per request; it is refreshed only by
    /// [`Hub::forget_topics`] (or a restart), so a topic that *gains*
    /// partitions while the hub runs needs one of those before the new
    /// ones are reachable.
    async fn known_partitions(&mut self, topic: &str) -> Result<&Vec<i32>, HubError> {
        self.check_topic(topic)?;
        if !self.topics.contains_key(topic) {
            let partitions = self.factory.partitions(topic).await?;
            self.topics.insert(topic.to_owned(), partitions);
        }
        Ok(self
            .topics
            .get(topic)
            .expect("the partition list was just inserted"))
    }

    /// Fail unless the source actually has this (topic, partition).
    ///
    /// This is what stands between the pump map and the request path.
    /// `partition` is a free `i32` out of the URL and `Position::Latest`
    /// (the default) needs no source call, so without this check a
    /// subscribe to a partition that does not exist would spawn a pump,
    /// insert a map entry, and only then die on its first fetch —
    /// leaving the entry behind, 2^31 times over, for one allowed topic.
    async fn check_partition(&mut self, topic: &str, partition: i32) -> Result<(), HubError> {
        if self.known_partitions(topic).await?.contains(&partition) {
            return Ok(());
        }
        Err(HubError::Source(SourceError::not_found(format!(
            "topic {topic:?} has no partition {partition}"
        ))))
    }

    /// Drop the map entries of pumps whose task has exited — died on a
    /// permanent error, spent its error budget, or exited idle — and
    /// return how many went. Subscribes do this before creating a pump,
    /// so entries are reclaimed without a background sweeper; call it
    /// directly if you want the count for a metric.
    pub fn reap_exited_pumps(&mut self) -> usize {
        let before = self.pumps.len();
        self.pumps.retain(|_, handle| !handle.is_dead());
        before - self.pumps.len()
    }

    /// Forget the cached partition lists, so the next subscribe asks
    /// the source again. For topics that were repartitioned (or
    /// created) since this hub started.
    pub fn forget_topics(&mut self) {
        self.topics.clear();
    }

    /// The handle for one partition, spawning its pump on first use;
    /// the flag says whether *this* call created it (and so owns
    /// cleaning up after a pump that refuses its first subscriber).
    ///
    /// Nothing reaches the map that has not passed the gate, been found
    /// in the source's partition list, and fit under the pump ceiling.
    ///
    /// This is all a [`SharedHub`] subscribe holds the lock for. It
    /// spans the first metadata lookup for a topic and the pump spawn —
    /// both once per topic/partition — but never a subscribe round trip
    /// to a running pump.
    async fn acquire(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<(PumpHandle, bool), HubError> {
        self.check_topic(topic)?;
        let key = (topic.to_owned(), partition);
        match self.pumps.get(&key) {
            // A dead entry is replaced below rather than handed out:
            // the subscribe to it could only fail.
            Some(handle) if !handle.is_dead() => return Ok((handle.clone(), false)),
            _ => {}
        }
        self.check_partition(topic, partition).await?;
        // Exited pumps must not hold slots against the ceiling.
        self.reap_exited_pumps();
        if self.pumps.len() >= self.max_pumps && !self.pumps.contains_key(&key) {
            tracing::warn!(
                topic,
                partition,
                max_pumps = self.max_pumps,
                "refusing a new pump: the hub is at capacity"
            );
            return Err(HubError::AtCapacity);
        }
        let handle = self.spawn_pump(topic, partition).await?;
        self.pumps.insert(key, handle.clone());
        Ok((handle, true))
    }

    /// Replace a pump that closed under a subscriber. If someone else
    /// already replaced it (the map entry is no longer `stale`), take
    /// theirs rather than spawn a second pump for the same partition.
    async fn respawn(
        &mut self,
        topic: &str,
        partition: i32,
        stale: &PumpHandle,
    ) -> Result<PumpHandle, HubError> {
        let key = (topic.to_owned(), partition);
        match self.pumps.get(&key) {
            Some(current) if !current.same_pump(stale) => return Ok(current.clone()),
            _ => {}
        }
        match self.spawn_pump(topic, partition).await {
            Ok(handle) => {
                self.pumps.insert(key, handle.clone());
                Ok(handle)
            }
            Err(e) => {
                // Do not cache a dead entry for a topic the source no
                // longer creates pumps for.
                self.pumps.remove(&key);
                Err(e)
            }
        }
    }

    /// Drop the map entry for a pump that turned out to be unusable —
    /// but only while it is still the entry, so a replacement installed
    /// by a concurrent subscribe survives.
    fn forget(&mut self, topic: &str, partition: i32, stale: &PumpHandle) {
        let key = (topic.to_owned(), partition);
        if self
            .pumps
            .get(&key)
            .is_some_and(|current| current.same_pump(stale))
        {
            self.pumps.remove(&key);
        }
    }

    async fn spawn_pump(&self, topic: &str, partition: i32) -> Result<PumpHandle, HubError> {
        let source = self.factory.create(topic, partition).await?;
        Ok(PumpHandle::spawn(
            source,
            topic,
            partition,
            self.config.clone(),
        ))
    }

    /// Stop every pump and refuse further subscribes with
    /// [`HubError::ShutDown`]. Remaining subscriber streams end
    /// *cleanly* (no [`StreamError`](crate::pump::StreamError) item) —
    /// the transports translate that into "going away". Hook this into
    /// your server's graceful shutdown.
    pub async fn shutdown(&mut self) {
        self.shut_down = true;
        for (_, handle) in self.pumps.drain() {
            handle.shutdown().await;
        }
        self.topics.clear();
    }

    /// The pumps currently running.
    pub fn active_partitions(&self) -> impl Iterator<Item = (&str, i32)> {
        self.pumps.keys().map(|(t, p)| (t.as_str(), *p))
    }
}

/// Where one partition of a topic-level subscription starts.
fn partition_position(position: &TopicPosition, partition: i32) -> Position {
    match position {
        TopicPosition::Earliest => Position::Earliest,
        TopicPosition::Latest => Position::Latest,
        // Absent from the cursor = never seen: replay from the start
        // rather than risk losing records.
        TopicPosition::Offsets(cursor) => cursor
            .get(&partition)
            .map_or(Position::Earliest, |next| Position::Offset(*next)),
    }
}

/// Fold per-partition subscriptions into one merged stream, one
/// forwarder task each.
fn merge(
    topic: &str,
    partitions: Vec<i32>,
    subscriptions: Vec<Subscription>,
    capacity: usize,
) -> TopicSubscription {
    let (merged, receiver) = mpsc::channel(capacity);
    for subscription in subscriptions {
        let merged = merged.clone();
        tokio::spawn(async move {
            let mut receiver = subscription.into_receiver();
            loop {
                tokio::select! {
                    // Watched rather than discovered on the next send:
                    // a forwarder parked in `recv()` still holds its
                    // partition's receiver, so the pump sees a live
                    // subscriber. On a quiet partition nothing ever
                    // arrives to make it notice the merged stream is
                    // gone, and the pump never idles — which is every
                    // disconnected client on a low-traffic topic.
                    () = merged.closed() => return,
                    item = receiver.recv() => match item {
                        Some(item) => {
                            if merged.send(item).await.is_err() {
                                return; // merged stream dropped
                            }
                        }
                        None => return, // this partition's stream ended
                    },
                }
            }
        });
    }
    TopicSubscription {
        topic: topic.to_owned(),
        partitions,
        receiver,
    }
}

/// A refused subscribe from the [`SharedHub`] front door: the
/// [`RejectionKind`] a transport maps to its status code, plus the
/// message for the response body.
///
/// The message is safe to send to an anonymous client. For everything
/// except [`RejectionKind::BadRequest`] — whose text describes the
/// client's own parameters — it is
/// [`RejectionKind::public_message`], and the underlying error is
/// logged at `warn` instead: a real cluster's error text carries
/// bootstrap hostnames and ports, leader and ACL state, and TLS/SASL
/// detail, none of which belongs in a response body.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Rejection {
    pub kind: RejectionKind,
    pub message: String,
}

impl Rejection {
    /// A malformed request parameter (bad `from`, bad filter).
    fn bad_request(message: String) -> Rejection {
        Rejection {
            kind: RejectionKind::BadRequest,
            message,
        }
    }
}

impl From<HubError> for Rejection {
    fn from(e: HubError) -> Rejection {
        let kind = e.rejection_kind();
        tracing::warn!(error = %e, kind = ?kind, "subscribe refused");
        Rejection {
            kind,
            message: kind.public_message().to_owned(),
        }
    }
}

/// A [`Hub`] behind a `tokio::sync::Mutex` — the shape every transport
/// needs: subscribe-time mutation is serialized, and streams run
/// lock-free once created. Wrap it (or a state type holding it) in an
/// `Arc` and share it across handlers; every method takes `&self`.
#[derive(Debug)]
pub struct SharedHub<F: SourceFactory> {
    hub: tokio::sync::Mutex<Hub<F>>,
}

impl<F: SourceFactory> SharedHub<F> {
    pub fn new(factory: F, config: PumpConfig) -> SharedHub<F> {
        SharedHub::from_hub(Hub::new(factory, config))
    }

    /// Wrap a pre-built hub — the way in for hub-level options such as
    /// [`Hub::with_topic_gate`].
    pub fn from_hub(hub: Hub<F>) -> SharedHub<F> {
        SharedHub {
            hub: tokio::sync::Mutex::new(hub),
        }
    }

    /// [`Hub::subscribe`], with the lock held only over the pump map.
    ///
    /// The subscribe round trip itself runs unlocked, so a pump parked
    /// in a long poll delays this caller alone. The lock does span the
    /// *first* subscribe to a partition (creating the pump), which is
    /// what keeps two callers from racing two pumps onto one partition.
    pub async fn subscribe(
        &self,
        topic: &str,
        partition: i32,
        position: Position,
        filter: Filter,
    ) -> Result<Subscription, HubError> {
        let (handle, created) = self.hub.lock().await.acquire(topic, partition).await?;
        match handle.subscribe(position, filter.clone()).await {
            Ok(sub) => Ok(sub),
            Err(HubError::PumpClosed) if !created => {
                let handle = self
                    .hub
                    .lock()
                    .await
                    .respawn(topic, partition, &handle)
                    .await?;
                match handle.subscribe(position, filter).await {
                    Ok(sub) => Ok(sub),
                    Err(e) => {
                        self.hub.lock().await.forget(topic, partition, &handle);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                if created {
                    self.hub.lock().await.forget(topic, partition, &handle);
                    handle.shutdown().await;
                }
                Err(e)
            }
        }
    }

    /// [`Hub::subscribe_topic`], with the per-partition round trips run
    /// concurrently and unlocked: a P-partition topic costs one pump
    /// round trip, not P of them in series behind the hub's lock.
    pub async fn subscribe_topic(
        &self,
        topic: &str,
        position: TopicPosition,
        filter: Filter,
    ) -> Result<TopicSubscription, HubError> {
        let partitions = self.hub.lock().await.partitions_of(topic).await?;
        let mut handles = Vec::with_capacity(partitions.len());
        let capacity = {
            let mut hub = self.hub.lock().await;
            // Rolled back under the same lock the pumps were created
            // under: a topic subscribe that cannot be served in full
            // must not hold slots for the partitions it did reach.
            let before = hub.running_partitions(topic, &partitions);
            for partition in &partitions {
                match hub.acquire(topic, *partition).await {
                    Ok(entry) => handles.push(entry),
                    Err(e) => {
                        hub.release_new(topic, &partitions, &before).await;
                        return Err(e);
                    }
                }
            }
            hub.config.queue_capacity
        };

        let mut pending = Vec::with_capacity(handles.len());
        for ((handle, created), partition) in handles.into_iter().zip(&partitions) {
            let start = partition_position(&position, *partition);
            let filter = filter.clone();
            let partition = *partition;
            pending.push(tokio::spawn(async move {
                let result = handle.subscribe(start, filter).await;
                (partition, created, handle, result)
            }));
        }

        let mut subscriptions = Vec::with_capacity(pending.len());
        let mut retry = Vec::new();
        for task in pending {
            let (partition, created, handle, result) =
                task.await.map_err(|_| HubError::PumpClosed)?;
            match result {
                Ok(subscription) => subscriptions.push(subscription),
                Err(e) if created => {
                    // A pump this call created and that refused its
                    // first subscriber leaves no entry behind.
                    self.hub.lock().await.forget(topic, partition, &handle);
                    handle.shutdown().await;
                    return Err(e);
                }
                // The pump closed between the lookup and the round trip
                // (died, or exited idle): the single-partition path
                // knows how to replace it, and this is rare enough to
                // do one at a time.
                Err(HubError::PumpClosed) => retry.push(partition),
                Err(e) => return Err(e),
            }
        }
        for partition in retry {
            let start = partition_position(&position, partition);
            subscriptions.push(
                self.subscribe(topic, partition, start, filter.clone())
                    .await?,
            );
        }

        Ok(merge(topic, partitions, subscriptions, capacity))
    }

    /// [`Hub::shutdown`]: stop every pump and refuse further
    /// subscribes. Call this from your server's graceful shutdown.
    pub async fn shutdown(&self) {
        self.hub.lock().await.shutdown().await;
    }

    /// How many pumps the hub is holding — the number
    /// [`Hub::with_max_pumps`] bounds. Worth a gauge.
    pub async fn active_pumps(&self) -> usize {
        self.hub.lock().await.active_partitions().count()
    }

    /// [`Hub::reap_exited_pumps`]. Subscribes already reap, so this is
    /// for metrics or for an idle process that wants its memory back
    /// without waiting for the next request.
    pub async fn reap_exited_pumps(&self) -> usize {
        self.hub.lock().await.reap_exited_pumps()
    }

    /// The whole front door for one partition's stream: parse `params`
    /// (with the transport's `resume` token winning over `from`), then
    /// subscribe. Parameter errors come back as
    /// [`RejectionKind::BadRequest`]; subscribe failures carry
    /// [`HubError::rejection_kind`].
    pub async fn stream(
        &self,
        topic: &str,
        partition: i32,
        params: &StreamParams,
        resume: Option<&str>,
    ) -> Result<Subscription, Rejection> {
        let position = params.position(resume).map_err(Rejection::bad_request)?;
        let filter = params.filter().map_err(Rejection::bad_request)?;
        self.subscribe(topic, partition, position, filter)
            .await
            .map_err(Rejection::from)
    }

    /// The front door for a whole-topic stream. Also hands back the
    /// parsed [`TopicPosition`], so a transport that emits cursors
    /// (SSE's event ids) can seed its running cursor from the resume
    /// point.
    pub async fn stream_topic(
        &self,
        topic: &str,
        params: &StreamParams,
        resume: Option<&str>,
    ) -> Result<(TopicSubscription, TopicPosition), Rejection> {
        let position = params
            .topic_position(resume)
            .map_err(Rejection::bad_request)?;
        let filter = params.filter().map_err(Rejection::bad_request)?;
        let subscription = self
            .subscribe_topic(topic, position.clone(), filter)
            .await
            .map_err(Rejection::from)?;
        Ok((subscription, position))
    }
}
