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

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::event::{Filter, Position, TopicPosition};
use crate::pump::{HubError, PumpConfig, PumpHandle, StreamItem, Subscription};
use crate::source::SourceFactory;

type TopicGate = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Fans partitions out to any number of subscribers, creating a
/// [`PumpHandle`] per (topic, partition) lazily via the factory.
pub struct Hub<F: SourceFactory> {
    factory: F,
    config: PumpConfig,
    pumps: HashMap<(String, i32), PumpHandle>,
    /// Discovered partitions per topic (first topic-level subscribe).
    topics: HashMap<String, Vec<i32>>,
    /// Topics this hub will serve at all; `None` allows everything.
    gate: Option<TopicGate>,
    shut_down: bool,
}

impl<F: SourceFactory + std::fmt::Debug> std::fmt::Debug for Hub<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("factory", &self.factory)
            .field("config", &self.config)
            .field("pumps", &self.pumps)
            .field("topics", &self.topics)
            .field("gated", &self.gate.is_some())
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
    pub fn new(factory: F, config: PumpConfig) -> Hub<F> {
        Hub {
            factory,
            config,
            pumps: HashMap::new(),
            topics: HashMap::new(),
            gate: None,
            shut_down: false,
        }
    }

    /// Serve only topics `gate` approves; everything else fails
    /// subscribe with [`HubError::Denied`] *before* any pump or source
    /// connection is created. Without a gate, every topic the source
    /// knows is reachable through the hub.
    #[must_use]
    pub fn with_topic_gate(mut self, gate: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.gate = Some(Arc::new(gate));
        self
    }

    fn check_topic(&self, topic: &str) -> Result<(), HubError> {
        if self.shut_down {
            return Err(HubError::ShutDown);
        }
        if let Some(gate) = &self.gate
            && !gate(topic)
        {
            return Err(HubError::Denied(topic.to_owned()));
        }
        Ok(())
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
        self.check_topic(topic)?;
        if !self.topics.contains_key(topic) {
            let partitions = self.factory.partitions(topic).await?;
            self.topics.insert(topic.to_owned(), partitions);
        }
        let partitions = self.topics[topic].clone();

        let (merged, receiver) = mpsc::channel(self.config.queue_capacity);
        for partition in &partitions {
            let partition_position = match &position {
                TopicPosition::Earliest => Position::Earliest,
                TopicPosition::Latest => Position::Latest,
                // Absent from the cursor = never seen: replay from the
                // start rather than risk losing records.
                TopicPosition::Offsets(cursor) => cursor
                    .get(partition)
                    .map_or(Position::Earliest, |next| Position::Offset(*next)),
            };
            let subscription = self
                .subscribe(topic, *partition, partition_position, filter.clone())
                .await?;
            let merged = merged.clone();
            tokio::spawn(async move {
                let mut receiver = subscription.into_receiver();
                while let Some(item) = receiver.recv().await {
                    if merged.send(item).await.is_err() {
                        return; // merged stream dropped
                    }
                }
            });
        }
        Ok(TopicSubscription {
            topic: topic.to_owned(),
            partitions,
            receiver,
        })
    }

    /// Subscribe to one partition, starting the pump on first use.
    pub async fn subscribe(
        &mut self,
        topic: &str,
        partition: i32,
        position: Position,
        filter: Filter,
    ) -> Result<Subscription, HubError> {
        self.check_topic(topic)?;
        let key = (topic.to_owned(), partition);
        let mut created = false;
        if !self.pumps.contains_key(&key) {
            let handle = self.spawn_pump(topic, partition).await?;
            self.pumps.insert(key.clone(), handle);
            created = true;
        }
        let handle = &self.pumps[&key];
        match handle.subscribe(position, filter.clone()).await {
            Ok(sub) => Ok(sub),
            Err(HubError::PumpClosed) if !created => {
                // The pump died (error budget exhausted) or exited
                // idle; replace it once and retry.
                let handle = match self.spawn_pump(topic, partition).await {
                    Ok(handle) => handle,
                    Err(e) => {
                        // Do not cache a dead entry for a topic the
                        // source no longer creates pumps for.
                        self.pumps.remove(&key);
                        return Err(e);
                    }
                };
                self.pumps.insert(key.clone(), handle);
                match self.pumps[&key].subscribe(position, filter).await {
                    Ok(sub) => Ok(sub),
                    Err(e) => {
                        self.pumps.remove(&key);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                if created {
                    // First creation failed outright (e.g. the topic
                    // does not exist): no permanent dead map entry.
                    if let Some(handle) = self.pumps.remove(&key) {
                        handle.shutdown().await;
                    }
                }
                Err(e)
            }
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
