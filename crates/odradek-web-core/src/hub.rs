//! The hub: pumps on demand, one per (topic, partition), and
//! topic-level subscriptions merging every partition into one stream.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::event::{Event, Filter, Position, TopicPosition};
use crate::pump::{HubError, PumpConfig, PumpHandle, Subscription};
use crate::source::SourceFactory;

/// Fans partitions out to any number of subscribers, creating a
/// [`PumpHandle`] per (topic, partition) lazily via the factory.
#[derive(Debug)]
pub struct Hub<F: SourceFactory> {
    factory: F,
    config: PumpConfig,
    pumps: HashMap<(String, i32), PumpHandle>,
    /// Discovered partitions per topic (first topic-level subscribe).
    topics: HashMap<String, Vec<i32>>,
}

/// A merged stream over every partition of one topic. Order holds
/// within each partition; partitions interleave.
#[derive(Debug)]
pub struct TopicSubscription {
    pub topic: String,
    partitions: Vec<i32>,
    receiver: mpsc::Receiver<Event>,
}

impl TopicSubscription {
    /// The next event, or `None` when every partition's pump has shut
    /// down.
    pub async fn recv(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }

    /// The underlying receiver, for `select!`-style composition.
    pub fn into_receiver(self) -> mpsc::Receiver<Event> {
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
        if !self.topics.contains_key(topic) {
            let partitions = self
                .factory
                .partitions(topic)
                .await
                .map_err(|e| HubError::Source(e.to_string()))?;
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
                while let Some(event) = receiver.recv().await {
                    if merged.send(event).await.is_err() {
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
        let key = (topic.to_owned(), partition);
        if !self.pumps.contains_key(&key) {
            let source = self
                .factory
                .create(topic, partition)
                .await
                .map_err(|e| HubError::Source(e.to_string()))?;
            let handle = PumpHandle::spawn(source, topic, partition, self.config.clone());
            self.pumps.insert(key.clone(), handle);
        }
        let handle = &self.pumps[&key];
        match handle.subscribe(position, filter.clone()).await {
            Ok(sub) => Ok(sub),
            Err(HubError::PumpClosed) => {
                // The pump died (source error budget exhausted); replace
                // it once and retry.
                let source = self
                    .factory
                    .create(topic, partition)
                    .await
                    .map_err(|e| HubError::Source(e.to_string()))?;
                let handle = PumpHandle::spawn(source, topic, partition, self.config.clone());
                self.pumps.insert(key.clone(), handle);
                self.pumps[&key].subscribe(position, filter).await
            }
            Err(e) => Err(e),
        }
    }

    /// The pumps currently running.
    pub fn active_partitions(&self) -> impl Iterator<Item = (&str, i32)> {
        self.pumps.keys().map(|(t, p)| (t.as_str(), *p))
    }
}
