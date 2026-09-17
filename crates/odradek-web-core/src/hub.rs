//! The hub: pumps on demand, one per (topic, partition).

use std::collections::HashMap;

use crate::event::{Filter, Position};
use crate::pump::{HubError, PumpConfig, PumpHandle, Subscription};
use crate::source::SourceFactory;

/// Fans partitions out to any number of subscribers, creating a
/// [`PumpHandle`] per (topic, partition) lazily via the factory.
#[derive(Debug)]
pub struct Hub<F: SourceFactory> {
    factory: F,
    config: PumpConfig,
    pumps: HashMap<(String, i32), PumpHandle>,
}

impl<F: SourceFactory> Hub<F> {
    pub fn new(factory: F, config: PumpConfig) -> Hub<F> {
        Hub {
            factory,
            config,
            pumps: HashMap::new(),
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
