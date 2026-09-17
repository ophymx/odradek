//! Where events come from: an offset-addressed record source.
//!
//! The pump is written against this trait so the engine tests run
//! against an in-memory log; [`KafkaSource`] adapts
//! [`odradek_client::Consumer`] for the real thing.

use std::future::Future;

use odradek_client::{ClientConfig, Cluster, Consumer};

use crate::event::Event;

/// A fetch's worth of events plus the cursors that follow it.
#[derive(Debug, Clone, Default)]
pub struct SourceBatch {
    pub events: Vec<Event>,
    /// Where the next fetch should start.
    pub next_offset: i64,
    pub high_watermark: i64,
}

/// An error from a source; the pump treats every source error as
/// transient and retries with backoff (a permanently broken source ends
/// the pump after the configured error budget).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SourceError(pub String);

/// An offset-addressed stream of records for one partition.
///
/// Methods return `impl Future + Send` (rather than plain `async fn`) so
/// pumps holding a source can be spawned.
pub trait RecordSource: Send + 'static {
    fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> impl Future<Output = Result<SourceBatch, SourceError>> + Send;

    fn earliest_offset(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> impl Future<Output = Result<i64, SourceError>> + Send;

    fn latest_offset(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> impl Future<Output = Result<i64, SourceError>> + Send;
}

/// Makes one source per pump; a pump owns its source exclusively.
pub trait SourceFactory: Send + Sync + 'static {
    type Source: RecordSource;

    fn create(
        &self,
        topic: &str,
        partition: i32,
    ) -> impl Future<Output = Result<Self::Source, SourceError>> + Send;
}

/// A [`RecordSource`] over a real Kafka cluster.
#[derive(Debug)]
pub struct KafkaSource {
    consumer: Consumer,
}

impl KafkaSource {
    pub fn new(consumer: Consumer) -> KafkaSource {
        KafkaSource { consumer }
    }
}

impl RecordSource for KafkaSource {
    async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<SourceBatch, SourceError> {
        let result = self
            .consumer
            .fetch(topic, partition, offset)
            .await
            .map_err(|e| SourceError(e.to_string()))?;
        Ok(SourceBatch {
            events: result
                .records
                .into_iter()
                .map(|r| Event {
                    topic: topic.to_owned(),
                    partition,
                    offset: r.offset,
                    timestamp: r.timestamp,
                    key: r.key,
                    value: r.value,
                    headers: r.headers.into_iter().map(|h| (h.key, h.value)).collect(),
                })
                .collect(),
            next_offset: result.next_offset,
            high_watermark: result.high_watermark,
        })
    }

    async fn earliest_offset(&mut self, topic: &str, partition: i32) -> Result<i64, SourceError> {
        self.consumer
            .earliest_offset(topic, partition)
            .await
            .map_err(|e| SourceError(e.to_string()))
    }

    async fn latest_offset(&mut self, topic: &str, partition: i32) -> Result<i64, SourceError> {
        self.consumer
            .latest_offset(topic, partition)
            .await
            .map_err(|e| SourceError(e.to_string()))
    }
}

/// Dials a fresh Kafka connection per pump.
#[derive(Debug, Clone)]
pub struct KafkaSourceFactory {
    pub config: ClientConfig,
}

impl SourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<KafkaSource, SourceError> {
        let cluster = Cluster::connect(self.config.clone())
            .await
            .map_err(|e| SourceError(e.to_string()))?;
        Ok(KafkaSource::new(Consumer::new(cluster)))
    }
}
