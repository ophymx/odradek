//! Engine tests over an in-memory log: fan-out, replay, filtering, and
//! the no-loss guarantee under backpressure.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use odradek_web_core::{
    Event, Filter, Hub, Position, PumpConfig, PumpHandle, RecordSource, SourceBatch, SourceError,
    SourceFactory, Subscription,
};

const TOPIC: &str = "bridge";

/// An in-memory partition log; offsets are indexes.
#[derive(Clone, Default)]
struct MemoryLog {
    events: Arc<Mutex<Vec<Event>>>,
}

impl MemoryLog {
    fn append(&self, key: Option<&str>, value: &str) {
        let mut events = self.events.lock().unwrap();
        let offset = i64::try_from(events.len()).unwrap();
        events.push(Event {
            topic: TOPIC.into(),
            partition: 0,
            offset,
            timestamp: 1_000 + offset,
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: Vec::new(),
        });
    }
}

#[derive(Clone)]
struct MemorySource {
    log: MemoryLog,
}

impl RecordSource for MemorySource {
    async fn fetch(
        &mut self,
        _topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<SourceBatch, SourceError> {
        let events = self.log.events.lock().unwrap();
        let len = i64::try_from(events.len()).unwrap();
        let start = usize::try_from(offset.clamp(0, len)).unwrap();
        // Cap batches so catch-up takes several rounds, like real fetches.
        let batch: Vec<Event> = events
            .iter()
            .skip(start)
            .take(3)
            .map(|e| Event {
                partition,
                ..e.clone()
            })
            .collect();
        let next_offset = offset.max(0) + i64::try_from(batch.len()).unwrap();
        Ok(SourceBatch {
            events: batch,
            next_offset,
            high_watermark: len,
        })
    }

    async fn earliest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Ok(0)
    }

    async fn latest_offset(&mut self, _topic: &str, _partition: i32) -> Result<i64, SourceError> {
        Ok(i64::try_from(self.log.events.lock().unwrap().len()).unwrap())
    }
}

#[derive(Clone)]
struct MemoryFactory {
    log: MemoryLog,
}

impl SourceFactory for MemoryFactory {
    type Source = MemorySource;

    async fn create(&self, _topic: &str, _partition: i32) -> Result<MemorySource, SourceError> {
        Ok(MemorySource {
            log: self.log.clone(),
        })
    }
}

fn pump_for(log: &MemoryLog, config: PumpConfig) -> PumpHandle {
    PumpHandle::spawn(MemorySource { log: log.clone() }, TOPIC, 0, config)
}

async fn collect(sub: &mut Subscription, n: usize) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for _ in 0..n {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out waiting for event")
            .expect("pump closed");
        let value = String::from_utf8(event.value.clone().unwrap().to_vec()).unwrap();
        out.push((event.offset, value));
    }
    out
}

#[tokio::test]
async fn live_fanout_reaches_every_subscriber() {
    let log = MemoryLog::default();
    let pump = pump_for(&log, PumpConfig::default());

    let mut a = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    let mut b = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();

    log.append(None, "one");
    log.append(None, "two");

    assert_eq!(
        collect(&mut a, 2).await,
        vec![(0, "one".into()), (1, "two".into())]
    );
    assert_eq!(
        collect(&mut b, 2).await,
        vec![(0, "one".into()), (1, "two".into())]
    );
}

#[tokio::test]
async fn replay_from_offset_then_live_without_gaps_or_dups() {
    let log = MemoryLog::default();
    for i in 0..7 {
        log.append(None, &format!("old-{i}"));
    }
    let pump = pump_for(&log, PumpConfig::default());

    // Start in history (offset 2), read through the tail, then keep
    // getting live appends — one contiguous offset sequence.
    let mut sub = pump
        .subscribe(Position::Offset(2), Filter::default())
        .await
        .unwrap();
    let history = collect(&mut sub, 5).await;
    assert_eq!(
        history.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
        vec![2, 3, 4, 5, 6]
    );

    log.append(None, "fresh");
    assert_eq!(collect(&mut sub, 1).await, vec![(7, "fresh".into())]);
}

#[tokio::test]
async fn earliest_replays_everything() {
    let log = MemoryLog::default();
    log.append(None, "zero");
    log.append(None, "one");
    let pump = pump_for(&log, PumpConfig::default());

    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();
    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(0, "zero".into()), (1, "one".into())]
    );
}

#[tokio::test]
async fn filters_apply_to_replay_and_live() {
    let log = MemoryLog::default();
    log.append(Some("user:1"), "keep-a");
    log.append(Some("order:9"), "drop");
    let pump = pump_for(&log, PumpConfig::default());

    let filter = Filter {
        key_prefix: Some(Bytes::from_static(b"user:")),
        ..Default::default()
    };
    let mut sub = pump.subscribe(Position::Earliest, filter).await.unwrap();

    log.append(Some("user:2"), "keep-b");
    log.append(Some("cart:3"), "drop");

    assert_eq!(
        collect(&mut sub, 2).await,
        vec![(0, "keep-a".into()), (2, "keep-b".into())]
    );
}

/// The core guarantee: a subscriber slower than the stream still sees
/// every event, in order, exactly once — backpressure demotes it to
/// catch-up instead of dropping.
#[tokio::test]
async fn slow_subscriber_loses_nothing() {
    let log = MemoryLog::default();
    let pump = pump_for(
        &log,
        PumpConfig {
            queue_capacity: 2,
            ring_capacity: 4, // force catch-up past the ring, from source
            ..Default::default()
        },
    );
    let mut sub = pump
        .subscribe(Position::Earliest, Filter::default())
        .await
        .unwrap();

    for i in 0..50 {
        log.append(None, &format!("v{i}"));
    }
    // Drain slowly: the tiny queue overflows many times over.
    let mut seen = Vec::new();
    for _ in 0..50 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("pump closed");
        seen.push(event.offset);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(seen, (0..50).collect::<Vec<_>>());
}

#[tokio::test]
async fn hub_runs_one_pump_per_partition() {
    let log = MemoryLog::default();
    let mut hub = Hub::new(MemoryFactory { log: log.clone() }, PumpConfig::default());

    let mut a = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    let _b = hub
        .subscribe(TOPIC, 1, Position::Latest, Filter::default())
        .await
        .unwrap();
    let _c = hub
        .subscribe(TOPIC, 0, Position::Latest, Filter::default())
        .await
        .unwrap();
    assert_eq!(hub.active_partitions().count(), 2);

    log.append(None, "hello");
    assert_eq!(collect(&mut a, 1).await, vec![(0, "hello".into())]);
}
