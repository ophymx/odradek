//! Engine tests over an in-memory log: fan-out, replay, filtering, and
//! the no-loss guarantee under backpressure.

use std::time::Duration;

use bytes::Bytes;
use odradek_web_core::memory::{MemoryFactory, MemoryLog};
use odradek_web_core::{Filter, Hub, Position, PumpConfig, PumpHandle, Subscription};

const TOPIC: &str = "bridge";

fn pump_for(log: &MemoryLog, config: PumpConfig) -> PumpHandle {
    PumpHandle::spawn(log.source(), TOPIC, 0, config)
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
    let log = MemoryLog::new();
    let pump = pump_for(&log, PumpConfig::default());

    let mut a = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();
    let mut b = pump
        .subscribe(Position::Latest, Filter::default())
        .await
        .unwrap();

    log.append(TOPIC, 0, None, b"one", Vec::new());
    log.append(TOPIC, 0, None, b"two", Vec::new());

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
    let log = MemoryLog::new();
    for i in 0..7 {
        log.append(TOPIC, 0, None, format!("old-{i}").as_bytes(), Vec::new());
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

    log.append(TOPIC, 0, None, b"fresh", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(7, "fresh".into())]);
}

#[tokio::test]
async fn earliest_replays_everything() {
    let log = MemoryLog::new();
    log.append(TOPIC, 0, None, b"zero", Vec::new());
    log.append(TOPIC, 0, None, b"one", Vec::new());
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
    let log = MemoryLog::new();
    log.append(TOPIC, 0, Some(b"user:1"), b"keep-a", Vec::new());
    log.append(TOPIC, 0, Some(b"order:9"), b"drop", Vec::new());
    let pump = pump_for(&log, PumpConfig::default());

    let filter = Filter {
        key_prefix: Some(Bytes::from_static(b"user:")),
        ..Default::default()
    };
    let mut sub = pump.subscribe(Position::Earliest, filter).await.unwrap();

    log.append(TOPIC, 0, Some(b"user:2"), b"keep-b", Vec::new());
    log.append(TOPIC, 0, Some(b"cart:3"), b"drop", Vec::new());

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
    let log = MemoryLog::new();
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
        log.append(TOPIC, 0, None, format!("v{i}").as_bytes(), Vec::new());
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
    let log = MemoryLog::new();
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

    log.append(TOPIC, 0, None, b"hello", Vec::new());
    assert_eq!(collect(&mut a, 1).await, vec![(0, "hello".into())]);
}

#[tokio::test]
async fn topic_subscribe_merges_partitions_in_partition_order() {
    use odradek_web_core::TopicPosition;
    use std::collections::HashMap;

    let log = MemoryLog::with_partitions(3);
    for i in 0..4 {
        log.append(TOPIC, i % 3, None, format!("v{i}").as_bytes(), Vec::new());
    }
    let mut hub = Hub::new(MemoryFactory { log: log.clone() }, PumpConfig::default());
    let mut sub = hub
        .subscribe_topic(TOPIC, TopicPosition::Earliest, Filter::default())
        .await
        .unwrap();
    assert_eq!(sub.partitions(), &[0, 1, 2]);

    // All four replayed events arrive; within each partition, offsets
    // ascend.
    let mut per_partition: HashMap<i32, Vec<i64>> = HashMap::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("stream closed");
        per_partition
            .entry(event.partition)
            .or_default()
            .push(event.offset);
    }
    assert_eq!(per_partition[&0], vec![0, 1]);
    assert_eq!(per_partition[&1], vec![0]);
    assert_eq!(per_partition[&2], vec![0]);

    // Live appends on any partition keep flowing.
    log.append(TOPIC, 2, None, b"live", Vec::new());
    let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out")
        .expect("stream closed");
    assert_eq!((event.partition, event.offset), (2, 1));
}

#[tokio::test]
async fn topic_cursor_resumes_seen_partitions_and_replays_unseen() {
    use odradek_web_core::TopicPosition;
    use std::collections::BTreeMap;

    let log = MemoryLog::with_partitions(2);
    for i in 0..3 {
        log.append(TOPIC, 0, None, format!("p0-{i}").as_bytes(), Vec::new());
        log.append(TOPIC, 1, None, format!("p1-{i}").as_bytes(), Vec::new());
    }
    let mut hub = Hub::new(MemoryFactory { log: log.clone() }, PumpConfig::default());

    // The cursor names partition 0 only: resume it at 2, replay
    // partition 1 from the start (absent = never seen).
    let mut cursor = BTreeMap::new();
    cursor.insert(0, 2i64);
    let mut sub = hub
        .subscribe_topic(TOPIC, TopicPosition::Offsets(cursor), Filter::default())
        .await
        .unwrap();

    let mut seen: Vec<(i32, i64)> = Vec::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("timed out")
            .expect("stream closed");
        seen.push((event.partition, event.offset));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(0, 2), (1, 0), (1, 1), (1, 2)]);
}
