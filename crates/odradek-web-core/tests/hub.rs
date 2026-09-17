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

    log.append(TOPIC, None, b"one", Vec::new());
    log.append(TOPIC, None, b"two", Vec::new());

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
        log.append(TOPIC, None, format!("old-{i}").as_bytes(), Vec::new());
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

    log.append(TOPIC, None, b"fresh", Vec::new());
    assert_eq!(collect(&mut sub, 1).await, vec![(7, "fresh".into())]);
}

#[tokio::test]
async fn earliest_replays_everything() {
    let log = MemoryLog::new();
    log.append(TOPIC, None, b"zero", Vec::new());
    log.append(TOPIC, None, b"one", Vec::new());
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
    log.append(TOPIC, Some(b"user:1"), b"keep-a", Vec::new());
    log.append(TOPIC, Some(b"order:9"), b"drop", Vec::new());
    let pump = pump_for(&log, PumpConfig::default());

    let filter = Filter {
        key_prefix: Some(Bytes::from_static(b"user:")),
        ..Default::default()
    };
    let mut sub = pump.subscribe(Position::Earliest, filter).await.unwrap();

    log.append(TOPIC, Some(b"user:2"), b"keep-b", Vec::new());
    log.append(TOPIC, Some(b"cart:3"), b"drop", Vec::new());

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
        log.append(TOPIC, None, format!("v{i}").as_bytes(), Vec::new());
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

    log.append(TOPIC, None, b"hello", Vec::new());
    assert_eq!(collect(&mut a, 1).await, vec![(0, "hello".into())]);
}
