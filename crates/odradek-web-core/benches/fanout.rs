//! Fan-out cost: one pump over an in-memory log feeding N subscribers
//! that do nothing but drain, plus the per-subscriber JSON rendering the
//! transports do on top.
//!
//! ```sh
//! cargo bench -p odradek-web-core --bench fanout
//! ```
//!
//! Each sample appends [`EVENTS`] records and waits until every
//! subscriber has received all of them, so the reported throughput is
//! *source* events per second (each one fanned out to N subscribers).

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use odradek_web_core::memory::MemoryLog;
use odradek_web_core::{Filter, Position, PumpConfig, PumpHandle, StreamItem, event_json};

const TOPIC: &str = "bench";
/// Records appended per sample.
const EVENTS: usize = 1_000;
/// Subscriber counts to compare.
const FANOUT: [usize; 4] = [1, 10, 100, 500];

/// Queues deep enough that a drain-only subscriber stays on the live
/// path: this measures fan-out, not the catch-up machinery.
fn config() -> PumpConfig {
    let mut config = PumpConfig::default();
    config.ring_capacity = EVENTS * 2;
    config.queue_capacity = EVENTS * 2;
    config.idle_poll = Duration::from_micros(200);
    config.idle_shutdown = None;
    config
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

/// One timed sample: N subscribers each receive `EVENTS` records, with
/// `per_event` applied to every delivery (the transport's share of the
/// work). Setup, teardown, and the warm-up record that puts every
/// subscriber on the live path are outside the clock.
async fn sample<F>(subscribers: usize, per_event: F) -> Duration
where
    F: Fn(&StreamItem) + Copy + Send + 'static,
{
    let log = MemoryLog::new().with_batch_limit(64);
    let pump = PumpHandle::spawn(log.source(), TOPIC, 0, config());
    // Everyone is on the live path once they have seen this record; the
    // barrier is what makes the timed section pure fan-out.
    let ready = Arc::new(tokio::sync::Barrier::new(subscribers + 1));

    let mut drains = Vec::with_capacity(subscribers);
    for _ in 0..subscribers {
        // Offset 0 on an empty log, not `Latest`: identical in effect,
        // but it cannot race the pump's first look at the live edge.
        let mut sub = pump
            .subscribe(Position::Offset(0), Filter::default())
            .await
            .expect("subscribe");
        let ready = Arc::clone(&ready);
        drains.push(tokio::spawn(async move {
            let warm_up = sub.recv().await.expect("warm-up record");
            drop(warm_up.expect("warm-up record"));
            ready.wait().await;
            for _ in 0..EVENTS {
                let item = sub.recv().await.expect("stream ended early");
                per_event(&item);
            }
        }));
    }
    log.append(TOPIC, 0, None, b"warm-up", Vec::new());
    ready.wait().await;

    let start = Instant::now();
    for i in 0..EVENTS {
        log.append(TOPIC, 0, None, format!("value-{i}").as_bytes(), Vec::new());
    }
    for drain in drains {
        drain.await.expect("drain task");
    }
    let elapsed = start.elapsed();
    pump.shutdown().await;
    elapsed
}

fn bench_fanout(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("fanout");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(
        u64::try_from(EVENTS).expect("event count fits u64"),
    ));

    for subscribers in FANOUT {
        // Pure delivery: what the pump itself costs per subscriber.
        group.bench_with_input(
            BenchmarkId::new("drain", subscribers),
            &subscribers,
            |b, &subscribers| {
                b.iter_custom(|iters| {
                    rt.block_on(async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += sample(subscribers, |_| {}).await;
                        }
                        total
                    })
                });
            },
        );

        // What a transport adds: rendering each delivery as JSON, once
        // per subscriber (what both transports used to do).
        group.bench_with_input(
            BenchmarkId::new("render_per_subscriber", subscribers),
            &subscribers,
            |b, &subscribers| {
                b.iter_custom(|iters| {
                    rt.block_on(async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += sample(subscribers, |item| {
                                if let Ok(event) = item {
                                    std::hint::black_box(event_json(event).to_string());
                                }
                            })
                            .await;
                        }
                        total
                    })
                });
            },
        );

        // What they do now: the first subscriber to look renders the
        // event, everyone else takes a handle on those bytes.
        group.bench_with_input(
            BenchmarkId::new("render_shared", subscribers),
            &subscribers,
            |b, &subscribers| {
                b.iter_custom(|iters| {
                    rt.block_on(async move {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            total += sample(subscribers, |item| {
                                if let Ok(event) = item {
                                    std::hint::black_box(event.json_bytes().clone());
                                }
                            })
                            .await;
                        }
                        total
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);
