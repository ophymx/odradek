//! Bridge smoke test against a real broker: produce records, then
//! subscribe through the hub — replay from earliest, keep tailing live.
//!
//! ```sh
//! cargo run -p odradek-web-core --example tail -- localhost:9092
//! ```

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{ClientConfig, Cluster, Producer};
use odradek_web_core::{Filter, Hub, KafkaSourceFactory, Position, PumpConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-web-tail".into();
    let topic = format!(
        "odradek-tail-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );

    let cluster = Cluster::connect(config.clone()).await?;
    cluster.create_topic(&topic, 1, 1).await?;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let mut producer = Producer::new(cluster);
    for i in 0..5 {
        producer
            .enqueue(
                &topic,
                0,
                Record {
                    key: Some(Bytes::from(format!("k{i}"))),
                    value: Some(Bytes::from(format!("historical {i}"))),
                    ..Default::default()
                },
            )
            .await?;
    }
    producer.flush().await?;
    println!("produced 5 historical records to {topic}");

    // A web-facing subscriber replays history, then rides the live
    // tail. The gate names the one topic this hub exists to serve —
    // a hub without one serves nothing at all, on purpose.
    let mut hub =
        Hub::new(KafkaSourceFactory::new(config), PumpConfig::default()).with_topic_gate({
            let allowed = topic.clone();
            move |topic| topic == allowed
        });
    let mut sub = hub
        .subscribe(&topic, 0, Position::Earliest, Filter::default())
        .await?;

    for _ in 0..5 {
        let event = sub.recv().await.ok_or("pump closed")??;
        println!(
            "  replay offset {}: {}",
            event.offset,
            String::from_utf8_lossy(event.value.as_deref().unwrap_or_default())
        );
    }

    producer
        .produce(
            &topic,
            0,
            vec![Record {
                value: Some(Bytes::from_static(b"live event")),
                ..Default::default()
            }],
        )
        .await?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(10), sub.recv())
        .await?
        .ok_or("pump closed")??;
    println!(
        "  live offset {}: {}",
        event.offset,
        String::from_utf8_lossy(event.value.as_deref().unwrap_or_default())
    );
    assert_eq!(event.offset, 5);
    println!("ok");
    Ok(())
}
