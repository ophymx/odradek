//! End-to-end smoke test against a real broker: create a topic, produce
//! a compressed batch, fetch it back, and commit/read an offset.
//!
//! ```sh
//! cargo run -p odradek-client --example produce_consume -- localhost:9092 [gzip|lz4|snappy|zstd|none]
//! ```

use bytes::Bytes;
use odradek_client::{ClientConfig, Cluster, Consumer, Producer, ProducerConfig};
use odradek_protocol::records::{Compression, Record};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let codec = match args.next().as_deref() {
        Some("gzip") => Compression::Gzip,
        Some("lz4") => Compression::Lz4,
        Some("snappy") => Compression::Snappy,
        Some("zstd") => Compression::Zstd,
        Some("none") | None => Compression::None,
        Some(other) => return Err(format!("unknown codec {other}").into()),
    };
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-example".into();

    // A fresh topic per run.
    let topic = format!(
        "odradek-example-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let cluster = Cluster::connect(config).await?;
    cluster.create_topic(&topic, 1, 1).await?;
    // Give leadership a moment to settle before the first produce.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!("created topic {topic}");

    // Produce one compressed batch of three records.
    let mut producer_config = ProducerConfig::default();
    producer_config.compression = codec;
    let mut producer = Producer::with_config(cluster.clone(), producer_config);
    for i in 0..3 {
        producer
            .enqueue(
                &topic,
                0,
                Record {
                    key: Some(Bytes::from(format!("key-{i}"))),
                    value: Some(Bytes::from(format!("value {i} via {codec:?}"))),
                    ..Default::default()
                },
            )
            .await?;
    }
    for delivery in producer.flush().await? {
        println!(
            "produced {} record(s) to {}[{}] at base offset {}",
            delivery.records, delivery.topic, delivery.partition, delivery.base_offset
        );
    }

    // Fetch them back and commit a position.
    // The consumer shares the producer's connections and metadata.
    let consumer = Consumer::new(cluster);
    let result = consumer.fetch(&topic, 0, 0).await?;
    for record in &result.records {
        println!(
            "  offset {}: {} = {}",
            record.offset,
            String::from_utf8_lossy(record.key.as_deref().unwrap_or_default()),
            String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()),
        );
    }
    assert_eq!(result.records.len(), 3, "expected the produced records");

    let group = format!("{topic}-group");
    consumer
        .commit_offset(&group, &topic, 0, result.next_offset)
        .await?;
    let committed = consumer.committed_offset(&group, &topic, 0).await?;
    println!("committed offset {committed:?} under group {group}");
    assert_eq!(committed, Some(result.next_offset));
    println!("ok");
    Ok(())
}
