//! End-to-end smoke test against a real broker: create a topic, produce
//! a compressed batch, fetch it back, and commit/read an offset.
//!
//! ```sh
//! cargo run -p odradek-client --example produce_consume -- localhost:9092 [gzip|lz4|snappy|zstd|none]
//! ```

use bytes::{Bytes, BytesMut};
use odradek_client::{ClientConfig, Cluster, Consumer, Producer, ProducerConfig};
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;
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
    create_topic(&cluster, &topic).await?;
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

async fn create_topic(cluster: &Cluster, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    // CreateTopics has no client-layer wrapper yet; speak it raw through
    // the bootstrap connection.
    let broker = cluster.bootstrap_broker();
    let version = broker.ranges.pick(CreateTopicsRequest::API_KEY, (2, 7))?;
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.to_owned();
    creatable.num_partitions = 1;
    creatable.replication_factor = 1;
    let mut request = CreateTopicsRequest::default();
    request.topics = vec![creatable];
    request.timeout_ms = 30_000;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = broker
        .conn
        .request(CreateTopicsRequest::API_KEY, version, &body)
        .await?;
    let resp = CreateTopicsResponse::decode(&mut resp, version)?;
    let code = resp.topics.first().map_or(-1, |t| t.error_code);
    if code != 0 {
        return Err(format!("CreateTopics failed with error code {code}").into());
    }
    // Give leadership a moment to settle before the first produce.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}
