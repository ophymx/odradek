//! Read an existing topic partition from earliest to latest and print
//! every record — handy for checking interop with records produced by
//! other clients (compression codecs, headers, keys).
//!
//! ```sh
//! cargo run -p odradek-client --example consume -- localhost:9092 my-topic [partition]
//! ```

use odradek_client::{ClientConfig, Cluster, Consumer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let topic = args
        .next()
        .ok_or("usage: consume <bootstrap> <topic> [partition]")?;
    let partition: i32 = args.next().map_or(Ok(0), |p| p.parse())?;

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-consume".into();
    let cluster = Cluster::connect(config).await?;
    let mut consumer = Consumer::new(cluster);

    let mut offset = consumer.earliest_offset(&topic, partition).await?;
    let end = consumer.latest_offset(&topic, partition).await?;
    let mut total = 0u64;
    while offset < end {
        let result = consumer.fetch(&topic, partition, offset).await?;
        for record in &result.records {
            println!(
                "offset {}: {} = {}",
                record.offset,
                String::from_utf8_lossy(record.key.as_deref().unwrap_or_default()),
                String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()),
            );
        }
        total += result.records.len() as u64;
        if result.next_offset == offset {
            break;
        }
        offset = result.next_offset;
    }
    println!("{total} record(s) in {topic}[{partition}]");
    Ok(())
}
