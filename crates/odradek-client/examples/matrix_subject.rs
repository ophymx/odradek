//! This client, put through the client-side acceptance harness as a
//! subject — the driver behind `cargo xtask client-matrix`'s
//! `odradek-client` rows.
//!
//! ```sh
//! cargo run -p odradek-client --example matrix_subject -- <bootstrap> \
//!     produce|consume [--topic t] [--partition n] [--record-per-request] \
//!     [--oauthbearer]
//! ```
//!
//! Every other subject in that matrix is somebody else's client, run
//! from a container. This one is ours, and the point of including it is
//! that until now nothing had ever put this client in front of a party
//! that judges what it says: its own tests answer it with fixtures it
//! agrees with by construction, and the conformance matrix runs the
//! other direction entirely. The harness is not a broker and does not
//! try to be — it serves empty logs and grades the requests.
//!
//! Which is why this is written against the public API and nothing else.
//! A driver that reached for internals, or that was shaped around what
//! the checks look for, would be testing the harness's opinion of itself.
//! What runs here is what a caller would write.

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{ClientConfig, Cluster, Consumer, Producer, ProducerConfig, SaslConfig};

/// Records per scenario.
///
/// More than one deliberately: a throttle is only observable if there is
/// a later request to hold back, and a leader move only if there is a
/// retry. One record would make half the catalogue skip.
const RECORDS: usize = 5;

/// The topic the harness advertises, one partition per impersonated
/// broker.
const DEFAULT_TOPIC: &str = "odradek-routing";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args
        .next()
        .ok_or("usage: matrix_subject <bootstrap> produce|consume [options]")?;
    let mode = args.next().ok_or("expected `produce` or `consume`")?;
    let mut topic = DEFAULT_TOPIC.to_owned();
    let mut partition: Option<i32> = None;
    let mut record_per_request = false;
    let mut oauthbearer = false;
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--topic" => topic = value()?,
            "--partition" => partition = Some(value()?.parse()?),
            "--record-per-request" => record_per_request = true,
            "--oauthbearer" => oauthbearer = true,
            other => return Err(format!("unknown flag {other}").into()),
        }
    }

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-matrix-subject".into();
    if oauthbearer {
        config.sasl = Some(SaslConfig::oauthbearer("odradek-matrix-token"));
        // The harness listens on plain TCP, and a bearer token is a
        // credential anyone on the path could replay, so the client
        // refuses to send one over an unencrypted socket unless told
        // the exposure is intended. Saying so here is the same
        // deliberate choice a loopback deployment makes; it is not a
        // concession the checks needed.
        config.allow_plaintext_credentials = true;
    }

    let cluster = Cluster::connect(config).await?;
    let partitions: Vec<i32> = match partition {
        Some(p) => vec![p],
        None => cluster
            .topic_partitions(&topic)
            .await?
            .iter()
            .map(|p| p.index)
            .collect(),
    };
    if partitions.is_empty() {
        return Err(format!("{topic} has no partitions").into());
    }

    match mode.as_str() {
        "produce" => produce(&cluster, &topic, &partitions, record_per_request).await?,
        "consume" => consume(&cluster, &topic, &partitions).await?,
        other => return Err(format!("unknown mode {other}").into()),
    }
    Ok(())
}

/// Send [`RECORDS`] records across `partitions`.
///
/// `record_per_request` is the throttle scenario's shape: one record per
/// produce request, so there is a second request on the same connection
/// for the client to hold back. Batched, the whole run is one request
/// per partition and a throttle has nothing behind it to delay.
async fn produce(
    cluster: &Cluster,
    topic: &str,
    partitions: &[i32],
    record_per_request: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = Producer::with_config(cluster.clone(), ProducerConfig::default());
    for i in 0..RECORDS {
        let partition = partitions[i % partitions.len()];
        let record = Record {
            value: Some(Bytes::from(format!("record {i}"))),
            ..Default::default()
        };
        if record_per_request {
            let offset = producer.produce(topic, partition, vec![record]).await?;
            println!("produced to {topic}[{partition}] at offset {offset}");
        } else {
            producer.enqueue(topic, partition, record).await?;
        }
    }
    for delivery in producer.flush().await? {
        println!(
            "produced {} record(s) to {}[{}] at base offset {}",
            delivery.records, delivery.topic, delivery.partition, delivery.base_offset
        );
    }
    Ok(())
}

/// Ask where each partition starts, then read from there.
///
/// The fetch is unconditional. Every log the harness serves is empty, so
/// a consumer that skipped the read because there was nothing to read
/// would be correct and would also never exercise the fetch half of
/// `client/routes-to-partition-leader`.
async fn consume(
    cluster: &Cluster,
    topic: &str,
    partitions: &[i32],
) -> Result<(), Box<dyn std::error::Error>> {
    let consumer = Consumer::new(cluster.clone());
    for &partition in partitions {
        let start = consumer.earliest_offset(topic, partition).await?;
        let result = consumer.fetch(topic, partition, start).await?;
        println!(
            "fetched {} record(s) from {topic}[{partition}] at offset {start}",
            result.records.len()
        );
    }
    Ok(())
}
