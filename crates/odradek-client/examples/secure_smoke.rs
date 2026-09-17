//! Security smoke test against a real broker: connect with TLS and/or
//! SASL, create a topic, produce one record, fetch it back.
//!
//! ```sh
//! cargo run -p odradek-client --example secure_smoke -- <bootstrap> \
//!     [--ca cert.pem] [--mechanism plain|scram256|scram512 --user u --pass p]
//! ```

use bytes::{Bytes, BytesMut};
use odradek_client::protocol::messages::create_topics_request::{
    CreatableTopic, CreateTopicsRequest,
};
use odradek_client::protocol::messages::create_topics_response::CreateTopicsResponse;
use odradek_client::protocol::records::Record;
use odradek_client::{ClientConfig, Cluster, Consumer, Mechanism, Producer, SaslConfig, Tls};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().ok_or("usage: secure_smoke <bootstrap> ...")?;
    let mut tls = Tls::None;
    let mut mechanism = None;
    let mut user = String::new();
    let mut pass = String::new();
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--ca" => tls = Tls::with_ca_pem(&std::fs::read(value()?)?)?,
            "--mechanism" => {
                mechanism = Some(match value()?.as_str() {
                    "plain" => Mechanism::Plain,
                    "scram256" => Mechanism::ScramSha256,
                    "scram512" => Mechanism::ScramSha512,
                    other => return Err(format!("unknown mechanism {other}").into()),
                })
            }
            "--user" => user = value()?,
            "--pass" => pass = value()?,
            other => return Err(format!("unknown flag {other}").into()),
        }
    }
    let config = ClientConfig {
        bootstrap_servers: vec![bootstrap],
        client_id: "odradek-secure-smoke".into(),
        tls,
        sasl: mechanism.map(|mechanism| SaslConfig {
            mechanism,
            username: user,
            password: pass,
        }),
    };

    let topic = format!(
        "odradek-secure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let cluster = Cluster::connect(config.clone()).await?;
    create_topic(&cluster, &topic).await?;

    let mut producer = Producer::new(cluster);
    producer
        .produce(
            &topic,
            0,
            vec![Record {
                value: Some(Bytes::from_static(b"secured")),
                ..Default::default()
            }],
        )
        .await?;

    let mut consumer = Consumer::new(Cluster::connect(config).await?);
    let result = consumer.fetch(&topic, 0, 0).await?;
    assert_eq!(result.records.len(), 1);
    assert_eq!(
        result.records[0].value.as_deref(),
        Some(b"secured".as_slice())
    );
    println!("ok: produced and fetched over the secured connection");
    Ok(())
}

async fn create_topic(cluster: &Cluster, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let broker = cluster.bootstrap_broker();
    let version = broker.ranges.pick(CreateTopicsRequest::API_KEY, (2, 7))?;
    let request = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_owned(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };
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
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}
