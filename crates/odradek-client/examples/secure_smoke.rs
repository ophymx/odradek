//! Security smoke test against a real broker: connect with TLS and/or
//! SASL, create a topic, produce one record, fetch it back.
//!
//! ```sh
//! cargo run -p odradek-client --example secure_smoke -- <bootstrap> \
//!     [--ca cert.pem] [--client-cert c.pem --client-key k.pem] \
//!     [--mechanism plain|scram256|scram512 --user u --pass p] \
//!     [--allow-plaintext-credentials]
//! ```
//!
//! `--client-cert`/`--client-key` turn on mutual TLS, for a cluster with
//! `ssl.client.auth=required`: the connection itself is authenticated by
//! the certificate, so SASL is optional alongside it rather than the only
//! way to say who you are. Both flags are needed together, and `--ca` is
//! what the broker's own certificate is checked against.
//!
//! `--mechanism plain` without `--ca` would put the password on an
//! unencrypted socket, so it is refused unless
//! `--allow-plaintext-credentials` says that is intended (a local broker
//! in a lab, say).

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{ClientConfig, Cluster, Consumer, Mechanism, Producer, SaslConfig, Tls};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().ok_or("usage: secure_smoke <bootstrap> ...")?;
    let mut ca = None;
    let mut client_cert = None;
    let mut client_key = None;
    let mut mechanism = None;
    let mut user = String::new();
    let mut pass = String::new();
    let mut allow_plaintext_credentials = false;
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--ca" => ca = Some(std::fs::read(value()?)?),
            "--client-cert" => client_cert = Some(std::fs::read(value()?)?),
            "--client-key" => client_key = Some(std::fs::read(value()?)?),
            "--allow-plaintext-credentials" => allow_plaintext_credentials = true,
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
    // Trust and identity are separate choices; combine whichever were
    // given. A client certificate without a CA would mean presenting an
    // identity to a broker whose own identity is unverified.
    let tls = match (&ca, &client_cert, &client_key) {
        (None, None, None) => Tls::None,
        (Some(ca), None, None) => Tls::with_ca_pem(ca)?,
        (Some(ca), Some(cert), Some(key)) => Tls::with_ca_and_client_auth(ca, cert, key)?,
        (None, Some(cert), Some(key)) => Tls::system_with_client_auth(cert, key)?,
        _ => return Err("--client-cert and --client-key go together".into()),
    };

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-secure-smoke".into();
    config.tls = tls;
    config.allow_plaintext_credentials = allow_plaintext_credentials;
    config.sasl = mechanism.map(|mechanism| SaslConfig::new(mechanism, user, pass));

    let topic = format!(
        "odradek-secure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let cluster = Cluster::connect(config).await?;
    cluster.create_topic(&topic, 1, 1).await?;
    // Give leadership a moment to settle before the first produce.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut producer = Producer::new(cluster.clone());
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

    let consumer = Consumer::new(cluster);
    let result = consumer.fetch(&topic, 0, 0).await?;
    assert_eq!(result.records.len(), 1);
    assert_eq!(
        result.records[0].value.as_deref(),
        Some(b"secured".as_slice())
    );
    println!("ok: produced and fetched over the secured connection");
    Ok(())
}
