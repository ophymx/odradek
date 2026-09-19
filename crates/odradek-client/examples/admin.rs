//! Operator questions: what groups exist, what are they doing, how is a
//! topic configured — and deleting one when you are done with it.
//!
//! ```sh
//! cargo run -p odradek-client --example admin -- localhost:9092
//! ```

use odradek_client::{ClientConfig, Cluster};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![std::env::args().nth(1).unwrap_or("localhost:9092".into())];
    config.client_id = "odradek-admin".into();
    let cluster = Cluster::connect(config).await?;

    let topic = format!("odradek-admin-{}", std::process::id());
    cluster.create_topic(&topic, 1, 1).await?;
    println!("created {topic}");

    let entries = cluster.describe_topic_config(&topic).await?;
    println!("{} config entries; a few:", entries.len());
    for entry in entries.iter().filter(|e| {
        matches!(
            e.name.as_str(),
            "cleanup.policy" | "retention.ms" | "max.message.bytes"
        )
    }) {
        println!(
            "  {:<20} = {:<12} (source {}{})",
            entry.name,
            entry.value.as_deref().unwrap_or("<not disclosed>"),
            entry.source,
            if entry.read_only { ", read-only" } else { "" }
        );
    }

    let groups = cluster.list_groups().await?;
    println!(
        "{} group(s) across {} broker(s)",
        groups.len(),
        cluster.brokers().len()
    );
    for group in groups.iter().take(3) {
        println!(
            "  {:<40} {:<10} coordinator {}",
            group.group_id, group.state, group.coordinator_id
        );
    }
    if let Some(first) = groups.first() {
        let described = cluster.describe_groups(&[&first.group_id]).await?;
        for d in &described {
            println!(
                "  described {}: state={} protocol={:?} members={}",
                d.group_id,
                d.state,
                d.protocol,
                d.members.len()
            );
        }
    }

    cluster.delete_topics(&[&topic]).await?;
    println!("deleted {topic}");
    Ok(())
}
