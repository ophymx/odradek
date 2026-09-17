//! KIP-848 smoke test against a real broker: two members join a
//! next-generation consumer group, the coordinator assigns server-side,
//! and the pair ends up sharing the topic's partitions.
//!
//! Needs a broker with the new group coordinator (Kafka 4.x default):
//!
//! ```sh
//! cargo run -p odradek-client --example group848_join -- localhost:9092
//! ```

use std::time::Duration;

use odradek_client::{ClientConfig, Cluster, ConsumerGroupConfig, ConsumerGroupMember, GroupEvent};

const PARTITIONS: i32 = 3;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-848-example".into();
    let topic = format!(
        "odradek-848-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let group = format!("{topic}-group");

    let cluster = Cluster::connect(config).await?;
    cluster.create_topic(&topic, PARTITIONS, 1).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("created {topic} with {PARTITIONS} partitions");

    // Member A joins alone; the coordinator assigns everything to it.
    let mut a = ConsumerGroupMember::join(
        cluster.clone(),
        &group,
        &[&topic],
        ConsumerGroupConfig::default(),
    )
    .await?;
    println!(
        "A joined as {} epoch {}, assignment {:?} (heartbeat every {:?})",
        a.member_id(),
        a.member_epoch(),
        a.assignment(),
        a.heartbeat_interval()
    );

    // A heartbeats in the background at the coordinator's cadence while
    // B joins and triggers a server-side rebalance.
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<()>();
    let a_task = tokio::spawn(async move {
        loop {
            match a.heartbeat().await.expect("A heartbeat") {
                GroupEvent::Stable => {}
                event => println!("A: {event:?}, assignment {:?}", a.assignment()),
            }
            tokio::select! {
                _ = tokio::time::sleep(a.heartbeat_interval()) => {}
                _ = &mut done_rx => return a,
            }
        }
    });

    let mut b =
        ConsumerGroupMember::join(cluster, &group, &[&topic], ConsumerGroupConfig::default())
            .await?;
    println!(
        "B joined as {} epoch {}, assignment {:?}",
        b.member_id(),
        b.member_epoch(),
        b.assignment()
    );

    let count =
        |m: &ConsumerGroupMember| m.assignment().iter().map(|(_, p)| p.len()).sum::<usize>();

    // Reconciliation is incremental: A must acknowledge releasing a
    // partition before the coordinator grants it to B, and B learns of
    // the grant on its next heartbeat — so B keeps beating until the
    // whole topic is owned.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while count(&b) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "reconciliation did not converge"
        );
        tokio::time::sleep(b.heartbeat_interval().min(Duration::from_secs(1))).await;
        if b.heartbeat().await? == GroupEvent::AssignmentChanged {
            println!("B: AssignmentChanged, assignment {:?}", b.assignment());
        }
    }
    let _ = done_tx.send(());
    let a = a_task.await?;
    println!(
        "final: A epoch {} owns {:?}, B epoch {} owns {:?}",
        a.member_epoch(),
        a.assignment(),
        b.member_epoch(),
        b.assignment()
    );
    assert_eq!(
        count(&a) + count(&b),
        PARTITIONS as usize,
        "all partitions owned exactly once"
    );
    assert!(count(&a) > 0 && count(&b) > 0, "both members own some");

    // Fenced commit path: commit under the live epoch.
    a.commit_offset(&topic, a.assignment()[0].1[0], 0).await?;
    println!("A committed under epoch {}", a.member_epoch());

    a.leave().await?;
    b.leave().await?;
    println!("ok");
    Ok(())
}
