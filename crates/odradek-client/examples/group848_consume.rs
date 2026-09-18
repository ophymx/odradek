//! The assembled consumer, on the KIP-848 protocol: join, read what the
//! coordinator assigns, commit, resume.
//!
//! ```sh
//! cargo run -p odradek-client --example group848_consume -- localhost:9092
//! ```
//!
//! The same loop as the `group_consume` example, against the newer group
//! protocol. Read that one first; this shows what changes.
//!
//! What changes is smaller than it looks, and all of it is in the
//! membership half:
//!
//! - **There is no join/sync round.** One `ConsumerGroupHeartbeat` call
//!   does everything, so [`ConsumerGroupMember::heartbeat`] returns a
//!   [`GroupEvent`] rather than a status that sends you back to a
//!   separate rejoin step. `AssignmentChanged` has already been adopted
//!   by the time you see it.
//! - **The broker computes the assignment.** No member is the leader,
//!   and no member runs a partitioning strategy.
//! - **Reconciliation is incremental**, so an assignment can change
//!   without the whole group stopping. A member keeps reading the
//!   partitions it still holds.
//! - **Fencing is by member epoch** rather than by generation id.
//!
//! What does not change: the reading half. [`Consumer`] is the same, the
//! cursors are the same, and commit-after-handling still means
//! at-least-once. Which is the point — the group protocol decides who
//! owns a partition, not how it is read.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{
    ClientConfig, Cluster, Consumer, ConsumerGroupConfig, ConsumerGroupMember, GroupEvent, Producer,
};

const PARTITIONS: i32 = 3;
/// How long to keep polling before deciding the records are not coming.
const DEADLINE: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-848-consume".into();

    let topic = format!(
        "odradek-848c-{}-{}",
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

    // ---- phase one: consume what is already there -------------------
    produce(&cluster, &topic, 0..6).await?;
    println!("produced 6 record(s)");

    let mut member =
        ConsumerGroupMember::join(cluster.clone(), &group, &[&topic], group_config()).await?;
    println!(
        "joined as {} epoch {}, assignment {:?}",
        member.member_id(),
        member.member_epoch(),
        member.assignment()
    );

    let first = consume(&cluster, &mut member, 6).await?;
    println!("phase one read {} record(s): {:?}", first.len(), first);
    assert_eq!(first.len(), 6, "phase one should see every record");

    member.leave().await?;
    println!("left the group");

    // ---- phase two: a new member resumes from the commits ------------
    produce(&cluster, &topic, 6..9).await?;
    println!("produced 3 more record(s)");

    let mut member =
        ConsumerGroupMember::join(cluster.clone(), &group, &[&topic], group_config()).await?;
    println!(
        "rejoined as {} epoch {}",
        member.member_id(),
        member.member_epoch()
    );

    let second = consume(&cluster, &mut member, 3).await?;
    println!("phase two read {} record(s): {:?}", second.len(), second);
    assert_eq!(
        second.len(),
        3,
        "phase two should see only what phase one had not committed"
    );
    for value in &first {
        assert!(
            !second.contains(value),
            "{value} was delivered twice across memberships"
        );
    }
    member.leave().await?;

    println!("ok");
    Ok(())
}

/// The poll loop: heartbeat, read each assigned partition, commit.
async fn consume(
    cluster: &Cluster,
    member: &mut ConsumerGroupMember,
    want: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let consumer = Consumer::new(cluster.clone());
    let mut cursors: HashMap<(String, i32), i64> = HashMap::new();
    let mut seen = Vec::new();
    let deadline = Instant::now() + DEADLINE;

    while seen.len() < want && Instant::now() < deadline {
        match member.heartbeat().await? {
            GroupEvent::Stable => {}
            event => {
                // No rejoin call: the heartbeat that reported this has
                // already adopted the new assignment. Drop the cursors
                // of partitions that are no longer ours and keep the
                // rest — that is what "incremental" buys.
                println!("  {event:?}, assignment {:?}", member.assignment());
                let held: Vec<(String, i32)> = member
                    .assignment()
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions.iter().map(move |p| (topic.clone(), *p))
                    })
                    .collect();
                cursors.retain(|key, _| held.contains(key));
            }
        }

        let assignment: Vec<(String, i32)> = member
            .assignment()
            .iter()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |p| (topic.clone(), *p)))
            .collect();

        for (topic, partition) in assignment {
            let key = (topic.clone(), partition);
            let offset = match cursors.get(&key) {
                Some(offset) => *offset,
                None => {
                    // KIP-848 commits are fetched exactly as before:
                    // OffsetFetch did not change with the protocol.
                    let resume = match member.committed_offset(&topic, partition).await? {
                        Some(committed) => committed,
                        None => consumer.earliest_offset(&topic, partition).await?,
                    };
                    println!("  {topic}[{partition}] starting at {resume}");
                    resume
                }
            };

            let result = consumer.fetch(&topic, partition, offset).await?;
            if result.records.is_empty() {
                cursors.insert(key, result.next_offset);
                continue;
            }
            for record in &result.records {
                let value = record
                    .value
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default()
                    .into_owned();
                seen.push(value);
            }
            cursors.insert(key, result.next_offset);

            // Fenced by member epoch rather than generation id, but the
            // guarantee is the same: a commit from a member the group
            // has moved past is refused, not applied.
            member
                .commit_offset(&topic, partition, result.next_offset)
                .await?;
        }
    }
    Ok(seen)
}

/// Spread `values` across the topic's partitions, one record each.
async fn produce(
    cluster: &Cluster,
    topic: &str,
    values: std::ops::Range<i32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = Producer::new(cluster.clone());
    for i in values {
        producer
            .enqueue(
                topic,
                i % PARTITIONS,
                Record {
                    key: Some(Bytes::from(format!("key-{i}"))),
                    value: Some(Bytes::from(format!("record-{i}"))),
                    ..Default::default()
                },
            )
            .await?;
    }
    producer.flush().await?;
    Ok(())
}

fn group_config() -> ConsumerGroupConfig {
    ConsumerGroupConfig::default()
}
