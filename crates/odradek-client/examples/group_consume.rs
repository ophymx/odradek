//! The assembled consumer: join a group, read the partitions it assigns
//! you, commit as you go, and resume where you left off.
//!
//! ```sh
//! cargo run -p odradek-client --example group_consume -- localhost:9092
//! ```
//!
//! [`GroupMember`] decides *which* partitions are yours; [`Consumer`]
//! reads a partition. This example is the loop between them, which the
//! library deliberately does not wrap: a poll loop encodes opinions —
//! when to commit, what to do with a record that fails to process,
//! whether a rebalance should drop in-flight work — and those belong to
//! the application, not to a client library that would have to guess.
//! The assembly is about forty lines, and they are all below.
//!
//! What it demonstrates, in two phases:
//!
//! 1. A member joins, is assigned every partition of a fresh topic,
//!    reads the records already there, and commits its position.
//! 2. That member leaves, more records are produced, and a *new* member
//!    joins the same group — picking up exactly the new records. No
//!    duplicates, no gap: the second member never saw phase one, it just
//!    read the committed offsets.
//!
//! Notice which `commit_offset` this uses. [`GroupMember::commit_offset`]
//! is fenced by the group generation, so a commit from a member the group
//! has already rebalanced away from is refused rather than allowed to
//! overwrite the new owner's progress. [`Consumer::commit_offset`] is the
//! unfenced simple-consumer path, and is the wrong one here.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{
    ClientConfig, Cluster, Consumer, GroupConfig, GroupMember, HeartbeatStatus, Producer,
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
    config.client_id = "odradek-group-consume".into();

    let topic = format!(
        "odradek-consume-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let group = format!("{topic}-group");

    let cluster = Cluster::connect(config).await?;
    cluster.create_topic(&topic, PARTITIONS, 1).await?;
    // Give leadership a moment to settle before the first produce.
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("created {topic} with {PARTITIONS} partitions");

    // ---- phase one: consume what is already there -------------------
    produce(&cluster, &topic, 0..6).await?;
    println!("produced 6 record(s)");

    let mut member = GroupMember::join(cluster.clone(), &group, &[&topic], group_config()).await?;
    println!(
        "joined as {} (leader: {}), assignment {:?}",
        member.member_id(),
        member.is_leader(),
        member.assignment()
    );

    let first = consume(&cluster, &mut member, 6).await?;
    println!("phase one read {} record(s): {:?}", first.len(), first);
    assert_eq!(first.len(), 6, "phase one should see every record");

    // Leaving tells the coordinator at once rather than waiting out the
    // session timeout. The committed offsets outlive the membership —
    // that is the entire point of committing them.
    member.leave().await?;
    println!("left the group");

    // ---- phase two: a new member resumes from the commits ------------
    produce(&cluster, &topic, 6..9).await?;
    println!("produced 3 more record(s)");

    let mut member = GroupMember::join(cluster.clone(), &group, &[&topic], group_config()).await?;
    println!("rejoined as {}", member.member_id());

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
///
/// Returns once `want` records have been read or [`DEADLINE`] passes.
/// A real consumer runs this until it is told to stop; the record count
/// is here so the example terminates.
async fn consume(
    cluster: &Cluster,
    member: &mut GroupMember,
    want: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let consumer = Consumer::new(cluster.clone());
    // Where to read next, per partition. Cleared on rebalance: a
    // partition that moves to another member takes its cursor with it.
    let mut cursors: HashMap<(String, i32), i64> = HashMap::new();
    let mut seen = Vec::new();
    let deadline = Instant::now() + DEADLINE;

    while seen.len() < want && Instant::now() < deadline {
        // Heartbeat first: it is how the coordinator learns this member
        // is alive, and how this member learns the group moved on.
        match member.heartbeat().await? {
            HeartbeatStatus::Stable => {}
            status => {
                // Rebalancing or evicted. Rejoin, then forget every
                // cursor — the new assignment decides what is ours, and
                // anything we still held may now belong to someone else.
                println!("  {status:?}: rejoining");
                member.rejoin().await?;
                cursors.clear();
                println!("  rejoined gen {}", member.generation_id());
                continue;
            }
        }

        // `assignment` is borrowed from the member, and committing needs
        // the member too, so take a copy of this round's partitions.
        let assignment: Vec<(String, i32)> = member
            .assignment()
            .iter()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |p| (topic.clone(), *p)))
            .collect();

        for (topic, partition) in assignment {
            let key = (topic.clone(), partition);
            // First sight of a partition this generation: resume from
            // the group's committed offset, or from the log start if
            // this group has never committed one.
            let offset = match cursors.get(&key) {
                Some(offset) => *offset,
                None => {
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

            // Commit after the records are handled, not before: a commit
            // is a promise that everything below this offset is done,
            // and crashing between the two should re-deliver rather than
            // skip. That choice — at-least-once here — is exactly the
            // kind of thing a wrapped poll loop would have made for you.
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

fn group_config() -> GroupConfig {
    let mut config = GroupConfig::default();
    // Short timeouts keep the example brisk; production values are the
    // defaults, which give a member room to be slow before eviction.
    config.session_timeout_ms = 10_000;
    config.rebalance_timeout_ms = 10_000;
    config
}
