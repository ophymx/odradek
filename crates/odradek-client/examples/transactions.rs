//! Transactions: write a set of records that a reader sees all of or
//! none of, and commit what was read in the same breath.
//!
//! ```sh
//! cargo run -p odradek-client --example transactions -- localhost:9092
//! ```
//!
//! Three phases, each demonstrating one thing that is only true with
//! transactions:
//!
//! 1. **An aborted transaction leaves nothing behind.** Records are
//!    produced and then abandoned. A `read_uncommitted` consumer sees
//!    them — they really were written to the log — and a
//!    `read_committed` consumer sees none of them. That difference is
//!    the whole feature, and it is worth seeing both halves: the data
//!    is not deleted, it is disowned, and filtering it is the reading
//!    client's job.
//!
//! 2. **A committed transaction appears at once.** Records across two
//!    partitions are committed together; neither partition shows a
//!    partial write.
//!
//! 3. **Consume-transform-produce is atomic end to end.** The input
//!    offsets are committed *inside* the output transaction, so there
//!    is no window where the output exists and the input looks
//!    unprocessed, or the reverse.
//!
//! # Why the reads retry
//!
//! [`Producer::commit_transaction`] returns when the coordinator has
//! durably decided to commit, which is a moment before the commit
//! markers reach the partitions. Until they do, a `read_committed`
//! fetch correctly returns nothing: the last stable offset has not
//! moved yet. A poll loop is the honest way to wait for that, and a
//! single fetch right after the commit would be a flake generator.

use std::time::{Duration, Instant};

use bytes::Bytes;
use odradek_client::protocol::records::Record;
use odradek_client::{
    ClientConfig, Cluster, Consumer, ConsumerConfig, FetchResult, GroupConfig, GroupMember,
    IsolationLevel, Producer, ProducerConfig, TransactionalOffset,
};

const PARTITIONS: i32 = 2;
/// How long to keep polling for records a commit has promised.
const DEADLINE: Duration = Duration::from_secs(30);

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-transactions".into();

    let stamp = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let input = format!("odradek-txn-in-{stamp}");
    let output = format!("odradek-txn-out-{stamp}");

    let cluster = Cluster::connect(config).await?;
    cluster.create_topic(&input, PARTITIONS, 1).await?;
    cluster.create_topic(&output, PARTITIONS, 1).await?;
    // Give leadership a moment to settle before the first produce.
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("created {input} and {output}, {PARTITIONS} partitions each");

    let mut producer = Producer::with_config(
        cluster.clone(),
        ProducerConfig::transactional(format!("odradek-txn-{stamp}")),
    );
    // Claims the id, fencing anyone who held it before and rolling back
    // whatever they left open. Once, before anything else.
    producer.init_transactions().await?;
    println!(
        "initialized transactions ({:?})",
        producer.transaction_state()
    );

    // ---- phase one: an abort leaves nothing behind -------------------
    producer.begin_transaction()?;
    for i in 0..3 {
        producer
            .produce(&output, 0, vec![record(&format!("abandoned-{i}"))])
            .await?;
    }
    producer.abort_transaction().await?;
    println!("aborted a transaction of 3 record(s)");

    let uncommitted = read(&cluster, &output, 0, IsolationLevel::ReadUncommitted).await?;
    println!(
        "read_uncommitted sees {} record(s): {:?}",
        uncommitted.records.len(),
        values(&uncommitted)
    );
    assert_eq!(
        uncommitted.records.len(),
        3,
        "the records were written; aborting does not unwrite them"
    );

    // Wait for the abort marker to reach the partition before reading
    // committed. Until it does, the last stable offset is still 0 and
    // the broker withholds those records on its own — a read that
    // returned nothing then would prove nothing about this client's
    // filtering, which is the thing being demonstrated.
    let committed = poll_stable_past(&cluster, &output, 0, uncommitted.next_offset).await?;
    println!(
        "read_committed sees {} record(s), stable through {}",
        committed.records.len(),
        committed.last_stable_offset
    );
    assert!(
        committed.last_stable_offset > uncommitted.next_offset - 1,
        "the abort marker has not landed yet, so nothing is being proven"
    );
    assert!(
        committed.records.is_empty(),
        "a read_committed consumer must see none of an aborted transaction"
    );

    // ---- phase two: a commit spans partitions ------------------------
    producer.begin_transaction()?;
    for partition in 0..PARTITIONS {
        producer
            .produce(
                &output,
                partition,
                vec![record(&format!("kept-p{partition}"))],
            )
            .await?;
    }
    producer.commit_transaction().await?;
    println!("committed across {PARTITIONS} partitions");

    for partition in 0..PARTITIONS {
        let seen = poll_committed(&cluster, &output, partition, 1).await?;
        println!("partition {partition} committed: {seen:?}");
        assert_eq!(seen, vec![format!("kept-p{partition}")]);
    }

    // ---- phase three: consume-transform-produce ----------------------
    // Input for the loop to process, written non-transactionally: it
    // stands in for whatever upstream system is feeding this one.
    let mut plain = Producer::new(cluster.clone());
    for i in 0..4 {
        plain
            .produce(&input, 0, vec![record(&format!("in-{i}"))])
            .await?;
    }
    println!("produced 4 input record(s)");

    let group = format!("{input}-group");
    let member = GroupMember::join(cluster.clone(), &group, &[&input], group_config()).await?;
    println!("joined {group} as {}", member.member_id());

    let consumer = Consumer::new(cluster.clone());
    let batch = consumer.fetch(&input, 0, 0).await?;
    let inputs: Vec<String> = batch
        .records
        .iter()
        .map(|record| value_of(record.value.as_deref()))
        .collect();
    println!("read {} input record(s): {inputs:?}", inputs.len());
    assert_eq!(inputs.len(), 4);

    // The transformation and the record of having consumed it go into
    // one transaction. A crash anywhere in here leaves both undone.
    producer.begin_transaction()?;
    for value in &inputs {
        producer
            .produce(&output, 1, vec![record(&format!("{value}->out"))])
            .await?;
    }
    producer
        .send_offsets_to_transaction(
            &member,
            &[TransactionalOffset::new(&input, 0, batch.next_offset)],
        )
        .await?;
    producer.commit_transaction().await?;
    println!("committed 4 output record(s) and the input position together");

    let produced = poll_committed(&cluster, &output, 1, inputs.len() + 1).await?;
    println!("partition 1 committed: {produced:?}");
    for value in &inputs {
        assert!(
            produced.contains(&format!("{value}->out")),
            "{value} was not transformed into the output"
        );
    }

    // The offset landed inside the transaction, so a new member of the
    // same group resumes past the records already processed.
    //
    // Polled, not read once, and the reason is the same one that makes
    // `poll_committed` above a loop. A commit makes the coordinator
    // write a marker to every partition the transaction touched, and
    // the offsets topic is one of those partitions — a separate write,
    // to a separate leader, from the one that publishes the output
    // records. Seeing the output is therefore no promise that the
    // offset is readable yet. On one broker the two land together often
    // enough to look synchronous; on a three-node cluster this read
    // came back `None` while the records were already there.
    let committed_offset = poll_offset(&member, &input, 0, batch.next_offset).await?;
    println!("committed input offset: {committed_offset:?}");
    assert_eq!(
        committed_offset,
        Some(batch.next_offset),
        "the input position should have been committed with the output"
    );
    member.leave().await?;

    println!("ok");
    Ok(())
}

fn record(value: &str) -> Record {
    Record {
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        ..Default::default()
    }
}

fn value_of(value: Option<&[u8]>) -> String {
    String::from_utf8_lossy(value.unwrap_or_default()).into_owned()
}

fn group_config() -> GroupConfig {
    GroupConfig::default()
}

/// One fetch at the given isolation level, from the start of the
/// partition.
async fn read(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    isolation_level: IsolationLevel,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let mut config = ConsumerConfig::default();
    config.isolation_level = isolation_level;
    let consumer = Consumer::with_config(cluster.clone(), config);
    Ok(consumer.fetch(topic, partition, 0).await?)
}

fn values(result: &FetchResult) -> Vec<String> {
    result
        .records
        .iter()
        .map(|record| value_of(record.value.as_deref()))
        .collect()
}

/// Poll a read_committed fetch until the partition is stable at or past
/// `offset` — that is, until every transaction below it has finished
/// one way or the other.
async fn poll_stable_past(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let result = read(cluster, topic, partition, IsolationLevel::ReadCommitted).await?;
        if result.last_stable_offset >= offset || Instant::now() >= deadline {
            return Ok(result);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll the group's committed offset until it reaches `want`.
///
/// Same wait as [`poll_committed`], for a different partition: the
/// offsets topic gets its own marker, written separately from the one
/// that publishes the output records, so the output being visible says
/// nothing about the offset being readable yet.
async fn poll_offset(
    member: &GroupMember,
    topic: &str,
    partition: i32,
    want: i64,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let seen = member.committed_offset(topic, partition).await?;
        if seen == Some(want) || Instant::now() >= deadline {
            return Ok(seen);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll a partition until `want` committed records show up.
///
/// The wait is the point: a commit returns before its markers land, so
/// the records become visible shortly *after* the call that committed
/// them returned. See the module docs.
async fn poll_committed(
    cluster: &Cluster,
    topic: &str,
    partition: i32,
    want: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let seen = values(&read(cluster, topic, partition, IsolationLevel::ReadCommitted).await?);
        if seen.len() >= want || Instant::now() >= deadline {
            return Ok(seen);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
