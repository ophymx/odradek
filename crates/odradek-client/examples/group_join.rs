//! Consumer-group smoke test against a real broker: two members join
//! the same group, ride the rebalance, and end up sharing the topic's
//! partitions.
//!
//! ```sh
//! cargo run -p odradek-client --example group_join -- localhost:9092
//! ```

use std::time::Duration;

use bytes::BytesMut;
use odradek_client::{ClientConfig, Cluster, GroupConfig, GroupMember, HeartbeatStatus};
use odradek_protocol::messages::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use odradek_protocol::messages::create_topics_response::CreateTopicsResponse;

const PARTITIONS: i32 = 3;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "localhost:9092".into());
    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap];
    config.client_id = "odradek-group-example".into();
    let config_b = config.clone();
    let topic = format!(
        "odradek-group-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
    );
    let group = format!("{topic}-group");

    let cluster = Cluster::connect(config).await?;
    create_topic(&cluster, &topic).await?;
    println!("created {topic} with {PARTITIONS} partitions");

    // Member A joins alone and owns everything.
    let mut a = GroupMember::join(cluster, &group, &[&topic], group_config()).await?;
    println!(
        "A joined as {} (leader: {}), assignment {:?}",
        a.member_id(),
        a.is_leader(),
        a.assignment()
    );
    assert_eq!(partition_count(&a), PARTITIONS as usize);

    // A keeps heartbeating in the background, rejoining when member B
    // triggers the rebalance.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let a_task = tokio::spawn(async move {
        let mut done_rx = done_rx;
        loop {
            match a.heartbeat().await.expect("A heartbeat") {
                HeartbeatStatus::Stable => {}
                status => {
                    println!("A: {status:?}; rejoining");
                    a.rejoin().await.expect("A rejoin");
                    println!(
                        "A: rejoined gen {}, assignment {:?}",
                        a.generation_id(),
                        a.assignment()
                    );
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(300)) => {}
                _ = &mut done_rx => return a,
            }
        }
    });

    // Member B joins on its own Cluster. Two members of one group must
    // not share a Cluster: they would share one connection to the
    // coordinator, and Kafka processes a connection's requests strictly
    // in order — B's JoinGroup parks for the whole rebalance and would
    // block A's heartbeats until A is evicted. One member per Cluster
    // (the natural one-per-process topology) sidesteps it.
    let cluster_b = Cluster::connect(config_b).await?;
    let b = GroupMember::join(cluster_b, &group, &[&topic], group_config()).await?;
    println!(
        "B joined as {} (leader: {}), assignment {:?}",
        b.member_id(),
        b.is_leader(),
        b.assignment()
    );

    // Give A a beat to finish its own rejoin, then reconcile.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let _ = done_tx.send(());
    let a = a_task.await?;

    let total = partition_count(&a) + partition_count(&b);
    println!(
        "final generation {}: A owns {:?}, B owns {:?}",
        a.generation_id(),
        a.assignment(),
        b.assignment()
    );
    assert_eq!(a.generation_id(), b.generation_id(), "same generation");
    assert_eq!(
        total, PARTITIONS as usize,
        "all partitions owned exactly once"
    );
    assert!(
        partition_count(&a) > 0 && partition_count(&b) > 0,
        "both members own some"
    );

    a.leave().await?;
    b.leave().await?;
    println!("ok");
    Ok(())
}

fn group_config() -> GroupConfig {
    let mut config = GroupConfig::default();
    config.session_timeout_ms = 10_000;
    config.rebalance_timeout_ms = 30_000;
    config
}

fn partition_count(m: &GroupMember) -> usize {
    m.assignment().iter().map(|(_, p)| p.len()).sum()
}

async fn create_topic(cluster: &Cluster, topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let broker = cluster.bootstrap_broker();
    let version = broker.ranges.pick(CreateTopicsRequest::API_KEY, (2, 7))?;
    let mut creatable = CreatableTopic::default();
    creatable.name = topic.to_owned();
    creatable.num_partitions = PARTITIONS;
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
    tokio::time::sleep(Duration::from_millis(500)).await;
    Ok(())
}
