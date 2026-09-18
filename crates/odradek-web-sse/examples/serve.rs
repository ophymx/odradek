//! A minimal SSE bridge server over a real Kafka cluster.
//!
//! ```sh
//! cargo run -p odradek-web-sse --example serve -- localhost:9092 127.0.0.1:8080 demo.
//! curl -N 'http://127.0.0.1:8080/topics/demo.orders/partitions/0/events?from=earliest'
//! ```
//!
//! The third argument is the topic prefix this bridge serves, and it is
//! not optional decoration: a hub is born serving nothing, and the gate
//! is what decides which topics this process will read. Without one,
//! "bridge Kafka to the web" means "publish `__consumer_offsets` to the
//! web".
//!
//! Still missing for anything internet-facing, and deliberately left to
//! the embedder: authentication over the router (`route_layer`),
//! per-user authorization if different readers may see different
//! topics, and a connection limit. See the crate README.

use odradek_web_sse::web_core::Hub;
use odradek_web_sse::{ClientConfig, KafkaSourceFactory, PumpConfig, SseState, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let listen = args.next().unwrap_or_else(|| "127.0.0.1:8080".into());
    let prefix = args.next().unwrap_or_else(|| "demo.".into());

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap.clone()];
    config.client_id = "odradek-sse".into();
    let factory = KafkaSourceFactory::new(config);

    // Serve one family of topics, and no more than 256 partitions'
    // worth of pumps at a time (each is a task, a connection, and a
    // ring of up to `ring_capacity` events).
    let hub = Hub::new(factory, PumpConfig::default())
        .with_topic_gate({
            let prefix = prefix.clone();
            move |topic| topic.starts_with(&prefix)
        })
        .with_max_pumps(256);
    let app = router(SseState::from_hub(hub));

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    println!("bridging {bootstrap} on http://{listen}");
    println!("serving topics starting with {prefix:?}; everything else is 403");
    println!(
        "try: curl -N 'http://{listen}/topics/{prefix}<topic>/partitions/0/events?from=earliest'"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
