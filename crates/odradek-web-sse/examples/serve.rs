//! A minimal SSE bridge server over a real Kafka cluster.
//!
//! ```sh
//! cargo run -p odradek-web-sse --example serve -- localhost:9092 127.0.0.1:8080
//! curl -N 'http://127.0.0.1:8080/topics/demo/partitions/0/events?from=earliest'
//! ```

use odradek_client::ClientConfig;
use odradek_web_core::{KafkaSourceFactory, PumpConfig};
use odradek_web_sse::{SseState, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let listen = args.next().unwrap_or_else(|| "127.0.0.1:8080".into());

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap.clone()];
    config.client_id = "odradek-sse".into();
    let factory = KafkaSourceFactory::new(config);
    let app = router(SseState::new(factory, PumpConfig::default()));
    let listener = tokio::net::TcpListener::bind(&listen).await?;
    println!("bridging {bootstrap} on http://{listen}");
    println!("try: curl -N 'http://{listen}/topics/<topic>/partitions/0/events?from=earliest'");
    axum::serve(listener, app).await?;
    Ok(())
}
