//! Both transports on one server over a real Kafka cluster — the
//! composability the library-first design is for.
//!
//! ```sh
//! cargo run -p odradek-web-ws --example serve -- localhost:9092 127.0.0.1:8080
//! curl -N 'http://127.0.0.1:8080/topics/demo/partitions/0/events?from=earliest'
//! # or connect a WebSocket to ws://127.0.0.1:8080/topics/demo/partitions/0/ws
//! ```

use odradek_web_ws::{ClientConfig, KafkaSourceFactory, PumpConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let listen = args.next().unwrap_or_else(|| "127.0.0.1:8080".into());

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap.clone()];
    config.client_id = "odradek-bridge".into();
    let factory = KafkaSourceFactory::new(config);
    // Each transport keeps its own hub (and thus its own pumps); one
    // shared hub across transports lands with topic-level subscriptions.
    let sse = odradek_web_sse::router(odradek_web_sse::SseState::new(
        factory.clone(),
        PumpConfig::default(),
    ));
    let ws = odradek_web_ws::router(odradek_web_ws::WsState::new(factory, PumpConfig::default()));
    let app = sse.merge(ws);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    println!("bridging {bootstrap} on http://{listen}");
    println!("  sse: /topics/{{topic}}/partitions/{{p}}/events");
    println!("  ws:  /topics/{{topic}}/partitions/{{p}}/ws");
    axum::serve(listener, app).await?;
    Ok(())
}
