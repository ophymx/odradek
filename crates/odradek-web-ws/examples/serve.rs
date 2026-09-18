//! Both transports on one server over a real Kafka cluster — the
//! composability the library-first design is for.
//!
//! ```sh
//! cargo run -p odradek-web-ws --example serve -- \
//!     localhost:9092 127.0.0.1:8080 demo. https://app.example.com
//! curl -N 'http://127.0.0.1:8080/topics/demo.orders/partitions/0/events?from=earliest'
//! # or connect a WebSocket to ws://127.0.0.1:8080/topics/demo.orders/partitions/0/ws
//! ```
//!
//! Two arguments here are security configuration rather than taste:
//!
//! - the **topic prefix** this bridge serves — a hub serves nothing
//!   until its gate says otherwise, because the alternative default is
//!   every topic on the cluster, `__consumer_offsets` included;
//! - the **browser origin** allowed to open WebSockets. CORS does not
//!   apply to WebSockets, so without this any page a user visits could
//!   open a socket here (with their cookies) and read the stream.
//!   Omit it and only non-browser clients can connect.
//!
//! Still left to the embedder, as a library should: authentication over
//! the merged router (`route_layer`), per-user authorization, and a
//! connection limit. See the crate README.

use odradek_web_ws::web_core::Hub;
use odradek_web_ws::{ClientConfig, KafkaSourceFactory, OriginPolicy, PumpConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let bootstrap = args.next().unwrap_or_else(|| "localhost:9092".into());
    let listen = args.next().unwrap_or_else(|| "127.0.0.1:8080".into());
    let prefix = args.next().unwrap_or_else(|| "demo.".into());
    let origin = args.next();

    let mut config = ClientConfig::default();
    config.bootstrap_servers = vec![bootstrap.clone()];
    config.client_id = "odradek-bridge".into();
    let factory = KafkaSourceFactory::new(config);

    let gate = {
        let prefix = prefix.clone();
        move |topic: &str| topic.starts_with(&prefix)
    };
    // Each transport keeps its own hub (and thus its own pumps); one
    // shared hub across transports lands with topic-level subscriptions.
    let sse_hub = Hub::new(factory.clone(), PumpConfig::default())
        .with_topic_gate(gate.clone())
        .with_max_pumps(256);
    let ws_hub = Hub::new(factory, PumpConfig::default())
        .with_topic_gate(gate)
        .with_max_pumps(256);

    let origins = match &origin {
        Some(origin) => OriginPolicy::allow([origin]),
        // No browser origin named: non-browser clients only.
        None => OriginPolicy::deny_cross_origin(),
    };

    let sse = odradek_web_sse::router(odradek_web_sse::SseState::from_hub(sse_hub));
    let ws = odradek_web_ws::router(odradek_web_ws::WsState::from_hub_with_origins(
        ws_hub, origins,
    ));
    let app = sse.merge(ws);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    println!("bridging {bootstrap} on http://{listen}");
    println!("  topics: {prefix}* (everything else is 403)");
    match &origin {
        Some(origin) => println!("  websocket origin: {origin}"),
        None => println!("  websocket origin: none allowed (non-browser clients only)"),
    }
    println!("  sse: /topics/{{topic}}/partitions/{{p}}/events");
    println!("  ws:  /topics/{{topic}}/partitions/{{p}}/ws");
    axum::serve(listener, app).await?;
    Ok(())
}
