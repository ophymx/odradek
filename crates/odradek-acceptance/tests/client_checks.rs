//! Calibration of the client-side checks: our own client must pass them
//! (dogfooding), and a deliberately misbehaving raw client must be caught.

use std::time::Duration;

use bytes::{BufMut, BytesMut};
use odradek_acceptance::Verdict;
use odradek_acceptance::checks::client::{ObserveConfig, run};
use odradek_client::{ClientConfig, Connection};
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn config() -> ObserveConfig {
    ObserveConfig {
        max_requests: 8,
        idle_timeout: Duration::from_millis(1500),
    }
}

#[tokio::test]
async fn odradek_client_passes_the_client_checks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let harness = tokio::spawn(async move { run(&listener, &config()).await });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ranges = conn.negotiate().await.unwrap();
    let version = ranges.pick(3, (0, 13)).unwrap();
    let req = MetadataRequest {
        topics: Some(vec![]),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let mut resp = conn.request(3, version, &body).await.unwrap();
    let meta = MetadataResponse::decode(&mut resp, version).unwrap();
    assert_eq!(meta.cluster_id.as_deref(), Some("odradek-harness"));
    drop(conn); // close the connection so the observation session ends

    let report = harness.await.unwrap();
    assert!(
        report.is_conformant(),
        "our client is nonconformant:\n{report}"
    );
    assert_eq!(
        report.passed(),
        5,
        "expected every client check to pass:\n{report}"
    );
}

#[tokio::test]
async fn misbehaving_client_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let harness = tokio::spawn(async move { run(&listener, &config()).await });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    // Sin #1: first request is Metadata, not ApiVersions.
    // Sin #2: the body is garbage.
    send_frame(&mut stream, &request_frame(3, 12, 7, &[0xff, 0xff, 0xff])).await;
    // Sin #3: version far outside anything advertised.
    // Sin #4: correlation id 7 reused.
    send_frame(&mut stream, &request_frame(3, 99, 7, &[0xff, 0xff, 0xff])).await;
    // Give the harness time to read both before we vanish.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(stream);

    let report = harness.await.unwrap();
    for id in [
        "client/starts-with-api-versions",
        "client/correlation-ids-unique",
        "client/respects-advertised-versions",
        "client/body-decodes",
    ] {
        assert!(
            matches!(report.verdict(id), Some(Verdict::Fail { .. })),
            "{id} did not catch the misbehavior:\n{report}"
        );
    }
    // The headers themselves were well-formed; no false positive there.
    assert!(
        matches!(
            report.verdict("client/header-well-formed"),
            Some(Verdict::Pass)
        ),
        "false positive on header check:\n{report}"
    );
}

fn request_frame(api_key: i16, api_version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let header = RequestHeader {
        request_api_key: api_key,
        request_api_version: api_version,
        correlation_id,
        client_id: Some("misbehaving".into()),
        unknown_tagged_fields: Vec::new(),
    };
    let mut payload = BytesMut::new();
    // Metadata v9+ is flexible → header v2 for both frames.
    header.encode(&mut payload, 2).unwrap();
    payload.extend_from_slice(body);
    payload.to_vec()
}

async fn send_frame(stream: &mut TcpStream, payload: &[u8]) {
    let mut frame = BytesMut::new();
    frame.put_i32(i32::try_from(payload.len()).unwrap());
    frame.extend_from_slice(payload);
    stream.write_all(&frame).await.unwrap();
}
