//! End-to-end tests of the connection layer against an in-process fake
//! broker speaking real wire bytes through the same protocol crate.

use bytes::{BufMut, Bytes, BytesMut};
use odradek_client::{ClientConfig, Connection};
use odradek_protocol::header::{request_header_version, response_header_version};
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::metadata_request::MetadataRequest;
use odradek_protocol::messages::metadata_response::MetadataResponse;
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Request {
    header: RequestHeader,
    body: Bytes,
}

async fn read_request(stream: &mut TcpStream) -> Request {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.expect("frame len");
    let len = i32::from_be_bytes(len_bytes) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.expect("frame body");
    let mut frame = Bytes::from(frame);

    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let header_version = request_header_version(api_key, api_version).expect("header version");
    let header = RequestHeader::decode(&mut frame, header_version).expect("request header");
    Request {
        header,
        body: frame,
    }
}

async fn write_response(stream: &mut TcpStream, req: &RequestHeader, body: &[u8]) {
    let header_version =
        response_header_version(req.request_api_key, req.request_api_version).unwrap();
    let header = ResponseHeader {
        correlation_id: req.correlation_id,
        unknown_tagged_fields: Vec::new(),
    };
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    header.encode(&mut frame, header_version).unwrap();
    frame.extend_from_slice(body);
    let len = i32::try_from(frame.len() - 4).unwrap();
    frame[..4].copy_from_slice(&len.to_be_bytes());
    stream.write_all(&frame).await.unwrap();
}

fn api_versions_body(version: i16, error_code: i16, keys: &[(i16, i16, i16)]) -> Bytes {
    let resp = ApiVersionsResponse {
        error_code,
        api_keys: keys
            .iter()
            .map(|&(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    resp.encode(&mut body, version).unwrap();
    body.freeze()
}

#[tokio::test]
async fn negotiate_with_modern_broker() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let broker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let req = read_request(&mut stream).await;
        assert_eq!(req.header.request_api_key, 18);
        assert_eq!(
            req.header.request_api_version,
            ApiVersionsRequest::MAX_VERSION
        );
        assert_eq!(req.header.client_id.as_deref(), Some("odradek"));
        let parsed =
            ApiVersionsRequest::decode(&mut req.body.clone(), req.header.request_api_version)
                .unwrap();
        assert_eq!(parsed.client_software_name, "odradek");
        let body = api_versions_body(
            req.header.request_api_version,
            0,
            &[(0, 3, 12), (1, 4, 17), (3, 0, 13), (18, 0, 4)],
        );
        write_response(&mut stream, &req.header, &body).await;
    });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ranges = conn.negotiate().await.unwrap();
    broker.await.unwrap();

    assert_eq!(ranges.range(3), Some((0, 13)));
    // Metadata: our generated support is 0-13, broker says 0-13 → pick 13.
    assert_eq!(ranges.pick(3, (0, 13)).unwrap(), 13);
    // Fetch: broker min 4, our hypothetical max 12 → pick 12.
    assert_eq!(ranges.pick(1, (0, 12)).unwrap(), 12);
    // Disjoint ranges are an error.
    assert!(ranges.pick(1, (0, 3)).is_err());
}

#[tokio::test]
async fn negotiate_falls_back_when_broker_is_older() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    // A broker that only speaks ApiVersions v0: it answers anything newer
    // with UNSUPPORTED_VERSION encoded at v0, advertising its real range.
    let broker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        loop {
            let req = read_request(&mut stream).await;
            assert_eq!(req.header.request_api_key, 18);
            if req.header.request_api_version > 0 {
                let body = api_versions_body(0, 35, &[(18, 0, 0)]);
                write_response(&mut stream, &req.header, &body).await;
            } else {
                let body = api_versions_body(0, 0, &[(18, 0, 0), (3, 0, 8)]);
                write_response(&mut stream, &req.header, &body).await;
                break;
            }
        }
    });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ranges = conn.negotiate().await.unwrap();
    broker.await.unwrap();

    assert_eq!(ranges.range(3), Some((0, 8)));
    assert_eq!(ranges.pick(3, (0, 13)).unwrap(), 8);
}

#[tokio::test]
async fn responses_match_by_correlation_id_out_of_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    // Read two Metadata requests, then answer them in reverse order, each
    // echoing the requested topic name as the cluster id.
    let broker = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let first = read_request(&mut stream).await;
        let second = read_request(&mut stream).await;
        for req in [second, first] {
            let parsed =
                MetadataRequest::decode(&mut req.body.clone(), req.header.request_api_version)
                    .unwrap();
            let topic = parsed.topics.unwrap()[0].name.clone();
            let resp = MetadataResponse {
                cluster_id: topic,
                ..Default::default()
            };
            let mut body = BytesMut::new();
            resp.encode(&mut body, req.header.request_api_version)
                .unwrap();
            write_response(&mut stream, &req.header, &body).await;
        }
    });

    let conn = Connection::connect(&addr, &ClientConfig::default())
        .await
        .unwrap();
    let ask = |name: &str| {
        let conn = conn.clone();
        let name = name.to_owned();
        async move {
            let req = MetadataRequest {
                topics: Some(vec![
                    odradek_protocol::messages::metadata_request::MetadataRequestTopic {
                        name: Some(name),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            };
            let mut body = BytesMut::new();
            req.encode(&mut body, 12).unwrap();
            let mut resp = conn.request(3, 12, &body).await.unwrap();
            MetadataResponse::decode(&mut resp, 12).unwrap().cluster_id
        }
    };

    let (one, two) = tokio::join!(ask("one"), ask("two"));
    broker.await.unwrap();
    assert_eq!(one.as_deref(), Some("one"));
    assert_eq!(two.as_deref(), Some("two"));
}
