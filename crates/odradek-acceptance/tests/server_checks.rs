//! The server-side check suite, run against in-process subjects: one
//! conformant, one deliberately broken.

use bytes::{BufMut, Bytes, BytesMut};
use odradek_acceptance::checks;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_SUPPORTED: i16 = 4;

fn advertised_keys() -> Vec<ApiVersion> {
    [(18, 0, MAX_SUPPORTED), (0, 3, 12), (3, 0, 13)]
        .into_iter()
        .map(|(api_key, min_version, max_version)| ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        })
        .collect()
}

/// A subject server. `corr_offset` != 0 makes it echo wrong correlation ids.
async fn subject_server(listener: TcpListener, corr_offset: i32) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(handle_connection(stream, corr_offset));
    }
}

async fn handle_connection(mut stream: TcpStream, corr_offset: i32) {
    loop {
        let mut len_bytes = [0u8; 4];
        if stream.read_exact(&mut len_bytes).await.is_err() {
            return;
        }
        let len = i32::from_be_bytes(len_bytes) as usize;
        let mut frame = vec![0u8; len];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let mut frame = Bytes::from(frame);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);

        let (header, resp, encode_at) = if api_version <= MAX_SUPPORTED {
            let header_version = if api_version >= 3 { 2 } else { 1 };
            let header = RequestHeader::decode(&mut frame, header_version).expect("request header");
            let resp = ApiVersionsResponse {
                error_code: 0,
                api_keys: advertised_keys(),
                ..Default::default()
            };
            (header, resp, api_version)
        } else {
            // Unknown future version: parse the header at our newest known
            // header version, reply UNSUPPORTED_VERSION encoded at v0.
            let header = RequestHeader::decode(&mut frame, 2).expect("request header");
            let resp = ApiVersionsResponse {
                error_code: 35,
                api_keys: advertised_keys(),
                ..Default::default()
            };
            (header, resp, 0)
        };

        // ApiVersions responses always use response header v0.
        let resp_header = ResponseHeader {
            correlation_id: header.correlation_id.wrapping_add(corr_offset),
            unknown_tagged_fields: Vec::new(),
        };
        let mut out = BytesMut::new();
        out.put_i32(0);
        resp_header.encode(&mut out, 0).unwrap();
        resp.encode(&mut out, encode_at).unwrap();
        let len = i32::try_from(out.len() - 4).unwrap();
        out[..4].copy_from_slice(&len.to_be_bytes());
        if stream.write_all(&out).await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn compliant_server_passes_all_checks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(subject_server(listener, 0));

    let report = checks::server::run(&addr).await;
    server.abort();

    assert!(report.is_conformant(), "unexpected failures:\n{report}");
    assert_eq!(report.passed(), 4, "expected all checks to pass:\n{report}");
}

#[tokio::test]
async fn broken_correlation_echo_is_caught() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(subject_server(listener, 1));

    let report = checks::server::run(&addr).await;
    server.abort();

    assert!(!report.is_conformant(), "broken subject passed:\n{report}");
}
