//! Round-trip and golden-byte tests for the generated message types.

use bytes::{Bytes, BytesMut};
use odradek_protocol::EncodeError;
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use odradek_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use odradek_protocol::messages::metadata_request::{MetadataRequest, MetadataRequestTopic};
use odradek_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use odradek_protocol::messages::request_header::RequestHeader;
use odradek_protocol::messages::response_header::ResponseHeader;
use odradek_protocol::wire::RawTaggedField;

fn encode<T>(
    msg: &T,
    version: i16,
    enc: impl Fn(&T, &mut BytesMut, i16) -> Result<(), EncodeError>,
) -> Bytes {
    let mut buf = BytesMut::new();
    enc(msg, &mut buf, version).expect("encode");
    buf.freeze()
}

macro_rules! roundtrip {
    ($ty:ty, $msg:expr, $version:expr) => {{
        let msg = $msg;
        let mut bytes = encode(&msg, $version, |m, b, v| m.encode(b, v));
        let decoded = <$ty>::decode(&mut bytes, $version).expect("decode");
        assert!(
            bytes.is_empty(),
            "decode left {} trailing byte(s)",
            bytes.len()
        );
        assert_eq!(decoded, msg, "round-trip mismatch at version {}", $version);
    }};
}

#[test]
fn api_versions_request_golden_v3() {
    let req = ApiVersionsRequest {
        client_software_name: "odradek".into(),
        client_software_version: "0.1.0".into(),
        unknown_tagged_fields: Vec::new(),
    };
    let bytes = encode(&req, 3, |m, b, v| m.encode(b, v));
    // compact "odradek" (len+1=8) + compact "0.1.0" (len+1=6) + empty tagged section
    let expected = [&[0x08][..], b"odradek", &[0x06], b"0.1.0", &[0x00]].concat();
    assert_eq!(&bytes[..], &expected[..]);
    roundtrip!(ApiVersionsRequest, req, 3);
}

#[test]
fn api_versions_request_v0_is_empty_body() {
    let req = ApiVersionsRequest::default();
    let bytes = encode(&req, 0, |m, b, v| m.encode(b, v));
    assert!(bytes.is_empty());
    roundtrip!(ApiVersionsRequest, req, 0);
}

#[test]
fn api_versions_response_roundtrip_all_versions() {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion {
                api_key: 0,
                min_version: 3,
                max_version: 13,
                ..Default::default()
            },
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            },
        ],
        throttle_time_ms: 0,
        ..Default::default()
    };
    for version in ApiVersionsResponse::MIN_VERSION..=ApiVersionsResponse::MAX_VERSION {
        // throttle_time_ms only exists from v1; keep it default (0) so
        // equality holds across versions.
        roundtrip!(ApiVersionsResponse, resp.clone(), version);
    }
}

#[test]
fn unknown_tagged_fields_roundtrip() {
    let resp = ApiVersionsResponse {
        unknown_tagged_fields: vec![RawTaggedField {
            tag: 99, // no known meaning; must survive raw
            data: Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
        }],
        ..Default::default()
    };
    // Flexible version keeps the raw tagged payload byte-for-byte.
    roundtrip!(ApiVersionsResponse, resp, 3);
}

#[test]
fn known_tagged_fields_materialize() {
    // A raw tag 1 payload (int64 -1) decodes into the materialized field,
    // not into unknown_tagged_fields.
    let carrier = ApiVersionsResponse {
        unknown_tagged_fields: vec![RawTaggedField {
            tag: 1,
            data: Bytes::from_static(&[0xff; 8]),
        }],
        ..Default::default()
    };
    let bytes = encode(&carrier, 3, |m, b, v| m.encode(b, v));
    let decoded = ApiVersionsResponse::decode(&mut bytes.clone(), 3).unwrap();
    assert_eq!(decoded.finalized_features_epoch, Some(-1));
    assert!(decoded.unknown_tagged_fields.is_empty());
    // Re-encoding the materialized form reproduces the same bytes.
    assert_eq!(encode(&decoded, 3, |m, b, v| m.encode(b, v)), bytes);
}

#[test]
fn tagged_struct_fields_roundtrip_mixed_with_unknown() {
    use odradek_protocol::messages::fetch_response::{
        FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
    };
    let resp = FetchResponse {
        responses: vec![FetchableTopicResponse {
            topic_id: [7u8; 16],
            partitions: vec![PartitionData {
                partition_index: 3,
                error_code: 6, // NOT_LEADER_OR_FOLLOWER
                current_leader: Some(LeaderIdAndEpoch {
                    leader_id: 2,
                    leader_epoch: 9,
                    ..Default::default()
                }),
                unknown_tagged_fields: vec![RawTaggedField {
                    tag: 42,
                    data: Bytes::from_static(b"future"),
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    // v16 also exercises the top-level NodeEndpoints tag staying absent.
    roundtrip!(FetchResponse, resp, 16);
}

#[test]
fn tagged_nullable_string_distinguishes_absent_from_null() {
    use odradek_protocol::messages::fetch_request::FetchRequest;
    // Absent, present-null, and present-value are three different wire
    // shapes; each must round-trip.
    for cluster_id in [None, Some(None), Some(Some("kRaft-cluster".to_owned()))] {
        let req = FetchRequest {
            cluster_id: cluster_id.clone(),
            ..Default::default()
        };
        roundtrip!(FetchRequest, req, 13);
    }
}

#[test]
fn tag_data_with_trailing_bytes_is_rejected() {
    // Tag 3 of ApiVersionsResponse is a bool (1 byte); a 2-byte payload
    // must error, not silently drop bytes.
    let resp = ApiVersionsResponse {
        unknown_tagged_fields: vec![RawTaggedField {
            tag: 3,
            data: Bytes::from_static(&[0x01, 0x00]),
        }],
        ..Default::default()
    };
    let bytes = encode(&resp, 3, |m, b, v| m.encode(b, v));
    assert!(ApiVersionsResponse::decode(&mut bytes.clone(), 3).is_err());
}

#[test]
fn request_header_v2_client_id_is_not_compact() {
    // KIP-482 quirk: RequestHeader v2 is flexible, but ClientId keeps the
    // classic i16-prefixed nullable string encoding.
    let header = RequestHeader {
        request_api_key: 18,
        request_api_version: 3,
        correlation_id: 7,
        client_id: Some("abc".into()),
        unknown_tagged_fields: Vec::new(),
    };
    let bytes = encode(&header, 2, |m, b, v| m.encode(b, v));
    let expected = [
        &[0x00, 0x12][..],         // api key 18
        &[0x00, 0x03],             // api version 3
        &[0x00, 0x00, 0x00, 0x07], // correlation id
        &[0x00, 0x03],             // classic i16 length prefix, NOT compact
        b"abc",
        &[0x00], // tagged fields
    ]
    .concat();
    assert_eq!(&bytes[..], &expected[..]);
    roundtrip!(RequestHeader, header, 2);
}

#[test]
fn response_header_versions() {
    let header = ResponseHeader {
        correlation_id: 42,
        unknown_tagged_fields: Vec::new(),
    };
    let v0 = encode(&header, 0, |m, b, v| m.encode(b, v));
    assert_eq!(&v0[..], &[0, 0, 0, 42]);
    let v1 = encode(&header, 1, |m, b, v| m.encode(b, v));
    assert_eq!(&v1[..], &[0, 0, 0, 42, 0]);
    roundtrip!(ResponseHeader, header.clone(), 0);
    roundtrip!(ResponseHeader, header, 1);
}

#[test]
fn metadata_request_null_topics_needs_v1() {
    let req = MetadataRequest {
        topics: None,
        ..Default::default()
    };
    // v0 does not allow a null topics array.
    let mut buf = BytesMut::new();
    assert!(matches!(
        req.encode(&mut buf, 0),
        Err(EncodeError::NullField("Topics"))
    ));
    // v1+ does.
    for version in [1, 9, 13] {
        roundtrip!(MetadataRequest, req.clone(), version);
    }
}

#[test]
fn metadata_request_roundtrip_flexible() {
    let req = MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            topic_id: [0u8; 16],
            name: Some("events".into()),
            unknown_tagged_fields: Vec::new(),
        }]),
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: true,
        unknown_tagged_fields: Vec::new(),
    };
    roundtrip!(MetadataRequest, req, 13);
}

#[test]
fn metadata_response_roundtrip_v12() {
    let resp = MetadataResponse {
        throttle_time_ms: 5,
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: "broker-1".into(),
            port: 9092,
            rack: None,
            unknown_tagged_fields: Vec::new(),
        }],
        cluster_id: Some("cluster-x".into()),
        controller_id: 1,
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some("events".into()),
            topic_id: *b"0123456789abcdef",
            is_internal: false,
            partitions: vec![MetadataResponsePartition {
                error_code: 0,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 4,
                replica_nodes: vec![1, 2, 3],
                isr_nodes: vec![1, 3],
                offline_replicas: vec![],
                unknown_tagged_fields: Vec::new(),
            }],
            // absent before v8 is fine; v12 carries it
            topic_authorized_operations: -2147483648,
            unknown_tagged_fields: Vec::new(),
        }],
        // only in v8-v10, so leave at default for a v12 round-trip
        ..Default::default()
    };
    roundtrip!(MetadataResponse, resp, 12);
}

#[test]
fn metadata_response_roundtrip_v1_non_flexible() {
    let resp = MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: "broker-1".into(),
            port: 9092,
            rack: Some("rack-a".into()),
            unknown_tagged_fields: Vec::new(),
        }],
        controller_id: 1,
        topics: vec![MetadataResponseTopic {
            error_code: 0,
            name: Some("events".into()),
            is_internal: false,
            partitions: vec![],
            ..Default::default()
        }],
        ..Default::default()
    };
    roundtrip!(MetadataResponse, resp, 1);
}

#[test]
fn fetch_request_roundtrip_min_and_flexible() {
    let base_partition = FetchPartition {
        partition: 3,
        fetch_offset: 1000,
        partition_max_bytes: 1 << 20,
        ..Default::default()
    };
    // v4: oldest supported, classic encoding, topic addressed by name.
    let v4 = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 50 << 20,
        isolation_level: 1,
        topics: vec![FetchTopic {
            topic: "events".into(),
            partitions: vec![base_partition.clone()],
            ..Default::default()
        }],
        ..Default::default()
    };
    roundtrip!(FetchRequest, v4, 4);

    // v16: flexible, topic addressed by id, session fields present.
    // replica_id is left at its default (-1): the field is not encoded at
    // v15+ (replaced by the tagged ReplicaState), so only the default can
    // round-trip.
    let v16 = FetchRequest {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 50 << 20,
        isolation_level: 0,
        session_id: 77,
        session_epoch: 2,
        topics: vec![FetchTopic {
            topic_id: *b"0123456789abcdef",
            partitions: vec![FetchPartition {
                current_leader_epoch: 9,
                last_fetched_epoch: 8,
                log_start_offset: 10,
                ..base_partition
            }],
            ..Default::default()
        }],
        rack_id: "rack-a".into(),
        ..Default::default()
    };
    roundtrip!(FetchRequest, v16, 16);
}

#[test]
fn truncated_input_errors_cleanly() {
    let resp = ApiVersionsResponse {
        api_keys: vec![ApiVersion {
            api_key: 0,
            min_version: 0,
            max_version: 9,
            ..Default::default()
        }],
        ..Default::default()
    };
    let bytes = encode(&resp, 3, |m, b, v| m.encode(b, v));
    // Every strict prefix must fail with an error, never panic.
    for cut in 0..bytes.len() {
        let mut prefix = bytes.slice(..cut);
        assert!(
            ApiVersionsResponse::decode(&mut prefix, 3).is_err(),
            "prefix of {cut} bytes unexpectedly decoded"
        );
    }
}
