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
    let mut req = ApiVersionsRequest::default();
    req.client_software_name = "odradek".into();
    req.client_software_version = "0.1.0".into();
    req.unknown_tagged_fields = Vec::new();
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
    let mut produce_key = ApiVersion::default();
    produce_key.api_key = 0;
    produce_key.min_version = 3;
    produce_key.max_version = 13;
    let mut api_versions_key = ApiVersion::default();
    api_versions_key.api_key = 18;
    api_versions_key.min_version = 0;
    api_versions_key.max_version = 4;
    let mut resp = ApiVersionsResponse::default();
    resp.error_code = 0;
    resp.api_keys = vec![produce_key, api_versions_key];
    resp.throttle_time_ms = 0;
    for version in ApiVersionsResponse::MIN_VERSION..=ApiVersionsResponse::MAX_VERSION {
        // throttle_time_ms only exists from v1; keep it default (0) so
        // equality holds across versions.
        roundtrip!(ApiVersionsResponse, resp.clone(), version);
    }
}

#[test]
fn unknown_tagged_fields_roundtrip() {
    let mut resp = ApiVersionsResponse::default();
    resp.unknown_tagged_fields = vec![RawTaggedField {
        tag: 99, // no known meaning; must survive raw
        data: Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
    }];
    // Flexible version keeps the raw tagged payload byte-for-byte.
    roundtrip!(ApiVersionsResponse, resp, 3);
}

#[test]
fn known_tagged_fields_materialize() {
    // A raw tag 1 payload (int64 -1) decodes into the materialized field,
    // not into unknown_tagged_fields.
    let mut carrier = ApiVersionsResponse::default();
    carrier.unknown_tagged_fields = vec![RawTaggedField {
        tag: 1,
        data: Bytes::from_static(&[0xff; 8]),
    }];
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
    let mut current_leader = LeaderIdAndEpoch::default();
    current_leader.leader_id = 2;
    current_leader.leader_epoch = 9;
    let mut partition = PartitionData::default();
    partition.partition_index = 3;
    partition.error_code = 6; // NOT_LEADER_OR_FOLLOWER
    partition.current_leader = Some(current_leader);
    partition.unknown_tagged_fields = vec![RawTaggedField {
        tag: 42,
        data: Bytes::from_static(b"future"),
    }];
    let mut topic_response = FetchableTopicResponse::default();
    topic_response.topic_id = [7u8; 16];
    topic_response.partitions = vec![partition];
    let mut resp = FetchResponse::default();
    resp.responses = vec![topic_response];
    // v16 also exercises the top-level NodeEndpoints tag staying absent.
    roundtrip!(FetchResponse, resp, 16);
}

#[test]
fn tagged_nullable_string_distinguishes_absent_from_null() {
    use odradek_protocol::messages::fetch_request::FetchRequest;
    // Absent, present-null, and present-value are three different wire
    // shapes; each must round-trip.
    for cluster_id in [None, Some(None), Some(Some("kRaft-cluster".to_owned()))] {
        let mut req = FetchRequest::default();
        req.cluster_id = cluster_id.clone();
        roundtrip!(FetchRequest, req, 13);
    }
}

#[test]
fn tag_data_with_trailing_bytes_is_rejected() {
    // Tag 3 of ApiVersionsResponse is a bool (1 byte); a 2-byte payload
    // must error, not silently drop bytes.
    let mut resp = ApiVersionsResponse::default();
    resp.unknown_tagged_fields = vec![RawTaggedField {
        tag: 3,
        data: Bytes::from_static(&[0x01, 0x00]),
    }];
    let bytes = encode(&resp, 3, |m, b, v| m.encode(b, v));
    assert!(ApiVersionsResponse::decode(&mut bytes.clone(), 3).is_err());
}

#[test]
fn request_header_v2_client_id_is_not_compact() {
    // KIP-482 quirk: RequestHeader v2 is flexible, but ClientId keeps the
    // classic i16-prefixed nullable string encoding.
    let mut header = RequestHeader::default();
    header.request_api_key = 18;
    header.request_api_version = 3;
    header.correlation_id = 7;
    header.client_id = Some("abc".into());
    header.unknown_tagged_fields = Vec::new();
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
    let mut header = ResponseHeader::default();
    header.correlation_id = 42;
    header.unknown_tagged_fields = Vec::new();
    let v0 = encode(&header, 0, |m, b, v| m.encode(b, v));
    assert_eq!(&v0[..], &[0, 0, 0, 42]);
    let v1 = encode(&header, 1, |m, b, v| m.encode(b, v));
    assert_eq!(&v1[..], &[0, 0, 0, 42, 0]);
    roundtrip!(ResponseHeader, header.clone(), 0);
    roundtrip!(ResponseHeader, header, 1);
}

#[test]
fn metadata_request_null_topics_needs_v1() {
    let mut req = MetadataRequest::default();
    req.topics = None;
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
    let mut topic = MetadataRequestTopic::default();
    topic.topic_id = [0u8; 16];
    topic.name = Some("events".into());
    topic.unknown_tagged_fields = Vec::new();
    let mut req = MetadataRequest::default();
    req.topics = Some(vec![topic]);
    req.allow_auto_topic_creation = false;
    req.include_cluster_authorized_operations = false;
    req.include_topic_authorized_operations = true;
    req.unknown_tagged_fields = Vec::new();
    roundtrip!(MetadataRequest, req, 13);
}

#[test]
fn metadata_response_roundtrip_v12() {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = "broker-1".into();
    broker.port = 9092;
    broker.rack = None;
    broker.unknown_tagged_fields = Vec::new();
    let mut partition = MetadataResponsePartition::default();
    partition.error_code = 0;
    partition.partition_index = 0;
    partition.leader_id = 1;
    partition.leader_epoch = 4;
    partition.replica_nodes = vec![1, 2, 3];
    partition.isr_nodes = vec![1, 3];
    partition.offline_replicas = vec![];
    partition.unknown_tagged_fields = Vec::new();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = 0;
    topic.name = Some("events".into());
    topic.topic_id = *b"0123456789abcdef";
    topic.is_internal = false;
    topic.partitions = vec![partition];
    // absent before v8 is fine; v12 carries it
    topic.topic_authorized_operations = -2147483648;
    topic.unknown_tagged_fields = Vec::new();
    // cluster_authorized_operations is only in v8-v10, so leave it at
    // default for a v12 round-trip.
    let mut resp = MetadataResponse::default();
    resp.throttle_time_ms = 5;
    resp.brokers = vec![broker];
    resp.cluster_id = Some("cluster-x".into());
    resp.controller_id = 1;
    resp.topics = vec![topic];
    roundtrip!(MetadataResponse, resp, 12);
}

#[test]
fn metadata_response_roundtrip_v1_non_flexible() {
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1;
    broker.host = "broker-1".into();
    broker.port = 9092;
    broker.rack = Some("rack-a".into());
    broker.unknown_tagged_fields = Vec::new();
    let mut topic = MetadataResponseTopic::default();
    topic.error_code = 0;
    topic.name = Some("events".into());
    topic.is_internal = false;
    topic.partitions = vec![];
    let mut resp = MetadataResponse::default();
    resp.brokers = vec![broker];
    resp.controller_id = 1;
    resp.topics = vec![topic];
    roundtrip!(MetadataResponse, resp, 1);
}

#[test]
fn fetch_request_roundtrip_min_and_flexible() {
    let mut base_partition = FetchPartition::default();
    base_partition.partition = 3;
    base_partition.fetch_offset = 1000;
    base_partition.partition_max_bytes = 1 << 20;
    // v4: oldest supported, classic encoding, topic addressed by name.
    let mut v4_topic = FetchTopic::default();
    v4_topic.topic = "events".into();
    v4_topic.partitions = vec![base_partition.clone()];
    let mut v4 = FetchRequest::default();
    v4.replica_id = -1;
    v4.max_wait_ms = 500;
    v4.min_bytes = 1;
    v4.max_bytes = 50 << 20;
    v4.isolation_level = 1;
    v4.topics = vec![v4_topic];
    roundtrip!(FetchRequest, v4, 4);

    // v16: flexible, topic addressed by id, session fields present.
    // replica_id is left at its default (-1): the field is not encoded at
    // v15+ (replaced by the tagged ReplicaState), so only the default can
    // round-trip.
    let mut v16_partition = base_partition;
    v16_partition.current_leader_epoch = 9;
    v16_partition.last_fetched_epoch = 8;
    v16_partition.log_start_offset = 10;
    let mut v16_topic = FetchTopic::default();
    v16_topic.topic_id = *b"0123456789abcdef";
    v16_topic.partitions = vec![v16_partition];
    let mut v16 = FetchRequest::default();
    v16.max_wait_ms = 500;
    v16.min_bytes = 1;
    v16.max_bytes = 50 << 20;
    v16.isolation_level = 0;
    v16.session_id = 77;
    v16.session_epoch = 2;
    v16.topics = vec![v16_topic];
    v16.rack_id = "rack-a".into();
    roundtrip!(FetchRequest, v16, 16);
}

#[test]
fn truncated_input_errors_cleanly() {
    let mut key = ApiVersion::default();
    key.api_key = 0;
    key.min_version = 0;
    key.max_version = 9;
    let mut resp = ApiVersionsResponse::default();
    resp.api_keys = vec![key];
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

/// The schemas carry fields whose nullableVersions is a strict subset of
/// their versions (e.g. MetadataRequest.Topics: on the wire from v0,
/// nullable only from v1; JoinGroupResponse.ProtocolName: v0+, nullable
/// from v7). The generated guards at those boundaries are load-bearing:
/// null must be an error below the nullable floor and legal at it.
#[test]
fn nullable_version_boundaries_are_enforced() {
    use odradek_protocol::DecodeError;
    use odradek_protocol::messages::join_group_response::JoinGroupResponse;

    // Encode: null Topics is an error at v0, the "all topics" wire form
    // at v1.
    let mut request = MetadataRequest::default();
    request.topics = None;
    let mut buf = BytesMut::new();
    assert!(matches!(
        request.encode(&mut buf, 0),
        Err(EncodeError::NullField("Topics"))
    ));
    buf.clear();
    request.encode(&mut buf, 1).unwrap();
    let mut bytes = buf.freeze();
    let decoded = MetadataRequest::decode(&mut bytes, 1).unwrap();
    assert!(decoded.topics.is_none());

    // Decode: a null Topics array in a v0 body is a wire violation.
    let mut null_topics_v0 = Bytes::copy_from_slice(&(-1i32).to_be_bytes());
    assert!(matches!(
        MetadataRequest::decode(&mut null_topics_v0, 0),
        Err(DecodeError::InvalidLength(-1))
    ));

    // JoinGroupResponse.ProtocolName: null is an error at v6, fine at v7.
    let mut response = JoinGroupResponse::default();
    response.protocol_name = None;
    let mut buf = BytesMut::new();
    assert!(matches!(
        response.encode(&mut buf, 6),
        Err(EncodeError::NullField("ProtocolName"))
    ));
    buf.clear();
    response.encode(&mut buf, 7).unwrap();
    let mut bytes = buf.freeze();
    let decoded = JoinGroupResponse::decode(&mut bytes, 7).unwrap();
    assert!(decoded.protocol_name.is_none());
}
