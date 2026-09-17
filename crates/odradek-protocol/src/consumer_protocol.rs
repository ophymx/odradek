//! The classic consumer protocol's embedded payloads.
//!
//! Brokers treat the subscription and assignment bytes inside
//! JoinGroup/SyncGroup as opaque; between *clients* they are a wire
//! format of their own — an `i16` version prefix, then the
//! [`ConsumerProtocolSubscription`]/[`ConsumerProtocolAssignment`]
//! schema body, with two forward-compatibility rules: decode at the
//! highest version we know when the prefix is newer, and ignore
//! trailing bytes newer members append. Every classic-group member
//! implementation needs exactly this envelope, so it lives here.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::DecodeError;
use crate::messages::consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition};
use crate::messages::consumer_protocol_subscription::ConsumerProtocolSubscription;

/// Encode a subscription to `topics` at envelope version 0.
pub fn encode_subscription(topics: &[String]) -> Bytes {
    let body = ConsumerProtocolSubscription {
        topics: topics.to_vec(),
        ..ConsumerProtocolSubscription::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0);
    body.encode(&mut out, 0).expect("v0 subscription encodes");
    out.freeze()
}

/// Decode a subscription's topic list, tolerating newer envelopes.
pub fn decode_subscription(data: &[u8]) -> Result<Vec<String>, DecodeError> {
    let mut buf = Bytes::copy_from_slice(data);
    if buf.len() < 2 {
        return Err(DecodeError::Truncated {
            needed: 2 - buf.len(),
        });
    }
    let version = buf.get_i16().min(ConsumerProtocolSubscription::MAX_VERSION);
    // Newer members may append fields; the prefix decodes, the rest is
    // deliberately ignored.
    let sub = ConsumerProtocolSubscription::decode(&mut buf, version.max(0))?;
    Ok(sub.topics)
}

/// Encode an assignment at envelope version 0.
pub fn encode_assignment(partitions: &[(String, Vec<i32>)]) -> Bytes {
    let body = ConsumerProtocolAssignment {
        assigned_partitions: partitions
            .iter()
            .map(|(topic, parts)| TopicPartition {
                topic: topic.clone(),
                partitions: parts.clone(),
                ..TopicPartition::default()
            })
            .collect(),
        ..ConsumerProtocolAssignment::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0);
    body.encode(&mut out, 0).expect("v0 assignment encodes");
    out.freeze()
}

/// Decode an assignment, tolerating newer envelopes; empty bytes are an
/// empty assignment (what a member awaiting partitions receives).
pub fn decode_assignment(data: &[u8]) -> Result<Vec<(String, Vec<i32>)>, DecodeError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Bytes::copy_from_slice(data);
    if buf.len() < 2 {
        return Err(DecodeError::Truncated {
            needed: 2 - buf.len(),
        });
    }
    let version = buf.get_i16().min(ConsumerProtocolAssignment::MAX_VERSION);
    let assignment = ConsumerProtocolAssignment::decode(&mut buf, version.max(0))?;
    Ok(assignment
        .assigned_partitions
        .into_iter()
        .map(|tp| (tp.topic, tp.partitions))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_roundtrip() {
        let topics = vec!["alpha".to_owned(), "beta".to_owned()];
        assert_eq!(
            decode_subscription(&encode_subscription(&topics)).unwrap(),
            topics
        );

        let parts = vec![("alpha".to_owned(), vec![0, 2])];
        assert_eq!(
            decode_assignment(&encode_assignment(&parts)).unwrap(),
            parts
        );
        assert!(decode_assignment(b"").unwrap().is_empty());
    }

    #[test]
    fn newer_envelopes_decode_and_short_ones_error() {
        // A newer member's payload: a version prefix past our snapshot,
        // a body carrying every field we know (newer versions are
        // supersets), then fields we do not — which must be ignored.
        let body = ConsumerProtocolSubscription {
            topics: vec!["t".to_owned()],
            ..ConsumerProtocolSubscription::default()
        };
        let mut encoded = BytesMut::new();
        encoded.put_i16(ConsumerProtocolSubscription::MAX_VERSION + 9);
        body.encode(&mut encoded, ConsumerProtocolSubscription::MAX_VERSION)
            .unwrap();
        encoded.put_slice(b"future-fields");
        assert_eq!(decode_subscription(&encoded).unwrap(), vec!["t".to_owned()]);

        assert!(decode_subscription(b"\x00").is_err());
        assert!(decode_assignment(b"\x00").is_err());
    }
}
