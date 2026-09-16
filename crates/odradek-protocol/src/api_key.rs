//! The Kafka API key registry.
//!
//! Every request on the wire opens with an `i16` API key identifying the
//! message type. This module covers the client-facing surface; it will be
//! generated from the upstream message schemas once codegen lands, at which
//! point [`ApiKey::Unknown`] should only appear for keys newer than the
//! schema snapshot.

use crate::error::DecodeError;

macro_rules! api_keys {
    ($($name:ident = $value:literal),+ $(,)?) => {
        /// A Kafka request type identifier.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum ApiKey {
            $($name,)+
        }

        impl ApiKey {
            /// The wire value of this API key.
            pub const fn code(self) -> i16 {
                match self {
                    $(ApiKey::$name => $value,)+
                }
            }

            /// All API keys known to this crate.
            pub const ALL: &'static [ApiKey] = &[$(ApiKey::$name,)+];
        }

        impl TryFrom<i16> for ApiKey {
            type Error = DecodeError;

            fn try_from(value: i16) -> Result<Self, Self::Error> {
                match value {
                    $($value => Ok(ApiKey::$name),)+
                    other => Err(DecodeError::UnknownDiscriminant {
                        kind: "api key",
                        value: other.into(),
                    }),
                }
            }
        }
    };
}

api_keys! {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    TxnOffsetCommit = 28,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    OffsetDelete = 47,
    DescribeCluster = 60,
    ConsumerGroupHeartbeat = 68,
    ConsumerGroupDescribe = 69,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_roundtrip() {
        for &key in ApiKey::ALL {
            assert_eq!(ApiKey::try_from(key.code()), Ok(key));
        }
    }

    #[test]
    fn unknown_key_rejected() {
        assert!(matches!(
            ApiKey::try_from(i16::MAX),
            Err(DecodeError::UnknownDiscriminant { .. })
        ));
    }
}
