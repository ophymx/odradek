//! Kafka protocol error codes.
//!
//! Unlike [`crate::ApiKey`], error codes arrive in responses from
//! implementations that may be newer than this crate, so unknown values must
//! be representable: `ErrorCode` is a transparent wrapper over the wire
//! `i16`, with named constants for the well-known codes.

/// An error code carried in a Kafka response.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(pub i16);

macro_rules! error_codes {
    ($($(#[$doc:meta])* $name:ident = $value:literal),+ $(,)?) => {
        impl ErrorCode {
            $($(#[$doc])* pub const $name: ErrorCode = ErrorCode($value);)+

            /// The upstream name of this code, if known.
            pub const fn name(self) -> Option<&'static str> {
                match self.0 {
                    $($value => Some(stringify!($name)),)+
                    _ => None,
                }
            }
        }
    };
}

error_codes! {
    UNKNOWN_SERVER_ERROR = -1,
    NONE = 0,
    OFFSET_OUT_OF_RANGE = 1,
    CORRUPT_MESSAGE = 2,
    UNKNOWN_TOPIC_OR_PARTITION = 3,
    INVALID_FETCH_SIZE = 4,
    LEADER_NOT_AVAILABLE = 5,
    NOT_LEADER_OR_FOLLOWER = 6,
    REQUEST_TIMED_OUT = 7,
    BROKER_NOT_AVAILABLE = 8,
    REPLICA_NOT_AVAILABLE = 9,
    MESSAGE_TOO_LARGE = 10,
    NETWORK_EXCEPTION = 13,
    COORDINATOR_LOAD_IN_PROGRESS = 14,
    COORDINATOR_NOT_AVAILABLE = 15,
    NOT_COORDINATOR = 16,
    INVALID_TOPIC_EXCEPTION = 17,
    RECORD_LIST_TOO_LARGE = 18,
    NOT_ENOUGH_REPLICAS = 19,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    INVALID_REQUIRED_ACKS = 21,
    ILLEGAL_GENERATION = 22,
    INCONSISTENT_GROUP_PROTOCOL = 23,
    INVALID_GROUP_ID = 24,
    UNKNOWN_MEMBER_ID = 25,
    INVALID_SESSION_TIMEOUT = 26,
    REBALANCE_IN_PROGRESS = 27,
    TOPIC_AUTHORIZATION_FAILED = 29,
    GROUP_AUTHORIZATION_FAILED = 30,
    CLUSTER_AUTHORIZATION_FAILED = 31,
    UNSUPPORTED_SASL_MECHANISM = 33,
    UNSUPPORTED_VERSION = 35,
    TOPIC_ALREADY_EXISTS = 36,
    INVALID_PARTITIONS = 37,
    INVALID_REPLICATION_FACTOR = 38,
    INVALID_REQUEST = 42,
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43,
    POLICY_VIOLATION = 44,
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45,
    DUPLICATE_SEQUENCE_NUMBER = 46,
    INVALID_PRODUCER_EPOCH = 47,
    INVALID_TXN_STATE = 48,
    CONCURRENT_TRANSACTIONS = 51,
    KAFKA_STORAGE_ERROR = 56,
    SASL_AUTHENTICATION_FAILED = 58,
    UNSUPPORTED_COMPRESSION_TYPE = 76,
    MEMBER_ID_REQUIRED = 79,
    GROUP_MAX_SIZE_REACHED = 81,
    UNKNOWN_TOPIC_ID = 100,
    FENCED_MEMBER_EPOCH = 110,
    UNRELEASED_INSTANCE_ID = 111,
    UNSUPPORTED_ASSIGNOR = 112,
}

impl ErrorCode {
    /// True when this code signals success.
    pub const fn is_ok(self) -> bool {
        self.0 == 0
    }
}

impl From<i16> for ErrorCode {
    fn from(value: i16) -> Self {
        ErrorCode(value)
    }
}

impl std::fmt::Debug for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => write!(f, "ErrorCode({} {name})", self.0),
            None => write!(f, "ErrorCode({})", self.0),
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{name} ({})", self.0),
            None => write!(f, "unknown error code {}", self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_display() {
        assert_eq!(ErrorCode::NONE.name(), Some("NONE"));
        assert!(ErrorCode::NONE.is_ok());
        assert_eq!(ErrorCode::UNSUPPORTED_VERSION.0, 35);
        assert_eq!(ErrorCode(9999).name(), None);
        assert_eq!(
            format!("{}", ErrorCode::UNSUPPORTED_VERSION),
            "UNSUPPORTED_VERSION (35)"
        );
    }
}
