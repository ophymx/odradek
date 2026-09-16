//! Request/response header version selection.
//!
//! Header versions are not negotiated on their own: they are derived from
//! the (api key, api version) of the request. Requests use header v2 when
//! the message version is flexible, else v1. Responses use header v1 when
//! flexible, else v0 — with one deliberate quirk: `ApiVersions` responses
//! always use header v0, so that a client which does not yet know whether
//! the broker understands flexible encodings can still parse the response
//! that tells it.

use crate::api_key::ApiKey;
use crate::messages;

/// The request header version for `api_version` of `api_key`, or `None`
/// when the api key has no generated message type in this crate.
pub fn request_header_version(api_key: i16, api_version: i16) -> Option<i16> {
    let flexible = messages::request_is_flexible(api_key, api_version)?;
    Some(if flexible { 2 } else { 1 })
}

/// The response header version for `api_version` of `api_key`, or `None`
/// when the api key has no generated message type in this crate.
pub fn response_header_version(api_key: i16, api_version: i16) -> Option<i16> {
    if api_key == ApiKey::ApiVersions.code() {
        return Some(0);
    }
    let flexible = messages::request_is_flexible(api_key, api_version)?;
    Some(if flexible { 1 } else { 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_quirk() {
        // Request header goes flexible at v3, response header never does.
        assert_eq!(request_header_version(18, 0), Some(1));
        assert_eq!(request_header_version(18, 3), Some(2));
        assert_eq!(response_header_version(18, 0), Some(0));
        assert_eq!(response_header_version(18, 3), Some(0));
    }

    #[test]
    fn fetch_header_versions() {
        // Fetch goes flexible at v12.
        assert_eq!(request_header_version(1, 11), Some(1));
        assert_eq!(request_header_version(1, 12), Some(2));
        assert_eq!(response_header_version(1, 11), Some(0));
        assert_eq!(response_header_version(1, 12), Some(1));
    }

    #[test]
    fn unknown_api_key() {
        assert_eq!(request_header_version(9999, 0), None);
    }
}
