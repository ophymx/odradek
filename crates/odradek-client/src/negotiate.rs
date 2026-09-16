//! ApiVersions negotiation.
//!
//! The first exchange on any connection: ask the broker which API versions
//! it supports, then use the intersection with what this client supports.
//!
//! The bootstrap problem is built into the protocol: we want to send the
//! newest ApiVersions request, but we don't yet know whether the broker
//! understands it. The protocol's answer: a broker that receives an
//! ApiVersions request too new for it responds with `UNSUPPORTED_VERSION`
//! encoded at version 0, including the range it does support — and the
//! ApiVersions response header is always v0. So: send the newest, and on
//! `UNSUPPORTED_VERSION` re-send at the broker's advertised maximum.

use std::collections::HashMap;

use bytes::BytesMut;
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::api_versions_request::ApiVersionsRequest;
use odradek_protocol::messages::api_versions_response::ApiVersionsResponse;

use crate::conn::Connection;
use crate::error::ClientError;

/// The version ranges a broker advertised, one entry per api key.
#[derive(Debug, Clone, Default)]
pub struct ApiVersionRanges {
    ranges: HashMap<i16, (i16, i16)>,
}

impl ApiVersionRanges {
    fn from_response(resp: &ApiVersionsResponse) -> ApiVersionRanges {
        ApiVersionRanges {
            ranges: resp
                .api_keys
                .iter()
                .map(|v| (v.api_key, (v.min_version, v.max_version)))
                .collect(),
        }
    }

    /// The broker's advertised range for `api_key`.
    pub fn range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.ranges.get(&api_key).copied()
    }

    /// The newest version of `api_key` inside both the broker's advertised
    /// range and `ours`.
    pub fn pick(&self, api_key: i16, ours: (i16, i16)) -> Result<i16, ClientError> {
        let (their_min, their_max) = self
            .range(api_key)
            .ok_or(ClientError::NoCommonVersion(api_key))?;
        let version = ours.1.min(their_max);
        if version >= ours.0.max(their_min) {
            Ok(version)
        } else {
            Err(ClientError::NoCommonVersion(api_key))
        }
    }
}

impl Connection {
    /// Exchange ApiVersions with the broker and return its advertised
    /// version ranges.
    pub async fn negotiate(&self) -> Result<ApiVersionRanges, ClientError> {
        let request = ApiVersionsRequest {
            client_software_name: "odradek".into(),
            client_software_version: env!("CARGO_PKG_VERSION").into(),
            unknown_tagged_fields: Vec::new(),
        };

        let resp = self
            .api_versions_at(&request, ApiVersionsRequest::MAX_VERSION)
            .await?;
        let code = ErrorCode(resp.error_code);
        if code.is_ok() {
            return Ok(ApiVersionRanges::from_response(&resp));
        }
        if code != ErrorCode::UNSUPPORTED_VERSION {
            return Err(ClientError::Broker(code));
        }

        // The error response tells us what the broker does support.
        let ranges = ApiVersionRanges::from_response(&resp);
        let version = ranges.pick(
            ApiVersionsRequest::API_KEY,
            (
                ApiVersionsRequest::MIN_VERSION,
                ApiVersionsRequest::MAX_VERSION,
            ),
        )?;
        let resp = self.api_versions_at(&request, version).await?;
        let code = ErrorCode(resp.error_code);
        if !code.is_ok() {
            return Err(ClientError::Broker(code));
        }
        Ok(ApiVersionRanges::from_response(&resp))
    }

    async fn api_versions_at(
        &self,
        request: &ApiVersionsRequest,
        version: i16,
    ) -> Result<ApiVersionsResponse, ClientError> {
        let mut body = BytesMut::new();
        request.encode(&mut body, version)?;
        let mut resp_body = self
            .request(ApiVersionsRequest::API_KEY, version, &body)
            .await?;
        // An UNSUPPORTED_VERSION error body arrives encoded at version 0
        // regardless of what we asked for; fall back when the requested
        // version fails to parse.
        let full = resp_body.clone();
        match ApiVersionsResponse::decode(&mut resp_body, version) {
            Ok(resp) if resp_body.is_empty() => Ok(resp),
            _ => Ok(ApiVersionsResponse::decode(&mut full.clone(), 0)?),
        }
    }
}
