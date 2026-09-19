//! `AWS_MSK_IAM`: authenticate to Amazon MSK with AWS credentials.
//!
//! Not OAUTHBEARER and not a token. MSK defines its own mechanism whose
//! single client message is a JSON object carrying a
//! Signature Version 4 signature over a notional request to connect to
//! the broker. The broker checks the signature against IAM, which is
//! what makes this "IAM auth" rather than a password.
//!
//! ```text
//! client   {"version":"2020_10_22","host":...,"x-amz-signature":...}
//! server   {"version":"2020_10_22","request-id":"..."}
//! ```
//!
//! # What this does and does not do
//!
//! It **signs**, given credentials. It does not **resolve** them: no
//! environment scanning, no profile parsing, no instance metadata, no
//! STS. Credential resolution is a large surface with its own failure
//! modes and its own dependency, and a crate that guesses where your
//! keys live is a crate that will one day pick the wrong ones. Pass in
//! what you already hold — from the AWS SDK, from your own resolver,
//! from configuration.
//!
//! It also does not refresh. A signature is valid for
//! [`EXPIRY_SECONDS`]; a session token expires on AWS's schedule. Both
//! are the caller's clock to watch.
//!
//! # Verification status
//!
//! The SigV4 core here is checked against AWS's own published worked
//! example — the documented signing key for
//! `20120215/us-east-1/iam/aws4_request` — so the key derivation and
//! HMAC chain are anchored to something external.
//!
//! The MSK-specific assembly around it — the field names, the canonical
//! query string, the action — is built from the published mechanism
//! description and is **not** verified against a real MSK cluster,
//! because this project has none to test against. Treat it as
//! unproven against the service until somebody runs it there, and
//! please report what happens.

use hmac::{Mac, SimpleHmac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::SaslError;

/// The payload version MSK expects.
const VERSION: &str = "2020_10_22";
/// The IAM action a client is asking permission for.
const ACTION: &str = "kafka-cluster:Connect";
/// The SigV4 service name MSK signs under.
const SERVICE: &str = "kafka-cluster";
const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// How long a signature stays valid. MSK's documented maximum.
pub const EXPIRY_SECONDS: u32 = 900;

/// AWS credentials, as much of them as signing needs.
///
/// The secret is zeroized on drop. The session token is not a secret in
/// the same way — it travels in the payload in clear — but it is
/// credential material with a lifetime, so it goes the same way.
#[derive(Clone)]
pub struct AwsCredentials {
    access_key_id: String,
    secret_access_key: Zeroizing<String>,
    session_token: Option<Zeroizing<String>>,
}

impl AwsCredentials {
    /// Long-lived credentials.
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        AwsCredentials {
            access_key_id: access_key_id.into(),
            secret_access_key: Zeroizing::new(secret_access_key.into()),
            session_token: None,
        }
    }

    /// Temporary credentials, which also carry a session token.
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(Zeroizing::new(token.into()));
        self
    }
}

impl std::fmt::Debug for AwsCredentials {
    /// The access key id identifies the credential and is not secret;
    /// everything else here is.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// The client's one message: a signed request to connect to `host`.
///
/// `timestamp` is seconds since the epoch, passed in rather than read
/// from the clock so the result is reproducible and testable. Use the
/// current time in production; a signature more than
/// [`EXPIRY_SECONDS`] old is refused by the broker, and one from the
/// future is refused too.
pub fn msk_iam_token(
    credentials: &AwsCredentials,
    host: &str,
    region: &str,
    timestamp: u64,
    user_agent: &str,
) -> Result<Zeroizing<Vec<u8>>, SaslError> {
    let (amz_date, datestamp) = format_timestamps(timestamp);
    let scope = format!("{datestamp}/{region}/{SERVICE}/aws4_request");
    let credential = format!("{}/{scope}", credentials.access_key_id);

    // The canonical query string: every signed parameter, sorted by key,
    // each percent-encoded. Sorting is not cosmetic — the broker rebuilds
    // this string and compares signatures, so a different order is a
    // different signature.
    let mut params: Vec<(String, String)> = vec![
        ("Action".to_owned(), ACTION.to_owned()),
        ("X-Amz-Algorithm".to_owned(), ALGORITHM.to_owned()),
        ("X-Amz-Credential".to_owned(), credential.clone()),
        ("X-Amz-Date".to_owned(), amz_date.clone()),
        ("X-Amz-Expires".to_owned(), EXPIRY_SECONDS.to_string()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ];
    if let Some(token) = &credentials.session_token {
        params.push(("X-Amz-Security-Token".to_owned(), token.to_string()));
    }
    params.sort();
    let canonical_query = params
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    // SHA-256 of an empty body: this request has none, and SigV4 still
    // wants the hash of what is not there.
    let empty_hash = hex(&Sha256::digest([]));
    let canonical_request = format!(
        "GET\n/\n{canonical_query}\nhost:{}\n\nhost\n{empty_hash}",
        host.to_lowercase()
    );
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signing_key = signing_key(&credentials.secret_access_key, &datestamp, region, SERVICE);
    let signature = hex(&hmac(&signing_key, string_to_sign.as_bytes()));

    let mut fields: Vec<(&str, String)> = vec![
        ("version", VERSION.to_owned()),
        ("host", host.to_owned()),
        ("user-agent", user_agent.to_owned()),
        ("action", ACTION.to_owned()),
        ("x-amz-algorithm", ALGORITHM.to_owned()),
        ("x-amz-credential", credential),
        ("x-amz-date", amz_date),
        ("x-amz-expires", EXPIRY_SECONDS.to_string()),
        ("x-amz-signedheaders", "host".to_owned()),
        ("x-amz-signature", signature),
    ];
    if let Some(token) = &credentials.session_token {
        fields.push(("x-amz-security-token", token.to_string()));
    }
    let object: serde_json::Map<String, serde_json::Value> = fields
        .into_iter()
        .map(|(k, v)| (k.to_owned(), serde_json::Value::String(v)))
        .collect();
    let payload = serde_json::to_vec(&object).map_err(|_| SaslError::Malformed("msk payload"))?;
    Ok(Zeroizing::new(payload))
}

/// `kSigning` — the end of SigV4's key derivation chain.
///
/// Four chained HMACs, each keyed by the last. The point of the chain is
/// that the key which actually signs is scoped to one date, one region
/// and one service, so a signature stolen from one cannot be replayed
/// against another.
fn signing_key(secret: &str, datestamp: &str, region: &str, service: &str) -> [u8; 32] {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), datestamp.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac =
        <SimpleHmac<Sha256> as Mac>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// RFC 3986 unreserved characters pass; everything else is percent
/// encoded, including `/`. AWS is specific about this and a signature
/// computed over a differently-encoded string simply does not verify.
fn uri_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `(20260919T001500Z, 20260919)` from a unix timestamp.
///
/// Done by hand rather than with a date library: the two formats SigV4
/// wants are the only ones needed, and a dependency that can render
/// every calendar in the world is a lot to carry for this.
fn format_timestamps(timestamp: u64) -> (String, String) {
    let days = timestamp / 86_400;
    let seconds = timestamp % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let amz = format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    );
    (amz, format!("{year:04}{month:02}{day:02}"))
}

/// Howard Hinnant's `civil_from_days`: days since the epoch to a
/// calendar date, without a calendar library.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's own published worked example for the signing key
    /// (`20120215/us-east-1/iam/aws4_request`). This is the part of the
    /// mechanism that can be anchored to something external, and it is
    /// the part where a mistake produces a signature that simply never
    /// verifies.
    #[test]
    fn the_signing_key_matches_aws_published_example() {
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex(&key),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    /// `/` and `:` must be encoded — the credential and the action both
    /// contain them, and AWS's canonical form is specific.
    #[test]
    fn uri_encoding_escapes_what_aws_expects() {
        assert_eq!(
            uri_encode("kafka-cluster:Connect"),
            "kafka-cluster%3AConnect"
        );
        assert_eq!(
            uri_encode("AKID/20260919/us-east-1"),
            "AKID%2F20260919%2Fus-east-1"
        );
        assert_eq!(uri_encode("-._~azAZ09"), "-._~azAZ09");
    }

    #[test]
    fn timestamps_render_in_both_sigv4_formats() {
        // 2026-09-19T00:15:00Z
        assert_eq!(
            format_timestamps(1_789_776_900),
            ("20260919T001500Z".to_owned(), "20260919".to_owned())
        );
        // The epoch itself, as a boundary.
        assert_eq!(
            format_timestamps(0),
            ("19700101T000000Z".to_owned(), "19700101".to_owned())
        );
    }

    /// The payload carries every field the mechanism names, and the
    /// signature is stable for a fixed input — which is what makes a
    /// change to the canonical form visible in review.
    #[test]
    fn the_payload_is_complete_and_deterministic() {
        let creds = AwsCredentials::new("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");
        let first = msk_iam_token(
            &creds,
            "b-1.example.kafka.us-east-1.amazonaws.com",
            "us-east-1",
            1_789_776_900,
            "odradek/test",
        )
        .unwrap();
        let second = msk_iam_token(
            &creds,
            "b-1.example.kafka.us-east-1.amazonaws.com",
            "us-east-1",
            1_789_776_900,
            "odradek/test",
        )
        .unwrap();
        assert_eq!(&first[..], &second[..], "same inputs, same signature");

        let json: serde_json::Value = serde_json::from_slice(&first).unwrap();
        for field in [
            "version",
            "host",
            "user-agent",
            "action",
            "x-amz-algorithm",
            "x-amz-credential",
            "x-amz-date",
            "x-amz-expires",
            "x-amz-signedheaders",
            "x-amz-signature",
        ] {
            assert!(json.get(field).is_some(), "missing {field}");
        }
        assert_eq!(json["version"], VERSION);
        assert_eq!(json["action"], ACTION);
        // No session token was supplied, so none is claimed.
        assert!(json.get("x-amz-security-token").is_none());
    }

    /// Temporary credentials put the session token in both the signed
    /// query string and the payload. Signing it is what stops an
    /// attacker swapping in a different one.
    #[test]
    fn a_session_token_changes_the_signature() {
        let plain = AwsCredentials::new("AKIDEXAMPLE", "secret");
        let temporary = AwsCredentials::new("AKIDEXAMPLE", "secret").with_session_token("tok");
        let a = msk_iam_token(&plain, "h", "us-east-1", 1_789_776_900, "ua").unwrap();
        let b = msk_iam_token(&temporary, "h", "us-east-1", 1_789_776_900, "ua").unwrap();
        let a: serde_json::Value = serde_json::from_slice(&a).unwrap();
        let b: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(b["x-amz-security-token"], "tok");
        assert_ne!(a["x-amz-signature"], b["x-amz-signature"]);
    }

    /// The secret never reaches Debug, however an error is logged.
    ///
    /// The searched-for values are deliberately unlike any field name:
    /// the first version of this looked for "tok" and matched
    /// `session_token`, passing or failing on the struct's own labels
    /// rather than on its contents.
    #[test]
    fn debug_redacts_the_secret() {
        let creds =
            AwsCredentials::new("AKIDEXAMPLE", "sekrit-QQQ").with_session_token("sessvalue-ZZZ");
        let rendered = format!("{creds:?}");
        // The access key id names the credential and is not secret.
        assert!(rendered.contains("AKIDEXAMPLE"), "{rendered}");
        assert!(!rendered.contains("sekrit-QQQ"), "{rendered}");
        assert!(!rendered.contains("sessvalue-ZZZ"), "{rendered}");
    }
}
