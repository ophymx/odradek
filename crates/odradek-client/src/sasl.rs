//! SASL authentication: PLAIN and SCRAM-SHA-256/512.
//!
//! The Kafka flow (SaslHandshake v1 + SaslAuthenticate) runs right
//! after connect, before any other API: the broker names the accepted
//! mechanisms, then each mechanism token rides in an ordinary
//! SaslAuthenticate request/response pair.
//!
//! SCRAM is implemented per RFC 5802 (verified against RFC 7677's
//! SCRAM-SHA-256 example exchange in the tests): client nonce,
//! salted-password via Hi (PBKDF2 with HMAC), client proof, and —
//! importantly — verification of the *server's* signature, so a broker
//! that doesn't know the password fails the handshake too.
//!
//! # Transport security
//!
//! SASL authenticates; it does not encrypt. What each mechanism exposes
//! when the underlying connection is plaintext TCP:
//!
//! - **PLAIN** sends the password itself. This client refuses to do that
//!   on an unencrypted connection — the attempt fails with
//!   [`ClientError::InsecureCredentials`] before the handshake begins —
//!   unless [`crate::ClientConfig::allow_plaintext_credentials`] is set.
//! - **SCRAM** never sends the password, so it is permitted over
//!   plaintext, but a passive observer still collects the username, the
//!   salt, the iteration count, the nonces, and the client proof. Those
//!   are exactly the inputs to an offline dictionary attack against the
//!   password, at the cost the iteration count sets. An active on-path
//!   attacker can additionally impersonate the broker up to the point of
//!   the server-signature check, harvesting one proof per connection.
//!
//! In other words: SCRAM over plaintext protects the password from
//! being *read*, not from being *cracked*. Configure TLS
//! (`ClientConfig::tls`) for anything beyond a loopback broker.
//!
//! # Broker-controlled work
//!
//! The broker picks the SCRAM iteration count, and the client must spend
//! it before it can decide whether the broker is even genuine. Both
//! directions are bounded here: counts below [`MIN_SCRAM_ITERATIONS`]
//! (RFC 7677's floor) are rejected because they downgrade the KDF toward
//! a single HMAC, counts above
//! [`crate::ClientConfig::scram_max_iterations`] are rejected because
//! they are a CPU-burn primitive, and the derivation itself runs on
//! [`tokio::task::spawn_blocking`] so an in-bounds but expensive count
//! cannot occupy an async runtime worker.

use bytes::{Bytes, BytesMut};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;

use crate::ClientConfig;
/// Re-exported because it names the type of a public field:
/// [`SaslConfig::password`] is a `Zeroizing<String>`, so a caller has to
/// be able to construct one without taking a direct dependency.
pub use zeroize::Zeroizing;

use crate::conn;
use crate::conn::Connection;
use crate::error::ClientError;
use crate::negotiate::ApiVersionRanges;

/// The smallest SCRAM iteration count this client will honour: RFC 7677
/// §4 makes 4096 the minimum for SCRAM-SHA-256, and Kafka's own default
/// is exactly that.
///
/// A broker that asks for less is either broken or hostile. The hostile
/// case matters: `pbkdf2` with one round (or zero, which produces
/// bit-identical output) collapses `Hi()` to a single HMAC, and the
/// client emits a valid client proof over that weak key *before* it gets
/// to check the server signature. The proof is on the wire either way,
/// so the only defence is to never compute it.
pub const MIN_SCRAM_ITERATIONS: u32 = 4096;

/// Default ceiling on the SCRAM iteration count; see
/// [`crate::ClientConfig::scram_max_iterations`].
pub const DEFAULT_MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

/// Credentials plus mechanism. `Debug` never prints the password.
///
/// The password is wrapped in [`Zeroizing`], so dropping this config —
/// or any clone of it — overwrites the bytes instead of returning them
/// to the allocator intact. That narrows how long a credential is
/// readable in a core dump, a swapped page, or a later heap allocation
/// that happens to land on the same memory.
///
/// Two honest limits. It cannot reach copies made before the value got
/// here: whatever produced the `String` (an environment variable, a
/// config parse, a `read_to_string`) may have left its own, and only
/// the caller can clear those. And a `String` that reallocated while
/// being built leaves the old buffer behind. Treat this as shortening
/// the window, not closing it.
///
/// Build with [`SaslConfig::new`]: the type is `#[non_exhaustive]`
/// because SASL is where a mechanism gets added, and the mechanism this
/// client does not yet speak — OAUTHBEARER — authenticates with a token
/// rather than a username and password. Carrying one means a new field,
/// and a new field must not be a major version for everyone.
///
/// There is no `Default`, deliberately: it would have to pick a
/// mechanism, and the only mechanism with an obvious claim to being
/// first alphabetically is the one that puts the password on the wire.
#[derive(Clone)]
#[non_exhaustive]
pub struct SaslConfig {
    pub mechanism: Mechanism,
    pub username: String,
    pub password: Zeroizing<String>,
}

impl SaslConfig {
    /// Credentials for `mechanism`. The password is wrapped for you;
    /// see the [type docs](SaslConfig) on what zeroizing does and does
    /// not cover.
    pub fn new(
        mechanism: Mechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> SaslConfig {
        SaslConfig {
            mechanism,
            username: username.into(),
            password: Zeroizing::new(password.into()),
        }
    }
}

impl std::fmt::Debug for SaslConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslConfig")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl Mechanism {
    fn name(self) -> &'static str {
        match self {
            Mechanism::Plain => "PLAIN",
            Mechanism::ScramSha256 => "SCRAM-SHA-256",
            Mechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// True when the mechanism puts the password itself on the wire, so
    /// the transport has to be encrypted for it to be safe at all.
    fn sends_password(self) -> bool {
        match self {
            Mechanism::Plain => true,
            // SCRAM sends a proof, never the password.
            Mechanism::ScramSha256 | Mechanism::ScramSha512 => false,
        }
    }
}

/// Authenticate `conn` with `config.sasl`; must run before any
/// non-handshake request. A config with no credentials is a no-op.
///
/// Fails with [`ClientError::InsecureCredentials`], before sending
/// anything, when the mechanism would put the password on an
/// unencrypted connection. See the [module docs](self) for what the
/// mechanisms expose and how broker-controlled work is bounded.
pub async fn authenticate(
    conn: &Connection,
    ranges: &ApiVersionRanges,
    config: &ClientConfig,
) -> Result<(), ClientError> {
    let Some(sasl) = &config.sasl else {
        return Ok(());
    };
    // Decide before the handshake: a mechanism that transmits the
    // password must not even be announced on a connection that cannot
    // protect it.
    if sasl.mechanism.sends_password()
        && !conn.is_encrypted()
        && !config.allow_plaintext_credentials
    {
        return Err(ClientError::InsecureCredentials {
            mechanism: sasl.mechanism.name(),
        });
    }

    // Handshake v1: v0 predates SaslAuthenticate framing.
    let version = ranges.pick(SaslHandshakeRequest::API_KEY, (1, 1))?;
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = sasl.mechanism.name().to_owned();
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = conn
        .request(SaslHandshakeRequest::API_KEY, version, &body)
        .await?;
    let resp = conn::decode_body::<SaslHandshakeResponse>(&mut resp, version)?;
    let code = ErrorCode(resp.error_code);
    if !code.is_ok() {
        return Err(ClientError::Sasl(format!(
            "broker rejected mechanism {} ({code}); it offers: {}",
            sasl.mechanism.name(),
            resp.mechanisms.join(", ")
        )));
    }

    let limits = odradek_sasl::Limits::new(MIN_SCRAM_ITERATIONS, config.scram_max_iterations);
    match sasl.mechanism {
        Mechanism::Plain => {
            let token =
                odradek_sasl::plain_token(&sasl.username, &sasl.password).map_err(scram_error)?;
            let resp = sasl_round(conn, ranges, Bytes::copy_from_slice(&token)).await?;
            check_auth(&resp)
        }
        Mechanism::ScramSha256 => {
            scram(
                conn,
                ranges,
                sasl,
                odradek_sasl::Mechanism::ScramSha256,
                limits,
            )
            .await
        }
        Mechanism::ScramSha512 => {
            scram(
                conn,
                ranges,
                sasl,
                odradek_sasl::Mechanism::ScramSha512,
                limits,
            )
            .await
        }
    }
}

/// One SaslAuthenticate request/response.
async fn sasl_round(
    conn: &Connection,
    ranges: &ApiVersionRanges,
    token: Bytes,
) -> Result<SaslAuthenticateResponse, ClientError> {
    let version = ranges.pick(
        SaslAuthenticateRequest::API_KEY,
        (
            SaslAuthenticateRequest::MIN_VERSION,
            SaslAuthenticateRequest::MAX_VERSION,
        ),
    )?;
    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = token;
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = conn
        .request(SaslAuthenticateRequest::API_KEY, version, &body)
        .await?;
    conn::decode_body::<SaslAuthenticateResponse>(&mut resp, version)
}

fn check_auth(resp: &SaslAuthenticateResponse) -> Result<(), ClientError> {
    let code = ErrorCode(resp.error_code);
    if code.is_ok() {
        Ok(())
    } else {
        Err(ClientError::Sasl(format!(
            "{code}{}",
            resp.error_message
                .as_deref()
                .map(|m| format!(": {m}"))
                .unwrap_or_default()
        )))
    }
}

/// Drive a SCRAM exchange over `conn`.
///
/// The protocol itself lives in [`odradek_sasl`], which speaks both
/// roles and is checked against the RFC 7677 vectors. What is here is
/// the part that is Kafka's: carrying the three messages inside
/// SaslAuthenticate requests, and keeping the key derivation off the
/// async runtime.
async fn scram(
    conn: &Connection,
    ranges: &ApiVersionRanges,
    sasl: &SaslConfig,
    mechanism: odradek_sasl::Mechanism,
    limits: odradek_sasl::Limits,
) -> Result<(), ClientError> {
    let mut client =
        odradek_sasl::ScramClient::new(mechanism, &sasl.username, &sasl.password, limits)
            .map_err(|e| ClientError::Sasl(e.to_string()))?;

    let resp = sasl_round(conn, ranges, Bytes::from(client.client_first())).await?;
    check_auth(&resp)?;
    let server_first = std::str::from_utf8(&resp.auth_bytes)
        .map_err(|_| ClientError::Sasl("server-first message is not utf-8".into()))?
        .to_owned();

    // PBKDF2 is a synchronous CPU burn proportional to a number the
    // broker chose: run it on a blocking thread so it cannot stall this
    // task's runtime worker (a `current_thread` runtime has exactly one,
    // and no timeout can interrupt a synchronous call that already
    // holds it). The client is moved in and back out because it carries
    // the derived keys the final verification needs.
    let (client, client_final) = tokio::task::spawn_blocking(move || {
        let mut client = client;
        let message = client.client_final(&server_first);
        (client, message)
    })
    .await
    .map_err(|e| ClientError::Sasl(format!("key derivation task failed: {e}")))?;
    let client_final = client_final.map_err(scram_error)?;

    let resp = sasl_round(conn, ranges, Bytes::from(client_final)).await?;
    check_auth(&resp)?;
    let server_final = std::str::from_utf8(&resp.auth_bytes)
        .map_err(|_| ClientError::Sasl("server-final message is not utf-8".into()))?;
    client
        .verify_server_final(server_final)
        .map_err(scram_error)
}

/// Carry a SASL error across into this crate's taxonomy.
///
/// The iteration bounds are the peer misusing the protocol rather than
/// an authentication failure, and the distinction is the one a caller
/// acts on: a `ProtocolViolation` is a broker to stop talking to, a
/// `Sasl` error is a credential to fix.
fn scram_error(error: odradek_sasl::SaslError) -> ClientError {
    match error {
        odradek_sasl::SaslError::IterationsTooLow { .. }
        | odradek_sasl::SaslError::IterationsTooHigh { .. }
        | odradek_sasl::SaslError::NonceNotExtended => {
            ClientError::ProtocolViolation(error.to_string())
        }
        other => ClientError::Sasl(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exchange itself is tested where it lives — `odradek-sasl`
    /// checks both roles against the RFC 7677 vectors. What is this
    /// crate's to get right is the *decision* not to start one: a
    /// mechanism that puts the password on the wire must not be
    /// attempted over a connection that cannot protect it, and that is
    /// a property of the client's configuration rather than of SCRAM.
    #[test]
    fn only_plain_is_refused_on_an_unencrypted_connection() {
        assert!(Mechanism::Plain.sends_password());
        assert!(!Mechanism::ScramSha256.sends_password());
        assert!(!Mechanism::ScramSha512.sends_password());
    }

    /// The mechanism names are wire values; a typo here is a handshake
    /// a broker answers with UNSUPPORTED_SASL_MECHANISM.
    #[test]
    fn mechanism_names_are_the_wire_names() {
        assert_eq!(Mechanism::Plain.name(), "PLAIN");
        assert_eq!(Mechanism::ScramSha256.name(), "SCRAM-SHA-256");
        assert_eq!(Mechanism::ScramSha512.name(), "SCRAM-SHA-512");
    }

    /// The password never reaches `Debug`, however a config is logged.
    #[test]
    fn debug_redacts_the_password() {
        let config = SaslConfig::new(Mechanism::ScramSha256, "admin", "hunter2");
        let rendered = format!("{config:?}");
        assert!(rendered.contains("admin"), "{rendered}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
    }
}
