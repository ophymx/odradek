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
use hmac::{Mac, SimpleHmac};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use sha2::digest::core_api::BlockSizeUser;
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

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

    match sasl.mechanism {
        Mechanism::Plain => {
            let mut token = Vec::new();
            token.push(0);
            token.extend_from_slice(sasl.username.as_bytes());
            token.push(0);
            token.extend_from_slice(sasl.password.as_bytes());
            let resp = sasl_round(conn, ranges, token.into()).await?;
            check_auth(&resp)
        }
        Mechanism::ScramSha256 => {
            scram::<Sha256>(
                conn,
                ranges,
                sasl,
                &fresh_nonce()?,
                config.scram_max_iterations,
            )
            .await
        }
        Mechanism::ScramSha512 => {
            scram::<Sha512>(
                conn,
                ranges,
                sasl,
                &fresh_nonce()?,
                config.scram_max_iterations,
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

fn fresh_nonce() -> Result<String, ClientError> {
    let mut raw = [0u8; 18];
    getrandom::fill(&mut raw).map_err(|e| ClientError::Sasl(format!("nonce: {e}")))?;
    Ok(base64_encode(&raw))
}

fn base64_encode(raw: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(raw)
}

fn base64_decode(raw: &str) -> Result<Vec<u8>, ClientError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(raw)
        .map_err(|e| ClientError::Sasl(format!("bad base64 in scram message: {e}")))
}

async fn scram<D>(
    conn: &Connection,
    ranges: &ApiVersionRanges,
    sasl: &SaslConfig,
    nonce: &str,
    max_iterations: u32,
) -> Result<(), ClientError>
where
    D: Digest + BlockSizeUser + Clone + Send + Sync + 'static,
{
    let client_first_bare = format!("n={},r={nonce}", saslname(&sasl.username));
    let client_first = format!("n,,{client_first_bare}");

    let resp = sasl_round(conn, ranges, Bytes::from(client_first)).await?;
    check_auth(&resp)?;
    let server_first = std::str::from_utf8(&resp.auth_bytes)
        .map_err(|_| ClientError::Sasl("server-first message is not utf-8".into()))?
        .to_owned();
    let attrs = parse_scram(&server_first)?;
    let server_nonce = attrs.get('r')?;
    if !server_nonce.starts_with(nonce) {
        return Err(ClientError::Sasl(
            "server nonce does not extend the client nonce".into(),
        ));
    }
    let salt = base64_decode(attrs.get('s')?)?;
    let iterations: u32 = attrs
        .get('i')?
        .parse()
        .map_err(|_| ClientError::Sasl("bad iteration count".into()))?;
    check_iterations(iterations, max_iterations)?;

    // PBKDF2 is a synchronous CPU burn proportional to a number the
    // broker chose: run it on a blocking thread so it cannot stall this
    // task's runtime worker (a `current_thread` runtime has exactly one,
    // and no timeout can interrupt a synchronous call that already
    // holds it).
    let (client_final, server_signature) = {
        // Clone into the blocking task as a zeroizing copy too:
        // the derivation holds it for the whole PBKDF2 burn.
        let password = sasl.password.clone();
        let client_first_bare = client_first_bare.clone();
        let server_first = server_first.clone();
        let server_nonce = server_nonce.to_owned();
        tokio::task::spawn_blocking(move || {
            scram_client_final::<D>(
                &password,
                &salt,
                iterations,
                &client_first_bare,
                &server_first,
                &server_nonce,
            )
        })
        .await
        .map_err(|e| ClientError::Sasl(format!("key derivation task failed: {e}")))??
    };

    let resp = sasl_round(conn, ranges, Bytes::from(client_final)).await?;
    check_auth(&resp)?;
    let server_final = std::str::from_utf8(&resp.auth_bytes)
        .map_err(|_| ClientError::Sasl("server-final message is not utf-8".into()))?;
    let attrs = parse_scram(server_final)?;
    if let Ok(err) = attrs.get('e') {
        return Err(ClientError::Sasl(format!("server error: {err}")));
    }
    let verifier = base64_decode(attrs.get('v')?)?;
    // Constant time: a length-independent, early-exit-free comparison
    // denies an attacker a timing oracle on the expected signature.
    if verifier.ct_eq(&server_signature).unwrap_u8() != 1 {
        return Err(ClientError::Sasl(
            "server signature mismatch: the broker does not know this password".into(),
        ));
    }
    Ok(())
}

/// Bound the broker's iteration count from both sides.
///
/// Below [`MIN_SCRAM_ITERATIONS`] the derivation is a KDF downgrade
/// dressed as a handshake; above `max` it is a CPU-burn primitive. Both
/// are the peer misusing the protocol rather than an authentication
/// failure, so both are [`ClientError::ProtocolViolation`].
fn check_iterations(iterations: u32, max: u32) -> Result<(), ClientError> {
    if iterations < MIN_SCRAM_ITERATIONS {
        return Err(ClientError::ProtocolViolation(format!(
            "broker asked for {iterations} scram iterations, below the \
             RFC 7677 minimum of {MIN_SCRAM_ITERATIONS}; refusing to derive \
             (and hand over) a proof under a weakened key"
        )));
    }
    if iterations > max {
        return Err(ClientError::ProtocolViolation(format!(
            "broker asked for {iterations} scram iterations, above this \
             client's ceiling of {max} (ClientConfig::scram_max_iterations)"
        )));
    }
    Ok(())
}

/// The client-final-message and the expected server signature. Pure so
/// the RFC 7677 vector can drive it.
///
/// Synchronous and, for a large `iterations`, slow: callers on an async
/// task must run it under [`tokio::task::spawn_blocking`].
fn scram_client_final<D>(
    password: &str,
    salt: &[u8],
    iterations: u32,
    client_first_bare: &str,
    server_first: &str,
    server_nonce: &str,
) -> Result<(String, Vec<u8>), ClientError>
where
    D: Digest + BlockSizeUser + Clone + Sync,
{
    let salted = hi::<D>(password.as_bytes(), salt, iterations);
    let client_key = hmac::<D>(&salted, b"Client Key");
    let stored_key = D::digest(&client_key);
    let without_proof = format!("c={},r={server_nonce}", base64_encode(b"n,,"));
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_signature = hmac::<D>(&stored_key, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_signature.iter())
        .map(|(k, s)| k ^ s)
        .collect();
    let server_key = hmac::<D>(&salted, b"Server Key");
    let server_signature = hmac::<D>(&server_key, auth_message.as_bytes());
    Ok((
        format!("{without_proof},p={}", base64_encode(&proof)),
        server_signature,
    ))
}

/// `Hi()` from RFC 5802 is PBKDF2 with HMAC-D at one block width.
///
/// Cost is linear in `iterations`, which the broker chose; the caller is
/// responsible for having bounded it (see [`check_iterations`]) and for
/// keeping this off an async runtime worker.
fn hi<D>(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8>
where
    D: Digest + BlockSizeUser + Clone + Sync,
{
    let mut out = vec![0u8; <D as Digest>::output_size()];
    pbkdf2::pbkdf2::<SimpleHmac<D>>(password, salt, iterations, &mut out)
        .expect("output length matches the digest");
    out
}

fn hmac<D>(key: &[u8], data: &[u8]) -> Vec<u8>
where
    D: Digest + BlockSizeUser + Clone + Sync,
{
    let mut mac = <SimpleHmac<D> as Mac>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Escape `,` and `=` per RFC 5802 saslname.
fn saslname(name: &str) -> String {
    name.replace('=', "=3D").replace(',', "=2C")
}

struct ScramAttrs<'a>(Vec<(char, &'a str)>);

impl<'a> ScramAttrs<'a> {
    fn get(&self, key: char) -> Result<&'a str, ClientError> {
        self.0
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| *v)
            .ok_or_else(|| ClientError::Sasl(format!("scram message lacks attribute {key}")))
    }
}

fn parse_scram(message: &str) -> Result<ScramAttrs<'_>, ClientError> {
    let mut attrs = Vec::new();
    for part in message.split(',') {
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| ClientError::Sasl(format!("bad scram attribute {part:?}")))?;
        let mut chars = key.chars();
        match (chars.next(), chars.next()) {
            (Some(k), None) => attrs.push((k, value)),
            _ => return Err(ClientError::Sasl(format!("bad scram attribute {part:?}"))),
        }
    }
    Ok(ScramAttrs(attrs))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677's SCRAM-SHA-256 example: user "user", password
    /// "pencil", known nonces and salt — the whole exchange is fixed.
    #[test]
    fn scram_sha256_matches_rfc_7677() {
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let client_first_bare = format!("n=user,r={client_nonce}");
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let attrs = parse_scram(server_first).unwrap();
        let server_nonce = attrs.get('r').unwrap();
        let salt = base64_decode(attrs.get('s').unwrap()).unwrap();
        let iterations: u32 = attrs.get('i').unwrap().parse().unwrap();

        let (client_final, server_signature) = scram_client_final::<Sha256>(
            "pencil",
            &salt,
            iterations,
            &client_first_bare,
            server_first,
            server_nonce,
        )
        .unwrap();

        assert_eq!(
            client_final,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        assert_eq!(
            base64_encode(&server_signature),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn plain_token_shape() {
        // authzid \0 user \0 password
        let sasl = SaslConfig {
            mechanism: Mechanism::Plain,
            username: "alice".into(),
            password: Zeroizing::new("secret".to_owned()),
        };
        assert!(format!("{sasl:?}").contains("<redacted>"));
        assert!(!format!("{sasl:?}").contains("secret"));
    }

    #[test]
    fn saslname_escapes() {
        assert_eq!(saslname("a=b,c"), "a=3Db=2Cc");
    }

    #[test]
    fn iteration_count_below_the_rfc_floor_is_rejected() {
        // i=0 and i=1 are the downgrade: pbkdf2 with zero or one round
        // is a single HMAC, and the proof derived from it is cheap to
        // attack offline once it has been sent.
        for iterations in [0, 1, 1000, MIN_SCRAM_ITERATIONS - 1] {
            assert!(
                matches!(
                    check_iterations(iterations, DEFAULT_MAX_SCRAM_ITERATIONS),
                    Err(ClientError::ProtocolViolation(_))
                ),
                "i={iterations} should have been rejected"
            );
        }
    }

    #[test]
    fn iteration_count_above_the_ceiling_is_rejected() {
        for iterations in [DEFAULT_MAX_SCRAM_ITERATIONS + 1, 1 << 31, u32::MAX] {
            assert!(
                matches!(
                    check_iterations(iterations, DEFAULT_MAX_SCRAM_ITERATIONS),
                    Err(ClientError::ProtocolViolation(_))
                ),
                "i={iterations} should have been rejected"
            );
        }
        // The ceiling is the caller's to set.
        assert!(check_iterations(1 << 20, 1 << 21).is_ok());
        assert!(check_iterations(1 << 20, 1 << 19).is_err());
    }

    #[test]
    fn in_range_iteration_counts_are_accepted() {
        for iterations in [
            MIN_SCRAM_ITERATIONS,
            8192,
            DEFAULT_MAX_SCRAM_ITERATIONS - 1,
            DEFAULT_MAX_SCRAM_ITERATIONS,
        ] {
            check_iterations(iterations, DEFAULT_MAX_SCRAM_ITERATIONS).unwrap();
        }
    }

    /// The bounds must not disturb the RFC 7677 exchange itself: its
    /// i=4096 sits exactly on the floor and still derives the documented
    /// proof.
    #[test]
    fn rfc_7677_iteration_count_survives_the_bounds() {
        let iterations = 4096;
        check_iterations(iterations, DEFAULT_MAX_SCRAM_ITERATIONS).unwrap();
        let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let (client_final, _) = scram_client_final::<Sha256>(
            "pencil",
            &base64_decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap(),
            iterations,
            "n=user,r=rOprNGfwEbeRWgbNEkqO",
            server_first,
            "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
        )
        .unwrap();
        assert!(client_final.ends_with("p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="));
    }

    /// The default must be safe: PLAIN is only allowed to reach an
    /// unencrypted socket when the caller opted in.
    #[test]
    fn plaintext_credential_policy() {
        let config = ClientConfig::default();
        assert!(!config.allow_plaintext_credentials);
        assert!(Mechanism::Plain.sends_password());
        assert!(!Mechanism::ScramSha256.sends_password());
        assert!(!Mechanism::ScramSha512.sends_password());
    }

    #[test]
    fn default_scram_ceiling_is_configured() {
        assert_eq!(
            ClientConfig::default().scram_max_iterations,
            DEFAULT_MAX_SCRAM_ITERATIONS
        );
        const { assert!(DEFAULT_MAX_SCRAM_ITERATIONS > MIN_SCRAM_ITERATIONS) };
    }
}
