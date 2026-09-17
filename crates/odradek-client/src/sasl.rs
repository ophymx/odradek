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

use bytes::{Bytes, BytesMut};
use hmac::{Mac, SimpleHmac};
use odradek_protocol::ErrorCode;
use odradek_protocol::messages::sasl_authenticate_request::SaslAuthenticateRequest;
use odradek_protocol::messages::sasl_authenticate_response::SaslAuthenticateResponse;
use odradek_protocol::messages::sasl_handshake_request::SaslHandshakeRequest;
use odradek_protocol::messages::sasl_handshake_response::SaslHandshakeResponse;
use sha2::digest::core_api::BlockSizeUser;
use sha2::{Digest, Sha256, Sha512};

use crate::conn::Connection;
use crate::error::ClientError;
use crate::negotiate::ApiVersionRanges;

/// Credentials plus mechanism. `Debug` never prints the password.
#[derive(Clone)]
pub struct SaslConfig {
    pub mechanism: Mechanism,
    pub username: String,
    pub password: String,
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
}

/// Authenticate `conn`; must run before any non-handshake request.
pub async fn authenticate(
    conn: &Connection,
    ranges: &ApiVersionRanges,
    sasl: &SaslConfig,
) -> Result<(), ClientError> {
    // Handshake v1: v0 predates SaslAuthenticate framing.
    let version = ranges.pick(SaslHandshakeRequest::API_KEY, (1, 1))?;
    let request = SaslHandshakeRequest {
        mechanism: sasl.mechanism.name().to_owned(),
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = conn
        .request(SaslHandshakeRequest::API_KEY, version, &body)
        .await?;
    let resp = SaslHandshakeResponse::decode(&mut resp, version)?;
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
        Mechanism::ScramSha256 => scram::<Sha256>(conn, ranges, sasl, &fresh_nonce()?).await,
        Mechanism::ScramSha512 => scram::<Sha512>(conn, ranges, sasl, &fresh_nonce()?).await,
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
    let request = SaslAuthenticateRequest {
        auth_bytes: token,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request.encode(&mut body, version)?;
    let mut resp = conn
        .request(SaslAuthenticateRequest::API_KEY, version, &body)
        .await?;
    Ok(SaslAuthenticateResponse::decode(&mut resp, version)?)
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
) -> Result<(), ClientError>
where
    D: Digest + BlockSizeUser + Clone + Sync,
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

    let (client_final, server_signature) = scram_client_final::<D>(
        &sasl.password,
        &salt,
        iterations,
        &client_first_bare,
        &server_first,
        server_nonce,
    )?;

    let resp = sasl_round(conn, ranges, Bytes::from(client_final)).await?;
    check_auth(&resp)?;
    let server_final = std::str::from_utf8(&resp.auth_bytes)
        .map_err(|_| ClientError::Sasl("server-final message is not utf-8".into()))?;
    let attrs = parse_scram(server_final)?;
    if let Ok(err) = attrs.get('e') {
        return Err(ClientError::Sasl(format!("server error: {err}")));
    }
    let verifier = base64_decode(attrs.get('v')?)?;
    if verifier != server_signature {
        return Err(ClientError::Sasl(
            "server signature mismatch: the broker does not know this password".into(),
        ));
    }
    Ok(())
}

/// The client-final-message and the expected server signature. Pure so
/// the RFC 7677 vector can drive it.
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
            password: "secret".into(),
        };
        assert!(format!("{sasl:?}").contains("<redacted>"));
        assert!(!format!("{sasl:?}").contains("secret"));
    }

    #[test]
    fn saslname_escapes() {
        assert_eq!(saslname("a=b,c"), "a=3Db=2Cc");
    }
}
