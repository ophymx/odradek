//! OAUTHBEARER (RFC 7628): authenticate with a token somebody else
//! issued.
//!
//! There is very little protocol here, and that is the point. The client
//! sends one message carrying a bearer token; the server either accepts
//! it or returns a JSON description of why not. Everything that makes
//! the token trustworthy — who issued it, how it was obtained, when it
//! expires, how it is refreshed — happens outside this exchange and
//! outside this crate.
//!
//! ```text
//! client-first  n,,\x01auth=Bearer <token>\x01\x01
//! server        (empty on success)
//!               {"status":"invalid_token",...} on failure
//! client        \x01        — acknowledges the failure
//! ```
//!
//! That last step is easy to skip and worth not skipping: RFC 7628 §3.1
//! has the client send a lone `\x01` after a failure so the server can
//! complete the exchange and report properly, rather than being left
//! waiting for a message that never comes.
//!
//! # What this does not do
//!
//! **It does not get you a token.** Acquiring one means talking to an
//! authorization server over HTTP, which is an I/O concern and a policy
//! concern, and this crate is neither. Pass in a token you already hold
//! and refresh it on your own schedule.
//!
//! **It does not validate the token.** A bearer token is opaque to the
//! mechanism by design; only the server can say whether it is good. A
//! client that parsed one would be guessing at a format it does not own.
//!
//! **The token is a password.** Anyone who reads it can use it until it
//! expires, so OAUTHBEARER over an unencrypted connection hands out
//! credentials exactly as PLAIN does. Use TLS.

use zeroize::Zeroizing;

use crate::SaslError;

/// The separator RFC 7628 uses between the message's parts.
const KVSEP: u8 = 0x01;

/// The GS2 header: no channel binding, no authorization identity.
const GS2_HEADER: &str = "n,,";

/// The client's one message: a bearer token, plus any extensions the
/// server asked for.
///
/// `extensions` are the `key=value` pairs some deployments require
/// alongside the token. They are written in the order given, which is
/// what a server expecting a particular order will want.
///
/// Zeroizing, because the token is a credential and this is a copy of
/// it.
pub fn oauthbearer_token(
    token: &str,
    extensions: &[(&str, &str)],
) -> Result<Zeroizing<Vec<u8>>, SaslError> {
    // A token containing the separator would end the field early and let
    // the rest be read as extensions — the same injection the SCRAM
    // username escaping prevents, except here there is no escaping
    // defined, so the only safe answer is to refuse.
    if token.bytes().any(|b| b == KVSEP) || token.is_empty() {
        return Err(SaslError::Malformed("bearer token"));
    }
    for (key, value) in extensions {
        if key.bytes().chain(value.bytes()).any(|b| b == KVSEP) {
            return Err(SaslError::Malformed("extension"));
        }
        if key.contains('=') {
            return Err(SaslError::Malformed("extension"));
        }
    }

    let mut message = Vec::new();
    message.extend_from_slice(GS2_HEADER.as_bytes());
    message.push(KVSEP);
    message.extend_from_slice(b"auth=Bearer ");
    message.extend_from_slice(token.as_bytes());
    message.push(KVSEP);
    for (key, value) in extensions {
        message.extend_from_slice(key.as_bytes());
        message.push(b'=');
        message.extend_from_slice(value.as_bytes());
        message.push(KVSEP);
    }
    message.push(KVSEP);
    Ok(Zeroizing::new(message))
}

/// What a client sends after the server rejects its token.
///
/// A lone separator. It carries no information and is required anyway:
/// without it the server is left mid-exchange, and the failure it
/// eventually reports is a timeout rather than the reason it already
/// knows.
pub fn oauthbearer_failure_ack() -> [u8; 1] {
    [KVSEP]
}

/// Read the token out of a client's first message, server side.
///
/// Returns the token and any extensions, or an error if the message is
/// not shaped like one. What makes the token acceptable is the caller's
/// question — this only says what was sent.
pub fn parse_oauthbearer_token(
    message: &[u8],
) -> Result<(String, Vec<(String, String)>), SaslError> {
    let mut parts = message.split(|b| *b == KVSEP);
    let gs2 = parts.next().ok_or(SaslError::Malformed("gs2 header"))?;
    if !gs2.starts_with(b"n,") && !gs2.starts_with(b"y,") && !gs2.starts_with(b"p=") {
        return Err(SaslError::Malformed("gs2 header"));
    }
    let auth = parts.next().ok_or(SaslError::Malformed("auth"))?;
    let auth = std::str::from_utf8(auth).map_err(|_| SaslError::Malformed("auth"))?;
    let token = auth
        .strip_prefix("auth=Bearer ")
        .ok_or(SaslError::Malformed("auth"))?;
    if token.is_empty() {
        return Err(SaslError::Malformed("bearer token"));
    }

    let mut extensions = Vec::new();
    for part in parts {
        if part.is_empty() {
            continue;
        }
        let part = std::str::from_utf8(part).map_err(|_| SaslError::Malformed("extension"))?;
        let (key, value) = part
            .split_once('=')
            .ok_or(SaslError::Malformed("extension"))?;
        extensions.push((key.to_owned(), value.to_owned()));
    }
    Ok((token.to_owned(), extensions))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_has_the_shape_rfc_7628_describes() {
        let message = oauthbearer_token("tok", &[]).unwrap();
        assert_eq!(&message[..], b"n,,\x01auth=Bearer tok\x01\x01");
    }

    #[test]
    fn extensions_are_written_in_order() {
        let message = oauthbearer_token("tok", &[("a", "1"), ("b", "2")]).unwrap();
        assert_eq!(
            &message[..],
            b"n,,\x01auth=Bearer tok\x01a=1\x01b=2\x01\x01"
        );
    }

    /// A token carrying the separator would end its own field and let
    /// the remainder be read as extensions. RFC 7628 defines no escaping
    /// for this, so refusing is the only correct answer.
    #[test]
    fn a_token_containing_the_separator_is_refused() {
        assert!(oauthbearer_token("tok\u{1}x=y", &[]).is_err());
        assert!(oauthbearer_token("", &[]).is_err());
        assert!(oauthbearer_token("tok", &[("a\u{1}b", "1")]).is_err());
        assert!(oauthbearer_token("tok", &[("a=b", "1")]).is_err());
    }

    #[test]
    fn the_roles_agree() {
        let message = oauthbearer_token("tok", &[("trace", "abc")]).unwrap();
        let (token, extensions) = parse_oauthbearer_token(&message).unwrap();
        assert_eq!(token, "tok");
        assert_eq!(extensions, vec![("trace".to_owned(), "abc".to_owned())]);
    }

    #[test]
    fn malformed_messages_are_errors_not_panics() {
        for junk in [
            &b""[..],
            b"\x01",
            b"n,,",
            b"n,,\x01",
            b"n,,\x01auth=\x01\x01",
            b"n,,\x01auth=Basic x\x01\x01",
            b"n,,\x01auth=Bearer \x01\x01",
            b"n,,\x01auth=Bearer t\x01noequals\x01\x01",
            b"\xff\xfe\x01auth=Bearer t\x01\x01",
        ] {
            let _ = parse_oauthbearer_token(junk);
        }
    }
}
