//! PLAIN (RFC 4616): the password, on the wire, as a password.
//!
//! There is nothing to get wrong in the encoding and everything to get
//! wrong in the decision to use it. The token is
//! `authzid \0 authcid \0 password`, and anyone who can read the
//! connection has the password. Callers are expected to refuse it on an
//! unencrypted transport — see [`Mechanism::sends_password`].
//!
//! [`Mechanism::sends_password`]: crate::Mechanism::sends_password

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::SaslError;
use crate::prep::saslprep;

/// The PLAIN token for `username`/`password`, with no authorization id.
///
/// Zeroizing: the token *is* the password plus two bytes, and it should
/// not outlive the send.
pub fn plain_token(username: &str, password: &str) -> Result<Zeroizing<Vec<u8>>, SaslError> {
    let username = saslprep(username)?;
    let password = Zeroizing::new(saslprep(password)?);
    let mut token = Vec::with_capacity(username.len() + password.len() + 2);
    token.push(0);
    token.extend_from_slice(username.as_bytes());
    token.push(0);
    token.extend_from_slice(password.as_bytes());
    Ok(Zeroizing::new(token))
}

/// Verify a PLAIN token against one account.
///
/// Both comparisons are constant time. The username's matters less than
/// the password's and is done the same way anyway: a username oracle
/// tells an attacker which accounts exist, which is the first half of
/// the work.
pub fn verify_plain_token(token: &[u8], username: &str, password: &str) -> Result<bool, SaslError> {
    let mut parts = token.splitn(3, |b| *b == 0);
    // The leading empty field is the authorization identity, which this
    // crate does not support: a non-empty one is a request to act as
    // someone else and must not be quietly ignored.
    let authzid = parts.next().ok_or(SaslError::Malformed("plain token"))?;
    if !authzid.is_empty() {
        return Ok(false);
    }
    let authcid = parts.next().ok_or(SaslError::Malformed("plain token"))?;
    let supplied = parts.next().ok_or(SaslError::Malformed("plain token"))?;

    let expected_user = saslprep(username)?;
    let expected_pass = Zeroizing::new(saslprep(password)?);
    let user_ok = authcid.ct_eq(expected_user.as_bytes()).unwrap_u8() == 1;
    let pass_ok = supplied.ct_eq(expected_pass.as_bytes()).unwrap_u8() == 1;
    Ok(user_ok && pass_ok)
}
