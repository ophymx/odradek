//! SASLprep (RFC 4013), the normalization both ends must agree on.
//!
//! Two implementations that skip this still interoperate perfectly —
//! right up until someone uses a password that is not plain ASCII.
//! SASLprep maps non-ASCII spaces to U+0020, deletes soft hyphens and
//! other zero-width characters, applies NFKC, and prohibits control and
//! unassigned code points. Skip it and `"pa ss"` typed with a non-break
//! space derives a different key than the same password normalized,
//! which surfaces as an authentication failure that looks exactly like
//! a wrong password and cannot be debugged from the logs.
//!
//! Kafka's Java client normalizes, so a Rust client that does not is
//! the one that is wrong.
//!
//! The Unicode work is [`stringprep`]'s, deliberately: NFKC and the
//! stringprep tables are precisely the kind of thing to take from a
//! maintained implementation rather than hand-roll.

use crate::SaslError;

/// Normalize a username or password for use in SCRAM key derivation.
///
/// Stored-value profile (RFC 4013 §2.3 "stored strings"): unassigned
/// code points are prohibited rather than passed through, because a
/// credential is stored and compared across implementations and time,
/// and an unassigned point may acquire a normalization tomorrow.
pub fn saslprep(value: &str) -> Result<String, SaslError> {
    // The overwhelmingly common case, and it is the identity function:
    // printable ASCII has nothing to map, delete, normalize, or
    // prohibit. Checking for it first keeps the usual path free of
    // Unicode tables entirely.
    if value.bytes().all(|b| b.is_ascii_graphic() || b == b' ') {
        return Ok(value.to_owned());
    }
    stringprep::saslprep(value)
        .map(|prepped| prepped.into_owned())
        .map_err(|_| SaslError::Unpreparable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_passes_through_unchanged() {
        for value in ["admin", "p@ssw0rd!", "with space", ""] {
            assert_eq!(saslprep(value).unwrap(), value);
        }
    }

    /// RFC 4013 §3: a non-break space maps to an ordinary one, so two
    /// clients that typed "different" passwords derive the same key.
    #[test]
    fn non_ascii_space_maps_to_ascii_space() {
        assert_eq!(saslprep("a\u{00A0}b").unwrap(), "a b");
    }

    /// Soft hyphen is a "commonly mapped to nothing" code point: it
    /// disappears rather than becoming a character.
    #[test]
    fn zero_width_characters_are_deleted() {
        assert_eq!(saslprep("pass\u{00AD}word").unwrap(), "password");
    }

    /// Without NFKC these are different strings; with it they are one
    /// password, which is the entire point of normalizing before the KDF.
    #[test]
    fn compatibility_forms_normalize_together() {
        assert_eq!(saslprep("\u{FB01}n").unwrap(), saslprep("fin").unwrap());
    }

    #[test]
    fn control_characters_are_refused() {
        assert!(matches!(
            saslprep("bad\u{0007}value"),
            Err(SaslError::Unpreparable)
        ));
    }
}
