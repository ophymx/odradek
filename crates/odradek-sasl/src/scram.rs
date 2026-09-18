//! SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802, RFC 7677), both roles.
//!
//! The exchange is four messages and the whole of its security is in
//! how they relate to each other:
//!
//! ```text
//! client-first  n,,n=<user>,r=<client nonce>
//! server-first  r=<client nonce><server nonce>,s=<salt>,i=<iterations>
//! client-final  c=biws,r=<combined nonce>,p=<client proof>
//! server-final  v=<server signature>
//! ```
//!
//! - The **nonces** are each side's evidence that the other is live.
//!   The server's must *extend* the client's, never replace it: the
//!   client's nonce is the only thing in the exchange it knows cannot
//!   have appeared in a recording.
//! - The **iteration count** is named by the server and paid by the
//!   client, before the client has learned anything. It is bounded here
//!   in both directions — see [`Limits`].
//! - The **proof** authenticates the client without the password ever
//!   crossing the wire, and the **signature** authenticates the server
//!   in return. A client that skips checking `v=` has proved itself to
//!   whatever answered the socket.
//! - Both are computed over the `AuthMessage`, which is every byte of
//!   the first three messages concatenated — so nothing in them can be
//!   altered in flight without both sides noticing.
//!
//! # What this does not protect
//!
//! SCRAM authenticates; it does not encrypt. Over a plaintext
//! connection a passive observer collects the username, salt, iteration
//! count, nonces and proof — exactly the inputs to an offline
//! dictionary attack, at the cost the iteration count sets. SCRAM over
//! plaintext protects a password from being *read*, not from being
//! *cracked*. Use TLS.

use hmac::digest::FixedOutputReset;
use hmac::digest::core_api::BlockSizeUser;
use hmac::{Mac, SimpleHmac};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::prep::saslprep;
use crate::{Limits, SaslError, b64, mechanism::Mechanism};

/// The GS2 header this implementation sends: no channel binding, no
/// authorization identity.
///
/// Channel binding is not implemented, and saying so explicitly is the
/// point — `c=biws` is the base64 of `n,,`, where the leading `n` means
/// "client does not support channel binding". A server that requires it
/// will refuse, which is the correct outcome, rather than a silent
/// downgrade that looks like success.
pub const GS2_HEADER: &str = "n,,";
const GS2_HEADER_B64: &str = "biws";

/// Bytes of randomness in a generated nonce.
///
/// RFC 5802 requires only that a nonce not repeat; unpredictability is
/// what actually matters, and 18 bytes of CSPRNG output is well past
/// what an attacker could anticipate.
const NONCE_BYTES: usize = 18;

/// A freshly generated, unpredictable nonce.
///
/// Both roles need one, and both need it from a CSPRNG: a server nonce
/// an attacker can predict lets a recorded exchange be replayed at a
/// client, which is the one thing the client's own nonce cannot catch.
pub fn fresh_nonce() -> Result<String, SaslError> {
    let mut raw = [0u8; NONCE_BYTES];
    getrandom::fill(&mut raw).map_err(|_| SaslError::Entropy)?;
    Ok(b64::encode(&raw))
}

/// One `k=v` attribute of a SCRAM message.
pub(crate) fn attr(message: &str, key: char) -> Option<&str> {
    message.split(',').find_map(|part| {
        let mut chars = part.chars();
        let found = chars.next()?;
        let rest = chars.as_str().strip_prefix('=')?;
        (found == key).then_some(rest)
    })
}

/// The key material both roles derive from a password.
///
/// Zeroized on drop, all of it. The salted password these descend from
/// is already wiped, but `client_key` is password-equivalent for
/// authentication — anyone holding it can compute a proof for any
/// challenge — and `server_key` can forge the signature that
/// authenticates the server. A client keeps these for the length of an
/// exchange, which is long enough to be worth not leaving behind.
#[derive(Zeroize, ZeroizeOnDrop)]
struct Keys {
    client_key: [u8; 64],
    stored_key: [u8; 64],
    server_key: [u8; 64],
    #[zeroize(skip)]
    len: usize,
}

impl Keys {
    /// Derive from a password that has already been SASLprep'd.
    fn derive<D>(password: &str, salt: &[u8], iterations: u32) -> Keys
    where
        D: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync,
    {
        let len = <D as Digest>::output_size();
        let mut salted = Zeroizing::new(vec![0u8; len]);
        pbkdf2::pbkdf2::<SimpleHmac<D>>(password.as_bytes(), salt, iterations, &mut salted)
            .expect("pbkdf2 accepts any output length");

        let client_key = hmac::<D>(&salted, b"Client Key");
        let mut stored = [0u8; 64];
        stored[..len].copy_from_slice(&<D as Digest>::digest(&client_key[..len]));
        Keys {
            client_key,
            stored_key: stored,
            server_key: hmac::<D>(&salted, b"Server Key"),
            len,
        }
    }

    /// `v=` — proof that the holder of this key material signed the
    /// exchange.
    fn server_signature<D>(&self, auth_message: &str) -> String
    where
        D: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync,
    {
        let sig = hmac::<D>(&self.server_key[..self.len], auth_message.as_bytes());
        b64::encode(&sig[..self.len])
    }

    /// `p=` — proof that the holder of this key material is the client.
    fn client_proof<D>(&self, auth_message: &str) -> String
    where
        D: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync,
    {
        let signature = hmac::<D>(&self.stored_key[..self.len], auth_message.as_bytes());
        let mut proof = [0u8; 64];
        for i in 0..self.len {
            proof[i] = self.client_key[i] ^ signature[i];
        }
        b64::encode(&proof[..self.len])
    }
}

fn hmac<D>(key: &[u8], message: &[u8]) -> [u8; 64]
where
    D: Digest + BlockSizeUser + FixedOutputReset + Clone + Sync,
{
    let mut mac = <SimpleHmac<D> as Mac>::new_from_slice(key).expect("hmac takes any key length");
    mac.update(message);
    let tag = mac.finalize().into_bytes();
    let mut out = [0u8; 64];
    out[..tag.len()].copy_from_slice(&tag);
    out
}

/// Dispatch a closure over the digest the mechanism names.
macro_rules! with_digest {
    ($mechanism:expr, |$d:ident| $body:expr) => {
        match $mechanism {
            Mechanism::ScramSha256 => {
                type $d = Sha256;
                $body
            }
            Mechanism::ScramSha512 => {
                type $d = Sha512;
                $body
            }
            Mechanism::Plain => return Err(SaslError::NotScram),
        }
    };
}

impl std::fmt::Debug for ScramClient {
    /// Never the password, the nonce, or the derived keys. A struct
    /// that holds a credential is exactly what ends up in a tracing
    /// span, and a nonce printed after the fact is a replay aid.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ScramServer {
    /// As above: the salt is the only value here that is safe to print,
    /// and printing it alone is not worth the risk of the habit.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramServer")
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("iterations", &self.iterations)
            .finish_non_exhaustive()
    }
}

/// The client half of a SCRAM exchange.
///
/// Drives the two messages a client sends and checks the one that comes
/// back. Sans-I/O: it produces and consumes strings, and the caller
/// carries them over whatever transport it has.
pub struct ScramClient {
    mechanism: Mechanism,
    username: String,
    password: Zeroizing<String>,
    nonce: String,
    limits: Limits,
    /// Set once `client_first` has been produced.
    client_first_bare: Option<String>,
    /// Set once `client_final` has been produced.
    pending: Option<Pending>,
}

struct Pending {
    auth_message: String,
    keys: Keys,
}

impl ScramClient {
    /// A client for `username`, with a freshly generated nonce.
    ///
    /// Both credentials are SASLprep'd here rather than at use, so a
    /// value this constructor accepted is one the whole exchange can
    /// derive from.
    pub fn new(
        mechanism: Mechanism,
        username: &str,
        password: &str,
        limits: Limits,
    ) -> Result<ScramClient, SaslError> {
        Ok(ScramClient {
            mechanism,
            username: saslprep(username)?,
            password: Zeroizing::new(saslprep(password)?),
            nonce: fresh_nonce()?,
            limits,
            client_first_bare: None,
            pending: None,
        })
    }

    /// Replace the generated nonce with a fixed one. **Tests only.**
    ///
    /// A fixed nonce is what makes the RFC's worked example
    /// reproducible and is a replay vulnerability anywhere else, so the
    /// name is deliberately one nobody types by accident and nobody
    /// reads past in review.
    #[doc(hidden)]
    pub fn with_fixed_nonce_for_tests(mut self, nonce: &str) -> ScramClient {
        self.nonce = nonce.to_owned();
        self
    }

    /// `client-first`: who this is, and a nonce.
    pub fn client_first(&mut self) -> String {
        // The username is escaped: a comma or equals sign in it would
        // otherwise end the attribute and forge the rest of the message.
        let bare = format!(
            "n={},r={}",
            self.username.replace('=', "=3D").replace(',', "=2C"),
            self.nonce
        );
        self.client_first_bare = Some(bare.clone());
        format!("{GS2_HEADER}{bare}")
    }

    /// `client-final`, from the server's first message.
    ///
    /// Rejects a server nonce that does not extend this client's, and
    /// an iteration count outside [`Limits`], before spending the key
    /// derivation the count would cost.
    pub fn client_final(&mut self, server_first: &str) -> Result<String, SaslError> {
        let client_first_bare = self
            .client_first_bare
            .clone()
            .ok_or(SaslError::OutOfSequence)?;

        let server_nonce = attr(server_first, 'r').ok_or(SaslError::Malformed("nonce"))?;
        if !server_nonce.starts_with(&self.nonce) || server_nonce == self.nonce {
            // Either a replay of an exchange this client never made, or
            // a server that did not contribute a nonce at all.
            return Err(SaslError::NonceNotExtended);
        }
        let salt = b64::decode(attr(server_first, 's').ok_or(SaslError::Malformed("salt"))?)?;
        let iterations: u32 = attr(server_first, 'i')
            .ok_or(SaslError::Malformed("iterations"))?
            .parse()
            .map_err(|_| SaslError::Malformed("iterations"))?;
        self.limits.check(iterations)?;

        let without_proof = format!("c={GS2_HEADER_B64},r={server_nonce}");
        let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
        let mechanism = self.mechanism;
        let (proof, keys) = with_digest!(mechanism, |D| {
            let keys = Keys::derive::<D>(&self.password, &salt, iterations);
            (keys.client_proof::<D>(&auth_message), keys)
        });
        self.pending = Some(Pending { auth_message, keys });
        Ok(format!("{without_proof},p={proof}"))
    }

    /// Verify `server-final`. This is the step that authenticates the
    /// *server*, and skipping it makes the whole exchange one-way.
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), SaslError> {
        let pending = self.pending.as_ref().ok_or(SaslError::OutOfSequence)?;
        if let Some(error) = attr(server_final, 'e') {
            return Err(SaslError::ServerRejected(error.to_owned()));
        }
        let signature = attr(server_final, 'v').ok_or(SaslError::NoServerSignature)?;
        let mechanism = self.mechanism;
        let expected = with_digest!(mechanism, |D| pending
            .keys
            .server_signature::<D>(&pending.auth_message));
        // Constant time: a server signature is attacker-influenced input
        // compared against a secret-derived value.
        if expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() == 1 {
            Ok(())
        } else {
            Err(SaslError::ServerSignatureMismatch)
        }
    }
}

/// The server half of a SCRAM exchange.
///
/// Holds one account's password because that is what this crate's
/// consumers need — a test double and a reference implementation, not a
/// credential store. Deriving keys per exchange rather than storing
/// them is the slower and simpler choice, and the one that cannot get
/// the stored-key format wrong.
pub struct ScramServer {
    mechanism: Mechanism,
    username: String,
    password: Zeroizing<String>,
    salt: Vec<u8>,
    iterations: u32,
    nonce: String,
    /// Set once `server_first` has been produced.
    pending: Option<ServerPending>,
}

struct ServerPending {
    client_first_bare: String,
    server_first: String,
    /// The GS2 header this exchange actually began with, base64'd the
    /// way `c=` will carry it back.
    expected_binding: String,
}

impl ScramServer {
    /// A server for one account, with a freshly generated nonce.
    pub fn new(
        mechanism: Mechanism,
        username: &str,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
        limits: Limits,
    ) -> Result<ScramServer, SaslError> {
        limits.check(iterations)?;
        Ok(ScramServer {
            mechanism,
            username: saslprep(username)?,
            password: Zeroizing::new(saslprep(password)?),
            salt,
            iterations,
            nonce: fresh_nonce()?,
            pending: None,
        })
    }

    /// Replace the generated nonce with a fixed one. **Tests only.**
    ///
    /// A predictable *server* nonce lets a recorded exchange be replayed
    /// at a client, which is the one attack the client's own nonce
    /// cannot detect. Named to be unmissable for that reason.
    #[doc(hidden)]
    pub fn with_fixed_nonce_for_tests(mut self, nonce: &str) -> ScramServer {
        self.nonce = nonce.to_owned();
        self
    }

    /// `server-first`, from the client's first message.
    pub fn server_first(&mut self, client_first: &str) -> Result<String, SaslError> {
        // The GS2 header is everything before the third field, and it
        // has to come back inside `c=`. RFC 5802 §5.1: the server
        // compares them, and that comparison is what catches an
        // attacker rewriting the header in flight — the proof cannot,
        // because both sides compute it from the client's own `c=`.
        let (header, bare) = client_first
            .match_indices(',')
            .nth(1)
            .map(|(i, _)| client_first.split_at(i + 1))
            .ok_or(SaslError::Malformed("gs2 header"))?;
        let expected_binding = b64::encode(header.as_bytes());
        let client_nonce = attr(bare, 'r').ok_or(SaslError::Malformed("nonce"))?;
        let user = attr(bare, 'n')
            .ok_or(SaslError::Malformed("username"))?
            .replace("=2C", ",")
            .replace("=3D", "=");
        if saslprep(&user)? != self.username {
            return Err(SaslError::UnknownUser);
        }

        // Extend, never replace: the client's nonce has to survive into
        // the answer or the client cannot rule out a recording.
        let server_first = format!(
            "r={client_nonce}{},s={},i={}",
            self.nonce,
            b64::encode(&self.salt),
            self.iterations
        );
        self.pending = Some(ServerPending {
            client_first_bare: bare.to_owned(),
            server_first: server_first.clone(),
            expected_binding,
        });
        Ok(server_first)
    }

    /// Verify `client-final` and produce `server-final`.
    pub fn server_final(&mut self, client_final: &str) -> Result<String, SaslError> {
        let pending = self.pending.as_ref().ok_or(SaslError::OutOfSequence)?;
        let binding = attr(client_final, 'c').ok_or(SaslError::Malformed("channel binding"))?;
        if binding != pending.expected_binding {
            // The client is reporting a different GS2 header than the
            // one that arrived, so something rewrote it on the way.
            return Err(SaslError::ChannelBindingMismatch);
        }
        let proof = attr(client_final, 'p').ok_or(SaslError::Malformed("proof"))?;
        let without_proof = client_final
            .rsplit_once(",p=")
            .map(|(head, _)| head)
            .ok_or(SaslError::Malformed("proof"))?;
        let auth_message = format!(
            "{},{},{}",
            pending.client_first_bare, pending.server_first, without_proof
        );

        let mechanism = self.mechanism;
        let keys = with_digest!(mechanism, |D| Keys::derive::<D>(
            &self.password,
            &self.salt,
            self.iterations
        ));
        let expected = with_digest!(mechanism, |D| keys.client_proof::<D>(&auth_message));
        // Constant time: the proof is attacker-supplied and compared
        // against a secret-derived value, which is the textbook shape of
        // a timing oracle.
        if expected.as_bytes().ct_eq(proof.as_bytes()).unwrap_u8() != 1 {
            return Err(SaslError::BadClientProof);
        }
        let signature = with_digest!(mechanism, |D| keys.server_signature::<D>(&auth_message));
        Ok(format!("v={signature}"))
    }
}
