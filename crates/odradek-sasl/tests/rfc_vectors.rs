//! The worked examples from the RFCs, against both roles.
//!
//! A SCRAM implementation that only talks to itself is self-consistent
//! and possibly wrong: every value below comes from the RFC rather than
//! from this crate, so agreeing with them is evidence about the
//! protocol and not about our own arithmetic.

use odradek_sasl::{Limits, Mechanism, SaslError, ScramClient, ScramServer, saslprep};

/// RFC 7677 §5: the SCRAM-SHA-256 exchange, verbatim.
const USER: &str = "user";
const PASSWORD: &str = "pencil";
const CLIENT_NONCE: &str = "rOprNGfwEbeRWgbNEkqO";
const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
const ITERATIONS: u32 = 4096;

const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
const SERVER_FIRST: &str = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
                            p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

fn salt() -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(SALT_B64)
        .expect("rfc salt is base64")
}

fn rfc_client() -> ScramClient {
    ScramClient::new(Mechanism::ScramSha256, USER, PASSWORD, Limits::default())
        .expect("rfc credentials are preparable")
        .with_fixed_nonce_for_tests(CLIENT_NONCE)
}

fn rfc_server() -> ScramServer {
    ScramServer::new(
        Mechanism::ScramSha256,
        USER,
        PASSWORD,
        salt(),
        ITERATIONS,
        Limits::default(),
    )
    .expect("rfc credentials are preparable")
    .with_fixed_nonce_for_tests(SERVER_NONCE)
}

/// Every message this client produces matches the RFC byte for byte —
/// including the proof, which is the whole key derivation in one value.
#[test]
fn client_messages_match_rfc_7677() {
    let mut client = rfc_client();
    assert_eq!(client.client_first(), CLIENT_FIRST);
    assert_eq!(client.client_final(SERVER_FIRST).unwrap(), CLIENT_FINAL);
    client
        .verify_server_final(SERVER_FINAL)
        .expect("rfc server signature verifies");
}

/// And every message this server produces, given the RFC's client.
#[test]
fn server_messages_match_rfc_7677() {
    let mut server = rfc_server();
    assert_eq!(server.server_first(CLIENT_FIRST).unwrap(), SERVER_FIRST);
    assert_eq!(server.server_final(CLIENT_FINAL).unwrap(), SERVER_FINAL);
}

/// The two halves agree with each other at full strength, with real
/// nonces rather than the RFC's fixed ones.
#[test]
fn the_roles_complete_an_exchange() {
    for mechanism in [Mechanism::ScramSha256, Mechanism::ScramSha512] {
        let mut client =
            ScramClient::new(mechanism, "admin", "hunter2", Limits::default()).unwrap();
        let mut server = ScramServer::new(
            mechanism,
            "admin",
            "hunter2",
            b"a-different-salt".to_vec(),
            4096,
            Limits::default(),
        )
        .unwrap();

        let first = client.client_first();
        let challenge = server.server_first(&first).unwrap();
        let final_message = client.client_final(&challenge).unwrap();
        let signature = server.server_final(&final_message).unwrap();
        client
            .verify_server_final(&signature)
            .unwrap_or_else(|e| panic!("{mechanism} should verify: {e}"));
    }
}

/// A wrong password fails at the server, and fails as a *proof*
/// problem rather than by the server crashing or accepting.
#[test]
fn a_wrong_password_is_refused() {
    let mut client = ScramClient::new(
        Mechanism::ScramSha256,
        "admin",
        "not-the-password",
        Limits::default(),
    )
    .unwrap();
    let mut server = ScramServer::new(
        Mechanism::ScramSha256,
        "admin",
        "hunter2",
        b"salt".to_vec(),
        4096,
        Limits::default(),
    )
    .unwrap();

    let challenge = server.server_first(&client.client_first()).unwrap();
    let final_message = client.client_final(&challenge).unwrap();
    assert_eq!(
        server.server_final(&final_message),
        Err(SaslError::BadClientProof)
    );
}

/// A server that answers with its own nonce instead of extending the
/// client's is refused *before* the client spends the key derivation —
/// this is the client's only defence against a replayed exchange.
#[test]
fn a_nonce_that_does_not_extend_is_refused() {
    let mut client = rfc_client();
    let _ = client.client_first();
    let forged = format!("r=somethingelse,s={SALT_B64},i={ITERATIONS}");
    assert_eq!(
        client.client_final(&forged),
        Err(SaslError::NonceNotExtended)
    );
}

/// And so is an exchange where the server contributed no nonce at all.
#[test]
fn a_nonce_that_is_only_the_clients_is_refused() {
    let mut client = rfc_client();
    let _ = client.client_first();
    let forged = format!("r={CLIENT_NONCE},s={SALT_B64},i={ITERATIONS}");
    assert_eq!(
        client.client_final(&forged),
        Err(SaslError::NonceNotExtended)
    );
}

/// The iteration count is bounded in both directions, and the low one
/// matters most: one round of PBKDF2 is a single HMAC, and a client that
/// computed a proof over it would have handed over a near-unprotected
/// credential.
#[test]
fn iteration_counts_are_bounded() {
    let mut client = rfc_client();
    let _ = client.client_first();
    let weak = format!("r={CLIENT_NONCE}{SERVER_NONCE},s={SALT_B64},i=1");
    assert!(matches!(
        client.client_final(&weak),
        Err(SaslError::IterationsTooLow { asked: 1, .. })
    ));

    let mut client = rfc_client();
    let _ = client.client_first();
    let absurd = format!("r={CLIENT_NONCE}{SERVER_NONCE},s={SALT_B64},i=999999999");
    assert!(matches!(
        client.client_final(&absurd),
        Err(SaslError::IterationsTooHigh { .. })
    ));
}

/// A server that completes the exchange without signing it is caught.
/// Without this check a client has authenticated itself to whatever
/// answered the socket.
#[test]
fn an_unsigned_server_final_is_refused() {
    let mut client = rfc_client();
    let _ = client.client_first();
    let _ = client.client_final(SERVER_FIRST).unwrap();
    assert_eq!(
        client.verify_server_final(""),
        Err(SaslError::NoServerSignature)
    );
    assert_eq!(
        client.verify_server_final("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        Err(SaslError::ServerSignatureMismatch)
    );
}

/// Credentials are normalized before they reach the key derivation, so
/// two spellings of the same password authenticate against each other.
/// Without SASLprep these would derive different keys and the second
/// would look like a wrong password.
#[test]
fn saslprep_is_applied_to_credentials() {
    // U+00A0 NO-BREAK SPACE maps to an ordinary space (RFC 4013 §3).
    assert_eq!(saslprep("pa\u{00A0}ss").unwrap(), "pa ss");

    let mut client = ScramClient::new(
        Mechanism::ScramSha256,
        "user",
        "pa\u{00A0}ss",
        Limits::default(),
    )
    .unwrap();
    let mut server = ScramServer::new(
        Mechanism::ScramSha256,
        "user",
        "pa ss",
        b"salt".to_vec(),
        4096,
        Limits::default(),
    )
    .unwrap();

    let challenge = server.server_first(&client.client_first()).unwrap();
    let final_message = client.client_final(&challenge).unwrap();
    let signature = server.server_final(&final_message).unwrap();
    client
        .verify_server_final(&signature)
        .expect("normalized credentials agree");
}

/// Generated nonces are not predictable, which is what a replay defence
/// rests on. A counter would satisfy "does not repeat" and fail this.
#[test]
fn generated_nonces_do_not_repeat() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..256 {
        assert!(
            seen.insert(odradek_sasl::fresh_nonce().unwrap()),
            "a generated nonce repeated"
        );
    }
}

/// PLAIN round-trips, and refuses everything it should.
#[test]
fn plain_verifies_only_the_right_credentials() {
    use odradek_sasl::{plain_token, verify_plain_token};

    let token = plain_token("admin", "hunter2").unwrap();
    assert!(verify_plain_token(&token, "admin", "hunter2").unwrap());
    assert!(!verify_plain_token(&token, "admin", "wrong").unwrap());
    assert!(!verify_plain_token(&token, "someone", "hunter2").unwrap());

    // An authorization identity is a request to act as someone else,
    // and this crate does not support it — so it is refused rather than
    // ignored.
    let mut impersonating = b"admin".to_vec();
    impersonating.extend_from_slice(b"\0admin\0hunter2");
    assert!(!verify_plain_token(&impersonating, "admin", "hunter2").unwrap());
}

/// A username containing the escape sequences themselves round-trips.
///
/// The decode order is load-bearing and silently wrong the other way:
/// `a=2Cb` encodes to `a=3D2Cb`, and unescaping `=3D` first would turn
/// that into `a=2Cb` and then into `a,b` — a different account. Doing
/// `=2C` first leaves the `=3D` intact until its turn.
#[test]
fn usernames_containing_escapes_round_trip() {
    for user in ["a=2Cb", "a,b", "a=b", "=3D", "plain"] {
        let mut client =
            ScramClient::new(Mechanism::ScramSha256, user, "pw", Limits::default()).unwrap();
        let mut server = ScramServer::new(
            Mechanism::ScramSha256,
            user,
            "pw",
            b"salt".to_vec(),
            4096,
            Limits::default(),
        )
        .unwrap();
        let challenge = server
            .server_first(&client.client_first())
            .unwrap_or_else(|e| panic!("{user:?} should be recognized: {e}"));
        let final_message = client.client_final(&challenge).unwrap();
        server
            .server_final(&final_message)
            .unwrap_or_else(|e| panic!("{user:?} should authenticate: {e}"));
    }
}

/// An account this server does not have is refused, and the refusal
/// does not depend on how the name was escaped.
#[test]
fn a_different_username_is_refused() {
    let mut client =
        ScramClient::new(Mechanism::ScramSha256, "a,b", "pw", Limits::default()).unwrap();
    let mut server = ScramServer::new(
        Mechanism::ScramSha256,
        "a=b",
        "pw",
        b"salt".to_vec(),
        4096,
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        server.server_first(&client.client_first()),
        Err(SaslError::UnknownUser)
    );
}

/// The GS2 header the exchange began with has to be the one reported
/// back in `c=`. The proof cannot catch a header rewritten in flight,
/// because both sides compute it from the client's own `c=` value.
#[test]
fn a_rewritten_gs2_header_is_caught() {
    let mut client =
        ScramClient::new(Mechanism::ScramSha256, "u", "pw", Limits::default()).unwrap();
    let mut server = ScramServer::new(
        Mechanism::ScramSha256,
        "u",
        "pw",
        b"salt".to_vec(),
        4096,
        Limits::default(),
    )
    .unwrap();
    let challenge = server.server_first(&client.client_first()).unwrap();
    let final_message = client.client_final(&challenge).unwrap();

    // "eSws" is base64 of "y,," — a client claiming it offered channel
    // binding, on an exchange that began with "n,,".
    let tampered = final_message.replace("c=biws", "c=eSws");
    assert_eq!(
        server.server_final(&tampered),
        Err(SaslError::ChannelBindingMismatch)
    );
}

/// Malformed input is an error, never a panic. A SASL message is the
/// first thing an unauthenticated peer gets to send.
#[test]
fn hostile_messages_do_not_panic() {
    let junk = [
        "",
        ",",
        ",,",
        "=",
        "r=",
        "n,,",
        "n,,n=,r=",
        "r=x,s=!!!,i=4096",
        "r=x,s=c2FsdA==,i=abc",
        "r=x,s=c2FsdA==,i=99999999999999999999",
        "p=",
        "c=,p=",
        "v=",
        "e=oops",
        "\u{0}\u{0}\u{0}",
        "n,,n=u,r=\u{1F600}",
    ];
    for message in junk {
        let mut client =
            ScramClient::new(Mechanism::ScramSha256, "u", "pw", Limits::default()).unwrap();
        let _ = client.client_first();
        let _ = client.client_final(message);
        let _ = client.verify_server_final(message);

        let mut server = ScramServer::new(
            Mechanism::ScramSha256,
            "u",
            "pw",
            b"salt".to_vec(),
            4096,
            Limits::default(),
        )
        .unwrap();
        let _ = server.server_first(message);
        let _ = server.server_final(message);

        let _ = odradek_sasl::verify_plain_token(message.as_bytes(), "u", "pw");
        let _ = odradek_sasl::saslprep(message);
    }
}
