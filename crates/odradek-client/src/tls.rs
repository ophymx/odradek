//! TLS for broker connections, via rustls.
//!
//! Two questions, answered separately: whom this client trusts, and how
//! it proves who *it* is.
//!
//! Trust — [`Tls::system`] takes the Mozilla root store (what a managed
//! Kafka with a public certificate needs); [`Tls::with_ca_pem`] takes a
//! specific CA or self-signed certificate (self-hosted clusters).
//!
//! Identity — by default, none: the broker authenticates to the client
//! and not the reverse, which is what SASL is then usually for. Clusters
//! configured with `ssl.client.auth=required` instead authenticate the
//! *connection*, by asking for a client certificate during the
//! handshake. [`Tls::with_ca_and_client_auth`] and
//! [`Tls::system_with_client_auth`] supply one. Nothing else changes: a
//! client certificate is presented at handshake time, so mutual TLS
//! needs no additional step once the connection is up, and it composes
//! with SASL if a cluster wants both.
//!
//! [`Tls::custom`] still accepts a fully caller-built rustls config, for
//! certificate pinning, hardware keys, or anything else this module does
//! not name.

use std::sync::Arc;

use tokio_rustls::rustls;

use crate::error::ClientError;

/// Whether and how connections are wrapped in TLS.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum Tls {
    /// Plaintext TCP (the default; fine for local development, not for
    /// networks you do not own).
    #[default]
    None,
    /// TLS with this rustls configuration.
    Rustls(Arc<rustls::ClientConfig>),
}

impl Tls {
    /// TLS trusting the bundled Mozilla root store.
    pub fn system() -> Tls {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        Tls::Rustls(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ))
    }

    /// TLS trusting exactly the CA (or self-signed) certificates in
    /// `pem`.
    pub fn with_ca_pem(pem: &[u8]) -> Result<Tls, ClientError> {
        Ok(Tls::Rustls(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store(pem)?)
                .with_no_client_auth(),
        )))
    }

    /// Mutual TLS: trust exactly `ca_pem`, and present `cert_pem` /
    /// `key_pem` when the broker asks who this client is.
    ///
    /// The shape a self-hosted cluster with `ssl.client.auth=required`
    /// wants, and the common one — a private CA issues both the broker's
    /// certificate and the client's.
    ///
    /// `cert_pem` is the client certificate, optionally followed by the
    /// intermediates needed to chain it to a CA the broker trusts; leaf
    /// first, as in every other PEM chain. `key_pem` is its private key
    /// in PKCS#8, PKCS#1, or SEC1 form.
    ///
    /// The key is parsed here and handed to rustls, which keeps it for
    /// the life of the configuration. Nothing in this crate logs it or
    /// prints it — errors below are deliberately fixed strings rather
    /// than anything derived from the input, because a parse error on a
    /// private key is the last place to start quoting the input back.
    pub fn with_ca_and_client_auth(
        ca_pem: &[u8],
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<Tls, ClientError> {
        let roots = root_store(ca_pem)?;
        let (chain, key) = client_identity(cert_pem, key_pem)?;
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map(|config| Tls::Rustls(Arc::new(config)))
            .map_err(|e| ClientError::Tls(format!("client certificate rejected: {e}")))
    }

    /// Mutual TLS against a broker with a publicly trusted certificate:
    /// the Mozilla root store for trust, `cert_pem` / `key_pem` for
    /// identity.
    ///
    /// See [`Tls::with_ca_and_client_auth`] for what the two PEMs hold.
    pub fn system_with_client_auth(cert_pem: &[u8], key_pem: &[u8]) -> Result<Tls, ClientError> {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let (chain, key) = client_identity(cert_pem, key_pem)?;
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map(|config| Tls::Rustls(Arc::new(config)))
            .map_err(|e| ClientError::Tls(format!("client certificate rejected: {e}")))
    }

    /// TLS with a caller-built configuration (pinning, hardware keys...).
    pub fn custom(config: Arc<rustls::ClientConfig>) -> Tls {
        Tls::Rustls(config)
    }
}

/// Parse a PEM bundle of CA certificates into a root store.
fn root_store(pem: &[u8]) -> Result<rustls::RootCertStore, ClientError> {
    use tokio_rustls::rustls::pki_types::CertificateDer;
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(pem) {
        let cert = cert.map_err(|e| ClientError::Tls(format!("bad ca pem: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| ClientError::Tls(format!("bad ca certificate: {e}")))?;
    }
    if roots.is_empty() {
        return Err(ClientError::Tls("ca pem holds no certificates".into()));
    }
    Ok(roots)
}

/// Parse a client certificate chain and its private key.
///
/// Neither error mentions the input. The certificate is public and could
/// safely be quoted, but the key is not, and an error that sometimes
/// includes its argument is one refactor away from always doing so.
type ClientIdentity = (
    Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
);

fn client_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<ClientIdentity, ClientError> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let chain = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ClientError::Tls("client certificate pem does not parse".into()))?;
    if chain.is_empty() {
        return Err(ClientError::Tls(
            "client certificate pem holds no certificates".into(),
        ));
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|_| ClientError::Tls("client key pem does not parse".into()))?;
    Ok((chain, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway self-signed EC certificate, only for parser coverage.
    const TEST_CA: &str = "-----BEGIN CERTIFICATE-----\nMIIBiTCCAS+gAwIBAgIUXuO2qZzBvtnctV6f9BAsqWijXxwwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMB4XDTI2MDkxNzE5MzcwMloXDTI2\nMDkxODE5MzcwMlowGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMFkwEwYHKoZI\nzj0CAQYIKoZIzj0DAQcDQgAEynJre9YxIxrdyndtNLz0QcJwLN0pcjem1+ijTcCr\nRsj/dV+iptafTN9tfdBkBHkUN1IvOOuvQ3GO1VSWVYbIzaNTMFEwHQYDVR0OBBYE\nFPerwBSQqwRtSiZFhbXv1UP+PUG3MB8GA1UdIwQYMBaAFPerwBSQqwRtSiZFhbXv\n1UP+PUG3MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIgT1sAzaf8\nKnUUbMT6bXmTIcE46ihE3LI3BlljV65VrGwCIQCJrumNats94UcKpcJP/m4Sooxf\nCvzLx7rQzHhfm/d3rg==\n-----END CERTIFICATE-----\n";

    /// Throwaway client certificate issued by `TEST_CLIENT_CA`, and its
    /// key. Generated for this test file and trusted by nothing.
    const TEST_CLIENT_CA: &str = "-----BEGIN CERTIFICATE-----\nMIIBiTCCAS+gAwIBAgIUEgmLKDfCgz4qQBw5OD7cYDILs+gwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMB4XDTI2MDkxODIwNDAwMloXDTM2\nMDkxNTIwNDAwMlowGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMFkwEwYHKoZI\nzj0CAQYIKoZIzj0DAQcDQgAEUN5K977nvsfn8UgKKogMJ2RkvUU6//eWX7mcNKZd\nHLUhxhmtbtOq2IizfjKd0Zvdh+8Jdr0GuJ6n3YPKq600/aNTMFEwHQYDVR0OBBYE\nFDOEL/eGUv/gPYLkd9mvvbfeF9G6MB8GA1UdIwQYMBaAFDOEL/eGUv/gPYLkd9mv\nvbfeF9G6MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIgQJCZKyc/\np6UnaY2IMWreS2EeVtr2qNt1De5vZ6Pc2KACIQDSeW4em3/+Uuh/vq+ZfKpMwk7i\n4aMJr5fC3XAOpGJU8A==\n-----END CERTIFICATE-----\n";
    const TEST_CLIENT_CERT: &str = "-----BEGIN CERTIFICATE-----\nMIIBdjCCAR2gAwIBAgIUFaKt7srOjNkoUNkds5eWwARHzigwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMB4XDTI2MDkxODIwNDAwMloXDTM2\nMDkxNTIwNDAwMlowGTEXMBUGA1UEAwwOb2RyYWRlay1jbGllbnQwWTATBgcqhkjO\nPQIBBggqhkjOPQMBBwNCAATu5dnP/JcmZqpV80esbevw6kvppxwyKHnq51laLCQ/\nHgxfmsf0eDkOYPEIgaMvKxXyZb3UxUH8xwffhjiYyjJ4o0IwQDAdBgNVHQ4EFgQU\nKQrQ4RGIVkqNxhUcE02qKaEyGG0wHwYDVR0jBBgwFoAUM4Qv94ZS/+A9guR32a+9\nt94X0bowCgYIKoZIzj0EAwIDRwAwRAIgT+sEWRBmlmun00LuEkJAWMintIqX6k58\nyioh18r3ZewCIDZ35DHWV1L80x686pj7mVYu+/a1/CnmkLTV3sY3GW3a\n-----END CERTIFICATE-----\n";
    const TEST_CLIENT_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgxmfltur5wx7yCMIp\nAp0W/TF7K/GHQizl49uiTRgHGaOhRANCAATu5dnP/JcmZqpV80esbevw6kvppxwy\nKHnq51laLCQ/Hgxfmsf0eDkOYPEIgaMvKxXyZb3UxUH8xwffhjiYyjJ4\n-----END PRIVATE KEY-----\n";

    #[test]
    fn client_auth_accepts_a_certificate_and_its_key() {
        assert!(matches!(
            Tls::with_ca_and_client_auth(
                TEST_CLIENT_CA.as_bytes(),
                TEST_CLIENT_CERT.as_bytes(),
                TEST_CLIENT_KEY.as_bytes(),
            ),
            Ok(Tls::Rustls(_))
        ));
        assert!(matches!(
            Tls::system_with_client_auth(TEST_CLIENT_CERT.as_bytes(), TEST_CLIENT_KEY.as_bytes()),
            Ok(Tls::Rustls(_))
        ));
    }

    #[test]
    fn client_auth_rejects_missing_or_unparseable_material() {
        let ca = TEST_CLIENT_CA.as_bytes();
        // No certificate, no key, and a key that is really a certificate.
        assert!(Tls::with_ca_and_client_auth(ca, b"", TEST_CLIENT_KEY.as_bytes()).is_err());
        assert!(Tls::with_ca_and_client_auth(ca, TEST_CLIENT_CERT.as_bytes(), b"").is_err());
        assert!(
            Tls::with_ca_and_client_auth(ca, TEST_CLIENT_CERT.as_bytes(), ca).is_err(),
            "a certificate is not a private key"
        );
    }

    /// The private key's DER, hex-encoded the way rustls renders DER in
    /// `Debug`. Derived from `TEST_CLIENT_KEY`:
    /// `openssl pkey -in client.key -outform DER | xxd -p | tr -d '\n'`.
    const TEST_CLIENT_KEY_SCALAR_HEX: &str =
        "c667e5b6eaf9c31ef208c229029d16fd317b2bf187422ce5e3dba24d180719a3";

    /// The key must not be reachable through the `Debug` that `Tls`
    /// derives — a config is the sort of thing that ends up in a tracing
    /// span or a panic message.
    ///
    /// Searching the rendering for the key's *base64* would prove
    /// nothing: rustls prints DER as hex, so base64 could never appear
    /// whether or not the key leaked. The search is therefore for the
    /// private scalar in hex, and the assertions below first establish
    /// that this rendering does print DER in exactly that form — the
    /// client certificate is right there in hex — so the key's absence
    /// is a fact about the key rather than about the format.
    #[test]
    fn debug_does_not_expose_the_private_key() {
        let tls = Tls::with_ca_and_client_auth(
            TEST_CLIENT_CA.as_bytes(),
            TEST_CLIENT_CERT.as_bytes(),
            TEST_CLIENT_KEY.as_bytes(),
        )
        .expect("builds");
        let rendered = format!("{tls:?}");

        // The guard is looking in the right place, at a format that
        // would show the key if it were shown at all.
        assert!(
            rendered.contains("SingleCertAndKey"),
            "Debug no longer renders the client identity; this test is \
             checking nothing: {rendered}"
        );
        assert!(
            rendered.contains("CertificateDer(0x3082"),
            "Debug no longer renders DER as hex; the search below would \
             be for the wrong encoding"
        );

        assert!(
            !rendered.contains(TEST_CLIENT_KEY_SCALAR_HEX),
            "Debug rendered the private key"
        );
        // Half of it would be as bad as all of it.
        assert!(
            !rendered.contains(&TEST_CLIENT_KEY_SCALAR_HEX[..32]),
            "Debug rendered part of the private key"
        );
    }

    #[test]
    fn ca_pem_parses_and_empty_or_garbage_is_an_error() {
        assert!(matches!(
            Tls::with_ca_pem(TEST_CA.as_bytes()),
            Ok(Tls::Rustls(_))
        ));
        assert!(Tls::with_ca_pem(b"").is_err());
        assert!(Tls::with_ca_pem(b"not pem at all").is_err());
    }
}
