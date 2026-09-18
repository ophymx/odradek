//! TLS for broker connections, via rustls.
//!
//! [`Tls::system`] trusts the Mozilla root store (what a managed Kafka
//! with a public certificate needs); [`Tls::with_ca_pem`] trusts a
//! specific CA or self-signed certificate (self-hosted clusters);
//! [`Tls::custom`] accepts a fully caller-built rustls config for
//! anything else (client certificates, pinning).

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
        Ok(Tls::Rustls(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )))
    }

    /// TLS with a caller-built configuration (client certs, pinning...).
    pub fn custom(config: Arc<rustls::ClientConfig>) -> Tls {
        Tls::Rustls(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway self-signed EC certificate, only for parser coverage.
    const TEST_CA: &str = "-----BEGIN CERTIFICATE-----\nMIIBiTCCAS+gAwIBAgIUXuO2qZzBvtnctV6f9BAsqWijXxwwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMB4XDTI2MDkxNzE5MzcwMloXDTI2\nMDkxODE5MzcwMlowGjEYMBYGA1UEAwwPb2RyYWRlay10ZXN0LWNhMFkwEwYHKoZI\nzj0CAQYIKoZIzj0DAQcDQgAEynJre9YxIxrdyndtNLz0QcJwLN0pcjem1+ijTcCr\nRsj/dV+iptafTN9tfdBkBHkUN1IvOOuvQ3GO1VSWVYbIzaNTMFEwHQYDVR0OBBYE\nFPerwBSQqwRtSiZFhbXv1UP+PUG3MB8GA1UdIwQYMBaAFPerwBSQqwRtSiZFhbXv\n1UP+PUG3MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIgT1sAzaf8\nKnUUbMT6bXmTIcE46ihE3LI3BlljV65VrGwCIQCJrumNats94UcKpcJP/m4Sooxf\nCvzLx7rQzHhfm/d3rg==\n-----END CERTIFICATE-----\n";

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
