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
        let mut roots = rustls::RootCertStore::empty();
        let mut cursor = std::io::Cursor::new(pem);
        for cert in rustls_pemfile::certs(&mut cursor) {
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
