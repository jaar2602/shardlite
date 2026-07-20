//! One socket type covering plaintext and TLS, so the protocol code is written once.
//!
//! # Why a single stream, not a split read/write pair
//!
//! The plaintext code used to `try_clone` the socket into an independent reader and writer.
//! A TLS connection cannot be split that way — the encryption state is one object with one
//! sequence number in each direction, and handing out two halves would corrupt the record
//! stream. It does not need to be split: the protocol is strict request-then-response, never
//! reading and writing the same connection at once, so a single [`Stream`] carries both.
//!
//! # TLS is optional and pays for itself only when used
//!
//! The plaintext variant is always present. The TLS variants exist only under the `tls`
//! feature, so a deployment on a trusted network compiles none of rustls and carries none of
//! its size. Choosing TLS or not is a matter of which config an operator supplies — the
//! protocol layer above never knows the difference.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};

/// A connection, encrypted or not, that the protocol reads and writes through uniformly.
pub enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    ServerTls(Box<rustls::StreamOwned<rustls::ServerConnection, TcpStream>>),
    #[cfg(feature = "tls")]
    ClientTls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Stream {
    /// The remote address, for logging and connection identity — always the underlying TCP
    /// peer, whatever wraps it.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.tcp().peer_addr()
    }

    fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            #[cfg(feature = "tls")]
            Stream::ServerTls(s) => s.get_ref(),
            #[cfg(feature = "tls")]
            Stream::ClientTls(s) => s.get_ref(),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            #[cfg(feature = "tls")]
            Stream::ServerTls(s) => s.read(buf),
            #[cfg(feature = "tls")]
            Stream::ClientTls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            #[cfg(feature = "tls")]
            Stream::ServerTls(s) => s.write(buf),
            #[cfg(feature = "tls")]
            Stream::ClientTls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            #[cfg(feature = "tls")]
            Stream::ServerTls(s) => s.flush(),
            #[cfg(feature = "tls")]
            Stream::ClientTls(s) => s.flush(),
        }
    }
}

#[cfg(feature = "tls")]
mod tls {
    use std::path::Path;
    use std::sync::Arc;

    use crate::error::{Error, Result};

    use super::Stream;

    /// Install the ring crypto provider as the process default, once.
    ///
    /// rustls needs a provider chosen before any config is built, and panics rather than
    /// guessing. Doing it here, idempotently, means an operator never has to — and there is
    /// exactly one provider compiled in, so there is nothing to get wrong.
    fn ensure_provider() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            // Err means another thread won the race; the provider is installed either way.
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    fn read_pem(path: &Path) -> Result<Vec<u8>> {
        std::fs::read(path).map_err(|e| Error::Protocol(format!("reading {}: {e}", path.display())))
    }

    /// A server's TLS identity: the certificate it presents and the key that proves it.
    #[derive(Clone)]
    pub struct TlsServerConfig {
        inner: Arc<rustls::ServerConfig>,
    }

    impl TlsServerConfig {
        /// Load a PEM certificate chain and private key.
        pub fn from_pem_files(cert: &Path, key: &Path) -> Result<Self> {
            ensure_provider();
            let cert_pem = read_pem(cert)?;
            let key_pem = read_pem(key)?;

            let certs: Vec<_> = rustls_pemfile::certs(&mut cert_pem.as_slice())
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| {
                    Error::Protocol(format!("parsing certificate {}: {e}", cert.display()))
                })?;
            if certs.is_empty() {
                return Err(Error::Protocol(format!(
                    "{} contained no certificates",
                    cert.display()
                )));
            }
            let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
                .map_err(|e| Error::Protocol(format!("parsing key {}: {e}", key.display())))?
                .ok_or_else(|| {
                    Error::Protocol(format!("{} contained no private key", key.display()))
                })?;

            let inner = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| Error::Protocol(format!("building TLS server config: {e}")))?;
            Ok(Self {
                inner: Arc::new(inner),
            })
        }

        /// Wrap an accepted TCP connection in TLS.
        pub fn accept(&self, tcp: std::net::TcpStream) -> Result<Stream> {
            let conn = rustls::ServerConnection::new(Arc::clone(&self.inner))
                .map_err(|e| Error::Protocol(format!("starting TLS handshake: {e}")))?;
            Ok(Stream::ServerTls(Box::new(rustls::StreamOwned::new(
                conn, tcp,
            ))))
        }
    }

    impl std::fmt::Debug for TlsServerConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("TlsServerConfig")
        }
    }

    /// A client's TLS trust: which server certificates it will accept.
    #[derive(Clone)]
    pub struct TlsClientConfig {
        inner: Arc<rustls::ClientConfig>,
        /// Name presented to the server for SNI and verified against its certificate.
        server_name: String,
    }

    impl TlsClientConfig {
        /// Verify the server against a PEM certificate authority.
        ///
        /// This is the mode that actually protects against a man-in-the-middle: only a server
        /// holding a certificate this CA signed is accepted. `server_name` must match a name
        /// in that certificate.
        pub fn with_ca_pem(ca: &Path, server_name: &str) -> Result<Self> {
            ensure_provider();
            let ca_pem = read_pem(ca)?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
                let cert =
                    cert.map_err(|e| Error::Protocol(format!("parsing CA {}: {e}", ca.display())))?;
                roots
                    .add(cert)
                    .map_err(|e| Error::Protocol(format!("adding CA certificate: {e}")))?;
            }
            let inner = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            Ok(Self {
                inner: Arc::new(inner),
                server_name: server_name.to_string(),
            })
        }

        /// Encrypt, but do **not** verify the server's certificate.
        ///
        /// This protects against a *passive* eavesdropper and nothing more: an active
        /// man-in-the-middle presents its own certificate, this accepts it, and reads
        /// everything. It exists for development and for tests, never for production, and it
        /// says so at every call — the name is deliberately unmissable.
        pub fn dangerous_accept_any_cert(server_name: &str) -> Self {
            ensure_provider();
            tracing::warn!(
                "TLS certificate verification is DISABLED: this encrypts against a passive \
                 eavesdropper but not an active man-in-the-middle. For development only"
            );
            let inner = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerification))
                .with_no_client_auth();
            Self {
                inner: Arc::new(inner),
                server_name: server_name.to_string(),
            }
        }

        /// Wrap a connected TCP stream in TLS.
        pub fn connect(&self, tcp: std::net::TcpStream) -> Result<Stream> {
            let name =
                rustls::pki_types::ServerName::try_from(self.server_name.clone()).map_err(|e| {
                    Error::Protocol(format!("invalid server name '{}': {e}", self.server_name))
                })?;
            let conn = rustls::ClientConnection::new(Arc::clone(&self.inner), name)
                .map_err(|e| Error::Protocol(format!("starting TLS handshake: {e}")))?;
            Ok(Stream::ClientTls(Box::new(rustls::StreamOwned::new(
                conn, tcp,
            ))))
        }
    }

    impl std::fmt::Debug for TlsClientConfig {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("TlsClientConfig")
        }
    }

    /// The verifier behind `dangerous_accept_any_cert`. Accepts everything, deliberately.
    #[derive(Debug)]
    struct NoVerification;

    impl rustls::client::danger::ServerCertVerifier for NoVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error>
        {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error>
        {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}

#[cfg(feature = "tls")]
pub use tls::{TlsClientConfig, TlsServerConfig};
