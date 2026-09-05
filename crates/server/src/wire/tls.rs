//! TLS support (SEC2): MySQL SSLRequest detection, rustls server config
//! loading, and a stream enum so the connection path runs over plaintext
//! or TLS without branching at every read/write.
//!
//! rustls + ring are pure Rust at runtime (ring needs a C compiler at build
//! time only). The `engine` crate stays dependency-free.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

/// Stream over a connection: plaintext TCP or a completed TLS session.
/// Read/Write impls keep `packet.rs` / `stmt.rs` generic over both.
pub enum ConnStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Read for ConnStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ConnStream::Plain(s) => s.read(buf),
            ConnStream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for ConnStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            ConnStream::Plain(s) => s.write(buf),
            ConnStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            ConnStream::Plain(s) => s.flush(),
            ConnStream::Tls(s) => s.flush(),
        }
    }
}

impl ConnStream {
    /// Read timeout on the underlying TCP socket in both modes (TLS has no
    /// per-stream timeout of its own; the socket option covers it).
    pub fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            ConnStream::Plain(s) => s.set_read_timeout(dur),
            ConnStream::Tls(s) => s.get_mut().set_read_timeout(dur),
        }
    }
}

fn io_err(kind: std::io::ErrorKind, msg: String) -> std::io::Error {
    std::io::Error::new(kind, msg)
}

/// Load a rustls server config from a PEM certificate chain and a PEM
/// private key (PKCS#8, RSA, or SEC1). Fails closed: I/O and parse errors
/// abort startup, never silently downgrade to plaintext.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> std::io::Result<Arc<ServerConfig>> {
    use std::io::BufReader;
    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| io_err(std::io::ErrorKind::NotFound, format!("tls cert {}: {e}", cert_path.display())))?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| io_err(std::io::ErrorKind::InvalidInput, format!("tls cert parse: {e}")))?;
    if certs.is_empty() {
        return Err(io_err(
            std::io::ErrorKind::InvalidInput,
            format!("tls cert {}: no certificates found", cert_path.display()),
        ));
    }
    let key_file = std::fs::File::open(key_path)
        .map_err(|e| io_err(std::io::ErrorKind::NotFound, format!("tls key {}: {e}", key_path.display())))?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| io_err(std::io::ErrorKind::InvalidInput, format!("tls key parse: {e}")))?
        .ok_or_else(|| {
            io_err(
                std::io::ErrorKind::InvalidInput,
                format!("tls key {}: no private key found", key_path.display()),
            )
        })?;
    // Process-wide default provider (no-op when already installed).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io_err(std::io::ErrorKind::InvalidInput, format!("tls config: {e}")))?;
    Ok(Arc::new(config))
}

/// Run the TLS handshake on an already-connected socket (blocking; the
/// caller bounds it with a read timeout set before the upgrade).
pub fn accept_tls(
    config: &Arc<ServerConfig>,
    sock: TcpStream,
) -> std::io::Result<StreamOwned<ServerConnection, TcpStream>> {
    let mut conn = ServerConnection::new(config.clone())
        .map_err(|e| io_err(std::io::ErrorKind::Other, format!("tls accept: {e}")))?;
    let mut sock = sock;
    while conn.is_handshaking() {
        conn.complete_io(&mut sock)
            .map_err(|e| io_err(e.kind(), format!("tls handshake: {e}")))?;
    }
    Ok(StreamOwned::new(conn, sock))
}
