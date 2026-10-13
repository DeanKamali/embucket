//! Shared mTLS material loading (M7.2, design §13.2).
//!
//! Each Avalanche process (router, scheduler, executors) sources its TLS
//! identity from a directory pointed at by `AVALANCHE_TLS_DIR`:
//!
//! - `ca.crt`       — the local CA that signed every role's certificate
//! - `server.crt` / `server.key` — identity served by listeners
//!   (scheduler gRPC, executor Arrow Flight)
//! - `client.crt` / `client.key` — identity presented when dialing
//!   (router → scheduler, executor → scheduler, scheduler → executor
//!   fetches, task shuffle reads)
//!
//! Servers verify client certificates against `ca.crt` and *require* them
//! (mutual TLS); clients verify the server against the same CA, using the
//! dialed hostname as the verification name — so server certificates must
//! carry SANs for every hostname peers dial (`localhost` / `127.0.0.1` for
//! the host-side router's mapped port, container DNS names in docker).

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ballista_core::utils::{GrpcClientConfig, create_grpc_client_endpoint};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tonic::transport::{
    Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};

/// Environment variable holding the TLS material directory. When set, every
/// listener serves TLS and every dial presents a client certificate.
pub const TLS_DIR_ENV: &str = "AVALANCHE_TLS_DIR";

/// Where the warehouse manager mounts the TLS material inside compute
/// containers (`AVALANCHE_TLS_DIR` is set to this path).
pub const CONTAINER_TLS_DIR: &str = "/etc/avalanche/tls";

/// Loaded TLS material for one process.
///
/// Holds both the raw PEM bytes (tonic's byte-oriented configs) and the
/// parsed DER forms (rustls server config for axum-served TLS).
#[derive(Debug)]
pub struct TlsMaterial {
    dir: PathBuf,
    ca_pem: Vec<u8>,
    server_cert_pem: Vec<u8>,
    server_key_pem: Vec<u8>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
    ca_certs: Vec<CertificateDer<'static>>,
    server_certs: Vec<CertificateDer<'static>>,
    server_key: PrivateKeyDer<'static>,
    /// Parsed client identity, kept in DER form so a rustls-based client
    /// config can be built without re-reading disk (currently unused; the
    /// tonic path consumes the PEM bytes above).
    #[allow(dead_code)]
    client_certs: Vec<CertificateDer<'static>>,
    #[allow(dead_code)]
    client_key: PrivateKeyDer<'static>,
}

/// Failure to load or convert TLS material.
#[derive(Debug)]
pub struct TlsError(String);

impl fmt::Display for TlsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tls: {}", self.0)
    }
}

impl std::error::Error for TlsError {}

impl TlsMaterial {
    /// The TLS directory from the environment, if any.
    ///
    /// An empty value counts as unset (handy for container env passthrough).
    #[must_use]
    pub fn dir_from_env() -> Option<PathBuf> {
        std::env::var_os(TLS_DIR_ENV)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    }

    /// Load all material from `dir`.
    ///
    /// # Errors
    ///
    /// Errors if any of the five files is missing or unparseable.
    pub fn load(dir: impl AsRef<Path>) -> Result<Self, TlsError> {
        install_crypto_provider();
        let dir = dir.as_ref();
        let raw = |name: &str| -> Result<Vec<u8>, TlsError> {
            std::fs::read(dir.join(name))
                .map_err(|e| TlsError(format!("{}: {e}", dir.join(name).display())))
        };
        let ca_pem = raw("ca.crt")?;
        let server_cert_pem = raw("server.crt")?;
        let server_key_pem = raw("server.key")?;
        let client_cert_pem = raw("client.crt")?;
        let client_key_pem = raw("client.key")?;

        let parse_certs = |pem: &[u8], name: &str| -> Result<Vec<CertificateDer<'static>>, TlsError> {
            // One PEM section per cert; walk them all (server.crt may be a chain).
            let mut certs = Vec::new();
            let mut rest = pem;
            loop {
                match rustls_pki_types::pem::from_buf(&mut &*rest) {
                    Ok(Some((_kind, der))) => certs.push(CertificateDer::from(der)),
                    Ok(None) => break,
                    Err(_) if certs.is_empty() => {
                        // fall back to the first-object helper for precise errors
                        return CertificateDer::from_pem_slice(pem)
                            .map(|c| vec![c])
                            .map_err(|e| TlsError(format!("{name}: {e}")));
                    }
                    Err(e) => return Err(TlsError(format!("{name}: {e}"))),
                }
                // Skip past the section just parsed: find the end marker.
                match find_section_end(rest) {
                    Some(next) => rest = &rest[next..],
                    None => break,
                }
            }
            if certs.is_empty() {
                return Err(TlsError(format!("{name}: no certificates")));
            }
            Ok(certs)
        };

        Ok(Self {
            ca_certs: parse_certs(&ca_pem, "ca.crt")?,
            server_certs: parse_certs(&server_cert_pem, "server.crt")?,
            client_certs: parse_certs(&client_cert_pem, "client.crt")?,
            server_key: PrivateKeyDer::from_pem_slice(&server_key_pem)
                .map_err(|e| TlsError(format!("server.key: {e}")))?,
            client_key: PrivateKeyDer::from_pem_slice(&client_key_pem)
                .map_err(|e| TlsError(format!("client.key: {e}")))?,
            ca_pem,
            server_cert_pem,
            server_key_pem,
            client_cert_pem,
            client_key_pem,
            dir: dir.to_path_buf(),
        })
    }

    /// Directory the material was loaded from.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// tonic server TLS config: server identity + client verification
    /// against the local CA. Client certificates are *required* (mutual
    /// TLS — `client_auth_optional` defaults to false in tonic 0.14).
    ///
    /// # Errors
    ///
    /// Errors if the PEM bytes are not valid header values.
    pub fn tonic_server_config(&self) -> Result<ServerTlsConfig, TlsError> {
        let identity = Identity::from_pem(&self.server_cert_pem, &self.server_key_pem);
        let ca = Certificate::from_pem(&self.ca_pem);
        Ok(ServerTlsConfig::new().identity(identity).client_ca_root(ca))
    }

    /// tonic client TLS config: verify servers against the local CA and
    /// present the client identity. The verification name defaults to the
    /// dialed host (no explicit `domain_name`), so server certs must carry
    /// SANs for every name peers dial.
    ///
    /// # Errors
    ///
    /// Errors if the PEM bytes are not valid header values.
    pub fn tonic_client_config(&self) -> Result<ClientTlsConfig, TlsError> {
        let identity = Identity::from_pem(&self.client_cert_pem, &self.client_key_pem);
        let ca = Certificate::from_pem(&self.ca_pem);
        Ok(ClientTlsConfig::new()
            .ca_certificate(ca)
            .identity(identity))
    }

    /// rustls server config for axum-served TLS (the scheduler's gRPC/REST
    /// listener). Mirrors the tonic server config: server identity, client
    /// certificates required and verified against the local CA.
    ///
    /// # Errors
    ///
    /// Errors if the certificates or key are unusable.
    pub fn rustls_server_config(&self) -> Result<rustls::ServerConfig, TlsError> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in &self.ca_certs {
            roots
                .add(cert.clone())
                .map_err(|e| TlsError(format!("ca.crt: {e}")))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TlsError(format!("client verifier: {e}")))?;
        let mut config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(self.server_certs.clone(), self.server_key.clone_key())
            .map_err(|e| TlsError(format!("server identity: {e}")))?;
        // tonic clients require ALPN h2 (they fail with `H2NotNegotiated`
        // otherwise); advertise h2 + http/1.1 so REST probes work too.
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(config)
    }
}

/// Attach client TLS to a gRPC endpoint, rewriting `http://` to `https://`.
///
/// tonic's connector only applies a `ClientTlsConfig` when the endpoint URI
/// scheme is `https` (`transport/channel/service/connector.rs`: `is_https`
/// gates the TLS upgrade) — for `http://` endpoints the TLS config is
/// *silently ignored* and the dial goes out in plaintext. Ballista builds
/// several of its peer URLs with a hardcoded `http://` scheme (executor
/// registration in `executor_process.rs`, scheduler→executor task
/// assignment in `executor_manager.rs`), so every TLS endpoint override
/// must run through this helper, which rebuilds such endpoints with the
/// `https` scheme (re-applying ballista's default gRPC settings, matching
/// what `create_grpc_client_endpoint` would have applied) before
/// attaching the TLS identity. Endpoints that already carry `https` keep
/// their settings and only gain the TLS config.
///
/// # Errors
///
/// Errors if the endpoint URI lacks an authority or cannot be rebuilt.
pub fn https_tls_endpoint(
    endpoint: Endpoint,
    tls: &ClientTlsConfig,
) -> Result<Endpoint, TlsError> {
    let tls_err = |what: &str, e: tonic::transport::Error| {
        TlsError(format!("{what}: {e}"))
    };
    if endpoint.uri().scheme_str() == Some("https") {
        return endpoint
            .tls_config(tls.clone())
            .map_err(|e| tls_err("tls config", e));
    }
    let uri = endpoint.uri();
    let authority = uri
        .authority()
        .map(|a| a.as_str().to_owned())
        .ok_or_else(|| TlsError("endpoint URI has no authority".to_string()))?;
    let path = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let rebuilt = create_grpc_client_endpoint(
        format!("https://{authority}{path}"),
        Some(&GrpcClientConfig::default()),
    )
    .map_err(|e| TlsError(format!("https endpoint rebuild: {e}")))?;
    rebuilt
        .tls_config(tls.clone())
        .map_err(|e| tls_err("tls config", e))
}

/// Byte offset of the end of the current PEM section (the END marker line).
fn find_section_end(buf: &[u8]) -> Option<usize> {
    let marker = b"-----END ";
    buf.windows(marker.len())
        .position(|w| w == marker)
        .map(|pos| {
            // advance past the rest of the END line
            buf[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(buf.len(), |nl| pos + nl + 1)
        })
}

/// Install the process-wide rustls crypto provider once. Uses `aws-lc-rs`
/// (the same provider tonic's `tls-aws-lc` feature selects). If another
/// provider is already installed — e.g. a ring-based one pulled in by an
/// unrelated dependency — we keep it: the certificates we generate are
/// RSA-signed, which every supported provider verifies.
fn install_crypto_provider() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let _ = provider.install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certs_dir() -> PathBuf {
        // Acceptance-time material; regenerable via scripts/gen_certs.sh.
        // Tests skip gracefully when it is absent so `cargo test` works on
        // machines without generated certs.
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../avalanche/certs-test")
    }

    #[test]
    fn dir_from_env_unset_is_none() {
        // Edition 2024: set/remove env in tests is unsafe.
        unsafe { std::env::remove_var(TLS_DIR_ENV) };
        assert!(TlsMaterial::dir_from_env().is_none());
    }

    #[test]
    fn dir_from_env_empty_is_none() {
        unsafe { std::env::set_var(TLS_DIR_ENV, "") };
        assert!(TlsMaterial::dir_from_env().is_none());
        unsafe { std::env::remove_var(TLS_DIR_ENV) };
    }

    #[test]
    fn load_missing_dir_errors() {
        let err = TlsMaterial::load("/nonexistent/tls-dir").unwrap_err();
        assert!(err.to_string().contains("ca.crt"));
    }

    #[test]
    fn load_and_build_configs() {
        let dir = certs_dir();
        if !dir.join("ca.crt").exists() {
            eprintln!("skipping: no certs at {}", dir.display());
            return;
        }
        let material = TlsMaterial::load(&dir).expect("load");
        // Both tonic configs construct from the same material.
        material.tonic_server_config().expect("server config");
        let client = material.tonic_client_config().expect("client config");
        // The client config is cloneable (shared across concurrent dials).
        let _cloned = client.clone();
        // And the axum-side rustls server config builds too.
        material.rustls_server_config().expect("rustls server");
    }
}