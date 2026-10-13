//! Embucket-flavoured Ballista executor binary.
//!
//! Mirrors `ballista-executor`'s stock `main` but injects Embucket codec
//! and session-config overrides so warehouse stages carrying Embucket UDFs
//! can be executed and decoded.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use avalanche_executor::embucket_codecs;
use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_core::config::LogRotationPolicy;
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::extension::{EndpointOverrideFn, SessionConfigExt};
use ballista_executor::config::Config;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_executor::flight_service::BallistaFlightService;
use ballista_executor::health::spawn_health_server;
use clap::Parser;
use datafusion::prelude::SessionConfig;
use tracing_subscriber::EnvFilter;

/// gRPC message sizes for the TLS Arrow Flight server (matches the
/// executor's stock `grpc_max_*` defaults; the
/// `override_arrow_flight_service` hook does not receive them).
const TLS_FLIGHT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[tokio::main]
async fn main() -> BallistaResult<()> {
    // parse command-line arguments
    let opt = Config::parse();
    let bind_host = opt.bind_host.clone();
    let bind_health_port = opt.bind_health_port;

    let mut config: ExecutorProcessConfig = opt.try_into()?;
    let (logical_codec, physical_codec) = embucket_codecs();
    config.override_logical_codec = Some(logical_codec);
    config.override_physical_codec = Some(physical_codec);
    config.override_config_producer = Some(Arc::new(avalanche_executor::plain_session_config));
    // Per-warehouse storage credentials (M7.1): register the warehouse's
    // scoped object store from container env in every task runtime env.
    let storage_work_dir = config.work_dir.clone().unwrap_or_else(|| "/tmp".into());
    config.override_runtime_producer = Some(Arc::new(move |_config: &SessionConfig| {
        let env = avalanche_executor_common::runtime_env_from_env(&storage_work_dir)?;
        Ok(Arc::new(env))
    }));

    // Mutual TLS (M7.2, design §13.2): when `AVALANCHE_TLS_DIR` is set, the
    // Arrow Flight listener serves TLS with *required* client certificates,
    // executor→scheduler connections (registration, poll loop, heartbeats)
    // and task-level shuffle fetches present the client identity. Without
    // the env the process stays fully plaintext.
    let mut tls_dir_display_opt: Option<String> = None;
    if let Some(dir) = avalanche_executor_common::TlsMaterial::dir_from_env() {
        let tls = avalanche_executor_common::TlsMaterial::load(&dir)
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        tls_dir_display_opt = Some(dir.display().to_string());

        let max_message_size = TLS_FLIGHT_MAX_MESSAGE_SIZE;
        let server_tls = tls
            .tonic_server_config()
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        config.override_arrow_flight_service =
            Some(Arc::new(move |work_dir, addr, mut shutdown, _| {
                let server_tls = server_tls.clone();
                tokio::spawn(async move {
                    tonic::transport::Server::builder()
                        .tls_config(server_tls)
                        .map_err(|e| BallistaError::General(format!("flight TLS: {e}")))?
                        .add_service(
                            FlightServiceServer::new(BallistaFlightService::new(work_dir))
                                .max_decoding_message_size(max_message_size)
                                .max_encoding_message_size(max_message_size),
                        )
                        .serve_with_shutdown(addr, shutdown.recv())
                        .await
                        .map_err(BallistaError::TonicError)
                })
            }));

        // Executor→scheduler client (registration + poll loop + heartbeats).
        let client_tls = tls
            .tonic_client_config()
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        config.override_create_grpc_client_endpoint =
            Some(Arc::new(move |endpoint| endpoint_with_tls(endpoint, &client_tls)));

        // Task sessions dial peer executors' TLS Flight endpoints to fetch
        // shuffle partitions.
        let task_client_tls = tls
            .tonic_client_config()
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        config.override_config_producer = Some(Arc::new(move || {
            let task_client_tls = task_client_tls.clone();
            let endpoint_override: EndpointOverrideFn =
                Arc::new(move |endpoint| endpoint_with_tls(endpoint, &task_client_tls));
            avalanche_executor::plain_session_config()
                .with_ballista_use_tls(true)
                .with_ballista_override_create_grpc_client_endpoint(endpoint_override)
        }));
    }

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter =
        EnvFilter::new(rust_log.unwrap_or(config.special_mod_log_level.clone()));

    let tracing = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(config.print_thread_info)
        .with_thread_ids(config.print_thread_info)
        .with_env_filter(log_filter);

    // File layer
    if let Some(log_dir) = &config.log_dir {
        let log_file = match config.log_rotation_policy {
            LogRotationPolicy::Minutely => {
                tracing_appender::rolling::minutely(log_dir, config.log_file_name_prefix())
            }
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, config.log_file_name_prefix())
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, config.log_file_name_prefix())
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, config.log_file_name_prefix())
            }
        };

        tracing.with_writer(log_file).init();
    } else {
        tracing.init();
    }

    // Logged after tracing init so acceptance tests can assert the mode.
    if let Some(tls_dir_display) = tls_dir_display_opt {
        tracing::info!("mTLS enabled from {}", tls_dir_display);
    }

    // Kubernetes-style health probes are a concern of the standalone binary,
    // not of the library, so wire them here on top of the config's shared
    // ExecutorHealth handle. `/healthz` is process-liveness, `/readyz`
    // reflects heartbeat state (see `ballista_executor::health`).
    let health = config.health.clone();
    let health_addr: SocketAddr = format!("{bind_host}:{bind_health_port}")
        .parse()
        .map_err(|e| {
            BallistaError::General(format!("invalid --bind-health-port address: {e}"))
        })?;
    let (health_shutdown_tx, health_shutdown_rx) = tokio::sync::oneshot::channel();
    let health_handle = spawn_health_server(health_addr, health, health_shutdown_rx);

    let result = start_executor_process(Arc::new(config)).await;

    // Ask the health server to shut down so the process can exit cleanly.
    let _ = health_shutdown_tx.send(());
    if let Err(join_err) = health_handle.await {
        tracing::warn!("health server task join error: {join_err}");
    }
    result
}

/// Attach the shared client identity to a gRPC endpoint (M7.2). Used by the
/// executor→scheduler client and by task shuffle-reader sessions.
fn endpoint_with_tls(
    endpoint: tonic::transport::Endpoint,
    tls: &tonic::transport::ClientTlsConfig,
) -> Result<tonic::transport::Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    avalanche_executor_common::tls::https_tls_endpoint(endpoint, tls)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}
