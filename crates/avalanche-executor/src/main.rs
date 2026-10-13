//! Embucket-flavoured Ballista executor binary.
//!
//! Mirrors `ballista-executor`'s stock `main` but injects Embucket codec
//! and session-config overrides so warehouse stages carrying Embucket UDFs
//! can be executed and decoded.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use avalanche_executor::embucket_codecs;
use ballista_core::config::LogRotationPolicy;
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_executor::config::Config;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_executor::health::spawn_health_server;
use clap::Parser;
use tracing_subscriber::EnvFilter;

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
