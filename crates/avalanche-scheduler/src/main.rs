//! Embucket-flavoured Ballista scheduler binary.
//!
//! Mirrors `ballista-scheduler`'s stock `main` but injects Embucket codec
//! and session-config overrides so warehouse plans carrying Embucket UDFs
//! can be scheduled and decoded.

use std::env;
use std::io;
use std::sync::Arc;

use avalanche_scheduler::embucket_codecs;
use ballista_core::config::LogRotationPolicy;
use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{Config, SchedulerConfig};
use ballista_scheduler::scheduler_process::start_server;
use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> BallistaResult<()> {
    let runtime = build_runtime()?;
    runtime.block_on(inner())
}

fn build_runtime() -> BallistaResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_stack_size(32 * 1024 * 1024) // 32MB
        .build()
        .map_err(|e| {
            BallistaError::Configuration(format!("failed to build tokio runtime: {e}"))
        })
}

async fn inner() -> BallistaResult<()> {
    // parse options
    let opt = Config::parse();

    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or(opt.log_level_setting.clone()));

    let tracing = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_thread_names(opt.print_thread_info)
        .with_thread_ids(opt.print_thread_info)
        .with_writer(io::stdout)
        .with_env_filter(log_filter);

    // File layer
    if let Some(log_dir) = &opt.log_dir {
        let log_file_name_prefix = format!(
            "scheduler_{}_{}_{}",
            opt.namespace, opt.external_host, opt.bind_port
        );

        let log_file = match opt.log_rotation_policy {
            LogRotationPolicy::Minutely => {
                tracing_appender::rolling::minutely(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Hourly => {
                tracing_appender::rolling::hourly(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Daily => {
                tracing_appender::rolling::daily(log_dir, &log_file_name_prefix)
            }
            LogRotationPolicy::Never => {
                tracing_appender::rolling::never(log_dir, &log_file_name_prefix)
            }
        };

        tracing.with_writer(log_file).init();
    } else {
        tracing.init();
    }

    let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
    let addr = addr.parse().map_err(|e: std::net::AddrParseError| {
        BallistaError::Configuration(e.to_string())
    })?;

    let (logical_codec, physical_codec) = embucket_codecs();
    let mut config: SchedulerConfig = opt.try_into()?;
    config.override_logical_codec = Some(logical_codec);
    config.override_physical_codec = Some(physical_codec);
    config.override_config_producer =
        Some(Arc::new(avalanche_scheduler::plain_session_config));
    // Per-warehouse storage credentials (M7.1): sessions the scheduler builds
    // must resolve the warehouse's scoped object store from container env.
    config.override_session_builder = Some(Arc::new(|session_config| {
        let state = ballista_core::utils::default_session_builder(session_config)?;
        avalanche_executor_common::register_storage_in_session(state)
    }));

    // Scheduler HA (M7.3, design §13.3): when `AVALANCHE_CLUSTER_PG_URL` is
    // set, executor/job state lives in shared Postgres tables and several
    // schedulers can serve one warehouse behind a common network alias.
    // Without the env the process keeps ballista's in-memory backend.
    let cluster = match env::var("AVALANCHE_CLUSTER_PG_URL") {
        Ok(url) if !url.trim().is_empty() => {
            let session_builder = config
                .override_session_builder
                .clone()
                .expect("session builder override set above");
            let config_producer = config
                .override_config_producer
                .clone()
                .expect("config producer override set above");
            let cluster = avalanche_cluster_store::PgClusterBuilder::new(
                url,
                config.scheduler_id.clone(),
            )
            .session_builder(session_builder)
            .config_producer(config_producer)
            .build()
            .await?;
            tracing::info!("cluster state: postgres (HA replicas)");
            cluster
        }
        _ => BallistaCluster::new_from_config(&config).await?,
    };

    // Mutual TLS (M7.2, design §13.2): when `AVALANCHE_TLS_DIR` is set, the
    // gRPC/REST listener serves TLS with *required* client certificates and
    // the Flight proxy's connections to executors present the client
    // identity. Without the env the process stays fully plaintext.
    if let Some(dir) = avalanche_executor_common::TlsMaterial::dir_from_env() {
        let tls = avalanche_executor_common::TlsMaterial::load(&dir)
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        tracing::info!("mTLS enabled from {}", dir.display());
        config.use_tls = true;
        let client_tls = tls
            .tonic_client_config()
            .map_err(|e| BallistaError::Configuration(e.to_string()))?;
        config.override_create_grpc_client_endpoint = Some(Arc::new(move |endpoint| {
            // Attach TLS config as-is: tonic applies it only to `https://`
            // endpoints. That is exactly the split ballista intends — the
            // scheduler's outbound dials are `https://` for executor Flight
            // fetches (TLS) and `http://` for the executor's internal task
            // gRPC (port 50052, which ballista serves plaintext and does not
            // let us TLS-serve — see `executor_server.rs`).
            endpoint
                .tls_config(client_tls.clone())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }));

        let config = Arc::new(config);
        avalanche_scheduler::tls::start_server_tls(cluster, addr, config, &tls).await?;
        return Ok(());
    }

    let config = Arc::new(config);
    start_server(cluster, addr, config).await?;

    Ok(())
}
