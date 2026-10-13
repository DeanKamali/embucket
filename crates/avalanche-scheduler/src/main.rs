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
    let config = Arc::new(config);

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, config).await?;

    Ok(())
}
