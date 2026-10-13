//! Shared pieces for running Embucket plans on Ballista (Avalanche warehouses).
//!
//! This crate provides:
//! - [`FunctionMaps`]: the Embucket UDF catalog (scalar / aggregate / window),
//!   collected from the `functions` crate without a live session.
//! - [`EmbucketLogicalExtensionCodec`] / [`EmbucketPhysicalExtensionCodec`]:
//!   DataFusion proto codecs that resolve Embucket UDFs by name on remote
//!   executors and delegate all other (de)serialization to Ballista's codecs.
//! - [`upgrade_session_for_warehouse`]: client-side helper that turns a
//!   fully-customized Embucket `SessionState` into one whose plans are
//!   submitted to a warehouse's Ballista scheduler.

pub mod codecs;
pub mod storage;
pub mod tls;
pub mod udf_registry;

pub use codecs::{EmbucketLogicalExtensionCodec, EmbucketPhysicalExtensionCodec};
pub use storage::{register_storage_in_session, runtime_env_from_env, s3_store_from_env};
pub use tls::{TlsError, TlsMaterial, TLS_DIR_ENV};
pub use tls::CONTAINER_TLS_DIR;
pub use udf_registry::FunctionMaps;

use std::sync::Arc;

use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// Fresh Embucket logical codec (client encode + scheduler/executor decode).
pub fn logical_codec() -> Arc<dyn LogicalExtensionCodec> {
    Arc::new(EmbucketLogicalExtensionCodec::new())
}

/// Fresh Embucket physical codec (executor stage decode).
pub fn physical_codec() -> Arc<dyn PhysicalExtensionCodec> {
    Arc::new(EmbucketPhysicalExtensionCodec::default())
}

/// Upgrade an Embucket-customized `SessionState` so that its queries are
/// submitted as distributed Ballista jobs against `scheduler_url`.
///
/// The Embucket codecs are attached first so the Ballista planner serializes
/// and the remote cluster deserializes Embucket UDFs and expressions.
///
/// # Errors
///
/// Returns an error if the Ballista state upgrade fails.
pub fn upgrade_session_for_warehouse(
    state: SessionState,
    scheduler_url: impl Into<String>,
) -> datafusion_common::Result<SessionState> {
    let scheduler_url = scheduler_url.into();
    let mut config = state
        .config()
        .clone()
        .with_ballista_logical_extension_codec(logical_codec())
        .with_ballista_physical_extension_codec(physical_codec());
    // Mutual TLS (M7.2, design §13.2): an `https://` scheduler URL dials the
    // scheduler and executors with a client identity and verifies servers
    // against the CA in `AVALANCHE_TLS_DIR`. Query submits and task shuffle
    // reads both honor these session-config knobs (ballista-core 55).
    if scheduler_url.starts_with("https://") {
        let dir = tls::TlsMaterial::dir_from_env().ok_or_else(|| {
            datafusion_common::DataFusionError::Execution(format!(
                "warehouse scheduler URL is {scheduler_url} but {} is not set \
                 on the router (mutual TLS material required)",
                tls::TLS_DIR_ENV
            ))
        })?;
        let material = tls::TlsMaterial::load(&dir).map_err(|e| {
            datafusion_common::DataFusionError::Execution(e.to_string())
        })?;
        let client_tls = material.tonic_client_config().map_err(|e| {
            datafusion_common::DataFusionError::Execution(e.to_string())
        })?;
        let endpoint_override: ballista_core::extension::EndpointOverrideFn =
            Arc::new(move |endpoint| {
                // TLS config attaches only to `https://` endpoints (tonic's
                // connector gates on scheme); every dial from a TLS session
                // is already `https://` (submit URL and Flight partition
                // fetches both), so this is purely additive.
                endpoint
                    .tls_config(client_tls.clone())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            });
        config = config
            .with_ballista_use_tls(true)
            .with_ballista_override_create_grpc_client_endpoint(endpoint_override);
    }
    let state = SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build();
    state.upgrade_for_ballista(scheduler_url)
}
