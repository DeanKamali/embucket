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
pub mod udf_registry;

pub use codecs::{EmbucketLogicalExtensionCodec, EmbucketPhysicalExtensionCodec};
pub use storage::{register_storage_in_session, runtime_env_from_env, s3_store_from_env};
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
    let config = state
        .config()
        .clone()
        .with_ballista_logical_extension_codec(logical_codec())
        .with_ballista_physical_extension_codec(physical_codec());
    let state = SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build();
    state.upgrade_for_ballista(scheduler_url.into())
}
