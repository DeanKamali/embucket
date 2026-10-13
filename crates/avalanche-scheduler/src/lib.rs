//! Embucket overrides for the Ballista scheduler binary.
//!
//! The binary in `main.rs` reuses Ballista's stock CLI parsing but injects
//! Embucket-specific codecs and session configuration so that distributed
//! plans containing Embucket UDFs can be scheduled and decoded.

use std::sync::Arc;

use avalanche_executor_common::{logical_codec, physical_codec};
use datafusion::execution::context::SessionConfig;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// Embucket codec overrides for the scheduler.
///
/// The logical codec is used when the scheduler (re)serializes logical
/// plans; the physical codec when it inspects stage plans. Both delegate
/// everything except Embucket UDFs to Ballista's stock codecs.
///
/// # Panics
///
/// Never panics; the test asserts construction stays that way.
pub fn embucket_codecs() -> (
    Arc<dyn LogicalExtensionCodec>,
    Arc<dyn PhysicalExtensionCodec>,
) {
    (logical_codec(), physical_codec())
}

/// Session config producer for scheduler-created sessions.
///
/// Plain `SessionConfig::new()` — warehouses in v1 use `file://` data, so
/// no S3-specific config support is needed.
///
/// # Panics
///
/// Never panics; the test asserts construction stays that way.
pub fn plain_session_config() -> SessionConfig {
    SessionConfig::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embucket_codecs_construct_without_panic() {
        let (logical, physical) = embucket_codecs();
        // The traits are the contract: both Arcs must be alive and usable.
        assert!(Arc::strong_count(&logical) >= 1);
        assert!(Arc::strong_count(&physical) >= 1);
    }

    #[test]
    fn plain_session_config_constructs_without_panic() {
        let config = plain_session_config();
        assert_eq!(config.information_schema(), false);
    }
}
