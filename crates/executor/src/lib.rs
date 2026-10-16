pub use catalog;
pub mod account_ddl;
pub mod account_usage;
pub mod automation_ddl;
pub mod datafusion;
#[cfg(feature = "dedicated-executor")]
pub mod dedicated_executor;
pub mod clone_ddl;
pub mod error;
pub mod error_code;
pub mod time_travel;
pub mod undrop_ddl;
pub mod models;
pub mod query;
pub mod query_task_result;
pub mod query_types;
pub mod result_cache;
pub mod running_queries;
pub mod scheduler;
pub mod service;
pub mod session;
pub mod share_catalog;
pub mod snowflake_error;
pub mod sql_text;
pub mod tracing;
pub mod transaction;
pub mod utils;
pub mod warehouse_ddl;

#[cfg(test)]
pub mod tests;

pub use error::{Error, Result};
pub use models::{QueryResult, SessionMetadata, SessionMetadataAttr};
pub use query_types::{ExecutionStatus, QueryId};
pub use running_queries::RunningQueryId;
pub use snowflake_error::SnowflakeError;

use crate::service::ExecutionService;
use std::sync::Arc;

pub trait ExecutionAppState {
    fn get_execution_svc(&self) -> Arc<dyn ExecutionService>;
}
