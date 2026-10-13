pub mod config;
pub mod error;
pub mod models;
pub mod state_store_memory;
pub mod state_store_pg;

pub use config::PgConfig;
pub use error::{Error, Result};
pub use models::{
    ExecutionStatus, Query, QueryMetric, SessionRecord, StorageCredentials, Variable, ViewRecord,
    WarehouseMeteringRecord, WarehouseRecord,
};
pub use state_store_memory::InMemoryStateStore;
pub use state_store_pg::PostgresStateStore;

#[mockall::automock]
#[async_trait::async_trait]
pub trait StateStore: Send + Sync {
    async fn put_new_session(&self, session_id: &str) -> Result<()>;
    async fn put_session(&self, session: SessionRecord) -> Result<()>;
    async fn get_session(&self, session_id: &str) -> Result<SessionRecord>;
    async fn delete_session(&self, session_id: &str) -> Result<()>;
    async fn update_session(&self, session: SessionRecord) -> Result<()>;
    async fn put_query(&self, query: &Query) -> Result<()>;
    async fn get_query(&self, query_id: &str) -> Result<Query>;
    async fn get_query_by_request_id(&self, request_id: &str) -> Result<Query>;
    async fn get_queries_by_session_id(&self, session_id: &str) -> Result<Vec<Query>>;
    async fn delete_query(&self, query_id: &str) -> Result<()>;
    async fn update_query(&self, query: &Query) -> Result<()>;

    async fn put_warehouse(&self, warehouse: WarehouseRecord) -> Result<()>;
    async fn get_warehouse(&self, name: &str) -> Result<WarehouseRecord>;
    async fn list_warehouses(&self) -> Result<Vec<WarehouseRecord>>;
    async fn delete_warehouse(&self, name: &str) -> Result<()>;

    /// Store one metering bucket (full replace of the bucket).
    async fn put_metering(&self, record: WarehouseMeteringRecord) -> Result<()>;
    /// List metering buckets, optionally filtered by warehouse, ordered by
    /// start time.
    async fn list_metering<'a>(
        &self,
        warehouse: Option<&'a str>,
    ) -> Result<Vec<WarehouseMeteringRecord>>;
}
