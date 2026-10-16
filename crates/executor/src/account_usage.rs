//! ACCOUNT_USAGE-style table functions (Avalanche design §23.1, M17.1).
//!
//! Snowflake-compatible observability surface, session-local like
//! `WAREHOUSE_METERING_HISTORY` (M5): they read control-plane state that
//! never lives on warehouse compute.
//!
//! - `QUERY_HISTORY()` — persisted query records from the state store
//!   (`state_queries`).
//! - `LOGIN_HISTORY()` — login attempts recorded by the account store.
//! - `GRANTS_TO_USERS()` — user→role links.
//! - `GRANTS_TO_ROLES()` — privilege grants per role.
//!
//! The `TABLE(...)` wrapper is stripped by the executor's
//! `TableFunctionVisitor`, so bare names in a FROM clause also resolve.

use avalanche_account::store::AccountStore;
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, exec_err};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::TableType;
use state_store::Query;
use state_store::StateStore;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A batch fetcher resolved at scan time: the underlying sources (state
/// store, account store) are async, but `TableFunctionImpl::call` is sync.
type FetchBatches = Pin<Box<dyn Future<Output = DFResult<Vec<RecordBatch>>> + Send>>;

/// Snapshot table whose rows are fetched lazily at scan time and executed
/// through a `MemTable` (same shape as the sync `WAREHOUSE_METERING_HISTORY`
/// provider, but tolerant of async sources).
struct LazySnapshot {
    schema: SchemaRef,
    fetch: Arc<dyn Fn() -> FetchBatches + Send + Sync>,
}

impl std::fmt::Debug for LazySnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazySnapshot")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl LazySnapshot {
    fn new(schema: Schema, fetch: impl Fn() -> FetchBatches + Send + Sync + 'static) -> Self {
        Self {
            schema: Arc::new(schema),
            fetch: Arc::new(fetch),
        }
    }
}

#[async_trait]
impl TableProvider for LazySnapshot {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batches = (self.fetch)().await?;
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![batches])?;
        table.scan(state, projection, filters, limit).await
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// chrono `DateTime` epoch-ms → nanoseconds (none → null).
fn ms_to_nanos(ms: Option<u64>) -> Option<i64> {
    ms.map(|ms| (ms as i128 * 1_000_000) as i64)
}

fn ts_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
}

fn str_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

// ---------------------------------------------------------------------------
// QUERY_HISTORY
// ---------------------------------------------------------------------------

/// `QUERY_HISTORY()` — persisted query records from the state store.
#[derive(Clone)]
pub struct QueryHistoryTableFunc {
    store: Arc<dyn StateStore>,
}

impl std::fmt::Debug for QueryHistoryTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueryHistoryTableFunc")
    }
}

impl QueryHistoryTableFunc {
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self { store }
    }

    /// Snowflake ACCOUNT_USAGE.QUERY_HISTORY column subset (lowercase, per
    /// the M5 convention so unquoted refs resolve).
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            str_field("query_id"),
            str_field("query_text"),
            str_field("database_name"),
            str_field("schema_name"),
            str_field("user_name"),
            str_field("role_name"),
            str_field("warehouse_name"),
            str_field("execution_status"),
            str_field("error_code"),
            str_field("error_message"),
            ts_field("start_time"),
            ts_field("end_time"),
            Field::new("total_elapsed_time", DataType::Int64, true),
            Field::new("bytes_scanned", DataType::Int64, true),
            Field::new("rows_produced", DataType::Int64, true),
            str_field("session_id"),
        ])
    }

    fn batch(queries: Vec<Query>) -> DFResult<RecordBatch> {
        let schema = Arc::new(Self::schema());
        fn opt_ref(
            pick: impl Fn(&Query) -> Option<String>,
            queries: &[Query],
        ) -> ArrayRef {
            Arc::new(StringArray::from(
                queries.iter().map(|q| pick(q)).collect::<Vec<_>>(),
            ))
        }
        let ms = |dt: &chrono::DateTime<chrono::Utc>| dt.timestamp_millis().max(0) as u64;
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    queries.iter().map(|q| q.query_id.to_string()).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    queries.iter().map(|q| q.query_text.clone()).collect::<Vec<_>>(),
                )) as ArrayRef,
                opt_ref(|q| q.database_name.clone(), &queries),
                opt_ref(|q| q.schema_name.clone(), &queries),
                opt_ref(|q| q.user_name.clone(), &queries),
                opt_ref(|q| q.role_name.clone(), &queries),
                opt_ref(|q| q.warehouse_name.clone(), &queries),
                opt_ref(
                    |q| q.execution_status.as_ref().map(std::string::ToString::to_string),
                    &queries,
                ),
                opt_ref(|q| q.error_code.clone(), &queries),
                opt_ref(|q| q.error_message.clone(), &queries),
                Arc::new(TimestampNanosecondArray::from(
                    queries
                        .iter()
                        .map(|q| ms_to_nanos(Some(ms(&q.start_time))))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(
                    queries
                        .iter()
                        .map(|q| ms_to_nanos(q.end_time.as_ref().map(|t| ms(t))))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    queries.iter().map(|q| q.total_elapsed_time.map(|v| v as i64)).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    queries.iter().map(|q| q.bytes_scanned.map(|v| v as i64)).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    queries.iter().map(|q| q.rows_produced.map(|v| v as i64)).collect::<Vec<_>>(),
                )) as ArrayRef,
                opt_ref(|q| Some(q.session_id.clone()), &queries),
            ],
        )?;
        Ok(batch)
    }
}

impl TableFunctionImpl for QueryHistoryTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!(
                "QUERY_HISTORY: no arguments are supported yet; call it with no arguments"
            );
        }
        let store = Arc::clone(&self.store);
        let schema = Self::schema();
        Ok(Arc::new(LazySnapshot::new(schema, move || {
            let store = Arc::clone(&store);
            Box::pin(async move {
                let queries = store
                    .list_queries()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                // Newest first (Snowflake returns most-recent-first).
                let mut queries = queries;
                queries.sort_by(|a, b| b.start_time.cmp(&a.start_time));
                Ok(vec![Self::batch(queries)?])
            })
        })))
    }
}

// ---------------------------------------------------------------------------
// LOGIN_HISTORY
// ---------------------------------------------------------------------------

/// `LOGIN_HISTORY()` — login attempts recorded by the account store.
#[derive(Clone)]
pub struct LoginHistoryTableFunc {
    account: Arc<dyn AccountStore>,
}

impl std::fmt::Debug for LoginHistoryTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LoginHistoryTableFunc")
    }
}

impl LoginHistoryTableFunc {
    pub fn new(account: Arc<dyn AccountStore>) -> Self {
        Self { account }
    }

    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            ts_field("event_timestamp"),
            str_field("user_name"),
            str_field("client_app_id"),
            str_field("client_ip"),
            Field::new("success", DataType::Boolean, false),
        ])
    }
}

impl TableFunctionImpl for LoginHistoryTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!(
                "LOGIN_HISTORY: no arguments are supported yet; call it with no arguments"
            );
        }
        let account = Arc::clone(&self.account);
        let schema = Self::schema();
        Ok(Arc::new(LazySnapshot::new(schema, move || {
            let account = Arc::clone(&account);
            Box::pin(async move {
                let events = account
                    .list_logins()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let schema = Arc::new(LoginHistoryTableFunc::schema());
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampNanosecondArray::from(
                            events
                                .iter()
                                .map(|e| ms_to_nanos(Some(e.timestamp_ms)))
                                .collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            events.iter().map(|e| e.user_name.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            events.iter().map(|e| e.client_app_id.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            events.iter().map(|e| e.client_ip.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(BooleanArray::from(
                            events.iter().map(|e| e.success).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ],
                )?;
                Ok(vec![batch])
            })
        })))
    }
}

// ---------------------------------------------------------------------------
// GRANTS_TO_USERS / GRANTS_TO_ROLES
// ---------------------------------------------------------------------------

/// `GRANTS_TO_USERS()` — user→role links (`GRANT ROLE … TO USER …`).
#[derive(Clone)]
pub struct GrantsToUsersTableFunc {
    account: Arc<dyn AccountStore>,
}

impl std::fmt::Debug for GrantsToUsersTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GrantsToUsersTableFunc")
    }
}

impl GrantsToUsersTableFunc {
    pub fn new(account: Arc<dyn AccountStore>) -> Self {
        Self { account }
    }

    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![str_field("grantee_name"), str_field("granted_role")])
    }
}

impl TableFunctionImpl for GrantsToUsersTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!("GRANTS_TO_USERS: no arguments are supported");
        }
        let account = Arc::clone(&self.account);
        let schema = GrantsToUsersTableFunc::schema();
        Ok(Arc::new(LazySnapshot::new(schema, move || {
            let account = Arc::clone(&account);
            Box::pin(async move {
                let users = account
                    .list_users()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let mut rows: Vec<(String, String)> = Vec::new();
                for user in &users {
                    for role in account
                        .user_roles(&user.name)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                    {
                        rows.push((user.name.clone(), role));
                    }
                }
                let schema = Arc::new(GrantsToUsersTableFunc::schema());
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(
                            rows.iter().map(|(u, _)| u.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            rows.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ],
                )?;
                Ok(vec![batch])
            })
        })))
    }
}

/// `GRANTS_TO_ROLES()` — privilege grants per role.
#[derive(Clone)]
pub struct GrantsToRolesTableFunc {
    account: Arc<dyn AccountStore>,
}

impl std::fmt::Debug for GrantsToRolesTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GrantsToRolesTableFunc")
    }
}

impl GrantsToRolesTableFunc {
    pub fn new(account: Arc<dyn AccountStore>) -> Self {
        Self { account }
    }

    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            str_field("grantee_name"),
            str_field("privilege"),
            str_field("granted_on"),
            str_field("name"),
        ])
    }
}

impl TableFunctionImpl for GrantsToRolesTableFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return exec_err!("GRANTS_TO_ROLES: no arguments are supported");
        }
        let account = Arc::clone(&self.account);
        let schema = GrantsToRolesTableFunc::schema();
        Ok(Arc::new(LazySnapshot::new(schema, move || {
            let account = Arc::clone(&account);
            Box::pin(async move {
                let roles = account
                    .list_roles()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let mut rows: Vec<(String, String, String, String)> = Vec::new();
                for role in &roles {
                    for grant in account
                        .role_grants(&role.name)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                    {
                        rows.push((
                            role.name.clone(),
                            grant.privilege.to_string(),
                            grant.object.kind().to_string(),
                            grant.object.to_string(),
                        ));
                    }
                }
                let schema = Arc::new(GrantsToRolesTableFunc::schema());
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(
                            rows.iter().map(|(g, ..)| g.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            rows.iter().map(|(_, p, ..)| p.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            rows.iter().map(|(_, _, k, _)| k.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                        Arc::new(StringArray::from(
                            rows.iter().map(|(_, _, _, n)| n.clone()).collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ],
                )?;
                Ok(vec![batch])
            })
        })))
    }
}

/// Convenience: epoch-ms now, for login-event producers.
#[must_use]
pub fn current_epoch_ms() -> u64 {
    now_ms()
}