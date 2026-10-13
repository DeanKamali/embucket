use bytes::{Buf, Bytes};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::csv::reader::Format;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::runtime::set_join_set_tracer;
use datafusion::datasource::memory::MemTable;
use datafusion::execution::DiskManager;
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_common::TableReference;
use snafu::ResultExt;
#[cfg(feature = "state-store-query")]
use state_store::ExecutionStatus;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::vec;
use std::{collections::HashMap, sync::Arc};
use time::{Duration as DateTimeDuration, OffsetDateTime};
use tokio::task;
use tokio_util::sync::CancellationToken;

use super::error::{self as ex_error, Result};
#[cfg(feature = "state-store")]
use super::models::SessionMetadataAttr;
use super::models::{QueryContext, QueryResult, SessionMetadata};
use super::running_queries::{RunningQueries, RunningQueriesRegistry, RunningQuery};
use super::session::UserSession;
use crate::query_task_result::ExecutionTaskResult;
use crate::query_types::QueryId;
use crate::running_queries::RunningQueryId;
use crate::session::{SESSION_INACTIVITY_EXPIRATION_SECONDS, to_unix};
use crate::tracing::SpanTracer;
use crate::utils::{Config, MemPoolType};
use avalanche_warehouse::{
    BallistaImages, ContainerDriver, DockerCliDriver, FakeDriver, KubectlDriver, WarehouseManager,
};
use catalog::catalog_list::EmbucketCatalogList;
use catalog_metastore::{InMemoryMetastore, Metastore, TableIdent as MetastoreTableIdent};
use state_store::StateStore;
#[cfg(feature = "state-store")]
use state_store::{PostgresStateStore, models::Query};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::Instrument;
use uuid::Uuid;

pub const TIMEOUT_SIGNAL_INTERVAL_SECONDS: u64 = 60;

pub const TIMEOUT_DISCARD_INTERVAL_SECONDS: u64 = 60;

#[async_trait::async_trait]
pub trait ExecutionService: Send + Sync {
    /// Creates (or returns an existing) session.
    ///
    /// When the `state-store` feature is enabled, the session record is
    /// persisted with its login context and inactivity expiry so it can be
    /// rehydrated by another process after a restart/failover.
    async fn create_session(
        &self,
        session_id: &str,
        metadata: Option<SessionMetadata>,
    ) -> Result<Arc<UserSession>>;
    async fn update_session_expiry(&self, session_id: &str) -> Result<bool>;
    async fn delete_expired_sessions(&self) -> Result<()>;
    async fn get_session(&self, session_id: &str) -> Result<Arc<UserSession>>;
    async fn session_exists(&self, session_id: &str) -> bool;
    async fn delete_session(&self, session_id: &str) -> Result<()>;
    fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>>;

    /// Locates a query by `running_query_id`.
    ///
    /// # Arguments
    ///
    /// * `running_query_id` - The running query id.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryId`. The `Ok` variant contains the query id,
    /// and the `Err` variant contains an `Error`.
    fn locate_query_id(&self, running_query_id: RunningQueryId) -> Result<QueryId>;

    /// Aborts a query by `query_id`.
    ///
    /// # Arguments
    ///
    /// * `query_id` - The query to abort.
    ///
    /// # Returns
    ///
    /// A `Result` of type `()`. The `Ok` variant contains an empty tuple,
    /// and the `Err` variant contains an `Error`.
    async fn abort(&self, query_id: QueryId) -> Result<()>;

    /// Submits a query to be executed asynchronously. Query result can be consumed with
    /// `wait`.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The ID of the user session.
    /// * `query` - The SQL query to be executed.
    /// * `query_context` - The context of the query execution.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryId`. The `Ok` variant contains the query id,
    /// to be used with `wait`. The `Err` variant contains submission `Error`.
    async fn submit(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryId>;

    /// Wait while sabmitted query finished, it returns query result or real context rich error
    /// # Arguments
    ///
    /// * `query_id` - The id of the submitted query.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryResult`. The `Ok` variant contains the query result,
    /// and the `Err` variant contains a real context rich error.
    async fn wait(&self, query_id: QueryId) -> Result<QueryResult>;

    /// Synchronously executes a query and returns the result.
    /// It is a wrapper around `submit` and `wait`.
    ///
    /// # Arguments
    ///
    /// * `session_id` - The ID of the user session.
    /// * `query` - The SQL query to be executed.
    /// * `query_context` - The context of the query execution.
    ///
    /// # Returns
    ///
    /// A `Result` of type `QueryResult`. The `Ok` variant contains the query result,
    /// and the `Err` variant contains a real context rich error.
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryResult>;

    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> Result<usize>;

    async fn timeout_signal(&self, interval: Duration, idle_timeout: Duration) -> ();
}

pub struct CoreExecutionService {
    metastore: Arc<dyn Metastore>,
    df_sessions: Arc<RwLock<HashMap<String, Arc<UserSession>>>>,
    config: Arc<Config>,
    catalog_list: Arc<EmbucketCatalogList>,
    runtime_env: Arc<RuntimeEnv>,
    queries: Arc<RunningQueriesRegistry>,
    /// Concurrency queue (M6): queries above `max_concurrency_level` wait
    /// here up to `queue_timeout_secs` instead of being rejected.
    queue: Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "state-store")]
    state_store: Arc<dyn StateStore>,
    /// Shared warehouse manager (design §5); one per process.
    warehouse: Arc<WarehouseManager>,
}

/// Manager with the real Docker driver (production).
fn build_warehouse_manager(
    state_store: Arc<dyn StateStore>,
    network: Option<String>,
) -> Arc<WarehouseManager> {
    // `WAREHOUSE_DRIVER=k8s` runs warehouse compute as k8s pods (kubectl);
    // default is the local Docker CLI driver.
    let k8s = std::env::var("WAREHOUSE_DRIVER").is_ok_and(|v| v.eq_ignore_ascii_case("k8s"));
    let driver: Arc<dyn ContainerDriver> = if k8s {
        Arc::new(KubectlDriver::from_env())
    } else {
        Arc::new(DockerCliDriver::default())
    };
    Arc::new(WarehouseManager::new(
        state_store,
        driver,
        BallistaImages::from_env(),
        network,
    ))
}

/// Manager with the in-memory driver (tests): no Docker required.
pub fn build_test_warehouse_manager(state_store: Arc<dyn StateStore>) -> Arc<WarehouseManager> {
    Arc::new(WarehouseManager::new(
        state_store,
        Arc::new(FakeDriver::new()),
        BallistaImages::default(),
        None,
    ))
}

/// Concurrency queue sized to `max_concurrency_level` permits (M6).
fn queue_semaphore(config: &Config) -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(config.max_concurrency_level.max(1)))
}

impl CoreExecutionService {
    #[cfg(feature = "state-store")]
    pub async fn new_test_executor(
        metastore: Arc<dyn Metastore>,
        state_store: Arc<dyn StateStore>,
        config: Arc<Config>,
    ) -> Result<Self> {
        Self::initialize_datafusion_tracer();

        let catalog_list = Self::catalog_list(metastore.clone(), &config).await?;
        let runtime_env = Self::runtime_env(&config, catalog_list.clone())?;
        Ok(Self {
            queue: queue_semaphore(&config),
            metastore,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            catalog_list,
            runtime_env,
            queries: Arc::new(RunningQueriesRegistry::new()),
            warehouse: build_test_warehouse_manager(state_store.clone()),
            state_store,
        })
    }

    #[tracing::instrument(
        name = "CoreExecutionService::new",
        level = "debug",
        skip(metastore, config),
        err
    )]
    pub async fn new(metastore: Arc<dyn Metastore>, config: Arc<Config>) -> Result<Self> {
        Self::initialize_datafusion_tracer();

        let catalog_list = Self::catalog_list(metastore.clone(), &config).await?;
        let runtime_env = Self::runtime_env(&config, catalog_list.clone())?;
        #[cfg(feature = "state-store")]
        let state_store = Arc::new(
            PostgresStateStore::new_from_env()
                .await
                .context(ex_error::StateStoreSnafu)?,
        ) as Arc<dyn StateStore>;
        #[cfg(feature = "state-store")]
        let warehouse =
            build_warehouse_manager(state_store.clone(), config.warehouse_network.clone());
        #[cfg(not(feature = "state-store"))]
        let warehouse =
            build_warehouse_manager(Arc::new(state_store::InMemoryStateStore::new()), None);
        // auto-suspend idle warehouses (design §5)
        warehouse.clone().spawn_auto_suspend(60);
        // metering history from prior runs (M5); logs and continues on failure
        warehouse.load_metering().await;
        Ok(Self {
            queue: queue_semaphore(&config),
            metastore,
            df_sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            catalog_list,
            runtime_env,
            queries: Arc::new(RunningQueriesRegistry::new()),
            warehouse,
            #[cfg(feature = "state-store")]
            state_store,
        })
    }

    #[tracing::instrument(
        name = "CoreExecutionService::catalog_list",
        level = "debug",
        skip(metastore),
        err
    )]
    pub async fn catalog_list(
        metastore: Arc<dyn Metastore>,
        config: &Config,
    ) -> Result<Arc<EmbucketCatalogList>> {
        let catalog_list = Arc::new(EmbucketCatalogList::new(metastore.clone(), config.into()));
        catalog_list
            .register_catalogs()
            .await
            .context(ex_error::RegisterCatalogSnafu)?;
        catalog_list
            .refresh()
            .await
            .context(ex_error::RefreshCatalogListSnafu)?;
        Ok(catalog_list)
    }

    #[allow(clippy::unwrap_used, clippy::as_conversions)]
    pub fn runtime_env(
        config: &Config,
        catalog_list: Arc<EmbucketCatalogList>,
    ) -> Result<Arc<RuntimeEnv>> {
        let mut rt_builder = RuntimeEnvBuilder::new().with_object_store_registry(catalog_list);

        if let Some(memory_limit_mb) = config.mem_pool_size_mb {
            const NUM_TRACKED_CONSUMERS: usize = 5;

            // set memory pool type
            let memory_limit = memory_limit_mb * 1024 * 1024;
            let enable_track = config.mem_enable_track_consumers_pool.unwrap_or(false);

            let memory_pool: Arc<dyn MemoryPool> = match config.mem_pool_type {
                MemPoolType::Fair => {
                    let pool = FairSpillPool::new(memory_limit);
                    if enable_track {
                        Arc::new(TrackConsumersPool::new(
                            pool,
                            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                        ))
                    } else {
                        Arc::new(FairSpillPool::new(memory_limit))
                    }
                }
                MemPoolType::Greedy => {
                    let pool = GreedyMemoryPool::new(memory_limit);
                    if enable_track {
                        Arc::new(TrackConsumersPool::new(
                            pool,
                            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                        ))
                    } else {
                        Arc::new(GreedyMemoryPool::new(memory_limit))
                    }
                }
            };
            rt_builder = rt_builder.with_memory_pool(memory_pool);
        }

        // set disk limit
        if let Some(disk_limit) = config.disk_pool_size_mb {
            let disk_limit_bytes = (disk_limit as u64) * 1024 * 1024;
            let disk_builder = DiskManager::builder()
                .with_mode(DiskManagerMode::OsTmpDirectory)
                .with_max_temp_directory_size(disk_limit_bytes);
            rt_builder = rt_builder.with_disk_manager_builder(disk_builder);
        }

        rt_builder.build_arc().context(ex_error::DataFusionSnafu)
    }

    fn initialize_datafusion_tracer() {
        let _ = set_join_set_tracer(&SpanTracer);
    }

    /// Persists a newly created session so it can be rehydrated on any
    /// process after a restart/failover (Avalanche design §4).
    #[cfg(feature = "state-store")]
    async fn persist_new_session(&self, session: &UserSession) -> state_store::Result<()> {
        // Preserve views already stored for this session (v1 sessions are
        // created once per id, but be defensive against recreate).
        let record = match self.state_store.get_session(&session.session_id).await {
            Ok(record) => record,
            Err(state_store::Error::NotFound) => {
                state_store::SessionRecord::new(&session.session_id)
            }
            Err(err) => return Err(err),
        };
        let record = state_store::SessionRecord {
            user_name: session.metadata.attr(SessionMetadataAttr::UserName),
            metadata: session.metadata.clone().into_map(),
            attrs: session.attrs_snapshot(),
            expiry: u64::try_from(session.expiry.load(Ordering::Relaxed)).ok(),
            ..record
        };
        self.state_store.put_session(record).await
    }

    /// Refreshes the persisted inactivity expiry of a session.
    #[cfg(feature = "state-store")]
    async fn touch_persisted_session(&self, session_id: &str) -> state_store::Result<()> {
        if let Ok(mut record) = self.state_store.get_session(session_id).await {
            record.expiry = u64::try_from(to_unix(
                OffsetDateTime::now_utc()
                    + DateTimeDuration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS),
            ))
            .ok();
            self.state_store.update_session(record).await?;
        }
        Ok(())
    }

    /// Recreates a `UserSession` from its persisted record, restoring session
    /// parameters and login context. Returns `Ok(false)` when no live record
    /// exists; stale (expired) records are cleaned up.
    #[cfg(feature = "state-store")]
    async fn rehydrate_session(&self, session_id: &str) -> Result<bool> {
        let record = match self.state_store.get_session(session_id).await {
            Ok(record) => record,
            Err(state_store::Error::NotFound) => return Ok(false),
            Err(err) => return Err(err).context(ex_error::StateStoreSnafu),
        };

        // A persisted expiry in the past means the session died elsewhere.
        let now = to_unix(OffsetDateTime::now_utc());
        let expiry = record
            .expiry
            .map_or(i64::MAX, |expiry| i64::try_from(expiry).unwrap_or(i64::MAX));
        if expiry <= now {
            self.state_store
                .delete_session(session_id)
                .await
                .context(ex_error::StateStoreSnafu)?;
            return Ok(false);
        }

        tracing::info!(session_id, "Rehydrating session from state store");
        let metadata = SessionMetadata::from_map(record.metadata.clone());
        let user_session: Arc<UserSession> = Arc::new(
            UserSession::new(
                self.metastore.clone(),
                self.queries.clone(),
                self.config.clone(),
                self.catalog_list.clone(),
                self.runtime_env.clone(),
                session_id,
                metadata,
                self.state_store.clone(),
                self.warehouse.clone(),
            )
            .await?,
        );
        // Session parameters were loaded from the state store by
        // `UserSession::new`; refresh the persisted expiry from now.
        let expiry = to_unix(
            OffsetDateTime::now_utc()
                + DateTimeDuration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS),
        );
        user_session.expiry.store(expiry, Ordering::Relaxed);
        // Restore ad-hoc session attributes persisted before the failover.
        for (key, value) in record.attrs {
            user_session.attrs.insert(key, value);
        }

        let mut sessions = self.df_sessions.write().await;
        // Another task may have created the session concurrently; either way
        // a live session now exists.
        sessions
            .entry(session_id.to_string())
            .or_insert(user_session);
        Ok(true)
    }
}

#[async_trait::async_trait]
impl ExecutionService for CoreExecutionService {
    #[tracing::instrument(
        name = "ExecutionService::create_session",
        level = "debug",
        skip(self),
        fields(new_sessions_count),
        err
    )]
    async fn create_session(
        &self,
        session_id: &str,
        metadata: Option<SessionMetadata>,
    ) -> Result<Arc<UserSession>> {
        {
            let sessions = self.df_sessions.read().await;
            if let Some(session) = sessions.get(session_id) {
                return Ok(session.clone());
            }
        }
        let metadata = metadata.unwrap_or_default();
        let user_session: Arc<UserSession> = Arc::new(
            UserSession::new(
                self.metastore.clone(),
                self.queries.clone(),
                self.config.clone(),
                self.catalog_list.clone(),
                self.runtime_env.clone(),
                session_id,
                metadata.clone(),
                #[cfg(feature = "state-store")]
                self.state_store.clone(),
                self.warehouse.clone(),
            )
            .await?,
        );
        {
            tracing::trace!("Acquiring write lock for df_sessions");
            let mut sessions = self.df_sessions.write().await;
            tracing::trace!("Acquired write lock for df_sessions");
            sessions.insert(session_id.to_string(), user_session.clone());

            #[cfg(feature = "state-store")]
            self.persist_new_session(&user_session)
                .await
                .context(ex_error::StateStoreSnafu)?;

            // Record the result as part of the current span.
            tracing::Span::current().record("new_sessions_count", sessions.len());
        }
        Ok(user_session)
    }

    #[tracing::instrument(
        name = "ExecutionService::update_session_expiry",
        level = "debug",
        skip(self),
        fields(old_sessions_count, new_sessions_count, now),
        err
    )]
    async fn update_session_expiry(&self, session_id: &str) -> Result<bool> {
        let found = {
            let mut sessions = self.df_sessions.write().await;

            if let Some(session) = sessions.get_mut(session_id) {
                let now = OffsetDateTime::now_utc();
                let new_expiry =
                    to_unix(now + DateTimeDuration::seconds(SESSION_INACTIVITY_EXPIRATION_SECONDS));
                session.expiry.store(new_expiry, Ordering::Relaxed);
                true
            } else {
                false
            }
        };

        if found {
            // Keep the persisted record in sync so another process can tell
            // this session is still alive after a failover.
            #[cfg(feature = "state-store")]
            self.touch_persisted_session(session_id)
                .await
                .context(ex_error::StateStoreSnafu)?;
            return Ok(true);
        }

        // Session not in this process: it may have been created by (or
        // outlived) a different instance. Rehydrate it from the state store.
        #[cfg(feature = "state-store")]
        if self.rehydrate_session(session_id).await? {
            return Ok(true);
        }

        Ok(false)
    }

    #[tracing::instrument(
        name = "ExecutionService::delete_expired_sessions",
        level = "trace",
        skip(self),
        fields(old_sessions_count, new_sessions_count, now),
        err
    )]
    async fn delete_expired_sessions(&self) -> Result<()> {
        let now = to_unix(OffsetDateTime::now_utc());
        let mut sessions = self.df_sessions.write().await;

        let old_sessions_count = sessions.len();

        let mut deleted_session_ids = Vec::new();
        sessions.retain(|session_id, session| {
            let expiry = session.expiry.load(Ordering::Relaxed);
            if expiry <= now {
                let running_queries_count = session.running_queries.count();
                // prevent deleting sessions when session is expired but query is running
                if running_queries_count == 0 {
                    let _ = tracing::debug_span!(
                        "ExecutionService::delete_expired_session",
                        session_id,
                        expiry,
                        running_queries_count,
                        now
                    )
                    .entered();
                    deleted_session_ids.push(session_id.clone());
                    false
                } else {
                    let _ = tracing::debug_span!(
                        "ExecutionService::skip_delete_expired_session",
                        session_id,
                        expiry,
                        running_queries_count,
                        now
                    )
                    .entered();
                    true
                }
            } else {
                true
            }
        });

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("old_sessions_count", old_sessions_count)
            .record("new_sessions_count", sessions.len())
            .record("now", now);

        // Purge the persisted records so another process does not rehydrate a
        // session that just expired.
        #[cfg(feature = "state-store")]
        for session_id in deleted_session_ids {
            if let Err(err) = self.state_store.delete_session(&session_id).await {
                tracing::warn!(session_id, %err, "Failed to delete expired session record");
            }
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "ExecutionService::get_session",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_session(&self, session_id: &str) -> Result<Arc<UserSession>> {
        let sessions = self.df_sessions.read().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| ex_error::MissingDataFusionSessionSnafu { id: session_id }.build())?;
        Ok(session.clone())
    }

    #[tracing::instrument(name = "ExecutionService::session_exists", level = "debug", skip(self))]
    async fn session_exists(&self, session_id: &str) -> bool {
        let sessions = self.df_sessions.read().await;
        sessions.contains_key(session_id)
    }

    #[tracing::instrument(
        name = "ExecutionService::delete_session",
        level = "debug",
        skip(self),
        fields(new_sessions_count),
        err
    )]
    async fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut session_list = self.df_sessions.write().await;
        session_list.remove(session_id);

        // Record the result as part of the current span.
        tracing::Span::current().record("session_id", session_id);
        tracing::Span::current().record("new_sessions_count", session_list.len());
        drop(session_list);

        // Purge the persisted record so the session cannot be rehydrated.
        #[cfg(feature = "state-store")]
        if let Err(err) = self.state_store.delete_session(session_id).await {
            tracing::warn!(session_id, %err, "Failed to delete persisted session record");
        }
        Ok(())
    }
    fn get_sessions(&self) -> Arc<RwLock<HashMap<String, Arc<UserSession>>>> {
        self.df_sessions.clone()
    }

    #[tracing::instrument(
        name = "ExecutionService::query",
        level = "debug",
        skip(self),
        fields(query_id),
        err
    )]
    #[allow(clippy::large_futures)]
    async fn query(
        &self,
        session_id: &str,
        query: &str,
        query_context: QueryContext,
    ) -> Result<QueryResult> {
        let query_id = self.submit(session_id, query, query_context).await?;
        self.wait(query_id).await
    }

    #[tracing::instrument(name = "ExecutionService::wait", level = "debug", skip(self), err)]
    async fn wait(&self, query_id: QueryId) -> Result<QueryResult> {
        let _query_status = self.queries.wait_query_finished(query_id).await?;
        let running_query = self.queries.remove(query_id)?;
        if let Some(result_handle) = running_query.result_handle {
            result_handle
                .await
                .context(ex_error::AsyncResultTaskJoinSnafu { query_id })?
        } else {
            Err(ex_error::NoJoinHandleSnafu { query_id }.build())
        }
    }

    #[tracing::instrument(
        name = "ExecutionService::locate_query_id",
        level = "debug",
        skip(self)
    )]
    fn locate_query_id(&self, running_query_id: RunningQueryId) -> Result<QueryId> {
        self.queries.locate_query_id(running_query_id)
    }

    #[tracing::instrument(
        name = "ExecutionService::abort",
        level = "debug",
        skip(self),
        fields(old_queries_count = self.queries.count()),
        err
    )]
    async fn abort(&self, query_id: QueryId) -> Result<()> {
        self.queries.abort(query_id)?;
        self.queries.wait_query_finished(query_id).await?;
        Ok(())
    }

    #[tracing::instrument(
        name = "ExecutionService::submit",
        level = "debug",
        skip(self),
        fields(query_id, with_timeout_secs, old_queries_count = self.queries.count()),
        err
    )]
    async fn submit(
        &self,
        session_id: &str,
        query_text: &str,
        query_context: QueryContext,
    ) -> Result<QueryId> {
        let user_session = self.get_session(session_id).await?;

        let query_id = query_context.query_id;

        cfg_if::cfg_if! {
            if #[cfg(feature = "state-store-query")] {
                let mut query = Query::new(
                    query_text,
                    query_id,
                    session_id,
                    query_context.request_id,
                );
                query.set_execution_status(ExecutionStatus::Running);
                query.set_warehouse_type(self.config.warehouse_type.clone());
                query.set_release_version(self.config.build_version.clone());
                // session context set by user during login
                if let Some(database) = &query_context.database {
                    query.set_user_database(database.clone());
                }
                if let Some(schema) = &query_context.schema {
                    query.set_user_schema(schema.clone());
                }
                if let Some(query_submission_time) = &query_context.query_submission_time {
                    query.set_query_submission_time(*query_submission_time);
                }
                if let Some(session_metadata) = &query_context.session_metadata {
                    if let Some(user_name) = session_metadata.attr(SessionMetadataAttr::UserName) {
                        query.set_user_name(user_name);
                    }
                    if let Some(client_app_id) = session_metadata.attr(SessionMetadataAttr::ClientAppId) {
                        query.set_client_app_id(client_app_id);
                    }
                    if let Some(client_app_version) = session_metadata.attr(SessionMetadataAttr::ClientAppVersion) {
                        query.set_client_app_version(client_app_version);
                    }
                }
            }
        }

        // M6 queueing: queries above `max_concurrency_level` no longer fail
        // here — they wait in the queue (see `queue` semaphore in the spawned
        // task) up to `queue_timeout_secs` before failing like before.

        #[cfg(feature = "state-store-query")]
        self.state_store
            .put_query(&query)
            .await
            .context(ex_error::StateStoreSnafu)?;

        // Record the result as part of the current span.
        tracing::Span::current()
            .record("query_id", query_id.to_string())
            .record("with_timeout_secs", self.config.query_timeout_secs);

        let request_id = query_context.request_id;
        let query_token = CancellationToken::new();

        let task_span = tracing::info_span!("spawn_query_task");

        let alloc_span = tracing::info_span!(
            target: "alloc",
            "query_alloc",
            query_id = %query_id,
            session_id = %session_id
        );
        let handle = tokio::spawn({
            #[cfg(feature = "state-store-query")]
            let state_store = self.state_store.clone();
            let query_text = query_text.to_string();
            let query_timeout = Duration::from_secs(self.config.query_timeout_secs);
            let queue_wait = Duration::from_secs(self.config.queue_timeout_secs);
            let queue_sem = self.queue.clone();
            let queries_registry = self.queries.clone();
            let query_token = query_token.clone();
            async move {
                let sub_task_span = tracing::info_span!("spawn_query_sub_task");
                let mut query_obj = user_session.query(query_text, query_context);
                // M6: per-warehouse STATEMENT_TIMEOUT overrides the global
                // query timeout (0 = no timeout).
                let query_timeout = if let Some(warehouse_name) =
                    query_obj.session.current_warehouse_name()
                    && let Some(record) = query_obj
                        .session
                        .warehouse
                        .find_record(&warehouse_name)
                        .await
                    && let Some(secs) = record.statement_timeout_secs
                {
                    Duration::from_secs(if secs == 0 { u64::MAX } else { secs })
                } else {
                    query_timeout
                };
                #[cfg(feature = "state-store-query")]
                {
                    // effective database/schema at planning/execution time
                    query.set_database_name(query_obj.current_database());
                    query.set_schema_name(query_obj.current_schema());
                    // effective warehouse at planning/execution time (M5):
                    // real warehouse_* fields on the query record
                    if let Some(warehouse_name) = query_obj.session.current_warehouse_name()
                        && let Some(record) = query_obj.session.warehouse.find_record(&warehouse_name).await
                    {
                        query.set_warehouse_name(record.name);
                        query.set_warehouse_size(record.size);
                        if query.warehouse_type.is_none() {
                            query.set_warehouse_type("STANDARD".to_string());
                        }
                    }
                }

                // M6: warm queue — wait for one of `max_concurrency_level`
                // slots (cancellable) before executing; on queue-timeout the
                // query fails exactly like the old hard reject did.
                let wait_for_slot = async {
                    match tokio::time::timeout(queue_wait, queue_sem.acquire_owned()).await {
                        Ok(permit) => Ok(permit),
                        Err(_) => {
                            tracing::info_span!("queue_timeout");
                            Err(())
                        }
                    }
                };
                let execution_result = tokio::select! {
                    () = query_token.cancelled() => {
                        ExecutionTaskResult::from_cancelled_query_task(query_id)
                    },
                    slot = wait_for_slot => {
                        match slot {
                            Ok(permit) => {
                                let _permit = permit;
                                // Create nested task so in case of abort/timeout it can
                                // be aborted and result is handled properly (status /
                                // query result saved)
                                let task_future = task::spawn(async move {
                                    query_obj.execute().instrument(sub_task_span).await
                                });

                                let subtask_abort_handle = task_future.abort_handle();
                                // wait for any future to be resolved
                                tokio::select! {
                                    finished = task_future => {
                                        match finished {
                                            Ok(inner_result) => ExecutionTaskResult::from_query_result(query_id, inner_result),
                                            Err(task_error) => {
                                                tracing::error!("Query {query_id} sub task join error: {task_error:?}");
                                                ExecutionTaskResult::from_failed_query_task(query_id, task_error)
                                            },
                                        }
                                    },
                                    () = query_token.cancelled() => {
                                        tracing::info_span!("abort_cancelled_query");
                                        subtask_abort_handle.abort();
                                        ExecutionTaskResult::from_cancelled_query_task(query_id)
                                    },
                                    // Execute the query with a timeout to prevent long-running or stuck queries
                                    // from blocking system resources indefinitely. If the timeout is exceeded,
                                    // convert the timeout into a standard QueryTimeout error so it can be handled
                                    // and recorded like any other execution failure
                                    () = tokio::time::sleep(query_timeout) => {
                                        tracing::info_span!("query_timeout_received_do_abort");
                                        subtask_abort_handle.abort();
                                        ExecutionTaskResult::from_timeout_query_task(query_id)
                                    }
                                }
                            },
                            Err(()) => ExecutionTaskResult::from_query_limit_exceeded(query_id),
                        }
                    }
                };

                let _ = tracing::info_span!(
                    "finished_query_status",
                    query_id = query_id.to_string(),
                    query_status = format!("{:?}", execution_result.execution_status),
                    error_code = format!("{:?}", execution_result.error_code),
                )
                .entered();

                cfg_if::cfg_if! {
                    if #[cfg(feature = "state-store-query")] {
                        execution_result.assign_query_attributes(&mut query);
                        if let Some(stats) = queries_registry.cloned_stats(query_id) && let Some(query_type) = stats.query_type {
                            query.set_query_type(query_type.to_string());
                            execution_result.assign_rows_counts_attributes(&mut query, query_type);
                        }
                        // just log error and do not raise it from task
                        if let Err(err) = state_store.update_query(&query).await {
                            tracing::error!("Failed to update query {query_id}: {err:?}");
                        }
                    } else {
                        user_session.record_query_id(query_id);
                    }
                }

                // Notify subscribers query finishes and result is ready.
                // Do not immediately remove query from running queries registry
                // as RunningQuery contains result handle that caller should consume.
                queries_registry.notify_query_finished(query_id, execution_result.execution_status)?;

                // Discard results after short timeout, to prevent memory leaks
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(TIMEOUT_DISCARD_INTERVAL_SECONDS)).await;
                    let running_query = queries_registry.remove(query_id);
                    if let Ok(RunningQuery {
                        result_handle: Some(result_handle),
                        ..
                    }) = running_query
                    {
                        tracing::debug!(
                            "Discard execution result '{:?}' for query {query_id}",
                            execution_result.execution_status
                        );
                        let _ = result_handle.await;
                    }
                });

                execution_result.result
            }
            .instrument(alloc_span)
            .instrument(task_span)
        });

        self.queries.add(
            RunningQuery::new(query_id)
                .with_request_id(request_id)
                .with_result_handle(handle)
                .with_cancellation_token(query_token),
        );

        Ok(query_id)
    }

    #[tracing::instrument(
        name = "ExecutionService::upload_data_to_table",
        level = "debug",
        skip(self, data),
        err,
        ret
    )]
    async fn upload_data_to_table(
        &self,
        session_id: &str,
        table_ident: &MetastoreTableIdent,
        data: Bytes,
        file_name: &str,
        format: Format,
    ) -> Result<usize> {
        // TODO: is there a way to avoid temp table approach altogether?
        // File upload works as follows:
        // 1. Convert incoming data to a record batch
        // 2. Create a temporary table in memory
        // 3. Use Execution service to insert data into the target table from the temporary table
        // 4. Drop the temporary table

        // use unique name to support simultaneous uploads
        let unique_id = Uuid::new_v4().to_string().replace('-', "_");
        let user_session = {
            let sessions = self.df_sessions.read().await;
            sessions
                .get(session_id)
                .ok_or_else(|| {
                    ex_error::MissingDataFusionSessionSnafu {
                        id: session_id.to_string(),
                    }
                    .build()
                })?
                .clone()
        };

        let source_table =
            TableReference::full("tmp_db", "tmp_schema", format!("tmp_table_{unique_id}"));
        let target_table = TableReference::full(
            table_ident.database.clone(),
            table_ident.schema.clone(),
            table_ident.table.clone(),
        );
        let inmem_catalog = MemoryCatalogProvider::new();
        inmem_catalog
            .register_schema(
                source_table.schema().unwrap_or_default(),
                Arc::new(MemorySchemaProvider::new()),
            )
            .context(ex_error::DataFusionSnafu)?;
        user_session.ctx.register_catalog(
            source_table.catalog().unwrap_or_default(),
            Arc::new(inmem_catalog),
        );
        // If target table already exists, we need to insert into it
        // otherwise, we need to create it
        let exists = user_session
            .ctx
            .table_exist(target_table.clone())
            .context(ex_error::DataFusionSnafu)?;

        let schema = if exists {
            let table = user_session
                .ctx
                .table(target_table)
                .await
                .context(ex_error::DataFusionSnafu)?;
            table.schema().as_arrow().to_owned()
        } else {
            let (schema, _) = format
                .infer_schema(data.clone().reader(), None)
                .context(ex_error::ArrowSnafu)?;
            schema
        };
        let schema = Arc::new(schema);

        // Here we create an arrow CSV reader that infers the schema from the entire dataset
        // (as `None` is passed for the number of rows) and then builds a record batch
        // TODO: This partially duplicates what Datafusion does with `CsvFormat::infer_schema`
        let csv = ReaderBuilder::new(schema.clone())
            .with_format(format)
            .build_buffered(data.reader())
            .context(ex_error::ArrowSnafu)?;

        let batches: std::result::Result<Vec<_>, _> = csv.collect();
        let batches = batches.context(ex_error::ArrowSnafu)?;

        let rows_loaded = batches
            .iter()
            .map(|batch: &RecordBatch| batch.num_rows())
            .sum();

        let table = MemTable::try_new(schema, vec![batches]).context(ex_error::DataFusionSnafu)?;
        user_session
            .ctx
            .register_table(source_table.clone(), Arc::new(table))
            .context(ex_error::DataFusionSnafu)?;

        let table = source_table.clone();
        let query = if exists {
            format!("INSERT INTO {table_ident} SELECT * FROM {table}")
        } else {
            format!("CREATE TABLE {table_ident} AS SELECT * FROM {table}")
        };

        let mut query = user_session.query(&query, QueryContext::default());
        Box::pin(query.execute()).await?;

        user_session
            .ctx
            .deregister_table(source_table)
            .context(ex_error::DataFusionSnafu)?;

        Ok(rows_loaded)
    }

    async fn timeout_signal(&self, interval: Duration, idle_timeout: Duration) -> () {
        let mut interval = tokio::time::interval(interval);
        interval.tick().await; // The first tick completes immediately; skip.
        let mut idle_since: Option<std::time::Instant> = None;
        loop {
            interval.tick().await;
            let sessions_empty = {
                let sessions = self.df_sessions.read().await;
                sessions.is_empty()
            };
            let queries_empty = self.queries.count() == 0;
            let idle_now = sessions_empty && queries_empty;
            match (idle_now, idle_since) {
                (true, None) => {
                    // just entered idle
                    idle_since = Some(std::time::Instant::now());
                }
                (true, Some(since)) => {
                    if since.elapsed() >= idle_timeout {
                        // stayed idle long enough
                        return;
                    }
                }
                (false, _) => {
                    // became active again, reset the idle window
                    idle_since = None;
                }
            }
        }
    }
}

//Test environment
#[allow(clippy::expect_used)]
pub async fn make_test_execution_svc() -> Arc<CoreExecutionService> {
    let metastore = Arc::new(InMemoryMetastore::new());
    Arc::new(
        CoreExecutionService::new(metastore, Arc::new(Config::default()))
            .await
            .expect("Failed to create a execution service"),
    )
}
