//! Postgres-backed [`ClusterState`] for scheduler HA (M7.3, DESIGN.md §13.3).
//!
//! Executor registration, heartbeats, and vcore accounting live in shared
//! Postgres tables so several schedulers can serve one warehouse. Two
//! schedulers never double-assign a task: `bind_schedulable_tasks` locks the
//! vcore rows (`SELECT … FOR UPDATE`) inside a transaction, binds with the
//! shared budgets, and commits the decrements atomically.
//!
//! `executor_heartbeats()` is a **sync** trait method, so this backend keeps
//! a local mirror refreshed by a background task (default every
//! [`REFRESH_INTERVAL_MS`]); new/removed rows discovered by the refresh are
//! translated into `RegisteredExecutor`/`RemovedExecutor` events locally,
//! which is how a replica learns about executors that registered through a
//! different scheduler.

use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::serde::protobuf::{
    executor_status, AvailableVcores, ExecutorHeartbeat, ExecutorStatus,
};
use ballista_core::serde::scheduler::{
    ExecutorData, ExecutorMetadata, ExecutorOperatingSystemSpecification,
    ExecutorSpecification,
};
use ballista_scheduler::cluster::event::ClusterEventSender;
use ballista_scheduler::cluster::{
    BoundTask, ClusterState, ClusterStateEvent, ClusterStateEventStream, ExecutorSlot,
};
use ballista_scheduler::config::TaskDistributionPolicy;
use ballista_scheduler::state::task_manager::JobInfoCache;
use ballista_scheduler::scheduler_server::timestamp_secs;
use dashmap::DashMap;
use deadpool_postgres::Pool;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::bind::{bind_task_bias, bind_task_round_robin};

/// How often the local heartbeat/executor mirror refreshes from Postgres.
/// Small enough that a replica discovers peer-scheduler executors well
/// inside ballista's executor-expiry window, large enough to be negligible
/// load on the state store.
pub const REFRESH_INTERVAL_MS: u64 = 500;

/// Postgres pool shared by the cluster/job backends.
pub async fn pg_pool(url: &str, max_size: usize) -> BallistaResult<Pool> {
    let pg_config: tokio_postgres::Config = url
        .parse()
        .map_err(|e| BallistaError::Internal(format!("invalid pg url: {e}")))?;
    let manager = deadpool_postgres::Manager::new(pg_config, tokio_postgres::NoTls);
    Pool::builder(manager)
        .max_size(max_size)
        .build()
        .map_err(|e| BallistaError::Internal(format!("pg pool: {e}")))
}

const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS ballista_executors (
        id            TEXT PRIMARY KEY,
        host          TEXT NOT NULL,
        port          INT  NOT NULL,
        grpc_port     INT  NOT NULL,
        spec_vcores   INT  NOT NULL,
        os_info       JSONB NOT NULL,
        registered_at TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE TABLE IF NOT EXISTS ballista_heartbeats (
        executor_id TEXT PRIMARY KEY,
        ts          BIGINT NOT NULL,
        pb          BYTEA  NOT NULL,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now())",
    "CREATE TABLE IF NOT EXISTS ballista_executor_vcores (
        executor_id TEXT PRIMARY KEY,
        free_vcores INT  NOT NULL)",
];

struct Inner {
    pool: Pool,
    /// Local mirror of `ballista_heartbeats`, refreshed in the background —
    /// `executor_heartbeats()` is a sync trait method and cannot query PG.
    mirror: std::sync::Mutex<HashMap<String, ExecutorHeartbeat>>,
    /// Executor ids this scheduler has seen (locally or via a peer), used by
    /// the refresh loop to detect registrations/removals and emit events.
    known: DashMap<String, ()>,
    sender: ClusterEventSender<ClusterStateEvent>,
}

pub struct PostgresClusterState {
    inner: Arc<Inner>,
}

impl PostgresClusterState {
    pub fn new(pool: Pool, _refresh_interval_ms: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                mirror: std::sync::Mutex::new(HashMap::new()),
                known: DashMap::new(),
                sender: ClusterEventSender::new(100),
            }),
        }
    }

    /// Runs the DDL and spawns the background mirror refresh.
    pub async fn init(&self) -> BallistaResult<()> {
        let conn = self.conn().await?;
        for stmt in DDL {
            conn.execute(*stmt, &[]).await.map_err(pg_err("cluster DDL"))?;
        }
        drop(conn);

        let inner = self.inner.clone();
        let period = Duration::from_millis(REFRESH_INTERVAL_MS.max(50));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                if let Err(err) = refresh_once(&inner).await {
                    tracing::warn!(%err, "cluster mirror refresh failed");
                }
            }
        });
        Ok(())
    }

    async fn conn(&self) -> BallistaResult<deadpool_postgres::Object> {
        self.inner
            .pool
            .get()
            .await
            .map_err(|e| BallistaError::Internal(format!("pg conn: {e}")))
    }

    async fn metadata_row(
        &self,
        executor_id: &str,
    ) -> BallistaResult<Option<ExecutorMetadata>> {
        let conn = self.conn().await?;
        let rows = conn
            .query(
                "SELECT id, host, port, grpc_port, spec_vcores, os_info
                 FROM ballista_executors WHERE id = $1",
                &[&executor_id],
            )
            .await
            .map_err(pg_err("select executor metadata"))?;
        Ok(rows.first().map(row_to_metadata))
    }

    async fn all_metadata_rows(&self) -> BallistaResult<Vec<ExecutorMetadata>> {
        let conn = self.conn().await?;
        let rows = conn
            .query(
                "SELECT id, host, port, grpc_port, spec_vcores, os_info
                 FROM ballista_executors",
                &[],
            )
            .await
            .map_err(pg_err("select executor metadata"))?;
        Ok(rows.iter().map(row_to_metadata).collect())
    }

    async fn upsert_heartbeat(&self, heartbeat: &ExecutorHeartbeat) -> BallistaResult<()> {
        let mut pb = Vec::new();
        heartbeat
            .encode(&mut pb)
            .map_err(|e| BallistaError::Internal(format!("encode heartbeat: {e}")))?;
        let ts = heartbeat.timestamp as i64;
        let executor_id = heartbeat.executor_id.clone();
        let conn = self.conn().await?;
        conn.execute(
            "INSERT INTO ballista_heartbeats (executor_id, ts, pb)
             VALUES ($1, $2, $3)
             ON CONFLICT (executor_id) DO UPDATE
               SET ts = EXCLUDED.ts, pb = EXCLUDED.pb, updated_at = now()",
            &[&executor_id.as_str(), &ts, &pb],
        )
        .await
        .map_err(pg_err("upsert heartbeat"))?;
        // Write-through: the local mirror must reflect our own writes
        // immediately (the refresh only pulls remote changes).
        self.inner
            .mirror
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(executor_id)
            .and_modify(|cur| {
                if cur.timestamp <= heartbeat.timestamp {
                    *cur = heartbeat.clone();
                }
            })
            .or_insert_with(|| heartbeat.clone());
        Ok(())
    }

    async fn delete_executor(&self, executor_id: &str) -> BallistaResult<()> {
        let conn = self.conn().await?;
        conn.execute(
            "DELETE FROM ballista_executors WHERE id = $1",
            &[&executor_id],
        )
        .await
        .map_err(pg_err("delete executor"))?;
        conn.execute(
            "DELETE FROM ballista_heartbeats WHERE executor_id = $1",
            &[&executor_id],
        )
        .await
        .map_err(pg_err("delete heartbeat"))?;
        conn.execute(
            "DELETE FROM ballista_executor_vcores WHERE executor_id = $1",
            &[&executor_id],
        )
        .await
        .map_err(pg_err("delete vcores"))?;
        Ok(())
    }
}

fn pg_err(what: &'static str) -> impl Fn(tokio_postgres::Error) -> BallistaError {
    move |e| BallistaError::Internal(format!("{what}: {e}"))
}

fn row_to_metadata(row: &tokio_postgres::Row) -> ExecutorMetadata {
    let os_info: serde_json::Value = row.get("os_info");
    let get_u64 = |key: &str| {
        os_info
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let get_str = |key: &str| {
        os_info
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    ExecutorMetadata {
        id: row.get("id"),
        host: row.get("host"),
        port: row.get::<_, i32>("port").max(0) as u16,
        grpc_port: row.get::<_, i32>("grpc_port").max(0) as u16,
        specification: ExecutorSpecification {
            vcores: row.get::<_, i32>("spec_vcores").max(0) as u32,
        },
        os_info: ExecutorOperatingSystemSpecification {
            system_name: get_str("system_name"),
            kernel_ver: get_str("kernel_ver"),
            os_ver: get_str("os_ver"),
            os_ver_long: get_str("os_ver_long"),
            physical_cores: get_u64("physical_cores") as u32,
            num_disks: get_u64("num_disks") as u32,
            total_disk_space: get_u64("total_disk_space"),
            total_available_disk_space: get_u64("total_available_disk_space"),
            open_files_limit: get_u64("open_files_limit"),
        },
    }
}

fn active_heartbeat(executor_id: &str) -> ExecutorHeartbeat {
    ExecutorHeartbeat {
        executor_id: executor_id.to_string(),
        timestamp: timestamp_secs(),
        metrics: vec![],
        status: Some(ExecutorStatus {
            status: Some(executor_status::Status::Active(String::default())),
        }),
        peak_proc_physical_memory: 0,
        peak_proc_virtual_memory: 0,
    }
}

/// One mirror-refresh cycle: pull heartbeats + executor ids from Postgres,
/// update the local mirror, and emit events for registrations/removals
/// discovered from peer schedulers.
async fn refresh_once(inner: &Inner) -> BallistaResult<()> {
    let conn = inner
        .pool
        .get()
        .await
        .map_err(|e| BallistaError::Internal(format!("pg conn: {e}")))?;
    let hb_rows = conn
        .query("SELECT executor_id, ts, pb FROM ballista_heartbeats", &[])
        .await
        .map_err(pg_err("select heartbeats"))?;
    let id_rows = conn
        .query("SELECT id FROM ballista_executors", &[])
        .await
        .map_err(pg_err("select executor ids"))?;
    drop(conn);

    let pg_ids: HashSet<String> = id_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let mut hbs: HashMap<String, ExecutorHeartbeat> = HashMap::new();
    for row in &hb_rows {
        let id: String = row.get(0);
        let pb: Vec<u8> = row.get(2);
        match ExecutorHeartbeat::decode(pb.as_slice()) {
            Ok(hb) => {
                hbs.insert(id, hb);
            }
            Err(err) => tracing::warn!(executor = %id, %err, "undecodable heartbeat"),
        }
    }

    // Update the mirror before emitting events so consumers observe a
    // consistent view when they react. Never let a stale PG row overwrite a
    // fresher local write.
    {
        let mut guard = inner.mirror.lock().unwrap_or_else(|p| p.into_inner());
        for (id, hb) in hbs {
            let newer = guard
                .get(&id)
                .map_or(true, |cur| cur.timestamp <= hb.timestamp);
            if newer {
                guard.insert(id, hb);
            }
        }
        // Heartbeats of executors removed by a peer vanish from the mirror.
        guard.retain(|id, _| pg_ids.contains(id));
    }

    // New executors (registered by a peer, or present before this scheduler
    // started) → announce locally so PushStaged schedules on them.
    for id in &pg_ids {
        if !inner.known.contains_key(id) {
            inner.known.insert(id.clone(), ());
            tracing::info!(executor = %id, "discovered executor via shared cluster state");
            inner.sender.send(&ClusterStateEvent::RegisteredExecutor {
                executor_id: id.clone(),
            });
        }
    }
    // Executors removed by a peer's expiry logic → retract locally.
    for id in inner
        .known
        .iter()
        .map(|e| e.key().clone())
        .collect::<Vec<_>>()
    {
        if !pg_ids.contains(&id) {
            inner.known.remove(&id);
            tracing::info!(executor = %id, "executor removed by peer detected");
            inner.sender.send(&ClusterStateEvent::RemovedExecutor {
                executor_id: id,
            });
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl ClusterState for PostgresClusterState {
    async fn bind_schedulable_tasks(
        &self,
        distribution: TaskDistributionPolicy,
        active_jobs: Arc<HashMap<ballista_core::JobId, JobInfoCache>>,
        executors: Option<HashSet<String>>,
    ) -> BallistaResult<Vec<BoundTask>> {
        if executors.as_ref().is_some_and(|s| s.is_empty()) {
            return Ok(vec![]);
        }

        let mut conn = self.conn().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(pg_err("begin bind txn"))?;
        let rows = if let Some(set) = &executors {
            let ids: Vec<String> = set.iter().cloned().collect();
            tx.query(
                "SELECT executor_id, free_vcores FROM ballista_executor_vcores
                 WHERE free_vcores > 0 AND executor_id = ANY($1)
                 ORDER BY executor_id FOR UPDATE",
                &[&ids],
            )
            .await
            .map_err(pg_err("lock vcores"))?
        } else {
            tx.query(
                "SELECT executor_id, free_vcores FROM ballista_executor_vcores
                 WHERE free_vcores > 0 ORDER BY executor_id FOR UPDATE",
                &[],
            )
            .await
            .map_err(pg_err("lock vcores"))?
        };

        let mut budgets: Vec<AvailableVcores> = rows
            .iter()
            .map(|r| AvailableVcores {
                executor_id: r.get::<_, String>(0),
                vcores: r.get::<_, i32>(1).max(0) as u32,
            })
            .collect();
        let initial: Vec<(String, u32)> = budgets
            .iter()
            .map(|b| (b.executor_id.clone(), b.vcores))
            .collect();

        let bound = match distribution {
            TaskDistributionPolicy::Bias => {
                bind_task_bias(budgets.iter_mut().collect(), active_jobs, |_| false).await?
            }
            TaskDistributionPolicy::RoundRobin => {
                bind_task_round_robin(budgets.iter_mut().collect(), active_jobs, |_| false)
                    .await?
            }
            TaskDistributionPolicy::Custom(ref policy) => {
                policy.bind_tasks(budgets.iter_mut().collect(), active_jobs).await?
            }
        };

        // Persist the decrements inside the same transaction — atomicity is
        // what guarantees two schedulers cannot double-assign a vcore.
        for (idx, (executor_id, before)) in initial.iter().enumerate() {
            let after = budgets[idx].vcores;
            if after != *before {
                tx.execute(
                    "UPDATE ballista_executor_vcores SET free_vcores = $2
                     WHERE executor_id = $1",
                    &[&executor_id, &(after as i32)],
                )
                .await
                .map_err(pg_err("persist bind"))?;
            }
        }
        tx.commit().await.map_err(pg_err("commit bind"))?;
        Ok(bound)
    }

    async fn unbind_tasks(&self, executor_slots: Vec<ExecutorSlot>) -> BallistaResult<()> {
        let mut increments: HashMap<String, u32> = HashMap::new();
        for (executor_id, num_vcores) in executor_slots {
            *increments.entry(executor_id).or_insert(0) += num_vcores;
        }
        let mut conn = self.conn().await?;
        let tx = conn
            .transaction()
            .await
            .map_err(pg_err("begin unbind txn"))?;
        for (executor_id, inc) in increments {
            // A missing row (executor removed concurrently) is ignored,
            // matching the in-memory backend's behaviour.
            tx.execute(
                "UPDATE ballista_executor_vcores SET free_vcores = free_vcores + $2
                 WHERE executor_id = $1",
                &[&executor_id, &(inc as i32)],
            )
            .await
            .map_err(pg_err("unbind"))?;
        }
        tx.commit().await.map_err(pg_err("commit unbind"))?;
        Ok(())
    }

    async fn register_executor(
        &self,
        metadata: ExecutorMetadata,
        spec: ExecutorData,
    ) -> BallistaResult<()> {
        let executor_id = metadata.id.clone();
        if spec.executor_id != executor_id {
            return Err(BallistaError::Configuration(format!(
                "executor data id {} does not match metadata id {executor_id}",
                spec.executor_id
            )));
        }
        if self.metadata_row(&executor_id).await?.is_some() {
            return Err(BallistaError::Configuration(format!(
                "executor_id {executor_id} is already registered"
            )));
        }

        let os_json = serde_json::to_value(&metadata.os_info)
            .map_err(|e| BallistaError::Internal(format!("serialize os_info: {e}")))?;
        let conn = self.conn().await?;
        conn.execute(
            "INSERT INTO ballista_executors
                 (id, host, port, grpc_port, spec_vcores, os_info)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                &metadata.id.as_str(),
                &metadata.host.as_str(),
                &(metadata.port as i32),
                &(metadata.grpc_port as i32),
                &(metadata.specification.vcores as i32),
                &os_json,
            ],
        )
        .await
        .map_err(pg_err("insert executor"))?;
        conn.execute(
            "INSERT INTO ballista_executor_vcores (executor_id, free_vcores)
             VALUES ($1, $2)
             ON CONFLICT (executor_id) DO UPDATE
               SET free_vcores = EXCLUDED.free_vcores",
            &[&executor_id.as_str(), &(spec.available_vcores as i32)],
        )
        .await
        .map_err(pg_err("insert vcores"))?;
        drop(conn);

        self.inner.known.insert(executor_id.clone(), ());
        self.inner.sender.send(&ClusterStateEvent::RegisteredExecutor {
            executor_id: executor_id.clone(),
        });
        self.upsert_heartbeat(&active_heartbeat(&executor_id)).await
    }

    async fn save_executor_metadata(&self, metadata: ExecutorMetadata) -> BallistaResult<()> {
        let executor_id = metadata.id.clone();
        match self.metadata_row(&executor_id).await? {
            Some(existing) if existing != metadata => {
                return Err(BallistaError::Configuration(format!(
                    "executor_id {executor_id} is already registered"
                )));
            }
            Some(_) => {} // identical re-save (pull-based executors) is a no-op
            None => {
                let os_json = serde_json::to_value(&metadata.os_info)
                    .map_err(|e| BallistaError::Internal(format!("serialize os_info: {e}")))?;
                let conn = self.conn().await?;
                conn.execute(
                    "INSERT INTO ballista_executors
                         (id, host, port, grpc_port, spec_vcores, os_info)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &metadata.id.as_str(),
                        &metadata.host.as_str(),
                        &(metadata.port as i32),
                        &(metadata.grpc_port as i32),
                        &(metadata.specification.vcores as i32),
                        &os_json,
                    ],
                )
                .await
                .map_err(pg_err("insert executor"))?;
                drop(conn);
                self.inner.known.insert(executor_id.clone(), ());
                self.inner
                    .sender
                    .send(&ClusterStateEvent::RegisteredExecutor {
                        executor_id: executor_id.clone(),
                    });
            }
        }

        // Pull-based executors send `save_executor_metadata` with all
        // requests (not `ExecutorHeartbeat`) — record a heartbeat so their
        // liveness is tracked uniformly (mirrors the in-memory backend).
        let hb = ExecutorHeartbeat {
            executor_id,
            timestamp: timestamp_secs(),
            metrics: vec![],
            status: None,
            peak_proc_physical_memory: 0,
            peak_proc_virtual_memory: 0,
        };
        self.upsert_heartbeat(&hb).await
    }

    async fn get_executor_metadata(&self, executor_id: &str) -> BallistaResult<ExecutorMetadata> {
        self.metadata_row(executor_id).await?.ok_or_else(|| {
            BallistaError::Internal(format!("Not executor with ID {executor_id} found"))
        })
    }

    async fn registered_executor_metadata(&self) -> Vec<ExecutorMetadata> {
        self.all_metadata_rows().await.unwrap_or_default()
    }

    async fn save_executor_heartbeat(&self, heartbeat: ExecutorHeartbeat) -> BallistaResult<()> {
        self.upsert_heartbeat(&heartbeat).await
    }

    async fn remove_executor(&self, executor_id: &str) -> BallistaResult<()> {
        self.delete_executor(executor_id).await?;
        self.inner.known.remove(executor_id);
        self.inner.mirror.lock().unwrap_or_else(|p| p.into_inner()).remove(executor_id);
        self.inner.sender.send(&ClusterStateEvent::RemovedExecutor {
            executor_id: executor_id.to_string(),
        });
        Ok(())
    }

    fn executor_heartbeats(&self) -> HashMap<String, ExecutorHeartbeat> {
        // Sync trait method: serve from the local mirror (refreshed every
        // REFRESH_INTERVAL_MS in the background).
        self.inner.mirror.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn get_executor_heartbeat(&self, executor_id: &str) -> Option<ExecutorHeartbeat> {
        self.inner.mirror.lock().unwrap_or_else(|p| p.into_inner()).get(executor_id).cloned()
    }

    async fn cluster_state_events(&self) -> BallistaResult<ClusterStateEventStream> {
        Ok(Box::pin(self.inner.sender.subscribe()))
    }
}