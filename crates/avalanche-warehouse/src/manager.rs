//! Warehouse lifecycle management (Avalanche design §5).
//!
//! One warehouse = one Ballista scheduler + an executor pool. Suspension
//! scales executors to 0 (scheduler stays up); a fully stopped warehouse is
//! cold (scheduler down too). Resume-on-query is [`WarehouseManager::ensure_running`].

use crate::driver::{ContainerDriver, ContainerSpec};
use crate::error::{
    AlreadyExistsSnafu, ConfigSnafu, InvalidAutoSuspendSnafu, InvalidSizeSnafu, NotFoundSnafu,
    Result, StateStoreSnafu, TlsSnafu,
};
use crate::metering::MeteringLedger;
use crate::model::{Warehouse, WarehouseSize, WarehouseState};
use avalanche_executor_common::{CONTAINER_TLS_DIR, TlsMaterial, TLS_DIR_ENV};
use snafu::ResultExt;
use state_store::StateStore;
use state_store::StorageCredentials;
use state_store::WarehouseRecord;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::info;

/// Ballista image coordinates used for warehouse compute.
#[derive(Debug, Clone)]
pub struct BallistaImages {
    pub scheduler: String,
    pub executor: String,
}

impl Default for BallistaImages {
    fn default() -> Self {
        Self {
            scheduler: "ghcr.io/apache/datafusion-ballista-scheduler:latest".to_string(),
            executor: "ghcr.io/apache/datafusion-ballista-executor:latest".to_string(),
        }
    }
}

impl BallistaImages {
    /// Images with env override: `WAREHOUSE_SCHEDULER_IMAGE` and
    /// `WAREHOUSE_EXECUTOR_IMAGE` (used by acceptance runs to point at the
    /// Embucket-codec wrapper images).
    #[must_use]
    pub fn from_env() -> Self {
        let default = Self::default();
        let scheduler = std::env::var("WAREHOUSE_SCHEDULER_IMAGE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(default.scheduler);
        let executor = std::env::var("WAREHOUSE_EXECUTOR_IMAGE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or(default.executor);
        Self { scheduler, executor }
    }
}

#[derive(Debug, Clone)]
pub struct CreateWarehouseRequest {
    pub name: String,
    pub size: WarehouseSize,
    /// Idle seconds before auto-suspend; 0 disables auto-suspend.
    pub auto_suspend_secs: u64,
    pub auto_resume: bool,
    /// Snowflake `INITIALLY_SUSPENDED` — create without starting compute.
    pub initially_suspended: bool,
    /// Per-warehouse statement timeout override (seconds); `None` = global,
    /// 0 = no timeout (M6).
    pub statement_timeout_secs: Option<u64>,
    /// Per-warehouse storage credentials (M7.1): injected into compute
    /// containers as env, never into plans.
    pub storage_credentials: Option<StorageCredentials>,
    /// Scheduler replicas sharing Postgres-backed cluster state (M7.3);
    /// 1 (default) is the plain single-scheduler warehouse.
    pub scheduler_replicas: u16,
    /// Mutual TLS (M7.2): start compute with `AVALANCHE_TLS_DIR` and
    /// advertise the scheduler over `https://`.
    pub tls: bool,
}

pub struct WarehouseManager {
    state_store: Arc<dyn StateStore>,
    driver: Arc<dyn ContainerDriver>,
    images: BallistaImages,
    /// Docker network all warehouse containers share (container-name DNS).
    network: Option<String>,
    /// Last query activity per warehouse, for auto-suspend.
    last_activity: Mutex<HashMap<String, Instant>>,
    /// Executor-seconds metering (M5).
    pub(crate) ledger: Arc<MeteringLedger>,
    /// Epoch seconds when the warehouse's current RUNNING window began
    /// (last resume/create). Used to bill running time on suspend/drop.
    running_since: Mutex<HashMap<String, u64>>,
}

const INTERNAL_SCHEDULER_PORT: u16 = 50050;

/// Env var that hands scheduler replicas their shared Postgres cluster state
/// (M7.3): value is a `postgres://` URL rewritten for container reachability.
pub const CLUSTER_STATE_PG_ENV: &str = "AVALANCHE_CLUSTER_PG_URL";

/// Hard cap on scheduler replicas per warehouse (resource sanity).
const MAX_SCHEDULER_REPLICAS: u16 = 4;

impl WarehouseManager {
    pub fn new(
        state_store: Arc<dyn StateStore>,
        driver: Arc<dyn ContainerDriver>,
        images: BallistaImages,
        network: Option<String>,
    ) -> Self {
        Self {
            state_store: state_store.clone(),
            driver,
            images,
            network,
            last_activity: Mutex::new(HashMap::new()),
            ledger: Arc::new(MeteringLedger::new(state_store)),
            running_since: Mutex::new(HashMap::new()),
        }
    }

    /// Metering ledger (executor-seconds buckets) backing
    /// `WAREHOUSE_METERING_HISTORY`.
    #[must_use]
    pub fn metering(&self) -> Arc<MeteringLedger> {
        Arc::clone(&self.ledger)
    }

    /// Case-tolerant record lookup (session variables may differ in case
    /// from the stored key): `USE WAREHOUSE etl` vs stored `ETL`.
    pub async fn find_record(&self, name: &str) -> Option<WarehouseRecord> {
        match self.record(name).await {
            Ok(record) => Some(record),
            Err(_) => self.record(&name.to_ascii_uppercase()).await.ok(),
        }
    }

    /// Load persisted metering history into the ledger. Call once at startup
    /// (best-effort: failures are logged and ignored).
    pub async fn load_metering(&self) {
        if let Err(err) = self.ledger.load().await {
            tracing::warn!(%err, "failed to load metering history");
        }
    }

    // ---------------------------------------------------------------- DDL

    pub async fn create(&self, req: CreateWarehouseRequest) -> Result<Warehouse> {
        if self.state_store.get_warehouse(&req.name).await.is_ok() {
            return Err(AlreadyExistsSnafu {
                name: req.name.clone(),
            }
            .build());
        }
        if self.state_store.get_session(&req.name).await.is_ok() {
            // defensive: name collisions in the shared store namespace
            return Err(AlreadyExistsSnafu {
                name: req.name.clone(),
            }
            .build());
        }
        let mut record = WarehouseRecord::new(
            &req.name,
            req.size.as_str(),
            req.auto_suspend_secs,
            req.auto_resume,
        );
        record.statement_timeout_secs = req.statement_timeout_secs;
        record.storage_credentials = req.storage_credentials.clone();
        record.tls = req.tls;
        record.scheduler_replicas = req.scheduler_replicas.max(1);
        if record.scheduler_replicas >= 2 && !shared_state_url_configured() {
            return Err(ConfigSnafu {
                message: "SCHEDULER_REPLICAS > 1 requires STATESTORE_PG_URL (or DATABASE_URL)                           on the router: replicas share executor/job state through Postgres"
                    .to_string(),
            }
            .build());
        }
        // Reserve a host port for the scheduler so host-side clients can dial
        // its gRPC endpoint. Best-effort: creation must not fail on it.
        record.scheduler_host_port = allocate_free_host_port();
        if record.scheduler_host_port.is_none() {
            tracing::warn!(
                warehouse = %req.name,
                "failed to allocate a free host port for the scheduler; \
                 host-side scheduler access will be unavailable"
            );
        }
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;

        let warehouse = self.get(&req.name).await?;
        if req.initially_suspended {
            return Ok(warehouse);
        }
        self.start_compute(&req.name, req.size, WarehouseState::Running)
            .await?;
        self.touch(&req.name).await;
        // re-read: start_compute persisted the RUNNING state
        self.get(&req.name).await
    }

    /// `ALTER WAREHOUSE name SET ...` — size / auto-suspend / auto-resume.
    pub async fn alter(
        &self,
        name: &str,
        size: Option<WarehouseSize>,
        auto_suspend_secs: Option<u64>,
        auto_resume: Option<bool>,
        statement_timeout_secs: Option<u64>,
    ) -> Result<Warehouse> {
        let mut record = self.record(name).await?;
        if let Some(size) = size {
            record.size = size.as_str().to_string();
        }
        if let Some(secs) = auto_suspend_secs {
            if secs > 86_400 {
                return Err(InvalidAutoSuspendSnafu { value: secs }.build());
            }
            record.auto_suspend_secs = secs;
        }
        if let Some(auto_resume) = auto_resume {
            record.auto_resume = auto_resume;
        }
        // 0 clears the override (fall back to the global timeout)
        if let Some(secs) = statement_timeout_secs {
            record.statement_timeout_secs = (secs > 0).then_some(secs);
        }
        record.updated_at = Some(now_secs());
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;

        // A size change on a running warehouse re-provisions the executor pool.
        if let (Some(size), true) = (
            size,
            self.state(name).await? == WarehouseState::Running,
        ) {
            // Bill the elapsed window before re-provisioning (at the new
            // count — close enough for the partial window).
            self.accrue_running_window(name, size.executor_count())
                .await;
            self.start_compute(name, size, WarehouseState::Running)
                .await?;
        }
        self.get(name).await
    }

    /// Set (or clear) per-warehouse storage credentials (M7.1). A running
    /// warehouse's compute is restarted so containers pick up the new env.
    pub async fn set_storage_credentials(
        &self,
        name: &str,
        credentials: Option<StorageCredentials>,
    ) -> Result<Warehouse> {
        let record = self.record(name).await?;
        let was_running = self.state(name).await? == WarehouseState::Running;
        let mut record = record;
        record.storage_credentials = credentials;
        record.updated_at = Some(now_secs());
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;
        if was_running {
            // Recreate containers so the new env takes effect (docker/k8s
            // only read env at container start).
            if let Some(size) = WarehouseSize::parse(
                &self.record(name).await?.size,
            ) {
                for i in 0..size.executor_count() {
                    self.driver
                        .stop(&executor_container_name(name, i))
                        .await?;
                }
                self.stop_schedulers(name, self.record(name).await?.scheduler_replicas)
                    .await?;
                self.start_compute(name, size, WarehouseState::Running)
                    .await?;
            }
        }
        self.get(name).await
    }

    /// Toggle mutual TLS for a warehouse (M7.2). A running warehouse's
    /// compute is restarted so containers pick up the TLS mounts.
    ///
    /// # Errors
    ///
    /// Errors if enabling TLS while the router has no `AVALANCHE_TLS_DIR`.
    pub async fn set_tls(&self, name: &str, tls: bool) -> Result<Warehouse> {
        let was_running = self.state(name).await? == WarehouseState::Running;
        if tls {
            // fail fast before persisting anything
            self.tls_volumes().await?;
        }
        let mut record = self.record(name).await?;
        record.tls = tls;
        record.updated_at = Some(now_secs());
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;
        if was_running {
            // Recreate containers so the new TLS mounts take effect.
            if let Some(size) = WarehouseSize::parse(&self.record(name).await?.size) {
                for i in 0..size.executor_count() {
                    self.driver
                        .stop(&executor_container_name(name, i))
                        .await?;
                }
                self.stop_schedulers(name, self.record(name).await?.scheduler_replicas)
                    .await?;
                self.start_compute(name, size, WarehouseState::Running)
                    .await?;
            }
        }
        self.get(name).await
    }

    /// Set the scheduler replica count for a warehouse (M7.3). Replicas >1
    /// requires a shared Postgres cluster state (see `create`); a running
    /// warehouse's schedulers are restarted so the replica set takes effect.
    ///
    /// # Errors
    ///
    /// Errors when replicas > 1 but no `STATESTORE_PG_URL` is configured, or
    /// the warehouse is unknown.
    pub async fn set_scheduler_replicas(&self, name: &str, replicas: u16) -> Result<Warehouse> {
        let replicas = replicas.max(1).min(MAX_SCHEDULER_REPLICAS);
        if replicas >= 2 && !shared_state_url_configured() {
            return Err(ConfigSnafu {
                message: "SCHEDULER_REPLICAS > 1 requires STATESTORE_PG_URL (or DATABASE_URL)                           on the router: replicas share executor/job state through Postgres"
                    .to_string(),
            }
            .build());
        }
        let was_running = self.state(name).await? == WarehouseState::Running;
        let old_replicas = self.record(name).await?.scheduler_replicas;
        let mut record = self.record(name).await?;
        record.scheduler_replicas = replicas;
        record.updated_at = Some(now_secs());
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;
        if was_running {
            if let Some(size) = WarehouseSize::parse(&self.record(name).await?.size) {
                self.stop_schedulers(name, old_replicas.max(replicas)).await?;
                self.start_compute(name, size, WarehouseState::Running).await?;
            }
        }
        self.get(name).await
    }

    pub async fn suspend(&self, name: &str) -> Result<Warehouse> {
        let record = self.record(name).await?;
        if self.state(name).await? == WarehouseState::Running {
            let size = WarehouseSize::parse(&record.size).ok_or_else(|| InvalidSizeSnafu {
                size: record.size.clone(),
            }
            .build())?;
            // Bill the running window before the executors go down (M5).
            self.accrue_running_window(&record.name, size.executor_count())
                .await;
            for i in 0..size.executor_count() {
                self.driver
                    .stop(&executor_container_name(name, i))
                    .await?;
            }
            info!(warehouse = name, "warehouse suspended (executors to 0)");
        }
        self.set_state(name, WarehouseState::Suspended).await
    }

    /// Start compute for a suspended/stopped warehouse. Client-facing calls
    /// block until the pool is up (Snowflake-like resume).
    pub async fn resume(&self, name: &str) -> Result<Warehouse> {
        let record = self.record(name).await?;
        let size = WarehouseSize::parse(&record.size)
            .ok_or_else(|| InvalidSizeSnafu {
                size: record.size.clone(),
            }
            .build())?;
        self.start_compute(name, size, WarehouseState::Running).await?;
        self.touch(name).await;
        self.get(name).await
    }

    /// Ballista scheduler URL for a host-side client (embucketd runs on the
    /// docker host, so it dials the mapped host port on `127.0.0.1`).
    ///
    /// ballista-core 55 accepts `http://host:port` scheduler URLs
    /// (`BallistaQueryPlanner::new(scheduler_url: String)`, which is handed to
    /// `tonic::transport::Endpoint::new` and parsed as a `Url`).
    pub async fn scheduler_url(&self, name: &str) -> Result<String> {
        // resolve the record's exact key: session variables may differ in
        // case (`USE WAREHOUSE etl` vs stored `ETL`)
        let record = self
            .find_record(name)
            .await
            .ok_or_else(|| NotFoundSnafu { name: name.to_string() }.build())?;
        // Replicas >= 2 (M7.3): advertise the shared network alias instead of
        // one mapped host port. The docker driver pins the alias to *every*
        // replica's bridge IP in /etc/hosts; the router's connector iterates
        // resolved addresses until one accepts — that iteration is the
        // failover. (k8s: the per-warehouse Service name load-balances.)
        if record.scheduler_replicas >= 2 {
            let alias = scheduler_container_name(&record.name);
            let scheme = if record.tls { "https" } else { "http" };
            return Ok(format!("{scheme}://{alias}:{INTERNAL_SCHEDULER_PORT}"));
        }
        // docker driver: mapped host port on loopback; k8s driver: scheduler
        // pod IP (re-resolved per call, so restarts don't leave stale URLs)
        let url = self
            .driver
            .scheduler_url(&record.name, record.scheduler_host_port)
            .await?;
        // TLS warehouses advertise over https (M7.2): the router keys its
        // session TLS config off the scheme.
        if record.tls && let Some(rest) = url.strip_prefix("http://") {
            return Ok(format!("https://{rest}"));
        }
        Ok(url)
    }

    pub async fn drop_warehouse(&self, name: &str) -> Result<()> {
        let record = self.record(name).await?;
        // Bill the running window before teardown (M5).
        if matches!(self.state(name).await, Ok(WarehouseState::Running))
            && let Some(size) = WarehouseSize::parse(&record.size)
        {
            self.accrue_running_window(&record.name, size.executor_count())
                .await;
            self.running_since.lock().await.remove(name);
        }
        // stop executors regardless of state; stop scheduler too (full teardown)
        if let Some(size) = WarehouseSize::parse(&record.size) {
            for i in 0..size.executor_count() {
                self.driver
                    .stop(&executor_container_name(name, i))
                    .await?;
            }
        }
        self.stop_schedulers(name, record.scheduler_replicas).await?;
        self.state_store
            .delete_warehouse(name)
            .await
            .context(StateStoreSnafu)?;
        self.last_activity.lock().await.remove(name);
        Ok(())
    }

    pub async fn get(&self, name: &str) -> Result<Warehouse> {
        let record = self.record(name).await?;
        Ok(Warehouse {
            name: record.name,
            size: WarehouseSize::parse(&record.size)
                .ok_or_else(|| InvalidSizeSnafu {
                    size: record.size.clone(),
                }
                .build())?,
            state: self.state(name).await?,
            auto_suspend_secs: Some(record.auto_suspend_secs),
            auto_resume: record.auto_resume,
            created_at: record.created_at,
            storage_summary: record
                .storage_credentials
                .as_ref()
                .map(StorageCredentials::summary),
            tls: record.tls,
            scheduler_replicas: record.scheduler_replicas,
        })
    }

    pub async fn list(&self) -> Result<Vec<Warehouse>> {
        let records = self
            .state_store
            .list_warehouses()
            .await
            .context(StateStoreSnafu)?;
        let mut warehouses = Vec::with_capacity(records.len());
        for record in records {
            warehouses.push(self.get(&record.name).await?);
        }
        warehouses.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(warehouses)
    }

    // ------------------------------------------------- query-path hooks

    /// Called from the query path. If the session targets a suspended
    /// warehouse with `AUTO_RESUME`, bring it back up (Snowflake-like
    /// resume-on-query). Never fails the query: unknown warehouses (dummy
    /// client profiles) and driver errors are logged and ignored.
    pub async fn ensure_running(&self, name: &str) {
        // resolve the record's exact key: session variables may differ in
        // case (`USE WAREHOUSE etl` vs stored `ETL`)
        let Some(record) = self.find_record(name).await else {
            // not a managed warehouse — nothing to do
            return;
        };
        self.touch(&record.name).await;
        let state = WarehouseState::parse(&record.state).unwrap_or(WarehouseState::Stopped);
        if state != WarehouseState::Running
            && record.auto_resume
            && let Err(err) = self.resume(&record.name).await
        {
            tracing::warn!(warehouse = record.name, %err, "auto-resume failed");
        }
    }

    /// Record query activity for auto-suspend bookkeeping.
    pub async fn touch(&self, name: &str) {
        self.last_activity
            .lock()
            .await
            .insert(name.to_string(), Instant::now());
    }

    /// Spawn the auto-suspend background loop: suspends running warehouses
    /// whose `AUTO_SUSPEND` idle window has elapsed. Call once at startup.
    pub fn spawn_auto_suspend(self: &Arc<Self>, period_secs: u64) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                period_secs.max(1),
            ));
            loop {
                interval.tick().await;
                if let Err(err) = manager.auto_suspend_tick().await {
                    tracing::warn!(%err, "auto-suspend tick failed");
                }
            }
        })
    }

    async fn auto_suspend_tick(&self) -> Result<()> {
        let now = Instant::now();
        let suspended: Vec<String> = {
            let activity = self.last_activity.lock().await;
            let mut out = Vec::new();
            for warehouse in self.list().await? {
                if warehouse.state != WarehouseState::Running {
                    continue;
                }
                // Metering: bill the running window incrementally each tick
                // (M5). Also adopts RUNNING warehouses found after a process
                // restart (no window start yet — start billing from now).
                if !self.running_since.lock().await.contains_key(&warehouse.name) {
                    self.running_since
                        .lock()
                        .await
                        .insert(warehouse.name.clone(), now_secs());
                } else {
                    self.accrue_running_window(&warehouse.name, warehouse.executor_count())
                        .await;
                }
                let Some(auto_suspend) = warehouse.auto_suspend_secs.filter(|s| *s > 0) else {
                    continue;
                };
                let idle = activity
                    .get(&warehouse.name)
                    .map_or(
                        // a running warehouse never touched by this manager
                        // instance: treat creation as activity
                        0,
                        |t| now.duration_since(*t).as_secs(),
                    );
                if idle >= auto_suspend {
                    out.push(warehouse.name);
                }
            }
            out
        };
        for name in suspended {
            tracing::info!(warehouse = name, "auto-suspending idle warehouse");
            self.suspend(&name).await?;
        }
        Ok(())
    }

    // -------------------------------------------------------- internals

    /// Start the scheduler (unless already running) and the executor pool for
    /// `size`, then persist `state`.
    async fn start_compute(
        &self,
        name: &str,
        size: WarehouseSize,
        state: WarehouseState,
    ) -> Result<()> {
        let base_sched_name = scheduler_container_name(name);
        // The scheduler's host port is fixed at creation and persisted on the
        // record, so suspend/resume cycles keep the same mapping.
        let record = self.record(name).await.ok();
        let scheduler_host_port = record.as_ref().and_then(|r| r.scheduler_host_port);
        // Per-warehouse storage credentials (M7.1): injected as container env
        // at start — never serialized into plans. Both scheduler and
        // executors get them (the scheduler decodes/re-plans scans).
        let storage_env: Vec<(String, String)> = record
            .as_ref()
            .and_then(|r| r.storage_credentials.as_ref())
            .map(StorageCredentials::env_vars)
            .unwrap_or_default();
        // Mutual TLS (M7.2): mount the router's TLS material into the
        // containers (read-only, CA private key excluded) and point the
        // binaries at it. Volumes are (host path, container path) pairs.
        let (mut container_env, container_volumes): (
            Vec<(String, String)>,
            Vec<(String, String)>,
        ) = if record.as_ref().is_some_and(|r| r.tls) {
            let mut env = vec![(TLS_DIR_ENV.to_string(), CONTAINER_TLS_DIR.to_string())];
            env.extend(storage_env);
            (env, self.tls_volumes().await?)
        } else {
            (storage_env, Vec::new())
        };
        container_env.sort();
        // Scheduler replicas (M7.3): replica 0 keeps the historical container
        // name and the mapped host port; replicas 1..n share the base name as
        // a docker network alias so executors dial one name for the whole
        // replica set, and the alias keeps resolving while one replica lives.
        // Replicas >1 share executor/job state through Postgres (validated at
        // create/ALTER time); the env hands each replica its URL.
        let replicas = record
            .as_ref()
            .map_or(1, |r| r.scheduler_replicas.max(1));
        let shared_env: Vec<(String, String)> = if replicas >= 2 {
            match self.shared_state_env().await {
                Some(env) => vec![env],
                None => {
                    return Err(ConfigSnafu {
                        message: format!(
                            "warehouse {name} has SCHEDULER_REPLICAS = {replicas} but no                              container-reachable STATESTORE_PG_URL/DATABASE_URL: replicas                              cannot share cluster state"
                        ),
                    }
                    .build());
                }
            }
        } else {
            Vec::new()
        };
        for replica in 0..replicas {
            let sched_name = scheduler_replica_name(name, replica);
            let mut sched_env = container_env.clone();
            sched_env.extend(shared_env.iter().cloned());
            sched_env.sort();
            self.driver
                .start(&ContainerSpec {
                    name: sched_name.clone(),
                    image: self.images.scheduler.clone(),
                    args: vec![
                        "--bind-host".to_string(),
                        "0.0.0.0".to_string(),
                        "--bind-port".to_string(),
                        INTERNAL_SCHEDULER_PORT.to_string(),
                        // executors in the same docker network connect by
                        // container name / shared alias
                        "--external-host".to_string(),
                        sched_name.clone(),
                    ],
                    env: sched_env,
                    volumes: container_volumes.clone(),
                    ports: (replica == 0)
                        .then(|| {
                            scheduler_host_port.map_or_else(Vec::new, |host_port| {
                                vec![(host_port, INTERNAL_SCHEDULER_PORT)]
                            })
                        })
                        .unwrap_or_default(),
                    network: self.network.clone(),
                    network_aliases: (replicas >= 2)
                        .then(|| vec![base_sched_name.clone()])
                        .unwrap_or_default(),
                })
                .await?;
        }

        let target = if state == WarehouseState::Running {
            size.executor_count()
        } else {
            0
        };
        for i in 0..target {
            let exec_name = executor_container_name(name, i);
            let mut exec_args = vec![
                "--bind-host".to_string(),
                "0.0.0.0".to_string(),
                "--scheduler-host".to_string(),
                base_sched_name.clone(),
                "--scheduler-port".to_string(),
                INTERNAL_SCHEDULER_PORT.to_string(),
                // Tight heartbeats (M7.3): the registration scheduler may die
                // mid-life; the survivor only sees the executor again on its
                // next heartbeat (self-registration from heartbeat metadata),
                // so a short interval bounds the HA failover window.
                "--executor-heartbeat-interval-seconds".to_string(),
                "10".to_string(),
            ];
            // TLS warehouses (M7.2): executors advertise their container DNS
            // name instead of the default container IP, so peers that dial
            // their Flight endpoint verify the certificate against a SAN we
            // can provision in advance (DNS names of the warehouse's own
            // containers). The docker network resolves these names for
            // scheduler-side fetches and cross-executor shuffle reads.
            if record.as_ref().is_some_and(|r| r.tls) {
                exec_args.push("--external-host".to_string());
                exec_args.push(exec_name.clone());
            }
            self.driver
                .start(&ContainerSpec {
                    name: exec_name,
                    image: self.images.executor.clone(),
                    args: exec_args,
                    env: container_env.clone(),
                    volumes: container_volumes.clone(),
                    ports: vec![],
                    network: self.network.clone(),
                    network_aliases: Vec::new(),
                })
                .await?;
        }

        self.set_state(name, state).await?;
        if state == WarehouseState::Running {
            // Begin (or restart) the billed running window (M5 metering).
            self.running_since
                .lock()
                .await
                .insert(name.to_string(), now_secs());
        }
        Ok(())
    }

    /// Bill `running_since → now` for a RUNNING warehouse at
    /// `executor_count` credits/hour, then reset the window start. No-op when
    /// the warehouse isn't running or the window is empty. Best-effort:
    /// metering failures must never break warehouse lifecycle.
    async fn accrue_running_window(&self, name: &str, executor_count: u32) {
        let window_start = self.running_since.lock().await.get(name).copied();
        let Some(start) = window_start else {
            return;
        };
        let now = now_secs();
        if now > start && executor_count > 0
            && let Err(err) = self.ledger.accrue(name, executor_count, start, now).await
        {
            tracing::warn!(warehouse = name, %err, "metering accrual failed");
        }
        self.running_since.lock().await.insert(name.to_string(), now);
    }

    /// Stop the scheduler containers `base` plus `-1..replicas` (M7.3).
    async fn stop_schedulers(&self, name: &str, replicas: u16) -> Result<()> {
        let replicas = replicas.max(1);
        for i in (0..replicas).rev() {
            let sched = scheduler_replica_name(name, i);
            self.driver.stop(&sched).await?;
        }
        Ok(())
    }

    /// `AVALANCHE_CLUSTER_PG_URL` env for scheduler containers (M7.3): the
    /// router's own state-store URL, rewritten for container reachability
    /// (`localhost` → docker bridge gateway). `None` = not configured or the
    /// rewrite is impossible.
    async fn shared_state_env(&self) -> Option<(String, String)> {
        let url = shared_state_url()?;
        if !pg_url_host_is_localhost(&url) {
            return Some((CLUSTER_STATE_PG_ENV.to_string(), url));
        }
        let gateway = self.driver.network_gateway(self.network.as_deref()).await?;
        let rewritten = rewrite_localhost_pg_url(&url, &gateway)?;
        Some((CLUSTER_STATE_PG_ENV.to_string(), rewritten))
    }

    async fn record(&self, name: &str) -> Result<WarehouseRecord> {
        self.state_store
            .get_warehouse(name)
            .await
            .context(StateStoreSnafu)
    }

    /// Host→container read-only mounts for TLS warehouses (M7.2): the five
    /// material files, never `ca.key`. Validates the files exist so TLS
    /// warehouses fail fast at start/ALTER rather than at first dial.
    async fn tls_volumes(&self) -> Result<Vec<(String, String)>> {
        let dir = TlsMaterial::dir_from_env().ok_or_else(|| {
            TlsSnafu {
                message: format!(
                    "TLS warehouse requires {TLS_DIR_ENV} on the router process \
                     (run scripts/gen_certs.sh and point the env at the output dir)"
                ),
            }
            .build()
        })?;
        let files = ["ca.crt", "server.crt", "server.key", "client.crt", "client.key"];
        for file in files {
            let path = dir.join(file);
            if !path.is_file() {
                return Err(TlsSnafu {
                    message: format!("missing TLS material file {}", path.display()),
                }
                .build());
            }
        }
        Ok(files
            .into_iter()
            .map(|file| {
                (
                    dir.join(file).to_string_lossy().to_string(),
                    format!("{CONTAINER_TLS_DIR}/{file}"),
                )
            })
            .collect())
    }

    async fn state(&self, name: &str) -> Result<WarehouseState> {
        let record = self.record(name).await?;
        WarehouseState::parse(&record.state)
            .ok_or_else(|| crate::error::InvalidStateSnafu {
                state: record.state.clone(),
            }
            .build())
    }

    async fn set_state(&self, name: &str, state: WarehouseState) -> Result<Warehouse> {
        let mut record = self.record(name).await?;
        record.state = state.as_str().to_string();
        record.updated_at = Some(now_secs());
        self.state_store
            .put_warehouse(record)
            .await
            .context(StateStoreSnafu)?;
        self.get(name).await
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Grab a free TCP port on the host by binding an ephemeral listener and
/// immediately releasing it (best-effort; the port can still be raced by
/// other processes between release and container start).
fn allocate_free_host_port() -> Option<u16> {
    std::net::TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok().map(|addr| addr.port()))
}

/// Container name of scheduler replica `replica` (M7.3): replica 0 keeps the
/// historical name; replicas >= 1 get a `-<n>` suffix.
#[must_use]
pub fn scheduler_replica_name(warehouse: &str, replica: u16) -> String {
    let base = scheduler_container_name(warehouse);
    if replica == 0 {
        base
    } else {
        format!("{base}-{replica}")
    }
}

/// The configured shared-state Postgres URL (`STATESTORE_PG_URL`, falling
/// back to `DATABASE_URL`), if any.
#[must_use]
pub fn shared_state_url() -> Option<String> {
    std::env::var("STATESTORE_PG_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

/// Whether a shared-state URL is configured at all (validation gate for
/// `SCHEDULER_REPLICAS > 1`).
#[must_use]
pub fn shared_state_url_configured() -> bool {
    shared_state_url().is_some()
}

/// Host portion of a `postgres://[user[:pass]@]host[:port][/db]` URL.
#[must_use]
pub fn pg_url_host(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))?;
    let hostport = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
    hostport
        .split(['/', '?'])
        .next()
        .and_then(|hp| hp.split(':').next())
        .filter(|h| !h.is_empty())
}

/// True when the URL's host must be rewritten for container reachability
/// (loopback only resolves inside the router process).
#[must_use]
pub fn pg_url_host_is_localhost(url: &str) -> bool {
    pg_url_host(url).is_some_and(|h| matches!(h, "localhost" | "127.0.0.1" | "::1"))
}

/// Rewrite a loopback host in a `postgres://` URL to `ip` (docker bridge
/// gateway) so containers reach the shared cluster-state store.
#[must_use]
pub fn rewrite_localhost_pg_url(url: &str, ip: &str) -> Option<String> {
    let host = pg_url_host(url)?;
    if !pg_url_host_is_localhost(url) {
        return Some(url.to_string());
    }
    let scheme_end = url.find("://")? + 3;
    let (scheme, rest) = url.split_at(scheme_end);
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let tail = &rest[authority_end..];
    let new_authority = match authority.rsplit_once('@') {
        Some((userinfo, hostport)) => {
            format!("{userinfo}@{}", hostport.replacen(host, ip, 1))
        }
        None => authority.replacen(host, ip, 1),
    };
    Some(format!("{scheme}{new_authority}{tail}"))
}

pub fn scheduler_container_name(warehouse: &str) -> String {
    format!("avalanche-wh-{warehouse}-scheduler")
}

#[must_use]
pub fn executor_container_name(warehouse: &str, index: u32) -> String {
    format!("avalanche-wh-{warehouse}-exec-{index}")
}
