//! Warehouse lifecycle management (Avalanche design §5).
//!
//! One warehouse = one Ballista scheduler + an executor pool. Suspension
//! scales executors to 0 (scheduler stays up); a fully stopped warehouse is
//! cold (scheduler down too). Resume-on-query is [`WarehouseManager::ensure_running`].

use crate::driver::{ContainerDriver, ContainerSpec};
use crate::error::{
    AlreadyExistsSnafu, InvalidAutoSuspendSnafu, InvalidSizeSnafu, NotFoundSnafu, Result,
    StateStoreSnafu,
};
use crate::metering::MeteringLedger;
use crate::model::{Warehouse, WarehouseSize, WarehouseState};
use snafu::ResultExt;
use state_store::StateStore;
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
        // docker driver: mapped host port on loopback; k8s driver: scheduler
        // pod IP (re-resolved per call, so restarts don't leave stale URLs)
        self.driver
            .scheduler_url(&record.name, record.scheduler_host_port)
            .await
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
        self.driver.stop(&scheduler_container_name(name)).await?;
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
        let sched_name = scheduler_container_name(name);
        // The scheduler's host port is fixed at creation and persisted on the
        // record, so suspend/resume cycles keep the same mapping.
        let scheduler_host_port = self
            .record(name)
            .await
            .ok()
            .and_then(|record| record.scheduler_host_port);
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
                    // container name
                    "--external-host".to_string(),
                    sched_name.clone(),
                ],
                env: vec![],
                ports: scheduler_host_port
                    .map_or_else(Vec::new, |host_port| {
                        vec![(host_port, INTERNAL_SCHEDULER_PORT)]
                    }),
                network: self.network.clone(),
            })
            .await?;

        let target = if state == WarehouseState::Running {
            size.executor_count()
        } else {
            0
        };
        for i in 0..target {
            self.driver
                .start(&ContainerSpec {
                    name: executor_container_name(name, i),
                    image: self.images.executor.clone(),
                    args: vec![
                        "--bind-host".to_string(),
                        "0.0.0.0".to_string(),
                        "--scheduler-host".to_string(),
                        sched_name.clone(),
                        "--scheduler-port".to_string(),
                        INTERNAL_SCHEDULER_PORT.to_string(),
                    ],
                    env: vec![],
                    ports: vec![],
                    network: self.network.clone(),
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

    async fn record(&self, name: &str) -> Result<WarehouseRecord> {
        self.state_store
            .get_warehouse(name)
            .await
            .context(StateStoreSnafu)
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

#[must_use]
pub fn scheduler_container_name(warehouse: &str) -> String {
    format!("avalanche-wh-{warehouse}-scheduler")
}

#[must_use]
pub fn executor_container_name(warehouse: &str, index: u32) -> String {
    format!("avalanche-wh-{warehouse}-exec-{index}")
}
