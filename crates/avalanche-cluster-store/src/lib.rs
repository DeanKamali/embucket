//! Postgres-backed ballista cluster/job state for scheduler HA (M7.3).
//!
//! `ballista-scheduler`'s shipped backends are in-memory only, which pins
//! every warehouse to a single scheduler process. This crate implements the
//! [`ClusterState`] and [`JobState`] traits over shared Postgres tables so
//! several schedulers can serve one warehouse through a common network
//! alias: any replica can schedule, and when one dies the survivors already
//! see every executor (heartbeats) and job (status) through the database.
//!
//! Semantics mirror `ballista-scheduler`'s `InMemoryClusterState` /
//! `InMemoryJobState` closely (including error strings), with three v1
//! limitations that are documented design decisions (DESIGN.md §13.3):
//!
//! 1. Execution graphs are **not** persisted (`ballista_jobs.graph` stays
//!    NULL); completed graphs are cached locally only, so a job planned by
//!    a scheduler that dies is lost (client gets a bounded error).
//! 2. `try_acquire_job` returns `None` (no job handover).
//! 3. Cross-replica job/executor discovery happens through a fast local
//!    mirror refreshed from the database in the background; `JobState`
//!    events are only emitted for locally-owned jobs.

use ballista_core::error::Result as BallistaResult;
use ballista_core::ConfigProducer;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::scheduler_server::SessionBuilder;

pub mod bind;
pub mod cluster_state;
pub mod job_state;

pub use cluster_state::{pg_pool, PostgresClusterState, REFRESH_INTERVAL_MS};
pub use job_state::PostgresJobState;

/// Builds a Postgres-backed [`BallistaCluster`]: creates the pool, runs the
/// DDL, spawns the background mirror refresh, and wires both backends.
///
/// Mirrors [`ballista_scheduler::cluster::BallistaCluster::new_from_config`]
/// except the backends are `PostgresClusterState`/`PostgresJobState`.
pub struct PgClusterBuilder {
    url: String,
    scheduler: String,
    session_builder: SessionBuilder,
    config_producer: ConfigProducer,
    refresh_interval_ms: u64,
}

impl PgClusterBuilder {
    pub fn new(url: impl Into<String>, scheduler: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            scheduler: scheduler.into(),
            session_builder: std::sync::Arc::new(
                ballista_core::utils::default_session_builder,
            ),
            config_producer: std::sync::Arc::new(
                ballista_core::utils::default_config_producer,
            ),
            refresh_interval_ms: REFRESH_INTERVAL_MS,
        }
    }

    pub fn session_builder(mut self, sb: SessionBuilder) -> Self {
        self.session_builder = sb;
        self
    }

    pub fn config_producer(mut self, cp: ConfigProducer) -> Self {
        self.config_producer = cp;
        self
    }

    pub fn refresh_interval_ms(mut self, ms: u64) -> Self {
        self.refresh_interval_ms = ms;
        self
    }

    pub async fn build(self) -> BallistaResult<BallistaCluster> {
        let pool = pg_pool(&self.url, 4).await?;
        let job_state = PostgresJobState::new(
            pool.clone(),
            self.scheduler,
            self.session_builder,
            self.config_producer,
        );
        let cluster_state = PostgresClusterState::new(pool, self.refresh_interval_ms);
        cluster_state.init().await?;
        Ok(BallistaCluster::new(
            std::sync::Arc::new(cluster_state),
            std::sync::Arc::new(job_state),
        ))
    }
}