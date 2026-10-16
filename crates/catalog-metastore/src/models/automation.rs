//! Automation objects (design §18, M11): streams, tasks, dynamic tables.
//!
//! All three share the schema object namespace (keyed by [`TableIdent`],
//! where the `table` field carries the object name) and are stored in the
//! metastore alongside tables, so the PG persistence follow-up covers them.

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::TableIdent;

/// A stream over an Iceberg table's change feed (design §18.1-1).
///
/// The stream stores only an offset — the source snapshot id at creation
/// (`None` = the source had no snapshots yet). Change rows are derived
/// lazily at consume time from the fork's `change_manifests(offset,
/// current]`; nothing is materialized.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stream {
    pub ident: TableIdent,
    /// The table the stream reads changes from.
    pub source: TableIdent,
    /// Source snapshot id the stream starts after; `None` = the source had
    /// no snapshots at creation (stream is empty until the first commit).
    pub offset_snapshot: Option<i64>,
    /// APPEND_ONLY streams surface only added files (ACTION always INSERT).
    pub append_only: bool,
    pub comment: Option<String>,
}

/// Task schedule (design §18.1-4): Snowflake-style `SCHEDULE` clause.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskSchedule {
    /// 5-field cron expression, UTC (Snowflake shape: `M H dom mon dow`).
    Cron(String),
    /// Every `N` minutes, measured from the previous run.
    Minutes(i64),
}

impl Display for TaskSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cron(expr) => write!(f, "USING CRON {expr} UTC"),
            Self::Minutes(n) => write!(f, "USING SCHEDULE {n} MINUTE"),
        }
    }
}

/// Whether a task is eligible for scheduled execution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    Started,
    Suspended,
}

/// Outcome of the most recent run (scheduled or `EXECUTE TASK`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskRunState {
    Succeeded,
    Failed,
}

/// A scheduled warehouse query (design §18.1-4).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Task {
    pub ident: TableIdent,
    /// The statement body (single SQL statement, v1).
    pub definition: String,
    pub schedule: Option<TaskSchedule>,
    pub state: TaskState,
    /// Warehouse the task runs on; `None` = scheduler default.
    pub warehouse: Option<String>,
    /// Predecessor task name (same schema) — runs after it succeeds.
    pub predecessor: Option<String>,
    /// Epoch-ms of the last scheduled run (`EXECUTE TASK` does not update
    /// the schedule clock).
    pub last_run_ms: Option<i64>,
    pub last_run_state: Option<TaskRunState>,
}

/// A dynamic table: a real Iceberg target table (registered separately in
/// the metastore) plus this definition record (design §18.1-5).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DynamicTable {
    pub ident: TableIdent,
    /// The SELECT query the target is refreshed from.
    pub definition: String,
    /// Warehouse refreshes run on; `None` = scheduler default.
    pub warehouse: Option<String>,
    /// Target lag in seconds (v1 constant-interval semantics).
    pub target_lag_seconds: Option<i64>,
    /// Epoch-ms of the last completed refresh.
    pub last_refresh_ms: Option<i64>,
    /// Refresh strategy (design §22.1-2): `Incremental` refreshes consume
    /// the base table's change feed for GROUP BY definitions; `Full` is
    /// the M11 full-overwrite refresh.
    #[serde(default)]
    pub refresh_mode: RefreshMode,
    /// Incremental mode only: the single base table the definition
    /// aggregates over.
    #[serde(default)]
    pub base_table: Option<TableIdent>,
    /// Incremental mode only: hidden change stream over `base_table`
    /// tracking the last consumed source snapshot.
    #[serde(default)]
    pub stream_ident: Option<TableIdent>,
}

/// Refresh strategy for a dynamic table (design §22.1-2).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RefreshMode {
    /// Full-overwrite refresh: one INSERT OVERWRITE commit from the whole
    /// definition (M11 behavior).
    #[default]
    Full,
    /// Incremental refresh: consume the base's change feed, recompute only
    /// affected groups, commit once (M16.2; GROUP BY definitions only).
    Incremental,
}