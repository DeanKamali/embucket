//! In-process automation scheduler (Avalanche design §18.1-4, M11): a tokio
//! loop that runs due tasks on their warehouse and refreshes dynamic tables
//! whose target lag has elapsed.
//!
//! Runs live inside the `CoreExecutionService` process. Task SQL executes
//! through a dedicated internal session (`authz: None` — established M10
//! internal semantics) with the task's database/schema context. All runs
//! are serialized within a tick (v1); a per-task name guard prevents an
//! overlapping scheduled run and a concurrent `EXECUTE TASK`.

use crate::models::QueryContext;
use crate::query::UserQuery;
use crate::service::CoreExecutionService;
use catalog_metastore::models::{DynamicTable, RwObject, TableIdent, Task, TaskRunState, TaskSchedule, TaskState};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, info, warn};

const SCHEDULER_INTERVAL_SECS: u64 = 10;
const SCHEDULER_SESSION_ID: &str = "__automation_scheduler";

/// Shared guard set: automation object keys currently executing.
#[derive(Debug, Default)]
struct RunningSet(Mutex<HashSet<String>>);

impl RunningSet {
    fn try_acquire(&self, key: &str) -> bool {
        self.0
            .lock()
            .map(|mut set| set.insert(key.to_string()))
            .unwrap_or(false)
    }

    fn release(&self, key: &str) {
        if let Ok(mut set) = self.0.lock() {
            set.remove(key);
        }
    }
}

/// Spawn the scheduler loop; it lives while the service lives (weak
/// reference) and exits when the service is dropped.
pub fn spawn(service: &Arc<CoreExecutionService>) {
    let weak = Arc::downgrade(service);
    tokio::spawn(async move {
        let running = Arc::new(RunningSet::default());
        let mut interval = tokio::time::interval(Duration::from_secs(SCHEDULER_INTERVAL_SECS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Some(service) = weak.upgrade() else {
                info!("automation scheduler: service dropped; exiting");
                break;
            };
            if let Err(e) = tick(&service, &running).await {
                error!(error = %e, "automation scheduler tick failed");
            }
        }
    });
    info!(interval_secs = SCHEDULER_INTERVAL_SECS, "automation scheduler started");
}

fn ident_string(ident: &TableIdent) -> String {
    let mut out = ident.database.clone();
    let _ = write!(out, ".{}.{}", ident.schema, ident.table);
    out
}

fn naive_ms(naive: NaiveDateTime) -> i64 {
    Utc.from_utc_datetime(&naive).timestamp_millis()
}

/// One scheduler tick: run due tasks (with predecessor chains) and refresh
/// lag-due dynamic tables.
async fn tick(
    service: &Arc<CoreExecutionService>,
    running: &RunningSet,
) -> crate::error::Result<()> {
    let metastore = service.metastore_clone();
    let now_ms = Utc::now().timestamp_millis();

    // --- Tasks ------------------------------------------------------------
    let tasks = metastore
        .list_all_tasks()
        .await
        .map_err(|e| crate::automation_ddl::automation_error("scheduler", e))?;
    let mut due: Vec<RwObject<Task>> = tasks
        .into_iter()
        .filter(|record| task_scheduled_due(record, now_ms))
        .collect();
    due.sort_by(|a, b| ident_string(&a.ident).cmp(&ident_string(&b.ident)));
    for record in due {
        run_task_tree(service, running, record, now_ms).await;
    }

    // --- Dynamic tables ----------------------------------------------------
    let dynamic_tables = metastore
        .list_all_dynamic_tables()
        .await
        .map_err(|e| crate::automation_ddl::automation_error("scheduler", e))?;
    let mut due_tables: Vec<RwObject<DynamicTable>> = dynamic_tables
        .into_iter()
        .filter(|record| dynamic_table_due(&record.data, now_ms))
        .collect();
    due_tables.sort_by(|a, b| ident_string(&a.ident).cmp(&ident_string(&b.ident)));
    for record in due_tables {
        let key = ident_string(&record.ident);
        if !running.try_acquire(&key) {
            continue;
        }
        let result = {
            let session = service
                .internal_automation_session(
                    SCHEDULER_SESSION_ID,
                    Some(record.ident.database.clone()),
                    Some(record.ident.schema.clone()),
                    record.data.warehouse.clone(),
                )
                .await?;
            crate::automation_ddl::refresh_dynamic_table(&session, &record.ident).await
        };
        running.release(&key);
        if let Err(e) = result {
            warn!(dynamic_table = %key, error = %e, "dynamic table refresh failed");
        }
    }
    Ok(())
}

/// Scheduled due-ness: started, has a schedule, no predecessor (children
/// are triggered by their parent's success, not by the clock). Base time
/// for the next fire is the last run, falling back to record creation.
fn task_scheduled_due(record: &RwObject<Task>, now_ms: i64) -> bool {
    let task = &record.data;
    if task.state != TaskState::Started {
        return false;
    }
    if task.predecessor.is_some() {
        return false;
    }
    let Some(schedule) = &task.schedule else {
        return false;
    };
    let base_ms = task.last_run_ms.unwrap_or_else(|| naive_ms(record.created_at));
    match schedule {
        TaskSchedule::Minutes(minutes) => now_ms >= base_ms + minutes * 60_000,
        TaskSchedule::Cron(expr) => cron_due(expr, base_ms, now_ms),
    }
}

/// Due when the schedule's next fire after `base_ms` has already passed.
fn cron_due(expr: &str, base_ms: i64, now_ms: i64) -> bool {
    let six_field = format!("0 {expr}");
    let Ok(schedule) = cron::Schedule::from_str(&six_field) else {
        return false; // validated at CREATE time; never fire on bad input
    };
    let (Some(base), Some(now)) = (
        DateTime::<Utc>::from_timestamp_millis(base_ms),
        DateTime::<Utc>::from_timestamp_millis(now_ms),
    ) else {
        return false;
    };
    schedule
        .after(&base)
        .next()
        .is_some_and(|next| next <= now)
}

fn dynamic_table_due(record: &DynamicTable, now_ms: i64) -> bool {
    let Some(lag) = record.target_lag_seconds else {
        return false;
    };
    // No refresh yet: due immediately (first convergence).
    let base_ms = record.last_refresh_ms.unwrap_or(0);
    now_ms >= base_ms + lag * 1_000
}

/// Execute a task, then (on success) its started children depth-first.
async fn run_task_tree(
    service: &Arc<CoreExecutionService>,
    running: &RunningSet,
    record: RwObject<Task>,
    now_ms: i64,
) {
    let key = ident_string(&record.ident);
    if !running.try_acquire(&key) {
        return;
    }
    let outcome = execute_task(service, &record, now_ms).await;
    running.release(&key);

    match outcome {
        Ok(()) => {
            // Children run after parent success (§18.1-4), same schema.
            let metastore = service.metastore_clone();
            let Ok(all) = metastore.list_all_tasks().await else {
                return;
            };
            let mut children: Vec<RwObject<Task>> = all
                .into_iter()
                .filter(|child| {
                    child.data.state == TaskState::Started
                        && child.data.predecessor.as_deref() == Some(record.ident.table.as_str())
                        && child.ident.database == record.ident.database
                        && child.ident.schema == record.ident.schema
                })
                .collect();
            children.sort_by(|a, b| ident_string(&a.ident).cmp(&ident_string(&b.ident)));
            for child in children {
                Box::pin(run_task_tree(service, running, child, now_ms)).await;
            }
        }
        Err(e) => {
            warn!(task = %key, error = %e, "scheduled task run failed");
        }
    }
}

/// Execute one task's definition on its warehouse and record the outcome.
/// `last_run_ms` advances on success AND failure (a failing task must not
/// re-fire every tick).
async fn execute_task(
    service: &Arc<CoreExecutionService>,
    record: &RwObject<Task>,
    now_ms: i64,
) -> crate::error::Result<()> {
    let task = &record.data;
    let session = service
        .internal_automation_session(
            SCHEDULER_SESSION_ID,
            Some(record.ident.database.clone()),
            Some(record.ident.schema.clone()),
            task.warehouse.clone(),
        )
        .await?;
    let definition = task.definition.clone();
    let run_result = crate::automation_ddl::run_nested(
        &session,
        definition,
        QueryContext::new(
            Some(record.ident.database.clone()),
            Some(record.ident.schema.clone()),
            None,
        ),
        "scheduled task",
    )
    .await;

    let metastore = service.metastore_clone();
    let updated = Task {
        last_run_ms: Some(now_ms),
        last_run_state: Some(match &run_result {
            Ok(_) => TaskRunState::Succeeded,
            Err(_) => TaskRunState::Failed,
        }),
        ..task.clone()
    };
    metastore
        .update_task(&record.ident, updated)
        .await
        .map_err(|e| crate::automation_ddl::automation_error("scheduler", e))?;
    run_result
        .map(|_| ())
        .map_err(|e| crate::automation_ddl::automation_error("scheduler", e))
}