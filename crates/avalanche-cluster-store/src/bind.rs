//! Task-binding algorithm, copied verbatim from
//! `ballista-scheduler-55.0.0/src/cluster/mod.rs` (`bind_one`,
//! `bind_task_bias`, `bind_task_round_robin`, `stage_has_input_collapse`).
//! Those functions are `pub(crate)` upstream, so the Postgres backend here
//! cannot call them; on a ballista fork upgrade, diff this file against the
//! new `cluster/mod.rs` and re-copy.

use ballista_core::config::BallistaConfig;
use ballista_core::error::Result as BallistaResult;
use ballista_core::execution_plans::{RangeShuffleReaderExec, ShuffleReaderExec};
use ballista_core::serde::protobuf::AvailableVcores;
use ballista_core::serde::scheduler::TaskKey;
use ballista_core::JobId;
use ballista_scheduler::cluster::BoundTask;
use ballista_scheduler::state::execution_graph::create_task_info;
use ballista_scheduler::state::execution_stage::RunningStage;
use ballista_scheduler::state::task_manager::JobInfoCache;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) fn stage_has_input_collapse(plan_root: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &Arc<dyn ExecutionPlan>) -> bool {
        if node.downcast_ref::<ShuffleReaderExec>().is_some()
            || node.downcast_ref::<RangeShuffleReaderExec>().is_some()
        {
            return false;
        }
        if node.properties().output_partitioning().partition_count() == 1 {
            return true;
        }
        match node.children().as_slice() {
            [child] => walk(child),
            _ => false,
        }
    }
    let result = match plan_root.children().as_slice() {
        [child] => walk(child),
        _ => false,
    };
    tracing::debug!(
        "stage_has_input_collapse: root={} root_partitions={} → {result}",
        plan_root.name(),
        plan_root
            .properties()
            .output_partitioning()
            .partition_count(),
    );
    result
}

fn bind_one(
    running_stage: &mut RunningStage,
    session_id: &str,
    job_id: &JobId,
    budget: &mut AvailableVcores,
) -> Option<BoundTask> {
    let is_collapse = stage_has_input_collapse(&running_stage.plan);
    // Cap non-collapse slices at the configured `max_partitions_per_task`.
    // Collapse stages must still pack their full pending queue into a single
    // task for correctness — a split collapse would produce partial results
    // downstream can't merge.
    let cap = running_stage
        .session_config
        .options()
        .extensions
        .get::<BallistaConfig>()
        .map(|bc| bc.max_partitions_per_task())
        .filter(|&n| n > 0)
        .unwrap_or(usize::MAX);
    let max_partitions = if is_collapse {
        usize::MAX
    } else {
        (budget.vcores as usize).min(cap)
    };
    let input_partition_ids = running_stage.pending.next_slice(max_partitions);
    if input_partition_ids.is_empty() {
        return None;
    }
    // Non-collapse: DataFusion's volcano/pull model drives one tokio task per
    // root output partition, so N input partitions bundled into a task need
    // N vcores of concurrent execution. Collapse: the plan's root has a
    // single output partition, so only 1 thread is ever active for the
    // whole pipeline no matter how many inputs the task packs — reserve
    // one vcore and leave the rest for other stages' tasks on this executor.
    let vcores_consumed = if is_collapse {
        1
    } else {
        input_partition_ids.len() as u32
    };
    tracing::debug!(
        "bind_one: job={} stage={} exec={} vcores={} slice_len={} consumed={} collapse={}",
        job_id,
        running_stage.stage_id,
        budget.executor_id,
        budget.vcores,
        input_partition_ids.len(),
        vcores_consumed,
        is_collapse,
    );
    let executor_id = budget.executor_id.clone();
    // task_id is the append-order slot in `task_infos` — since we're
    // about to push, that's `task_infos.len()`. `(job_id, stage_id,
    // task_id)` is globally unique.
    let task_id = running_stage.task_infos.len();
    let task_attempt = input_partition_ids
        .iter()
        .map(|pid| running_stage.task_failure_numbers[*pid])
        .max()
        .unwrap_or(0);
    let mut task_info = create_task_info(executor_id.clone(), task_id);
    task_info.global_input_partition_ids = input_partition_ids.clone();
    task_info.vcores_consumed = vcores_consumed;
    running_stage.task_infos.push(task_info);
    let key = TaskKey {
        job_id: job_id.clone(),
        stage_id: running_stage.stage_id,
        task_id,
    };
    let task_desc = ballista_scheduler::state::execution_graph::TaskDescription {
        session_id: session_id.to_string(),
        key,
        stage_attempt_num: running_stage.stage_attempt_num,
        task_attempt,
        global_input_partition_ids: input_partition_ids,
        vcores_consumed,
        plan: running_stage.plan.clone(),
        session_config: running_stage.session_config.clone(),
    };
    budget.vcores -= vcores_consumed;
    Some((executor_id, task_desc))
}

/// Bias policy: pack tasks onto the largest executor until its vcore budget
/// drains, then move to the next-largest.
pub(crate) async fn bind_task_bias(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> BallistaResult<Vec<BoundTask>> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    if budgets.iter().all(|b| b.vcores == 0) {
        tracing::debug!("No executor vcores available for task binding");
        return Ok(schedulable_tasks);
    }

    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(ballista_core::serde::protobuf::job_status::Status::Running(_))) {
            tracing::debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                tracing::debug!(
                    "Will skip stage {}/{} for bias task binding",
                    job_id,
                    running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }
            while idx < budgets.len() {
                while idx < budgets.len() && budgets[idx].vcores == 0 {
                    idx += 1;
                }
                if idx >= budgets.len() {
                    return Ok(schedulable_tasks);
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some(bound) => schedulable_tasks.push(bound),
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    Ok(schedulable_tasks)
}

/// Round-robin policy: rotate across executors so multiple running stages
/// each get one executor per rotation.
pub(crate) async fn bind_task_round_robin(
    mut budgets: Vec<&mut AvailableVcores>,
    running_jobs: Arc<HashMap<JobId, JobInfoCache>>,
    if_skip: fn(Arc<dyn ExecutionPlan>) -> bool,
) -> BallistaResult<Vec<BoundTask>> {
    let mut schedulable_tasks: Vec<BoundTask> = vec![];

    if budgets.iter().all(|b| b.vcores == 0) {
        tracing::debug!("No executor vcores available for task binding");
        return Ok(schedulable_tasks);
    }

    budgets.sort_by(|a, b| Ord::cmp(&b.vcores, &a.vcores));

    let mut idx = 0usize;
    for (job_id, job_info) in running_jobs.iter() {
        if !matches!(job_info.status, Some(ballista_core::serde::protobuf::job_status::Status::Running(_))) {
            tracing::debug!("Job {job_id} is not in running status and will be skipped");
            continue;
        }
        let mut graph = job_info.execution_graph.write().await;
        let session_id = graph.session_id().to_string();
        let mut black_list = vec![];
        while let Some(running_stage) = graph.fetch_running_stage(&black_list) {
            if if_skip(running_stage.plan.clone()) {
                tracing::debug!(
                    "Will skip stage {}/{} for round robin task binding",
                    job_id,
                    running_stage.stage_id
                );
                black_list.push(running_stage.stage_id);
                continue;
            }
            loop {
                let mut scanned = 0usize;
                while budgets[idx].vcores == 0 {
                    idx = (idx + 1) % budgets.len();
                    scanned += 1;
                    if scanned >= budgets.len() {
                        return Ok(schedulable_tasks);
                    }
                }
                match bind_one(running_stage, &session_id, job_id, &mut *budgets[idx]) {
                    Some(bound) => {
                        schedulable_tasks.push(bound);
                        idx = (idx + 1) % budgets.len();
                    }
                    None => break, // stage's pending is drained
                }
            }
        }
    }

    Ok(schedulable_tasks)
}