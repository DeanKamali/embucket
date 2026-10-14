//! Postgres-backed [`JobState`] for scheduler HA (M7.3, DESIGN.md §13.3).
//!
//! Job *status* metadata (queued/running/completed) is persisted in the
//! shared `ballista_jobs` table so any replica can answer `get_job_status`
//! for any job, and status survives a scheduler crash. Execution graphs are
//! deliberately **not** persisted in v1 (`graph` stays NULL, completed
//! graphs are cached locally only): a job planned by a scheduler that dies
//! is lost — the client gets a bounded error, which is the documented v1
//! limitation (job handover / `try_acquire_job` returns `None`, mirroring
//! the in-memory backend).

use ballista_core::error::{BallistaError, Result as BallistaResult};
use ballista_core::serde::protobuf::{
    job_status::Status, FailedJob, JobStatus, QueuedJob,
};
use ballista_core::{ConfigProducer, JobId, JobStatusSubscriber};
use ballista_scheduler::cluster::event::ClusterEventSender;
use ballista_scheduler::cluster::{JobState, JobStateEvent, JobStateEventStream};
use ballista_scheduler::scheduler_server::SessionBuilder;
use ballista_scheduler::scheduler_server::{timestamp_millis, timestamp_secs};
use ballista_scheduler::state::execution_graph::ExecutionGraphBox;
use ballista_scheduler::state::session_manager::create_datafusion_context;
use datafusion::prelude::{SessionConfig, SessionContext};
use dashmap::DashMap;
use deadpool_postgres::Pool;
use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;

const JOBS_DDL: &str = "CREATE TABLE IF NOT EXISTS ballista_jobs (
    job_id      TEXT PRIMARY KEY,
    job_name    TEXT NOT NULL,
    queued_at   BIGINT NOT NULL,
    owner       TEXT NOT NULL,
    status_kind TEXT NOT NULL,
    status_pb   BYTEA NOT NULL,
    graph       BYTEA,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ)";

#[derive(Clone)]
struct ExtendedJobStatus {
    status: JobStatus,
    subscriber: Option<JobStatusSubscriber>,
}

impl ExtendedJobStatus {
    fn update_subscribers(&self, status: JobStatus) {
        let job_id = status.job_id.clone();
        if let Some(subscriber) = &self.subscriber
            && matches!(
                subscriber.try_send(status),
                Err(tokio::sync::mpsc::error::TrySendError::Full(_))
            )
        {
            // Buffer should be big enough for all cases; dropping here
            // matches the in-memory backend's behaviour.
            tracing::error!(
                "jobs notification subscriber for job {job_id} is blocked, can't deliver status update, job notification will be missed"
            );
        }
    }
}

pub struct PostgresJobState {
    pool: Pool,
    scheduler: String,
    session_builder: SessionBuilder,
    config_producer: ConfigProducer,
    job_event_sender: ClusterEventSender<JobStateEvent>,
    queued_jobs: DashMap<JobId, (String, u64)>,
    running_jobs: DashMap<JobId, ExtendedJobStatus>,
    completed_jobs: DashMap<JobId, (JobStatus, Option<ExecutionGraphBox>)>,
}

fn status_kind(status: &Status) -> &'static str {
    match status {
        Status::Queued(_) => "queued",
        Status::Running(_) => "running",
        Status::Failed(_) | Status::Successful(_) => "completed",
    }
}

/// The job's original queue timestamp, taken from the status it carries.
fn status_queued_at(status: &JobStatus) -> u64 {
    match &status.status {
        Some(Status::Queued(q)) => q.queued_at,
        Some(Status::Running(r)) => r.queued_at,
        Some(Status::Failed(f)) => f.queued_at,
        Some(Status::Successful(s)) => s.queued_at,
        None => timestamp_secs(),
    }
}

fn encode_status(status: &JobStatus) -> BallistaResult<Vec<u8>> {
    let mut pb = Vec::new();
    status
        .encode(&mut pb)
        .map_err(|e| BallistaError::Internal(format!("encode job status: {e}")))?;
    Ok(pb)
}

/// Durable upsert of a job row (shared by the async trait methods and the
/// fire-and-forget `accept_job` persistence task). `graph` stays NULL in v1.
async fn upsert_job(
    pool: &Pool,
    scheduler: &str,
    job_id: &str,
    job_name: &str,
    queued_at: u64,
    kind: &str,
    status: &JobStatus,
    completed: bool,
) -> BallistaResult<()> {
    let pb = encode_status(status)?;
    let conn = pool
        .get()
        .await
        .map_err(|e| BallistaError::Internal(format!("pg conn: {e}")))?;
    conn.execute(
        "INSERT INTO ballista_jobs
             (job_id, job_name, queued_at, owner, status_kind, status_pb, completed_at)
         VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $7 THEN now() ELSE NULL END)
         ON CONFLICT (job_id) DO UPDATE
           SET owner = EXCLUDED.owner,
               status_kind = EXCLUDED.status_kind,
               status_pb = EXCLUDED.status_pb,
               completed_at = EXCLUDED.completed_at",
        &[
            &job_id,
            &job_name,
            &(queued_at as i64),
            &scheduler,
            &kind,
            &pb,
            &completed,
        ],
    )
    .await
    .map_err(|e| BallistaError::Internal(format!("upsert job: {e}")))?;
    Ok(())
}

fn pg_err(what: &'static str) -> impl Fn(tokio_postgres::Error) -> BallistaError {
    move |e| BallistaError::Internal(format!("{what}: {e}"))
}

impl PostgresJobState {
    pub fn new(
        pool: Pool,
        scheduler: impl Into<String>,
        session_builder: SessionBuilder,
        config_producer: ConfigProducer,
    ) -> Self {
        Self {
            pool,
            scheduler: scheduler.into(),
            session_builder,
            config_producer,
            job_event_sender: ClusterEventSender::new(100),
            queued_jobs: DashMap::new(),
            running_jobs: DashMap::new(),
            completed_jobs: DashMap::new(),
        }
    }

    /// Runs the jobs DDL.
    pub async fn init(&self) -> BallistaResult<()> {
        let conn = self.conn().await?;
        conn.execute(JOBS_DDL, &[]).await.map_err(pg_err("jobs DDL"))?;
        Ok(())
    }

    async fn conn(&self) -> BallistaResult<deadpool_postgres::Object> {
        self.pool
            .get()
            .await
            .map_err(|e| BallistaError::Internal(format!("pg conn: {e}")))
    }

    async fn job_status_from_db(&self, job_id: &JobId) -> BallistaResult<Option<JobStatus>> {
        let conn = self.conn().await?;
        let rows = conn
            .query(
                "SELECT status_pb FROM ballista_jobs WHERE job_id = $1",
                &[&job_id.as_str()],
            )
            .await
            .map_err(pg_err("select job status"))?;
        rows.first()
            .map(|r| {
                let pb: Vec<u8> = r.get(0);
                JobStatus::decode(pb.as_slice())
                    .map_err(|e| BallistaError::Internal(format!("decode job status: {e}")))
            })
            .transpose()
    }

    async fn db_job_ids(&self, kind: Option<&str>) -> BallistaResult<HashSet<JobId>> {
        let conn = self.conn().await?;
        let rows = match kind {
            Some(kind) => {
                conn.query(
                    "SELECT job_id FROM ballista_jobs WHERE status_kind = $1",
                    &[&kind],
                )
                .await
                .map_err(pg_err("select job ids"))?
            }
            None => {
                conn.query("SELECT job_id FROM ballista_jobs", &[])
                    .await
                    .map_err(pg_err("select job ids"))?
            }
        };
        Ok(rows
            .iter()
            .map(|r| JobId::from(r.get::<_, String>(0)))
            .collect())
    }
}

#[async_trait::async_trait]
impl JobState for PostgresJobState {
    fn accept_job(&self, job_id: &JobId, job_name: &str, queued_at: u64) -> BallistaResult<()> {
        self.queued_jobs
            .insert(job_id.to_owned(), (job_name.to_owned(), queued_at));

        // Sync trait method → the durable row write is fire-and-forget;
        // submit_job's upsert reconciles any ordering race.
        let pool = self.pool.clone();
        let scheduler = self.scheduler.clone();
        let job_id = job_id.to_owned();
        let job_name = job_name.to_owned();
        tokio::spawn(async move {
            let status = JobStatus {
                job_id: job_id.clone().into(),
                job_name: job_name.clone(),
                status: Some(Status::Queued(QueuedJob { queued_at })),
            };
            if let Err(err) = upsert_job(&pool, &scheduler, job_id.as_ref(), &job_name,
                                          queued_at, "queued", &status, false)
                .await
            {
                tracing::warn!(job = %job_id, %err, "persisting queued job failed");
            }
        });
        Ok(())
    }

    fn pending_job_number(&self) -> usize {
        self.queued_jobs.len()
    }

    async fn submit_job(
        &self,
        job_id: JobId,
        graph: &ExecutionGraphBox,
        subscriber: Option<JobStatusSubscriber>,
    ) -> BallistaResult<()> {
        let Some(entry) = self.queued_jobs.get(&job_id) else {
            return Err(BallistaError::Internal(format!(
                "Failed to submit job {job_id}, not found in queued jobs"
            )));
        };
        let (job_name, queued_at) = (entry.0.clone(), entry.1);
        drop(entry);
        self.queued_jobs.remove(&job_id);

        let status = graph.status().clone();
        self.running_jobs.insert(
            job_id.clone(),
            ExtendedJobStatus {
                status: status.clone(),
                subscriber,
            },
        );
        upsert_job(
            &self.pool,
            &self.scheduler,
            job_id.as_ref(),
            &job_name,
            queued_at,
            status_kind(status.status.as_ref().unwrap_or(&Status::Queued(QueuedJob { queued_at }))),
            &status,
            false,
        )
        .await?;

        self.job_event_sender.send(&JobStateEvent::JobAcquired {
            job_id,
            owner: self.scheduler.clone(),
        });
        Ok(())
    }

    async fn get_job_status(&self, job_id: &JobId) -> BallistaResult<Option<JobStatus>> {
        if let Some(entry) = self.queued_jobs.get(job_id) {
            let (job_name, queued_at) = (entry.0.clone(), entry.1);
            return Ok(Some(JobStatus {
                job_id: job_id.to_owned().into(),
                job_name,
                status: Some(Status::Queued(QueuedJob { queued_at })),
            }));
        }
        if let Some(status) = self.running_jobs.get(job_id).as_deref().cloned() {
            return Ok(Some(status.status));
        }
        if let Some((status, _)) = self.completed_jobs.get(job_id).as_deref() {
            return Ok(Some(status.clone()));
        }
        self.job_status_from_db(job_id).await
    }

    async fn get_execution_graph(
        &self,
        job_id: &JobId,
    ) -> BallistaResult<Option<ExecutionGraphBox>> {
        // v1 limitation: graphs are cached locally only; a job completed on
        // another replica has no graph here (and a dead replica's graphs are
        // gone). See module docs.
        Ok(self
            .completed_jobs
            .get(job_id)
            .as_deref()
            .and_then(|(_, graph)| graph.as_ref().map(|e| e.cloned())))
    }

    async fn try_acquire_job(&self, _job_id: &JobId) -> BallistaResult<Option<ExecutionGraphBox>> {
        // Always return None. No job handover in v1: the only cross-replica
        // state is status metadata, which cannot be acquired.
        Ok(None)
    }

    async fn save_job(&self, job_id: &JobId, graph: &ExecutionGraphBox) -> BallistaResult<()> {
        let status = graph.status().clone();
        let status_inner = status.status.clone();
        let queued_at = status_queued_at(&status);
        if matches!(
            status_inner,
            Some(Status::Successful(_)) | Some(Status::Failed(_))
        ) {
            if let Some((_, job_info)) = self.running_jobs.remove(job_id) {
                job_info.update_subscribers(status.clone());
            }
            self.completed_jobs
                .insert(job_id.to_owned(), (status.clone(), Some(graph.cloned())));
            upsert_job(
                &self.pool,
                &self.scheduler,
                job_id.as_ref(),
                &status.job_name,
                queued_at,
                "completed",
                &status,
                true,
            )
            .await?;
        } else {
            let Some(mut job_info) = self.running_jobs.get_mut(job_id) else {
                return Err(BallistaError::Internal(format!(
                    "scheduler state can't find job: {job_id}"
                )));
            };
            job_info.status = status.clone();
            job_info.update_subscribers(status.clone());
            drop(job_info);
            upsert_job(
                &self.pool,
                &self.scheduler,
                job_id.as_ref(),
                &status.job_name,
                queued_at,
                "running",
                &status,
                false,
            )
            .await?;
        }

        self.job_event_sender.send(&JobStateEvent::JobUpdated {
            job_id: job_id.to_owned(),
            status,
        });
        Ok(())
    }

    async fn fail_unscheduled_job(&self, job_id: &JobId, reason: String) -> BallistaResult<()> {
        let Some((job_id, (job_name, queued_at))) = self.queued_jobs.remove(job_id) else {
            return Err(BallistaError::Internal(format!(
                "Could not fail unscheduler job {job_id}, job not found in queued jobs"
            )));
        };
        let status = JobStatus {
            job_id: job_id.clone().into(),
            job_name: job_name.clone(),
            status: Some(Status::Failed(FailedJob {
                error: reason,
                queued_at,
                started_at: 0,
                ended_at: timestamp_millis(),
            })),
        };
        self.completed_jobs
            .insert(job_id.clone(), (status.clone(), None));
        upsert_job(
            &self.pool,
            &self.scheduler,
            job_id.as_ref(),
            &job_name,
            queued_at,
            "completed",
            &status,
            true,
        )
        .await?;
        self.job_event_sender
            .send(&JobStateEvent::JobUpdated { job_id, status });
        Ok(())
    }

    async fn remove_job(&self, job_id: &JobId) -> BallistaResult<()> {
        if self.completed_jobs.remove(job_id).is_none() {
            tracing::warn!("Tried to delete non-existent job {job_id} from state");
        }
        let conn = self.conn().await?;
        conn.execute(
            "DELETE FROM ballista_jobs WHERE job_id = $1",
            &[&job_id.as_str()],
        )
        .await
        .map_err(pg_err("delete job"))?;
        Ok(())
    }

    async fn get_jobs(&self) -> BallistaResult<HashSet<JobId>> {
        let mut jobs = self.db_job_ids(Some("completed")).await?;
        jobs.extend(self.completed_jobs.iter().map(|e| e.key().clone()));
        Ok(jobs)
    }

    async fn get_all_jobs(&self) -> BallistaResult<HashSet<JobId>> {
        let mut jobs = self.db_job_ids(None).await?;
        jobs.extend(self.queued_jobs.iter().map(|e| e.key().clone()));
        jobs.extend(self.running_jobs.iter().map(|e| e.key().clone()));
        jobs.extend(self.completed_jobs.iter().map(|e| e.key().clone()));
        Ok(jobs)
    }

    async fn job_state_events(&self) -> BallistaResult<JobStateEventStream> {
        Ok(Box::pin(self.job_event_sender.subscribe()))
    }

    async fn create_or_update_session(
        &self,
        session_id: &str,
        config: &SessionConfig,
    ) -> BallistaResult<Arc<SessionContext>> {
        self.job_event_sender.send(&JobStateEvent::SessionAccessed {
            session_id: session_id.to_string(),
        });
        Ok(create_datafusion_context(
            config,
            self.session_builder.clone(),
        )?)
    }

    async fn remove_session(&self, session_id: &str) -> BallistaResult<()> {
        self.job_event_sender.send(&JobStateEvent::SessionRemoved {
            session_id: session_id.to_string(),
        });
        Ok(())
    }

    fn produce_config(&self) -> SessionConfig {
        (self.config_producer)()
    }
}