use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Entities {
    Session,
    View,
    Query,
}

impl Display for Entities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Session => write!(f, "session"),
            Self::View => write!(f, "view"),
            Self::Query => write!(f, "query"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStatus {
    Running,
    Success,
    Fail,
    Incident,
}

impl Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Running => "running",
            Self::Success => "success",
            Self::Fail => "fail",
            Self::Incident => "incident",
        };
        write!(f, "{value}")
    }
}

/// Warehouse entity persisted in the durable state store (Postgres).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarehouseRecord {
    pub name: String,
    /// Snowflake-style size, e.g. "XSMALL", "LARGE" (size → executor count is
    /// an `avalanche-warehouse` concern).
    pub size: String,
    /// "RUNNING" or "SUSPENDED".
    pub state: String,
    /// Idle seconds before auto-suspend; 0 = never suspend.
    pub auto_suspend_secs: u64,
    pub auto_resume: bool,
    pub created_at: u64,
    /// Host port on the docker host mapped to the scheduler's gRPC port, so
    /// host-side clients (embucketd) can dial the scheduler. `None` for
    /// records persisted before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_host_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
    /// Per-warehouse statement timeout override in seconds (M6). `None` =
    /// use the router global; 0 = no timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_timeout_secs: Option<u64>,
    /// Per-warehouse storage credentials (M7.1). `None` = warehouse has no
    /// scoped storage access (v1 `file://` behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<StorageCredentials>,
    /// Mutual TLS (M7.2, design §13.2). When true, the warehouse compute is
    /// started with `AVALANCHE_TLS_DIR` and the scheduler is advertised over
    /// `https://`; without it everything stays plaintext.
    #[serde(default)]
    pub tls: bool,
}

impl WarehouseRecord {
    #[must_use]
    pub fn new(name: &str, size: &str, auto_suspend_secs: u64, auto_resume: bool) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Self {
            name: name.to_string(),
            size: size.to_string(),
            state: "SUSPENDED".to_string(),
            auto_suspend_secs,
            auto_resume,
            created_at,
            scheduler_host_port: None,
            updated_at: None,
            statement_timeout_secs: None,
            storage_credentials: None,
            tls: false,
        }
    }
}

/// Per-warehouse storage credentials (M7.1, design §13.1).
///
/// Injected into warehouse executor containers as environment variables at
/// container start — never inside serialized query plans. S3-compatible
/// (includes MinIO); `InstanceProfile` (no static keys) is a future shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCredentials {
    /// Credential kind; v1 is always `"s3"`.
    pub kind: String,
    /// Object-store endpoint URL, e.g. `http://minio:9000` or
    /// `https://s3.us-east-1.amazonaws.com`.
    pub endpoint: String,
    /// Bucket the warehouse is scoped to.
    pub bucket: String,
    pub access_key_id: String,
    pub secret_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    /// Region hint used for request signing (e.g. `us-east-1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl StorageCredentials {
    /// The kind string accepted by DDL (`STORAGE_CREDENTIALS = (TYPE = S3 ...)`).
    pub const KIND_S3: &'static str = "s3";

    /// Environment variables injected into warehouse containers. Keys use
    /// the standard AWS names so `object_store`'s `AmazonS3Builder::from_env`
    /// picks them up; `AVALANCHE_S3_*` names the Avalanche-specific knobs.
    /// Returns in a stable order suitable for tests.
    #[must_use]
    pub fn env_vars(&self) -> Vec<(String, String)> {
        // HTTP endpoints (local MinIO) need the allow-http escape hatch.
        let allow_http = self.endpoint.starts_with("http://");
        let mut env = vec![
            (
                "AVALANCHE_S3_ENDPOINT".to_string(),
                self.endpoint.clone(),
            ),
            ("AVALANCHE_S3_BUCKET".to_string(), self.bucket.clone()),
            ("AWS_ENDPOINT".to_string(), self.endpoint.clone()),
            (
                "AWS_ACCESS_KEY_ID".to_string(),
                self.access_key_id.clone(),
            ),
            (
                "AWS_SECRET_ACCESS_KEY".to_string(),
                self.secret_key.clone(),
            ),
        ];
        if allow_http {
            env.push(("AWS_ALLOW_HTTP".to_string(), "true".to_string()));
        }
        if let Some(token) = &self.session_token {
            env.push(("AWS_SESSION_TOKEN".to_string(), token.clone()));
        }
        // S3 request signing needs a region; MinIO/default endpoints use
        // us-east-1. Emitted unconditionally so executors never sign without
        // one.
        let region = self
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string());
        env.push(("AWS_REGION".to_string(), region));
        env
    }

    /// Display-safe summary for `SHOW/DESCRIBE WAREHOUSE` — no secret values.
    #[must_use]
    pub fn summary(&self) -> String {
        format!("S3 endpoint={} bucket={}", self.endpoint, self.bucket)
    }
}

/// Per-hour credit usage for one warehouse (Avalanche design §5 metering).
///
/// Mirrors one row of Snowflake's `WAREHOUSE_METERING_HISTORY`: usage is
/// aggregated into hourly buckets (`start_time`..`end_time`), with
/// `credits_used` = executor-seconds × executor-count / 3600. A warehouse
/// resize mid-hour produces a separate row per executor count.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarehouseMeteringRecord {
    pub warehouse: String,
    /// Hour-bucket start, seconds since UNIX epoch.
    pub start_time: u64,
    /// Hour-bucket end (`start_time + 3600`).
    pub end_time: u64,
    /// Executor count (size factor) during this bucket slice.
    pub executor_count: u32,
    /// Accrued credits in this bucket slice.
    pub credits_used: f64,
}

impl WarehouseMeteringRecord {
    /// Bucket key used as the durable store's primary key: one row per
    /// (warehouse, hour, executor count).
    #[must_use]
    pub fn bucket_key(&self) -> String {
        format!("{}:{}:{}", self.warehouse, self.start_time, self.executor_count)
    }
}

/// Session entity persisted in the durable state store (Postgres).
///
/// This record is the durable representation of a session: it survives process
/// restarts so that a reconnecting client can be rehydrated on any
/// `embucketd`/router instance (Avalanche workstream A).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    /// Login-time context (account, client app, initial database/schema/warehouse).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    /// Ad-hoc session attributes (`UserSession::attrs`), write-through
    /// persisted so they survive rehydration on another process.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attrs: HashMap<String, String>,
    /// Unix-seconds inactivity expiry; sessions past this point are dead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<u64>,
    #[serde(default)]
    pub variables: HashMap<String, Variable>,
    #[serde(default)]
    pub views: HashMap<String, ViewRecord>,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
}

impl SessionRecord {
    /// Create a new session record with default values and a current timestamp.
    #[must_use]
    pub fn new(session_id: &str) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());

        Self {
            session_id: session_id.to_string(),
            ttl_seconds: None,
            user_name: None,
            metadata: HashMap::new(),
            attrs: HashMap::new(),
            expiry: None,
            variables: HashMap::new(),
            views: HashMap::new(),
            created_at,
            updated_at: None,
        }
    }

    #[must_use]
    pub fn entity(&self) -> String {
        Entities::Session.to_string()
    }
}

/// Logical view entity describing embucket-managed views.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewRecord {
    pub view_id: String,
    pub database: String,
    pub schema: String,
    pub name: String,
    pub sql_definition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Variable {
    /// full name of the variable with the name space
    pub name: String,
    pub value: String,
    pub value_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    pub created_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Query {
    pub query_id: Uuid,
    pub request_id: Option<Uuid>,
    pub query_text: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authn_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_number: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_status: Option<ExecutionStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    pub start_time: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_elapsed_time: Option<u64>, // in ms
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentage_scanned_from_cache: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_written_to_result: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_read_from_result: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_produced: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_inserted: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_updated: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_deleted: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_unloaded: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_deleted: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions_total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_spilled_to_local_storage: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_spilled_to_remote_storage: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_sent_over_the_network: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compilation_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_time: Option<u64>, // in ms
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_provisioning_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_repair_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_overload_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_blocked_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_data_transfer_cloud: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_data_transfer_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_data_transfer_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_data_transfer_cloud: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_data_transfer_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_data_transfer_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_external_files_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits_used_cloud_services: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_function_total_invocations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_function_total_sent_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_function_total_received_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_function_total_sent_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_function_total_received_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_load_percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_client_generated_statement: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_acceleration_bytes_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_acceleration_partitions_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_acceleration_upper_limit_scale_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_queries_wait_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_hash_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_parameterized_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_parameterized_hash_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_role_stats: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_written_to_result: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_retry_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_retry_cause: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fault_handling_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_database_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_database_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_schema_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_schema_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_values: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_metrics: Option<Vec<QueryMetric>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_history_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_result_time: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_app_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_app_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_submission_time: Option<DateTime<Utc>>,
}

impl Query {
    #[must_use]
    pub fn new(
        query_str: &str,
        query_id: Uuid,
        session_id: &str,
        request_id: Option<Uuid>,
    ) -> Self {
        Self {
            query_id,
            query_text: query_str.chars().take(100_000).collect(),
            session_id: session_id.to_string(),
            request_id,
            start_time: Utc::now(),
            query_hash: Some(xxh3_64(query_str.as_bytes()).to_string()),
            query_hash_version: Some(1),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn entity(&self) -> String {
        Entities::Query.to_string()
    }

    pub fn set_database_name(&mut self, database: String) {
        self.database_name = Some(database);
    }

    pub fn set_schema_name(&mut self, schema: String) {
        self.schema_name = Some(schema);
    }

    pub fn set_user_name(&mut self, user: String) {
        self.user_name = Some(user);
    }

    pub fn set_user_database(&mut self, database: String) {
        self.user_database_name = Some(database);
    }

    pub fn set_user_schema(&mut self, schema: String) {
        self.user_schema_name = Some(schema);
    }

    pub const fn set_execution_status(&mut self, status: ExecutionStatus) {
        self.execution_status = Some(status);
    }

    pub fn set_error_code(&mut self, error_code: String) {
        self.error_code = Some(error_code);
    }

    pub fn set_error_message(&mut self, error_message: String) {
        self.error_message = Some(error_message);
    }

    pub fn set_query_metrics(&mut self, metrics: Vec<QueryMetric>) {
        self.query_metrics = Some(metrics);
    }

    pub fn set_warehouse_type(&mut self, warehouse_type: String) {
        self.warehouse_type = Some(warehouse_type);
    }

    pub fn set_warehouse_name(&mut self, warehouse_name: String) {
        self.warehouse_name = Some(warehouse_name);
    }

    pub fn set_warehouse_size(&mut self, warehouse_size: String) {
        self.warehouse_size = Some(warehouse_size);
    }

    pub fn set_release_version(&mut self, release_version: String) {
        self.release_version = Some(release_version);
    }

    pub const fn set_rows_produced(&mut self, rows_count: u64) {
        self.rows_produced = Some(rows_count);
    }

    pub const fn set_rows_inserted(&mut self, rows_count: u64) {
        self.rows_inserted = Some(rows_count);
    }

    pub const fn set_rows_deleted(&mut self, rows_count: u64) {
        self.rows_deleted = Some(rows_count);
    }

    pub const fn set_rows_updated(&mut self, rows_count: u64) {
        self.rows_updated = Some(rows_count);
    }

    pub fn set_query_type(&mut self, query_type: String) {
        self.query_type = Some(query_type);
    }

    pub fn set_client_app_id(&mut self, client_app_id: String) {
        self.client_app_id = Some(client_app_id);
    }

    pub fn set_client_app_version(&mut self, client_app_version: String) {
        self.client_app_version = Some(client_app_version);
    }

    #[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
    pub const fn set_query_submission_time(&mut self, query_submission_time: u64) {
        // Convert u64 timestamp to DateTime<Utc>
        let dt = DateTime::<Utc>::from_timestamp_millis(query_submission_time as i64);
        self.query_submission_time = dt;
    }

    #[allow(clippy::as_conversions, clippy::cast_sign_loss)]
    pub fn set_end_time(&mut self) {
        let end_time = Utc::now();
        self.end_time = Some(end_time);
        self.execution_time = Some((end_time - self.start_time).num_milliseconds() as u64);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryMetric {
    pub node_id: usize,
    pub parent_node_id: Option<usize>,
    pub operator: String,
    pub metrics: Value, // serialized metrics as JSON object
}
