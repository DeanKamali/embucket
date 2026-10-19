//! Snowpipe Streaming compatible ingest API (named channels).
//!
//! Implements the published REST contract shape:
//! - `GET  /v2/streaming/hostname`
//! - `POST /oauth/token` (JWT-bearer grant, RFC 7523 style)
//! - `PUT  /v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}`
//! - `POST .../channels/{channel}/rows?continuationToken=...`
//! - `POST .../pipes/{pipe}:bulk-channel-status`
//! - `DELETE .../channels/{channel}`
//!
//! Deliberate deviations from Snowflake (documented for the M18 spike):
//! - There is no `CREATE PIPE` yet: the pipe segment names an implicit pipe
//!   whose target table is `{db}.{schema}.{pipe}` (auto-created on first
//!   append if missing, like the file-upload path).
//! - Commits are synchronous: rows are inserted and the offset token is
//!   recorded before the 200 response, so a batch is all-or-nothing
//!   (stronger than Snowflake's async commit).
//! - Offset tokens are client-owned strings stored server-side at commit;
//!   continuation tokens are server-owned and opaque (`{sequencer}-{n}`).
//! - Authorization is the existing JWT bearer contract (`require_auth`
//!   middleware); requests execute under a shared ingest session, so RBAC
//!   is bypassed for channel appends.

use super::state::AppState;
use axum::body::Bytes;
use axum::extract::{Form, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use catalog_metastore::TableIdent;
use datafusion::arrow::json;
use executor::service::{
    StreamingChannelRecord, STREAMING_INGEST_SESSION_ID, streaming_channel_key,
};
use serde_json::json;
use snafu::prelude::*;
use std::collections::HashMap;
use std::future::Future;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// 4 MB maximum decompressed row payload (Snowpipe Streaming contract).
const MAX_ROWS_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StreamingError {
    #[snafu(display("Channel {channel} does not exist"))]
    ChannelDoesNotExist { channel: String },

    #[snafu(display("Stale continuation token sequencer: reopen the channel"))]
    StaleContinuationTokenSequencer,

    #[snafu(display("Invalid continuation token"))]
    InvalidContinuationToken,

    #[snafu(display("Invalid request: {message}"))]
    InvalidRequest { message: String },

    #[snafu(display("Invalid grant: {message}"))]
    InvalidGrant { message: String },

    #[snafu(display("Rows append failed: {message}"))]
    AppendFailed { message: String },

    #[snafu(display("Executor error: {source}"))]
    Executor { source: executor::error::Error },
}

impl From<executor::error::Error> for StreamingError {
    fn from(source: executor::error::Error) -> Self {
        Self::Executor { source }
    }
}

impl IntoResponse for StreamingError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::ChannelDoesNotExist { .. } => (StatusCode::NOT_FOUND, "CHANNEL_DOES_NOT_EXIST"),
            Self::StaleContinuationTokenSequencer => {
                (StatusCode::CONFLICT, "STALE_CONTINUATION_TOKEN_SEQUENCER")
            }
            Self::InvalidContinuationToken => {
                (StatusCode::BAD_REQUEST, "INVALID_CONTINUATION_TOKEN")
            }
            Self::InvalidRequest { .. } | Self::AppendFailed { .. } => {
                (StatusCode::BAD_REQUEST, "INVALID_REQUEST")
            }
            Self::InvalidGrant { .. } => (StatusCode::UNAUTHORIZED, "INVALID_GRANT"),
            Self::Executor { .. } => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR"),
        };
        let body = json!({
            "code": code,
            "message": self.to_string(),
        });
        (status, Json(body)).into_response()
    }
}

type StreamingResult<T> = std::result::Result<T, StreamingError>;

/// No-auth-mode note: when demo credentials are configured empty the
/// `require_auth` middleware is a no-op; streaming routes behave the same
/// way (JWT secret empty disables assertion validation on `/oauth/token`
/// and passes the assertion through as the access token).
pub fn create_streaming_router() -> Router<AppState> {
    Router::new()
        .route(
            "/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}",
            put(open_channel).delete(drop_channel),
        )
        .route(
            "/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows",
            post(append_rows),
        )
        .route(
            "/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe_op}",
            post(pipe_op),
        )
        .layer(axum::extract::DefaultBodyLimit::max(MAX_ROWS_PAYLOAD_BYTES))
}

/// No-auth streaming routes: `/oauth/token` carries its own assertion (the
/// JWT itself is the credential) and `/v2/streaming/hostname` is a
/// discovery endpoint.
pub fn create_streaming_auth_router() -> Router<AppState> {
    Router::new()
        .route("/v2/streaming/hostname", get(hostname))
        .route("/oauth/token", post(oauth_token))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_ROWS_PAYLOAD_BYTES))
}

fn ensure_ingest_session(state: &AppState) -> impl Future<Output = StreamingResult<()>> + Send {
    let svc = state.execution_svc.clone();
    async move {
        svc.create_session(STREAMING_INGEST_SESSION_ID, None).await?;
        Ok(())
    }
}

/// Ingest host: return the host the request was addressed to (clients use
/// it to point appends at the service).
async fn hostname(headers: axum::http::HeaderMap) -> String {
    headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// RFC 7523 JWT-bearer grant: the assertion is a JWT minted with the same
/// JWT secret the regular session tokens use (the `/session/v1/login-request`
/// response token, or a client-side mint). The access token is a freshly
/// signed copy of the assertion's claims.
async fn oauth_token(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> StreamingResult<Json<serde_json::Value>> {
    if form.get("grant_type").map(String::as_str)
        != Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
    {
        return InvalidGrantSnafu {
            message: "grant_type must be urn:ietf:params:oauth:grant-type:jwt-bearer",
        }
        .fail();
    }
    let assertion = form.get("assertion").ok_or_else(|| StreamingError::InvalidGrant {
        message: "missing assertion".to_string(),
    })?;

    let jwt_secret = &state.config.auth.jwt_secret;
    if jwt_secret.is_empty() {
        // No-auth mode: nothing to validate against; pass the assertion
        // through as the access token.
        return Ok(Json(json!({ "token": assertion })));
    }

    let claims: api_snowflake_rest_sessions::helpers::Claims =
        api_snowflake_rest_sessions::helpers::get_claims_validate_jwt_token(
            assertion,
            // Audience is the host the request was addressed to, matching
            // how the login flow mints session tokens.
            headers
                .get(axum::http::header::HOST)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
            jwt_secret,
        )
        .map_err(|err| StreamingError::InvalidGrant {
            message: format!("invalid assertion: {err}"),
        })?;

    let token = api_snowflake_rest_sessions::helpers::create_jwt(&claims, jwt_secret)
        .map_err(|err| StreamingError::InvalidGrant {
            message: format!("mint access token: {err}"),
        })?;

    Ok(Json(json!({ "token": token })))
}

#[derive(Debug, serde::Deserialize)]
pub struct OpenChannelBody {
    #[serde(default, rename = "offset_token")]
    offset_token: Option<String>,
    #[serde(default)]
    fail_on_uncommitted_rows: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
pub struct OpenChannelQuery {
    #[serde(default, rename = "offSetToken", alias = "offsetToken")]
    offset_token: Option<String>,
    #[serde(default, rename = "failOnUncommittedRows")]
    fail_on_uncommitted_rows: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
pub struct AppendRowsQuery {
    #[serde(default, rename = "continuationToken")]
    continuation_token: Option<String>,
    #[serde(default, rename = "startOffsetToken")]
    start_offset_token: Option<String>,
    #[serde(default, rename = "endOffsetToken")]
    end_offset_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct BulkChannelStatusBody {
    #[serde(default, rename = "channel_names")]
    channel_names: Option<Vec<String>>,
}

fn channel_status(record: &StreamingChannelRecord) -> serde_json::Value {
    json!({
        "last_committed_offset_token": record.last_committed_offset_token,
        "rows_inserted": record.rows_inserted,
        "rows_parsed": record.rows_parsed,
        "rows_error_count": record.rows_error_count,
        "last_error_message": record.last_error_message,
        "snowflake_avg_processing_latency_ms": 0,
        "created_on_ms": record.created_on_ms,
        "sequencer": record.sequencer,
        "next_continuation": record.next_continuation,
    })
}

fn continuation_token(record: &StreamingChannelRecord) -> String {
    format!("{}-{}", record.sequencer, record.next_continuation)
}

async fn find_channel(
    state: &AppState,
    key: &str,
) -> StreamingResult<Option<StreamingChannelRecord>> {
    let channels = state.execution_svc.list_streaming_channels().await?;
    Ok(channels.into_iter().find(|record| record.ident_key() == key))
}

/// PUT: open (or reopen) a named channel. Reopen bumps the sequencer so
/// continuation tokens issued before the reopen go stale.
async fn open_channel(
    State(state): State<AppState>,
    Path((db, schema, pipe, channel)): Path<(String, String, String, String)>,
    Query(query): Query<OpenChannelQuery>,
    body: Option<Json<OpenChannelBody>>,
) -> StreamingResult<Json<serde_json::Value>> {
    ensure_ingest_session(&state).await?;

    let key = streaming_channel_key(&db, &schema, &pipe, &channel);
    let offset_token = body
        .as_ref()
        .and_then(|Json(body)| body.offset_token.clone())
        .or(query.offset_token);

    let mut record = match find_channel(&state, &key).await? {
        Some(mut record) => {
            // Reopen: stale old continuation tokens.
            record.sequencer += 1;
            record.next_continuation += 1;
            record
        }
        None => StreamingChannelRecord {
            database: db.to_uppercase(),
            schema: schema.to_uppercase(),
            pipe: pipe.to_uppercase(),
            channel: channel.to_uppercase(),
            sequencer: 1,
            next_continuation: 1,
            last_committed_offset_token: None,
            rows_inserted: 0,
            rows_parsed: 0,
            rows_error_count: 0,
            last_error_message: None,
            created_on_ms: now_millis(),
        },
    };
    if offset_token.is_some() {
        record.last_committed_offset_token = offset_token;
    }
    // fail_on_uncommitted_rows is accepted for contract parity; with
    // synchronous commits there is never uncommitted data, so it is a no-op.
    let _ = body
        .as_ref()
        .and_then(|Json(body)| body.fail_on_uncommitted_rows)
        .or(query.fail_on_uncommitted_rows);

    state.execution_svc.put_streaming_channel(&record).await?;

    let token = continuation_token(&record);
    Ok(Json(json!({
        "next_continuation_token": token,
        "channel_status": channel_status(&record),
    })))
}

/// POST rows: NDJSON batch append against the channel's implicit pipe table.
async fn append_rows(
    State(state): State<AppState>,
    Path((db, schema, pipe, channel)): Path<(String, String, String, String)>,
    Query(query): Query<AppendRowsQuery>,
    data: Bytes,
) -> StreamingResult<Json<serde_json::Value>> {
    let started = Instant::now();
    ensure_ingest_session(&state).await?;

    let provided_token = query.continuation_token.ok_or_else(|| {
        StreamingError::InvalidRequest {
            message: "missing continuationToken query parameter".to_string(),
        }
    })?;

    let key = streaming_channel_key(&db, &schema, &pipe, &channel);
    let mut record = find_channel(&state, &key)
        .await?
        .ok_or_else(|| StreamingError::ChannelDoesNotExist {
            channel: channel.clone(),
        })?;

    let expected = format!("{}-{}", record.sequencer, record.next_continuation);
    if provided_token != expected {
        // Distinguish a malformed token from a stale one: a token whose
        // sequencer generation matches but whose counter does not is just
        // invalid; anything else is stale.
        if provided_token.split_once('-').is_none_or(|(seq, _)| {
            seq.parse::<u64>() != Ok(record.sequencer)
        }) {
            return StaleContinuationTokenSequencerSnafu.fail();
        }
        return InvalidContinuationTokenSnafu.fail();
    }
    if data.is_empty() {
        return InvalidRequestSnafu {
            message: "empty rows payload",
        }
        .fail();
    }

    // Parse the NDJSON payload into typed record batches (arrow-json
    // infers the schema; field names must match the target table's columns
    // when it already exists, flat rows are the supported shape).
    let cursor = Cursor::new(data.clone());
    let json_schema = Arc::new(
        json::reader::infer_json_schema(cursor, None)
            .map_err(|err| StreamingError::AppendFailed {
                message: format!("infer schema: {err}"),
            })?
            .0
            .clone(),
    );
    let reader = json::ReaderBuilder::new(json_schema)
        .build(Cursor::new(data))
        .map_err(|err| StreamingError::AppendFailed {
            message: format!("parse rows: {err}"),
        })?;
    let batches: Vec<_> = reader
        .map(|batch| batch.map_err(|err| StreamingError::AppendFailed {
            message: format!("parse rows: {err}"),
        }))
        .collect::<Result<Vec<_>, _>>()?;

    // Target table = implicit pipe table `{db}.{schema}.{pipe}`.
    let table_ident = TableIdent {
        database: db.clone(),
        schema: schema.clone(),
        table: pipe.clone(),
    };
    let rows = state
        .execution_svc
        .upload_batches_to_table(STREAMING_INGEST_SESSION_ID, &table_ident, batches)
        .await
        .map_err(|err| StreamingError::AppendFailed {
            message: err.to_string(),
        })?;

    record.rows_inserted += rows as u64;
    record.rows_parsed += rows as u64;
    record.next_continuation += 1;
    record.last_committed_offset_token = query.end_offset_token.or(query.start_offset_token);
    record.last_error_message = None;

    state.execution_svc.put_streaming_channel(&record).await?;

    tracing::info!(
        channel = %key,
        rows,
        latency_ms = started.elapsed().as_millis() as u64,
        "snowpipe streaming append committed"
    );

    Ok(Json(json!({
        "next_continuation_token": continuation_token(&record),
    })))
}

/// POST `/pipes/{pipe}:bulk-channel-status` — matchit cannot express a
/// param-plus-suffix segment, so the pipe+op segment is parsed here.
async fn pipe_op(
    State(state): State<AppState>,
    Path((db, schema, pipe_op)): Path<(String, String, String)>,
    body: Option<Json<BulkChannelStatusBody>>,
) -> StreamingResult<Json<serde_json::Value>> {
    let (pipe, op) = pipe_op.rsplit_once(':').ok_or_else(|| {
        StreamingError::InvalidRequest {
            message: format!("expected {{pipe}}:bulk-channel-status, got `{pipe_op}`"),
        }
    })?;
    if op != "bulk-channel-status" {
        return InvalidRequestSnafu {
            message: format!("unknown pipe operation `{op}`"),
        }
        .fail();
    }

    let key_prefix = streaming_channel_key(&db, &schema, &pipe, "");
    let channels = state.execution_svc.list_streaming_channels().await?;
    let wanted = body.and_then(|Json(body)| body.channel_names);

    let mut statuses = serde_json::Map::new();
    for record in channels {
        if !record.ident_key().starts_with(&key_prefix) {
            continue;
        }
        if let Some(names) = &wanted {
            if !names
                .iter()
                .any(|name| name.to_lowercase() == record.channel.to_lowercase())
            {
                continue;
            }
        }
        statuses.insert(record.channel.clone(), channel_status(&record));
    }

    Ok(Json(json!({ "channels": statuses })))
}

/// DELETE: drop a channel (404 when it does not exist, like Snowflake).
async fn drop_channel(
    State(state): State<AppState>,
    Path((db, schema, pipe, channel)): Path<(String, String, String, String)>,
) -> StreamingResult<StatusCode> {
    let key = streaming_channel_key(&db, &schema, &pipe, &channel);
    find_channel(&state, &key)
        .await?
        .ok_or_else(|| StreamingError::ChannelDoesNotExist {
            channel: channel.clone(),
        })?;
    state.execution_svc.delete_streaming_channel(&key).await?;
    Ok(StatusCode::NO_CONTENT)
}