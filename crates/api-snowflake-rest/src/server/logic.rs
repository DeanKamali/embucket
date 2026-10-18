use super::state::AppState;
use crate::models::{
    JsonResponse, LoginRequestData, LoginRequestQueryParams, LoginResponse, LoginResponseData,
    QueryRequest, QueryRequestBody,
};
use crate::server::error::{
    self as api_snowflake_rest_error, CreateJwtSnafu, NoJwtSecretSnafu, Result,
};
use crate::server::helpers::handle_query_ok_result;
use api_snowflake_rest_sessions::TokenizedSession;
use api_snowflake_rest_sessions::helpers::{create_jwt, ensure_jwt_secret_is_valid, jwt_claims};
use executor::RunningQueryId;
use executor::account_usage::current_epoch_ms;
use executor::models::{QueryContext, SessionMetadata, SessionMetadataAttr};
use avalanche_account::models::LoginEvent;
use avalanche_account::store::AccountStore;
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;
use time::Duration;

pub const JWT_TOKEN_EXPIRATION_SECONDS: u32 = 3 * 24 * 60 * 60;

/// LOGIN_HISTORY (M17.1): best-effort login recording — never blocks or
/// fails authentication.
async fn record_login(
    store: &Arc<dyn AccountStore>,
    login_name: &str,
    client_app_id: Option<String>,
    client_ip: Option<String>,
    success: bool,
) {
    let event = LoginEvent {
        timestamp_ms: current_epoch_ms(),
        sequence: 0,
        // The store stamps the session's account (M15.4) on record.
        account: String::new(),
        user_name: login_name.to_string(),
        client_app_id,
        client_ip,
        success,
    };
    if let Err(e) = store.record_login(event).await {
        tracing::warn!(error = %e, "failed to record login event");
    }
}

/// Master token lifetime (Snowflake default is 4h session / 24h master).
pub const MASTER_TOKEN_EXPIRATION_SECONDS: u32 = 24 * 60 * 60;

#[tracing::instrument(
    name = "api_snowflake_rest::handle_login_request",
    level = "debug",
    skip(state, credentials),
    fields(session_metadata),
    err,
    ret(level = tracing::Level::TRACE)
)]
pub async fn handle_login_request(
    state: &AppState,
    host: String,
    credentials: LoginRequestData,
    params: LoginRequestQueryParams,
    client_ip: Option<String>,
) -> Result<LoginResponse> {
    let LoginRequestData {
        login_name,
        password,
        account_name,
        client_app_id,
        client_app_version,
        ..
    } = credentials;

    // Account-plane login (design §17, M10): the account store is the primary
    // credential source when configured; the static demo credentials remain
    // the fallback for deployments without an account store.
    // M15.4 (design §21.4): `ACCOUNT_NAME` routes the login to that
    // account's scoped store; an unknown account fails like bad
    // credentials. Empty `ACCOUNT_NAME` = the built-in default account.
    let session_store: Option<Arc<dyn avalanche_account::store::AccountStore>> =
        match &state.config.account {
            Some(store) => {
                let scoped = if account_name.is_empty() {
                    Some(Arc::clone(store))
                } else {
                    match Arc::clone(store).account_scope(&account_name).await {
                        Ok(Some(s)) => Some(s),
                        Ok(None) => None,
                        Err(e) => {
                            tracing::error!(error = %e, "account scope resolution failed");
                            None
                        }
                    }
                };
                let Some(scoped) = scoped else {
                    record_login(
                        store,
                        &login_name,
                        Some(client_app_id.clone()),
                        client_ip.clone(),
                        false,
                    )
                    .await;
                    return api_snowflake_rest_error::InvalidAuthDataSnafu.fail();
                };
                match scoped.authenticate(&login_name, &password).await {
                    Ok(Some(_)) => Some(scoped),
                    Ok(None) => {
                        // LOGIN_HISTORY (M17.1): failed attempts are recorded too.
                        record_login(
                            &scoped,
                            &login_name,
                            Some(client_app_id.clone()),
                            client_ip.clone(),
                            false,
                        )
                        .await;
                        return api_snowflake_rest_error::InvalidAuthDataSnafu.fail();
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "account store authentication failed");
                        return api_snowflake_rest_error::InvalidAuthDataSnafu.fail();
                    }
                }
            }
            None => {
                if login_name != *state.config.auth.demo_user
                    || password != *state.config.auth.demo_password
                {
                    return api_snowflake_rest_error::InvalidAuthDataSnafu.fail();
                }
                None
            }
        };
    if let Some(store) = &session_store {
        record_login(store, &login_name, Some(client_app_id.clone()), client_ip.clone(), true).await;
    }

    // host is required to check token audience claim
    let jwt_secret = &*state.config.auth.jwt_secret;
    let _ = ensure_jwt_secret_is_valid(jwt_secret).context(NoJwtSecretSnafu)?;

    let mut session_metadata = SessionMetadata::default();
    session_metadata.set_attr(SessionMetadataAttr::UserName, login_name.clone());
    session_metadata.set_attr(SessionMetadataAttr::AccountName, account_name);
    session_metadata.set_attr(SessionMetadataAttr::ClientAppId, client_app_id);
    session_metadata.set_attr(SessionMetadataAttr::ClientAppVersion, client_app_version);
    // set database, schema when provided
    if let Some(db) = params.database_name {
        session_metadata.set_attr(SessionMetadataAttr::Database, db);
    }
    if let Some(schema) = params.schema_name {
        session_metadata.set_attr(SessionMetadataAttr::Schema, schema);
    }
    if let Some(warehouse) = params.warehouse {
        session_metadata.set_attr(SessionMetadataAttr::Warehouse, warehouse);
    }

    tracing::Span::current().record("session_metadata", format!("{session_metadata:?}"));

    let tokenized_session = TokenizedSession::default().with_metadata(session_metadata.clone());

    let jwt_claims = jwt_claims(
        &login_name,
        &host,
        Duration::seconds(JWT_TOKEN_EXPIRATION_SECONDS.into()),
        tokenized_session.clone(),
    );

    tracing::info!("Host '{host}' for token creation");

    let session_id = jwt_claims.session.session_id();
    // The driver core expects a numeric `sessionId`; our session ids are
    // strings (uuid-like), so v1 emits a best-effort numeric id and keeps
    // the authoritative id in the JWT claims.
    let numeric_session_id: i64 = session_id.parse().unwrap_or(0);
    let _session = state
        .execution_svc
        .create_session(session_id, Some(jwt_claims.session.metadata().clone()))
        .await?;

    let jwt_token = create_jwt(&jwt_claims, jwt_secret).context(CreateJwtSnafu)?;

    // Master token (M13): the driver core requires `masterToken` in the login
    // response for session renewal. v1 issues a second JWT over the *same*
    // session with a longer lifetime; refresh/renewal endpoints are a
    // follow-up (§16 record).
    let master_claims = api_snowflake_rest_sessions::helpers::jwt_claims(
        &login_name,
        &host,
        Duration::seconds(MASTER_TOKEN_EXPIRATION_SECONDS.into()),
        tokenized_session.clone(),
    );
    let master_token = create_jwt(&master_claims, jwt_secret).context(CreateJwtSnafu)?;

    Ok(LoginResponse {
        data: Option::from(LoginResponseData {
            token: jwt_token,
            master_token,
            validity_in_seconds: Some(JWT_TOKEN_EXPIRATION_SECONDS.into()),
            master_validity_in_seconds: Some(MASTER_TOKEN_EXPIRATION_SECONDS.into()),
            display_user_name: Some(login_name.clone()),
            server_version: None,
            session_id: numeric_session_id,
        }),
        success: true,
        message: Option::from("successfully executed".to_string()),
    })
}

#[tracing::instrument(
    name = "api_snowflake_rest::handle_query_request",
    level = "debug",
    skip(state, query_body, client_ip),
    fields(request_id = %query.request_id),
    err,
    ret(level = tracing::Level::TRACE)
)]
pub async fn handle_query_request(
    state: &AppState,
    TokenizedSession(session_id, session_metadata): TokenizedSession,
    query: QueryRequest,
    query_body: QueryRequestBody,
    client_ip: Option<String>,
) -> Result<JsonResponse> {
    let QueryRequestBody {
        sql_text,
        async_exec,
        query_submission_time,
    } = query_body;
    let async_exec = async_exec.unwrap_or(false);
    if async_exec {
        return api_snowflake_rest_error::NotImplementedSnafu.fail();
    }

    let serialization_format = state.config.dbt_serialization_format;
    let mut query_context = QueryContext::new(
        session_metadata.attr(SessionMetadataAttr::Database),
        session_metadata.attr(SessionMetadataAttr::Schema),
        None,
    )
    .with_request_id(query.request_id)
    .with_query_submission_time(query_submission_time)
    .with_session_metadata(Some(session_metadata));

    if let Some(ip) = client_ip {
        query_context = query_context.with_ip_address(ip);
    }

    // find running query by request_id
    let query_id_res = state
        .execution_svc
        .locate_query_id(RunningQueryId::ByRequestId(
            query.request_id,
            sql_text.clone(),
        ));

    // if retry-disable feature is enabled we ignore retries regardless of query_id is located or not
    #[cfg(feature = "retry-disable")]
    if query.retry_count.unwrap_or_default() > 0 {
        return api_snowflake_rest_error::RetryDisabledSnafu.fail();
    }

    let (result, query_id) = if query.retry_count.unwrap_or_default() > 0
        && let Ok(query_id) = query_id_res
    {
        let result = state.execution_svc.wait(query_id).await?;
        (result, query_id)
    } else {
        let query_id = query_context.query_id;
        let result = state
            .execution_svc
            .query(&session_id, &sql_text, query_context)
            .await?;
        (result, query_id)
    };

    handle_query_ok_result(&sql_text, query_id, result, serialization_format)
}
