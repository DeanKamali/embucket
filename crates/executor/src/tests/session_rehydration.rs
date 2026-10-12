use crate::models::{QueryContext, SessionMetadata, SessionMetadataAttr};
use crate::service::{CoreExecutionService, ExecutionService};
use crate::utils::Config;
use catalog_metastore::InMemoryMetastore;
use state_store::InMemoryStateStore;
use state_store::StateStore;
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;

// Run unittests:
// cargo test --lib --features=state-store-query-test tests::session_rehydration

fn test_metadata() -> SessionMetadata {
    let mut metadata = SessionMetadata::default();
    metadata.set_attr(SessionMetadataAttr::UserName, "test_user".to_string());
    metadata.set_attr(SessionMetadataAttr::Warehouse, "test_wh".to_string());
    metadata
}

async fn make_service(state_store: Arc<InMemoryStateStore>) -> Arc<CoreExecutionService> {
    Arc::new(
        CoreExecutionService::new_test_executor(
            Arc::new(InMemoryMetastore::new()),
            state_store,
            Arc::new(Config::default()),
        )
        .await
        .expect("Failed to create execution service"),
    )
}

// A session created on one service instance must be rehydrated by another
// instance sharing the same state store (process restart / failover).
#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_rehydrate_session_from_state_store() {
    let state_store = Arc::new(InMemoryStateStore::new());
    let svc_a = make_service(state_store.clone()).await;

    svc_a
        .create_session("s1", Some(test_metadata()))
        .await
        .expect("Failed to create session");

    // Session-level setting (write-through to the state store).
    svc_a
        .query("s1", "SET myvar = 'hello'", QueryContext::default())
        .await
        .expect("Failed to set session variable");

    // Simulate a restart: a fresh service instance with an empty session map
    // but the same persistent state store.
    let svc_b = make_service(state_store.clone()).await;
    assert!(!svc_b.session_exists("s1").await);

    assert!(
        svc_b.update_session_expiry("s1").await.expect("update_session_expiry"),
        "session must be rehydrated from the state store"
    );
    assert!(svc_b.session_exists("s1").await);

    // Login context is restored on the rehydrated session.
    let session = svc_b.get_session("s1").await.expect("get_session");
    assert_eq!(
        session.metadata.attr(SessionMetadataAttr::UserName).as_deref(),
        Some("test_user")
    );
    assert_eq!(
        session.metadata.attr(SessionMetadataAttr::Warehouse).as_deref(),
        Some("test_wh")
    );
    // Session parameters are restored.
    assert_eq!(
        session.session_params.get_property("myvar").as_deref(),
        Some("hello")
    );

    // The persisted record carries the login user.
    let record = state_store.get_session("s1").await.expect("get_session record");
    assert_eq!(record.user_name.as_deref(), Some("test_user"));
    assert_eq!(
        record.metadata.get("UserName").map(String::as_str),
        Some("test_user")
    );
}

// A session whose persisted expiry has passed must not be rehydrated; the
// stale record is cleaned up and a fresh (empty) session is created instead.
#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_expired_session_is_not_rehydrated() {
    let state_store = Arc::new(InMemoryStateStore::new());
    let svc_a = make_service(state_store.clone()).await;

    svc_a
        .create_session("s2", Some(test_metadata()))
        .await
        .expect("Failed to create session");

    // Force the persisted record into the past.
    let mut record = state_store.get_session("s2").await.expect("get_session");
    record.expiry = Some(
        OffsetDateTime::now_utc()
            .unix_timestamp()
            .unsigned_abs()
            .saturating_sub(1),
    );
    state_store.update_session(record).await.expect("update_session");

    let svc_b = make_service(state_store.clone()).await;
    assert!(
        !svc_b.update_session_expiry("s2").await.expect("update_session_expiry"),
        "expired session must not be rehydrated"
    );
    // Stale record was purged; a brand new session is created afterwards.
    assert!(state_store.get_session("s2").await.is_err());

    svc_b
        .create_session("s2", None)
        .await
        .expect("Failed to create new session");
    let session = svc_b.get_session("s2").await.expect("get_session");
    // New session does not carry the old login context.
    assert_eq!(session.metadata.attr(SessionMetadataAttr::UserName), None);
}

// Creating a session with login metadata persists the user for the record.
#[tokio::test]
#[allow(clippy::expect_used)]
async fn test_create_session_persists_metadata() {
    let state_store = Arc::new(InMemoryStateStore::new());
    let svc = make_service(state_store.clone()).await;

    let mut metadata = test_metadata();
    metadata.set_attr(SessionMetadataAttr::ClientAppId, "my-client".to_string());
    svc.create_session("s3", Some(metadata))
        .await
        .expect("Failed to create session");

    let record = state_store.get_session("s3").await.expect("get_session");
    assert_eq!(record.user_name.as_deref(), Some("test_user"));
    let mut expected = HashMap::new();
    expected.insert("UserName".to_string(), "test_user".to_string());
    expected.insert("Warehouse".to_string(), "test_wh".to_string());
    expected.insert("ClientAppId".to_string(), "my-client".to_string());
    assert_eq!(record.metadata, expected);
}
