//! End-to-end warehouse DDL through the query pipeline (design §5, M3).
//! Uses the FakeDriver-backed manager, so no Docker is required. Container
//! level effects are asserted in `avalanche-warehouse` manager tests; these
//! verify the DDL plumbing and state transitions.

use crate::models::QueryContext;
use avalanche_warehouse::WarehouseState;
use datafusion::arrow::util::pretty::pretty_format_batches;

fn fmt(result: &crate::QueryResult) -> String {
    pretty_format_batches(&result.records)
        .map(|t| t.to_string())
        .unwrap_or_else(|_| "<no rows>".to_string())
}

#[tokio::test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn warehouse_ddl_lifecycle_over_query_pipeline() {
    let session = crate::tests::query::create_df_session().await;

    // CREATE
    let mut exec = session.query(
        "CREATE WAREHOUSE analytics WITH SIZE = 'MEDIUM' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE",
        QueryContext::default(),
    );
    let result = exec.execute().await.expect("create warehouse");
    assert!(fmt(&result).contains("Statement executed successfully"));

    // duplicate CREATE fails
    let mut exec = session.query("CREATE WAREHOUSE analytics", QueryContext::default());
    assert!(exec.execute().await.is_err());

    // IF NOT EXISTS succeeds on duplicate
    let mut exec = session.query(
        "CREATE WAREHOUSE IF NOT EXISTS analytics",
        QueryContext::default(),
    );
    exec.execute().await.expect("if not exists");

    // SHOW WAREHOUSES is Snowflake-shaped
    let mut exec = session.query("SHOW WAREHOUSES", QueryContext::default());
    let result = exec.execute().await.expect("show warehouses");
    let rendered = fmt(&result);
    assert!(rendered.contains("ANALYTICS"), "show output: {rendered}");
    assert!(rendered.contains("RUNNING"), "show output: {rendered}");
    assert!(rendered.contains("MEDIUM"), "show output: {rendered}");
    assert!(rendered.contains("CREATED_ON"), "show output: {rendered}");

    // DESCRIBE WAREHOUSE
    let mut exec = session.query("DESC WAREHOUSE analytics", QueryContext::default());
    let result = exec.execute().await.expect("describe");
    assert!(fmt(&result).contains("ANALYTICS"));

    // ALTER size re-provisions the pool
    let mut exec = session.query(
        "ALTER WAREHOUSE analytics SET WAREHOUSE_SIZE = 'LARGE'",
        QueryContext::default(),
    );
    exec.execute().await.expect("alter size");
    let wh = session.warehouse.get("ANALYTICS").await.expect("get");
    assert_eq!(wh.size, avalanche_warehouse::WarehouseSize::Large);
    assert_eq!(wh.state, WarehouseState::Running);
    assert_eq!(wh.executor_count(), 8);

    // SUSPEND: state flips, executor count goes to 0
    let mut exec = session.query("ALTER WAREHOUSE analytics SUSPEND", QueryContext::default());
    exec.execute().await.expect("suspend");
    let wh = session.warehouse.get("ANALYTICS").await.expect("get");
    assert_eq!(wh.state, WarehouseState::Suspended);
    assert_eq!(wh.executor_count(), 0);

    // RESUME via ALTER
    let mut exec = session.query("ALTER WAREHOUSE analytics RESUME", QueryContext::default());
    exec.execute().await.expect("resume");
    let wh = session.warehouse.get("ANALYTICS").await.expect("get");
    assert_eq!(wh.state, WarehouseState::Running);

    // DROP
    let mut exec = session.query("DROP WAREHOUSE analytics", QueryContext::default());
    exec.execute().await.expect("drop");
    let mut exec = session.query("SHOW WAREHOUSES", QueryContext::default());
    let result = exec.execute().await.expect("show after drop");
    assert!(
        !fmt(&result).contains("ANALYTICS"),
        "drop should remove the row"
    );
}

#[tokio::test]
#[allow(clippy::unwrap_used, clippy::expect_used)]
async fn use_warehouse_enables_resume_on_query() {
    let session = crate::tests::query::create_df_session().await;

    let mut exec = session.query(
        "CREATE WAREHOUSE etl WITH AUTO_RESUME = TRUE",
        QueryContext::default(),
    );
    exec.execute().await.expect("create");

    let mut exec = session.query("ALTER WAREHOUSE etl SUSPEND", QueryContext::default());
    exec.execute().await.expect("suspend");

    // Point the session at the warehouse; the next query auto-resumes it.
    let mut exec = session.query("USE WAREHOUSE etl", QueryContext::default());
    exec.execute().await.expect("use warehouse");

    let mut exec = session.query("SELECT 1", QueryContext::default());
    exec.execute().await.expect("select");

    let warehouse = session.warehouse.get("ETL").await.expect("get");
    assert_eq!(warehouse.state, WarehouseState::Running);

    session
        .warehouse
        .drop_warehouse("ETL")
        .await
        .expect("cleanup");
}
