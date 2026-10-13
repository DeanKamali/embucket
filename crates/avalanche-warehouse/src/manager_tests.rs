use crate::driver::{ContainerDriver, DockerCliDriver, FakeDriver};
use crate::manager::{
    BallistaImages, CreateWarehouseRequest, WarehouseManager, executor_container_name,
    scheduler_container_name,
};
use crate::model::{WarehouseSize, WarehouseState};
use state_store::{InMemoryStateStore, StateStore as _};
use std::sync::Arc;

fn test_manager() -> (Arc<WarehouseManager>, Arc<FakeDriver>) {
    let driver = Arc::new(FakeDriver::new());
    let manager = Arc::new(WarehouseManager::new(
        Arc::new(InMemoryStateStore::new()),
        driver.clone(),
        BallistaImages::default(),
        Some("avalanche-test".to_string()),
    ));
    (manager, driver)
}

/// Like [`test_manager`], but also hands back the state store so tests can
/// inspect the persisted warehouse records.
fn test_manager_with_store() -> (
    Arc<WarehouseManager>,
    Arc<FakeDriver>,
    Arc<InMemoryStateStore>,
) {
    let driver = Arc::new(FakeDriver::new());
    let store = Arc::new(InMemoryStateStore::new());
    let manager = Arc::new(WarehouseManager::new(
        store.clone(),
        driver.clone(),
        BallistaImages::default(),
        Some("avalanche-test".to_string()),
    ));
    (manager, driver, store)
}

fn create_req(name: &str, size: WarehouseSize) -> CreateWarehouseRequest {
    CreateWarehouseRequest {
        name: name.to_string(),
        size,
        auto_suspend_secs: 0,
        auto_resume: true,
        initially_suspended: false,
        statement_timeout_secs: None,
        storage_credentials: None,
        tls: false,
    }
}

#[tokio::test]
async fn statement_timeout_round_trips() {
    let (manager, _driver, store) = test_manager_with_store();

    let mut req = create_req("TIMEOUT", WarehouseSize::Xsmall);
    req.statement_timeout_secs = Some(45);
    manager.create(req).await.expect("create");
    let record = store.get_warehouse("TIMEOUT").await.expect("record");
    assert_eq!(record.statement_timeout_secs, Some(45));

    // ALTER clears on 0 (falls back to the global timeout)
    manager
        .alter("TIMEOUT", None, None, None, Some(0))
        .await
        .expect("alter");
    let record = store.get_warehouse("TIMEOUT").await.expect("record");
    assert_eq!(record.statement_timeout_secs, None);
}

#[tokio::test]
async fn create_starts_scheduler_and_executor_pool() {
    let (manager, driver) = test_manager();

    let wh = manager
        .create(create_req("analytics", WarehouseSize::Medium))
        .await
        .expect("create");
    assert_eq!(wh.state, WarehouseState::Running);
    assert_eq!(wh.size, WarehouseSize::Medium);
    assert_eq!(wh.executor_count(), 4);

    assert!(driver.running(&scheduler_container_name("analytics")));
    for i in 0..4 {
        assert!(driver.running(&executor_container_name("analytics", i)));
    }
    // medium = 4 executors, not 5
    assert!(!driver.running(&executor_container_name("analytics", 4)));
}

#[tokio::test]
async fn create_rejects_duplicate() {
    let (manager, _driver) = test_manager();
    manager
        .create(create_req("dupe", WarehouseSize::Xsmall))
        .await
        .expect("create");
    assert!(manager.create(create_req("dupe", WarehouseSize::Xsmall)).await.is_err());
}

#[tokio::test]
async fn suspend_stops_executors_keeps_scheduler() {
    let (manager, driver) = test_manager();
    manager
        .create(create_req("wh1", WarehouseSize::Small))
        .await
        .expect("create");

    let wh = manager.suspend("wh1").await.expect("suspend");
    assert_eq!(wh.state, WarehouseState::Suspended);
    assert_eq!(wh.executor_count(), 0);
    assert!(driver.running(&scheduler_container_name("wh1")), "scheduler stays up");
    for i in 0..2 {
        assert!(!driver.running(&executor_container_name("wh1", i)));
    }
}

#[tokio::test]
async fn resume_on_suspended_warehouse_scales_out() {
    let (manager, driver) = test_manager();
    manager
        .create(create_req("wh2", WarehouseSize::Small))
        .await
        .expect("create");
    manager.suspend("wh2").await.expect("suspend");
    assert!(!driver.running(&executor_container_name("wh2", 0)));

    let wh = manager.resume("wh2").await.expect("resume");
    assert_eq!(wh.state, WarehouseState::Running);
    assert_eq!(wh.executor_count(), 2);
    assert!(driver.running(&executor_container_name("wh2", 0)));
    assert!(driver.running(&executor_container_name("wh2", 1)));
}

#[tokio::test]
async fn ensure_running_auto_resumes_suspended_warehouse() {
    let (manager, _driver) = test_manager();
    manager
        .create(create_req("wh3", WarehouseSize::Xsmall))
        .await
        .expect("create");
    manager.suspend("wh3").await.expect("suspend");

    // Query-path hook must not fail on unknown warehouses...
    manager.ensure_running("not-a-real-warehouse").await;
    // ...and must auto-resume a suspended managed one.
    manager.ensure_running("wh3").await;
    let wh = manager.get("wh3").await.expect("get");
    assert_eq!(wh.state, WarehouseState::Running);
}

#[tokio::test]
async fn initially_suspended_creates_without_compute() {
    let (manager, driver) = test_manager();
    let mut req = create_req("cold", WarehouseSize::Large);
    req.initially_suspended = true;
    let wh = manager.create(req).await.expect("create");
    assert_eq!(wh.state, WarehouseState::Suspended);
    assert_eq!(wh.executor_count(), 0);
    assert!(!driver.running(&scheduler_container_name("cold")));
    // resume-on-query brings the full pool up
    manager.ensure_running("cold").await;
    assert!(driver.running(&scheduler_container_name("cold")));
    assert_eq!(driver.started_count("avalanche-wh-cold-exec"), 8);
}

#[tokio::test]
async fn alter_resizes_running_pool() {
    let (manager, _driver) = test_manager();
    manager
        .create(create_req("wh4", WarehouseSize::Xsmall))
        .await
        .expect("create");
    let wh = manager
        .alter("wh4", Some(WarehouseSize::Medium), None, None, None)
        .await
        .expect("alter");
    assert_eq!(wh.size, WarehouseSize::Medium);
    assert_eq!(wh.executor_count(), 4);
}

#[tokio::test]
async fn drop_stops_everything_and_removes_record() {
    let (manager, driver) = test_manager();
    manager
        .create(create_req("wh5", WarehouseSize::Small))
        .await
        .expect("create");
    manager.drop_warehouse("wh5").await.expect("drop");
    assert!(manager.get("wh5").await.is_err());
    assert!(!driver.running(&scheduler_container_name("wh5")));
    assert!(!driver.running(&executor_container_name("wh5", 0)));
    assert_eq!(driver.stopped_count("avalanche-wh-wh5"), 3); // 2 execs + scheduler
}

#[tokio::test]
async fn list_returns_all_warehouses_sorted() {
    let (manager, _driver) = test_manager();
    manager
        .create(create_req("b-wh", WarehouseSize::Xsmall))
        .await
        .expect("create");
    manager
        .create(create_req("a-wh", WarehouseSize::Xsmall))
        .await
        .expect("create");
    let names: Vec<String> = manager
        .list()
        .await
        .expect("list")
        .into_iter()
        .map(|wh| wh.name)
        .collect();
    assert_eq!(names, vec!["a-wh", "b-wh"]);
}

// The Docker CLI driver is exercised only when docker is available; the test
// verifies the command plumbing (inspect a container that never existed).
#[tokio::test]
async fn docker_cli_driver_exists_false_for_missing() {
    let driver = DockerCliDriver::new();
    assert!(!driver.exists("avalanche-does-not-exist-xyz").await.expect("exists"));
}

// Ballista scheduler gRPC port, as bound by start_compute.
const SCHEDULER_INTERNAL_PORT: u16 = 50050;

#[tokio::test]
async fn create_allocates_and_persists_scheduler_host_port() {
    let (manager, driver, store) = test_manager_with_store();
    manager
        .create(create_req("portwh", WarehouseSize::Xsmall))
        .await
        .expect("create");

    let record = store.get_warehouse("portwh").await.expect("record");
    let port = record
        .scheduler_host_port
        .expect("scheduler_host_port persisted");
    assert!(port > 0);

    // the scheduler container maps the persisted host port; executors do not
    let sched_spec = driver
        .started_spec(&scheduler_container_name("portwh"))
        .expect("scheduler spec");
    assert_eq!(sched_spec.ports, vec![(port, SCHEDULER_INTERNAL_PORT)]);
    let exec_spec = driver
        .started_spec(&executor_container_name("portwh", 0))
        .expect("executor spec");
    assert!(exec_spec.ports.is_empty());
}

#[tokio::test]
async fn scheduler_url_contains_loopback_host_and_port() {
    let (manager, _driver, store) = test_manager_with_store();
    manager
        .create(create_req("urlwh", WarehouseSize::Xsmall))
        .await
        .expect("create");

    let record = store.get_warehouse("urlwh").await.expect("record");
    let port = record.scheduler_host_port.expect("port");

    let url = manager.scheduler_url("urlwh").await.expect("scheduler_url");
    assert_eq!(url, format!("http://127.0.0.1:{port}"));
    assert!(url.contains("127.0.0.1"));
    assert!(url.contains(&port.to_string()));
}

#[tokio::test]
async fn scheduler_url_errors_for_record_without_port() {
    // simulate a record persisted before scheduler_host_port existed
    let (manager, _driver, store) = test_manager_with_store();
    manager
        .create(create_req("legacy", WarehouseSize::Xsmall))
        .await
        .expect("create");
    let mut record = store.get_warehouse("legacy").await.expect("record");
    record.scheduler_host_port = None;
    store.put_warehouse(record).await.expect("put");

    assert!(manager.scheduler_url("legacy").await.is_err());
}

#[tokio::test]
async fn suspend_resume_keeps_same_scheduler_port() {
    let (manager, driver, store) = test_manager_with_store();
    manager
        .create(create_req("stable", WarehouseSize::Small))
        .await
        .expect("create");
    let before = store
        .get_warehouse("stable")
        .await
        .expect("record")
        .scheduler_host_port
        .expect("port");

    manager.suspend("stable").await.expect("suspend");
    manager.resume("stable").await.expect("resume");

    let after = store
        .get_warehouse("stable")
        .await
        .expect("record")
        .scheduler_host_port
        .expect("port");
    assert_eq!(before, after);

    // resume re-applies the same mapping on the scheduler container
    let sched_spec = driver
        .started_spec(&scheduler_container_name("stable"))
        .expect("scheduler spec");
    assert_eq!(sched_spec.ports, vec![(before, SCHEDULER_INTERNAL_PORT)]);
}
