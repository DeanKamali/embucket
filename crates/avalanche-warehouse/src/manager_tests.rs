use crate::driver::{ContainerDriver, DockerCliDriver, FakeDriver};
use crate::manager::{
    BallistaImages, CreateWarehouseRequest, WarehouseManager, executor_container_name,
    scheduler_container_name,
};
use crate::model::{WarehouseSize, WarehouseState};
use state_store::InMemoryStateStore;
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

fn create_req(name: &str, size: WarehouseSize) -> CreateWarehouseRequest {
    CreateWarehouseRequest {
        name: name.to_string(),
        size,
        auto_suspend_secs: 0,
        auto_resume: true,
        initially_suspended: false,
    }
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
        .alter("wh4", Some(WarehouseSize::Medium), None, None)
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
