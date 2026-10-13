//! Avalanche warehouse management: lifecycle of Snowflake-style virtual
//! warehouses, each backed by a Ballista cluster (design §5).

pub mod driver;
pub mod error;
pub mod manager;
#[cfg(test)]
mod manager_tests;
pub mod model;

pub use driver::{ContainerDriver, ContainerSpec, DockerCliDriver, FakeDriver};
pub use error::{Error, Result};
pub use manager::{
    BallistaImages, CreateWarehouseRequest, WarehouseManager, executor_container_name,
    scheduler_container_name,
};
pub use model::{Warehouse, WarehouseSize, WarehouseState};
