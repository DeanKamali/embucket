use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by the warehouse manager.
#[derive(Snafu, Debug)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Warehouse '{name}' already exists"))]
    AlreadyExists { name: String },
    #[snafu(display("Warehouse '{name}' not found"))]
    NotFound { name: String },
    #[snafu(display("Invalid warehouse size '{size}': expected one of {}",
        crate::model::WarehouseSize::valid_sizes().join(", ")))]
    InvalidSize { size: String },
    #[snafu(display("Invalid auto-suspend seconds {value}: must be 60-86400, or 0 to disable"))]
    InvalidAutoSuspend { value: u64 },
    #[snafu(display("Invalid warehouse state '{state}' for operation"))]
    InvalidState { state: String },
    #[snafu(display("State store error: {source}"))]
    StateStore {
        source: state_store::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Container driver error: {message}: {detail}"))]
    ContainerDriver { message: String, detail: String },
    #[snafu(display("{message}"))]
    Tls {
        message: String,
        #[snafu(implicit)]
        location: Location,
    },
}
