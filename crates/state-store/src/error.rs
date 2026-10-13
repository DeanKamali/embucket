#![allow(unused_assignments)]
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by the state store helpers.
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
#[error_stack_trace::debug]
pub enum Error {
    #[snafu(display("Environment variable {reason} is not set"))]
    MissingEnvVar { reason: String },
    #[snafu(display("Failed to serialize JSON: {error}"))]
    FailedToParseJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Postgres pool build error: {error}"))]
    PgPoolBuild {
        #[snafu(source(from(deadpool_postgres::BuildError, Box::new)))]
        error: Box<deadpool_postgres::BuildError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Postgres pool error: {error}"))]
    PgPool {
        #[snafu(source(from(deadpool_postgres::PoolError, Box::new)))]
        error: Box<deadpool_postgres::PoolError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Postgres error: {error}"))]
    Pg {
        #[snafu(source(from(tokio_postgres::Error, Box::new)))]
        error: Box<tokio_postgres::Error>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to serialize DB record: {error}"))]
    FailedToSerializeRecord {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to deserialize DB record: {error}"))]
    FailedToDeserializeRecord {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("item not found"))]
    NotFound,
    #[snafu(display("invalid time: {value}"))]
    InvalidTime { value: String },
}
