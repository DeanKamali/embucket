use crate::error::{Error, Result};
use std::env;

/// Postgres connection settings for the persistent state store.
#[derive(Debug, Clone)]
pub struct PgConfig {
    /// libpq-style connection URL, e.g. `postgres://user:pass@host:5432/db`.
    pub connection_url: String,
}

impl PgConfig {
    /// Read the connection URL from the environment.
    ///
    /// Uses `STATESTORE_PG_URL`, falling back to `DATABASE_URL`.
    pub fn from_env() -> Result<Self> {
        let connection_url = env::var("STATESTORE_PG_URL")
            .or_else(|_| env::var("DATABASE_URL"))
            .map_err(|_| Error::MissingEnvVar {
                reason: "STATESTORE_PG_URL (or DATABASE_URL)".to_string(),
            })?;
        Ok(Self { connection_url })
    }
}
