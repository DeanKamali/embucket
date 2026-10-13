//! Postgres-backed [`StateStore`] implementation.
//!
//! Open-source, multi-process durable store for sessions and query history.
//! Records are stored as `JSONB` documents keyed by their natural ids, which
//! keeps the schema simple and lets the serde models evolve without
//! migrations for additive changes.

use crate::StateStore;
use crate::config::PgConfig;
use crate::error::{
    Error, FailedToDeserializeRecordSnafu, FailedToSerializeRecordSnafu, PgPoolBuildSnafu,
    PgPoolSnafu, PgSnafu, Result,
};
use crate::models::{Query, SessionRecord, WarehouseRecord};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use std::str::FromStr;
use tokio_postgres::NoTls;

/// [`StateStore`] backed by Postgres.
#[derive(Clone, Debug)]
pub struct PostgresStateStore {
    pool: Pool,
}

impl PostgresStateStore {
    /// Create a store from environment configuration (`STATESTORE_PG_URL` or
    /// `DATABASE_URL`) and ensure the schema exists.
    pub async fn new_from_env() -> Result<Self> {
        let config = PgConfig::from_env()?;
        Self::new(&config.connection_url).await
    }

    /// Create a store from a libpq connection URL and ensure the schema exists.
    pub async fn new(connection_url: &str) -> Result<Self> {
        let pg_config = tokio_postgres::Config::from_str(connection_url).context(PgSnafu)?;
        let mgr = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(mgr)
            .max_size(16)
            .build()
            .context(PgPoolBuildSnafu)?;
        let store = Self { pool };
        store.ensure_tables().await?;
        Ok(store)
    }

    #[allow(clippy::expect_used)]
    pub async fn new_from_pool(pool: Pool) -> Self {
        let store = Self { pool };
        store
            .ensure_tables()
            .await
            .expect("Failed to ensure state store schema");
        store
    }

    async fn ensure_tables(&self) -> Result<()> {
        let client = self.client().await?;
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS state_sessions (
                    session_id TEXT PRIMARY KEY,
                    record JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS state_queries (
                    query_id TEXT PRIMARY KEY,
                    request_id TEXT,
                    session_id TEXT NOT NULL,
                    record JSONB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS state_warehouses (
                    name TEXT PRIMARY KEY,
                    record JSONB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS state_queries_session_id
                    ON state_queries (session_id);
                CREATE INDEX IF NOT EXISTS state_queries_request_id
                    ON state_queries (request_id)
                    WHERE request_id IS NOT NULL;",
            )
            .await
            .context(PgSnafu)
    }

    async fn client(&self) -> Result<deadpool_postgres::Client> {
        self.pool.get().await.context(PgPoolSnafu)
    }

    async fn upsert<T: Serialize + Sync>(
        &self,
        table: &str,
        key: (&str, &str),
        record: &T,
    ) -> Result<()> {
        let json = serde_json::to_value(record).context(FailedToSerializeRecordSnafu)?;
        let client = self.client().await?;
        let sql = format!(
            "INSERT INTO {table} ({k0}, record) VALUES ($1, $2)
             ON CONFLICT ({k0}) DO UPDATE SET record = EXCLUDED.record",
            k0 = key.0
        );
        client
            .execute(&sql, &[&key.1, &json])
            .await
            .context(PgSnafu)?;
        Ok(())
    }
}

async fn get_one<T: DeserializeOwned>(
    store: &PostgresStateStore,
    sql: &str,
    param: &str,
) -> Result<T> {
    let client = store.client().await?;
    let rows = client.query(sql, &[&param]).await.context(PgSnafu)?;
    let row = rows.into_iter().next().ok_or(Error::NotFound)?;
    let json: serde_json::Value = row.try_get("record").context(PgSnafu)?;
    serde_json::from_value(json).context(FailedToDeserializeRecordSnafu)
}

#[async_trait::async_trait]
impl StateStore for PostgresStateStore {
    async fn put_new_session(&self, session_id: &str) -> Result<()> {
        self.put_session(SessionRecord::new(session_id)).await
    }

    async fn put_session(&self, session: SessionRecord) -> Result<()> {
        self.upsert(
            "state_sessions",
            ("session_id", &session.session_id),
            &session,
        )
        .await
    }

    async fn get_session(&self, session_id: &str) -> Result<SessionRecord> {
        get_one(
            self,
            "SELECT record FROM state_sessions WHERE session_id = $1",
            session_id,
        )
        .await
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "DELETE FROM state_sessions WHERE session_id = $1",
                &[&session_id],
            )
            .await
            .context(PgSnafu)?;
        Ok(())
    }

    async fn update_session(&self, session: SessionRecord) -> Result<()> {
        self.put_session(session).await
    }

    async fn put_query(&self, query: &Query) -> Result<()> {
        let json = serde_json::to_value(query).context(FailedToSerializeRecordSnafu)?;
        let client = self.client().await?;
        client
            .execute(
                "INSERT INTO state_queries (query_id, request_id, session_id, record)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (query_id) DO UPDATE
                 SET request_id = EXCLUDED.request_id,
                     session_id = EXCLUDED.session_id,
                     record = EXCLUDED.record",
                &[
                    &query.query_id.to_string(),
                    &query.request_id.as_ref().map(ToString::to_string),
                    &query.session_id,
                    &json,
                ],
            )
            .await
            .context(PgSnafu)?;
        Ok(())
    }

    async fn get_query(&self, query_id: &str) -> Result<Query> {
        get_one(
            self,
            "SELECT record FROM state_queries WHERE query_id = $1",
            query_id,
        )
        .await
    }

    async fn get_query_by_request_id(&self, request_id: &str) -> Result<Query> {
        get_one(
            self,
            "SELECT record FROM state_queries WHERE request_id = $1",
            request_id,
        )
        .await
    }

    async fn get_queries_by_session_id(&self, session_id: &str) -> Result<Vec<Query>> {
        let client = self.client().await?;
        let rows = client
            .query(
                "SELECT record FROM state_queries WHERE session_id = $1",
                &[&session_id],
            )
            .await
            .context(PgSnafu)?;
        rows.into_iter()
            .map(|row| {
                let json: serde_json::Value = row.try_get("record").context(PgSnafu)?;
                serde_json::from_value(json).context(FailedToDeserializeRecordSnafu)
            })
            .collect()
    }

    async fn delete_query(&self, query_id: &str) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                "DELETE FROM state_queries WHERE query_id = $1",
                &[&query_id],
            )
            .await
            .context(PgSnafu)?;
        Ok(())
    }

    async fn update_query(&self, query: &Query) -> Result<()> {
        self.put_query(query).await
    }

    async fn put_warehouse(&self, warehouse: WarehouseRecord) -> Result<()> {
        self.upsert("state_warehouses", ("name", &warehouse.name), &warehouse)
            .await
    }

    async fn get_warehouse(&self, name: &str) -> Result<WarehouseRecord> {
        get_one(
            self,
            "SELECT record FROM state_warehouses WHERE name = $1",
            name,
        )
        .await
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseRecord>> {
        let client = self.client().await?;
        let rows = client
            .query("SELECT record FROM state_warehouses", &[])
            .await
            .context(PgSnafu)?;
        rows.into_iter()
            .map(|row| {
                let json: serde_json::Value = row.try_get("record").context(PgSnafu)?;
                serde_json::from_value(json).context(FailedToDeserializeRecordSnafu)
            })
            .collect()
    }

    async fn delete_warehouse(&self, name: &str) -> Result<()> {
        let client = self.client().await?;
        client
            .execute("DELETE FROM state_warehouses WHERE name = $1", &[&name])
            .await
            .context(PgSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Variable;
    use std::collections::HashMap;

    const TEST_URL_ENV: &str = "STATESTORE_TEST_PG_URL";

    /// Returns a connection URL for a live Postgres, or `None` when the
    /// integration test prerequisites are missing (env not set / server down).
    async fn test_store() -> Option<PostgresStateStore> {
        let url = std::env::var(TEST_URL_ENV).ok()?;
        match PostgresStateStore::new(&url).await {
            Ok(store) => Some(store),
            Err(err) => {
                eprintln!("skipping Postgres state store test: {err}");
                None
            }
        }
    }

    #[tokio::test]
    async fn test_session_round_trip() {
        let Some(store) = test_store().await else {
            return;
        };
        store
            .put_new_session("pg-s1")
            .await
            .expect("put_new_session");

        let mut record = store.get_session("pg-s1").await.expect("get_session");
        record.user_name = Some("user".to_string());
        record.expiry = Some(12345);
        record.variables.insert(
            "warehouse".to_string(),
            Variable {
                name: "warehouse".to_string(),
                value: "wh".to_string(),
                value_type: "text".to_string(),
                comment: None,
                created_at: 0,
                updated_at: None,
            },
        );
        store.update_session(record).await.expect("update_session");

        let loaded = store.get_session("pg-s1").await.expect("get_session");
        assert_eq!(loaded.user_name.as_deref(), Some("user"));
        assert_eq!(loaded.expiry, Some(12345));
        assert_eq!(loaded.variables["warehouse"].value, "wh");
    }

    #[tokio::test]
    async fn test_query_round_trip() {
        let Some(store) = test_store().await else {
            return;
        };
        let query_id = uuid::Uuid::new_v4();
        let query = Query::new("SELECT 1", query_id, "pg-s1", None);
        store.put_query(&query).await.expect("put_query");

        let loaded = store
            .get_query(&query_id.to_string())
            .await
            .expect("get_query");
        assert_eq!(loaded.session_id, "pg-s1");

        let by_session = store
            .get_queries_by_session_id("pg-s1")
            .await
            .expect("get_queries_by_session_id");
        assert_eq!(by_session.len(), 1);

        store
            .delete_query(&query_id.to_string())
            .await
            .expect("delete_query");
        assert!(store.get_query(&query_id.to_string()).await.is_err());
    }

    // Silence unused warning for HashMap import used by doc example above.
    #[allow(dead_code)]
    fn _assert_hashmap() {
        let _ = HashMap::<String, String>::new();
    }
}
