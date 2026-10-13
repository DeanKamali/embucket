//! In-memory [`StateStore`] implementation.
//!
//! Intended for tests and local development where no durable store is available.
//! Data is lost when the instance is dropped, which is exactly the semantics
//! of "no externalized state".

use crate::StateStore;
use crate::error::Result;
use crate::error::Error;
use crate::models::{Query, SessionRecord, WarehouseRecord};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Process-memory backed [`StateStore`].
#[derive(Clone, Default, Debug)]
pub struct InMemoryStateStore {
    sessions: Arc<RwLock<HashMap<String, SessionRecord>>>,
    queries: Arc<RwLock<HashMap<String, Query>>>,
    queries_by_request_id: Arc<RwLock<HashMap<String, String>>>,
    warehouses: Arc<RwLock<HashMap<String, WarehouseRecord>>>,
}

impl InMemoryStateStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl StateStore for InMemoryStateStore {
    async fn put_new_session(&self, session_id: &str) -> Result<()> {
        self.put_session(SessionRecord::new(session_id)).await
    }

    async fn put_session(&self, session: SessionRecord) -> Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.session_id.clone(), session);
        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<SessionRecord> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        self.sessions.write().await.remove(session_id);
        Ok(())
    }

    async fn update_session(&self, session: SessionRecord) -> Result<()> {
        self.put_session(session).await
    }

    async fn put_query(&self, query: &Query) -> Result<()> {
        let mut queries = self.queries.write().await;
        if let Some(request_id) = &query.request_id {
            self.queries_by_request_id
                .write()
                .await
                .insert(request_id.to_string(), query.query_id.to_string());
        }
        queries.insert(query.query_id.to_string(), query.clone());
        Ok(())
    }

    async fn get_query(&self, query_id: &str) -> Result<Query> {
        self.queries
            .read()
            .await
            .get(query_id)
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn get_query_by_request_id(&self, request_id: &str) -> Result<Query> {
        let query_id = self
            .queries_by_request_id
            .read()
            .await
            .get(request_id)
            .cloned()
            .ok_or(Error::NotFound)?;
        self.get_query(&query_id).await
    }

    async fn get_queries_by_session_id(&self, session_id: &str) -> Result<Vec<Query>> {
        let queries = self.queries.read().await;
        Ok(queries
            .values()
            .filter(|query| query.session_id == session_id)
            .cloned()
            .collect())
    }

    async fn delete_query(&self, query_id: &str) -> Result<()> {
        let mut queries = self.queries.write().await;
        if let Some(request_id) = queries.remove(query_id).and_then(|q| q.request_id) {
            self.queries_by_request_id
                .write()
                .await
                .remove(&request_id.to_string());
        }
        Ok(())
    }

    async fn update_query(&self, query: &Query) -> Result<()> {
        self.put_query(query).await
    }

    async fn put_warehouse(&self, warehouse: WarehouseRecord) -> Result<()> {
        self.warehouses
            .write()
            .await
            .insert(warehouse.name.clone(), warehouse);
        Ok(())
    }

    async fn get_warehouse(&self, name: &str) -> Result<WarehouseRecord> {
        self.warehouses
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn list_warehouses(&self) -> Result<Vec<WarehouseRecord>> {
        Ok(self.warehouses.read().await.values().cloned().collect())
    }

    async fn delete_warehouse(&self, name: &str) -> Result<()> {
        self.warehouses.write().await.remove(name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Variable;

    #[tokio::test]
    async fn test_session_round_trip() {
        let store = InMemoryStateStore::new();
        store.put_new_session("s1").await.expect("put_new_session");

        let mut record = store.get_session("s1").await.expect("get_session");
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

        let loaded = store.get_session("s1").await.expect("get_session");
        assert_eq!(loaded.user_name.as_deref(), Some("user"));
        assert_eq!(loaded.expiry, Some(12345));
        assert_eq!(loaded.variables["warehouse"].value, "wh");

        store.delete_session("s1").await.expect("delete_session");
        assert!(store.get_session("s1").await.is_err());
    }

    #[tokio::test]
    async fn test_query_round_trip() {
        let store = InMemoryStateStore::new();
        let query_id = uuid::Uuid::new_v4();
        let query = Query::new("SELECT 1", query_id, "s1", None);
        store.put_query(&query).await.expect("put_query");

        assert_eq!(
            store
                .get_query(&query_id.to_string())
                .await
                .expect("get_query")
                .session_id,
            "s1"
        );
        assert_eq!(
            store
                .get_queries_by_session_id("s1")
                .await
                .expect("get_queries_by_session_id")
                .len(),
            1
        );
        store.delete_query(&query_id.to_string()).await.expect("delete_query");
        assert!(store.get_query(&query_id.to_string()).await.is_err());
    }
}
