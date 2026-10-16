//! State-store-backed [`CatalogJournal`] adapter (design §23, M17.0).
//!
//! Bridges the typed `StateStore` backend (Postgres / memory) to the
//! metastore's write-through journal: raw catalog records are persisted
//! under `domain`/`key` pairs and replayed on bootstrap, so the catalog
//! survives a crash (kill -9) or restart.

use async_trait::async_trait;
use bytes::Bytes;
use catalog_metastore::CatalogJournal;
use catalog_metastore::JournalError;
use catalog_metastore::JournalResult;
use state_store::StateStore;
use std::fmt;
use std::sync::Arc;

/// Thin adapter: every journal write becomes a raw state-store upsert.
pub struct StateStoreJournal {
    store: Arc<dyn StateStore>,
}

impl fmt::Debug for StateStoreJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoreJournal").finish_non_exhaustive()
    }
}

impl StateStoreJournal {
    #[must_use]
    pub fn new(store: Arc<dyn StateStore>) -> Self {
        Self { store }
    }
}

fn backend_err(e: state_store::Error) -> JournalError {
    JournalError::Backend {
        message: e.to_string(),
    }
}

#[async_trait]
impl CatalogJournal for StateStoreJournal {
    async fn put(&self, domain: &str, key: &str, value: Bytes) -> JournalResult<()> {
        self.store
            .put_raw(domain, key, value.to_vec())
            .await
            .map_err(backend_err)
    }

    async fn delete(&self, domain: &str, key: &str) -> JournalResult<()> {
        self.store
            .delete_raw(domain, key)
            .await
            .map_err(backend_err)
    }

    async fn list(&self, domain: &str) -> JournalResult<Vec<(String, Bytes)>> {
        let entries = self.store.list_raw(domain).await.map_err(backend_err)?;
        Ok(entries
            .into_iter()
            .map(|(key, value)| (key, Bytes::from(value)))
            .collect())
    }
}