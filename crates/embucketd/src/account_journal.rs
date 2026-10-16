//! State-store-backed [`AccountJournal`] adapter (design §23, M17.3.1).
//!
//! Bridges the typed `StateStore` backend (Postgres / memory) to the
//! account store's write-through journal: account records are persisted
//! under `domain`/`key` pairs (raw KV, same mechanism as the catalog
//! journal's `state_catalog`) and replayed on bootstrap, so users, roles,
//! grants and shares survive a crash (kill -9) or restart.

use avalanche_account::journal::{AccountJournal, JournalError, JournalResult};
use state_store::StateStore;
use std::fmt;
use std::sync::Arc;

/// Thin adapter: every journal write becomes a raw state-store upsert.
pub struct AccountStateStoreJournal {
    store: Arc<dyn StateStore>,
}

impl fmt::Debug for AccountStateStoreJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountStateStoreJournal").finish_non_exhaustive()
    }
}

impl AccountStateStoreJournal {
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

#[async_trait::async_trait]
impl AccountJournal for AccountStateStoreJournal {
    async fn put(&self, domain: &str, key: &str, value: Vec<u8>) -> JournalResult<()> {
        self.store
            .put_raw(domain, key, value)
            .await
            .map_err(backend_err)
    }

    async fn delete(&self, domain: &str, key: &str) -> JournalResult<()> {
        self.store
            .delete_raw(domain, key)
            .await
            .map_err(backend_err)
    }

    async fn list(&self, domain: &str) -> JournalResult<Vec<(String, Vec<u8>)>> {
        self.store.list_raw(domain).await.map_err(backend_err)
    }
}