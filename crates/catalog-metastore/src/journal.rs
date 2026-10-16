//! Durable catalog journal (design §23.0, M17.0).
//!
//! `InMemoryMetastore` keeps the catalog in HashMaps; this module defines the
//! write-through sink that makes those records survive a restart. Every
//! metastore mutation journals its record *before* mutating memory, and a
//! fresh metastore replays the journal at construction (bootstrap replay).
//!
//! The journal is a flat KV space per logical domain (`volumes`, `databases`,
//! `schemas`, `tables`, `dropped_tables`, `streams`, `tasks`,
//! `dynamic_tables`). Values are serde-serialized metastore records; the key
//! is derived from the record's ident so replay can recompute keys without
//! storing them separately. Dropped-table tombstones (design §15, M9.3)
//! include the drop timestamp so UNDROP semantics survive restarts.

use crate::models::{
    RwObject,
    automation::{DynamicTable, Stream, Task},
    database::Database,
    schema::Schema,
    table::{Table, TableIdent},
    volumes::Volume,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::fmt::Debug;
use std::sync::Arc;

/// Logical domains of the catalog journal. Each domain's values deserialize
/// to exactly one record type (see [`catalog_record_from_bytes`]).
pub mod domain {
    pub const VOLUMES: &str = "volumes";
    pub const DATABASES: &str = "databases";
    pub const SCHEMAS: &str = "schemas";
    pub const TABLES: &str = "tables";
    pub const DROPPED_TABLES: &str = "dropped_tables";
    pub const STREAMS: &str = "streams";
    pub const TASKS: &str = "tasks";
    pub const DYNAMIC_TABLES: &str = "dynamic_tables";

    /// Replay order: dropped-table tombstones are applied before live
    /// records so a record present in both (crash between the two journal
    /// writes of an undrop, or a re-create over a drop) resolves to live.
    pub const REPLAY_ORDER: [&str; 8] = [
        DROPPED_TABLES,
        VOLUMES,
        DATABASES,
        SCHEMAS,
        TABLES,
        STREAMS,
        TASKS,
        DYNAMIC_TABLES,
    ];
}

/// The record payloads stored per journal domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum CatalogRecord {
    Volume(RwObject<Volume>),
    Database(RwObject<Database>),
    Schema(RwObject<Schema>),
    Table(RwObject<Table>),
    /// Tombstone for a dropped table (record + epoch-ms drop time).
    DroppedTable(RwObject<Table>, i64),
    Stream(RwObject<Stream>),
    Task(RwObject<Task>),
    DynamicTable(RwObject<DynamicTable>),
}

impl CatalogRecord {
    /// Domain this record is stored under (tombstones get their own domain
    /// so replay can apply them first — see [`domain::REPLAY_ORDER`]).
    #[must_use]
    pub fn domain(&self) -> &'static str {
        match self {
            Self::Volume(_) => domain::VOLUMES,
            Self::Database(_) => domain::DATABASES,
            Self::Schema(_) => domain::SCHEMAS,
            Self::Table(_) => domain::TABLES,
            Self::DroppedTable(..) => domain::DROPPED_TABLES,
            Self::Stream(_) => domain::STREAMS,
            Self::Task(_) => domain::TASKS,
            Self::DynamicTable(_) => domain::DYNAMIC_TABLES,
        }
    }

    /// The record's ident, lowercased — the journal key (see module docs).
    /// `DroppedTable` uses the *dropped table's* ident.
    #[must_use]
    pub fn journal_key(&self) -> String {
        match self {
            Self::Volume(v) => v.data.ident.to_ascii_lowercase(),
            Self::Database(d) => d.data.ident.to_ascii_lowercase(),
            Self::Schema(s) => format!(
                "{}{}{}",
                s.data.ident.database.to_ascii_lowercase(),
                key_sep(),
                s.data.ident.schema.to_ascii_lowercase()
            ),
            Self::Table(t) => table_key_string(&t.data.ident),
            Self::DroppedTable(t, _) => table_key_string(&t.data.ident),
            Self::Stream(s) => table_key_string(&s.data.ident),
            Self::Task(t) => table_key_string(&t.data.ident),
            Self::DynamicTable(d) => table_key_string(&d.data.ident),
        }
    }

    /// Serialize for the journal.
    ///
    /// # Errors
    /// When the record fails to serialize (models are serde-backed, so in
    /// practice this is infallible).
    pub fn to_bytes(&self) -> JournalResult<Bytes> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| {
                JournalError::Backend {
                    message: format!("serialize catalog record: {e}"),
                }
            })
    }

    /// Deserialize a record and validate it belongs to `domain`.
    ///
    /// # Errors
    /// When the bytes do not deserialize to a catalog record, or the record
    /// does not belong to the given domain.
    pub fn from_bytes(domain: &str, bytes: &[u8]) -> JournalResult<Self> {
        let record: Self = serde_json::from_slice(bytes).map_err(|e| {
            JournalError::Backend {
                message: format!("deserialize catalog record: {e}"),
            }
        })?;
        if record.domain() != domain {
            return Err(JournalError::Backend {
                message: format!(
                    "record belongs to domain '{}' but was read from '{domain}'",
                    record.domain()
                ),
            });
        }
        Ok(record)
    }
}

/// Journal keys avoid `.`-ambiguity between quoted idents by using a
/// separator that cannot appear in any identifier.
#[must_use]
pub fn key_sep() -> char {
    '\u{1}'
}

#[must_use]
pub fn table_key_string(ident: &TableIdent) -> String {
    format!(
        "{}{sep}{}{sep}{}",
        ident.database.to_ascii_lowercase(),
        ident.schema.to_ascii_lowercase(),
        ident.table.to_ascii_lowercase(),
        sep = key_sep()
    )
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum JournalError {
    #[snafu(display("journal backend error: {message}"))]
    Backend { message: String },
}

pub type JournalResult<T> = std::result::Result<T, JournalError>;

/// Write-through sink for catalog records (see module docs). Implemented by
/// the state store (PG / memory) via a thin adapter.
#[async_trait]
pub trait CatalogJournal: Debug + Send + Sync {
    /// Store (upsert) one record under `domain`/`key`.
    async fn put(&self, domain: &str, key: &str, value: Bytes) -> JournalResult<()>;
    /// Remove the record under `domain`/`key`.
    async fn delete(&self, domain: &str, key: &str) -> JournalResult<()>;
    /// All `(key, value)` pairs in `domain`.
    async fn list(&self, domain: &str) -> JournalResult<Vec<(String, Bytes)>>;
}

/// Convenience: journal one record (serialize + put under its storage
/// domain and derived key).
///
/// # Errors
/// Propagates serialization / backend failures.
pub async fn put_record(
    journal: &Arc<dyn CatalogJournal>,
    record: &CatalogRecord,
) -> JournalResult<()> {
    let domain = record.domain();
    let key = record.journal_key();
    let value = record.to_bytes()?;
    journal.put(domain, &key, value).await
}

/// Convenience: delete the journal entry for a record's ident.
///
/// # Errors
/// Propagates backend failures.
pub async fn delete_record(
    journal: &Arc<dyn CatalogJournal>,
    domain: &str,
    key: &str,
) -> JournalResult<()> {
    journal.delete(domain, key).await
}