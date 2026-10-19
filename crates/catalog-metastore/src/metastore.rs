use crate::error::{self as metastore_error, Result};
use crate::journal::{self, CatalogJournal, CatalogRecord};
use crate::metastore_settings_config::MetastoreSettingsConfig;
use crate::models::{
    RwObject,
    automation::{DynamicTable, Stream, Task},
    database::{Database, DatabaseIdent},
    schema::{Schema, SchemaIdent},
    sequence::Sequence,
    table::{Table, TableCreateRequest, TableFormat, TableIdent, TableRequirementExt, TableUpdate},
    volumes::{Volume, VolumeIdent},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use iceberg_rust::catalog::commit::{TableUpdate as IcebergTableUpdate, apply_table_updates};
use iceberg_rust_spec::{
    schema::Schema as IcebergSchema,
    snapshot::{SnapshotReference, SnapshotRetention},
    table_metadata::{FormatVersion, SnapshotLog, TableMetadataBuilder},
    types::{StructField, Type},
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use snafu::ResultExt;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::instrument;
use uuid::Uuid;

#[async_trait]
pub trait Metastore: std::fmt::Debug + Send + Sync {
    async fn list_volumes(&self) -> Result<Vec<RwObject<Volume>>>;
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>>;
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>>;
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()>;
    async fn volume_object_store(&self, name: &VolumeIdent)
    -> Result<Option<Arc<dyn ObjectStore>>>;

    async fn list_databases(&self) -> Result<Vec<RwObject<Database>>>;
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>>;
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>>;
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()>;

    async fn list_schemas(&self, database: &DatabaseIdent) -> Result<Vec<RwObject<Schema>>>;
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>>;
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>>;
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()>;

    async fn list_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>>;
    async fn create_table(
        &self,
        ident: &TableIdent,
        table: TableCreateRequest,
    ) -> Result<RwObject<Table>>;
    async fn register_table(&self, ident: &TableIdent, table: Table) -> Result<RwObject<Table>>;
    /// The un-scoped metastore behind a wrapper view (M15.5, cross-account
    /// replication refresh): `None` for plain metastores (they are already
    /// raw).
    fn inner_raw(&self) -> Option<std::sync::Arc<dyn Metastore>> {
        None
    }

    /// Zero-copy clone (M8, design §14): creates `to` as a metadata-only
    /// copy of `from` — the clone shares the source's table location,
    /// volume and current metadata lineage (zero data or metadata copy).
    /// Commits to either table after the clone diverge independently.
    async fn clone_table(&self, from: &TableIdent, to: &TableIdent) -> Result<RwObject<Table>>;

    /// Zero-copy as-of clone (M9.4, design §15): creates `to` as a
    /// metadata-only copy of `from` pinned to `snapshot_id` — the clone's
    /// metadata contains only that snapshot (snapshot_log restarts with a
    /// single `as_of_ms` entry), so reads on the clone reproduce the
    /// source's state at that point. Data/manifest files stay under the
    /// shared source location; the clone's pinned metadata JSON is written
    /// under the clone's own prefix. Errors when the snapshot is unknown.
    async fn clone_table_as_of(
        &self,
        from: &TableIdent,
        to: &TableIdent,
        snapshot_id: i64,
        as_of_ms: i64,
    ) -> Result<RwObject<Table>>;

    /// Zero-copy clone of a schema (M8, design §14): copies the schema
    /// record and every contained table against the source's *current*
    /// metadata at clone time. Returns the number of cloned tables.
    async fn clone_schema(&self, from: &SchemaIdent, to: &SchemaIdent) -> Result<usize> {
        let source = self.get_schema(from).await?.ok_or_else(|| {
            metastore_error::SchemaNotFoundSnafu {
                schema: from.schema.clone(),
                db: from.database.clone(),
            }
            .build()
        })?;
        let schema = Schema {
            ident: to.clone(),
            properties: source.data.properties.clone(),
        };
        self.create_schema(to, schema).await?;
        let tables = self.list_tables(from).await?;
        for table in &tables {
            let target = TableIdent::new(&to.database, &to.schema, &table.data.ident.table);
            let source = &table.data.ident;
            self.clone_table(source, &target).await?;
        }
        Ok(tables.len())
    }

    /// Zero-copy clone of a database (M8, design §14): copies the database
    /// record (same volume), every schema, and every table against the
    /// source's current metadata at clone time.
    async fn clone_database(&self, from: &DatabaseIdent, to: &DatabaseIdent) -> Result<usize> {
        let source = self.get_database(from).await?.ok_or_else(|| {
            metastore_error::DatabaseNotFoundSnafu {
                db: from.to_string(),
            }
            .build()
        })?;
        let database = Database {
            ident: to.to_string(),
            properties: source.data.properties.clone(),
            volume: source.data.volume.clone(),
            should_refresh: false,
            account: source.data.account.clone(),
        };
        self.create_database(&to.to_string(), database).await?;
        let mut cloned = 0;
        for schema in self.list_schemas(from).await? {
            let target_schema = SchemaIdent::new(to.to_string(), schema.data.ident.schema.clone());
            let source_schema = schema.data.ident.clone();
            cloned += self.clone_schema(&source_schema, &target_schema).await?;
        }
        Ok(cloned)
    }
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>>;
    async fn update_table(
        &self,
        ident: &TableIdent,
        update: TableUpdate,
    ) -> Result<RwObject<Table>>;
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> Result<()>;

    /// Restore the most recently dropped version of `ident` (design §15,
    /// M9.3). Errors when a live table already occupies the name, or when
    /// there is no tombstone for it.
    async fn undrop_table(&self, ident: &TableIdent) -> Result<RwObject<Table>>;

    /// Restore the most recently dropped schema (M17.2.2): the schema record
    /// plus every table tombstone created by its cascade drop. Errors when a
    /// live schema already occupies the name, when the parent database is
    /// not live, or when there is no tombstone.
    async fn undrop_schema(&self, ident: &SchemaIdent) -> Result<RwObject<Schema>>;

    /// Restore the most recently dropped database (M17.2.2): the database
    /// record plus every schema tombstone and contained-table tombstone
    /// created by its cascade drop. Errors when a live database already
    /// occupies the name or when there is no tombstone.
    async fn undrop_database(&self, ident: &DatabaseIdent) -> Result<RwObject<Database>>;

    /// Tables currently tombstoned (dropped) in `schema`, most recent drop
    /// first.
    async fn list_dropped_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>>;

    /// Schemas currently tombstoned (dropped) in `database`, most recent
    /// drop first (M17.2.2).
    async fn list_dropped_schemas(
        &self,
        database: &DatabaseIdent,
    ) -> Result<Vec<(RwObject<Schema>, i64)>>;

    /// Databases currently tombstoned (dropped), most recent drop first
    /// (M17.2.2).
    async fn list_dropped_databases(&self) -> Result<Vec<(RwObject<Database>, i64)>>;

    /// Zero-copy as-of clone of a schema (M17.2.2, design §15): creates `to`
    /// as a copy of the schema containing every table's state at `point_ms`.
    /// Tables created after the point and dropped before it are excluded;
    /// tables dropped after the point are cloned from their tombstoned
    /// record as of the point. Returns the number of cloned tables.
    async fn clone_schema_as_of(
        &self,
        from: &SchemaIdent,
        to: &SchemaIdent,
        point_ms: i64,
    ) -> Result<usize> {
        let source = self.get_schema(from).await?.ok_or_else(|| {
            metastore_error::SchemaNotFoundSnafu {
                schema: from.schema.clone(),
                db: from.database.clone(),
            }
            .build()
        })?;
        let schema = Schema {
            ident: to.clone(),
            properties: source.data.properties.clone(),
        };
        self.create_schema(to, schema).await?;
        let mut cloned = 0usize;
        for table in self.list_tables(from).await? {
            if self.clone_table_record_as_of(&table, to, point_ms).await? {
                cloned += 1;
            }
        }
        // Tables dropped after the point (or whose drop is still pending
        // undrop) existed live at `point_ms` — clone them from their
        // tombstoned record.
        for (table, dropped_at) in self.list_dropped_tables_with_time(from).await? {
            if dropped_at > point_ms && self.clone_table_record_as_of(&table, to, point_ms).await? {
                cloned += 1;
            }
        }
        Ok(cloned)
    }

    /// Zero-copy as-of clone of a database (M17.2.2, design §15): creates
    /// `to` with every schema's state at `point_ms` (see
    /// [`Metastore::clone_schema_as_of`]). Returns the number of cloned
    /// tables across all schemas.
    async fn clone_database_as_of(
        &self,
        from: &DatabaseIdent,
        to: &DatabaseIdent,
        point_ms: i64,
    ) -> Result<usize> {
        let source = self.get_database(from).await?.ok_or_else(|| {
            metastore_error::DatabaseNotFoundSnafu {
                db: from.to_string(),
            }
            .build()
        })?;
        let database = Database {
            ident: to.to_string(),
            properties: source.data.properties.clone(),
            volume: source.data.volume.clone(),
            should_refresh: false,
            account: source.data.account.clone(),
        };
        self.create_database(&to.to_string(), database).await?;
        let mut cloned = 0usize;
        for schema in self.list_schemas(from).await? {
            if rw_ms(&schema) > point_ms {
                continue; // created after the point
            }
            let target_schema = SchemaIdent::new(to.to_string(), schema.data.ident.schema.clone());
            let source_schema = schema.data.ident.clone();
            cloned += self
                .clone_schema_as_of(&source_schema, &target_schema, point_ms)
                .await?;
        }
        // Schemas dropped after the point existed live at it — clone their
        // tombstoned records plus qualifying contained tables.
        for (schema, dropped_at) in self.list_dropped_schemas(from).await? {
            if dropped_at <= point_ms {
                continue;
            }
            let target_schema = SchemaIdent::new(to.to_string(), schema.data.ident.schema.clone());
            let target = Schema {
                ident: target_schema.clone(),
                properties: schema.data.properties.clone(),
            };
            self.create_schema(&target_schema, target).await?;
            let source_schema = schema.data.ident.clone();
            for (table, table_dropped_at) in
                self.list_dropped_tables_with_time(&source_schema).await?
            {
                if table_dropped_at > point_ms
                    && self
                        .clone_table_record_as_of(&table, &target_schema, point_ms)
                        .await?
                {
                    cloned += 1;
                }
            }
        }
        Ok(cloned)
    }

    /// Clone one table *record* (live or tombstoned) as-of `point_ms` into
    /// `to_schema` (M17.2.2 helper). Iceberg tables are pinned to the latest
    /// snapshot at or before `point_ms`; tables with no commit before the
    /// point (or non-Iceberg) clone their current record — for a
    /// never-rewritten table that is exactly its as-of state. Returns false
    /// when the table did not exist at the point or the target name is
    /// occupied.
    async fn clone_table_record_as_of(
        &self,
        table: &RwObject<Table>,
        to_schema: &SchemaIdent,
        point_ms: i64,
    ) -> Result<bool>;

    /// Dropped tables with their drop timestamps (M17.2.2), most recent
    /// first.
    async fn list_dropped_tables_with_time(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<(RwObject<Table>, i64)>>;
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>>;

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool>;
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String>;
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>>;
    fn settings_config(&self) -> Option<MetastoreSettingsConfig>;

    /// Streams (design §18, M11). All three automation object families
    /// share the schema namespace with tables — creation rejects a name
    /// occupied by any other object kind.
    async fn create_stream(&self, ident: &TableIdent, stream: Stream) -> Result<RwObject<Stream>>;
    async fn get_stream(&self, ident: &TableIdent) -> Result<Option<RwObject<Stream>>>;
    async fn list_streams(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Stream>>>;
    async fn update_stream(&self, ident: &TableIdent, stream: Stream) -> Result<RwObject<Stream>>;
    async fn delete_stream(&self, ident: &TableIdent) -> Result<()>;

    /// Tasks (design §18, M11).
    async fn create_task(&self, ident: &TableIdent, task: Task) -> Result<RwObject<Task>>;
    async fn get_task(&self, ident: &TableIdent) -> Result<Option<RwObject<Task>>>;
    async fn list_tasks(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Task>>>;
    /// Every task across the metastore (scheduler iteration).
    async fn list_all_tasks(&self) -> Result<Vec<RwObject<Task>>>;
    async fn update_task(&self, ident: &TableIdent, task: Task) -> Result<RwObject<Task>>;
    async fn delete_task(&self, ident: &TableIdent) -> Result<()>;

    /// Sequences (design §22.8, M16.4 deferred item). The record's `current`
    /// field is the reservation cursor; `NEXTVAL` evaluation advances it in
    /// chunks and journals the update like any other mutation.
    async fn create_sequence(
        &self,
        ident: &TableIdent,
        sequence: Sequence,
    ) -> Result<RwObject<Sequence>>;
    async fn get_sequence(&self, ident: &TableIdent) -> Result<Option<RwObject<Sequence>>>;
    async fn list_sequences(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Sequence>>>;
    /// Every sequence across the metastore (SHOW SEQUENCES without a scope).
    async fn list_all_sequences(&self) -> Result<Vec<RwObject<Sequence>>>;
    async fn update_sequence(
        &self,
        ident: &TableIdent,
        sequence: Sequence,
    ) -> Result<RwObject<Sequence>>;
    async fn delete_sequence(&self, ident: &TableIdent) -> Result<()>;

    /// Dynamic-table definition records (design §18, M11). The target table
    /// itself is a regular metastore table.
    async fn create_dynamic_table(
        &self,
        ident: &TableIdent,
        dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>>;
    async fn get_dynamic_table(&self, ident: &TableIdent)
    -> Result<Option<RwObject<DynamicTable>>>;
    async fn list_dynamic_tables(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<RwObject<DynamicTable>>>;
    /// Every dynamic-table record across the metastore (scheduler iteration).
    async fn list_all_dynamic_tables(&self) -> Result<Vec<RwObject<DynamicTable>>>;
    async fn update_dynamic_table(
        &self,
        ident: &TableIdent,
        dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>>;
    async fn delete_dynamic_table(&self, ident: &TableIdent) -> Result<()>;
}

#[derive(Debug, Default)]
struct MetastoreState {
    volumes: HashMap<VolumeIdent, RwObject<Volume>>,
    databases: HashMap<DatabaseIdent, RwObject<Database>>,
    schemas: HashMap<(DatabaseIdent, String), RwObject<Schema>>,
    tables: HashMap<(DatabaseIdent, String, String), RwObject<Table>>,
    /// Tombstones for dropped tables (design §15, M9.3): record + epoch-ms
    /// drop time. Keyed by table key; a re-drop of the same name overwrites
    /// the previous tombstone (UNDROP restores the most recent drop).
    dropped_tables: HashMap<(DatabaseIdent, String, String), (RwObject<Table>, i64)>,
    /// Tombstones for dropped schemas (M17.2.2): record + epoch-ms drop time.
    /// UNDROP SCHEMA restores the most recent drop, together with the tables
    /// the cascade tombstoned at the same instant.
    dropped_schemas: HashMap<(DatabaseIdent, String), (RwObject<Schema>, i64)>,
    /// Tombstones for dropped databases (M17.2.2): record + epoch-ms drop time.
    dropped_databases: HashMap<DatabaseIdent, (RwObject<Database>, i64)>,
    streams: HashMap<(DatabaseIdent, String, String), RwObject<Stream>>,
    tasks: HashMap<(DatabaseIdent, String, String), RwObject<Task>>,
    dynamic_tables: HashMap<(DatabaseIdent, String, String), RwObject<DynamicTable>>,
    sequences: HashMap<(DatabaseIdent, String, String), RwObject<Sequence>>,
}

#[derive(Debug, Default)]
pub struct InMemoryMetastore {
    state: RwLock<MetastoreState>,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
    settings_config: Option<MetastoreSettingsConfig>,
    /// Durable write-through journal (design §23.0, M17.0). `None` keeps the
    /// legacy pure in-memory behavior (tests, non-durable builds).
    journal: Option<Arc<dyn CatalogJournal>>,
    /// Strictly-increasing tombstone clock (ms): wall-clock ms collide for
    /// fast successive drops, and tombstone ordering (container vs contents,
    /// individual vs cascade) relies on strict monotonicity.
    tombstone_clock: std::sync::atomic::AtomicI64,
}

impl InMemoryMetastore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MetastoreState::default()),
            object_store_cache: DashMap::new(),
            settings_config: None,
            journal: None,
            tombstone_clock: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Durable metastore: journals every mutation and replays the journal
    /// into the in-memory state at construction (design §23.0, M17.0).
    ///
    /// # Errors
    /// When the journal cannot be read (bootstrap replay fails).
    pub async fn new_with_journal(
        journal: Arc<dyn CatalogJournal>,
        settings: MetastoreSettingsConfig,
    ) -> Result<Self> {
        let state = Self::replay(&journal).await?;
        Ok(Self {
            state: RwLock::new(state),
            object_store_cache: DashMap::new(),
            settings_config: Some(settings),
            journal: Some(journal),
            tombstone_clock: std::sync::atomic::AtomicI64::new(0),
        })
    }

    /// True when no catalog records were replayed (fresh catalog). Used to
    /// gate the bootstrap config so it only seeds a first boot.
    ///
    /// # Errors
    /// When the replayed state cannot be read.
    pub async fn is_empty(&self) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.volumes.is_empty()
            && state.databases.is_empty()
            && state.schemas.is_empty()
            && state.tables.is_empty()
            && state.dropped_tables.is_empty()
            && state.dropped_schemas.is_empty()
            && state.dropped_databases.is_empty()
            && state.streams.is_empty()
            && state.tasks.is_empty()
            && state.dynamic_tables.is_empty())
    }

    /// Bootstrap replay: apply every journaled record into the state.
    /// Dropped-table tombstones apply first so live records win over
    /// stale tombstones (see `journal::domain::REPLAY_ORDER`).
    async fn replay(journal: &Arc<dyn CatalogJournal>) -> Result<MetastoreState> {
        let mut state = MetastoreState::default();
        for domain in journal::domain::REPLAY_ORDER {
            let entries = journal.list(domain).await.map_err(|e| {
                metastore_error::JournalSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;
            for (_, value) in entries {
                let record = CatalogRecord::from_bytes(domain, &value).map_err(|e| {
                    // One corrupt record must not brick the whole catalog:
                    // log and skip (matches the metering ledger's tolerant
                    // load).
                    tracing::error!(domain, error = %e, "skipping corrupt catalog record");
                    metastore_error::JournalSnafu {
                        message: e.to_string(),
                    }
                    .build()
                })?;
                Self::apply_record(&mut state, record);
            }
        }
        Ok(state)
    }

    /// Apply one replayed record to the state (key recomputed from the
    /// record's ident — see `journal::CatalogRecord::journal_key`).
    fn apply_record(state: &mut MetastoreState, record: CatalogRecord) {
        match record {
            CatalogRecord::Volume(row) => {
                state
                    .volumes
                    .insert(row.data.ident.to_ascii_lowercase(), row);
            }
            CatalogRecord::Database(row) => {
                state
                    .databases
                    .insert(row.data.ident.to_ascii_lowercase(), row);
            }
            CatalogRecord::Schema(row) => {
                state.schemas.insert(Self::schema_key(&row.data.ident), row);
            }
            CatalogRecord::Table(row) => {
                state.tables.insert(Self::table_key(&row.data.ident), row);
            }
            CatalogRecord::DroppedTable(row, dropped_at_ms) => {
                state
                    .dropped_tables
                    .insert(Self::table_key(&row.data.ident), (row, dropped_at_ms));
            }
            CatalogRecord::DroppedSchema(row, dropped_at_ms) => {
                state
                    .dropped_schemas
                    .insert(Self::schema_key(&row.data.ident), (row, dropped_at_ms));
            }
            CatalogRecord::DroppedDatabase(row, dropped_at_ms) => {
                state
                    .dropped_databases
                    .insert(row.data.ident.to_ascii_lowercase(), (row, dropped_at_ms));
            }
            CatalogRecord::Stream(row) => {
                state.streams.insert(Self::table_key(&row.data.ident), row);
            }
            CatalogRecord::Task(row) => {
                state.tasks.insert(Self::table_key(&row.data.ident), row);
            }
            CatalogRecord::DynamicTable(row) => {
                state
                    .dynamic_tables
                    .insert(Self::table_key(&row.data.ident), row);
            }
            CatalogRecord::Sequence(row) => {
                state
                    .sequences
                    .insert(Self::table_key(&row.data.ident), row);
            }
        }
    }

    /// Write-through: serialize + persist a record before the in-memory
    /// mutation. With no journal this is a no-op.
    async fn journal_put(&self, record: &CatalogRecord) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal::put_record(journal, record).await.map_err(|e| {
                metastore_error::JournalSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;
        }
        Ok(())
    }

    /// Write-through: remove a record from the journal.
    async fn journal_delete(&self, domain: &str, key: &str) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.delete(domain, key).await.map_err(|e| {
                metastore_error::JournalSnafu {
                    message: format!("{domain}/{key}: {e}"),
                }
                .build()
            })?;
        }
        Ok(())
    }

    /// Journal key strings for state-map keys (cascades remove whole ranges
    /// of state keys; these derive the journal key form directly).
    fn database_journal_key(db: &str) -> String {
        db.to_ascii_lowercase()
    }

    fn schema_journal_key(db: &str, schema: &str) -> String {
        format!(
            "{}{}{}",
            db.to_ascii_lowercase(),
            journal::key_sep(),
            schema.to_ascii_lowercase()
        )
    }

    fn table_journal_key(db: &str, schema: &str, table: &str) -> String {
        format!(
            "{}{}{}{}{}",
            db.to_ascii_lowercase(),
            journal::key_sep(),
            schema.to_ascii_lowercase(),
            journal::key_sep(),
            table.to_ascii_lowercase()
        )
    }

    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn with_settings_config(self, settings: MetastoreSettingsConfig) -> Self {
        Self {
            settings_config: Some(settings),
            ..self
        }
    }

    #[instrument(name = "Metastore::metadata_file_name", level = "trace", ret)]
    fn metadata_file_name() -> String {
        format!("{}.metadata.json", Uuid::new_v4())
    }

    fn now_ms() -> i64 {
        Utc::now().timestamp_millis()
    }

    /// Strictly-increasing tombstone timestamp (see `tombstone_clock`).
    /// Wall-clock ms collide for back-to-back drops; the compare-exchange
    /// loop advances past any value already handed out.
    fn tombstone_ms(&self) -> i64 {
        use std::sync::atomic::Ordering;
        let now = Self::now_ms();
        let mut last = self.tombstone_clock.load(Ordering::Relaxed);
        loop {
            let target = now.max(last + 1);
            match self.tombstone_clock.compare_exchange(
                last,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return target,
                Err(seen) => last = seen,
            }
        }
    }

    fn default_table_location(ident: &TableIdent) -> String {
        format!("{}/{}/{}", ident.database, ident.schema, ident.table)
    }

    #[must_use]
    pub fn table_key(ident: &TableIdent) -> (DatabaseIdent, String, String) {
        (
            ident.database.to_ascii_lowercase(),
            ident.schema.to_ascii_lowercase(),
            ident.table.to_ascii_lowercase(),
        )
    }

    fn schema_key(ident: &SchemaIdent) -> (DatabaseIdent, String) {
        (
            ident.database.to_ascii_lowercase(),
            ident.schema.to_ascii_lowercase(),
        )
    }

    fn database_key(ident: &TableIdent) -> DatabaseIdent {
        ident.database.to_ascii_lowercase()
    }

    #[instrument(name = "Metastore::ensure_database", level = "trace", ret)]
    fn ensure_database(state: &MetastoreState, name: &DatabaseIdent) -> Result<RwObject<Database>> {
        state
            .databases
            .get(name)
            .cloned()
            .ok_or_else(|| metastore_error::DatabaseNotFoundSnafu { db: name.clone() }.build())
    }

    #[instrument(name = "Metastore::ensure_volume", level = "trace", ret)]
    fn ensure_volume(state: &MetastoreState, name: &VolumeIdent) -> Result<RwObject<Volume>> {
        state.volumes.get(name).cloned().ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: name.clone(),
            }
            .build()
        })
    }

    #[instrument(name = "Metastore::put_metadata", level = "trace", ret)]
    async fn put_metadata(
        &self,
        table: &TableIdent,
        object_store: Arc<dyn ObjectStore>,
        metadata: &iceberg_rust_spec::table_metadata::TableMetadata,
    ) -> Result<String> {
        let name = Self::metadata_file_name();
        let location = Self::default_table_location(table);
        let path = format!("{location}/metadata/{name}");
        let bytes =
            serde_json::to_vec(metadata).context(metastore_error::SerializeMetadataSnafu)?;
        object_store
            .put(
                &Path::from(path.clone()),
                PutPayload::from_bytes(Bytes::from(bytes)),
            )
            .await
            .context(metastore_error::ObjectStoreSnafu)?;
        Ok(path)
    }
}

/// `RwObject.created_at` as epoch milliseconds (as-of container clones).
fn rw_ms<T: Eq + PartialEq>(o: &RwObject<T>) -> i64 {
    o.created_at.and_utc().timestamp_millis()
}

#[async_trait]
impl Metastore for InMemoryMetastore {
    #[instrument(name = "Metastore::list_volumes", level = "trace", ret)]
    async fn list_volumes(&self) -> Result<Vec<RwObject<Volume>>> {
        let state = self.state.read().await;
        Ok(state.volumes.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::create_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let object_store = volume.get_object_store()?;
        let mut state = self.state.write().await;
        if state.volumes.contains_key(name) {
            return metastore_error::VolumeAlreadyExistsSnafu {
                volume: name.clone(),
            }
            .fail();
        }
        let row = RwObject::new(volume);
        self.journal_put(&CatalogRecord::Volume(row.clone()))
            .await?;
        state.volumes.insert(name.clone(), row.clone());
        self.object_store_cache.insert(name.clone(), object_store);
        Ok(row)
    }

    #[instrument(name = "Metastore::get_volume", level = "trace", skip(self), err)]
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        let state = self.state.read().await;
        Ok(state.volumes.get(name).cloned())
    }

    #[instrument(
        name = "Metastore::update_volume",
        level = "debug",
        skip(self, volume),
        err
    )]
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        let object_store = volume.get_object_store()?;
        let mut state = self.state.write().await;
        let entry = state.volumes.get_mut(name).ok_or_else(|| {
            metastore_error::VolumeNotFoundSnafu {
                volume: name.clone(),
            }
            .build()
        })?;
        entry.update(volume);
        self.journal_put(&CatalogRecord::Volume(entry.clone()))
            .await?;
        self.object_store_cache
            .insert(name.clone(), object_store.clone());
        Ok(entry.clone())
    }

    #[instrument(name = "Metastore::delete_volume", level = "debug", skip(self), err)]
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let in_use = state
            .databases
            .values()
            .filter(|db| &db.volume == name)
            .map(|db| db.ident.clone())
            .collect::<Vec<_>>();
        if !in_use.is_empty() && !cascade {
            return metastore_error::VolumeInUseSnafu {
                database: in_use.join(", "),
            }
            .fail();
        }
        if cascade {
            let schema_keys: Vec<_> = state
                .schemas
                .keys()
                .filter(|(db, _)| in_use.iter().any(|d| d == db))
                .cloned()
                .collect();
            for key in schema_keys {
                state.schemas.remove(&key);
                self.journal_delete(
                    journal::domain::SCHEMAS,
                    &Self::schema_journal_key(&key.0, &key.1),
                )
                .await?;
            }
            let table_keys: Vec<_> = state
                .tables
                .keys()
                .filter(|(db, _, _)| in_use.iter().any(|d| d == db))
                .cloned()
                .collect();
            for key in table_keys {
                if let Some(table) = state.tables.remove(&key) {
                    let dropped_at = self.tombstone_ms();
                    self.journal_put(&CatalogRecord::DroppedTable(table.clone(), dropped_at))
                        .await?;
                    self.journal_delete(
                        journal::domain::TABLES,
                        &Self::table_journal_key(&key.0, &key.1, &key.2),
                    )
                    .await?;
                    state.dropped_tables.insert(key, (table, dropped_at));
                }
            }
            for db in in_use {
                state.databases.remove(&db);
                self.journal_delete(journal::domain::DATABASES, &Self::database_journal_key(&db))
                    .await?;
            }
        } else if !in_use.is_empty() {
            return metastore_error::VolumeInUseSnafu {
                database: in_use.join(", "),
            }
            .fail();
        }
        if state.volumes.remove(name).is_some() {
            self.object_store_cache.remove(name);
        }
        self.journal_delete(journal::domain::VOLUMES, &Self::database_journal_key(name))
            .await?;
        Ok(())
    }

    #[instrument(name = "Metastore::volume_object_store", level = "trace", ret)]
    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(store) = self.object_store_cache.get(name) {
            return Ok(Some(store.clone()));
        }
        let volume = self.get_volume(name).await?;
        if let Some(volume) = volume {
            let object_store = volume.get_object_store()?;
            self.object_store_cache
                .insert(name.clone(), object_store.clone());
            Ok(Some(object_store))
        } else {
            Ok(None)
        }
    }

    #[instrument(name = "Metastore::list_databases", level = "trace", ret)]
    async fn list_databases(&self) -> Result<Vec<RwObject<Database>>> {
        let state = self.state.read().await;
        Ok(state.databases.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::create_database",
        level = "debug",
        skip(self, database),
        err
    )]
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let mut state = self.state.write().await;
        Self::ensure_volume(&state, &database.volume)?;
        if state.databases.contains_key(name) {
            return metastore_error::DatabaseAlreadyExistsSnafu { db: name }.fail();
        }
        let row = RwObject::new(database);
        self.journal_put(&CatalogRecord::Database(row.clone()))
            .await?;
        state.databases.insert(name.clone(), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_database", level = "trace", ret)]
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let state = self.state.read().await;
        Ok(state.databases.get(name).cloned())
    }

    #[instrument(
        name = "Metastore::update_database",
        level = "debug",
        skip(self, database),
        err
    )]
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let mut state = self.state.write().await;
        let entry = state
            .databases
            .get_mut(name)
            .ok_or_else(|| metastore_error::DatabaseNotFoundSnafu { db: name.clone() }.build())?;
        entry.update(database);
        self.journal_put(&CatalogRecord::Database(entry.clone()))
            .await?;
        Ok(entry.clone())
    }

    #[instrument(name = "Metastore::delete_database", level = "debug", skip(self), err)]
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let schema_keys: Vec<_> = state
            .schemas
            .keys()
            .filter(|(db, _)| db == name)
            .cloned()
            .collect();
        // Auxiliary schema-scoped objects (streams, tasks, dynamic tables,
        // sequences) also block a non-cascade drop.
        let aux_in_use = state.streams.keys().any(|(db, _, _)| db == name)
            || state.tasks.keys().any(|(db, _, _)| db == name)
            || state.dynamic_tables.keys().any(|(db, _, _)| db == name)
            || state.sequences.keys().any(|(db, _, _)| db == name);
        if (!schema_keys.is_empty() || aux_in_use) && !cascade {
            return metastore_error::DatabaseInUseSnafu {
                database: name,
                schema: schema_keys
                    .iter()
                    .map(|(_, schema)| schema.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            }
            .fail();
        }
        // One cascade event = one tombstone timestamp: contained schemas and
        // tables share the database's drop time (UNDROP DATABASE restores
        // exactly what this statement tombstoned).
        let dropped_at = self.tombstone_ms();
        for key in &schema_keys {
            if let Some(schema) = state.schemas.remove(key) {
                self.journal_put(&CatalogRecord::DroppedSchema(schema.clone(), dropped_at))
                    .await?;
                state
                    .dropped_schemas
                    .insert(key.clone(), (schema, dropped_at));
            }
            self.journal_delete(
                journal::domain::SCHEMAS,
                &Self::schema_journal_key(&key.0, &key.1),
            )
            .await?;
        }
        let table_keys: Vec<_> = state
            .tables
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in &table_keys {
            if let Some(table) = state.tables.remove(key) {
                self.journal_put(&CatalogRecord::DroppedTable(table.clone(), dropped_at))
                    .await?;
                self.journal_delete(
                    journal::domain::TABLES,
                    &Self::table_journal_key(&key.0, &key.1, &key.2),
                )
                .await?;
                state
                    .dropped_tables
                    .insert(key.clone(), (table, dropped_at));
            }
        }
        // Cascade the auxiliary domains too: streams, tasks, dynamic tables
        // and sequences are schema-scoped records that must not survive (or
        // block re-creation in) a dropped database. They are removed from
        // the state maps and the journal but not tombstoned — UNDROP
        // DATABASE restores schemas/tables only (M17.2.2 scope).
        let stream_keys: Vec<_> = state
            .streams
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in stream_keys {
            state.streams.remove(&key);
            self.journal_delete(
                journal::domain::STREAMS,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let task_keys: Vec<_> = state
            .tasks
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in task_keys {
            state.tasks.remove(&key);
            self.journal_delete(
                journal::domain::TASKS,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let dt_keys: Vec<_> = state
            .dynamic_tables
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in dt_keys {
            state.dynamic_tables.remove(&key);
            self.journal_delete(
                journal::domain::DYNAMIC_TABLES,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let seq_keys: Vec<_> = state
            .sequences
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in seq_keys {
            state.sequences.remove(&key);
            self.journal_delete(
                journal::domain::SEQUENCES,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        // M17.2.2: tombstone the database record itself (UNDROP DATABASE).
        if let Some(database) = state.databases.remove(name) {
            self.journal_put(&CatalogRecord::DroppedDatabase(
                database.clone(),
                dropped_at,
            ))
            .await?;
            self.journal_delete(
                journal::domain::DATABASES,
                &Self::database_journal_key(name),
            )
            .await?;
            state
                .dropped_databases
                .insert(name.clone(), (database, dropped_at));
        } else {
            self.journal_delete(
                journal::domain::DATABASES,
                &Self::database_journal_key(name),
            )
            .await?;
        }
        Ok(())
    }

    #[instrument(name = "Metastore::list_schemas", level = "trace", ret)]
    async fn list_schemas(&self, database: &DatabaseIdent) -> Result<Vec<RwObject<Schema>>> {
        let state = self.state.read().await;

        let mut items: Vec<RwObject<Schema>> = state
            .schemas
            .iter()
            .filter(|((db, _), _)| db == database)
            .map(|(_, schema)| schema.clone())
            .collect();

        // Sort by schema name
        items.sort_by(|a, b| b.ident.schema.cmp(&a.ident.schema));

        Ok(items)
    }

    #[instrument(
        name = "Metastore::create_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn create_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let mut state = self.state.write().await;
        if state.schemas.contains_key(&Self::schema_key(ident)) {
            return metastore_error::SchemaAlreadyExistsSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if !state.databases.contains_key(&ident.database) {
            return metastore_error::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .fail();
        }
        let row = RwObject::new(schema);
        self.journal_put(&CatalogRecord::Schema(row.clone()))
            .await?;
        state.schemas.insert(Self::schema_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_schema", level = "trace", ret)]
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        let state = self.state.read().await;
        Ok(state.schemas.get(&Self::schema_key(ident)).cloned())
    }

    #[instrument(
        name = "Metastore::update_schema",
        level = "debug",
        skip(self, schema),
        err
    )]
    async fn update_schema(&self, ident: &SchemaIdent, schema: Schema) -> Result<RwObject<Schema>> {
        let mut state = self.state.write().await;
        let entry = state
            .schemas
            .get_mut(&Self::schema_key(ident))
            .ok_or_else(|| {
                metastore_error::SchemaNotFoundSnafu {
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        entry.update(schema);
        self.journal_put(&CatalogRecord::Schema(entry.clone()))
            .await?;
        Ok(entry.clone())
    }

    #[instrument(name = "Metastore::delete_schema", level = "debug", skip(self), err)]
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        let tables: Vec<_> = state
            .tables
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        // Auxiliary schema-scoped objects also block a non-cascade drop.
        let aux_in_use = state
            .streams
            .keys()
            .chain(state.tasks.keys())
            .chain(state.dynamic_tables.keys())
            .chain(state.sequences.keys())
            .any(|(db, schema, _)| db == &ident.database && schema == &ident.schema);
        if (!tables.is_empty() || aux_in_use) && !cascade {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        // One cascade event = one tombstone timestamp (shared with the
        // schema tombstone below), so UNDROP SCHEMA restores exactly the
        // tables this statement tombstoned.
        let dropped_at = self.tombstone_ms();
        for key in tables {
            if let Some(table) = state.tables.remove(&key) {
                self.journal_put(&CatalogRecord::DroppedTable(table.clone(), dropped_at))
                    .await?;
                self.journal_delete(
                    journal::domain::TABLES,
                    &Self::table_journal_key(&key.0, &key.1, &key.2),
                )
                .await?;
                state.dropped_tables.insert(key, (table, dropped_at));
            }
        }
        // Cascade the auxiliary domains of this schema (streams, tasks,
        // dynamic tables, sequences): removed from state + journal, not
        // tombstoned (UNDROP SCHEMA restores tables only, M17.2.2 scope).
        let stream_keys: Vec<_> = state
            .streams
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        for key in stream_keys {
            state.streams.remove(&key);
            self.journal_delete(
                journal::domain::STREAMS,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let task_keys: Vec<_> = state
            .tasks
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        for key in task_keys {
            state.tasks.remove(&key);
            self.journal_delete(
                journal::domain::TASKS,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let dt_keys: Vec<_> = state
            .dynamic_tables
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        for key in dt_keys {
            state.dynamic_tables.remove(&key);
            self.journal_delete(
                journal::domain::DYNAMIC_TABLES,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        let seq_keys: Vec<_> = state
            .sequences
            .keys()
            .filter(|(db, schema, _)| db == &ident.database && schema == &ident.schema)
            .cloned()
            .collect();
        for key in seq_keys {
            state.sequences.remove(&key);
            self.journal_delete(
                journal::domain::SEQUENCES,
                &Self::table_journal_key(&key.0, &key.1, &key.2),
            )
            .await?;
        }
        // M17.2.2: tombstone the schema record itself (UNDROP SCHEMA, as-of
        // container clones) before removing it.
        if let Some(schema) = state.schemas.remove(&Self::schema_key(ident)) {
            self.journal_put(&CatalogRecord::DroppedSchema(schema.clone(), dropped_at))
                .await?;
            self.journal_delete(
                journal::domain::SCHEMAS,
                &Self::schema_journal_key(&ident.database, &ident.schema),
            )
            .await?;
            state
                .dropped_schemas
                .insert(Self::schema_key(ident), (schema, dropped_at));
        } else {
            self.journal_delete(
                journal::domain::SCHEMAS,
                &Self::schema_journal_key(&ident.database, &ident.schema),
            )
            .await?;
        }
        Ok(())
    }

    #[instrument(name = "Metastore::list_tables", level = "trace", ret)]
    async fn list_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        let state = self.state.read().await;
        Ok(state
            .tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, table)| table.clone())
            .collect())
    }

    #[instrument(
        name = "Metastore::create_table",
        level = "debug",
        skip(self, table),
        err
    )]
    #[allow(clippy::too_many_lines)]
    async fn create_table(
        &self,
        ident: &TableIdent,
        mut table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        let mut state = self.state.write().await;
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }

        if state.tables.contains_key(&Self::table_key(ident)) {
            return metastore_error::TableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }

        // Schema objects share one namespace (design §18.1-6): reject a
        // name occupied by a stream, task, or dynamic-table record.
        Self::assert_object_name_free(&state, ident)?;

        if table.volume_ident.is_none() {
            let database = Self::ensure_database(&state, &ident.database)?;
            table.volume_ident = Some(database.volume.clone());
        }

        if table.location.is_none() {
            table.location = Some(Self::default_table_location(ident));
        }

        let schema_id = *table.schema.schema_id();
        let mut schemas = HashMap::new();
        schemas.insert(schema_id, table.schema.clone());

        let mut metadata_builder = TableMetadataBuilder::default();
        metadata_builder
            .schemas(schemas)
            .current_schema_id(schema_id)
            .location(table.location.clone().unwrap_or_default())
            .last_updated_ms(Utc::now().timestamp_millis())
            .format_version(FormatVersion::V2)
            .last_column_id(max_field_id(&table.schema));

        if let Some(spec) = table.partition_spec.take() {
            let spec_id = *spec.spec_id();
            let max_partition_id = spec
                .fields()
                .iter()
                .map(|field| *field.field_id())
                .max()
                .unwrap_or(0);
            let mut partition_specs = HashMap::new();
            partition_specs.insert(spec_id, spec);
            metadata_builder
                .partition_specs(partition_specs)
                .default_spec_id(spec_id)
                .last_partition_id(max_partition_id);
        }
        if let Some(order) = table.sort_order.take() {
            let order_id = order.order_id;
            let mut sort_orders = HashMap::new();
            sort_orders.insert(order_id, order);
            metadata_builder
                .sort_orders(sort_orders)
                .default_sort_order_id(order_id);
        }

        let mut metadata = metadata_builder
            .build()
            .context(metastore_error::TableMetadataBuilderSnafu)?;

        if metadata.properties.is_empty() {
            metadata.properties = HashMap::new();
        }

        let object_store = self
            .table_object_store_from_request(&state, &table, ident)
            .await?;
        let metadata_location = self
            .put_metadata(ident, object_store.clone(), &metadata)
            .await?;

        let mut properties = table.properties.take().unwrap_or_default();
        Self::update_properties_timestamps(&mut properties);

        let stored_table = Table {
            ident: ident.clone(),
            metadata,
            metadata_location,
            properties,
            volume_ident: table.volume_ident.clone(),
            volume_location: table.location,
            is_temporary: table.is_temporary.unwrap_or(false),
            format: table.format.unwrap_or(TableFormat::Iceberg),
        };

        let row = RwObject::new(stored_table);
        self.journal_put(&CatalogRecord::Table(row.clone())).await?;
        state.tables.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(
        name = "Metastore::register_table",
        level = "debug",
        skip(self, table),
        err
    )]
    async fn register_table(&self, ident: &TableIdent, table: Table) -> Result<RwObject<Table>> {
        let mut state = self.state.write().await;
        let row = RwObject::new(table);
        self.journal_put(&CatalogRecord::Table(row.clone())).await?;
        state.tables.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    /// Zero-copy clone (M8, design §14). Metadata-only: the clone shares the
    /// source's snapshots/manifests (absolute paths) and volume, but gets its
    /// own location and a fresh metadata JSON, so subsequent commits to
    /// either table are independent.
    #[instrument(name = "Metastore::clone_table", level = "debug", skip(self), err)]
    async fn clone_table(&self, from: &TableIdent, to: &TableIdent) -> Result<RwObject<Table>> {
        let source = self.get_table(from).await?.ok_or_else(|| {
            metastore_error::TableNotFoundSnafu {
                table: from.table.clone(),
                schema: from.schema.clone(),
                db: from.database.clone(),
            }
            .build()
        })?;

        let mut state = self.state.write().await;

        if !state
            .schemas
            .contains_key(&Self::schema_key(&SchemaIdent::new(
                to.database.clone(),
                to.schema.clone(),
            )))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: to.schema.clone(),
                db: to.database.clone(),
            }
            .fail();
        }

        if state.tables.contains_key(&Self::table_key(to)) {
            return metastore_error::TableAlreadyExistsSnafu {
                table: to.table.clone(),
                schema: to.schema.clone(),
                db: to.database.clone(),
            }
            .fail();
        }

        // v1 (design §14): the clone shares the source's table location and
        // its current metadata lineage — zero data/metadata copy. All Iceberg
        // paths inside the metadata are relative to the shared volume root,
        // so the clone reads the source's manifests as-is. Commits to either
        // table diverge at the record level (each writes its own
        // uuid-named metadata JSON and random-id snapshots into the shared
        // location), so writes are independent after the clone. A
        // branch-per-clone ref model is a v2 refinement.
        let clone = Table {
            ident: to.clone(),
            ..source.data.clone()
        };
        let row = RwObject::new(clone);
        self.journal_put(&CatalogRecord::Table(row.clone())).await?;
        state.tables.insert(Self::table_key(to), row.clone());
        Ok(row)
    }

    /// Zero-copy as-of clone (M9.4, design §15). See the trait doc.
    #[instrument(
        name = "Metastore::clone_table_as_of",
        level = "debug",
        skip(self),
        err
    )]
    async fn clone_table_as_of(
        &self,
        from: &TableIdent,
        to: &TableIdent,
        snapshot_id: i64,
        as_of_ms: i64,
    ) -> Result<RwObject<Table>> {
        let source = self.get_table(from).await?.ok_or_else(|| {
            metastore_error::TableNotFoundSnafu {
                table: from.table.clone(),
                schema: from.schema.clone(),
                db: from.database.clone(),
            }
            .build()
        })?;
        if source.data.format != TableFormat::Iceberg {
            return metastore_error::UnsupportedSnafu {
                message: format!(
                    "time-travel clone requires an Iceberg table, '{}' is {:?}",
                    from.table, source.data.format
                ),
            }
            .fail();
        }
        let snapshot = source
            .data
            .metadata
            .snapshots
            .get(&snapshot_id)
            .ok_or_else(|| {
                metastore_error::SnapshotNotFoundSnafu {
                    snapshot: snapshot_id,
                    table: from.table.clone(),
                    schema: from.schema.clone(),
                    db: from.database.clone(),
                }
                .build()
            })?;

        {
            let state = self.state.write().await;
            if !state
                .schemas
                .contains_key(&Self::schema_key(&SchemaIdent::new(
                    to.database.clone(),
                    to.schema.clone(),
                )))
            {
                return metastore_error::SchemaNotFoundSnafu {
                    schema: to.schema.clone(),
                    db: to.database.clone(),
                }
                .fail();
            }
            if state.tables.contains_key(&Self::table_key(to)) {
                return metastore_error::TableAlreadyExistsSnafu {
                    table: to.table.clone(),
                    schema: to.schema.clone(),
                    db: to.database.clone(),
                }
                .fail();
            }
        }

        // Pinned metadata: same schema/location lineage as the source (all
        // paths are relative to the shared volume root), but a snapshot
        // history that starts at the as-of point. The fresh uuid +
        // metadata JSON under the clone's own prefix make commits to either
        // table independent, mirroring the M8 commit behavior.
        let metadata = TableMetadataBuilder::default()
            .format_version(source.data.metadata.format_version)
            .location(source.data.metadata.location.clone())
            .last_updated_ms(as_of_ms)
            .last_column_id(source.data.metadata.last_column_id)
            .schemas(source.data.metadata.schemas.clone())
            .current_schema_id(source.data.metadata.current_schema_id)
            .partition_specs(source.data.metadata.partition_specs.clone())
            .default_spec_id(source.data.metadata.default_spec_id)
            .last_partition_id(source.data.metadata.last_partition_id)
            .properties(source.data.metadata.properties.clone())
            .current_snapshot_id(Some(snapshot_id))
            .snapshots(HashMap::from_iter([(snapshot_id, snapshot.clone())]))
            .snapshot_log(vec![SnapshotLog {
                snapshot_id,
                timestamp_ms: as_of_ms,
            }])
            .refs(HashMap::from_iter([(
                "main".to_string(),
                SnapshotReference {
                    snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            )]))
            .build()
            .context(metastore_error::TableMetadataBuilderSnafu)?;

        let object_store = self.table_object_store(to).await?.ok_or_else(|| {
            metastore_error::TableObjectStoreNotFoundSnafu {
                table: to.table.clone(),
                schema: to.schema.clone(),
                db: to.database.clone(),
            }
            .build()
        })?;
        let metadata_location = self.put_metadata(to, object_store, &metadata).await?;

        let clone = Table {
            ident: to.clone(),
            metadata,
            metadata_location,
            properties: source.data.properties.clone(),
            volume_ident: source.data.volume_ident.clone(),
            volume_location: source.data.volume_location.clone(),
            is_temporary: source.data.is_temporary,
            format: source.data.format,
        };
        let row = RwObject::new(clone);
        self.journal_put(&CatalogRecord::Table(row.clone())).await?;
        self.state
            .write()
            .await
            .tables
            .insert(Self::table_key(to), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_table", level = "trace", ret)]
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        let state = self.state.read().await;
        Ok(state.tables.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(
        name = "Metastore::update_table",
        level = "debug",
        skip(self, update),
        err
    )]
    async fn update_table(
        &self,
        ident: &TableIdent,
        mut update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        let object_store = self.table_object_store(ident).await?.ok_or_else(|| {
            metastore_error::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build()
        })?;

        let mut state = self.state.write().await;
        let table_entry = state
            .tables
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::TableNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        update
            .requirements
            .into_iter()
            .map(TableRequirementExt::new)
            .try_for_each(|req| req.assert(&table_entry.metadata))?;

        convert_add_schema_update_to_lowercase(&mut update.updates)?;

        let mut metadata = table_entry.metadata.clone();
        apply_table_updates(&mut metadata, update.updates.clone())
            .context(metastore_error::IcebergSnafu)?;

        let metadata_location = self.put_metadata(ident, object_store, &metadata).await?;
        table_entry.data.metadata = metadata;
        table_entry.data.metadata_location = metadata_location;
        table_entry.touch();
        // Write-through is essential here: the table record carries the
        // metadata_location pointer, so a commit that is not journaled would
        // make the table's snapshot advance invisible after a restart.
        self.journal_put(&CatalogRecord::Table(table_entry.clone()))
            .await?;
        Ok(table_entry.clone())
    }

    #[instrument(name = "Metastore::delete_table", level = "debug", skip(self), err)]
    async fn delete_table(&self, ident: &TableIdent, _cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(table) = state.tables.remove(&Self::table_key(ident)) {
            let dropped_at = self.tombstone_ms();
            self.journal_put(&CatalogRecord::DroppedTable(table.clone(), dropped_at))
                .await?;
            self.journal_delete(
                journal::domain::TABLES,
                &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
            )
            .await?;
            state
                .dropped_tables
                .insert(Self::table_key(ident), (table, dropped_at));
        }
        Ok(())
    }

    #[instrument(name = "Metastore::undrop_table", level = "debug", skip(self), err)]
    async fn undrop_table(&self, ident: &TableIdent) -> Result<RwObject<Table>> {
        let mut state = self.state.write().await;
        let key = Self::table_key(ident);
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            // Parent schema gone (dropped): UNDROP TABLE would create an
            // orphan — Snowflake errors; UNDROP SCHEMA restores the pair.
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.tables.contains_key(&key) {
            return metastore_error::TableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        let Some((table, _)) = state.dropped_tables.remove(&key) else {
            return metastore_error::TableNotDroppedSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        };
        self.journal_put(&CatalogRecord::Table(table.clone()))
            .await?;
        self.journal_delete(
            journal::domain::DROPPED_TABLES,
            &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
        )
        .await?;
        state.tables.insert(key, table.clone());
        Ok(table)
    }

    #[instrument(
        name = "Metastore::list_dropped_tables",
        level = "trace",
        skip(self),
        err
    )]
    async fn list_dropped_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        let state = self.state.read().await;
        let mut items: Vec<(RwObject<Table>, i64)> = state
            .dropped_tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, (table, dropped_at))| (table.clone(), *dropped_at))
            .collect();
        items.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(items.into_iter().map(|(table, _)| table).collect())
    }

    #[instrument(
        name = "Metastore::list_dropped_tables_with_time",
        level = "trace",
        ret
    )]
    async fn list_dropped_tables_with_time(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<(RwObject<Table>, i64)>> {
        let state = self.state.read().await;
        let mut items: Vec<(RwObject<Table>, i64)> = state
            .dropped_tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, (table, dropped_at))| (table.clone(), *dropped_at))
            .collect();
        items.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(items)
    }

    #[instrument(name = "Metastore::list_dropped_schemas", level = "trace", ret)]
    async fn list_dropped_schemas(
        &self,
        database: &DatabaseIdent,
    ) -> Result<Vec<(RwObject<Schema>, i64)>> {
        let state = self.state.read().await;
        let mut items: Vec<(RwObject<Schema>, i64)> = state
            .dropped_schemas
            .iter()
            .filter(|((db, _), _)| db == database)
            .map(|(_, (schema, dropped_at))| (schema.clone(), *dropped_at))
            .collect();
        items.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(items)
    }

    #[instrument(name = "Metastore::list_dropped_databases", level = "trace", ret)]
    async fn list_dropped_databases(&self) -> Result<Vec<(RwObject<Database>, i64)>> {
        let state = self.state.read().await;
        let mut items: Vec<(RwObject<Database>, i64)> = state
            .dropped_databases
            .values()
            .map(|(database, dropped_at)| (database.clone(), *dropped_at))
            .collect();
        items.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
        Ok(items)
    }

    #[instrument(name = "Metastore::undrop_schema", level = "debug", skip(self), err)]
    async fn undrop_schema(&self, ident: &SchemaIdent) -> Result<RwObject<Schema>> {
        let mut state = self.state.write().await;
        let key = Self::schema_key(ident);
        if state.schemas.contains_key(&key) {
            return metastore_error::SchemaAlreadyExistsSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if !state.databases.contains_key(&ident.database) {
            return metastore_error::DatabaseNotFoundSnafu {
                db: ident.database.clone(),
            }
            .fail();
        }
        let Some((schema, dropped_at)) = state.dropped_schemas.remove(&key) else {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        };
        // The cascade drop tombstoned every contained table at the same
        // moment — restore those tombstones with the schema (tables dropped
        // individually before the schema drop stay tombstoned).
        let cascade_tables: Vec<_> = state
            .dropped_tables
            .iter()
            .filter(|((db, sch, _), (_, table_dropped_at))| {
                db == &ident.database && sch == &ident.schema && *table_dropped_at >= dropped_at
            })
            .map(|(key, _)| key.clone())
            .collect();
        for table_key in cascade_tables {
            if let Some((table, _)) = state.dropped_tables.remove(&table_key) {
                self.journal_put(&CatalogRecord::Table(table.clone()))
                    .await?;
                self.journal_delete(
                    journal::domain::DROPPED_TABLES,
                    &Self::table_journal_key(&table_key.0, &table_key.1, &table_key.2),
                )
                .await?;
                state.tables.insert(table_key, table.clone());
            }
        }
        self.journal_put(&CatalogRecord::Schema(schema.clone()))
            .await?;
        self.journal_delete(
            journal::domain::DROPPED_SCHEMAS,
            &Self::schema_journal_key(&ident.database, &ident.schema),
        )
        .await?;
        state.schemas.insert(key, schema.clone());
        Ok(schema)
    }

    #[instrument(name = "Metastore::undrop_database", level = "debug", skip(self), err)]
    async fn undrop_database(&self, ident: &DatabaseIdent) -> Result<RwObject<Database>> {
        let mut state = self.state.write().await;
        if state.databases.contains_key(ident) {
            return metastore_error::DatabaseAlreadyExistsSnafu { db: ident.clone() }.fail();
        }
        let Some((database, dropped_at)) = state.dropped_databases.remove(ident) else {
            return metastore_error::DatabaseNotFoundSnafu { db: ident.clone() }.fail();
        };
        // Restore the schema tombstones the cascade created (schemas dropped
        // individually before the database drop stay tombstoned), then the
        // tables tombstoned at/after the database drop whose parent schema
        // is live.
        let cascade_schemas: Vec<_> = state
            .dropped_schemas
            .iter()
            .filter(|((db, _), (_, schema_dropped_at))| {
                db == ident && *schema_dropped_at >= dropped_at
            })
            .map(|(key, _)| key.clone())
            .collect();
        for schema_key in cascade_schemas {
            if let Some((schema, _)) = state.dropped_schemas.remove(&schema_key) {
                self.journal_put(&CatalogRecord::Schema(schema.clone()))
                    .await?;
                self.journal_delete(
                    journal::domain::DROPPED_SCHEMAS,
                    &Self::schema_journal_key(&schema_key.0, &schema_key.1),
                )
                .await?;
                state.schemas.insert(schema_key.clone(), schema);
            }
        }
        let cascade_tables: Vec<_> = state
            .dropped_tables
            .iter()
            .filter(|((db, sch, _), (_, table_dropped_at))| {
                db == ident
                    && *table_dropped_at >= dropped_at
                    && state.schemas.contains_key(&(db.clone(), sch.clone()))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for table_key in cascade_tables {
            if let Some((table, _)) = state.dropped_tables.remove(&table_key) {
                self.journal_put(&CatalogRecord::Table(table.clone()))
                    .await?;
                self.journal_delete(
                    journal::domain::DROPPED_TABLES,
                    &Self::table_journal_key(&table_key.0, &table_key.1, &table_key.2),
                )
                .await?;
                state.tables.insert(table_key, table.clone());
            }
        }
        self.journal_put(&CatalogRecord::Database(database.clone()))
            .await?;
        self.journal_delete(
            journal::domain::DROPPED_DATABASES,
            &Self::database_journal_key(ident),
        )
        .await?;
        state.databases.insert(ident.clone(), database.clone());
        Ok(database)
    }

    #[instrument(
        name = "Metastore::clone_table_record_as_of",
        level = "debug",
        skip(self),
        err
    )]
    async fn clone_table_record_as_of(
        &self,
        table: &RwObject<Table>,
        to_schema: &SchemaIdent,
        point_ms: i64,
    ) -> Result<bool> {
        let to = TableIdent::new(
            &to_schema.database,
            &to_schema.schema,
            &table.data.ident.table,
        );
        {
            let state = self.state.read().await;
            if !state.schemas.contains_key(&Self::schema_key(to_schema)) {
                return metastore_error::SchemaNotFoundSnafu {
                    schema: to_schema.schema.clone(),
                    db: to_schema.database.clone(),
                }
                .fail();
            }
            if state.tables.contains_key(&Self::table_key(&to)) {
                // Target name occupied — the live table was already cloned
                // from the live loop (recreated after the drop).
                return Ok(false);
            }
        }
        if rw_ms(table) > point_ms {
            return Ok(false); // created after the point
        }

        // As-of pinning: latest Iceberg snapshot at or before the point.
        // Snowflake parity: a table that existed before the point but whose
        // commits all land after it clones as an EMPTY table (schema only,
        // no data). A never-committed (or non-Iceberg) table clones its
        // record as-is — that is exactly its as-of state.
        if table.data.format == TableFormat::Iceberg
            && (!table.data.metadata.snapshot_log.is_empty()
                || table.data.metadata.current_snapshot_id.is_some())
        {
            let source = table;
            let snapshot_id = source
                .data
                .metadata
                .snapshot_log
                .iter()
                .filter(|e| e.timestamp_ms <= point_ms)
                .max_by_key(|e| e.timestamp_ms)
                .map(|e| e.snapshot_id);
            let snapshot = match snapshot_id {
                Some(id) => Some(source.data.metadata.snapshots.get(&id).ok_or_else(|| {
                    metastore_error::SnapshotNotFoundSnafu {
                        snapshot: id,
                        table: to.table.clone(),
                        schema: to.schema.clone(),
                        db: to.database.clone(),
                    }
                    .build()
                })?),
                None => None,
            };
            let (snapshots, snapshot_log, refs) = match (snapshot_id, snapshot) {
                (Some(id), Some(s)) => (
                    HashMap::from_iter([(id, s.clone())]),
                    vec![SnapshotLog {
                        snapshot_id: id,
                        timestamp_ms: point_ms,
                    }],
                    HashMap::from_iter([(
                        "main".to_string(),
                        SnapshotReference {
                            snapshot_id: id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    )]),
                ),
                _ => (HashMap::new(), Vec::new(), HashMap::new()),
            };
            let metadata = TableMetadataBuilder::default()
                .format_version(source.data.metadata.format_version)
                .location(source.data.metadata.location.clone())
                .last_updated_ms(point_ms)
                .last_column_id(source.data.metadata.last_column_id)
                .schemas(source.data.metadata.schemas.clone())
                .current_schema_id(source.data.metadata.current_schema_id)
                .partition_specs(source.data.metadata.partition_specs.clone())
                .default_spec_id(source.data.metadata.default_spec_id)
                .last_partition_id(source.data.metadata.last_partition_id)
                .properties(source.data.metadata.properties.clone())
                .current_snapshot_id(snapshot_id)
                .snapshots(snapshots)
                .snapshot_log(snapshot_log)
                .refs(refs)
                .build()
                .context(metastore_error::TableMetadataBuilderSnafu)?;

            let object_store = self.table_object_store(&to).await?.ok_or_else(|| {
                metastore_error::TableObjectStoreNotFoundSnafu {
                    table: to.table.clone(),
                    schema: to.schema.clone(),
                    db: to.database.clone(),
                }
                .build()
            })?;
            let metadata_location = self.put_metadata(&to, object_store, &metadata).await?;

            let clone = Table {
                ident: to.clone(),
                metadata,
                metadata_location,
                properties: source.data.properties.clone(),
                volume_ident: source.data.volume_ident.clone(),
                volume_location: source.data.volume_location.clone(),
                is_temporary: source.data.is_temporary,
                format: source.data.format.clone(),
            };
            let row = RwObject::new(clone);
            self.journal_put(&CatalogRecord::Table(row.clone())).await?;
            self.state
                .write()
                .await
                .tables
                .insert(Self::table_key(&to), row.clone());
            return Ok(true);
        }

        // Plain clone of the record (zero-copy, current lineage).
        let clone = Table {
            ident: to.clone(),
            ..table.data.clone()
        };
        let row = RwObject::new(clone);
        self.journal_put(&CatalogRecord::Table(row.clone())).await?;
        self.state
            .write()
            .await
            .tables
            .insert(Self::table_key(&to), row.clone());
        Ok(true)
    }

    #[instrument(name = "Metastore::table_object_store", level = "trace", ret)]
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        if let Some(volume) = self.volume_for_table(ident).await? {
            self.volume_object_store(&volume.ident).await
        } else {
            Ok(None)
        }
    }

    #[instrument(name = "Metastore::table_exists", level = "trace", ret)]
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.tables.contains_key(&Self::table_key(ident)))
    }

    #[instrument(name = "Metastore::url_for_table", level = "trace", ret)]
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String> {
        let state = self.state.read().await;
        if let Some(table) = state.tables.get(&Self::table_key(ident)) {
            Ok(table
                .volume_location
                .clone()
                .unwrap_or_else(|| format!("memory://{}", ident.table)))
        } else {
            metastore_error::TableNotFoundSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail()
        }
    }

    #[instrument(name = "Metastore::volume_for_table", level = "trace", ret)]
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        let state = self.state.read().await;
        if let Some(volume_ident) = state
            .tables
            .get(&Self::table_key(ident))
            .and_then(|table| table.volume_ident.as_ref())
        {
            Ok(state.volumes.get(volume_ident).cloned())
        } else if let Some(volume_ident) = state
            .databases
            .get(&Self::database_key(ident))
            .map(|database| &database.volume)
        {
            Ok(state.volumes.get(volume_ident).cloned())
        } else {
            Ok(None)
        }
    }

    fn settings_config(&self) -> Option<MetastoreSettingsConfig> {
        self.settings_config.clone()
    }

    #[instrument(
        name = "Metastore::create_stream",
        level = "debug",
        skip(self, stream),
        err
    )]
    async fn create_stream(&self, ident: &TableIdent, stream: Stream) -> Result<RwObject<Stream>> {
        let mut state = self.state.write().await;
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        Self::assert_object_name_free(&state, ident)?;
        let row = RwObject::new(stream);
        self.journal_put(&CatalogRecord::Stream(row.clone()))
            .await?;
        state.streams.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_stream", level = "debug", skip(self), err)]
    async fn get_stream(&self, ident: &TableIdent) -> Result<Option<RwObject<Stream>>> {
        let state = self.state.read().await;
        Ok(state.streams.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(name = "Metastore::list_streams", level = "debug", skip(self), err)]
    async fn list_streams(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Stream>>> {
        let state = self.state.read().await;
        Ok(state
            .streams
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, stream)| stream.clone())
            .collect())
    }

    #[instrument(
        name = "Metastore::update_stream",
        level = "debug",
        skip(self, stream),
        err
    )]
    async fn update_stream(&self, ident: &TableIdent, stream: Stream) -> Result<RwObject<Stream>> {
        let mut state = self.state.write().await;
        let row = state
            .streams
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::StreamNotFoundSnafu {
                    stream: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        row.update(stream);
        self.journal_put(&CatalogRecord::Stream(row.clone()))
            .await?;
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_stream", level = "debug", skip(self), err)]
    async fn delete_stream(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        let removed = state
            .streams
            .remove(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::StreamNotFoundSnafu {
                    stream: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        drop(removed);
        self.journal_delete(
            journal::domain::STREAMS,
            &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
        )
        .await?;
        Ok(())
    }

    #[instrument(
        name = "Metastore::create_task",
        level = "debug",
        skip(self, task),
        err
    )]
    async fn create_task(&self, ident: &TableIdent, task: Task) -> Result<RwObject<Task>> {
        let mut state = self.state.write().await;
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        Self::assert_object_name_free(&state, ident)?;
        let row = RwObject::new(task);
        self.journal_put(&CatalogRecord::Task(row.clone())).await?;
        state.tasks.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_task", level = "debug", skip(self), err)]
    async fn get_task(&self, ident: &TableIdent) -> Result<Option<RwObject<Task>>> {
        let state = self.state.read().await;
        Ok(state.tasks.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(name = "Metastore::list_tasks", level = "debug", skip(self), err)]
    async fn list_tasks(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Task>>> {
        let state = self.state.read().await;
        Ok(state
            .tasks
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, task)| task.clone())
            .collect())
    }

    #[instrument(name = "Metastore::list_all_tasks", level = "debug", skip(self), err)]
    async fn list_all_tasks(&self) -> Result<Vec<RwObject<Task>>> {
        let state = self.state.read().await;
        Ok(state.tasks.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::update_task",
        level = "debug",
        skip(self, task),
        err
    )]
    async fn update_task(&self, ident: &TableIdent, task: Task) -> Result<RwObject<Task>> {
        let mut state = self.state.write().await;
        let row = state
            .tasks
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::TaskNotFoundSnafu {
                    task: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        row.update(task);
        self.journal_put(&CatalogRecord::Task(row.clone())).await?;
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_task", level = "debug", skip(self), err)]
    async fn delete_task(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        let removed = state.tasks.remove(&Self::table_key(ident)).ok_or_else(|| {
            metastore_error::TaskNotFoundSnafu {
                task: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .build()
        })?;
        drop(removed);
        self.journal_delete(
            journal::domain::TASKS,
            &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
        )
        .await?;
        Ok(())
    }

    #[instrument(
        name = "Metastore::create_sequence",
        level = "debug",
        skip(self, sequence),
        err
    )]
    async fn create_sequence(
        &self,
        ident: &TableIdent,
        sequence: Sequence,
    ) -> Result<RwObject<Sequence>> {
        let mut state = self.state.write().await;
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        Self::assert_object_name_free(&state, ident)?;
        let row = RwObject::new(sequence);
        self.journal_put(&CatalogRecord::Sequence(row.clone()))
            .await?;
        state.sequences.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_sequence", level = "debug", skip(self), err)]
    async fn get_sequence(&self, ident: &TableIdent) -> Result<Option<RwObject<Sequence>>> {
        let state = self.state.read().await;
        Ok(state.sequences.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(name = "Metastore::list_sequences", level = "debug", skip(self), err)]
    async fn list_sequences(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Sequence>>> {
        let state = self.state.read().await;
        Ok(state
            .sequences
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, sequence)| sequence.clone())
            .collect())
    }

    #[instrument(
        name = "Metastore::list_all_sequences",
        level = "debug",
        skip(self),
        err
    )]
    async fn list_all_sequences(&self) -> Result<Vec<RwObject<Sequence>>> {
        let state = self.state.read().await;
        Ok(state.sequences.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::update_sequence",
        level = "debug",
        skip(self, sequence),
        err
    )]
    async fn update_sequence(
        &self,
        ident: &TableIdent,
        sequence: Sequence,
    ) -> Result<RwObject<Sequence>> {
        let mut state = self.state.write().await;
        let row = state
            .sequences
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::SequenceNotFoundSnafu {
                    sequence: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        row.update(sequence);
        self.journal_put(&CatalogRecord::Sequence(row.clone()))
            .await?;
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_sequence", level = "debug", skip(self), err)]
    async fn delete_sequence(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        let removed = state
            .sequences
            .remove(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::SequenceNotFoundSnafu {
                    sequence: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        drop(removed);
        self.journal_delete(
            journal::domain::SEQUENCES,
            &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
        )
        .await?;
        Ok(())
    }

    #[instrument(
        name = "Metastore::create_dynamic_table",
        level = "debug",
        skip(self, dynamic_table),
        err
    )]
    async fn create_dynamic_table(
        &self,
        ident: &TableIdent,
        dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>> {
        let mut state = self.state.write().await;
        if !state
            .schemas
            .contains_key(&Self::schema_key(&ident.clone().into()))
        {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        // A dynamic table shares its ident with the underlying table
        // created by the DT's CTAS (design §18.1-5): an existing table
        // record is expected here and allowed. Streams, tasks, and
        // duplicate DT records still conflict.
        let key = Self::table_key(ident);
        if state.streams.contains_key(&key) {
            return metastore_error::StreamAlreadyExistsSnafu {
                stream: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.tasks.contains_key(&key) {
            return metastore_error::TaskAlreadyExistsSnafu {
                task: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.dynamic_tables.contains_key(&key) {
            return metastore_error::DynamicTableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        let row = RwObject::new(dynamic_table);
        self.journal_put(&CatalogRecord::DynamicTable(row.clone()))
            .await?;
        state
            .dynamic_tables
            .insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(
        name = "Metastore::get_dynamic_table",
        level = "debug",
        skip(self),
        err
    )]
    async fn get_dynamic_table(
        &self,
        ident: &TableIdent,
    ) -> Result<Option<RwObject<DynamicTable>>> {
        let state = self.state.read().await;
        Ok(state.dynamic_tables.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(
        name = "Metastore::list_dynamic_tables",
        level = "debug",
        skip(self),
        err
    )]
    async fn list_dynamic_tables(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<RwObject<DynamicTable>>> {
        let state = self.state.read().await;
        Ok(state
            .dynamic_tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, dynamic_table)| dynamic_table.clone())
            .collect())
    }

    #[instrument(
        name = "Metastore::list_all_dynamic_tables",
        level = "debug",
        skip(self),
        err
    )]
    async fn list_all_dynamic_tables(&self) -> Result<Vec<RwObject<DynamicTable>>> {
        let state = self.state.read().await;
        Ok(state.dynamic_tables.values().cloned().collect())
    }

    #[instrument(
        name = "Metastore::update_dynamic_table",
        level = "debug",
        skip(self, dynamic_table),
        err
    )]
    async fn update_dynamic_table(
        &self,
        ident: &TableIdent,
        dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>> {
        let mut state = self.state.write().await;
        let row = state
            .dynamic_tables
            .get_mut(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::DynamicTableNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        row.update(dynamic_table);
        self.journal_put(&CatalogRecord::DynamicTable(row.clone()))
            .await?;
        Ok(row.clone())
    }

    #[instrument(
        name = "Metastore::delete_dynamic_table",
        level = "debug",
        skip(self),
        err
    )]
    async fn delete_dynamic_table(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        let removed = state
            .dynamic_tables
            .remove(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::DynamicTableNotFoundSnafu {
                    table: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
        drop(removed);
        self.journal_delete(
            journal::domain::DYNAMIC_TABLES,
            &Self::table_journal_key(&ident.database, &ident.schema, &ident.table),
        )
        .await?;
        Ok(())
    }
}

impl InMemoryMetastore {
    /// Every schema object kind shares one namespace (design §18.1-6):
    /// reject `ident` when a table, stream, task, or dynamic-table record
    /// already occupies the name.
    fn assert_object_name_free(state: &MetastoreState, ident: &TableIdent) -> Result<()> {
        let key = Self::table_key(ident);
        if state.tables.contains_key(&key) {
            return metastore_error::TableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.streams.contains_key(&key) {
            return metastore_error::StreamAlreadyExistsSnafu {
                stream: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.tasks.contains_key(&key) {
            return metastore_error::TaskAlreadyExistsSnafu {
                task: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.dynamic_tables.contains_key(&key) {
            return metastore_error::DynamicTableAlreadyExistsSnafu {
                table: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        if state.sequences.contains_key(&key) {
            return metastore_error::SequenceAlreadyExistsSnafu {
                sequence: ident.table.clone(),
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        Ok(())
    }

    #[instrument(
        name = "Metastore::table_object_store_from_request",
        level = "trace",
        ret
    )]
    async fn table_object_store_from_request(
        &self,
        state: &MetastoreState,
        table: &TableCreateRequest,
        ident: &TableIdent,
    ) -> Result<Arc<dyn ObjectStore>> {
        let volume_ident = table.volume_ident.as_ref().ok_or_else(|| {
            metastore_error::TableVolumeMissingSnafu {
                table: ident.table.clone(),
            }
            .build()
        })?;
        Self::ensure_volume(state, volume_ident)?;
        self.volume_object_store(volume_ident)
            .await?
            .ok_or_else(|| {
                metastore_error::VolumeNotFoundSnafu {
                    volume: volume_ident.clone(),
                }
                .build()
            })
    }

    #[instrument(name = "Metastore::update_properties_timestamps", level = "trace")]
    fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
        let utc_now = Utc::now();
        let utc_now_str = utc_now.to_rfc3339();
        properties.insert("created_at".to_string(), utc_now_str.clone());
        properties.insert("updated_at".to_string(), utc_now_str);
    }
}

fn max_field_id(schema: &IcebergSchema) -> i32 {
    fn recurse(field: &StructField) -> i32 {
        let mut max_id = field.id;
        if let Type::Struct(inner) = &field.field_type {
            for child in inner.iter() {
                max_id = max_id.max(recurse(child));
            }
        }
        max_id
    }

    schema.fields().iter().map(recurse).max().unwrap_or(0)
}

#[instrument(
    name = "Metastore::convert_add_schema_update_to_lowercase",
    level = "trace"
)]
fn convert_add_schema_update_to_lowercase(updates: &mut Vec<IcebergTableUpdate>) -> Result<()> {
    for update in updates {
        if let IcebergTableUpdate::AddSchema {
            schema,
            last_column_id,
        } = update
        {
            let schema = convert_schema_fields_to_lowercase(schema)?;
            *update = IcebergTableUpdate::AddSchema {
                schema,
                last_column_id: *last_column_id,
            }
        }
    }
    Ok(())
}

#[instrument(
    name = "Metastore::convert_schema_fields_to_lowercase",
    level = "trace"
)]
fn convert_schema_fields_to_lowercase(schema: &IcebergSchema) -> Result<IcebergSchema> {
    let converted_fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            StructField::new(
                field.id,
                &field.name.to_lowercase(),
                field.required,
                field.field_type.clone(),
                field.doc.clone(),
            )
        })
        .collect();

    let mut builder = IcebergSchema::builder();
    builder.with_schema_id(*schema.schema_id());

    for field in converted_fields {
        builder.with_struct_field(field);
    }

    builder.build().context(metastore_error::IcebergSpecSnafu)
}

#[cfg(test)]
mod undrop_tests {
    //! Tombstone semantics for M9.3 (design §15): drop → hidden + tombstoned,
    //! UNDROP restores the most recent drop, bounded errors otherwise.

    use super::*;
    use crate::models::volumes::{Volume, VolumeType};
    use iceberg_rust_spec::types::PrimitiveType;

    fn test_schema() -> IcebergSchema {
        let mut builder = IcebergSchema::builder();
        builder.with_schema_id(0);
        builder.with_struct_field(StructField::new(
            1,
            "id",
            false,
            Type::Primitive(PrimitiveType::Long),
            None,
        ));
        builder.build().expect("schema builds")
    }

    async fn metastore_with_table(db: &str, schema: &str, table: &str) -> InMemoryMetastore {
        let ms = InMemoryMetastore::new();
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("database");
        ms.create_schema(
            &SchemaIdent::new(db.to_string(), schema.to_string()),
            Schema {
                ident: SchemaIdent::new(db.to_string(), schema.to_string()),
                properties: None,
            },
        )
        .await
        .expect("schema");
        ms.create_table(
            &TableIdent::new(db, schema, table),
            TableCreateRequest {
                ident: TableIdent::new(db, schema, table),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("table");
        ms
    }

    #[tokio::test]
    async fn drop_tombstones_and_undrop_restores() {
        let ms = metastore_with_table("d", "s", "t").await;
        let ident = TableIdent::new("d", "s", "t");

        ms.delete_table(&ident, false).await.expect("drop");
        // Hidden from resolution and listing…
        assert!(!ms.get_table(&ident).await.unwrap().is_some());
        assert!(
            ms.list_tables(&SchemaIdent::new("d".into(), "s".into()))
                .await
                .unwrap()
                .is_empty()
        );
        // …but tombstoned.
        let dropped = ms
            .list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);

        // UNDROP restores the record.
        let restored = ms.undrop_table(&ident).await.expect("undrop");
        assert_eq!(restored.ident, ident);
        assert!(ms.get_table(&ident).await.unwrap().is_some());
        assert!(
            ms.list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
                .await
                .unwrap()
                .is_empty()
        );

        // UNDROP of a live table errors (name occupied).
        let err = ms.undrop_table(&ident).await.unwrap_err().to_string();
        assert!(err.contains("already exists"), "got: {err}");

        // UNDROP with no tombstone and no live table errors.
        let err = ms
            .undrop_table(&TableIdent::new("d", "s", "nope"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not dropped"), "got: {err}");
    }

    #[tokio::test]
    async fn re_drop_overwrites_tombstone_most_recent_wins() {
        let ms = metastore_with_table("d", "s", "t").await;
        let ident = TableIdent::new("d", "s", "t");

        // Capture the first record, drop, recreate (new record), drop again.
        let first = ms.get_table(&ident).await.unwrap().expect("table");
        ms.delete_table(&ident, false).await.expect("drop");
        ms.create_table(
            &ident,
            TableCreateRequest {
                ident: ident.clone(),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("recreate");
        let second = ms.get_table(&ident).await.unwrap().expect("table");
        assert_ne!(first.metadata_location, second.metadata_location);
        ms.delete_table(&ident, false).await.expect("drop");

        // Only the most recent tombstone survives; UNDROP restores it.
        let dropped = ms
            .list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);
        let restored = ms.undrop_table(&ident).await.expect("undrop");
        assert_eq!(restored.metadata_location, second.metadata_location);
    }

    #[tokio::test]
    async fn schema_cascade_tombstones_tables() {
        let ms = metastore_with_table("d", "s", "t").await;
        let ident = TableIdent::new("d", "s", "t");
        ms.delete_schema(&SchemaIdent::new("d".into(), "s".into()), true)
            .await
            .expect("schema drop");
        assert!(!ms.get_table(&ident).await.unwrap().is_some());
        let dropped = ms
            .list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap();
        assert_eq!(dropped.len(), 1);
        // Schema is gone — UNDROP TABLE would orphan the table and errors
        // (M17.2.2); UNDROP SCHEMA restores the pair.
        let err = ms.undrop_table(&ident).await.unwrap_err().to_string();
        assert!(err.contains("not found"), "got: {err}");
        ms.undrop_schema(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .expect("undrop schema");
        assert!(
            ms.get_schema(&SchemaIdent::new("d".into(), "s".into()))
                .await
                .unwrap()
                .is_some()
        );
        // The cascade table tombstone was consumed by the schema restore.
        assert!(ms.get_table(&ident).await.unwrap().is_some());
        let dropped = ms
            .list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap();
        assert!(dropped.is_empty());
    }

    async fn add_table(ms: &InMemoryMetastore, db: &str, schema: &str, table: &str) {
        ms.create_table(
            &TableIdent::new(db, schema, table),
            TableCreateRequest {
                ident: TableIdent::new(db, schema, table),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("table");
    }

    #[tokio::test]
    async fn undrop_database_restores_cascade_containers() {
        let ms = metastore_with_table("d", "s1", "t1").await;
        ms.create_schema(
            &SchemaIdent::new("d".into(), "s2".into()),
            Schema {
                ident: SchemaIdent::new("d".into(), "s2".into()),
                properties: None,
            },
        )
        .await
        .expect("schema s2");
        add_table(&ms, "d", "s2", "t2").await;

        ms.delete_database(&"d".to_string(), true)
            .await
            .expect("db drop");
        assert!(!ms.get_database(&"d".to_string()).await.unwrap().is_some());
        assert_eq!(
            ms.list_dropped_schemas(&"d".to_string())
                .await
                .unwrap()
                .len(),
            2
        );

        let restored = ms
            .undrop_database(&"d".to_string())
            .await
            .expect("undrop database");
        assert_eq!(restored.data.ident, "d");
        // Both schemas and both cascade tables are back; tombstones consumed.
        for s in ["s1", "s2"] {
            assert!(
                ms.get_schema(&SchemaIdent::new("d".into(), s.into()))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert!(
            ms.get_table(&TableIdent::new("d", "s1", "t1"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ms.get_table(&TableIdent::new("d", "s2", "t2"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ms.list_dropped_schemas(&"d".to_string())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(ms.list_dropped_databases().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn undrop_schema_skips_individually_dropped_tables() {
        let ms = metastore_with_table("d", "s", "t1").await;
        add_table(&ms, "d", "s", "t2").await;

        // t1 dropped individually (tombstone predates the schema drop).
        ms.delete_table(&TableIdent::new("d", "s", "t1"), false)
            .await
            .expect("drop t1");
        ms.delete_schema(&SchemaIdent::new("d".into(), "s".into()), true)
            .await
            .expect("schema drop");

        ms.undrop_schema(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .expect("undrop schema");
        // t2 (cascade drop) is restored; t1 (individual drop) stays dropped.
        assert!(
            ms.get_table(&TableIdent::new("d", "s", "t2"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            !ms.get_table(&TableIdent::new("d", "s", "t1"))
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            ms.list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn undrop_errors_when_live_name_occupied() {
        let ms = metastore_with_table("d", "s", "t").await;
        ms.delete_schema(&SchemaIdent::new("d".into(), "s".into()), true)
            .await
            .expect("schema drop");
        ms.create_schema(
            &SchemaIdent::new("d".into(), "s".into()),
            Schema {
                ident: SchemaIdent::new("d".into(), "s".into()),
                properties: None,
            },
        )
        .await
        .expect("recreate schema");
        let err = ms
            .undrop_schema(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("already exists"), "got: {err}");

        ms.delete_database(&"d".to_string(), true)
            .await
            .expect("db drop");
        ms.create_database(
            &"d".to_string(),
            Database {
                ident: "d".to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("recreate database");
        let err = ms
            .undrop_database(&"d".to_string())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("already exists"), "got: {err}");
    }
}

#[cfg(test)]
mod clone_as_of_tests {
    //! As-of clone bounds (M9.4, design §15). The happy path is exercised by
    //! the wire probe (`/tmp/m8probe/run10.sh`).

    use super::*;
    use crate::models::volumes::{Volume, VolumeType};
    use iceberg_rust_spec::types::PrimitiveType;

    fn test_schema() -> IcebergSchema {
        let mut builder = IcebergSchema::builder();
        builder.with_schema_id(0);
        builder.with_struct_field(StructField::new(
            1,
            "id",
            false,
            Type::Primitive(PrimitiveType::Long),
            None,
        ));
        builder.build().expect("schema builds")
    }

    async fn metastore_with_table(db: &str, schema: &str, table: &str) -> InMemoryMetastore {
        let ms = InMemoryMetastore::new();
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("database");
        ms.create_schema(
            &SchemaIdent::new(db.to_string(), schema.to_string()),
            Schema {
                ident: SchemaIdent::new(db.to_string(), schema.to_string()),
                properties: None,
            },
        )
        .await
        .expect("schema");
        ms.create_table(
            &TableIdent::new(db, schema, table),
            TableCreateRequest {
                ident: TableIdent::new(db, schema, table),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("table");
        ms
    }

    #[tokio::test]
    async fn as_of_clone_rejects_unknown_snapshot() {
        let ms = metastore_with_table("d", "s", "t").await;
        let err = ms
            .clone_table_as_of(
                &TableIdent::new("d", "s", "t"),
                &TableIdent::new("d", "s", "t2"),
                42,
                0,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("Snapshot 42 not found"), "got: {err}");
        // Target must not have been created by the failed clone.
        assert!(
            !ms.get_table(&TableIdent::new("d", "s", "t2"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn schema_clone_as_of_bounds_tables_by_creation() {
        let ms = metastore_with_table("d", "s", "t").await;
        let table = ms
            .get_table(&TableIdent::new("d", "s", "t"))
            .await
            .unwrap()
            .expect("table");
        let created = table.created_at.and_utc().timestamp_millis();

        // Point before the table's creation: nothing cloned (target schema
        // is still created — it exists as of "now").
        let cloned = ms
            .clone_schema_as_of(
                &SchemaIdent::new("d".into(), "s".into()),
                &SchemaIdent::new("d".into(), "s2".into()),
                created - 1000,
            )
            .await
            .expect("clone as-of before creation");
        assert_eq!(cloned, 0);
        assert!(
            !ms.get_table(&TableIdent::new("d", "s2", "t"))
                .await
                .unwrap()
                .is_some()
        );

        // Point after creation: the never-committed record clones as-is.
        let cloned = ms
            .clone_schema_as_of(
                &SchemaIdent::new("d".into(), "s".into()),
                &SchemaIdent::new("d".into(), "s3".into()),
                created + 1000,
            )
            .await
            .expect("clone as-of after creation");
        assert_eq!(cloned, 1);
        assert!(
            ms.get_table(&TableIdent::new("d", "s3", "t"))
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[cfg(test)]
mod automation_tests {
    //! Automation object store semantics for M11 (design §18): shared
    //! schema namespace across object kinds, stream offset updates, task
    //! state updates, dynamic-table record lifecycle.

    use super::*;
    use crate::models::automation::{RefreshMode, TaskRunState, TaskSchedule, TaskState};
    use crate::models::volumes::{Volume, VolumeType};
    use iceberg_rust_spec::types::PrimitiveType;

    fn test_schema() -> IcebergSchema {
        let mut builder = IcebergSchema::builder();
        builder.with_schema_id(0);
        builder.with_struct_field(StructField::new(
            1,
            "id",
            false,
            Type::Primitive(PrimitiveType::Long),
            None,
        ));
        builder.build().expect("schema builds")
    }

    async fn metastore_with_table(db: &str, schema: &str, table: &str) -> InMemoryMetastore {
        let ms = InMemoryMetastore::new();
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("database");
        ms.create_schema(
            &SchemaIdent::new(db.to_string(), schema.to_string()),
            Schema {
                ident: SchemaIdent::new(db.to_string(), schema.to_string()),
                properties: None,
            },
        )
        .await
        .expect("schema");
        ms.create_table(
            &TableIdent::new(db, schema, table),
            TableCreateRequest {
                ident: TableIdent::new(db, schema, table),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("table");
        ms
    }

    #[tokio::test]
    async fn stream_create_get_update_delete_roundtrip() {
        let ms = metastore_with_table("d", "s", "src").await;
        let ident = TableIdent::new("d", "s", "st");
        let stream = Stream {
            ident: ident.clone(),
            source: TableIdent::new("d", "s", "src"),
            offset_snapshot: Some(42),
            append_only: false,
            comment: None,
        };
        let row = ms.create_stream(&ident, stream).await.expect("create");
        assert_eq!(row.offset_snapshot, Some(42));

        let updated = Stream {
            offset_snapshot: Some(77),
            ..row.data.clone()
        };
        let advanced = ms.update_stream(&ident, updated).await.expect("update");
        assert_eq!(advanced.offset_snapshot, Some(77));
        let fetched = ms.get_stream(&ident).await.expect("get").expect("exists");
        assert_eq!(fetched.offset_snapshot, Some(77));
        assert_eq!(
            ms.list_streams(&SchemaIdent::new("d".to_string(), "s".to_string()))
                .await
                .expect("list")
                .len(),
            1
        );

        ms.delete_stream(&ident).await.expect("delete");
        assert!(ms.get_stream(&ident).await.expect("get").is_none());
        assert!(ms.delete_stream(&ident).await.is_err());
    }

    #[tokio::test]
    async fn object_names_share_one_namespace() {
        let ms = metastore_with_table("d", "s", "src").await;
        // Stream named like the existing table is rejected.
        let stream = Stream {
            ident: TableIdent::new("d", "s", "src"),
            source: TableIdent::new("d", "s", "src"),
            offset_snapshot: None,
            append_only: false,
            comment: None,
        };
        assert!(
            ms.create_stream(&stream.ident.clone(), stream)
                .await
                .is_err()
        );
        // A second stream with the same name is rejected.
        let st = TableIdent::new("d", "s", "st");
        let stream = Stream {
            ident: st.clone(),
            source: TableIdent::new("d", "s", "src"),
            offset_snapshot: None,
            append_only: false,
            comment: None,
        };
        ms.create_stream(&st, stream).await.expect("stream");
        let colliding = Stream {
            ident: st.clone(),
            source: TableIdent::new("d", "s", "src"),
            offset_snapshot: None,
            append_only: false,
            comment: None,
        };
        assert!(ms.create_stream(&st, colliding).await.is_err());
        // Task colliding with the stream is rejected.
        let task = Task {
            ident: st.clone(),
            definition: "SELECT 1".to_string(),
            schedule: None,
            state: TaskState::Started,
            warehouse: None,
            predecessor: None,
            last_run_ms: None,
            last_run_state: None,
        };
        assert!(ms.create_task(&st, task.clone()).await.is_err());
        // Table colliding with the stream is rejected.
        assert!(
            ms.create_table(
                &st,
                TableCreateRequest {
                    ident: st.clone(),
                    properties: None,
                    format: Some(TableFormat::Iceberg),
                    location: None,
                    schema: test_schema(),
                    partition_spec: None,
                    sort_order: None,
                    stage_create: None,
                    volume_ident: Some("vol".to_string()),
                    is_temporary: Some(false),
                }
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn task_lifecycle_and_listing() {
        let ms = metastore_with_table("d", "s", "src").await;
        let ident = TableIdent::new("d", "s", "tk");
        let task = Task {
            ident: ident.clone(),
            definition: "INSERT INTO d.s.src VALUES (1)".to_string(),
            schedule: Some(TaskSchedule::Minutes(5)),
            state: TaskState::Started,
            warehouse: Some("wh".to_string()),
            predecessor: None,
            last_run_ms: None,
            last_run_state: None,
        };
        ms.create_task(&ident, task).await.expect("create");

        let fetched = ms.get_task(&ident).await.expect("get").expect("exists");
        assert_eq!(fetched.state, TaskState::Started);
        assert_eq!(fetched.schedule, Some(TaskSchedule::Minutes(5)));

        let suspended = Task {
            state: TaskState::Suspended,
            last_run_ms: Some(1_000),
            last_run_state: Some(TaskRunState::Succeeded),
            ..fetched.data.clone()
        };
        ms.update_task(&ident, suspended).await.expect("update");
        let after = ms.get_task(&ident).await.expect("get").expect("exists");
        assert_eq!(after.state, TaskState::Suspended);
        assert_eq!(after.last_run_state, Some(TaskRunState::Succeeded));

        assert_eq!(ms.list_all_tasks().await.expect("list_all").len(), 1);
        assert_eq!(
            ms.list_tasks(&SchemaIdent::new("d".to_string(), "s".to_string()))
                .await
                .expect("list")
                .len(),
            1
        );
        assert!(
            ms.list_tasks(&SchemaIdent::new("d".to_string(), "other".to_string()))
                .await
                .expect("list other")
                .is_empty()
        );

        ms.delete_task(&ident).await.expect("delete");
        assert!(ms.get_task(&ident).await.expect("get").is_none());
    }

    #[tokio::test]
    async fn dynamic_table_record_lifecycle() {
        let ms = metastore_with_table("d", "s", "src").await;
        let ident = TableIdent::new("d", "s", "dt");
        let record = DynamicTable {
            ident: ident.clone(),
            definition: "SELECT * FROM d.s.src".to_string(),
            warehouse: Some("wh".to_string()),
            target_lag_seconds: Some(60),
            last_refresh_ms: None,
            refresh_mode: RefreshMode::default(),
            base_table: None,
            stream_ident: None,
        };
        ms.create_dynamic_table(&ident, record)
            .await
            .expect("create");
        let fetched = ms
            .get_dynamic_table(&ident)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(fetched.target_lag_seconds, Some(60));

        let refreshed = DynamicTable {
            last_refresh_ms: Some(5_000),
            ..fetched.data.clone()
        };
        ms.update_dynamic_table(&ident, refreshed)
            .await
            .expect("update");
        assert_eq!(
            ms.list_all_dynamic_tables().await.expect("list_all").len(),
            1
        );
        ms.delete_dynamic_table(&ident).await.expect("delete");
        assert!(ms.get_dynamic_table(&ident).await.expect("get").is_none());
    }
}

#[cfg(test)]
mod cascade_tests {
    //! Cascade-drop regression (design §22.9): `delete_database` /
    //! `delete_schema` must also drop the auxiliary domains (streams,
    //! tasks, dynamic tables, sequences) from state *and* the journal, or
    //! re-creating a same-named database/schema fails with
    //! "already exists" (M16.4 probe regression).

    use super::*;
    use crate::models::automation::{RefreshMode, TaskSchedule, TaskState};
    use crate::models::table::TableFormat;
    use crate::models::volumes::{Volume, VolumeType};
    use iceberg_rust_spec::types::PrimitiveType;

    fn test_schema() -> IcebergSchema {
        let mut builder = IcebergSchema::builder();
        builder.with_schema_id(0);
        builder.with_struct_field(StructField::new(
            1,
            "id",
            false,
            Type::Primitive(PrimitiveType::Long),
            None,
        ));
        builder.build().expect("schema builds")
    }

    async fn metastore_with_fixtures(db: &str, schema: &str) -> InMemoryMetastore {
        let ms = InMemoryMetastore::new();
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("database");
        ms.create_schema(
            &SchemaIdent::new(db.to_string(), schema.to_string()),
            Schema {
                ident: SchemaIdent::new(db.to_string(), schema.to_string()),
                properties: None,
            },
        )
        .await
        .expect("schema");
        ms.create_table(
            &TableIdent::new(db, schema, "src"),
            TableCreateRequest {
                ident: TableIdent::new(db, schema, "src"),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: test_schema(),
                partition_spec: None,
                sort_order: None,
                stage_create: None,
                volume_ident: Some("vol".to_string()),
                is_temporary: Some(false),
            },
        )
        .await
        .expect("table");

        let stream_ident = TableIdent::new(db, schema, "st1");
        ms.create_stream(
            &stream_ident,
            Stream {
                ident: stream_ident.clone(),
                source: TableIdent::new(db, schema, "src"),
                offset_snapshot: Some(1),
                append_only: false,
                comment: None,
            },
        )
        .await
        .expect("stream");
        let task_ident = TableIdent::new(db, schema, "tk1");
        ms.create_task(
            &task_ident,
            Task {
                ident: task_ident.clone(),
                definition: "SELECT 1".to_string(),
                schedule: Some(TaskSchedule::Minutes(5)),
                state: TaskState::Suspended,
                warehouse: None,
                predecessor: None,
                last_run_ms: None,
                last_run_state: None,
            },
        )
        .await
        .expect("task");
        let dt_ident = TableIdent::new(db, schema, "dt1");
        ms.create_dynamic_table(
            &dt_ident,
            DynamicTable {
                ident: dt_ident.clone(),
                definition: "SELECT 1".to_string(),
                warehouse: None,
                target_lag_seconds: Some(60),
                last_refresh_ms: None,
                refresh_mode: RefreshMode::Full,
                base_table: None,
                stream_ident: None,
            },
        )
        .await
        .expect("dynamic table");
        let seq_ident = TableIdent::new(db, schema, "sq1");
        ms.create_sequence(
            &seq_ident,
            Sequence {
                ident: seq_ident.clone(),
                start: 1,
                increment: 1,
                current: 0,
                comment: None,
            },
        )
        .await
        .expect("sequence");
        ms
    }

    async fn recreate_database_and_schema(ms: &InMemoryMetastore, db: &str, schema: &str) {
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
                account: crate::DEFAULT_DATABASE_ACCOUNT.to_string(),
            },
        )
        .await
        .expect("recreate database");
        ms.create_schema(
            &SchemaIdent::new(db.to_string(), schema.to_string()),
            Schema {
                ident: SchemaIdent::new(db.to_string(), schema.to_string()),
                properties: None,
            },
        )
        .await
        .expect("recreate schema");
    }

    async fn assert_aux_lists_empty(ms: &InMemoryMetastore, db: &str, schema: &str) {
        let schema_ident = SchemaIdent::new(db.to_string(), schema.to_string());
        assert!(
            ms.list_streams(&schema_ident)
                .await
                .expect("streams")
                .is_empty()
        );
        assert!(
            ms.list_tasks(&schema_ident)
                .await
                .expect("tasks")
                .is_empty()
        );
        assert!(
            ms.list_dynamic_tables(&schema_ident)
                .await
                .expect("dynamic tables")
                .is_empty()
        );
        assert!(
            ms.list_sequences(&schema_ident)
                .await
                .expect("sequences")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_database_cascades_aux_domains_and_allows_recreation() {
        let ms = metastore_with_fixtures("d", "s").await;
        ms.delete_database(&"d".to_string(), true)
            .await
            .expect("cascade drop");
        recreate_database_and_schema(&ms, "d", "s").await;
        assert_aux_lists_empty(&ms, "d", "s").await;

        // Same names must be free again (the old "already exists" bug).
        let stream_ident = TableIdent::new("d", "s", "st1");
        ms.create_stream(
            &stream_ident,
            Stream {
                ident: stream_ident.clone(),
                source: TableIdent::new("d", "s", "src"),
                offset_snapshot: None,
                append_only: false,
                comment: None,
            },
        )
        .await
        .expect("re-create stream");
        let seq_ident = TableIdent::new("d", "s", "sq1");
        ms.create_sequence(
            &seq_ident,
            Sequence {
                ident: seq_ident.clone(),
                start: 1,
                increment: 1,
                current: 0,
                comment: None,
            },
        )
        .await
        .expect("re-create sequence");
    }

    #[tokio::test]
    async fn delete_schema_cascades_aux_domains_and_allows_recreation() {
        let ms = metastore_with_fixtures("d", "s").await;
        ms.delete_schema(&SchemaIdent::new("d".to_string(), "s".to_string()), true)
            .await
            .expect("cascade drop");
        ms.create_schema(
            &SchemaIdent::new("d".to_string(), "s".to_string()),
            Schema {
                ident: SchemaIdent::new("d".to_string(), "s".to_string()),
                properties: None,
            },
        )
        .await
        .expect("recreate schema");
        assert_aux_lists_empty(&ms, "d", "s").await;

        let seq_ident = TableIdent::new("d", "s", "sq1");
        ms.create_sequence(
            &seq_ident,
            Sequence {
                ident: seq_ident.clone(),
                start: 1,
                increment: 1,
                current: 0,
                comment: None,
            },
        )
        .await
        .expect("re-create sequence");
    }

    #[tokio::test]
    async fn delete_database_non_cascade_still_rejects_aux_domains() {
        let ms = metastore_with_fixtures("d", "s").await;
        let err = ms
            .delete_database(&"d".to_string(), false)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("in use"), "got: {err}");
    }
}
