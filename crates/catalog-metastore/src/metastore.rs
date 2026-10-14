use crate::error::{self as metastore_error, Result};
use crate::metastore_settings_config::MetastoreSettingsConfig;
use crate::models::{
    RwObject,
    automation::{DynamicTable, Stream, Task},
    database::{Database, DatabaseIdent},
    schema::{Schema, SchemaIdent},
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
use object_store::{ObjectStoreExt, ObjectStore, PutPayload, path::Path};
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
            let target = TableIdent::new(
                &to.database,
                &to.schema,
                &table.data.ident.table,
            );
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
        };
        self.create_database(&to.to_string(), database).await?;
        let mut cloned = 0;
        for schema in self.list_schemas(from).await? {
            let target_schema =
                SchemaIdent::new(to.to_string(), schema.data.ident.schema.clone());
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

    /// Tables currently tombstoned (dropped) in `schema`, most recent drop
    /// first.
    async fn list_dropped_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>>;
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

    /// Dynamic-table definition records (design §18, M11). The target table
    /// itself is a regular metastore table.
    async fn create_dynamic_table(
        &self,
        ident: &TableIdent,
        dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>>;
    async fn get_dynamic_table(&self, ident: &TableIdent)
    -> Result<Option<RwObject<DynamicTable>>>;
    async fn list_dynamic_tables(&self, schema: &SchemaIdent)
    -> Result<Vec<RwObject<DynamicTable>>>;
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
    streams: HashMap<(DatabaseIdent, String, String), RwObject<Stream>>,
    tasks: HashMap<(DatabaseIdent, String, String), RwObject<Task>>,
    dynamic_tables: HashMap<(DatabaseIdent, String, String), RwObject<DynamicTable>>,
}

#[derive(Debug, Default)]
pub struct InMemoryMetastore {
    state: RwLock<MetastoreState>,
    object_store_cache: DashMap<VolumeIdent, Arc<dyn ObjectStore>>,
    settings_config: Option<MetastoreSettingsConfig>,
}

impl InMemoryMetastore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RwLock::new(MetastoreState::default()),
            object_store_cache: DashMap::new(),
            settings_config: None,
        }
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
            }
            let table_keys: Vec<_> = state
                .tables
                .keys()
                .filter(|(db, _, _)| in_use.iter().any(|d| d == db))
                .cloned()
                .collect();
            for key in table_keys {
                if let Some(table) = state.tables.remove(&key) {
                    state.dropped_tables.insert(key, (table, Self::now_ms()));
                }
            }
            for db in in_use {
                state.databases.remove(&db);
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
        if !schema_keys.is_empty() && !cascade {
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
        for key in &schema_keys {
            state.schemas.remove(key);
        }
        let table_keys: Vec<_> = state
            .tables
            .keys()
            .filter(|(db, _, _)| db == name)
            .cloned()
            .collect();
        for key in &table_keys {
            if let Some(table) = state.tables.remove(key) {
                state
                    .dropped_tables
                    .insert(key.clone(), (table, Self::now_ms()));
            }
        }
        state.databases.remove(name);
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
        if !tables.is_empty() && !cascade {
            return metastore_error::SchemaNotFoundSnafu {
                schema: ident.schema.clone(),
                db: ident.database.clone(),
            }
            .fail();
        }
        for key in tables {
            if let Some(table) = state.tables.remove(&key) {
                state.dropped_tables.insert(key, (table, Self::now_ms()));
            }
        }
        state.schemas.remove(&Self::schema_key(ident));
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
        state.tables.insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    /// Zero-copy clone (M8, design §14). Metadata-only: the clone shares the
    /// source's snapshots/manifests (absolute paths) and volume, but gets its
    /// own location and a fresh metadata JSON, so subsequent commits to
    /// either table are independent.
    #[instrument(
        name = "Metastore::clone_table",
        level = "debug",
        skip(self),
        err
    )]
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
        Ok(table_entry.clone())
    }

    #[instrument(name = "Metastore::delete_table", level = "debug", skip(self), err)]
    async fn delete_table(&self, ident: &TableIdent, _cascade: bool) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(table) = state.tables.remove(&Self::table_key(ident)) {
            state
                .dropped_tables
                .insert(Self::table_key(ident), (table, Self::now_ms()));
        }
        Ok(())
    }

    #[instrument(name = "Metastore::undrop_table", level = "debug", skip(self), err)]
    async fn undrop_table(&self, ident: &TableIdent) -> Result<RwObject<Table>> {
        let mut state = self.state.write().await;
        let key = Self::table_key(ident);
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
        state.tables.insert(key, table.clone());
        Ok(table)
    }

    #[instrument(name = "Metastore::list_dropped_tables", level = "trace", skip(self), err)]
    async fn list_dropped_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        let state = self.state.read().await;
        let mut items: Vec<(RwObject<Table>, i64)> = state
            .dropped_tables
            .iter()
            .filter(|((db, sch, _), _)| db == &schema.database && sch == &schema.schema)
            .map(|(_, (table, dropped_at))| (table.clone(), *dropped_at))
            .collect();
        items.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(items.into_iter().map(|(table, _)| table).collect())
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

    #[instrument(name = "Metastore::create_stream", level = "debug", skip(self, stream), err)]
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

    #[instrument(name = "Metastore::update_stream", level = "debug", skip(self, stream), err)]
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
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_stream", level = "debug", skip(self), err)]
    async fn delete_stream(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        state
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
        Ok(())
    }

    #[instrument(name = "Metastore::create_task", level = "debug", skip(self, task), err)]
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

    #[instrument(name = "Metastore::update_task", level = "debug", skip(self, task), err)]
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
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_task", level = "debug", skip(self), err)]
    async fn delete_task(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        state
            .tasks
            .remove(&Self::table_key(ident))
            .ok_or_else(|| {
                metastore_error::TaskNotFoundSnafu {
                    task: ident.table.clone(),
                    schema: ident.schema.clone(),
                    db: ident.database.clone(),
                }
                .build()
            })?;
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
        Self::assert_object_name_free(&state, ident)?;
        let row = RwObject::new(dynamic_table);
        state
            .dynamic_tables
            .insert(Self::table_key(ident), row.clone());
        Ok(row)
    }

    #[instrument(name = "Metastore::get_dynamic_table", level = "debug", skip(self), err)]
    async fn get_dynamic_table(
        &self,
        ident: &TableIdent,
    ) -> Result<Option<RwObject<DynamicTable>>> {
        let state = self.state.read().await;
        Ok(state.dynamic_tables.get(&Self::table_key(ident)).cloned())
    }

    #[instrument(name = "Metastore::list_dynamic_tables", level = "debug", skip(self), err)]
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
        Ok(row.clone())
    }

    #[instrument(name = "Metastore::delete_dynamic_table", level = "debug", skip(self), err)]
    async fn delete_dynamic_table(&self, ident: &TableIdent) -> Result<()> {
        let mut state = self.state.write().await;
        state
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
        ms.create_volume(&"vol".to_string(), Volume::new("vol".into(), VolumeType::Memory))
            .await
            .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
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
        assert!(ms.list_tables(&SchemaIdent::new("d".into(), "s".into())).await.unwrap().is_empty());
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
        assert!(ms
            .list_dropped_tables(&SchemaIdent::new("d".into(), "s".into()))
            .await
            .unwrap()
            .is_empty());

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
        // Schema is gone — restore still works at the metastore level.
        ms.undrop_table(&ident).await.expect("undrop");
        assert!(ms.get_table(&ident).await.unwrap().is_some());
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
        ms.create_volume(&"vol".to_string(), Volume::new("vol".into(), VolumeType::Memory))
            .await
            .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
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
        assert!(!ms
            .get_table(&TableIdent::new("d", "s", "t2"))
            .await
            .unwrap()
            .is_some());
    }
}

#[cfg(test)]
mod automation_tests {
    //! Automation object store semantics for M11 (design §18): shared
    //! schema namespace across object kinds, stream offset updates, task
    //! state updates, dynamic-table record lifecycle.

    use super::*;
    use crate::models::automation::{TaskRunState, TaskSchedule, TaskState};
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
        ms.create_volume(&"vol".to_string(), Volume::new("vol".into(), VolumeType::Memory))
            .await
            .expect("volume");
        ms.create_database(
            &db.to_string(),
            Database {
                ident: db.to_string(),
                properties: None,
                volume: "vol".to_string(),
                should_refresh: false,
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
        assert!(ms.create_stream(&stream.ident.clone(), stream).await.is_err());
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
        assert!(ms
            .create_table(
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
            .is_err());
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

        assert_eq!(
            ms.list_all_tasks().await.expect("list_all").len(),
            1
        );
        assert_eq!(
            ms.list_tasks(&SchemaIdent::new("d".to_string(), "s".to_string()))
                .await
                .expect("list")
                .len(),
            1
        );
        assert!(ms
            .list_tasks(&SchemaIdent::new("d".to_string(), "other".to_string()))
            .await
            .expect("list other")
            .is_empty());

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
        };
        ms.create_dynamic_table(&ident, record).await.expect("create");
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
        assert!(ms
            .get_dynamic_table(&ident)
            .await
            .expect("get")
            .is_none());
    }
}
