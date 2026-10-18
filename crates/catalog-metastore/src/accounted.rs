//! Account-scoped metastore view (M15.4, multi-account tenancy).
//!
//! [`AccountedMetastore`] wraps a shared [`Metastore`] and namespaces every
//! database for a non-default account by qualifying the database component of
//! each ident (`db` -> `{account}/{db}`) before delegating. Callers keep
//! working with unqualified display names: the wrapper strips the prefix from
//! every record it returns. Schema and table components are never rewritten.
//!
//! For the default account (`embucket`, see [`DEFAULT_DATABASE_ACCOUNT`]) the
//! wrapper is an identity passthrough, so pre-tenancy deployments keep
//! working unchanged. Volumes are global and always pass through.

use crate::error::Result;
use crate::metastore::Metastore;
use crate::metastore_settings_config::MetastoreSettingsConfig;
use crate::models::{
    RwObject,
    automation::{DynamicTable, Stream, Task},
    database::{DEFAULT_DATABASE_ACCOUNT, Database, DatabaseIdent},
    schema::{Schema, SchemaIdent},
    sequence::Sequence,
    table::{Table, TableCreateRequest, TableIdent, TableUpdate},
    volumes::{Volume, VolumeIdent},
};
use async_trait::async_trait;
use object_store::ObjectStore;
use std::sync::Arc;

/// Account-scoped view over a shared metastore. See the module docs.
#[derive(Debug, Clone)]
pub struct AccountedMetastore {
    inner: Arc<dyn Metastore>,
    account: String,
}

impl AccountedMetastore {
    #[must_use]
    pub fn new(inner: Arc<dyn Metastore>, account: &str) -> Self {
        Self {
            inner,
            account: account.to_string(),
        }
    }

    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }

    #[must_use]
    pub const fn inner(&self) -> &Arc<dyn Metastore> {
        &self.inner
    }

    /// True for the default account (identity passthrough).
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.account == DEFAULT_DATABASE_ACCOUNT
    }

    fn prefix(&self) -> String {
        format!("{}/", self.account)
    }

    /// `db` -> `{account}/{db}` (idempotent; identity for the default
    /// account).
    #[must_use]
    pub fn qualify_database(&self, db: &str) -> String {
        if self.is_default() || db.starts_with(&self.prefix()) {
            db.to_string()
        } else {
            format!("{}/{db}", self.account)
        }
    }

    /// Removes a leading `{account}/` prefix if present (identity for the
    /// default account).
    #[must_use]
    pub fn strip_database(&self, db: &str) -> String {
        if self.is_default() {
            return db.to_string();
        }
        db.strip_prefix(&self.prefix()).unwrap_or(db).to_string()
    }

    #[must_use]
    pub fn qualify_ident(&self, ident: &TableIdent) -> TableIdent {
        TableIdent {
            table: ident.table.clone(),
            schema: ident.schema.clone(),
            database: self.qualify_database(&ident.database),
        }
    }

    #[must_use]
    pub fn qualify_schema_ident(&self, ident: &SchemaIdent) -> SchemaIdent {
        SchemaIdent {
            schema: ident.schema.clone(),
            database: self.qualify_database(&ident.database),
        }
    }

    #[must_use]
    pub fn strip_table_ident(&self, ident: &TableIdent) -> TableIdent {
        TableIdent {
            table: ident.table.clone(),
            schema: ident.schema.clone(),
            database: self.strip_database(&ident.database),
        }
    }

    #[must_use]
    pub fn strip_schema_ident(&self, ident: &SchemaIdent) -> SchemaIdent {
        SchemaIdent {
            schema: ident.schema.clone(),
            database: self.strip_database(&ident.database),
        }
    }

    /// Whether a (qualified, stored) database name belongs to this account.
    /// Non-default accounts own `{account}/…` names; the default account
    /// owns every unqualified name.
    fn owns_database(&self, stored: &str) -> bool {
        if self.is_default() {
            !stored.contains('/')
        } else {
            stored.starts_with(&self.prefix())
        }
    }

    fn out_database(&self, mut o: RwObject<Database>) -> RwObject<Database> {
        o.data.ident = self.strip_database(&o.data.ident);
        o
    }

    fn out_schema(&self, mut o: RwObject<Schema>) -> RwObject<Schema> {
        o.data.ident = self.strip_schema_ident(&o.data.ident);
        o
    }

    fn out_table(&self, mut o: RwObject<Table>) -> RwObject<Table> {
        o.data.ident = self.strip_table_ident(&o.data.ident);
        o
    }

    fn out_stream(&self, mut o: RwObject<Stream>) -> RwObject<Stream> {
        o.data.ident = self.strip_table_ident(&o.data.ident);
        o
    }

    fn out_task(&self, mut o: RwObject<Task>) -> RwObject<Task> {
        o.data.ident = self.strip_table_ident(&o.data.ident);
        o
    }

    fn out_sequence(&self, mut o: RwObject<Sequence>) -> RwObject<Sequence> {
        o.data.ident = self.strip_table_ident(&o.data.ident);
        o
    }

    fn out_dynamic(&self, mut o: RwObject<DynamicTable>) -> RwObject<DynamicTable> {
        o.data.ident = self.strip_table_ident(&o.data.ident);
        o
    }

    /// Stamp an inbound database record (account + qualified ident).
    fn in_database(&self, qualified: &str, mut database: Database) -> Database {
        database.account.clone_from(&self.account);
        if !self.is_default() {
            database.ident = qualified.to_string();
        }
        database
    }
}

#[async_trait]
impl Metastore for AccountedMetastore {
    // ---- volumes: global, pass through unchanged ----
    async fn list_volumes(&self) -> Result<Vec<RwObject<Volume>>> {
        self.inner.list_volumes().await
    }
    async fn create_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        self.inner.create_volume(name, volume).await
    }
    async fn get_volume(&self, name: &VolumeIdent) -> Result<Option<RwObject<Volume>>> {
        self.inner.get_volume(name).await
    }
    async fn update_volume(&self, name: &VolumeIdent, volume: Volume) -> Result<RwObject<Volume>> {
        self.inner.update_volume(name, volume).await
    }
    async fn delete_volume(&self, name: &VolumeIdent, cascade: bool) -> Result<()> {
        self.inner.delete_volume(name, cascade).await
    }
    async fn volume_object_store(
        &self,
        name: &VolumeIdent,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        self.inner.volume_object_store(name).await
    }

    // ---- databases ----
    async fn list_databases(&self) -> Result<Vec<RwObject<Database>>> {
        Ok(self
            .inner
            .list_databases()
            .await?
            .into_iter()
            .filter(|d| d.data.account == self.account)
            .map(|d| self.out_database(d))
            .collect())
    }
    async fn create_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let q = self.qualify_database(name);
        let database = self.in_database(&q, database);
        let row = self.inner.create_database(&q, database).await?;
        Ok(self.out_database(row))
    }
    async fn get_database(&self, name: &DatabaseIdent) -> Result<Option<RwObject<Database>>> {
        let q = self.qualify_database(name);
        Ok(self
            .inner
            .get_database(&q)
            .await?
            .filter(|d| d.data.account == self.account)
            .map(|d| self.out_database(d)))
    }
    async fn update_database(
        &self,
        name: &DatabaseIdent,
        database: Database,
    ) -> Result<RwObject<Database>> {
        let q = self.qualify_database(name);
        let database = self.in_database(&q, database);
        let row = self.inner.update_database(&q, database).await?;
        Ok(self.out_database(row))
    }
    async fn delete_database(&self, name: &DatabaseIdent, cascade: bool) -> Result<()> {
        self.inner
            .delete_database(&self.qualify_database(name), cascade)
            .await
    }

    // ---- schemas ----
    async fn list_schemas(&self, database: &DatabaseIdent) -> Result<Vec<RwObject<Schema>>> {
        Ok(self
            .inner
            .list_schemas(&self.qualify_database(database))
            .await?
            .into_iter()
            .map(|s| self.out_schema(s))
            .collect())
    }
    async fn create_schema(
        &self,
        ident: &SchemaIdent,
        mut schema: Schema,
    ) -> Result<RwObject<Schema>> {
        let q = self.qualify_schema_ident(ident);
        schema.ident = self.qualify_schema_ident(&schema.ident);
        Ok(self.out_schema(self.inner.create_schema(&q, schema).await?))
    }
    async fn get_schema(&self, ident: &SchemaIdent) -> Result<Option<RwObject<Schema>>> {
        Ok(self
            .inner
            .get_schema(&self.qualify_schema_ident(ident))
            .await?
            .map(|s| self.out_schema(s)))
    }
    async fn update_schema(
        &self,
        ident: &SchemaIdent,
        mut schema: Schema,
    ) -> Result<RwObject<Schema>> {
        let q = self.qualify_schema_ident(ident);
        schema.ident = self.qualify_schema_ident(&schema.ident);
        Ok(self.out_schema(self.inner.update_schema(&q, schema).await?))
    }
    async fn delete_schema(&self, ident: &SchemaIdent, cascade: bool) -> Result<()> {
        self.inner
            .delete_schema(&self.qualify_schema_ident(ident), cascade)
            .await
    }

    // ---- tables ----
    async fn list_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        Ok(self
            .inner
            .list_tables(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|t| self.out_table(t))
            .collect())
    }
    async fn create_table(
        &self,
        ident: &TableIdent,
        mut table: TableCreateRequest,
    ) -> Result<RwObject<Table>> {
        let q = self.qualify_ident(ident);
        table.ident = self.qualify_ident(&table.ident);
        Ok(self.out_table(self.inner.create_table(&q, table).await?))
    }
    async fn register_table(
        &self,
        ident: &TableIdent,
        mut table: Table,
    ) -> Result<RwObject<Table>> {
        let q = self.qualify_ident(ident);
        table.ident = self.qualify_ident(&table.ident);
        Ok(self.out_table(self.inner.register_table(&q, table).await?))
    }
    async fn clone_table(&self, from: &TableIdent, to: &TableIdent) -> Result<RwObject<Table>> {
        let row = self
            .inner
            .clone_table(&self.qualify_ident(from), &self.qualify_ident(to))
            .await?;
        Ok(self.out_table(row))
    }
    async fn clone_table_as_of(
        &self,
        from: &TableIdent,
        to: &TableIdent,
        snapshot_id: i64,
        as_of_ms: i64,
    ) -> Result<RwObject<Table>> {
        let row = self
            .inner
            .clone_table_as_of(
                &self.qualify_ident(from),
                &self.qualify_ident(to),
                snapshot_id,
                as_of_ms,
            )
            .await?;
        Ok(self.out_table(row))
    }
    // The following default-implemented trait methods take idents, so they
    // are overridden to qualify before delegating to the inner metastore's
    // own implementation.
    async fn clone_schema(&self, from: &SchemaIdent, to: &SchemaIdent) -> Result<usize> {
        self.inner
            .clone_schema(
                &self.qualify_schema_ident(from),
                &self.qualify_schema_ident(to),
            )
            .await
    }
    async fn clone_database(&self, from: &DatabaseIdent, to: &DatabaseIdent) -> Result<usize> {
        self.inner
            .clone_database(&self.qualify_database(from), &self.qualify_database(to))
            .await
    }
    async fn clone_schema_as_of(
        &self,
        from: &SchemaIdent,
        to: &SchemaIdent,
        point_ms: i64,
    ) -> Result<usize> {
        self.inner
            .clone_schema_as_of(
                &self.qualify_schema_ident(from),
                &self.qualify_schema_ident(to),
                point_ms,
            )
            .await
    }
    async fn clone_database_as_of(
        &self,
        from: &DatabaseIdent,
        to: &DatabaseIdent,
        point_ms: i64,
    ) -> Result<usize> {
        self.inner
            .clone_database_as_of(
                &self.qualify_database(from),
                &self.qualify_database(to),
                point_ms,
            )
            .await
    }
    async fn clone_table_record_as_of(
        &self,
        table: &RwObject<Table>,
        to_schema: &SchemaIdent,
        point_ms: i64,
    ) -> Result<bool> {
        let mut table = table.clone();
        table.data.ident = self.qualify_ident(&table.data.ident);
        self.inner
            .clone_table_record_as_of(&table, &self.qualify_schema_ident(to_schema), point_ms)
            .await
    }
    async fn get_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Table>>> {
        Ok(self
            .inner
            .get_table(&self.qualify_ident(ident))
            .await?
            .map(|t| self.out_table(t)))
    }
    async fn update_table(
        &self,
        ident: &TableIdent,
        update: TableUpdate,
    ) -> Result<RwObject<Table>> {
        Ok(self.out_table(
            self.inner
                .update_table(&self.qualify_ident(ident), update)
                .await?,
        ))
    }
    async fn delete_table(&self, ident: &TableIdent, cascade: bool) -> Result<()> {
        self.inner
            .delete_table(&self.qualify_ident(ident), cascade)
            .await
    }

    // ---- undrop / dropped listings ----
    async fn undrop_table(&self, ident: &TableIdent) -> Result<RwObject<Table>> {
        Ok(self.out_table(self.inner.undrop_table(&self.qualify_ident(ident)).await?))
    }
    async fn undrop_schema(&self, ident: &SchemaIdent) -> Result<RwObject<Schema>> {
        Ok(self.out_schema(
            self.inner
                .undrop_schema(&self.qualify_schema_ident(ident))
                .await?,
        ))
    }
    async fn undrop_database(&self, ident: &DatabaseIdent) -> Result<RwObject<Database>> {
        Ok(self.out_database(
            self.inner
                .undrop_database(&self.qualify_database(ident))
                .await?,
        ))
    }
    async fn list_dropped_tables(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Table>>> {
        Ok(self
            .inner
            .list_dropped_tables(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|t| self.out_table(t))
            .collect())
    }
    async fn list_dropped_schemas(
        &self,
        database: &DatabaseIdent,
    ) -> Result<Vec<(RwObject<Schema>, i64)>> {
        Ok(self
            .inner
            .list_dropped_schemas(&self.qualify_database(database))
            .await?
            .into_iter()
            .map(|(s, at)| (self.out_schema(s), at))
            .collect())
    }
    async fn list_dropped_databases(&self) -> Result<Vec<(RwObject<Database>, i64)>> {
        Ok(self
            .inner
            .list_dropped_databases()
            .await?
            .into_iter()
            .filter(|(d, _)| d.data.account == self.account)
            .map(|(d, at)| (self.out_database(d), at))
            .collect())
    }
    async fn list_dropped_tables_with_time(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<(RwObject<Table>, i64)>> {
        Ok(self
            .inner
            .list_dropped_tables_with_time(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|(t, at)| (self.out_table(t), at))
            .collect())
    }

    // ---- table storage lookups ----
    async fn table_object_store(&self, ident: &TableIdent) -> Result<Option<Arc<dyn ObjectStore>>> {
        self.inner
            .table_object_store(&self.qualify_ident(ident))
            .await
    }
    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.inner.table_exists(&self.qualify_ident(ident)).await
    }
    async fn url_for_table(&self, ident: &TableIdent) -> Result<String> {
        self.inner.url_for_table(&self.qualify_ident(ident)).await
    }
    async fn volume_for_table(&self, ident: &TableIdent) -> Result<Option<RwObject<Volume>>> {
        self.inner
            .volume_for_table(&self.qualify_ident(ident))
            .await
    }
    fn settings_config(&self) -> Option<MetastoreSettingsConfig> {
        self.inner.settings_config()
    }

    // ---- streams ----
    async fn create_stream(
        &self,
        ident: &TableIdent,
        mut stream: Stream,
    ) -> Result<RwObject<Stream>> {
        let q = self.qualify_ident(ident);
        stream.ident = self.qualify_ident(&stream.ident);
        Ok(self.out_stream(self.inner.create_stream(&q, stream).await?))
    }
    async fn get_stream(&self, ident: &TableIdent) -> Result<Option<RwObject<Stream>>> {
        Ok(self
            .inner
            .get_stream(&self.qualify_ident(ident))
            .await?
            .map(|s| self.out_stream(s)))
    }
    async fn list_streams(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Stream>>> {
        Ok(self
            .inner
            .list_streams(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|s| self.out_stream(s))
            .collect())
    }
    async fn update_stream(
        &self,
        ident: &TableIdent,
        mut stream: Stream,
    ) -> Result<RwObject<Stream>> {
        let q = self.qualify_ident(ident);
        stream.ident = self.qualify_ident(&stream.ident);
        Ok(self.out_stream(self.inner.update_stream(&q, stream).await?))
    }
    async fn delete_stream(&self, ident: &TableIdent) -> Result<()> {
        self.inner.delete_stream(&self.qualify_ident(ident)).await
    }

    // ---- tasks ----
    async fn create_task(&self, ident: &TableIdent, mut task: Task) -> Result<RwObject<Task>> {
        let q = self.qualify_ident(ident);
        task.ident = self.qualify_ident(&task.ident);
        Ok(self.out_task(self.inner.create_task(&q, task).await?))
    }
    async fn get_task(&self, ident: &TableIdent) -> Result<Option<RwObject<Task>>> {
        Ok(self
            .inner
            .get_task(&self.qualify_ident(ident))
            .await?
            .map(|t| self.out_task(t)))
    }
    async fn list_tasks(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Task>>> {
        Ok(self
            .inner
            .list_tasks(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|t| self.out_task(t))
            .collect())
    }
    async fn list_all_tasks(&self) -> Result<Vec<RwObject<Task>>> {
        Ok(self
            .inner
            .list_all_tasks()
            .await?
            .into_iter()
            .filter(|t| self.owns_database(&t.data.ident.database))
            .map(|t| self.out_task(t))
            .collect())
    }
    async fn update_task(&self, ident: &TableIdent, mut task: Task) -> Result<RwObject<Task>> {
        let q = self.qualify_ident(ident);
        task.ident = self.qualify_ident(&task.ident);
        Ok(self.out_task(self.inner.update_task(&q, task).await?))
    }
    async fn delete_task(&self, ident: &TableIdent) -> Result<()> {
        self.inner.delete_task(&self.qualify_ident(ident)).await
    }

    // ---- sequences ----
    async fn create_sequence(
        &self,
        ident: &TableIdent,
        mut sequence: Sequence,
    ) -> Result<RwObject<Sequence>> {
        let q = self.qualify_ident(ident);
        sequence.ident = self.qualify_ident(&sequence.ident);
        Ok(self.out_sequence(self.inner.create_sequence(&q, sequence).await?))
    }
    async fn get_sequence(&self, ident: &TableIdent) -> Result<Option<RwObject<Sequence>>> {
        Ok(self
            .inner
            .get_sequence(&self.qualify_ident(ident))
            .await?
            .map(|s| self.out_sequence(s)))
    }
    async fn list_sequences(&self, schema: &SchemaIdent) -> Result<Vec<RwObject<Sequence>>> {
        Ok(self
            .inner
            .list_sequences(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|s| self.out_sequence(s))
            .collect())
    }
    async fn list_all_sequences(&self) -> Result<Vec<RwObject<Sequence>>> {
        Ok(self
            .inner
            .list_all_sequences()
            .await?
            .into_iter()
            .filter(|s| self.owns_database(&s.data.ident.database))
            .map(|s| self.out_sequence(s))
            .collect())
    }
    async fn update_sequence(
        &self,
        ident: &TableIdent,
        mut sequence: Sequence,
    ) -> Result<RwObject<Sequence>> {
        let q = self.qualify_ident(ident);
        sequence.ident = self.qualify_ident(&sequence.ident);
        Ok(self.out_sequence(self.inner.update_sequence(&q, sequence).await?))
    }
    async fn delete_sequence(&self, ident: &TableIdent) -> Result<()> {
        self.inner.delete_sequence(&self.qualify_ident(ident)).await
    }

    // ---- dynamic tables ----
    async fn create_dynamic_table(
        &self,
        ident: &TableIdent,
        mut dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>> {
        let q = self.qualify_ident(ident);
        dynamic_table.ident = self.qualify_ident(&dynamic_table.ident);
        Ok(self.out_dynamic(self.inner.create_dynamic_table(&q, dynamic_table).await?))
    }
    async fn get_dynamic_table(
        &self,
        ident: &TableIdent,
    ) -> Result<Option<RwObject<DynamicTable>>> {
        Ok(self
            .inner
            .get_dynamic_table(&self.qualify_ident(ident))
            .await?
            .map(|d| self.out_dynamic(d)))
    }
    async fn list_dynamic_tables(
        &self,
        schema: &SchemaIdent,
    ) -> Result<Vec<RwObject<DynamicTable>>> {
        Ok(self
            .inner
            .list_dynamic_tables(&self.qualify_schema_ident(schema))
            .await?
            .into_iter()
            .map(|d| self.out_dynamic(d))
            .collect())
    }
    async fn list_all_dynamic_tables(&self) -> Result<Vec<RwObject<DynamicTable>>> {
        Ok(self
            .inner
            .list_all_dynamic_tables()
            .await?
            .into_iter()
            .filter(|d| self.owns_database(&d.data.ident.database))
            .map(|d| self.out_dynamic(d))
            .collect())
    }
    async fn update_dynamic_table(
        &self,
        ident: &TableIdent,
        mut dynamic_table: DynamicTable,
    ) -> Result<RwObject<DynamicTable>> {
        let q = self.qualify_ident(ident);
        dynamic_table.ident = self.qualify_ident(&dynamic_table.ident);
        Ok(self.out_dynamic(self.inner.update_dynamic_table(&q, dynamic_table).await?))
    }
    async fn delete_dynamic_table(&self, ident: &TableIdent) -> Result<()> {
        self.inner
            .delete_dynamic_table(&self.qualify_ident(ident))
            .await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::metastore::InMemoryMetastore;
    use crate::models::table::TableFormat;
    use crate::models::volumes::VolumeType;
    use iceberg_rust_spec::{
        schema::Schema as IcebergSchema,
        types::{PrimitiveType, StructField, Type},
    };

    fn iceberg_schema() -> IcebergSchema {
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

    fn db(name: &str) -> Database {
        Database {
            ident: name.to_string(),
            properties: None,
            volume: "vol".to_string(),
            should_refresh: false,
            account: DEFAULT_DATABASE_ACCOUNT.to_string(),
        }
    }

    async fn shared() -> Arc<dyn Metastore> {
        let ms = InMemoryMetastore::new();
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .expect("volume");
        Arc::new(ms)
    }

    async fn create_table(ms: &AccountedMetastore, d: &str, s: &str, t: &str) {
        let ident = TableIdent::new(d, s, t);
        ms.create_table(
            &ident,
            TableCreateRequest {
                ident: ident.clone(),
                properties: None,
                format: Some(TableFormat::Iceberg),
                location: None,
                schema: iceberg_schema(),
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

    #[test]
    fn qualify_strip_roundtrip() {
        let inner: Arc<dyn Metastore> = Arc::new(InMemoryMetastore::new());
        let acct = AccountedMetastore::new(inner.clone(), "acme");
        assert_eq!(acct.qualify_database("db"), "acme/db");
        // idempotent
        assert_eq!(acct.qualify_database("acme/db"), "acme/db");
        assert_eq!(acct.strip_database("acme/db"), "db");
        assert_eq!(acct.strip_database("db"), "db");
        let t = TableIdent::new("db", "s", "t");
        let q = acct.qualify_ident(&t);
        assert_eq!(q, TableIdent::new("acme/db", "s", "t"));
        assert_eq!(acct.strip_table_ident(&q), t);
        let s = SchemaIdent::new("db".into(), "s".into());
        assert_eq!(
            acct.qualify_schema_ident(&s),
            SchemaIdent::new("acme/db".into(), "s".into())
        );
        // default account: identity
        let def = AccountedMetastore::new(inner, DEFAULT_DATABASE_ACCOUNT);
        assert_eq!(def.qualify_database("db"), "db");
        assert_eq!(def.strip_database("acme/db"), "acme/db");
    }

    #[tokio::test]
    async fn accounts_are_isolated_over_one_metastore() {
        let raw = shared().await;
        let a = AccountedMetastore::new(raw.clone(), "acme");
        let b = AccountedMetastore::new(raw.clone(), "globex");
        let def = AccountedMetastore::new(raw.clone(), DEFAULT_DATABASE_ACCOUNT);

        // Same display name in all three accounts: no collision.
        let rec = a
            .create_database(&"sales".to_string(), db("sales"))
            .await
            .unwrap();
        assert_eq!(rec.data.ident, "sales");
        assert_eq!(rec.data.account, "acme");
        b.create_database(&"sales".to_string(), db("sales"))
            .await
            .unwrap();
        def.create_database(&"sales".to_string(), db("sales"))
            .await
            .unwrap();
        assert!(
            a.create_database(&"sales".to_string(), db("sales"))
                .await
                .is_err()
        );

        // State keys are qualified for non-default accounts only.
        assert!(
            raw.get_database(&"acme/sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            raw.get_database(&"globex/sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            raw.get_database(&"sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(raw.list_databases().await.unwrap().len(), 3);

        // Each view lists only its own database, with the display ident.
        for (view, account) in [(&a, "acme"), (&b, "globex"), (&def, "embucket")] {
            let dbs = view.list_databases().await.unwrap();
            assert_eq!(dbs.len(), 1, "{account}");
            assert_eq!(dbs[0].data.ident, "sales");
            assert_eq!(dbs[0].data.account, account);
            let got = view
                .get_database(&"sales".to_string())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(got.data.ident, "sales");
        }

        // Schemas + tables round-trip with stripped idents.
        let sid = SchemaIdent::new("sales".into(), "public".into());
        let schema = a
            .create_schema(
                &sid,
                Schema {
                    ident: sid.clone(),
                    properties: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(schema.data.ident, sid);
        create_table(&a, "sales", "public", "orders").await;
        let tid = TableIdent::new("sales", "public", "orders");
        let got = a.get_table(&tid).await.unwrap().unwrap();
        assert_eq!(got.data.ident, tid);
        let listed = a.list_tables(&sid).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].data.ident, tid);
        assert!(a.table_exists(&tid).await.unwrap());

        // The other account (same display names) sees nothing.
        assert!(b.get_table(&tid).await.unwrap().is_none());
        assert!(!b.table_exists(&tid).await.unwrap());
        assert!(
            b.list_schemas(&"sales".to_string())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(def.get_table(&tid).await.unwrap().is_none());

        // Raw state holds the qualified ident.
        let raw_tid = TableIdent::new("acme/sales", "public", "orders");
        let raw_rec = raw.get_table(&raw_tid).await.unwrap().unwrap();
        assert_eq!(raw_rec.data.ident, raw_tid);

        // Clone within the account, then drop (tombstone) + undrop.
        let cloned = TableIdent::new("sales", "public", "orders_copy");
        let rec = a.clone_table(&tid, &cloned).await.unwrap();
        assert_eq!(rec.data.ident, cloned);
        a.delete_table(&cloned, false).await.unwrap();
        let dropped = a.list_dropped_tables(&sid).await.unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].data.ident, cloned);
        let restored = a.undrop_table(&cloned).await.unwrap();
        assert_eq!(restored.data.ident, cloned);

        // Database clone stays inside the account.
        a.clone_database(&"sales".to_string(), &"sales2".to_string())
            .await
            .unwrap();
        let names: Vec<_> = a
            .list_databases()
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.data.ident)
            .collect();
        assert!(names.contains(&"sales2".to_string()));
        assert!(
            !b.list_databases()
                .await
                .unwrap()
                .iter()
                .any(|d| d.data.ident == "sales2")
        );
        assert!(
            a.get_table(&TableIdent::new("sales2", "public", "orders"))
                .await
                .unwrap()
                .is_some()
        );

        // Drop the database in one account leaves the others intact.
        a.delete_database(&"sales".to_string(), true).await.unwrap();
        assert!(
            a.get_database(&"sales".to_string())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            b.get_database(&"sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        let dropped = a.list_dropped_databases().await.unwrap();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].0.data.ident, "sales");
        assert!(b.list_dropped_databases().await.unwrap().is_empty());
        a.undrop_database(&"sales".to_string()).await.unwrap();
        assert!(
            a.get_database(&"sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn default_account_is_passthrough() {
        let raw = shared().await;
        let def = AccountedMetastore::new(raw.clone(), DEFAULT_DATABASE_ACCOUNT);
        def.create_database(&"d".to_string(), db("d"))
            .await
            .unwrap();
        // Visible under the bare key in the raw metastore.
        assert!(raw.get_database(&"d".to_string()).await.unwrap().is_some());
        let sid = SchemaIdent::new("d".into(), "s".into());
        def.create_schema(
            &sid,
            Schema {
                ident: sid.clone(),
                properties: None,
            },
        )
        .await
        .unwrap();
        create_table(&def, "d", "s", "t").await;
        let raw_rec = raw
            .get_table(&TableIdent::new("d", "s", "t"))
            .await
            .unwrap();
        assert!(raw_rec.is_some());
    }
}
