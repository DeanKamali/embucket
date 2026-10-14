use crate::catalog::CatalogConfig;
use crate::{block_on_with_timeout, error};
use async_trait::async_trait;
use catalog_metastore::error as metastore_error;
use catalog_metastore::{Metastore, SchemaIdent, TableFormat, TableIdent};
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion_common::DataFusionError;
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::{catalog::tabular::Tabular as IcebergTabular, table::Table as IcebergTable};
use snafu::ResultExt;
use std::sync::Arc;
use tracing::error;

/// Builds a `ListingTable` provider over the parquet files stored at the
/// table's registered location, using the schema captured at `CREATE
/// EXTERNAL TABLE` time.
///
/// Parquet-format metastore tables have no Iceberg snapshots, so resolving
/// them through the Iceberg path would yield an empty scan.
fn parquet_listing_provider(
    table: &catalog_metastore::Table,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let location = table
        .volume_location
        .clone()
        .or_else(|| Some(table.metadata.location.clone()))
        .filter(|loc| !loc.is_empty())
        .ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Parquet table '{}' has no location registered",
                table.ident.table
            )
            .into())
        })?;

    let table_path = ListingTableUrl::parse(&location)?;
    let schema_ref = table
        .metadata
        .current_schema()
        .map_err(|err| DataFusionError::External(Box::new(err)))?;
    // `Schema` derefs to `StructType`, which implements `TryInto<ArrowSchema>`
    let schema: ArrowSchema = {
        let struct_type: &iceberg_rust_spec::spec::types::StructType = schema_ref;
        struct_type
            .try_into()
            .map_err(|err| DataFusionError::External(Box::new(err)))?
    };

    let file_format = Arc::new(ParquetFormat::default()) as Arc<dyn FileFormat>;
    let listing_options = ListingOptions::new(file_format);
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(Arc::new(schema));
    let listing_table = ListingTable::try_new(config)?;

    Ok(Arc::new(listing_table) as Arc<dyn TableProvider>)
}

pub struct EmbucketSchema {
    pub database: String,
    pub schema: String,
    pub metastore: Arc<dyn Metastore>,
    pub iceberg_catalog: Arc<dyn IcebergCatalog>,
    pub config: CatalogConfig,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for EmbucketSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DFSchema")
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("metastore", &"")
            .field("iceberg_catalog", &"")
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for EmbucketSchema {
    #[tracing::instrument(
        name = "EmbucketSchema::table_names",
        level = "debug",
        skip(self),
        fields(tables_names_count, schema_name=format!("{}.{}", self.database, self.schema))
    )]
    fn table_names(&self) -> Vec<String> {
        let metastore = self.metastore.clone();
        let database = self.database.clone();
        let schema = self.schema.clone();

        #[allow(clippy::expect_used)]
        let table_names = block_on_with_timeout(
            async move {
                metastore
                    .list_tables(&SchemaIdent::new(database, schema))
                    .await
                    .map(|tables| tables.into_iter().map(|s| s.ident.table.clone()).collect())
                    .context(error::MetastoreSnafu)
            },
            self.config.catalog_timeout(),
        )
        .expect("Catalog timeout on: list_tables")
        .unwrap_or_else(|error| {
            error!(?error, "Failed to list tables; returning empty list");
            vec![]
        });
        // Record the result as part of the current span.
        tracing::Span::current().record("tables_names_count", table_names.len());

        table_names
    }

    #[tracing::instrument(name = "EmbucketSchema::table", level = "debug", skip(self), err)]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let ident = &TableIdent::new(&self.database.clone(), &self.schema.clone(), name);

        // Streams (design §18.1-2, M11): resolved before the table lookup.
        // A stream is not an Iceberg table — it scans its source's change
        // feed for the snapshot range (offset, current].
        if let Some(stream) = self
            .metastore
            .get_stream(ident)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            return crate::stream_table::stream_table_provider(
                &self.metastore,
                self.iceberg_catalog.clone(),
                &stream,
            )
            .await
            .map(Some);
        }

        let object_store = self
            .metastore
            .table_object_store(ident)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                DataFusionError::External(Box::new(
                    metastore_error::TableObjectStoreNotFoundSnafu {
                        table: ident.table.clone(),
                        schema: ident.schema.clone(),
                        db: ident.database.clone(),
                    }
                    .build(),
                ))
            })?;
        match self.metastore.get_table(ident).await {
            Ok(Some(table)) => {
                let table_provider: Arc<dyn TableProvider> = match table.data.format {
                    TableFormat::Parquet => parquet_listing_provider(&table.data)?,
                    TableFormat::Iceberg => {
                        let iceberg_table = IcebergTable::new(
                            ident.to_iceberg_ident(),
                            self.iceberg_catalog.clone(),
                            object_store,
                            table.metadata.clone(),
                        )
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        let tabular = IcebergTabular::Table(iceberg_table);
                        Arc::new(IcebergDataFusionTable::new(tabular, None, None, None))
                    }
                };
                Ok(Some(table_provider))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    #[tracing::instrument(name = "EmbucketSchema::table_exist", level = "debug", skip(self))]
    fn table_exist(&self, name: &str) -> bool {
        let iceberg_catalog = self.iceberg_catalog.clone();
        let database = self.database.clone();
        let schema = self.schema.clone();
        let table = name.to_string();
        let ident = TableIdent::new(&database, &schema, &table);
        let ident_for_runtime = ident.clone();

        #[allow(clippy::expect_used)]
        block_on_with_timeout(
            async move {
                iceberg_catalog
                    .tabular_exists(&ident_for_runtime.to_iceberg_ident())
                    .await
                    .context(error::IcebergSnafu)
            },
            self.config.catalog_timeout(),
        )
        .expect("Catalog timeout on: tabular_exists")
        .unwrap_or_else(|error| {
            error!(
                ?error,
                schema_name = %ident.schema,
                table_name = %ident.table,
                "Failed to check table existence; assuming missing",
            );
            false
        })
    }
}
