use crate::block_on_with_timeout;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion_common::DataFusionError;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::error::Error as IcebergError;
use iceberg_rust_spec::namespace::Namespace;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

/// Local replacement for `datafusion_iceberg::catalog::schema::IcebergSchema`,
/// whose constructor is `pub(crate)` in the `iceberg-rust` fork.
///
/// Unlike the fork type, which delegates to the fork-internal `Mirror` cache,
/// this goes straight to the underlying [`Catalog`]. [`CachingSchema`] above it
/// already provides caching, so the extra mirror layer is not required.
#[derive(Debug)]
pub struct IcebergSchema {
    schema: Namespace,
    catalog: Arc<dyn Catalog>,
    timeout: Duration,
}

impl IcebergSchema {
    pub fn new(schema: Namespace, catalog: Arc<dyn Catalog>, timeout: Duration) -> Self {
        Self {
            schema,
            catalog,
            timeout,
        }
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchema {
    fn table_names(&self) -> Vec<String> {
        let catalog = self.catalog.clone();
        let namespace = self.schema.clone();

        #[allow(clippy::expect_used)]
        block_on_with_timeout(
            async move { catalog.list_tabulars(&namespace).await },
            self.timeout,
        )
        .expect("Catalog timeout on: list_tabulars")
        .map(|tables| tables.iter().map(|id| id.name().to_owned()).collect())
        .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let identifier = Identifier::try_new(
            &[self.schema.deref(), &[name.to_string()]].concat(),
            None,
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        let tabular = match self.catalog.clone().load_tabular(&identifier).await {
            Ok(tabular) => tabular,
            Err(IcebergError::CatalogNotFound) => return Ok(None),
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };

        // Branch is always `None` for catalogs built in this crate
        // (`DataFusionIcebergCatalog::new_sync(catalog, None)`), so there is no
        // ref endpoint to resolve.
        Ok(Some(Arc::new(DataFusionTable::new(
            tabular, None, None, None,
        )) as Arc<dyn TableProvider>))
    }

    fn table_exist(&self, name: &str) -> bool {
        let Ok(identifier) =
            Identifier::try_new(&[self.schema.deref(), &[name.to_string()]].concat(), None)
        else {
            return false;
        };
        let catalog = self.catalog.clone();

        #[allow(clippy::expect_used)]
        block_on_with_timeout(
            async move { catalog.tabular_exists(&identifier).await },
            self.timeout,
        )
        .expect("Catalog timeout on: tabular_exists")
        .unwrap_or(false)
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Table registration flows through the metastore / `EmbucketSchema`
        // path, not through iceberg REST catalogs.
        Ok(None)
    }

    fn deregister_table(&self, _name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Table deregistration flows through the metastore /
        // `EmbucketSchema` path, not through iceberg REST catalogs.
        Ok(None)
    }
}
