use datafusion::error::Result as DFResult;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

/// No-op placeholder for the former DataFusion 50 rule that installed a
/// case-insensitive [`datafusion::datasource::schema_adapter::SchemaAdapterFactory`]
/// on every Parquet scan, normalizing file field names to lowercase before
/// delegating to the default adapter.
///
/// DataFusion 55 removed `SchemaAdapterFactory` / `SchemaAdapter` / `SchemaMapper`
/// (and the batch-level file-schema mapping hook they provided) in favor of
/// `PhysicalExprAdapterFactory`, which only rewrites filter/projection
/// *expressions* — it cannot rename file columns at the batch level. There is
/// currently no equivalent extension point on the Parquet read path, so the
/// rule is temporarily a no-op.
///
/// Follow-up (ADE backlog): reimplement case-insensitive Parquet field-name
/// matching on DataFusion 55 — e.g. via a custom `FileSource`/schema coercion
/// or by normalizing field names when writing files.
#[derive(Default, Debug)]
pub struct CaseInsensitiveSchemaDataSourceExec;

impl CaseInsensitiveSchemaDataSourceExec {
    pub const fn new() -> Self {
        Self
    }
}

impl datafusion::physical_optimizer::PhysicalOptimizerRule
    for CaseInsensitiveSchemaDataSourceExec
{
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // No-op on DataFusion 55: see type-level docs.
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "CaseInsensitiveSchemaDataSourceExec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("Id", DataType::Int32, false)]))
    }

    #[tokio::test]
    async fn test_is_noop_on_parquet_source() -> DFResult<()> {
        let object_store_url = datafusion::datasource::object_store::ObjectStoreUrl::parse(
            "s3://bucket",
        )?;
        let file_source = Arc::new(ParquetSource::new(schema()));
        let file_scan_config = Arc::new(
            FileScanConfigBuilder::new(object_store_url, file_source)
                .with_file_groups(vec![FileGroup::new(vec![
                    datafusion::datasource::listing::PartitionedFile::new("path", 1),
                ])])
                .build(),
        );

        let data_source_exec: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(file_scan_config));
        let rule = CaseInsensitiveSchemaDataSourceExec::new();
        let optimized = rule.optimize(Arc::clone(&data_source_exec), &ConfigOptions::default())?;

        // The rule must leave the plan untouched (no error, no transformation).
        assert!(Arc::ptr_eq(&optimized, &data_source_exec));

        Ok(())
    }
}
