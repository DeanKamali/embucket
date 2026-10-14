//! Stream table provider (design §18, M11).
//!
//! A stream resolves to a scan of its source table restricted to the
//! snapshot range `(offset, current]` with `scan_changed_data_files(true)`,
//! projected onto the Snowflake stream shape: source columns plus
//! `METADATA$ACTION` (INSERT/DELETE), `METADATA$ISUPDATE`, and
//! `METADATA$ROW_ID`. Changes are derived lazily from the fork's
//! `change_manifests` at plan time — nothing is materialized (§18.1-1).

use catalog_metastore::error as metastore_error;
use catalog_metastore::models::Stream;
use catalog_metastore::{Metastore, RwObject};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, col, lit};
use datafusion_iceberg::table::{
    CHANGE_FILE_STATUS_COLUMN, DataFusionTable, DataFusionTableConfigBuilder,
};
use iceberg_rust::catalog::tabular::Tabular as IcebergTabular;
use iceberg_rust::table::Table as IcebergTable;
use std::collections::HashMap;
use std::sync::Arc;

/// The fork's data-file path metadata column (private constant upstream —
/// same string value; kept local so the stream projection can reference it).
const DATA_FILE_PATH_COLUMN: &str = "__data_file_path";

/// Snowflake stream metadata columns.
pub const METADATA_ACTION_COLUMN: &str = "METADATA$ACTION";
pub const METADATA_ISUPDATE_COLUMN: &str = "METADATA$ISUPDATE";
pub const METADATA_ROW_ID_COLUMN: &str = "METADATA$ROW_ID";

/// Builds a stream provider for `stream`, verifying the source exists and
/// the offset snapshot is still retained (staleness, §18.1-1).
pub async fn stream_table_provider(
    metastore: &Arc<dyn Metastore>,
    iceberg_catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
    stream: &RwObject<Stream>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let source_ident = stream.source.clone();
    let source = metastore
        .get_table(&source_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .ok_or_else(|| {
            DataFusionError::External(Box::new(
                metastore_error::TableNotFoundSnafu {
                    table: source_ident.table.clone(),
                    schema: source_ident.schema.clone(),
                    db: source_ident.database.clone(),
                }
                .build(),
            ))
        })?;

    let metadata = source.data.metadata.clone();
    if let Some(offset) = stream.offset_snapshot {
        if !metadata.snapshots.contains_key(&offset) {
            return Err(DataFusionError::External(Box::new(
                metastore_error::StreamStaleSnafu {
                    stream: stream.ident.table.clone(),
                    snapshot: offset,
                }
                .build(),
            )));
        }
    }

    let object_store = metastore
        .table_object_store(&source_ident)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .ok_or_else(|| {
            DataFusionError::External(Box::new(
                metastore_error::TableObjectStoreNotFoundSnafu {
                    table: source_ident.table.clone(),
                    schema: source_ident.schema.clone(),
                    db: source_ident.database.clone(),
                }
                .build(),
            ))
        })?;

    let iceberg_table = IcebergTable::new(
        source_ident.to_iceberg_ident(),
        iceberg_catalog,
        object_store,
        metadata,
    )
    .await
    .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // End of the change range: the source's current snapshot. `None` when
    // the source has no snapshots yet — the scan yields no rows.
    let end = iceberg_table
        .metadata()
        .current_snapshot(None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .map(|snapshot| *snapshot.snapshot_id());

    let config = DataFusionTableConfigBuilder::default()
        .scan_changed_data_files(true)
        // ROW_ID v1 is the source data-file path (§18.1-2): stable per file,
        // without requiring row-lineage first-row-ids from the writer.
        .enable_data_file_path_column(true)
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let inner = Arc::new(DataFusionTable::new_with_config(
        IcebergTabular::Table(iceberg_table),
        stream.offset_snapshot,
        end,
        None,
        Some(config),
    ));

    Ok(Arc::new(StreamTable::new(inner, stream.append_only)))
}

/// Wraps the fork's changed-file scan and projects the Snowflake stream
/// shape. `append_only` streams drop DELETE rows entirely (§18.1-2).
#[derive(Debug)]
pub struct StreamTable {
    inner: Arc<DataFusionTable>,
    append_only: bool,
    schema: SchemaRef,
    has_status_column: bool,
    has_file_path_column: bool,
}

impl StreamTable {
    fn new(inner: Arc<DataFusionTable>, append_only: bool) -> Self {
        let inner_schema = inner.schema();
        let mut fields: Vec<Field> = Vec::new();
        let mut has_status_column = false;
        let mut has_file_path_column = false;
        for field in inner_schema.fields() {
            match field.name().as_str() {
                CHANGE_FILE_STATUS_COLUMN => has_status_column = true,
                DATA_FILE_PATH_COLUMN => has_file_path_column = true,
                _ => fields.push((**field).clone()),
            }
        }
        fields.push(Field::new(METADATA_ACTION_COLUMN, DataType::Utf8, false));
        fields.push(Field::new(
            METADATA_ISUPDATE_COLUMN,
            DataType::Boolean,
            false,
        ));
        fields.push(Field::new(METADATA_ROW_ID_COLUMN, DataType::Utf8, true));
        Self {
            inner,
            append_only,
            schema: Arc::new(ArrowSchema::new(fields)),
            has_status_column,
            has_file_path_column,
        }
    }

    /// Logical expression producing one declared output column from the
    /// inner scan schema (unqualified). Metadata columns degrade to
    /// constants when the fork's change columns are absent.
    fn output_expr(&self, name: &str) -> Expr {
        match name {
            METADATA_ACTION_COLUMN => {
                if self.has_status_column {
                    let status_col = col(CHANGE_FILE_STATUS_COLUMN);
                    datafusion_expr::when(
                        status_col.clone().eq(lit("ADDED")),
                        lit("INSERT"),
                    )
                    .otherwise(lit("DELETE"))
                    .expect("case expression builds")
                } else {
                    lit("INSERT")
                }
            }
            METADATA_ISUPDATE_COLUMN => lit(false),
            METADATA_ROW_ID_COLUMN => {
                if self.has_file_path_column {
                    col(DATA_FILE_PATH_COLUMN)
                } else {
                    Expr::Literal(ScalarValue::Utf8(None), None)
                }
            }
            _ => col(name),
        }
    }

    fn physical(
        &self,
        expr: &Expr,
        schema: &ArrowSchema,
    ) -> Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>, DataFusionError> {
        let df_schema = datafusion_common::DFSchema::from_unqualified_fields(
            schema.fields().clone(),
            HashMap::new(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        create_physical_expr(
            expr,
            &df_schema,
            &ExecutionProps::new(),
            &PhysicalPlanningContext::default(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[async_trait]
impl TableProvider for StreamTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let inner_exec = self.inner.scan(session, None, &[], None).await?;

        // APPEND_ONLY streams surface only added files (§18.1-2): filter on
        // the inner change-status column BEFORE the projection drops it.
        let mut plan: Arc<dyn ExecutionPlan> = if self.append_only && self.has_status_column {
            let predicate = self.physical(
                &col(CHANGE_FILE_STATUS_COLUMN).eq(lit("ADDED")),
                &inner_exec.schema(),
            )?;
            Arc::new(FilterExec::try_new(predicate, inner_exec)?)
        } else {
            inner_exec
        };

        // Projection onto the declared stream schema (source columns in
        // order, then the three METADATA$ columns).
        let props_exprs = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let expr = self.output_expr(field.name());
                let physical = self.physical(&expr, &self.schema)?;
                Ok((physical, field.name().clone()))
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        plan = Arc::new(ProjectionExec::try_new(props_exprs, plan)?);

        // Narrow the declared-schema output to the requested projection.
        if let Some(indices) = projection {
            let exprs = indices
                .iter()
                .map(|&idx| {
                    let field = self.schema.field(idx);
                    let physical =
                        self.physical(&col(field.name()), &self.schema)?;
                    Ok((physical, field.name().clone()))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }

        if let Some(limit) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit)));
        }

        // Filters are evaluated above the scan (correctness over pushdown
        // in v1); DataFusion wraps them into a filter node itself when the
        // scan does not report pushdown support.
        let _ = filters;
        Ok(plan)
    }
}