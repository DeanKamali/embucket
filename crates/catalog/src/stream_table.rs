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
// Metadata columns are lowercase: DataFusion normalizes unquoted
// identifiers to lowercase, so `select metadata$action` (the common,
// unquoted form) resolves. Quoted `"metadata$action"` matches too;
// quoted uppercase is not supported in v1.
pub const METADATA_ACTION_COLUMN: &str = "metadata$action";
pub const METADATA_ISUPDATE_COLUMN: &str = "metadata$isupdate";
pub const METADATA_ROW_ID_COLUMN: &str = "metadata$row_id";

/// Builds a stream provider for `stream`, verifying the source exists and
/// the offset snapshot is still retained (staleness, §18.1-1).
pub async fn stream_table_provider(
    metastore: &Arc<dyn Metastore>,
    iceberg_catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
    stream: &RwObject<Stream>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    // NOTE: the resolved provider is cached by `CachingSchema` (catalog
    // list), so `StreamTable::scan` re-resolves the stream record and the
    // source's current snapshot from the metastore on every scan — the
    // change range must be fresh per scan, not baked in at resolution
    // time (offset advance on consume + new source commits).
    let inner = build_inner_stream_scan(metastore, &iceberg_catalog, stream).await?;
    Ok(Arc::new(StreamTable::new(
        metastore.clone(),
        iceberg_catalog,
        stream.clone(),
        inner,
    )))
}

/// Builds the fork's changed-file scan for `stream`: verifies the source
/// exists and the offset snapshot is still retained (staleness, §18.1-1),
/// then wraps the source table in `DataFusionTable` over the snapshot
/// range `(offset, current]` with `scan_changed_data_files(true)`.
async fn build_inner_stream_scan(
    metastore: &Arc<dyn Metastore>,
    iceberg_catalog: &Arc<dyn iceberg_rust::catalog::Catalog>,
    stream: &RwObject<Stream>,
) -> Result<Arc<DataFusionTable>, DataFusionError> {
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
        Arc::clone(iceberg_catalog),
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
        // File row positions sharpen ROW_ID to `<path>#<pos>` when present;
        // manifest paths are not needed by the stream projection.
        .enable_data_file_row_position_column(true)
        .enable_manifest_file_path_column(false)
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(Arc::new(DataFusionTable::new_with_config(
        IcebergTabular::Table(iceberg_table),
        stream.offset_snapshot,
        end,
        None,
        Some(config),
    )))
}

/// Wraps the fork's changed-file scan and projects the Snowflake stream
/// shape. `append_only` streams drop DELETE rows entirely (§18.1-2).
///
/// The resolved provider is cached by `CachingSchema`, so the stream
/// record and source snapshot range are re-resolved from the metastore
/// on **every scan**: consumption advances the stream offset, and the
/// source keeps committing new snapshots.
#[derive(Debug)]
pub struct StreamTable {
    metastore: Arc<dyn Metastore>,
    iceberg_catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
    stream: RwObject<Stream>,
    append_only: bool,
    schema: SchemaRef,
    resolve_schema: SchemaRef,
    has_status_column: bool,
    has_file_path_column: bool,
}

impl StreamTable {
    fn new(
        metastore: Arc<dyn Metastore>,
        iceberg_catalog: Arc<dyn iceberg_rust::catalog::Catalog>,
        stream: RwObject<Stream>,
        inner: Arc<DataFusionTable>,
    ) -> Self {
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
        // Resolution schema for the projection expressions: the inner scan
        // keeps its change columns (status/path) even though the declared
        // stream schema drops them — `METADATA$ACTION` is derived from the
        // status column, so exprs must resolve against the merged set.
        let mut resolve_fields: Vec<Field> = inner_schema.fields().iter().map(|f| (**f).clone()).collect();
        resolve_fields.push(Field::new(METADATA_ACTION_COLUMN, DataType::Utf8, false));
        resolve_fields.push(Field::new(METADATA_ISUPDATE_COLUMN, DataType::Boolean, false));
        resolve_fields.push(Field::new(METADATA_ROW_ID_COLUMN, DataType::Utf8, true));
        let append_only = stream.append_only;
        Self {
            metastore,
            iceberg_catalog,
            stream,
            append_only,
            schema: Arc::new(ArrowSchema::new(fields)),
            resolve_schema: Arc::new(ArrowSchema::new(resolve_fields)),
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
        // Re-resolve per scan (see struct docs): pick up the current
        // offset (advanced by DML consumption) and the source's current
        // snapshot (new commits since the provider was cached).
        let stream = self
            .metastore
            .get_stream(&self.stream.ident)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .ok_or_else(|| {
                DataFusionError::External(Box::new(
                    metastore_error::StreamNotFoundSnafu {
                        stream: self.stream.ident.table.clone(),
                        schema: self.stream.ident.schema.clone(),
                        db: self.stream.ident.database.clone(),
                    }
                    .build(),
                ))
            })?;
        let inner = build_inner_stream_scan(
            &self.metastore,
            &self.iceberg_catalog,
            &stream,
        )
        .await?;
        let inner_exec = inner.scan(session, None, &[], None).await?;

        // APPEND_ONLY streams surface only added files (§18.1-2): filter on
        // the inner change-status column BEFORE the projection drops it.
        // For standard streams, wrap in a no-op `TRUE` filter: the inner
        // changed-file scan reports Exact statistics that count only added
        // files, which lets DataFusion's aggregate rule replace COUNT(*)
        // with a placeholder that undercounts (DELETE rows from rewritten
        // files are invisible to those statistics). The filter degrades
        // statistics to inexact so aggregates execute against real rows.
        let mut plan: Arc<dyn ExecutionPlan> = if self.append_only && self.has_status_column {
            let predicate = self.physical(
                &col(CHANGE_FILE_STATUS_COLUMN).eq(lit("ADDED")),
                &inner_exec.schema(),
            )?;
            Arc::new(FilterExec::try_new(predicate, inner_exec)?)
        } else {
            let predicate = self.physical(&lit(true), &inner_exec.schema())?;
            Arc::new(FilterExec::try_new(predicate, inner_exec)?)
        };

        // Projection onto the declared stream schema (source columns in
        // order, then the three METADATA$ columns).
        let props_exprs = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let expr = self.output_expr(field.name());
                let physical = self.physical(&expr, &self.resolve_schema)?;
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