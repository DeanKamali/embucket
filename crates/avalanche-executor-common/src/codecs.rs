//! DataFusion proto codecs bridging Embucket and Ballista.
//!
//! Both codecs delegate structural (de)serialization to Ballista's codecs and
//! add what Embucket needs on top:
//! - UDF resolution by name (encoded plans carry only names; the remote side
//!   rebuilds the full `ScalarUDF`/`AggregateUDF`/`WindowUDF` instances from
//!   the [`FunctionMaps`] catalog collected from the `functions` crate).
//! - Managed-Iceberg table scans: the router encodes each managed table's
//!   snapshot metadata + volume storage config (`ManagedIcebergPayload`);
//!   the executor rebuilds a read-only `DataFusionTable` from the payload
//!   (no metastore/REST-catalog access needed remotely).
//! - LATERAL FLATTEN scans (`FlattenPayload`): the router pre-builds the
//!   provider's input plan (a scan over the input table projecting the input
//!   expression) and encodes it; the executor rebuilds the provider with the
//!   decoded plan so `FlattenExec` never needs session catalog resolution.
//! - A loud error if an Embucket write node (e.g. the MERGE INTO sink) ever
//!   reaches the distributed path — in v1 all writes stay local.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, Extension, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::ScalarValue;
use datafusion_common::TableReference;
use datafusion_iceberg::DataFusionTable;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::{AsLogicalPlan, LogicalExtensionCodec};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::{LogicalExprNode, LogicalPlanNode};
use futures::executor::block_on;
use functions::json::PathToken;
use functions::table::flatten::provider::{FlattenArgs, FlattenMode, FlattenTableProvider};
use iceberg_rust::catalog::identifier::Identifier as IcebergIdentifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::error::Error as IcebergError;
use iceberg_rust::table::Table as IcebergTable;
use iceberg_rust_spec::spec::table_metadata::TableMetadata;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::udf_registry::FunctionMaps;
use catalog::table::CachingTable;

/// Magic prefix marking an Avalanche-encoded table provider payload, so
/// decode can distinguish it from Ballista's protobuf table encodings.
pub const TABLE_PAYLOAD_MAGIC: &[u8] = b"AVALANCHE-TABLE-V1\n";

/// Storage configuration for a managed table's volume, embedded in the plan
/// payload so the executor can rebuild the table's object store (M7.1-style
/// inline credentials; executors are scheduler-scoped and trust the router).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ManagedVolumeSpec {
    S3 {
        region: Option<String>,
        bucket: String,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        allow_http: bool,
    },
    File {
        path: String,
    },
}

impl ManagedVolumeSpec {
    /// Rebuilds the volume's `ObjectStore` on the executor side.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be constructed (bad credentials,
    /// missing filesystem path, ...).
    pub fn to_object_store(&self) -> datafusion_common::Result<Arc<dyn ObjectStore>> {
        match self {
            Self::S3 {
                region,
                bucket,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket.clone())
                    .with_conditional_put(S3ConditionalPut::ETagMatch);
                if let Some(region) = region {
                    builder = builder.with_region(region.clone());
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint.clone());
                    builder = builder.with_allow_http(*allow_http);
                }
                if let Some(access_key_id) = access_key_id {
                    builder = builder.with_access_key_id(access_key_id.clone());
                }
                if let Some(secret_access_key) = secret_access_key {
                    builder = builder.with_secret_access_key(secret_access_key.clone());
                }
                if let Some(session_token) = session_token {
                    builder = builder.with_token(session_token.clone());
                }
                builder
                    .build()
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "failed to build S3 object store for encoded managed table \
                             (bucket {bucket}): {e}"
                        ))
                    })
            }
            Self::File { path } => {
                std::fs::create_dir_all(path).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to create filesystem volume path {path} for encoded \
                         managed table: {e}"
                    ))
                })?;
                LocalFileSystem::new_with_prefix(path)
                    .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "failed to build filesystem object store for encoded managed \
                             table (path {path}): {e}"
                        ))
                    })
            }
        }
    }
}

/// Self-contained payload for rebuilding a managed-Iceberg table scan on an
/// executor: the snapshot's table metadata (serialized the same way as
/// Iceberg metadata JSON), the snapshot range / branch of the scan, and the
/// volume storage config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedIcebergPayload {
    pub namespace: Vec<String>,
    pub name: String,
    pub metadata: serde_json::Value,
    pub snapshot_start: Option<i64>,
    pub snapshot_end: Option<i64>,
    pub branch: Option<String>,
    pub volume: ManagedVolumeSpec,
}

/// JSON-friendly path token for FLATTEN payloads (`functions::json::PathToken`
/// lives in a crate-private module, so it travels as this DTO).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PathTokenDto {
    Key(String),
    Index(usize),
}

/// FLATTEN input: either a router-encoded input expression (no table
/// reference — evaluated as a scalar expression on the executor) or the full
/// input plan (a scan over the input table projecting the input expression),
/// so the remote `FlattenExec` never needs session catalog resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "kebab-case")]
pub enum FlattenInput {
    Expr { proto: Vec<u8> },
    Plan { proto: Vec<u8> },
}

/// Self-contained payload for a LATERAL FLATTEN table scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlattenPayload {
    pub path: Vec<PathTokenDto>,
    pub is_outer: bool,
    pub is_recursive: bool,
    pub mode: String,
    pub input: FlattenInput,
}

/// Payload written into the plan for a distributed table scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "table", rename_all = "kebab-case")]
pub enum TablePayload {
    ManagedIceberg(ManagedIcebergPayload),
    Flatten(FlattenPayload),
}

/// Serializes a payload with the Avalanche magic prefix.
///
/// # Errors
///
/// Returns an error if JSON serialization fails.
pub fn encode_table_payload(payload: &TablePayload) -> datafusion_common::Result<Vec<u8>> {
    let json = serde_json::to_vec(payload)
        .map_err(|e| DataFusionError::Internal(format!("failed to serialize table payload: {e}")))?;
    let mut bytes = Vec::with_capacity(TABLE_PAYLOAD_MAGIC.len() + json.len());
    bytes.extend_from_slice(TABLE_PAYLOAD_MAGIC);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

/// Read-only `Catalog` for executor-side managed-table rebuilds. Scans never
/// commit through the catalog (manifests/data files are read straight from
/// the table's object store), so every mutation reports unsupported.
#[derive(Debug)]
struct NoopCatalog;

#[async_trait]
impl iceberg_rust::catalog::Catalog for NoopCatalog {
    fn name(&self) -> &str {
        "avalanche-noop"
    }

    async fn create_namespace(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
        _properties: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, String>, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn drop_namespace(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
    ) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn load_namespace(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
    ) -> Result<HashMap<String, String>, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
        _updates: Option<HashMap<String, String>>,
        _removals: Option<Vec<String>>,
    ) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn namespace_exists(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
    ) -> Result<bool, IcebergError> {
        Ok(true)
    }

    async fn list_tabulars(
        &self,
        _namespace: &iceberg_rust::catalog::namespace::Namespace,
    ) -> Result<Vec<IcebergIdentifier>, IcebergError> {
        Ok(vec![])
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&str>,
    ) -> Result<Vec<iceberg_rust::catalog::namespace::Namespace>, IcebergError> {
        Ok(vec![])
    }

    async fn tabular_exists(&self, _identifier: &IcebergIdentifier) -> Result<bool, IcebergError> {
        Ok(true)
    }

    async fn drop_table(&self, _identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn drop_view(&self, _identifier: &IcebergIdentifier) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn drop_materialized_view(
        &self,
        _identifier: &IcebergIdentifier,
    ) -> Result<(), IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn load_tabular(
        self: Arc<Self>,
        _identifier: &IcebergIdentifier,
    ) -> Result<Tabular, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn create_table(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_table: iceberg_rust::catalog::create::CreateTable,
    ) -> Result<IcebergTable, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn create_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: iceberg_rust::catalog::create::CreateView<Option<()>>,
    ) -> Result<iceberg_rust::view::View, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn create_materialized_view(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _create_view: iceberg_rust::catalog::create::CreateMaterializedView,
    ) -> Result<iceberg_rust::materialized_view::MaterializedView, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn update_table(
        self: Arc<Self>,
        _commit: iceberg_rust::catalog::commit::CommitTable,
    ) -> Result<IcebergTable, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn update_view(
        self: Arc<Self>,
        _commit: iceberg_rust::catalog::commit::CommitView<Option<()>>,
    ) -> Result<iceberg_rust::view::View, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn update_materialized_view(
        self: Arc<Self>,
        _commit: iceberg_rust::catalog::commit::CommitView<IcebergIdentifier>,
    ) -> Result<iceberg_rust::materialized_view::MaterializedView, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }

    async fn register_table(
        self: Arc<Self>,
        _identifier: IcebergIdentifier,
        _metadata_location: &str,
    ) -> Result<IcebergTable, IcebergError> {
        Err(IcebergError::NotSupported(
            "executor-side managed tables are read-only".to_string(),
        ))
    }
}

/// Rebuilds a managed-Iceberg `DataFusionTable` from its payload.
fn decode_managed_iceberg(
    payload: &ManagedIcebergPayload,
) -> datafusion_common::Result<Arc<dyn TableProvider>> {
    let metadata: TableMetadata = serde_json::from_value(payload.metadata.clone()).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to parse encoded managed table metadata for '{}': {e}",
            payload.name
        ))
    })?;
    let object_store = payload.volume.to_object_store()?;
    let identifier = IcebergIdentifier::new(&payload.namespace, &payload.name);
    // `Table::new` is async but performs no awaits — it only assembles the
    // struct, so blocking on it inside the (sync) codec decode is safe.
    let table = block_on(IcebergTable::new(
        identifier,
        Arc::new(NoopCatalog),
        object_store,
        metadata,
    ))
    .map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to rebuild encoded managed table '{}': {e}",
            payload.name
        ))
    })?;
    let provider = DataFusionTable::new(
        Tabular::Table(table),
        payload.snapshot_start,
        payload.snapshot_end,
        payload.branch.as_deref(),
    );
    Ok(Arc::new(provider) as Arc<dyn TableProvider>)
}

/// Rebuilds a `FlattenTableProvider` from its payload.
fn decode_flatten(
    payload: &FlattenPayload,
    table_ref: &TableReference,
    ctx: &TaskContext,
    codec: &EmbucketLogicalExtensionCodec,
) -> datafusion_common::Result<Arc<dyn TableProvider>> {
    let path = payload
        .path
        .iter()
        .map(|token| match token {
            PathTokenDto::Key(key) => PathToken::Key(key.clone()),
            PathTokenDto::Index(index) => PathToken::Index(*index),
        })
        .collect();
    let mode = match payload.mode.as_str() {
        "object" => FlattenMode::Object,
        "array" => FlattenMode::Array,
        _ => FlattenMode::Both,
    };
    let (input_expr, input_plan) = match &payload.input {
        FlattenInput::Expr { proto } => {
            let node = LogicalExprNode::decode(proto.as_slice()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to decode FLATTEN input expression for '{table_ref}': {e}"
                ))
            })?;
            (parse_expr(&node, ctx, codec)?, None)
        }
        FlattenInput::Plan { proto } => {
            let node = LogicalPlanNode::decode(proto.as_slice()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to decode FLATTEN input plan for '{table_ref}': {e}"
                ))
            })?;
            let plan = Arc::new(node.try_into_logical_plan(ctx, codec)?);
            // Placeholder literal: `FlattenExec` consults `input_plan` first;
            // the expression is only reached when no plan was encoded.
            (Expr::Literal(ScalarValue::Utf8(None), None), Some(plan))
        }
    };
    let args = FlattenArgs {
        input_expr,
        path,
        is_outer: payload.is_outer,
        is_recursive: payload.is_recursive,
        mode,
    };
    let provider = FlattenTableProvider::new_with_plan(args, input_plan).map_err(|e| {
        DataFusionError::Execution(format!(
            "failed to rebuild FLATTEN table '{table_ref}': {e}"
        ))
    })?;
    Ok(Arc::new(provider) as Arc<dyn TableProvider>)
}

/// Marker bytes written as a UDF "definition" so `from_proto` resolves the
/// function via `try_decode_udf` (Embucket `FunctionMaps`) instead of the
/// plain session registry, where datafusion builtins with the same name could
/// shadow the Embucket overload.
const UDF_MARKER: &[u8] = b"embucket";

#[derive(Debug)]
pub struct EmbucketLogicalExtensionCodec {
    functions: FunctionMaps,
    inner: BallistaLogicalExtensionCodec,
    /// Pre-encoded table payloads collected by the router's distributed
    /// walk, keyed by `TableReference::to_string()` of the scan they belong
    /// to (plus bare-name aliases for FLATTEN input plans built by name).
    encoded_tables: HashMap<String, Vec<u8>>,
}

impl EmbucketLogicalExtensionCodec {
    pub fn new() -> Self {
        Self {
            functions: FunctionMaps::embucket(),
            inner: BallistaLogicalExtensionCodec::default(),
            encoded_tables: HashMap::new(),
        }
    }

    /// Codec carrying the router-collected table payloads for one query.
    #[must_use]
    pub fn with_encoded_tables(encoded_tables: HashMap<String, Vec<u8>>) -> Self {
        Self {
            functions: FunctionMaps::embucket(),
            inner: BallistaLogicalExtensionCodec::default(),
            encoded_tables,
        }
    }

    /// Unwraps Embucket's provider wrappers (CachingTable over the real
    /// provider) down to the inner DataFusion-native table.
    fn unwrap_provider(node: &Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        let mut provider = Arc::clone(node);
        for _ in 0..4 {
            let Some(caching) = provider.downcast_ref::<CachingTable>() else {
                break;
            };
            provider = Arc::clone(&caching.table);
        }
        provider
    }

    /// Encodes an Avalanche table payload if the codec carries one for this
    /// scan reference.
    fn encode_avalanche_payload(
        &self,
        table_ref: &TableReference,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<bool> {
        let Some(bytes) = self.encoded_tables.get(&table_ref.to_string()) else {
            return Ok(false);
        };
        buf.extend_from_slice(bytes);
        Ok(true)
    }

    fn reject_write_node(&self, node: &Extension) -> datafusion_common::Result<()> {
        if node.node.name() == "MergeIntoSink" {
            return Err(DataFusionError::NotImplemented(
                "MERGE INTO is not executed in a warehouse (distributed writes are a \
                 post-v1 feature); it must run locally"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl Default for EmbucketLogicalExtensionCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalExtensionCodec for EmbucketLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> datafusion_common::Result<Extension> {
        self.inner.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> datafusion_common::Result<()> {
        self.reject_write_node(node)?;
        self.inner.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &TaskContext,
    ) -> datafusion_common::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        if buf.starts_with(TABLE_PAYLOAD_MAGIC) {
            let payload: TablePayload = serde_json::from_slice(&buf[TABLE_PAYLOAD_MAGIC.len()..])
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "failed to parse encoded table payload for '{table_ref}': {e}"
                    ))
                })?;
            return match payload {
                TablePayload::ManagedIceberg(managed) => decode_managed_iceberg(&managed),
                TablePayload::Flatten(flatten) => {
                    decode_flatten(&flatten, table_ref, ctx, self)
                }
            };
        }
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        let provider = Self::unwrap_provider(&node);
        // Router-collected payloads (managed Iceberg tables, LATERAL
        // FLATTEN) travel as self-contained Avalanche payloads.
        if self.encode_avalanche_payload(table_ref, buf)? {
            return Ok(());
        }
        // Managed tables and views reach this codec with no payload only
        // when the router walk did not run (e.g. encoding a plan subtree
        // directly); fail loudly instead of Ballista's opaque codec error.
        let is_datafusion_iceberg = provider.downcast_ref::<DataFusionTable>().is_some();
        if is_datafusion_iceberg {
            return Err(DataFusionError::NotImplemented(format!(
                "table '{table_ref}' is backed by Embucket's managed Iceberg provider, \
                 which requires a distributed-plan walk before serialization; it cannot \
                 be encoded here"
            )));
        }
        if provider.downcast_ref::<FlattenTableProvider>().is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "table '{table_ref}' is a LATERAL FLATTEN source, which requires a \
                 distributed-plan walk before serialization; it cannot be encoded here"
            )));
        }
        self.inner.try_encode_table_provider(table_ref, provider, buf)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "table '{table_ref}' cannot be serialized for distributed execution \
                     (provider type not supported by the codec): {e}"
                ))
            })
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> datafusion_common::Result<Arc<ScalarUDF>> {
        self.functions.udf(name)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> datafusion_common::Result<()> {
        // The function name travels in the protobuf expression itself; the
        // remote side resolves the instance via `try_decode_udf`. A non-empty
        // definition is REQUIRED for Embucket UDFs: `from_proto` consults the
        // plain session registry (datafusion builtins) BEFORE this codec when
        // the definition is `None`, so same-name functions (e.g. datafusion's
        // 2-arg `decode` vs Embucket's conditional DECODE) would silently
        // resolve to the wrong overload on executors.
        if self.functions.scalar.contains_key(node.name()) {
            buf.extend_from_slice(UDF_MARKER);
        }
        Ok(())
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        _buf: &[u8],
    ) -> datafusion_common::Result<Arc<AggregateUDF>> {
        self.functions.udaf(name)
    }

    fn try_encode_udaf(
        &self,
        _node: &AggregateUDF,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, _buf: &[u8]) -> datafusion_common::Result<Arc<WindowUDF>> {
        self.functions.udwf(name)
    }

    fn try_encode_udwf(
        &self,
        _node: &WindowUDF,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
}

/// Physical codec: Ballista behavior + Embucket UDF decode for stage plans.
#[derive(Debug)]
pub struct EmbucketPhysicalExtensionCodec {
    inner: BallistaPhysicalExtensionCodec,
    functions: FunctionMaps,
}

impl Default for EmbucketPhysicalExtensionCodec {
    fn default() -> Self {
        Self {
            inner: BallistaPhysicalExtensionCodec::default(),
            functions: FunctionMaps::embucket(),
        }
    }
}

impl EmbucketPhysicalExtensionCodec {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalExtensionCodec for EmbucketPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.try_decode(buf, inputs, ctx, proto_converter)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn datafusion_proto::physical_plan::PhysicalProtoConverterExtension,
    ) -> datafusion_common::Result<()> {
        self.inner.try_encode(node, buf, proto_converter)
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> datafusion_common::Result<Arc<ScalarUDF>> {
        self.functions.udf(name)
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> datafusion_common::Result<()> {
        if self.functions.scalar.contains_key(node.name()) {
            buf.extend_from_slice(UDF_MARKER);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{Expr, col};
    use datafusion_proto::logical_plan::from_proto::parse_expr;
    use datafusion_proto::logical_plan::to_proto::serialize_expr;

    #[test]
    fn decodes_embucket_udf_from_name() {
        let maps = FunctionMaps::embucket();
        let name = maps.scalar.keys().next().expect("non-empty").clone();
        let codec = EmbucketLogicalExtensionCodec::new();
        let decoded = codec
            .try_decode_udf(&name, &[])
            .unwrap_or_else(|_| panic!("codec must resolve '{name}'"));
        assert_eq!(decoded.name(), name);
    }

    #[test]
    fn rejects_unknown_udf() {
        let codec = EmbucketLogicalExtensionCodec::new();
        assert!(codec.try_decode_udf("__nope__", &[]).is_err());
    }

    /// Regression: the physical codec previously derived `Default` with an
    /// empty `FunctionMaps`, so stage-plan UDF resolution failed on executors
    /// with "not an Embucket function" for every Embucket UDF.
    #[test]
    fn physical_codec_collects_embucket_udfs() {
        let codec = EmbucketPhysicalExtensionCodec::new();
        for name in ["variant_element", "decode", "get_path"] {
            let decoded = codec
                .try_decode_udf(name, &[])
                .unwrap_or_else(|_| panic!("physical codec must resolve '{name}'"));
            assert_eq!(decoded.name(), name);
        }
        assert!(codec.try_decode_udf("__nope__", &[]).is_err());
    }

    #[test]
    fn udf_expr_roundtrips_through_proto() {
        let maps = FunctionMaps::embucket();
        let name = maps.scalar.keys().next().expect("non-empty").clone();
        let udf = maps.udf(&name).expect("lookup");
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: udf,
            args: vec![col("a")],
        });

        let codec = EmbucketLogicalExtensionCodec::new();
        let proto = serialize_expr(&expr, &codec).expect("encode");
        let ctx = TaskContext::default();
        let decoded = parse_expr(&proto, &ctx, &codec).expect("decode");
        assert_eq!(decoded, expr);
    }
}
