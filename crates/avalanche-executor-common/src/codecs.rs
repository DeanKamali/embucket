//! DataFusion proto codecs bridging Embucket and Ballista.
//!
//! Both codecs delegate structural (de)serialization to Ballista's codecs and
//! add what Embucket needs on top:
//! - UDF resolution by name (encoded plans carry only names; the remote side
//!   rebuilds the full `ScalarUDF`/`AggregateUDF`/`WindowUDF` instances from
//!   the [`FunctionMaps`] catalog collected from the `functions` crate).
//! - A loud error if an Embucket write node (e.g. the MERGE INTO sink) ever
//!   reaches the distributed path — in v1 all writes stay local.

use std::sync::Arc;

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_common::TableReference;

use crate::udf_registry::FunctionMaps;

/// Logical codec: Ballista behavior + Embucket UDF decode.
#[derive(Debug)]
pub struct EmbucketLogicalExtensionCodec {
    functions: FunctionMaps,
    inner: BallistaLogicalExtensionCodec,
}

impl EmbucketLogicalExtensionCodec {
    pub fn new() -> Self {
        Self {
            functions: FunctionMaps::embucket(),
            inner: BallistaLogicalExtensionCodec::default(),
        }
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
        self.inner
            .try_decode_table_provider(buf, table_ref, schema, ctx)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        self.inner.try_encode_table_provider(table_ref, node, buf)
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> datafusion_common::Result<Arc<ScalarUDF>> {
        self.functions.udf(name)
    }

    fn try_encode_udf(
        &self,
        _node: &ScalarUDF,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
        // The function name travels in the protobuf expression itself; the
        // remote side resolves the instance via `try_decode_udf`.
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
#[derive(Debug, Default)]
pub struct EmbucketPhysicalExtensionCodec {
    inner: BallistaPhysicalExtensionCodec,
    functions: FunctionMaps,
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

    fn try_encode_udf(
        &self,
        _node: &ScalarUDF,
        _buf: &mut Vec<u8>,
    ) -> datafusion_common::Result<()> {
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
