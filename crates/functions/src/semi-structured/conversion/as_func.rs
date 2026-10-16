//! `AS_ARRAY` / `AS_OBJECT` — typed extraction from VARIANT values.
//!
//! VARIANT columns are string-typed JSON (see `variant/visitors/type_rewrite.rs`).
//! `AS_ARRAY(<variant>)` returns the value only if it holds a JSON array;
//! `AS_OBJECT(<variant>)` only if it holds a JSON object; otherwise NULL.

use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["as_object".to_string()],
        }
    }

    fn accepts(&self, value: &Value) -> bool {
        match self.name() {
            "as_array" => matches!(value, Value::Array(_)),
            _ => matches!(value, Value::Object(_)),
        }
    }
}

impl ScalarUDFImpl for AsFunc {
    fn name(&self) -> &'static str {
        "as_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let values = common::json_values(&args.args)?;
        let res: StringArray = values
            .into_iter()
            .map(|opt| opt.filter(|v| self.accepts(v)).map(|v| v.to_string()))
            .collect();
        common::to_columnar_value(res, len)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(AsFunc::new()));
        let q = "SELECT AS_ARRAY('[1,2,3]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+",
                "| v       |",
                "+---------+",
                "| [1,2,3] |",
                "+---------+",
            ],
            &result
        );

        let q = "SELECT AS_ARRAY(NULL) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        // A non-array VARIANT yields NULL (Snowflake semantics), not the input.
        let q = "SELECT AS_ARRAY(123) as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        Ok(())
    }
}