use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_OBJECT` — typed extraction of a JSON object from a VARIANT.
///
/// Syntax: `AS_OBJECT(<variant>)`
///
/// Returns the object value if the VARIANT holds a JSON object, else NULL
/// (Snowflake semantics).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsObjectFunc {
    signature: Signature,
}

impl Default for AsObjectFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsObjectFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsObjectFunc {
    fn name(&self) -> &'static str {
        "as_object"
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
            .map(|opt| opt.filter(|v| matches!(v, Value::Object(_))).map(|v| v.to_string()))
            .collect();
        common::to_columnar_value(res, len)
    }
}

make_udf_function!(AsObjectFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(AsObjectFunc::new()));
        let q = "SELECT AS_OBJECT('{\"a\": 1}') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+---------+",
                "| v       |",
                "+---------+",
                "| {\"a\":1} |",
                "+---------+",
            ],
            &result
        );

        // A non-object VARIANT yields NULL (Snowflake semantics), not the input.
        let q = "SELECT AS_OBJECT('[1,2,3]') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        Ok(())
    }
}