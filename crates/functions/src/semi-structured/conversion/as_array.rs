use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;

/// `AS_ARRAY` — typed extraction of a JSON array from a VARIANT.
///
/// Syntax: `AS_ARRAY(<variant>)`
///
/// Returns the array value if the VARIANT holds a JSON array, else NULL
/// (Snowflake semantics).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsArrayFunc {
    signature: Signature,
}

impl Default for AsArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsArrayFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AsArrayFunc {
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
            .map(|opt| opt.filter(|v| matches!(v, Value::Array(_))).map(|v| v.to_string()))
            .collect();
        common::to_columnar_value(res, len)
    }
}

make_udf_function!(AsArrayFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::assert_batches_eq;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_basic() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(AsArrayFunc::new()));
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
        let q = "SELECT AS_ARRAY('123') as v;";
        let result = ctx.sql(q).await?.collect().await?;

        assert_batches_eq!(&["+---+", "| v |", "+---+", "|   |", "+---+",], &result);

        Ok(())
    }
}