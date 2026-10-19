use chrono::Utc;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::Result as DFResult;
use datafusion::scalar::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// `SYSDATE` SQL function
///
/// Returns the current timestamp for the system, expressed in the UTC time
/// zone. Unlike Snowflake's `CURRENT_TIMESTAMP()` (which returns
/// TIMESTAMP_LTZ), `SYSDATE()` is always UTC (TIMESTAMP_NTZ semantics).
///
/// Syntax: SYSDATE()
///
/// Example: SELECT SYSDATE() IS NOT NULL;
///
/// Returns: the current system timestamp in UTC.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SysDateFunc {
    signature: Signature,
}

impl Default for SysDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SysDateFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for SysDateFunc {
    fn name(&self) -> &'static str {
        "sysdate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap_or_default();
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(now_ns),
            None,
        )))
    }
}

crate::macros::make_udf_function!(SysDateFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(SysDateFunc::new()));
        let result = ctx
            .sql("SELECT SYSDATE() IS NOT NULL;")
            .await?
            .collect()
            .await?;
        let batch = &result[0];
        let col = batch.column(0);
        assert!(!col.is_null(0));
        Ok(())
    }
}
