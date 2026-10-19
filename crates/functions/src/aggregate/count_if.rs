use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};

/// `COUNT_IF` aggregate function
///
/// Returns the number of records for which the condition evaluates to
/// TRUE (NULL and FALSE rows are not counted).
///
/// Syntax: COUNT_IF(<condition>)
///
/// Example: SELECT COUNT_IF(qty > 2) FROM sales;
///
/// Returns: the count of rows where the condition is true.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct CountIfUDAF {
    signature: Signature,
}

impl Default for CountIfUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl CountIfUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountIfUDAF {
    fn name(&self) -> &'static str {
        "count_if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator::default()))
    }
}

#[derive(Debug, Default)]
struct CountIfAccumulator {
    count: i64,
}

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        use datafusion::arrow::array::BooleanArray;
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        let casted;
        let b: &BooleanArray = if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
            b
        } else {
            casted = datafusion::arrow::compute::cast(arr, &DataType::Boolean)?;
            casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "count_if: condition is not boolean".to_string(),
                    )
                })?
        };
        for v in b.iter() {
            if v == Some(true) {
                self.count += 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        use datafusion::arrow::array::Int64Array;
        if states.is_empty() {
            return Ok(());
        }
        let s = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "count_if: invalid merge state".to_string(),
                )
            })?;
        for v in s.iter() {
            self.count += v.unwrap_or(0);
        }
        Ok(())
    }
}

crate::aggregate::macros::make_udaf_function!(CountIfUDAF);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::AggregateUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_udaf(AggregateUDF::from(CountIfUDAF::new()));
        ctx.sql("CREATE TABLE t AS VALUES (1), (2), (3), (NULL);")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT COUNT_IF(column1 > 1) FROM t;")
            .await?
            .collect()
            .await?;
        assert_eq!(
            result[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        Ok(())
    }
}
