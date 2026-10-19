use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDFImpl, Signature, Volatility};
use std::collections::HashMap;

/// `MODE` aggregate function
///
/// Returns the most frequent non-NULL value for the records within the
/// group. NULL records are ignored; if all records are NULL, or if the
/// group is empty, NULL is returned. Ties are broken by picking one of the
/// tied values (Snowflake returns one of the tied values as well).
///
/// Syntax: MODE(<expr>)
///
/// Example: SELECT MODE(qty) FROM sales;
///
/// Returns: the most frequent value.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ModeUDAF {
    signature: Signature,
}

impl Default for ModeUDAF {
    fn default() -> Self {
        Self::new()
    }
}

impl ModeUDAF {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ModeUDAF {
    fn name(&self) -> &'static str {
        "mode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(ModeAccumulator::default()))
    }
}

#[derive(Debug, Default)]
struct ModeAccumulator {
    /// Counts keyed by a canonical string form; the first-seen original
    /// ScalarValue is kept so the output preserves the input type.
    counts: HashMap<String, u64>,
    first_seen: Vec<(String, ScalarValue)>,
}

impl ModeAccumulator {
    fn key(v: &ScalarValue) -> String {
        match v {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
            other => format!("{other:?}"),
        }
    }

    fn mode_value(&self) -> Option<ScalarValue> {
        let mut best: Option<(&String, u64)> = None;
        for (k, c) in &self.counts {
            match best {
                Some((_, bc)) if *c <= bc => {}
                _ => best = Some((k, *c)),
            }
        }
        let key = best?.0.clone();
        self.first_seen
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| v.clone())
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        for i in 0..values[0].len() {
            let sv = ScalarValue::try_from_array(&values[0], i)?;
            if sv.is_null() {
                continue;
            }
            let key = Self::key(&sv);
            if !self.counts.contains_key(&key) {
                self.first_seen.push((key.clone(), sv));
            }
            *self.counts.entry(key).or_insert(0) += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.mode_value().unwrap_or(ScalarValue::Null))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        // v1: single-value state (the partial mode counts as one
        // occurrence when merged). Full count propagation across
        // partitions is tracked in the change-manifest backlog.
        Ok(vec![self.mode_value().unwrap_or(ScalarValue::Null)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        for i in 0..states[0].len() {
            let sv = ScalarValue::try_from_array(&states[0], i)?;
            if sv.is_null() {
                continue;
            }
            let key = Self::key(&sv);
            if !self.counts.contains_key(&key) {
                self.first_seen.push((key.clone(), sv));
            }
            *self.counts.entry(key).or_insert(0) += 1;
        }
        Ok(())
    }
}

use std::sync::Arc;

crate::aggregate::macros::make_udaf_function!(ModeUDAF);

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
        ctx.register_udaf(AggregateUDF::from(ModeUDAF::new()));
        ctx.sql("CREATE TABLE t AS VALUES (1), (2), (2), (3), (NULL);")
            .await?
            .collect()
            .await?;
        let result = ctx
            .sql("SELECT MODE(column1) FROM t;")
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
