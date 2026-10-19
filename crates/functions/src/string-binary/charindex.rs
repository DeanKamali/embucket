use datafusion::arrow::array::AsArray;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::error::Result as DFResult;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::sync::Arc;

/// `CHARINDEX` SQL function
///
/// Searches the first occurrence of <expr1> (the pattern) inside <expr2>
/// (the subject) and returns the 1-based position, or 0 when the pattern
/// is not found. An optional third argument gives the 1-based position
/// the search should start from.
///
/// Syntax: CHARINDEX(<expr1>, <expr2> [, <start_pos>])
///
/// Arguments:
/// - expr1: the string to search for (pattern)
/// - expr2: the string to search within (subject)
/// - start_pos: optional 1-based starting position (default 1)
///
/// Example: SELECT CHARINDEX('b', 'abc'), CHARINDEX('b', 'abcabc', 3);
///
/// Returns: 2 and 5.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CharIndexFunc {
    signature: Signature,
}

impl Default for CharIndexFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CharIndexFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

fn to_utf8(v: &ColumnarValue, len: usize) -> DFResult<ArrayRef> {
    let arr = v.to_array(len)?;
    if matches!(arr.data_type(), DataType::Utf8) {
        Ok(arr)
    } else {
        Ok(cast(&arr, &DataType::Utf8)?)
    }
}

fn to_i64(v: &ColumnarValue, len: usize) -> DFResult<ArrayRef> {
    let arr = v.to_array(len)?;
    if matches!(arr.data_type(), DataType::Int64) {
        Ok(arr)
    } else {
        Ok(cast(&arr, &DataType::Int64)?)
    }
}

impl ScalarUDFImpl for CharIndexFunc {
    fn name(&self) -> &'static str {
        "charindex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = args.number_rows;
        let pattern = to_utf8(&args.args[0], len)?;
        let subject = to_utf8(&args.args[1], len)?;
        let start: Option<ArrayRef> = if args.args.len() == 3 {
            Some(to_i64(&args.args[2], len)?)
        } else {
            None
        };

        let subject = subject.as_string::<i32>();
        let pattern = pattern.as_string::<i32>();
        let mut out: Vec<Option<i64>> = Vec::with_capacity(len);
        for i in 0..len {
            if subject.is_null(i) || pattern.is_null(i) {
                out.push(None);
                continue;
            }
            let subject_val = subject.value(i);
            let pattern_val = pattern.value(i);
            let start_pos: usize = match &start {
                Some(arr) => {
                    let v = arr.as_primitive::<Int64Type>().value(i);
                    if v <= 0 {
                        return datafusion_common::exec_err!(
                            "start_pos must be a positive integer"
                        );
                    }
                    (v as usize).saturating_sub(1)
                }
                None => 0,
            };
            if pattern_val.is_empty() {
                // Snowflake: empty pattern matches at the start position.
                out.push(Some(
                    (start_pos + 1).min(subject_val.chars().count() + 1) as i64
                ));
                continue;
            }
            let hay: Vec<char> = subject_val.chars().collect();
            let needle: Vec<char> = pattern_val.chars().collect();
            let mut found = Some(0);
            if start_pos < hay.len() && needle.len() <= hay.len() {
                for pos in start_pos..=hay.len() - needle.len() {
                    if hay[pos..pos + needle.len()] == needle[..] {
                        found = Some(pos as i64 + 1);
                        break;
                    }
                }
            }
            out.push(found);
        }

        Ok(ColumnarValue::Array(Arc::new(Int64Array::from(out))))
    }
}

crate::macros::make_udf_function!(CharIndexFunc);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarUDF;

    #[tokio::test]
    async fn test_it_works() -> DFResult<()> {
        let ctx = SessionContext::new();
        ctx.register_udf(ScalarUDF::from(CharIndexFunc::new()));
        let result = ctx
            .sql(
                "SELECT CHARINDEX('b', 'abc'), CHARINDEX('z', 'abc'), CHARINDEX('b', 'abcabc', 3);",
            )
            .await?
            .collect()
            .await?;
        let col = |i: usize| {
            result[0]
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(col(0), 2);
        assert_eq!(col(1), 0);
        assert_eq!(col(2), 5);
        Ok(())
    }
}
