use crate::macros::make_udf_function;
use crate::semi_structured::conversion::common;
use datafusion::arrow::array::{Decimal128Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility};
use datafusion_common::ScalarValue;
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use serde_json::Value;
use std::sync::Arc;

/// `AS_DECIMAL` / `AS_NUMBER` — extracts a fixed-point NUMBER from a VARIANT.
///
/// Syntax:
/// - `AS_DECIMAL(<variant>)` — defaults to `NUMBER(38, 0)`
/// - `AS_DECIMAL(<variant>, <precision>, <scale>)`
///
/// Returns the rounded fixed-point value if the VARIANT holds a number,
/// else NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AsDecimalFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for AsDecimalFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDecimalFunc {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // AS_DECIMAL( <variant> )
                    TypeSignature::Any(1),
                    // AS_DECIMAL( <variant>, <precision>, <scale> )
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["as_number".to_string()],
        }
    }

    fn scalar_to_usize(scalar: &ScalarValue) -> Option<usize> {
        match scalar {
            ScalarValue::Int64(Some(v)) if *v >= 0 => usize::try_from(*v).ok(),
            ScalarValue::Int32(Some(v)) if *v >= 0 => usize::try_from(i64::from(*v)).ok(),
            ScalarValue::UInt64(Some(v)) => usize::try_from(*v).ok(),
            _ => None,
        }
    }
}

impl ScalarUDFImpl for AsDecimalFunc {
    fn name(&self) -> &'static str {
        "as_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        datafusion_common::exec_err!("AS_DECIMAL requires return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<datafusion::arrow::datatypes::FieldRef> {
        let (precision, scale) = match args.arg_fields.len() {
            1 => (38usize, 0i8),
            3 => {
                let Some(precision_scalar) = &args.scalar_arguments[1] else {
                    return datafusion_common::exec_err!(
                        "AS_DECIMAL precision must be a constant integer"
                    );
                };
                let Some(scale_scalar) = &args.scalar_arguments[2] else {
                    return datafusion_common::exec_err!(
                        "AS_DECIMAL scale must be a constant integer"
                    );
                };
                let precision = Self::scalar_to_usize(precision_scalar)
                    .filter(|p| (1..=38).contains(p))
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!(
                            "AS_DECIMAL precision must be an integer between 1 and 38"
                        )
                    })?;
                let scale = Self::scalar_to_usize(scale_scalar)
                    .and_then(|s| i8::try_from(s).ok())
                    .filter(|s| *s >= 0 && *s <= precision.saturating_sub(1) as i8)
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!(
                            "AS_DECIMAL scale must be an integer between 0 and precision - 1"
                        )
                    })?;
                (precision, scale)
            }
            n => {
                return datafusion_common::exec_err!(
                    "AS_DECIMAL expects 1 or 3 arguments, got {n}"
                );
            }
        };
        Ok(Arc::new(datafusion::arrow::datatypes::Field::new(
            self.name(),
            DataType::Decimal128(precision as u8, scale),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let len = common::input_len(&args)?;
        let (precision, scale) = match args.args.len() {
            1 => (38u8, 0i8),
            3 => {
                let ColumnarValue::Scalar(precision_scalar) = &args.args[1] else {
                    return datafusion_common::exec_err!(
                        "AS_DECIMAL precision must be a constant integer"
                    );
                };
                let ColumnarValue::Scalar(scale_scalar) = &args.args[2] else {
                    return datafusion_common::exec_err!(
                        "AS_DECIMAL scale must be a constant integer"
                    );
                };
                let precision = Self::scalar_to_usize(&precision_scalar)
                    .filter(|p| (1..=38).contains(p))
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!(
                            "AS_DECIMAL precision must be an integer between 1 and 38"
                        )
                    })? as u8;
                let scale = Self::scalar_to_usize(&scale_scalar)
                    .and_then(|s| i8::try_from(s).ok())
                    .filter(|s| *s >= 0 && *s <= precision.saturating_sub(1) as i8)
                    .ok_or_else(|| {
                        datafusion_common::exec_datafusion_err!(
                            "AS_DECIMAL scale must be an integer between 0 and precision - 1"
                        )
                    })?;
                (precision, scale)
            }
            n => {
                return datafusion_common::exec_err!(
                    "AS_DECIMAL expects 1 or 3 arguments, got {n}"
                );
            }
        };
        let values = common::json_values(&args.args)?;
        let multiplier = 10_i128.pow(scale as u32);
        let out: Decimal128Array = values
            .into_iter()
            .map(|v| match v {
                Some(Value::Number(n)) => n
                    .as_f64()
                    .map(|f| (f * multiplier as f64).round() as i128),
                _ => None,
            })
            .collect();
        let out = out.with_precision_and_scale(precision, scale)?;
        common::to_columnar_value(out, len)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(AsDecimalFunc);