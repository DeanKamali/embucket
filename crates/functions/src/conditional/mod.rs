pub mod booland;
pub mod boolnot;
pub mod boolor;
pub mod boolxor;
pub mod decode;
pub mod equal_null;
pub mod iff;
pub mod is_not_null;
pub mod is_null;
pub mod nullifzero;
pub mod zeroifnull;

use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;
// Re-export the get_udf functions
pub use booland::get_udf as booland_get_udf;
pub use boolnot::get_udf as boolnot_get_udf;
pub use boolor::get_udf as boolor_get_udf;
pub use boolxor::get_udf as boolxor_get_udf;
pub use decode::get_udf as decode_get_udf;
pub use equal_null::get_udf as equal_null_get_udf;
pub use iff::get_udf as iff_get_udf;
pub use is_not_null::get_udf as is_not_null_get_udf;
pub use is_null::get_udf as is_null_get_udf;
pub use nullifzero::get_udf as nullifzero_get_udf;
pub use zeroifnull::get_udf as zeroifnull_get_udf;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        booland::get_udf(),
        boolnot::get_udf(),
        boolor::get_udf(),
        boolxor::get_udf(),
        decode::get_udf(),
        equal_null::get_udf(),
        iff::get_udf(),
        is_null::get_udf(),
        is_not_null::get_udf(),
        nullifzero::get_udf(),
        zeroifnull::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    Ok(())
}
