use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod base64;
pub mod base64_encode;
pub mod md5;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> =
        vec![base64::get_udf(), base64_encode::get_udf(), md5::get_udf()];
    for func in functions {
        registry.register_udf(func)?;
    }
    Ok(())
}
