use crate::numeric::div0::Div0Func;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use std::sync::Arc;

pub mod bitand;
pub mod bitor;
pub mod bitxor;
pub mod div0;
pub mod errors;
pub mod mod_func;

pub use errors::Error;

pub fn register_udfs(registry: &mut dyn FunctionRegistry) -> datafusion_common::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![
        Arc::new(ScalarUDF::from(Div0Func::new(false))),
        Arc::new(ScalarUDF::from(Div0Func::new(true))),
        bitand::get_udf(),
        bitor::get_udf(),
        bitxor::get_udf(),
        mod_func::get_udf(),
    ];

    for func in functions {
        registry.register_udf(func)?;
    }

    // SIGN is DataFusion's `signum` (same -1/0/+1 semantics).
    if let Ok(signum) = registry.udf("signum") {
        registry.register_udf(std::sync::Arc::new({
            let signum = (*signum).clone();
            signum.with_aliases(["sign"])
        }))?;
    }

    Ok(())
}
