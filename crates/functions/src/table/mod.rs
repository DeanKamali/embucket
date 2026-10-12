use crate::table::flatten::func::FlattenTableFunc;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub mod errors;
pub mod flatten;
pub use errors::Error;

pub fn register_udtfs(ctx: &SessionContext) {
    // DataFusion 55 resolves table function names case-sensitively
    // (`SessionState::table_functions` is a plain `HashMap`), while Snowflake
    // treats them case-insensitively. Register the common spellings; the
    // executor-side `TableFunctionVisitor` normalizes the rest.
    ctx.register_udtf("flatten", Arc::new(FlattenTableFunc::new()));
    ctx.register_udtf("FLATTEN", Arc::new(FlattenTableFunc::new()));
}
