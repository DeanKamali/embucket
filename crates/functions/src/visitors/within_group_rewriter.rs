use datafusion_expr::sqlparser::ast::VisitMut;
use datafusion_expr::sqlparser::ast::{
    Expr, Function, FunctionArgumentClause, FunctionArguments, OrderByExpr, Statement,
    VisitorMut,
};
use std::ops::ControlFlow;

/// Rewrites `ARRAY_AGG(expr) WITHIN GROUP (ORDER BY ...)` into the equivalent
/// native form `ARRAY_AGG(expr ORDER BY ...)`.
///
/// DataFusion 55 (upstream #18607) made `WITHIN GROUP` an opt-in feature for
/// aggregate UDAFs via `supports_within_group_clause()`. DataFusion's built-in
/// `array_agg` does not opt in, so `WITHIN GROUP` on it now fails planning with
/// "WITHIN GROUP is only supported for ordered-set aggregate functions".
/// Both spellings are semantically identical, so the visitor moves the ordering
/// into the function argument list before planning.
///
/// Embucket UDAFs that do opt in (`listagg`, `percentile_cont`) are left
/// untouched so their `WITHIN GROUP` handling keeps using the planner's
/// argument-prepending convention.
#[derive(Debug, Default)]
pub struct WithinGroupRewriter;

impl WithinGroupRewriter {
    pub fn visit_stmt(stmt: &mut Statement) {
        let _ = stmt.visit(&mut Self);
    }

    fn rewrite_function(&self, function: &mut Function) {
        let is_array_agg = function
            .name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("array_agg"));
        if !is_array_agg || function.within_group.is_empty() {
            return;
        }

        let order_by: Vec<OrderByExpr> = std::mem::take(&mut function.within_group);
        if let FunctionArguments::List(list) = &mut function.args {
            // Preserve any existing in-args ORDER BY (rare) by appending the
            // WITHIN GROUP ordering after it.
            list.clauses.push(FunctionArgumentClause::OrderBy(order_by));
        }
    }
}

impl VisitorMut for WithinGroupRewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expr {
            self.rewrite_function(function);
        }
        ControlFlow::Continue(())
    }
}

pub fn visit(stmt: &mut Statement) {
    WithinGroupRewriter::visit_stmt(stmt);
}
