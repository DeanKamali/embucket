use crate::visitors::query_with_body;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr,
    Ident, ObjectName, ObjectNamePart, Select, SelectFlavor, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableFactor, TableWithJoins, VisitMut,
    WildcardAdditionalOptions,
};
use datafusion::sql::sqlparser::ast::{
    Query, Statement, Value as AstValue, Visit, Visitor, VisitorMut,
};
use datafusion_expr::sqlparser::ast::helpers::attached_token::AttachedToken;
use std::collections::HashMap;
use std::ops::ControlFlow;

/// Rewrites correlated `FLATTEN` table-function factors into UNNEST-based
/// derived tables so the flatten is applied **per row** of the other FROM
/// items (lateral semantics) instead of planning as a cross join.
///
/// # Background
/// A `TableFactor::Function` (like `FLATTEN`) is planned with an empty input
/// schema, so arguments that reference columns of sibling tables either fail
/// to plan or are evaluated once against a single value — producing wrong
/// results for multi-row inputs (see the M16.3.1 spike in the design doc).
///
/// # Rewrite
/// ```sql
/// SELECT v, f.value FROM t, LATERAL FLATTEN(INPUT => v:arr) f
/// ```
/// becomes
/// ```sql
/// SELECT v, value FROM (
///   SELECT *,
///     UNNEST(__flatten_seq(v:arr, '', false, false, 'both'))   AS "SEQ",
///     UNNEST(__flatten_key(v:arr, '', false, false, 'both'))   AS "KEY",
///     UNNEST(__flatten_path(v:arr, '', false, false, 'both'))  AS "PATH",
///     UNNEST(__flatten_index(v:arr, '', false, false, 'both')) AS "INDEX",
///     UNNEST(__flatten_value(v:arr, '', false, false, 'both')) AS "VALUE",
///     UNNEST(__flatten_this(v:arr, '', false, false, 'both'))  AS "THIS"
///   FROM t)
/// ```
///
/// The six `__flatten_*` scalar UDFs (see
/// [`crate::table::flatten::rows`]) evaluate their input expression per row —
/// all column references resolve naturally against the inner FROM — and
/// DataFusion zips sibling UNNEST expressions positionally, reproducing
/// lateral-flatten semantics.
///
/// # Notes
/// - Parent references to the flatten alias (`f.value`) are rewritten to the
///   derived output columns; a single flatten factor produces the canonical
///   `SEQ/KEY/PATH/INDEX/VALUE/THIS` names, multiple flatten factors in the
///   same FROM produce `<alias>_<COL>` names to avoid collisions.
/// - `f.*` in the projection is expanded to those columns.
/// - The derived table is anonymous so qualifiers of the base tables are
///   preserved.
/// - Bails out (leaves the query untouched, preserving the legacy behavior)
///   when: the flatten factor carries joins, a conditional join references a
///   flatten alias, the flatten is the only FROM item, INPUT is missing, or
///   args mix named and positional forms.
#[derive(Debug, Default)]
pub struct FlattenCorrelationVisitor {
    /// One frame per query level: flatten alias (lowercase) -> (output idents,
    /// prefixed naming flag).
    stack: Vec<FlattenFrame>,
}

#[derive(Debug, Default)]
struct FlattenFrame {
    aliases: HashMap<String, (Vec<Ident>, bool)>,
}

impl FlattenFrame {
    fn top_column_ident(&self, alias: &str, col: &str) -> Option<Ident> {
        let (_names, prefixed) = self.aliases.get(alias)?;
        if *prefixed {
            Some(Ident::new(format!("{alias}_{col}").to_lowercase()))
        } else {
            // Unquoted: DataFusion normalizes the identifier, so any case the
            // user wrote resolves against the lowercase output column.
            Some(Ident::new(col))
        }
    }
}

const CANONICAL_COLUMNS: [&str; 6] = ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"];
const UDF_NAMES: [&str; 6] = [
    "__flatten_seq",
    "__flatten_key",
    "__flatten_path",
    "__flatten_index",
    "__flatten_value",
    "__flatten_this",
];

struct FlattenSpec {
    alias: String,
    input: Expr,
    path: Expr,
    is_outer: Expr,
    is_recursive: Expr,
    mode: Expr,
}

fn is_flatten_name(name: &ObjectName) -> bool {
    name.to_string().eq_ignore_ascii_case("flatten")
}

fn str_literal(v: &str) -> Expr {
    Expr::Value(AstValue::SingleQuotedString(v.to_string()).into())
}

fn bool_literal(v: bool) -> Expr {
    Expr::Value(AstValue::Boolean(v).into())
}

fn scalar_function(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        args: FunctionArguments::List(FunctionArgumentList {
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            duplicate_treatment: None,
            clauses: vec![],
        }),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// Name of the input-row ordinal column computed by the rewrite with
/// `ROW_NUMBER() OVER ()` and consumed by `__flatten_seq`.
const SEQ_ID_COLUMN: &str = "__flatten_seq_id";

fn window_function(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(name)]),
        args: FunctionArguments::List(FunctionArgumentList {
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            duplicate_treatment: None,
            clauses: vec![],
        }),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: Some(datafusion::logical_expr::sqlparser::ast::WindowType::WindowSpec(
            datafusion::logical_expr::sqlparser::ast::WindowSpec {
                window_name: None,
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
            },
        )),
        within_group: vec![],
    })
}

impl FlattenSpec {
    fn unnest_exprs(&self) -> Vec<Expr> {
        let mut exprs = Vec::with_capacity(UDF_NAMES.len());
        let seq_id = Expr::Identifier(Ident::new(SEQ_ID_COLUMN));
        for (idx, udf) in UDF_NAMES.iter().enumerate() {
            let mut args = vec![
                self.input.clone(),
                self.path.clone(),
                self.is_outer.clone(),
                self.is_recursive.clone(),
                self.mode.clone(),
            ];
            if idx == 0 {
                // `__flatten_seq` takes the input row ordinal as its 6th arg.
                args.push(seq_id.clone());
            }
            exprs.push(scalar_function("unnest", vec![scalar_function(udf, args)]));
        }
        exprs
    }
}

/// Extracts the flatten arguments (named `INPUT/PATH/IS_OUTER/IS_RECURSIVE/
/// MODE` or positional), preserving the argument expressions verbatim.
/// Returns `None` when the form is not supported (missing INPUT, or mixing
/// named and positional arguments).
fn extract_flatten_args(args: &[FunctionArg]) -> Option<(Expr, Expr, Expr, Expr, Expr)> {
    let named_count = args
        .iter()
        .filter(|a| matches!(a, FunctionArg::Named { .. }))
        .count();
    let default_path = str_literal("");
    let default_outer = bool_literal(false);
    let default_recursive = bool_literal(false);
    let default_mode = str_literal("both");

    if named_count > 0 {
        if named_count != args.len() {
            return None;
        }
        let get = |name: &str| -> Option<&Expr> {
            args.iter().find_map(|a| match a {
                FunctionArg::Named {
                    name: n,
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } if n.to_string().eq_ignore_ascii_case(name) => Some(e),
                _ => None,
            })
        };
        // Snowflake spells the flags `OUTER`/`RECURSIVE`; accept the
        // `IS_OUTER`/`IS_RECURSIVE` spelling as well.
        let either = |a: &str, b: &str| get(a).or_else(|| get(b));
        let input = get("input")?;
        let path = get("path").cloned().unwrap_or_else(|| default_path.clone());
        let is_outer = either("outer", "is_outer").cloned().unwrap_or_else(|| default_outer.clone());
        let is_recursive = either("recursive", "is_recursive")
            .cloned()
            .unwrap_or(default_recursive.clone());
        let mode = get("mode").cloned().unwrap_or_else(|| default_mode.clone());
        Some((input.clone(), path, is_outer, is_recursive, mode))
    } else {
        // Positional args are a flat list of Unnamed args.
        let exprs: Vec<&Expr> = args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect();
        if exprs.is_empty() {
            return None;
        }
        let input = (*exprs[0]).clone();
        let path = exprs
            .get(1)
            .map(|e| (*e).clone())
            .unwrap_or_else(|| default_path.clone());
        let is_outer = exprs
            .get(2)
            .map(|e| (*e).clone())
            .unwrap_or_else(|| default_outer.clone());
        let is_recursive = exprs
            .get(3)
            .map(|e| (*e).clone())
            .unwrap_or(default_recursive.clone());
        let mode = exprs
            .get(4)
            .map(|e| (*e).clone())
            .unwrap_or_else(|| default_mode.clone());
        Some((input, path, is_outer, is_recursive, mode))
    }
}

/// Checks whether an expression references a flatten alias (as the first part
/// of a compound identifier).
struct AliasRefFinder<'a> {
    aliases: &'a [String],
    found: bool,
}

impl Visitor for AliasRefFinder<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::CompoundIdentifier(idents) = expr
            && let Some(first) = idents.first()
            && self.aliases.iter().any(|a| a.eq_ignore_ascii_case(&first.value))
        {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn references_flatten_alias(expr: &Expr, aliases: &[String]) -> bool {
    if aliases.is_empty() {
        return false;
    }
    let mut finder = AliasRefFinder { aliases, found: false };
    let _ = expr.visit(&mut finder);
    finder.found
}

/// Builds `(SELECT *, ROW_NUMBER() OVER () AS __flatten_seq_id FROM <from>)`.
fn build_window_select(from: Vec<TableWithJoins>) -> Select {
    let projection = vec![
        SelectItem::Wildcard(WildcardAdditionalOptions::default()),
        SelectItem::ExprWithAlias {
            expr: window_function("row_number", vec![]),
            alias: Ident::new(SEQ_ID_COLUMN),
        },
    ];
    Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        exclude: None,
        into: None,
        from,
        lateral_views: vec![],
        prewhere: None,
        selection: None,
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: vec![],
        flavor: SelectFlavor::Standard,
        optimizer_hints: vec![],
        select_modifiers: None,
    }
}

fn build_inner_select(from: Vec<TableWithJoins>, specs: &[FlattenSpec], prefixed: bool) -> Select {
    // Hide the SEQ ordinal helper column from the parent projection.
    let mut projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(datafusion::logical_expr::sqlparser::ast::ExcludeSelectItem::Single(
            ObjectName::from(vec![Ident::new(SEQ_ID_COLUMN)]),
        )),
        ..WildcardAdditionalOptions::default()
    })];
    for spec in specs {
        for (expr, name) in spec.unnest_exprs().into_iter().zip(CANONICAL_COLUMNS) {
            // Lowercase unquoted aliases: matches the legacy `FLATTEN`
            // provider output (its schema is lowercased) so existing
            // references resolve unchanged.
            let alias_name = if prefixed {
                format!("{}_{}", spec.alias, name).to_lowercase()
            } else {
                name.to_lowercase()
            };
            projection.push(SelectItem::ExprWithAlias {
                expr,
                alias: Ident::new(alias_name),
            });
        }
    }
    Select {
        select_token: AttachedToken::empty(),
        distinct: None,
        top: None,
        top_before_distinct: false,
        projection,
        exclude: None,
        into: None,
        from,
        lateral_views: vec![],
        prewhere: None,
        selection: None,
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        window_before_qualify: false,
        value_table_mode: None,
        connect_by: vec![],
        flavor: SelectFlavor::Standard,
        optimizer_hints: vec![],
        select_modifiers: None,
    }
}

impl VisitorMut for FlattenCorrelationVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let mut frame = FlattenFrame::default();

        if let SetExpr::Select(select) = query.body.as_mut() {
            rewrite_select_from(select, &mut frame);
            expand_flatten_wildcards(select, &frame);
        }

        self.stack.push(frame);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
        self.stack.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Some(frame) = self.stack.last() else {
            return ControlFlow::Continue(());
        };
        if let Expr::CompoundIdentifier(idents) = expr
            && idents.len() == 2
        {
            let alias = idents[0].value.to_lowercase();
            if let Some(col_ident) =
                frame.top_column_ident(&alias, &idents[1].value.to_uppercase())
            {
                *expr = Expr::Identifier(col_ident);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Replaces flatten table-function factors in the FROM clause with an
/// anonymous derived table applying UNNEST per row; records the flatten
/// aliases in `frame`. Leaves the query untouched when the shape is not
/// supported.
fn rewrite_select_from(select: &mut Select, frame: &mut FlattenFrame) {
    // Locate flatten factors.
    let mut flatten_aliases: Vec<String> = vec![];
    let mut specs: Vec<FlattenSpec> = vec![];
    let mut has_flatten = false;
    for twj in &select.from {
        if let TableFactor::Function { name, args, alias, with_ordinality, .. } = &twj.relation
            && is_flatten_name(name)
        {
            has_flatten = true;
            if *with_ordinality {
                return;
            }
            let Some((input, path, is_outer, is_recursive, mode)) = extract_flatten_args(args)
            else {
                return;
            };
            let alias_name = alias
                .as_ref()
                .map(|a| a.name.value.to_lowercase())
                .unwrap_or_else(|| format!("__flatten{}", specs.len() + 1));
            flatten_aliases.push(alias_name.clone());
            specs.push(FlattenSpec {
                alias: alias_name,
                input,
                path,
                is_outer,
                is_recursive,
                mode,
            });
        }
        for join in &twj.joins {
            if let TableFactor::Function { name, .. } = &join.relation
                && is_flatten_name(name)
            {
                has_flatten = true;
            }
        }
    }
    if !has_flatten {
        return;
    }

    // Bail conditions: flatten factor with joins attached, a conditional join
    // referencing a flatten alias, or no remaining non-flatten FROM items.
    for twj in &select.from {
        if let TableFactor::Function { name, .. } = &twj.relation
            && is_flatten_name(name)
            && !twj.joins.is_empty()
        {
            return;
        }
        for join in &twj.joins {
            if let TableFactor::Function { name, .. } = &join.relation
                && is_flatten_name(name)
                && !matches!(
                    join.join_operator,
                    datafusion::logical_expr::sqlparser::ast::JoinOperator::CrossJoin(_)
                )
            {
                return;
            }
            let constraint_refs_flatten = match &join.join_operator {
                datafusion::logical_expr::sqlparser::ast::JoinOperator::Inner(constraint)
                | datafusion::logical_expr::sqlparser::ast::JoinOperator::LeftOuter(constraint)
                | datafusion::logical_expr::sqlparser::ast::JoinOperator::RightOuter(constraint)
                | datafusion::logical_expr::sqlparser::ast::JoinOperator::FullOuter(constraint) => {
                    match constraint {
                        datafusion::logical_expr::sqlparser::ast::JoinConstraint::On(e) => {
                            references_flatten_alias(e, &flatten_aliases)
                        }
                        // USING/NATURAL constrain by column name — a flatten
                        // alias in scope would change the constraint's meaning.
                        datafusion::logical_expr::sqlparser::ast::JoinConstraint::Using(_)
                        | datafusion::logical_expr::sqlparser::ast::JoinConstraint::Natural => {
                            true
                        }
                        datafusion::logical_expr::sqlparser::ast::JoinConstraint::None => false,
                    }
                }
                _ => false,
            };
            if constraint_refs_flatten {
                return;
            }
        }
    }

    let remaining_count = select
        .from
        .iter()
        .filter(|twj| {
            let rel_is_flatten = matches!(twj.relation, TableFactor::Function { ref name, .. } if is_flatten_name(name));
            let join_count_without_flatten = twj
                .joins
                .iter()
                .filter(|j| !matches!(j.relation, TableFactor::Function { ref name, .. } if is_flatten_name(name)))
                .count();
            !rel_is_flatten || join_count_without_flatten > 0
        })
        .count();
    if remaining_count == 0 {
        return;
    }

    // Prefixed output names when several flatten factors share the FROM, so
    // their columns don't collide in the derived table.
    let prefixed = specs.len() > 1;

    // Remove flatten factors from the FROM, keeping everything else.
    let mut new_from: Vec<TableWithJoins> = vec![];
    for mut twj in select.from.drain(..) {
        if matches!(twj.relation, TableFactor::Function { ref name, .. } if is_flatten_name(name)) {
            continue;
        }
        twj.joins
            .retain(|j| !matches!(j.relation, TableFactor::Function { ref name, .. } if is_flatten_name(name)));
        new_from.push(twj);
    }

    let inner_from = std::mem::take(&mut new_from);
    // Wrap the base FROM in a derived that assigns the input row ordinal
    // consumed by `__flatten_seq` (a stateless UDF cannot know row positions
    // across streaming batches).
    let window_select = build_window_select(inner_from);
    let window_factor = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query_with_body(window_select)),
        alias: None,
        sample: None,
    };
    let inner_select = build_inner_select(
        vec![TableWithJoins {
            relation: window_factor,
            joins: vec![],
        }],
        &specs,
        prefixed,
    );
    let derived_factor = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(query_with_body(inner_select)),
        alias: None,
        sample: None,
    };
    new_from.push(TableWithJoins {
        relation: derived_factor,
        joins: vec![],
    });
    select.from = new_from;

    for spec in &specs {
        let names: Vec<Ident> = CANONICAL_COLUMNS
            .iter()
            .map(|col| {
                if prefixed {
                    Ident::new(format!("{}_{}", spec.alias, col).to_lowercase())
                } else {
                    Ident::new(col.to_lowercase())
                }
            })
            .collect();
        frame.aliases.insert(spec.alias.clone(), (names, prefixed));
    }
}

/// Expands `f.*` (qualified wildcard over a flatten alias) in the projection
/// into the derived table's flatten output columns.
fn expand_flatten_wildcards(select: &mut Select, frame: &FlattenFrame) {
    if frame.aliases.is_empty() {
        return;
    }
    let mut new_projection = Vec::with_capacity(select.projection.len());
    for item in select.projection.drain(..) {
        match item {
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(on), _) => {
                let alias = match on.0.last() {
                    Some(ObjectNamePart::Identifier(ident)) => ident.value.to_lowercase(),
                    _ => {
                        new_projection.push(SelectItem::QualifiedWildcard(
                            SelectItemQualifiedWildcardKind::ObjectName(on),
                            WildcardAdditionalOptions::default(),
                        ));
                        continue;
                    }
                };
                match frame.aliases.get(&alias) {
                    Some((names, _)) => {
                        new_projection.extend(names.iter().map(|n| {
                            SelectItem::UnnamedExpr(Expr::Identifier(n.clone()))
                        }));
                    }
                    None => new_projection.push(SelectItem::QualifiedWildcard(
                        SelectItemQualifiedWildcardKind::ObjectName(on),
                        WildcardAdditionalOptions::default(),
                    )),
                }
            }
            other => new_projection.push(other),
        }
    }
    select.projection = new_projection;
}

pub fn visit(stmt: &mut Statement) {
    let _ = stmt.visit(&mut FlattenCorrelationVisitor::default());
}