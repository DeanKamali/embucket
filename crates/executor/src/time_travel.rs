//! Pre-parse interception of Snowflake time-travel predicates (Avalanche
//! design §15, M9.2).
//!
//! The sqlparser fork has no `AT(...)` grammar, so time-travel predicates are
//! rewritten on the raw SQL before DataFusion parsing:
//!
//! ```text
//! SELECT … FROM <ref> [AS <alias>] AT(TIMESTAMP => '<ts>' | OFFSET => <secs>)
//! ```
//!
//! Each predicate resolves against the referenced table's Iceberg
//! `snapshot_log` (timestamp/offset → snapshot id), and the table reference is
//! rewritten to a temp-table name whose provider is a `DataFusionTable` pinned
//! to that snapshot (`end = Some(snap)` scans the manifest list of that
//! snapshot, i.e. full as-of state — verified against `iceberg-rust
//! Table::manifests`, no fork change needed).
//!
//! v1 limits: reads; `STATEMENT`/`STREAM` keys unsupported; local sessions
//! only (no warehouse) because the temp table lives in the router's catalog.

use crate::error::{self as ex_error, Error, Result};
use crate::session::UserSession;
use catalog::catalog::CatalogConfig;
use avalanche_account::AuthzCatalogList;
use catalog::catalog_list::EmbucketCatalogList;
use catalog::catalogs::embucket::iceberg_catalog::EmbucketIcebergCatalog;
use catalog_metastore::models::{TableFormat, TableIdent};
use catalog_metastore::Metastore;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_iceberg::DataFusionTable as IcebergDataFusionTable;
use iceberg_rust::catalog::tabular::Tabular as IcebergTabular;
use iceberg_rust::catalog::Catalog as IcebergCatalog;
use iceberg_rust::spec::table_metadata::TableMetadata;
use iceberg_rust::table::Table as IcebergTable;
use snafu::Location;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

const TT_CATALOG: &str = "__tt";
const TT_SCHEMA: &str = "__s";

static TT_SCHEMA_PROVIDER: OnceLock<Arc<MemorySchemaProvider>> = OnceLock::new();
static TT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A time-travel point: absolute timestamp (ms since epoch) or seconds back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimePoint {
    TimestampMs(i64),
    OffsetSecs(i64),
}

impl TimePoint {
    fn resolve_ms(&self) -> i64 {
        match self {
            Self::TimestampMs(ms) => *ms,
            Self::OffsetSecs(secs) => Utc::now().timestamp_millis() - secs * 1000,
        }
    }
}

/// A located `AT(...)` predicate: byte range to replace (reference + optional
/// alias through the closing paren), the reference name parts as written, the
/// alias suffix to preserve, and the time point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRewrite {
    pub start: usize,
    pub end: usize,
    pub ref_parts: Vec<String>,
    pub alias_suffix: String,
    pub point: TimePoint,
}

/// Lower-case, quotes stripped.
fn word(s: &str) -> String {
    s.trim_matches(|c| c == '"' || c == '`' || c == '\'')
        .to_ascii_lowercase()
}

fn tokenize(sql: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (i, c) in sql.char_indices() {
        if c.is_whitespace() {
            if let Some(s) = start.take() {
                out.push((s, &sql[s..i]));
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        out.push((s, &sql[s..]));
    }
    out
}

/// Strip trailing `,` and `;` (closing parens are handled by the AT scanner).
fn strip_trailing_punct(s: &str) -> &str {
    s.trim_end_matches([',', ';'])
}

/// Locate every `FROM|JOIN <ref> [AS <alias>] AT(<spec>)` occurrence.
/// Pure — unit-testable; resolution/registration happens in [`rewrite`].
pub fn plan(sql: &str) -> Result<Vec<PlannedRewrite>> {
    let toks = tokenize(sql);
    let mut plans = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        let kw = word(toks[i].1);
        if kw != "from" && kw != "join" {
            i += 1;
            continue;
        }
        // Table reference: next token. Subqueries are skipped.
        let Some(&(rstart, rtext)) = toks.get(i + 1) else {
            break;
        };
        if rtext.starts_with('(') {
            i += 1;
            continue;
        }
        let ref_clean = strip_trailing_punct(rtext);
        let ref_parts: Vec<String> = ref_clean
            .trim_matches(|c| c == '"' || c == '`' || c == '\'')
            .split('.')
            .map(word)
            .collect();
        if ref_parts.is_empty() || ref_parts.iter().any(String::is_empty) {
            i += 1;
            continue;
        }
        // Optional alias between ref and AT: `AS <alias>`, or a bare alias
        // immediately followed by AT.
        let mut j = i + 2;
        let mut alias_suffix = String::new();
        if let Some((_, t2)) = toks.get(j) {
            let w2 = word(t2);
            if w2 == "as" {
                if let Some((_, alias)) = toks.get(j + 1) {
                    alias_suffix = format!(" {alias}");
                    j += 2;
                }
            } else if !w2.is_empty()
                && !t2.starts_with('(')
                && w2 != ")"
                && toks
                    .get(j + 1)
                    .is_some_and(|(_, t3)| word(strip_at_paren(t3)) == "at")
            {
                alias_suffix = format!(" {t2}");
                j += 1;
            }
        }
        // Expect AT( … ).
        let Some(&(at_start, at_tok)) = toks.get(j) else {
            break;
        };
        if word(strip_at_paren(at_tok)) != "at" {
            i += 1;
            continue;
        }
        match parse_at_spec(&toks, j) {
            Some(Ok((point, end))) => {
                plans.push(PlannedRewrite {
                    start: rstart,
                    end,
                    ref_parts,
                    alias_suffix,
                    point,
                });
                let _ = at_start;
                i = toks.iter().position(|(s, _)| *s >= end).unwrap_or(toks.len());
            }
            // Malformed spec — let the planner report the syntax error.
            Some(Err(err)) => return Err(err),
            None => {
                i += 1;
            }
        }
    }
    Ok(plans)
}

/// If the token is `AT(` return just `AT`, else the token unchanged.
fn strip_at_paren(tok: &str) -> &str {
    if tok.len() > 2
        && tok.as_bytes()[..2].eq_ignore_ascii_case(b"at")
        && tok[2..].starts_with('(')
    {
        &tok[..2]
    } else {
        tok
    }
}

type AtSpec = Option<std::result::Result<(TimePoint, usize), Error>>;

/// Scan the `AT(...)` spec starting at token index `at_idx`.
///
/// - `Some(Ok((point, byte_end_of_closing_paren)))` — a parseable spec
/// - `Some(Err(...))` — a well-formed spec with an unsupported key
/// - `None` — malformed (caller falls through to the planner)
fn parse_at_spec(toks: &[(usize, &str)], at_idx: usize) -> AtSpec {
    let (_, at_tok) = toks[at_idx];
    let mut k = at_idx + 1;
    let mut spec: Vec<String> = Vec::new();
    if at_tok.len() > 2 && at_tok[..2].eq_ignore_ascii_case("at") && at_tok[2..].starts_with('(') {
        // `AT(TIMESTAMP` — the remainder is the first spec token.
        let rest = &at_tok[3..];
        if !rest.is_empty() {
            spec.push(rest.to_string());
        }
    } else {
        // `AT ( …` — skip the standalone paren, absorbing any attached text.
        if let Some((_, t)) = toks.get(k) {
            if let Some(rest) = t.strip_prefix('(') {
                if !rest.is_empty() {
                    spec.push(rest.to_string());
                }
                k += 1;
            }
        }
    }
    loop {
        let Some(&(tstart, tok)) = toks.get(k) else {
            return None;
        };
        // Closing paren may be attached to the last spec token (`86400)`,
        // `'10:00:00')`) or standalone (`)` / `),`).
        if let Some(idx) = tok.find(')') {
            let head = &tok[..idx];
            if !head.is_empty() {
                spec.push(head.to_string());
            }
            let key = spec.first()?.to_ascii_uppercase();
            let value = spec.get(2..).unwrap_or_default().join(" ");
            let point = match key.as_str() {
                "TIMESTAMP" => {
                    let Some(ms) = parse_timestamp(value.trim_matches('\'')) else {
                        return Some(Err(external_error(
                            "time travel",
                            format!("unparseable TIMESTAMP '{value}' (expected e.g. '2026-10-14 10:00:00')"),
                        )));
                    };
                    TimePoint::TimestampMs(ms)
                }
                "OFFSET" => {
                    let Ok(secs) = value.trim().parse::<i64>() else {
                        return Some(Err(external_error(
                            "time travel",
                            format!("unparseable OFFSET '{value}' (expected integer seconds)"),
                        )));
                    };
                    TimePoint::OffsetSecs(secs)
                }
                other => {
                    return Some(Err(external_error(
                        "time travel",
                        format!("unsupported AT() key '{other}' (v1 supports TIMESTAMP and OFFSET)"),
                    )));
                }
            };
            return Some(Ok((point, tstart + idx + 1)));
        }
        spec.push(tok.to_string());
        k += 1;
        if spec.len() > 8 {
            return None;
        }
    }
}

/// Parse a Snowflake-style timestamp literal. Naive values are treated as UTC.
pub(crate) fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc).timestamp_millis());
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ] {
        if fmt == "%Y-%m-%d" {
            if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                return Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
            }
        } else if let Ok(n) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(n.and_utc().timestamp_millis());
        }
    }
    None
}

fn external_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {err}").into(),
        )),
        location: Location::default(),
    }
}

fn normalized(session: &UserSession, name: &str) -> String {
    session
        .ident_normalizer
        .normalize(datafusion::sql::sqlparser::ast::Ident::new(name))
}

/// Resolve possibly-qualified table reference parts against session defaults.
fn resolve_table_parts(session: &UserSession, parts: &[String]) -> Result<TableIdent> {
    let (db, schema) = (
        session
            .get_session_variable("database")
            .unwrap_or_else(|| "embucket".to_string()),
        session
            .get_session_variable("schema")
            .unwrap_or_else(|| "public".to_string()),
    );
    if parts.is_empty() || parts.len() > 3 {
        return ex_error::InvalidSchemaIdentifierSnafu {
            ident: parts.join("."),
        }
        .fail();
    }
    let mut out = vec![db, schema];
    let keep = 3 - parts.len();
    out.truncate(keep);
    out.extend(parts.iter().map(|p| normalized(session, p)));
    Ok(TableIdent::new(&out[0], &out[1], &out[2]))
}

/// Resolve the time point to a snapshot id from the table's snapshot log:
/// the latest commit at or before the point.
pub(crate) fn resolve_snapshot(metadata: &TableMetadata, point_ms: i64) -> Option<i64> {
    metadata
        .snapshot_log
        .iter()
        .filter(|e| e.timestamp_ms <= point_ms)
        .max_by_key(|e| e.timestamp_ms)
        .map(|e| e.snapshot_id)
}

/// Get (or lazily create) the process-wide temp schema `__tt.__s`.
fn temp_schema(list: &EmbucketCatalogList) -> Result<&'static Arc<MemorySchemaProvider>> {
    let provider = TT_SCHEMA_PROVIDER.get_or_init(|| Arc::new(MemorySchemaProvider::default()));
    if list.catalog(TT_CATALOG).is_none() {
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema(TT_SCHEMA, Arc::clone(provider) as Arc<dyn SchemaProvider>)
            .map_err(|e| external_error("time travel", e))?;
        <EmbucketCatalogList as CatalogProviderList>::register_catalog(
            list,
            TT_CATALOG.to_string(),
            Arc::new(catalog),
        );
    }
    Ok(provider)
}

/// Build an as-of table provider pinned to `snapshot_id` for `ident`.
async fn as_of_provider(
    metastore: &Arc<dyn Metastore>,
    config: &CatalogConfig,
    ident: &TableIdent,
    snapshot_id: i64,
) -> Result<Arc<dyn TableProvider>> {
    let table = metastore
        .get_table(ident)
        .await
        .map_err(|e| external_error("time travel", e))?
        .ok_or_else(|| {
            external_error(
                "time travel",
                format!(
                    "table '{}.{}.{}' not found",
                    ident.database, ident.schema, ident.table
                ),
            )
        })?;
    if table.data.format != TableFormat::Iceberg {
        return Err(external_error(
            "time travel",
            format!(
                "table '{}.{}.{}' is not an Iceberg table; time travel is only supported for managed Iceberg tables",
                ident.database, ident.schema, ident.table
            ),
        ));
    }
    let object_store = metastore
        .table_object_store(ident)
        .await
        .map_err(|e| external_error("time travel", e))?
        .ok_or_else(|| {
            external_error(
                "time travel",
                format!("object store not found for table '{}'", ident.table),
            )
        })?;
    let iceberg_catalog: Arc<dyn IcebergCatalog> = Arc::new(
        EmbucketIcebergCatalog::new(Arc::clone(metastore), ident.database.clone(), config.clone())
            .await
            .map_err(|e| external_error("time travel", e))?,
    );
    let iceberg_table = IcebergTable::new(
        ident.to_iceberg_ident(),
        iceberg_catalog,
        object_store,
        table.data.metadata.clone(),
    )
    .await
    .map_err(|e| external_error("time travel", e))?;
    Ok(Arc::new(IcebergDataFusionTable::new(
        IcebergTabular::Table(iceberg_table),
        None,
        Some(snapshot_id),
        None,
    )))
}

/// Pre-parse hook for `UserQuery::execute`: rewrites every
/// `FROM|JOIN <ref> [AS <alias>] AT(...)` in place. No-op when the query has
/// no time-travel predicates.
///
/// # Errors
///
/// Returns an error for unresolvable references, unsupported `AT()` keys,
/// unparseable timestamps, points before table creation, or warehouse
/// sessions (distributed AT() is a v2 item).
pub async fn rewrite(raw: &mut String, session: &UserSession) -> Result<()> {
    let plans = plan(raw)?;
    if plans.is_empty() {
        return Ok(());
    }
    if session.get_session_variable("warehouse").is_some() {
        return Err(external_error(
            "time travel",
            "AT()/BEFORE() time travel requires a local session in v1 (no active warehouse)",
        ));
    }
    let catalog_list: Arc<dyn CatalogProviderList> =
        Arc::clone(session.ctx.state().catalog_list());
    let list = AuthzCatalogList::unwrap(&catalog_list).ok_or_else(|| {
        external_error("time travel", "catalog list is not EmbucketCatalogList")
    })?;
    let config: CatalogConfig = list.config.clone().into();
    let schema = temp_schema(&list)?;

    let mut replacements = Vec::with_capacity(plans.len());
    for p in &plans {
        let ident = resolve_table_parts(session, &p.ref_parts)?;
        let table = session
            .metastore
            .get_table(&ident)
            .await
            .map_err(|e| external_error("time travel", e))?
            .ok_or_else(|| {
                external_error(
                    "time travel",
                    format!(
                        "table '{}.{}.{}' not found",
                        ident.database, ident.schema, ident.table
                    ),
                )
            })?;
        if table.data.format != TableFormat::Iceberg {
            return Err(external_error(
                "time travel",
                format!(
                    "table '{}.{}.{}' is not an Iceberg table; time travel is only supported for managed Iceberg tables",
                    ident.database, ident.schema, ident.table
                ),
            ));
        }
        let point_ms = p.point.resolve_ms();
        let Some(snapshot_id) = resolve_snapshot(&table.data.metadata, point_ms) else {
            return Err(external_error(
                "time travel",
                format!(
                    "requested time is before table '{}.{}' creation or first commit",
                    ident.schema, ident.table
                ),
            ));
        };
        let provider = as_of_provider(&session.metastore, &config, &ident, snapshot_id).await?;
        let temp = format!("tt{}", TT_COUNTER.fetch_add(1, Ordering::Relaxed));
        schema
            .register_table(temp.clone(), provider)
            .map_err(|e| external_error("time travel", e))?;
        replacements.push((
            p.start,
            p.end,
            format!("{TT_CATALOG}.{TT_SCHEMA}.{temp}{}", p.alias_suffix),
        ));
    }
    // Apply high→low so earlier byte offsets stay valid.
    replacements.sort_by_key(|(start, _, _)| *start);
    for (start, end, repl) in replacements.into_iter().rev() {
        raw.replace_range(start..end, &repl);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{plan, parse_timestamp, TimePoint};

    fn planned(sql: &str) -> Vec<PlannedRewrite> {
        plan(sql).unwrap_or_else(|e| panic!("plan failed: {e}"))
    }
    use super::PlannedRewrite;

    #[test]
    fn parses_simple_at_timestamp() {
        let sql = "SELECT * FROM t1 AT(TIMESTAMP => '2026-10-14 10:00:00') ORDER BY id";
        let plans = planned(sql);
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.ref_parts, vec!["t1"]);
        assert_eq!(p.alias_suffix, "");
        assert!(matches!(p.point, TimePoint::TimestampMs(_)));
        // Range covers `t1 AT(TIMESTAMP => '2026-10-14 10:00:00')`.
        let span = &sql[p.start..p.end];
        assert_eq!(span, "t1 AT(TIMESTAMP => '2026-10-14 10:00:00')");
    }

    #[test]
    fn parses_qualified_alias_and_offset() {
        let sql = "SELECT * FROM mydb.mys.t1 AS a AT(OFFSET => 86400) WHERE a.id > 1";
        let plans = planned(sql);
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.ref_parts, vec!["mydb", "mys", "t1"]);
        assert_eq!(p.alias_suffix, " a");
        assert_eq!(p.point, TimePoint::OffsetSecs(86400));
        assert_eq!(&sql[p.start..p.end], "mydb.mys.t1 AS a AT(OFFSET => 86400)");
    }

    #[test]
    fn parses_spaced_and_attached_parens() {
        let sql = "SELECT * FROM t1 AT ( TIMESTAMP => '2026-10-14 10:00:00' ) , t2";
        let plans = planned(sql);
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.ref_parts, vec!["t1"]);
        assert_eq!(&sql[p.start..p.end], "t1 AT ( TIMESTAMP => '2026-10-14 10:00:00' )");
        // The trailing comma survives the replacement.
        let rewritten = format!(
            "{}{}{}",
            &sql[..p.start],
            "__tt.__s.tt0",
            &sql[p.end..]
        );
        assert!(rewritten.ends_with(", t2"));
    }

    #[test]
    fn parses_two_tables() {
        let sql = "SELECT * FROM a AT(OFFSET => 60) JOIN b AT(OFFSET => 120) ON a.id = b.id";
        let plans = planned(sql);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].ref_parts, vec!["a"]);
        assert_eq!(plans[1].ref_parts, vec!["b"]);
    }

    #[test]
    fn bare_alias_before_at() {
        let sql = "SELECT * FROM t1 a AT(OFFSET => 60)";
        let plans = planned(sql);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].alias_suffix, " a");
    }

    #[test]
    fn no_predicates_no_plans() {
        for sql in [
            "SELECT * FROM t1",
            "SELECT 1",
            "UPDATE t1 SET a = 1",
            "SELECT * FROM (SELECT 1) x",
            "CREATE TABLE t2 CLONE t1",
        ] {
            assert!(planned(sql).is_empty(), "unexpected plan for {sql}");
        }
    }

    #[test]
    fn unsupported_key_is_bounded_error() {
        let err = plan("SELECT * FROM t1 AT(STATEMENT => 'x')").unwrap_err();
        assert!(err.to_string().contains("unsupported AT() key 'STATEMENT'"));
    }

    #[test]
    fn timestamp_formats() {
        assert_eq!(
            parse_timestamp("2026-10-14 10:00:00"),
            parse_timestamp("2026-10-14T10:00:00")
        );
        assert!(parse_timestamp("2026-10-14").is_some());
        assert!(parse_timestamp("2026-10-14 10:00:00.123").is_some());
        assert!(parse_timestamp("not a date").is_none());
    }
}