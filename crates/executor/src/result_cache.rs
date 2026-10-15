//! Result cache (design §19.1, M12): identical-SELECT reuse validated by
//! snapshot fingerprints.
//!
//! A cache entry is keyed on `(user, warehouse, normalized SQL text)` and
//! stores the executed `QueryResult` plus a **snapshot fingerprint**: for
//! every source table the query names, its current Iceberg snapshot id +
//! metadata location from the metastore record. A lookup re-computes the
//! fingerprint *before* returning — equality proves no source committed
//! since the cached run, so invalidation is lazy (no write-path hooks).
//!
//! Eligibility (all enforced before any cache traffic): top-level SELECT
//! over the wire from a known account user; no time-travel `AT()` reads;
//! no non-deterministic functions; every referenced object resolves to a
//! real table (views, streams, TVFs, CTE-only queries are skipped in v1).

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::sql::sqlparser::ast::{
    visit_expressions, visit_relations, Expr, ObjectName, Query,
};
use dashmap::DashMap;

use catalog_metastore::TableIdent;

use crate::models::{QueryContext, QueryResult, SessionMetadataAttr};
use crate::session::UserSession;

/// Entries older than this are treated as misses and evicted (Snowflake
/// parity: 24h).
const TTL_MS: u64 = 24 * 60 * 60 * 1000;
/// Per-entry size cap: larger results are not cached.
const MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;
/// Total entry cap; eviction drops the oldest when exceeded.
const MAX_ENTRIES: usize = 1024;

/// Functions whose value varies per call/session: queries referencing any
/// of them are never cached (Snowflake semantics).
const NON_DETERMINISTIC: &[&str] = &[
    "RANDOM",
    "RAND",
    "UUID",
    "GEN_RANDOM_UUID",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "NOW",
    "SYSDATE",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NEXTVAL",
];

#[derive(Debug)]
struct Entry {
    fingerprint: u64,
    stored_ms: u64,
    result: QueryResult,
}

/// Process-wide cache. `OnceLock` global rather than a session/service
/// field: the cache must span sessions (Snowflake reuses results across a
/// user's sessions) and this keeps the wiring out of every constructor.
#[derive(Debug, Default)]
pub struct ResultCache {
    map: DashMap<String, Entry>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn global() -> &'static ResultCache {
    static CACHE: OnceLock<ResultCache> = OnceLock::new();
    CACHE.get_or_init(ResultCache::default)
}

#[must_use]
pub fn has_non_deterministic_function(query: &Query) -> bool {
    let mut found = false;
    let _ = visit_expressions(query, &mut |expr: &Expr| {
        if !found
            && let Expr::Function(func) = expr
        {
            let upper = func
                .name
                .0
                .last()
                .map(|p| p.to_string().to_ascii_uppercase())
                .unwrap_or_default();
            if NON_DETERMINISTIC.contains(&upper.as_str()) {
                found = true;
            }
        }
        if found {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    });
    found
}

/// Stable cache key for an eligible query, `None` when not eligible.
/// The user name is part of the key (a cached result must never cross
/// users — grants can differ); role and object state are covered by the
/// fingerprint.
#[must_use]
pub fn cache_key(session: &UserSession, sql: &str) -> Option<String> {
    let user = session.metadata.attr(SessionMetadataAttr::UserName)?;
    let warehouse = session.current_warehouse_name().unwrap_or_default();
    // Normalization v1: case-fold + trim. Comments/different spacing in
    // identical-position queries are the common case.
    Some(format!(
        "{user}|{warehouse}|{}",
        sql.trim().to_ascii_lowercase()
    ))
}

/// Snapshot fingerprint over the query's referenced tables. `None` means
/// "do not cache" (an unresolvable reference: stream, view, TVF, CTE-only
/// name, or unknown object).
pub async fn fingerprint(session: &Arc<UserSession>, query: &Query) -> Option<u64> {
    if has_non_deterministic_function(query) {
        return None;
    }
    // CTE aliases are not metastore objects — exclude them from resolution.
    let mut ctes: BTreeSet<String> = BTreeSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            ctes.insert(cte.alias.name.value.to_ascii_lowercase());
        }
    }
    let mut refs: BTreeSet<String> = BTreeSet::new();
    let _ = visit_relations(query, &mut |relation: &ObjectName| {
        let name = relation
            .0
            .iter()
            .map(|part| part.to_string().to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(".");
        if !ctes.contains(&name) {
            refs.insert(name);
        }
        std::ops::ControlFlow::<()>::Continue(())
    });

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // Role participates: the same user under a different role may see
    // different rows (M10 grants).
    session
        .get_session_variable("role")
        .unwrap_or_default()
        .hash(&mut hasher);
    for name in refs {
        let ident = resolve_ref(session, &name).await?;
        let Some(record) = session
            .metastore
            .get_table(&ident)
            .await
            .ok()
            .flatten() else {
            // Streams, views, TVFs, temp objects — not cacheable in v1.
            return None;
        };
        let snapshot = record
            .data
            .metadata
            .current_snapshot(None)
            .ok()
            .flatten()
            .map(|s| *s.snapshot_id())
            .unwrap_or_default();
        // Metadata location changes on every commit AND on table
        // replacement — belt-and-braces next to the snapshot id.
        (name, snapshot, record.data.metadata_location).hash(&mut hasher);
    }
    Some(hasher.finish())
}

/// Resolve a (possibly unqualified) object name against the session
/// context into a metastore `TableIdent`.
async fn resolve_ref(session: &Arc<UserSession>, name: &str) -> Option<TableIdent> {
    use datafusion::sql::sqlparser::ast::{Ident, ObjectNamePart};
    let parts: Vec<ObjectNamePart> = name
        .split('.')
        .filter(|p| !p.is_empty())
        .map(|p| ObjectNamePart::Identifier(Ident::new(p)))
        .collect();
    let query = crate::query::UserQuery::new(Arc::clone(session), String::new(), QueryContext::default());
    let normalized = query.resolve_table_object_name(parts).ok()?;
    Some(normalized.into())
}

/// Validate + fetch. `Ok(None)` = miss (or stale).
pub async fn lookup(key: &str, fingerprint: u64) -> Option<QueryResult> {
    let cache = global();
    let now = now_ms();
    let mut stale_key: Option<String> = None;
    for pair in cache.map.iter() {
        if now.saturating_sub(pair.value().stored_ms) > TTL_MS {
            stale_key = Some(pair.key().clone());
        }
    }
    if let Some(k) = stale_key {
        cache.map.remove(&k);
    }
    let entry = cache.map.get(key)?;
    if entry.fingerprint != fingerprint
        || now.saturating_sub(entry.stored_ms) > TTL_MS
    {
        return None;
    }
    let mut result = entry.result.clone();
    result.metrics.clear();
    Some(result)
}

/// Store an executed SELECT result (silently skips oversized entries).
pub fn store(key: String, fingerprint: u64, result: &QueryResult) {
    let bytes: usize = result
        .records
        .iter()
        .map(|b| b.get_array_memory_size())
        .sum::<usize>()
        // Schema metadata is small relative to rows; a flat constant keeps
        // the cap simple.
        + 4096;
    if bytes > MAX_ENTRY_BYTES {
        return;
    }
    let cache = global();
    if cache.map.len() >= MAX_ENTRIES && !cache.map.contains_key(&key) {
        // Drop the oldest entry (no background janitor in v1).
        let mut oldest: Option<(String, u64)> = None;
        for pair in cache.map.iter() {
            let take = match &oldest {
                Some((_, ms)) => pair.value().stored_ms < *ms,
                None => true,
            };
            if take {
                oldest = Some((pair.key().clone(), pair.value().stored_ms));
            }
        }
        if let Some((k, _)) = oldest {
            cache.map.remove(&k);
        }
    }
    cache.map.insert(
        key,
        Entry {
            fingerprint,
            stored_ms: now_ms(),
            result: result.clone(),
        },
    );
}

/// Test helper: wipe the process-wide cache between cases.
#[cfg(test)]
pub fn clear_for_tests() {
    global().map.clear();
}

#[cfg(test)]
mod serialization {
    // Tests share the process-wide cache; cargo runs test fns on parallel
    // threads, so without a guard one test's clear_for_tests() can wipe
    // another's entry between store and lookup (observed flake).
    static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
    pub fn lock() -> std::sync::MutexGuard<'static, ()> {
        GUARD.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::SnowflakeDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    /// Parse one statement; tests panic on failure (explicit, not unwrap).
    fn parse_query(sql: &str) -> Query {
        let dialect = SnowflakeDialect {};
        let mut parser = match Parser::new(&dialect).try_with_sql(sql) {
            Ok(p) => p,
            Err(e) => panic!("parse failed for {sql:?}: {e}"),
        };
        match parser.parse_statement() {
            Ok(Statement::Query(q)) => *q,
            other => panic!("expected a query for {sql:?}, got {other:?}"),
        }
    }

    fn empty_result() -> QueryResult {
        QueryResult::new(
            vec![],
            Arc::new(ArrowSchema::new(vec![Field::new(
                "x",
                DataType::Int64,
                false,
            )])),
        )
    }

    #[test]
    fn volatile_queries_are_not_cacheable() {
        assert!(has_non_deterministic_function(&parse_query(
            "SELECT RANDOM() FROM t"
        )));
        assert!(has_non_deterministic_function(&parse_query(
            "SELECT CURRENT_TIMESTAMP FROM t"
        )));
        assert!(has_non_deterministic_function(&parse_query(
            "SELECT uuid() FROM t"
        )));
        assert!(!has_non_deterministic_function(&parse_query(
            "SELECT SUM(id) FROM t"
        )));
    }

    #[tokio::test]
    async fn fingerprint_mismatch_is_a_miss() {
        let _serial = serialization::lock();
        clear_for_tests();
        let key = "u|wh|select * from t".to_owned();
        store(key.clone(), 11, &empty_result());
        assert!(lookup(&key, 11).await.is_some());
        // Source committed a new snapshot -> fingerprint differs -> miss.
        assert!(lookup(&key, 12).await.is_none());
        clear_for_tests();
    }

    #[tokio::test]
    async fn expired_entries_are_misses() {
        let _serial = serialization::lock();
        clear_for_tests();
        let key = "u|wh|select 1".to_owned();
        store(key.clone(), 7, &empty_result());
        // Age the entry past the TTL by rewriting its stamp.
        if let Some(mut e) = global().map.get_mut(&key) {
            e.stored_ms = now_ms().saturating_sub(TTL_MS + 1);
        }
        assert!(lookup(&key, 7).await.is_none());
        clear_for_tests();
    }

    #[tokio::test]
    async fn lookup_is_key_scoped() {
        let _serial = serialization::lock();
        clear_for_tests();
        store("u|wh|q1".to_owned(), 5, &empty_result());
        assert!(lookup("u|wh|q2", 5).await.is_none());
        assert!(lookup("u|wh|q1", 5).await.is_some());
        clear_for_tests();
    }
}
