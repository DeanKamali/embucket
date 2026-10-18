//! Replication & failover groups (M15.5, design §21.5).
//!
//! A replication group in the source account replicates a listed set of
//! tables into every allowed target account's *same-named* database.
//! Refresh is a cross-account metadata clone over the shared Iceberg
//! storage (zero data copy): the replica table record registers the
//! source's current metadata, stamped
//! `properties["avalanche.replica"] = <group name>`.
//!
//! Enforcement: DML (INSERT/UPDATE/DELETE/MERGE/TRUNCATE) against a
//! replica-stamped table is denied at the query boundary — the check is
//! live against the session's account-scoped metastore, so a failover
//! promotion (which removes the stamp) takes effect immediately, no new
//! session needed. The source account's tables are never stamped.

use crate::error::{Error, Result};
use crate::session::UserSession;
use avalanche_account::models::{AccountObject, ReplicationGroup};
use avalanche_account::store::AccountStore;
use catalog_metastore::accounted::AccountedMetastore;
use catalog_metastore::metastore::Metastore;
use catalog_metastore::models::{
    database::Database,
    schema::{Schema, SchemaIdent},
    table::TableIdent,
};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    FromTable, Ident, ObjectName, ObjectNamePart, Statement as SqlStatement, TableFactor,
    TableObject, TableWithJoins,
};
use snafu::Location;
use std::sync::Arc;

/// Table property marking a read-only replica (M15.5): the value is the
/// replication group that refreshed it. Defined in `catalog-metastore` so
/// the catalog cache can validate stale replica providers; re-exported here
/// as the executor-side seam.
pub use catalog_metastore::models::table::REPLICA_PROPERTY;

fn external_error(context: &str, err: impl std::fmt::Display) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {err}").into(),
        )),
        location: Location::default(),
    }
}

/// The raw (un-scoped) metastore behind the session's view. Cross-account
/// refresh builds one `AccountedMetastore` view per endpoint over it.
#[must_use]
pub fn raw_metastore(session: &UserSession) -> Arc<dyn Metastore> {
    session
        .metastore
        .inner_raw()
        .unwrap_or_else(|| Arc::clone(&session.metastore))
}

// ================= replica read-only enforcement =================

/// Identifier parts of an [`ObjectName`] (quote styles preserved); `None`
/// when any part is not a plain identifier.
fn object_name_idents(name: &ObjectName) -> Option<Vec<Ident>> {
    let mut out = Vec::with_capacity(name.0.len());
    for part in &name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => out.push(ident.clone()),
            ObjectNamePart::Function(_) => return None,
        }
    }
    Some(out)
}

fn table_factor_idents(factor: &TableFactor) -> Option<Vec<Ident>> {
    match factor {
        TableFactor::Table { name, .. } => object_name_idents(name),
        _ => None,
    }
}

fn table_with_joins_idents(tw: &TableWithJoins) -> Option<Vec<Ident>> {
    table_factor_idents(&tw.relation)
}

/// Deny write statements (INSERT/UPDATE/DELETE/MERGE/TRUNCATE) whose target
/// table carries the replica stamp. Live metastore lookup — promotion and
/// re-stamping take effect without a new session. Called pre-execution in
/// `UserQuery::execute` (alongside the inbound-share denial).
///
/// # Errors
/// When the target is a replica, or the metastore lookup fails.
pub async fn deny_replica_dml(statement: &DFStatement, session: &UserSession) -> Result<()> {
    let DFStatement::Statement(s) = statement else {
        return Ok(());
    };
    let norm = |ident: &Ident| session.ident_normalizer.normalize(ident.clone());
    let targets: Vec<Vec<String>> = match &**s {
        SqlStatement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => object_name_idents(name)
                .map(|ids| ids.iter().map(norm).collect())
                .into_iter()
                .collect(),
            TableObject::TableFunction(_) | TableObject::TableQuery(_) => vec![],
        },
        SqlStatement::Update(update) => table_with_joins_idents(&update.table)
            .map(|ids| ids.iter().map(norm).collect())
            .into_iter()
            .collect(),
        SqlStatement::Delete(delete) => match &delete.from {
            FromTable::WithFromKeyword(tws) | FromTable::WithoutKeyword(tws) => tws
                .iter()
                .filter_map(table_with_joins_idents)
                .map(|ids| ids.iter().map(norm).collect())
                .collect(),
        },
        SqlStatement::Merge(merge) => table_factor_idents(&merge.table)
            .map(|ids| ids.iter().map(norm).collect())
            .into_iter()
            .collect(),
        SqlStatement::Truncate(truncate) => truncate
            .table_names
            .iter()
            .filter_map(|t| object_name_idents(&t.name))
            .map(|ids| ids.iter().map(norm).collect())
            .collect(),
        _ => vec![],
    };
    for parts in targets {
        if is_replica_target(session, &parts).await? {
            return Err(external_error(
                "dml",
                format!(
                    "cannot write to table '{}': it is a read-only replica \
                     (refreshed by a replication group; use FAILOVER to promote it)",
                    parts.join(".")
                ),
            ));
        }
    }
    Ok(())
}

/// Whether the (possibly unqualified) target resolves to a table stamped
/// [`REPLICA_PROPERTY`] in the session's account view.
async fn is_replica_target(session: &UserSession, parts: &[String]) -> Result<bool> {
    if parts.is_empty() || parts.len() > 3 {
        return Ok(false);
    }
    let database = session
        .get_session_variable("database")
        .unwrap_or_else(|| "embucket".to_string());
    let schema = session
        .get_session_variable("schema")
        .unwrap_or_else(|| "public".to_string());
    let ident = match parts.len() {
        3 => TableIdent::new(&parts[0], &parts[1], &parts[2]),
        2 => TableIdent::new(&database, &parts[0], &parts[1]),
        1 => TableIdent::new(&database, &schema, &parts[0]),
        _ => return Ok(false),
    };
    let Some(table) = session
        .metastore
        .get_table(&ident)
        .await
        .map_err(|e| external_error("dml", e))?
    else {
        return Ok(false);
    };
    Ok(table.data.properties.contains_key(REPLICA_PROPERTY))
}

// ================= refresh (cross-account metadata clone) =================

/// Refresh `group`: for every allowed target account, register each listed
/// table's *current* source metadata as a replica-stamped table record in
/// the target's same-named database (created on demand with the target as
/// owner; schema too). Re-refresh upserts in place — no tombstone churn.
/// Returns the number of replica records written.
///
/// # Errors
/// When a listed table (or its database) is missing in the source account,
/// or a metastore operation fails.
#[allow(clippy::too_many_lines)]
pub async fn refresh_group(
    session: &UserSession,
    group: &ReplicationGroup,
    store: &Arc<dyn AccountStore>,
) -> Result<usize> {
    let raw = raw_metastore(session);
    let src = AccountedMetastore::new(Arc::clone(&raw), &group.account);
    let mut written = 0usize;
    for target in &group.accounts {
        let tgt = AccountedMetastore::new(Arc::clone(&raw), target);
        for object in &group.tables {
            let AccountObject::Table(db, schema, table) = object else {
                continue; // v1: OBJECT_TYPES = TABLES only
            };
            let tid = TableIdent::new(db, schema, table);
            let Some(source) = src
                .get_table(&tid)
                .await
                .map_err(|e| external_error("refresh", e))?
            else {
                return Err(external_error(
                    "refresh",
                    format!(
                        "table '{db}.{schema}.{table}' does not exist in account '{}'",
                        group.account
                    ),
                ));
            };
            // Target database on demand (target account owns it).
            if tgt
                .get_database(db)
                .await
                .map_err(|e| external_error("refresh", e))?
                .is_none()
            {
                let source_db = src
                    .get_database(db)
                    .await
                    .map_err(|e| external_error("refresh", e))?
                    .ok_or_else(|| {
                        external_error(
                            "refresh",
                            format!("database '{db}' does not exist in source account"),
                        )
                    })?;
                tgt.create_database(
                    db,
                    Database {
                        ident: db.clone(),
                        properties: source_db.data.properties.clone(),
                        volume: source_db.data.volume.clone(),
                        should_refresh: source_db.data.should_refresh,
                        account: target.clone(),
                    },
                )
                .await
                .map_err(|e| external_error("refresh", e))?;
            }
            // Target schema on demand (mirrors the source's properties).
            let sid = SchemaIdent::new(db.clone(), schema.clone());
            if tgt
                .get_schema(&sid)
                .await
                .map_err(|e| external_error("refresh", e))?
                .is_none()
            {
                let source_schema = src
                    .get_schema(&sid)
                    .await
                    .map_err(|e| external_error("refresh", e))?;
                tgt.create_schema(
                    &sid,
                    Schema {
                        ident: sid.clone(),
                        properties: source_schema.map(|s| s.data.properties).unwrap_or(None),
                    },
                )
                .await
                .map_err(|e| external_error("refresh", e))?;
            }
            // Register the replica: the source's current metadata record,
            // restamped with the target-qualified ident + the replica stamp
            // (upsert — replaces a previous refresh's record).
            let qualified = tgt.qualify_ident(&tid);
            let mut replica = source.data;
            replica.ident = qualified.clone();
            replica
                .properties
                .insert(REPLICA_PROPERTY.to_string(), group.name.clone());
            raw.register_table(&qualified, replica)
                .await
                .map_err(|e| external_error("refresh", e))?;
            written += 1;
        }
    }
    let now_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
    store
        .alter_replication_group_refreshed(&group.name, now_ms)
        .await
        .map_err(|e| external_error("refresh", e))?;
    Ok(written)
}

/// Promote `target_account`'s replicas of `group` to writable: remove the
/// replica stamp from every listed table that carries it. The group is
/// then marked failed-over (no further refreshes). REPLICATION groups
/// never promote (validated at the DDL layer).
///
/// # Errors
/// When a metastore operation or the failed-over stamp write fails.
pub async fn failover_group(
    session: &UserSession,
    group: &ReplicationGroup,
    target_account: &str,
    store: &Arc<dyn AccountStore>,
) -> Result<()> {
    let raw = raw_metastore(session);
    let tgt = AccountedMetastore::new(Arc::clone(&raw), target_account);
    for object in &group.tables {
        let AccountObject::Table(db, schema, table) = object else {
            continue;
        };
        let tid = TableIdent::new(db, schema, table);
        let qualified = tgt.qualify_ident(&tid);
        if let Some(record) = raw
            .get_table(&qualified)
            .await
            .map_err(|e| external_error("failover", e))?
        {
            if record.data.properties.contains_key(REPLICA_PROPERTY) {
                let mut promoted = record.data;
                promoted.properties.remove(REPLICA_PROPERTY);
                raw.register_table(&qualified, promoted)
                    .await
                    .map_err(|e| external_error("failover", e))?;
            }
        }
    }
    store
        .alter_replication_group_failed_over(&group.name)
        .await
        .map_err(|e| external_error("failover", e))
}

#[cfg(test)]
mod tests {
    use super::REPLICA_PROPERTY;

    #[test]
    fn replica_property_is_stable() {
        assert_eq!(REPLICA_PROPERTY, "avalanche.replica");
    }
}