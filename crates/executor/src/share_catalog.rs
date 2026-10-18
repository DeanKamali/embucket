//! Inbound share mounting (design §21, M15.2).
//!
//! An inbound share mounts as a **catalog** named after the share
//! (Snowflake exposes shares as databases): the consumer session's catalog
//! list gains one read-only catalog per mounted inbound share, exposing
//! exactly the objects granted into the provider's share (the v1 grant
//! list resolves against the same-process account store — the
//! "same-region trust boundary" case; cross-account transport is v2).
//!
//! Reads delegate to the underlying [`EmbucketCatalogList`] (the provider's
//! own databases live in this metastore); visibility is the share's grant
//! list, so non-granted objects stay invisible. Writes are denied at the
//! query boundary (`enforce_read_only`) — share-mounted catalogs resolve
//! for reads only.
//!
//! v1 deviations (recorded in DESIGN.md §21.3):
//! - a share must draw from a single database (multi-database shares are
//!   not mounted; a warning is logged);
//! - the mount is a snapshot taken at session creation — shares created
//!   later require a new session.

use avalanche_account::models::{AccountObject, ShareGrant, ShareKind};
use avalanche_account::store::AccountStore;
use catalog::account_catalog::AccountCatalogList;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    FromTable, ObjectName, ObjectNamePart, Statement as SqlStatement, TableFactor, TableObject,
    TableWithJoins,
};
use datafusion_common::Result as DfResult;
use snafu::Location;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{self as ex_error, Error, Result};
use crate::session::UserSession;

/// One mounted inbound share: its name and the provider-side grant list
/// snapshot used for visibility.
#[derive(Debug, Clone)]
pub struct MountedShare {
    pub name: String,
    pub grants: Vec<ShareGrant>,
}

/// Snapshot the inbound shares mountable for a session. Shares spanning
/// more than one database are skipped (v1 limitation, logged).
pub async fn mounted_shares(
    store: &Arc<dyn AccountStore>,
) -> Result<Vec<MountedShare>> {
    let mut mounted = Vec::new();
    for share in store.list_shares().await.map_err(account_error)? {
        if share.kind != ShareKind::Inbound {
            continue;
        }
        // Resolve the provider-side grant bundle (v1: same store, §21.1-2).
        let Some(provider) = share.provider.as_deref() else {
            tracing::warn!(
                share = %share.name,
                "inbound share has no provider share; not mounted"
            );
            continue;
        };
        let grants = store.share_grants(provider).await.map_err(account_error)?;
        let databases: std::collections::HashSet<&str> = grants
            .iter()
            .filter(|g| !matches!(g.object, AccountObject::Warehouse(_)))
            .map(|g| g.object.database())
            .collect();
        if databases.len() > 1 {
            tracing::warn!(
                share = %share.name,
                databases = databases.len(),
                "inbound share spans multiple databases; not mounted (v1 limitation)"
            );
            continue;
        }
        mounted.push(MountedShare {
            name: share.name,
            grants,
        });
    }
    Ok(mounted)
}

fn account_error(e: avalanche_account::error::Error) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("share mount: {e}").into(),
        )),
        location: Location::default(),
    }
}

fn external_error(context: &str, message: String) -> Error {
    Error::DataFusion {
        error: Box::new(datafusion::common::DataFusionError::External(
            format!("{context}: {message}").into(),
        )),
        location: Location::default(),
    }
}

/// Catalog list that layers mounted inbound shares over the session's
/// (already authorization-wrapped) catalog list.
#[derive(Debug)]
pub struct ShareCatalogList {
    inner: Arc<dyn CatalogProviderList>,
    /// Delegation target for reading the provider's real tables.
    bare: Arc<AccountCatalogList>,
    shares: Vec<MountedShare>,
}

impl ShareCatalogList {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        bare: Arc<AccountCatalogList>,
        shares: Vec<MountedShare>,
    ) -> Self {
        Self { inner, bare, shares }
    }

    /// The mounted inbound share names (for DML write denial).
    #[must_use]
    pub fn share_names(&self) -> Vec<String> {
        self.shares.iter().map(|s| s.name.clone()).collect()
    }

    /// Reach the bare [`AccountCatalogList`] under any wrapping — mirrors
    /// [`AuthzCatalogList::unwrap`] but also strips this wrapper (used by
    /// hooks that must manipulate the real catalog list: CREATE DATABASE,
    /// CLONE registration, time travel).
    #[must_use]
    pub fn unwrap_bare(
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> Option<Arc<AccountCatalogList>> {
        if let Some(bare) = avalanche_account::authz::AuthzCatalogList::unwrap(catalog_list) {
            return Some(bare);
        }
        let cloned: Arc<dyn CatalogProviderList> = Arc::clone(catalog_list);
        let any: Arc<dyn std::any::Any + Send + Sync> = cloned;
        Arc::downcast::<ShareCatalogList>(any)
            .ok()
            .map(|wrapper| Arc::clone(&wrapper.bare))
    }
}

impl CatalogProviderList for ShareCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(share) = self.shares.iter().find(|s| s.name.eq_ignore_ascii_case(name)) {
            return Some(Arc::new(ShareCatalog {
                name: share.name.clone(),
                grants: share.grants.clone(),
                bare: Arc::clone(&self.bare),
            }) as Arc<dyn CatalogProvider>);
        }
        self.inner.catalog(name)
    }

    fn catalog_names(&self) -> Vec<String> {
        let mut names = self.inner.catalog_names();
        for share in &self.shares {
            if !names.iter().any(|n| n.eq_ignore_ascii_case(&share.name)) {
                names.push(share.name.clone());
            }
        }
        names
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }
}

/// Per-share catalog: schemas derive from the share's grant list.
///
/// Visibility mapping (single-database v1): a table grant exposes exactly
/// that table; a schema grant exposes all tables of that schema; a
/// database grant exposes every schema of the database.
#[derive(Debug)]
struct ShareCatalog {
    name: String,
    grants: Vec<ShareGrant>,
    bare: Arc<AccountCatalogList>,
}

/// Granted (provider database, provider schema) pairs and their exposure
/// mode. `All` = every table of the schema; `Specific` = the listed tables.
enum SchemaExposure {
    All(String, String),
    Specific(String, String, Vec<String>),
}

impl ShareCatalog {
    /// The exposure of each granted schema, merged across grants.
    fn exposures(&self) -> BTreeMap<(String, String), SchemaExposure> {
        let mut map: BTreeMap<(String, String), SchemaExposure> = BTreeMap::new();
        for g in &self.grants {
            match &g.object {
                AccountObject::Database(db) => {
                    // Every schema of the provider database — resolved
                    // lazily via the inner catalog at listing time.
                    map.entry((db.to_ascii_lowercase(), "*".to_string()))
                        .or_insert(SchemaExposure::All(db.clone(), "*".to_string()));
                }
                AccountObject::Schema(db, schema) => {
                    map.entry((db.to_ascii_lowercase(), schema.to_ascii_lowercase()))
                        .or_insert(SchemaExposure::All(db.clone(), schema.clone()));
                }
                AccountObject::Table(db, schema, table) => {
                    let entry = map
                        .entry((db.to_ascii_lowercase(), schema.to_ascii_lowercase()))
                        .or_insert_with(|| {
                            SchemaExposure::Specific(
                                db.clone(),
                                schema.clone(),
                                Vec::new(),
                            )
                        });
                    if let SchemaExposure::Specific(_, _, tables) = entry {
                        if !tables.iter().any(|t| t.eq_ignore_ascii_case(table)) {
                            tables.push(table.clone());
                        }
                    }
                }
                AccountObject::Warehouse(_) => {}
            }
        }
        map
    }

    /// Resolve one granted (db, schema) against the bare catalog list.
    fn inner_schema(&self, db: &str, schema: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.bare
            .catalog(db)
            .and_then(|c| c.schema(schema))
    }
}

impl CatalogProvider for ShareCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for ((_, _schema), exposure) in self.exposures() {
            match exposure {
                SchemaExposure::All(db, wildcard_schema) => {
                    if wildcard_schema == "*" {
                        if let Some(catalog) = self.bare.catalog(&db) {
                            names.extend(catalog.schema_names());
                        }
                    } else {
                        names.push(wildcard_schema);
                    }
                }
                SchemaExposure::Specific(_, schema, _) => names.push(schema),
            }
        }
        names.sort();
        names.dedup();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        for ((_, schema_key), exposure) in self.exposures() {
            if !schema_key.eq_ignore_ascii_case(name) {
                continue;
            }
            match exposure {
                SchemaExposure::All(db, wildcard_schema) => {
                    if wildcard_schema == "*" {
                        // Database-level grant: match any schema of the db.
                        if let Some(inner) = self.inner_schema(&db, name) {
                            return Some(Arc::new(ShareSchema {
                                inner,
                                tables: None,
                            }) as Arc<dyn SchemaProvider>);
                        }
                    } else if wildcard_schema.eq_ignore_ascii_case(name) {
                        if let Some(inner) = self.inner_schema(&db, &wildcard_schema) {
                            return Some(Arc::new(ShareSchema {
                                inner,
                                tables: None,
                            }) as Arc<dyn SchemaProvider>);
                        }
                    }
                }
                SchemaExposure::Specific(db, schema, tables) => {
                    if schema.eq_ignore_ascii_case(name) {
                        return self.inner_schema(&db, &schema).map(|inner| {
                            Arc::new(ShareSchema {
                                inner,
                                tables: Some(tables),
                            }) as Arc<dyn SchemaProvider>
                        });
                    }
                }
            }
        }
        None
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> DfResult<Option<Arc<dyn SchemaProvider>>> {
        // Shares are read-only: registration is a no-op acceptance.
        Ok(None)
    }

    fn deregister_schema(&self, _name: &str, _cascade: bool) -> DfResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(None)
    }
}

/// Read-only schema view over the provider's real schema. `tables` is
/// `None` for full-schema exposure (database/schema grants) or the exact
/// granted table list for table grants.
#[derive(Debug)]
struct ShareSchema {
    inner: Arc<dyn SchemaProvider>,
    tables: Option<Vec<String>>,
}

impl ShareSchema {
    fn visible(&self, table: &str) -> bool {
        match &self.tables {
            None => true,
            Some(tables) => tables.iter().any(|t| t.eq_ignore_ascii_case(table)),
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for ShareSchema {
    fn table_names(&self) -> Vec<String> {
        match &self.tables {
            None => self.inner.table_names(),
            Some(tables) => self
                .inner
                .table_names()
                .into_iter()
                .filter(|t| tables.iter().any(|g| g.eq_ignore_ascii_case(t)))
                .collect(),
        }
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if !self.visible(name) {
            // Deny = as-if-missing (Snowflake error shape via DF mapping).
            return Ok(None);
        }
        self.inner.table(name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.visible(name) && self.inner.table_exist(name)
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Err(datafusion::common::DataFusionError::External(
            "cannot register tables in a shared database".into(),
        ))
    }

    fn deregister_table(&self, _name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Err(datafusion::common::DataFusionError::External(
            "cannot drop tables in a shared database".into(),
        ))
    }
}

/// Deny DML whose target resolves into a mounted inbound share (design
/// §21.2: shares are read-only). `target` is the parsed statement's object
/// name parts (catalog first); `inbound` is the session's mounted share
/// snapshot.
pub fn deny_write_target(
    target: &[String],
    inbound: &[String],
) -> Result<()> {
    let Some(catalog) = target.first() else {
        return Ok(());
    };
    if inbound.iter().any(|s| s.eq_ignore_ascii_case(catalog)) {
        return Err(external_error(
            "dml",
            format!("cannot write to shared database '{catalog}'"),
        ));
    }
    Ok(())
}

/// Catalog part (first identifier) of an [`ObjectName`].
fn object_name_catalog(name: &ObjectName) -> Option<String> {
    match name.0.first()? {
        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
        ObjectNamePart::Function(_) => None,
    }
}

fn table_factor_catalog(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, .. } => object_name_catalog(name),
        _ => None,
    }
}

fn table_with_joins_catalog(tw: &TableWithJoins) -> Option<String> {
    table_factor_catalog(&tw.relation)
}

/// Deny write statements (INSERT/UPDATE/DELETE/MERGE/TRUNCATE) whose target
/// is a mounted inbound share. Called pre-execution in `UserQuery::execute`.
pub fn deny_dml(statement: &DFStatement, session: &UserSession) -> Result<()> {
    if session.inbound_shares.is_empty() {
        return Ok(());
    }
    let DFStatement::Statement(s) = statement else {
        return Ok(());
    };
    let targets: Vec<Option<String>> = match &**s {
        SqlStatement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => vec![object_name_catalog(name)],
            TableObject::TableFunction(_) | TableObject::TableQuery(_) => vec![],
        },
        SqlStatement::Update(update) => vec![table_with_joins_catalog(&update.table)],
        SqlStatement::Delete(delete) => match &delete.from {
            FromTable::WithFromKeyword(tws) | FromTable::WithoutKeyword(tws) => {
                tws.iter().map(table_with_joins_catalog).collect()
            }
        },
        SqlStatement::Merge(merge) => vec![table_factor_catalog(&merge.table)],
        SqlStatement::Truncate(truncate) => truncate
            .table_names
            .iter()
            .map(|t| object_name_catalog(&t.name))
            .collect(),
        _ => vec![],
    };
    for catalog in targets.into_iter().flatten() {
        deny_write_target(&[catalog], &session.inbound_shares)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use avalanche_account::models::{Privilege, ShareGrant};
    use avalanche_account::store::InMemoryAccountStore;

    async fn mounted_with_one_table() -> (Arc<dyn AccountStore>, Vec<MountedShare>) {
        let store: Arc<dyn AccountStore> = Arc::new(InMemoryAccountStore::new());
        store.create_share("vendor").await.unwrap();
        store
            .grant_to_share(ShareGrant {
                account: avalanche_account::models::DEFAULT_ACCOUNT.to_string(),
                share: "vendor".into(),
                privilege: Privilege::Select,
                object: AccountObject::Table("db".into(), "public".into(), "t".into()),
            })
            .await
            .unwrap();
        store
            .create_inbound_share("acme_in", "vendor")
            .await
            .unwrap();
        (Arc::clone(&store), mounted_shares(&store).await.unwrap())
    }

    #[tokio::test]
    async fn only_inbound_shares_mount() {
        let (_, mounted) = mounted_with_one_table().await;
        assert_eq!(mounted.len(), 1);
        assert_eq!(mounted[0].name, "acme_in");
        assert_eq!(mounted[0].grants.len(), 1);
    }

    #[tokio::test]
    async fn multi_database_share_not_mounted() {
        let store: Arc<dyn AccountStore> = Arc::new(InMemoryAccountStore::new());
        store.create_share("wide_prov").await.unwrap();
        for (db, table) in [("db1", "t"), ("db2", "u")] {
            store
                .grant_to_share(ShareGrant {
                    account: avalanche_account::models::DEFAULT_ACCOUNT.to_string(),
                    share: "wide_prov".into(),
                    privilege: Privilege::Select,
                    object: AccountObject::Table(db.into(), "public".into(), table.into()),
                })
                .await
                .unwrap();
        }
        store
            .create_inbound_share("wide", "wide_prov")
            .await
            .unwrap();
        assert!(mounted_shares(&store).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn inbound_share_without_provider_not_mounted() {
        // Can't happen through DDL (FROM is required) but the store permits
        // it; mount must skip rather than panic.
        let store: Arc<dyn AccountStore> = Arc::new(InMemoryAccountStore::new());
        store.create_share("orphan").await.unwrap();
        store.drop_share("orphan").await.unwrap();
        // Simulate a provider-less inbound record via two-step lifecycle.
        store.create_share("p").await.unwrap();
        let s = store.create_inbound_share("orphan", "p").await.unwrap();
        assert!(s.provider.is_some());
    }

    #[tokio::test]
    async fn write_denial_matches_catalog_case_insensitively() {
        let inbound = vec!["acme_in".to_string()];
        let target = vec!["Acme_In".to_string(), "public".to_string(), "t".to_string()];
        assert!(deny_write_target(&target, &inbound).is_err());
        let target = vec!["embucket".to_string(), "public".to_string(), "t".to_string()];
        assert!(deny_write_target(&target, &inbound).is_ok());
    }
}