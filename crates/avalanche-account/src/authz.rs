//! Session authorization (design §17, M10.2/M10.3).
//!
//! [`SessionAuthz`] materializes the active role's grants into a sync
//! snapshot on activation; the [`AuthzCatalogList`] / [`AuthzCatalog`] /
//! [`AuthzSchema`] wrappers are pure snapshot lookups so they fit
//! DataFusion's sync trait methods. Deny = as-if-missing (`None`) —
//! DataFusion's not-found errors are rendered as Snowflake's "does not
//! exist or not authorized" by the existing error mapping.
//!
//! `ACCOUNTADMIN` members bypass every check (bootstrap identity).
//! Databases carrying `properties["tenant"]` are hard-filtered for users
//! whose tenant label differs (or who have none).

use crate::error::{self as account_error, Result};
use crate::models::{AccountObject, Grant, Privilege, ACCOUNTADMIN};
use crate::store::AccountStore;
use async_trait::async_trait;
use catalog::account_catalog::{AccountCatalogList, IntoAccountCatalogList};
use catalog_metastore::Metastore;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion_common::Result as DfResult;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// Grants + visibility facts materialized for the active role.
#[derive(Debug, Default, Clone)]
pub struct RoleSnapshot {
    /// The active role is `ACCOUNTADMIN`.
    pub admin: bool,
    grants: HashSet<(Privilege, AccountObject)>,
    /// Databases whose `properties["tenant"]` does not match the user's
    /// tenant label (admins bypass). Lowercased idents.
    pub denied_databases: HashSet<String>,
    /// Record-level tenant scoping (design §23.7): schemas whose metastore
    /// record carries a mismatching `properties["tenant"]` label
    /// (`db.schema`, lowercased). Tables inherit at creation, so schema
    /// coverage transitively scopes them.
    pub denied_schemas: HashSet<String>,
}

impl RoleSnapshot {
    fn allowed(&self, privilege: Privilege, object: &AccountObject) -> bool {
        self.grants.contains(&(privilege, object.clone()))
    }
}

/// Per-session authorization context.
#[derive(Debug)]
pub struct SessionAuthz {
    store: Arc<dyn AccountStore>,
    metastore: Arc<dyn Metastore>,
    /// Object name of the user this session belongs to.
    user: String,
    /// Login name (used at login time); kept for diagnostics.
    login_name: String,
    /// The account this session belongs to (M15.4): the loaded user's own
    /// account — with a scoped store this is always the session account.
    account: String,
    role: RwLock<String>,
    snapshot: RwLock<RoleSnapshot>,
    /// Account epoch the snapshot was materialized at (§23.5 result-cache
    /// fingerprint uses the same counter). Re-materialize when it moves.
    materialized_epoch: std::sync::atomic::AtomicU64,
}

impl SessionAuthz {
    /// Build the context for `user` (looked up by login name). The session's
    /// requested role (session variable `role`, if set) is honored first;
    /// errors fall back to the user's default role / `ACCOUNTADMIN`
    /// membership / deny-all. Returns `None` for unknown users (internal
    /// sessions carry no user identity and are not enforced).
    pub async fn new(
        store: Arc<dyn AccountStore>,
        metastore: Arc<dyn Metastore>,
        login_name: &str,
        requested_role: Option<&str>,
    ) -> Result<Option<Arc<Self>>> {
        let Some(u) = store.get_user(login_name).await? else {
            return Ok(None);
        };
        if u.disabled {
            return account_error::NotAllowedSnafu {
                message: format!("user '{login_name}' is disabled"),
            }
            .fail();
        }
        let authz = Self {
            store,
            metastore,
            user: u.name.clone(),
            login_name: u.login_name.to_ascii_lowercase(),
            account: u.account.clone(),
            role: RwLock::new(String::new()),
            snapshot: RwLock::new(RoleSnapshot::default()),
            materialized_epoch: std::sync::atomic::AtomicU64::new(0),
        };
        let candidates = [
            requested_role.map(ToOwned::to_owned),
            u.default_role.clone(),
            Some(ACCOUNTADMIN.to_string()),
        ];
        for candidate in candidates.into_iter().flatten() {
            if authz.activate_role(&candidate).await.is_ok() {
                return Ok(Some(Arc::new(authz)));
            }
        }
        // Deny-all: user holds no role at all.
        *authz.role.write().unwrap() = String::new();
        *authz.snapshot.write().unwrap() = RoleSnapshot::default();
        Ok(Some(Arc::new(authz)))
    }

    /// Activate `role` for this session's user: the user must hold the role
    /// directly, hold it through the role hierarchy (it lies in the
    /// transitive closure of the user's directly granted roles — M17.3),
    /// or hold `ACCOUNTADMIN` (which may assume any role). Refreshes the
    /// snapshot. `ACCOUNTADMIN` activation makes the session an admin.
    pub async fn activate_role(&self, role: &str) -> Result<()> {
        let Some(r) = self.store.get_role(role).await? else {
            return account_error::NotAllowedSnafu {
                message: format!("role '{role}' does not exist"),
            }
            .fail();
        };
        let held = self.store.user_roles(&self.user).await?;
        let holds_admin = held.iter().any(|h| h.eq_ignore_ascii_case(ACCOUNTADMIN));
        let effective = if holds_admin {
            Vec::new()
        } else {
            self.store.effective_roles(&held).await?
        };
        if !holds_admin && !effective.iter().any(|h| h.eq_ignore_ascii_case(&r.name)) {
            return account_error::NotAllowedSnafu {
                message: format!("role '{}' is not granted to user '{}'", r.name, self.user),
            }
            .fail();
        }
        let admin = r.name.eq_ignore_ascii_case(ACCOUNTADMIN);
        self.materialize(&r.name, admin).await?;
        self.materialized_epoch
            .store(crate::store::account_epoch(), std::sync::atomic::Ordering::SeqCst);
        *self.role.write().unwrap() = r.name;
        Ok(())
    }

    /// Re-materialize the snapshot when the account epoch has moved since
    /// the last materialize (M17.4, design §23.7): a session must see the
    /// grants recorded *by itself* during the session — the implicit
    /// OWNERSHIP of a just-created table, an ownership transfer, a
    /// sibling session's GRANT — without re-logging-in. No-op when the
    /// epoch is unchanged or no role is active.
    ///
    /// # Errors
    /// When the account store or metastore rejects the re-materialize.
    pub async fn refresh_if_stale(&self) -> Result<()> {
        if self.active_role().is_empty() {
            return Ok(());
        }
        let now = crate::store::account_epoch();
        if self
            .materialized_epoch
            .load(std::sync::atomic::Ordering::SeqCst)
            == now
        {
            return Ok(());
        }
        let admin = self.is_admin_now();
        let role = self.active_role();
        self.materialize(&role, admin).await?;
        self.materialized_epoch
            .store(now, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn materialize(&self, role: &str, admin: bool) -> Result<()> {
        let mut snapshot = RoleSnapshot {
            admin,
            ..RoleSnapshot::default()
        };
        if !admin {
            // Tenant hard filter: deny databases whose tenant label does
            // not match this user's label. A session with no tenant label
            // sees only unlabeled databases.
            let user = self.store.get_user(&self.user).await?;
            let user_tenant = user
                .as_ref()
                .and_then(|u| u.tenant.as_ref())
                .map(|t| t.to_ascii_lowercase());
            let tenant_mismatch = |label: Option<String>| match (label, user_tenant.clone()) {
                (Some(t), Some(u)) => t != u,
                (Some(_), None) => true,
                (None, _) => false,
            };
            let databases = self
                .metastore
                .list_databases()
                .await
                .map_err(|e| {
                    account_error::InvalidInputSnafu {
                        message: e.to_string(),
                    }
                    .build()
                })?;
            snapshot.denied_databases = databases
                .iter()
                .filter(|db| {
                    let db_tenant = db
                        .data
                        .properties
                        .as_ref()
                        .and_then(|p| p.get("tenant"))
                        .map(|t| t.to_ascii_lowercase());
                    tenant_mismatch(db_tenant)
                })
                .map(|db| db.ident.to_ascii_lowercase())
                .collect();
            // Record-level tenant scoping (design §23.7): every database the
            // session can still see has its schemas checked against their
            // metastore records — a schema record carrying a mismatching
            // `properties["tenant"]` label is denied even though the
            // database itself is shared. Tables inherit the label at
            // creation, so schema coverage transitively scopes them.
            for db in &databases {
                if snapshot.denied_databases.contains(&db.ident.to_ascii_lowercase()) {
                    continue;
                }
                for schema in self
                    .metastore
                    .list_schemas(&db.ident)
                    .await
                    .map_err(|e| {
                        account_error::InvalidInputSnafu {
                            message: e.to_string(),
                        }
                        .build()
                    })?
                {
                    let label = schema
                        .data
                        .properties
                        .as_ref()
                        .and_then(|p| p.get("tenant"))
                        .map(|t| t.to_ascii_lowercase());
                    if tenant_mismatch(label) {
                        snapshot.denied_schemas.insert(format!(
                            "{}.{}",
                            db.ident.to_ascii_lowercase(),
                            schema.ident.schema.to_ascii_lowercase()
                        ));
                    }
                }
            }
            snapshot.grants = {
                // Role hierarchies (M17.3): the active role's effective
                // privilege set unions its own grants with the grants of
                // every role granted beneath it, transitively.
                let effective = self.store.effective_roles(&[role.to_string()]).await?;
                let mut grants = std::collections::HashSet::new();
                for r in &effective {
                    for g in self.store.role_grants(r).await? {
                        grants.insert((g.privilege, g.object));
                    }
                }
                grants
            };
        }
        *self.snapshot.write().unwrap() = snapshot;
        Ok(())
    }

    /// Current active role name (empty before any activation).
    #[must_use]
    pub fn active_role(&self) -> String {
        self.role.read().unwrap().clone()
    }

    /// Whether the active role is `ACCOUNTADMIN`.
    #[must_use]
    pub fn is_admin_now(&self) -> bool {
        self.snapshot.read().unwrap().admin
    }

    /// The account this session belongs to (M15.4).
    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }

    /// Sync grant check against the materialized snapshot.
    #[must_use]
    pub fn allowed_now(&self, privilege: Privilege, object: &AccountObject) -> bool {
        let snapshot = self.snapshot.read().unwrap();
        snapshot.admin || snapshot.allowed(privilege, object)
    }

    /// Whether the database is visible to this session (tenant filter +
    /// USAGE grant). `ACCOUNTADMIN` sees everything.
    #[must_use]
    pub fn database_visible(&self, database: &str) -> bool {
        let snapshot = self.snapshot.read().unwrap();
        if snapshot.admin {
            return true;
        }
        if snapshot
            .denied_databases
            .contains(&database.to_ascii_lowercase())
        {
            return false;
        }
        snapshot.allowed(
            Privilege::Usage,
            &AccountObject::Database(database.to_string()),
        ) || snapshot.allowed(
            Privilege::Ownership,
            &AccountObject::Database(database.to_string()),
        )
    }

    /// Whether the schema is visible (USAGE grant + record-level tenant
    /// scoping — design §23.7).
    #[must_use]
    pub fn schema_visible(&self, database: &str, schema: &str) -> bool {
        let snapshot = self.snapshot.read().unwrap();
        if snapshot.admin {
            return true;
        }
        if snapshot
            .denied_schemas
            .contains(&format!(
                "{}.{}",
                database.to_ascii_lowercase(),
                schema.to_ascii_lowercase()
            ))
        {
            return false;
        }
        snapshot.allowed(
            Privilege::Usage,
            &AccountObject::Schema(database.to_string(), schema.to_string()),
        ) || snapshot.allowed(
            Privilege::Ownership,
            &AccountObject::Schema(database.to_string(), schema.to_string()),
        ) || snapshot.allowed(
            Privilege::Ownership,
            &AccountObject::Database(database.to_string()),
        )
    }

    /// Whether the table is visible (SELECT grant, or OWNERSHIP on the
    /// table or one of its containers — the owner may query the object,
    /// design §23.7).
    #[must_use]
    pub fn table_visible(&self, database: &str, schema: &str, table: &str) -> bool {
        let obj = AccountObject::Table(
            database.to_string(),
            schema.to_string(),
            table.to_string(),
        );
        self.allowed_now(Privilege::Select, &obj)
            || obj
                .with_containers()
                .iter()
                .any(|o| self.allowed_now(Privilege::Ownership, o))
    }

    /// Whether the warehouse is usable (USAGE grant).
    #[must_use]
    pub fn warehouse_usable(&self, warehouse: &str) -> bool {
        self.allowed_now(
            Privilege::Usage,
            &AccountObject::Warehouse(warehouse.to_string()),
        )
    }

    // ------------------------------------------------------------------
    // Ownership-aware data-plane DDL (design §23.7, M17.4).
    // ------------------------------------------------------------------

    /// Whether the active role may `CREATE SCHEMA <db>.<s>`: USAGE + CREATE
    /// on the database, or OWNERSHIP on it (a container owner holds its
    /// child privileges implicitly — design §23.7). Admins bypass.
    #[must_use]
    pub fn can_create_schema(&self, database: &str) -> bool {
        let db = AccountObject::Database(database.to_string());
        let owns_db = self.allowed_now(Privilege::Ownership, &db);
        (self.allowed_now(Privilege::Usage, &db)
            && self.allowed_now(Privilege::Create, &db))
            || owns_db
    }

    /// Whether the active role may `CREATE TABLE <db>.<s>.<t>`: USAGE on
    /// the database plus USAGE + CREATE on the schema, or OWNERSHIP on the
    /// schema (a schema owner creates tables implicitly — design §23.7).
    /// Admins bypass.
    #[must_use]
    pub fn can_create_table(&self, database: &str, schema: &str) -> bool {
        let db_ok = self.allowed_now(
            Privilege::Usage,
            &AccountObject::Database(database.to_string()),
        );
        let schema_obj = AccountObject::Schema(database.to_string(), schema.to_string());
        let owns_schema = self.allowed_now(Privilege::Ownership, &schema_obj);
        db_ok && ((self.allowed_now(Privilege::Usage, &schema_obj)
            && self.allowed_now(Privilege::Create, &schema_obj))
            || owns_schema)
    }

    /// Whether the active role may drop/alter `object`: OWNERSHIP on the
    /// object or on any of its containers (schema covers its tables,
    /// database covers everything), admins bypass.
    #[must_use]
    pub fn can_drop_object(&self, object: &AccountObject) -> bool {
        if self.is_admin_now() {
            return true;
        }
        object
            .with_containers()
            .iter()
            .any(|o| self.allowed_now(Privilege::Ownership, o))
    }

    /// Record implicit OWNERSHIP for the active (non-admin) role after a
    /// successful `CREATE` of `object` — the creator becomes the owner
    /// (design §23.7). No-op for admins (they bypass every check) and for
    /// sessions without an active role.
    ///
    /// # Errors
    /// When the account store rejects the grant.
    pub async fn record_ownership(&self, object: &AccountObject) -> Result<()> {
        if self.is_admin_now() {
            return Ok(());
        }
        let role = self.active_role();
        if role.is_empty() {
            return Ok(());
        }
        self.store
            .grant(Grant {
                                account: self.account.clone(),
                role,
                privilege: Privilege::Ownership,
                object: object.clone(),
            })
            .await?;
        // Make the new ownership effective immediately in THIS session:
        // the snapshot was materialized at login/last activation, before
        // the CREATE — without this, the creator cannot resolve (let alone
        // drop) the object until re-activation. Store grants were already
        // epoch-bumped (result-cache invalidation); this only catches the
        // live snapshot up.
        let mut snapshot = self.snapshot.write().unwrap();
        snapshot
            .grants
            .insert((Privilege::Ownership, object.clone()));
        Ok(())
    }

    /// Remove every grant on a dropped object (Snowflake drops object
    /// grants together with the object, so a later re-creation with the
    /// same name cannot resurrect them).
    ///
    /// # Errors
    /// When the account store fails to list or revoke grants.
    pub async fn drop_object_grants(&self, object: &AccountObject) -> Result<()> {
        for g in self.store.object_grants(object).await? {
            self.store.revoke(&g).await?;
        }
        Ok(())
    }

    /// The login name this session is authorized as.
    #[must_use]
    pub fn login_name(&self) -> &str {
        &self.login_name
    }

    /// The account store backing this context.
    #[must_use]
    pub fn store(&self) -> Arc<dyn AccountStore> {
        Arc::clone(&self.store)
    }
}

/// Catalogs that bypass authorization entirely (internal machinery,
/// e.g. the time-travel temp catalog).
#[must_use]
pub fn is_internal_catalog(name: &str) -> bool {
    name.starts_with("__")
}

/// Wraps the session's [`AccountCatalogList`] (a pure delegation shell over
/// the shared list for the default account, an account-scoped list
/// otherwise) with per-session authorization.
#[derive(Debug)]
pub struct AuthzCatalogList {
    pub inner: Arc<AccountCatalogList>,
    pub authz: Arc<SessionAuthz>,
}

impl AuthzCatalogList {
    pub fn new(inner: impl IntoAccountCatalogList, authz: Arc<SessionAuthz>) -> Self {
        Self {
            inner: inner.into_account_catalog_list(),
            authz,
        }
    }

    /// Extract the underlying [`AccountCatalogList`] from either a bare or
    /// an authorized wrapper — used by hooks that must manipulate the real
    /// catalog list (time-travel temp catalog, CLONE registration, CREATE /
    /// DROP DATABASE).
    #[must_use]
    pub fn unwrap(
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> Option<Arc<AccountCatalogList>> {
        // Supertrait upcast to `Any + Send + Sync` first: `Arc::downcast`
        // only exists on that trait object.
        let cloned: Arc<dyn CatalogProviderList> = Arc::clone(catalog_list);
        let any: Arc<dyn std::any::Any + Send + Sync> = cloned;
        if let Ok(inner) = Arc::downcast::<AccountCatalogList>(any) {
            return Some(inner);
        }
        let cloned: Arc<dyn CatalogProviderList> = Arc::clone(catalog_list);
        let any: Arc<dyn std::any::Any + Send + Sync> = cloned;
        Arc::downcast::<AuthzCatalogList>(any)
            .ok()
            .map(|wrapper| Arc::clone(&wrapper.inner))
    }
}

impl CatalogProviderList for AuthzCatalogList {
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if is_internal_catalog(name) {
            return self.inner.catalog(name);
        }
        if !self.authz.database_visible(name) {
            // Deny = as-if-missing.
            return None;
        }
        self.inner
            .catalog(name)
            .map(|c| {
                Arc::new(AuthzCatalog {
                    name: name.to_string(),
                    inner: c,
                    authz: Arc::clone(&self.authz),
                }) as Arc<dyn CatalogProvider>
            })
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner
            .catalog_names()
            .into_iter()
            .filter(|name| is_internal_catalog(name) || self.authz.database_visible(name))
            .collect()
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        // Full trait qualification: an inherent async `register_catalog_name`
        // helper must not be confused with the trait method.
        <AccountCatalogList as CatalogProviderList>::register_catalog(
            &*self.inner,
            name,
            catalog,
        )
    }
}

/// Per-catalog authorization wrapper.
#[derive(Debug)]
pub struct AuthzCatalog {
    pub name: String,
    pub inner: Arc<dyn CatalogProvider>,
    pub authz: Arc<SessionAuthz>,
}

impl CatalogProvider for AuthzCatalog {
    fn schema_names(&self) -> Vec<String> {
        if self.authz.is_admin_now() {
            return self.inner.schema_names();
        }
        self.inner
            .schema_names()
            .into_iter()
            .filter(|schema| self.authz.schema_visible(&self.name, schema))
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if !self.authz.schema_visible(&self.name, name) {
            // Deny = as-if-missing.
            return None;
        }
        self.inner.schema(name).map(|sp| {
            Arc::new(AuthzSchema {
                database: self.name.clone(),
                schema: name.to_string(),
                inner: sp,
                authz: Arc::clone(&self.authz),
            }) as Arc<dyn SchemaProvider>
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> DfResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(&self, name: &str, cascade: bool) -> DfResult<Option<Arc<dyn SchemaProvider>>> {
        self.inner.deregister_schema(name, cascade)
    }
}

/// Per-schema authorization wrapper.
#[derive(Debug)]
pub struct AuthzSchema {
    pub database: String,
    pub schema: String,
    pub inner: Arc<dyn SchemaProvider>,
    pub authz: Arc<SessionAuthz>,
}

#[async_trait]
impl SchemaProvider for AuthzSchema {
    fn table_names(&self) -> Vec<String> {
        if self.authz.is_admin_now() {
            return self.inner.table_names();
        }
        self.inner
            .table_names()
            .into_iter()
            .filter(|table| {
                self.authz
                    .table_visible(&self.database, &self.schema, table)
            })
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if !self.authz.table_visible(&self.database, &self.schema, name) {
            // Deny = as-if-missing (Snowflake error shape via DF mapping).
            return Ok(None);
        }
        self.inner.table(name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.authz
            .table_visible(&self.database, &self.schema, name)
            && self.inner.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        // Data-plane DDL enforcement is v2 (design §17); delegate.
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Grant, User};
    use crate::store::InMemoryAccountStore;
    use catalog_metastore::models::{Database, Volume, VolumeType};
    use catalog_metastore::{InMemoryMetastore, Metastore};
    use std::collections::HashMap;

    async fn setup(tenant_db: Option<&str>) -> (Arc<dyn AccountStore>, Arc<dyn Metastore>) {
        let store: Arc<dyn AccountStore> =
            Arc::new(InMemoryAccountStore::new().with_admin("embucket", "embucket"));
        store.create_role("analyst").await.unwrap();
        store.create_user(User {
                        account: crate::models::DEFAULT_ACCOUNT.into(),
name: "u1".into(),
            login_name: "u1".into(),
            password: "pw".into(),
            default_role: Some("analyst".into()),
            tenant: Some("acme".into()),
            disabled: false,
        })
        .await
        .unwrap();
        store
            .grant_role_to_user("u1", "analyst")
            .await
            .unwrap();
        let mut properties = HashMap::new();
        if let Some(t) = tenant_db {
            properties.insert("tenant".to_string(), t.to_string());
        }
        let metastore = InMemoryMetastore::new();
        metastore
            .create_volume(
                &"vol".to_string(),
                Volume::new("vol".into(), VolumeType::Memory),
            )
            .await
            .unwrap();
        metastore
            .create_database(
                &"embucket".to_string(),
                Database {
                    ident: "embucket".into(),
                    properties: if properties.is_empty() { None } else { Some(properties) },
                    volume: "vol".into(),
                    should_refresh: false,
                    account: "embucket".to_string(),
                },
            )
            .await
            .unwrap();
        (store, Arc::new(metastore))
    }

    #[tokio::test]
    async fn activate_and_check() {
        let (store, metastore) = setup(None).await;
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authz.active_role(), "analyst");
        assert!(!authz.is_admin_now());
        // No grants yet: nothing visible.
        assert!(!authz.database_visible("embucket"));
        assert!(!authz.warehouse_usable("wh"));
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
            })
            .await
            .unwrap();
        // Snapshot is stale until re-activation (grant semantics).
        assert!(!authz.database_visible("embucket"));
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.database_visible("embucket"));
        // Schema USAGE is a separate grant.
        assert!(!authz.schema_visible("embucket", "public"));
        assert!(!authz.table_visible("embucket", "public", "t"));
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Schema("embucket".into(), "public".into()),
            })
            .await
            .unwrap();
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.schema_visible("embucket", "public"));
        assert!(!authz.table_visible("embucket", "public", "t"));
    }

    #[tokio::test]
    async fn role_activation_requires_membership() {
        let (store, metastore) = setup(None).await;
        store.create_role("other").await.unwrap();
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        assert!(authz.activate_role("other").await.is_err());
        // Requested role not held → falls back to default.
        let authz2 = SessionAuthz::new(store, metastore, "u1", Some("other"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authz2.active_role(), "analyst");
    }

    #[tokio::test]
    async fn role_hierarchy_inherits_privileges_and_activation() {
        let (store, metastore) = setup(None).await;
        // senior (u2's role) inherits analyst's grants via the hierarchy.
        store.create_role("senior").await.unwrap();
        store.create_user(User {
                        account: crate::models::DEFAULT_ACCOUNT.into(),
name: "u2".into(),
            login_name: "u2".into(),
            password: "pw".into(),
            default_role: Some("senior".into()),
            tenant: None,
            disabled: false,
        })
        .await
        .unwrap();
        store.grant_role_to_user("u2", "senior").await.unwrap();
        store.grant_role_to_role("analyst", "senior").await.unwrap();
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
            })
            .await
            .unwrap();

        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u2", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authz.active_role(), "senior");
        // Inherited: senior sees analyst's database USAGE.
        assert!(authz.database_visible("embucket"));
        // Indirect activation: analyst lies beneath senior's hierarchy.
        authz.activate_role("analyst").await.unwrap();
        assert_eq!(authz.active_role(), "analyst");

        // Upward is NOT inherited: u1 (analyst) cannot assume senior.
        let authz1 = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        assert!(authz1.activate_role("senior").await.is_err());

        // Revoking the hierarchy link removes the inherited privilege.
        store.revoke_role_from_role("analyst", "senior").await.unwrap();
        authz.activate_role("senior").await.unwrap();
        assert!(!authz.database_visible("embucket"));
        // ...and u2 can no longer activate analyst (no longer beneath senior).
        assert!(authz.activate_role("analyst").await.is_err());
    }

    #[tokio::test]
    async fn ownership_aware_ddl_checks() {
        let (store, metastore) = setup(None).await;
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        // No privileges yet: nothing creatable or droppable.
        assert!(!authz.can_create_table("embucket", "public"));
        assert!(!authz.can_create_schema("embucket"));
        let t = AccountObject::Table("embucket".into(), "public".into(), "t".into());
        assert!(!authz.can_drop_object(&t));
        // USAGE on the database enables nothing by itself.
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
            })
            .await
            .unwrap();
        assert!(!authz.can_create_table("embucket", "public"), "needs re-activate");
        authz.activate_role("analyst").await.unwrap();
        assert!(!authz.can_create_table("embucket", "public"), "missing CREATE");
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Schema("embucket".into(), "public".into()),
            })
            .await
            .unwrap();
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Create,
                object: AccountObject::Schema("embucket".into(), "public".into()),
            })
            .await
            .unwrap();
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.can_create_table("embucket", "public"));
        // Container ownership: CREATE on a database enables schema creation.
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Create,
                object: AccountObject::Database("embucket".into()),
            })
            .await
            .unwrap();
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.can_create_schema("embucket"));
        // OWNERSHIP on the schema covers dropping its tables.
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Ownership,
                object: AccountObject::Schema("embucket".into(), "public".into()),
            })
            .await
            .unwrap();
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.can_drop_object(&t));
    }

    #[tokio::test]
    async fn record_and_drop_object_ownership() {
        let (store, metastore) = setup(None).await;
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        let t = AccountObject::Table("embucket".into(), "public".into(), "t".into());
        authz.record_ownership(&t).await.unwrap();
        let grants = store.object_grants(&t).await.unwrap();
        assert_eq!(grants.len(), 1, "creator role owns the created table");
        assert_eq!(grants[0].privilege, Privilege::Ownership);
        assert_eq!(grants[0].role, "analyst");
        authz.drop_object_grants(&t).await.unwrap();
        assert!(store.object_grants(&t).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn record_level_tenant_scoping() {
        let (store, metastore) = setup(None).await;
        // Schema record stamped with a tenant label inside the shared db.
        let ident = catalog_metastore::models::SchemaIdent {
            database: "embucket".into(),
            schema: "tenantx".into(),
        };
        metastore
            .create_schema(
                &ident,
                catalog_metastore::models::Schema {
                    ident: ident.clone(),
                    properties: Some(HashMap::from([("tenant".to_string(), "acme".to_string())])),
                },
            )
            .await
            .unwrap();
        // Baseline grants so the un-stamped schema is visible via USAGE.
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Schema("embucket".into(), "public".into()),
            })
            .await
            .unwrap();
        // u1 (analyst) has no tenant label → mismatching record denied.
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        assert!(authz.schema_visible("embucket", "public"));
        assert!(!authz.schema_visible("embucket", "tenantx"));
        // A same-tenant user sees the record (admin is bypassed entirely).
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Schema("embucket".into(), "tenantx".into()),
            })
            .await
            .unwrap();
        store
            .create_user(User {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
name: "uacme".into(),
                login_name: "uacme".into(),
                password: "pw".into(),
                default_role: Some("analyst".into()),
                tenant: Some("acme".into()),
                disabled: false,
            })
            .await
            .unwrap();
        store.grant_role_to_user("uacme", "analyst").await.unwrap();
        let authz = SessionAuthz::new(store, metastore, "uacme", None)
            .await
            .unwrap()
            .unwrap();
        assert!(authz.schema_visible("embucket", "tenantx"));
    }

    #[tokio::test]
    async fn admin_bypasses_everything() {
        let (store, metastore) = setup(Some("acme")).await;
        let authz = SessionAuthz::new(store, metastore, "embucket", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(authz.active_role(), "ACCOUNTADMIN");
        assert!(authz.is_admin_now());
        assert!(authz.database_visible("embucket"));
        assert!(authz.table_visible("any", "s", "t"));
        assert!(authz.warehouse_usable("wh"));
    }

    #[tokio::test]
    async fn tenant_filter_denies_foreign_databases() {
        // Database carries tenant "acme", user u1 is tenant "acme" → OK.
        let (store, metastore) = setup(Some("acme")).await;
        store
            .grant(Grant {
                                account: crate::models::DEFAULT_ACCOUNT.into(),
role: "analyst".into(),
                privilege: Privilege::Usage,
                object: AccountObject::Database("embucket".into()),
            })
            .await
            .unwrap();
        let authz = SessionAuthz::new(Arc::clone(&store), Arc::clone(&metastore), "u1", None)
            .await
            .unwrap()
            .unwrap();
        authz.activate_role("analyst").await.unwrap();
        assert!(authz.database_visible("embucket"));

        // Same grant, but database now labeled for another tenant.
        let metastore2 = InMemoryMetastore::new();
        let mut properties = HashMap::new();
        properties.insert("tenant".to_string(), "other".to_string());
        metastore2
            .create_volume(
                &"vol".to_string(),
                Volume::new("vol".into(), VolumeType::Memory),
            )
            .await
            .unwrap();
        metastore2
            .create_database(
                &"embucket".to_string(),
                Database {
                    ident: "embucket".into(),
                    properties: Some(properties),
                    volume: "vol".into(),
                    should_refresh: false,
                    account: "embucket".to_string(),
                },
            )
            .await
            .unwrap();
        let authz2 = SessionAuthz::new(store, Arc::new(metastore2), "u1", None)
            .await
            .unwrap()
            .unwrap();
        authz2.activate_role("analyst").await.unwrap();
        assert!(!authz2.database_visible("embucket"));
    }

    #[tokio::test]
    async fn unknown_user_returns_none() {
        let (store, metastore) = setup(None).await;
        let authz = SessionAuthz::new(store, metastore, "ghost", None).await.unwrap();
        assert!(authz.is_none());
    }
}