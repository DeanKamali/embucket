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
use crate::models::{AccountObject, Privilege, ACCOUNTADMIN};
use crate::store::AccountStore;
use async_trait::async_trait;
use catalog::catalog_list::EmbucketCatalogList;
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
    role: RwLock<String>,
    snapshot: RwLock<RoleSnapshot>,
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
            role: RwLock::new(String::new()),
            snapshot: RwLock::new(RoleSnapshot::default()),
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

    /// Activate `role` for this session's user: the user must hold the role,
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
        if !holds_admin && !held.iter().any(|h| h.eq_ignore_ascii_case(&r.name)) {
            return account_error::NotAllowedSnafu {
                message: format!("role '{}' is not granted to user '{}'", r.name, self.user),
            }
            .fail();
        }
        let admin = r.name.eq_ignore_ascii_case(ACCOUNTADMIN);
        self.materialize(&r.name, admin).await?;
        *self.role.write().unwrap() = r.name;
        Ok(())
    }

    async fn materialize(&self, role: &str, admin: bool) -> Result<()> {
        let mut snapshot = RoleSnapshot {
            admin,
            ..RoleSnapshot::default()
        };
        if !admin {
            // Tenant hard filter: deny databases whose tenant label does
            // not match this user's label.
            let user = self.store.get_user(&self.user).await?;
            snapshot.denied_databases = self
                .metastore
                .list_databases()
                .await
                .map_err(|e| {
                    account_error::InvalidInputSnafu {
                        message: e.to_string(),
                    }
                    .build()
                })?
                .into_iter()
                .filter(|db| {
                    let db_tenant = db
                        .data
                        .properties
                        .as_ref()
                        .and_then(|p| p.get("tenant"))
                        .map(|t| t.to_ascii_lowercase());
                    let user_tenant = user
                        .as_ref()
                        .and_then(|u| u.tenant.as_ref())
                        .map(|t| t.to_ascii_lowercase());
                    match (db_tenant, user_tenant) {
                        (Some(db_t), Some(u_t)) => db_t != u_t,
                        (Some(_), None) => true,
                        _ => false,
                    }
                })
                .map(|db| db.ident.to_ascii_lowercase())
                .collect();
            snapshot.grants = self
                .store
                .role_grants(role)
                .await?
                .into_iter()
                .map(|g| (g.privilege, g.object))
                .collect();
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
        )
    }

    /// Whether the schema is visible (USAGE grant).
    #[must_use]
    pub fn schema_visible(&self, database: &str, schema: &str) -> bool {
        self.allowed_now(
            Privilege::Usage,
            &AccountObject::Schema(database.to_string(), schema.to_string()),
        )
    }

    /// Whether the table is visible (SELECT grant).
    #[must_use]
    pub fn table_visible(&self, database: &str, schema: &str, table: &str) -> bool {
        self.allowed_now(
            Privilege::Select,
            &AccountObject::Table(
                database.to_string(),
                schema.to_string(),
                table.to_string(),
            ),
        )
    }

    /// Whether the warehouse is usable (USAGE grant).
    #[must_use]
    pub fn warehouse_usable(&self, warehouse: &str) -> bool {
        self.allowed_now(
            Privilege::Usage,
            &AccountObject::Warehouse(warehouse.to_string()),
        )
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

/// Wraps [`EmbucketCatalogList`] with per-session authorization.
#[derive(Debug)]
pub struct AuthzCatalogList {
    pub inner: Arc<EmbucketCatalogList>,
    pub authz: Arc<SessionAuthz>,
}

impl AuthzCatalogList {
    pub fn new(inner: Arc<EmbucketCatalogList>, authz: Arc<SessionAuthz>) -> Self {
        Self { inner, authz }
    }

    /// Extract the underlying [`EmbucketCatalogList`] from either a bare or
    /// an authorized wrapper — used by hooks that must manipulate the real
    /// catalog list (time-travel temp catalog, CLONE registration).
    #[must_use]
    pub fn unwrap(
        catalog_list: &Arc<dyn CatalogProviderList>,
    ) -> Option<Arc<EmbucketCatalogList>> {
        // Supertrait upcast to `Any + Send + Sync` first: `Arc::downcast`
        // only exists on that trait object.
        let cloned: Arc<dyn CatalogProviderList> = Arc::clone(catalog_list);
        let any: Arc<dyn std::any::Any + Send + Sync> = cloned;
        if let Ok(inner) = Arc::downcast::<EmbucketCatalogList>(any) {
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
        // Full trait qualification: an inherent async `register_catalog`
        // on `EmbucketCatalogList` shadows the trait method.
        <EmbucketCatalogList as CatalogProviderList>::register_catalog(
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