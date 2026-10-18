//! Account-scoped catalog list (M15.4, multi-account tenancy).
//!
//! [`AccountCatalogList`] is the per-session view over the shared
//! [`EmbucketCatalogList`]:
//!
//! * **Default account** (`embucket`): a thin delegating shell — every call
//!   goes to the shared list, so behavior is identical to a bare
//!   `EmbucketCatalogList`.
//! * **Other accounts**: the shared list is never consulted for catalog
//!   resolution. Providers for the account's databases are materialized when
//!   the view is created ([`AccountCatalogList::for_account`]) and kept in a
//!   per-view `local` map keyed by display name; they are built over an
//!   [`AccountedMetastore`] so state lookups hit `{account}/{db}` keys.
//!   Runtime registrations (`tmp_db`, `__tt`, ...) stay in `local` too and
//!   never leak into the shared list or other accounts.

use crate::catalog::{CachingCatalog, CatalogType};
use crate::catalog_list::EmbucketCatalogList;
use crate::error::{
    InvalidCacheSnafu, MetastoreSnafu, MissingVolumeSnafu, NotImplementedSnafu, Result,
    UnsupportedFeature,
};
use catalog_metastore::{
    AccountedMetastore, DEFAULT_DATABASE_ACCOUNT, Database, Metastore, RwObject,
};
use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use snafu::{OptionExt, ResultExt};
use std::sync::Arc;

/// Per-account catalog list. See the module docs.
pub struct AccountCatalogList {
    account: String,
    inner: Arc<EmbucketCatalogList>,
    scoped_metastore: Arc<dyn Metastore>,
    local: DashMap<String, Arc<CachingCatalog>>,
}

impl std::fmt::Debug for AccountCatalogList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountCatalogList")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

/// Conversion into the session's catalog-list type: a bare shared list
/// becomes the default-account shell; an already scoped list is passed
/// through.
pub trait IntoAccountCatalogList {
    fn into_account_catalog_list(self) -> Arc<AccountCatalogList>;
}

impl IntoAccountCatalogList for Arc<AccountCatalogList> {
    fn into_account_catalog_list(self) -> Arc<AccountCatalogList> {
        self
    }
}

impl IntoAccountCatalogList for Arc<EmbucketCatalogList> {
    fn into_account_catalog_list(self) -> Arc<AccountCatalogList> {
        AccountCatalogList::shared(self)
    }
}

impl AccountCatalogList {
    /// Raw constructor. Prefer [`Self::shared`] / [`Self::for_account`].
    #[must_use]
    pub fn new(
        account: &str,
        inner: Arc<EmbucketCatalogList>,
        scoped_metastore: Arc<dyn Metastore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            account: account.to_string(),
            inner,
            scoped_metastore,
            local: DashMap::new(),
        })
    }

    /// Default-account shell over the shared list (pure delegation).
    #[must_use]
    pub fn shared(inner: Arc<EmbucketCatalogList>) -> Arc<Self> {
        let metastore = inner.metastore.clone();
        Self::new(DEFAULT_DATABASE_ACCOUNT, inner, metastore)
    }

    /// Build the catalog list for `account`. The default account yields the
    /// delegating shell; any other account gets an [`AccountedMetastore`]
    /// view and eagerly materialized providers for each of its databases
    /// (a `list_databases` plus N provider builds — metadata only).
    ///
    /// # Errors
    /// When the metastore listing or a provider build fails.
    pub async fn for_account(
        account: &str,
        inner: Arc<EmbucketCatalogList>,
        raw_metastore: Arc<dyn Metastore>,
    ) -> Result<Arc<Self>> {
        if account == DEFAULT_DATABASE_ACCOUNT {
            return Ok(Self::new(account, inner, raw_metastore));
        }
        let scoped: Arc<dyn Metastore> = Arc::new(AccountedMetastore::new(raw_metastore, account));
        let list = Self::new(account, inner, scoped);
        let databases = list
            .scoped_metastore
            .list_databases()
            .await
            .context(MetastoreSnafu)?;
        for db in databases {
            let display = db.data.ident.clone();
            list.build_local(&display, &db).await?;
        }
        Ok(list)
    }

    #[must_use]
    pub fn account(&self) -> &str {
        &self.account
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        self.account == DEFAULT_DATABASE_ACCOUNT
    }

    /// The shared list underneath (object-store registry, config).
    #[must_use]
    pub const fn shared_list(&self) -> &Arc<EmbucketCatalogList> {
        &self.inner
    }

    /// The metastore catalogs of this view must use (account-scoped for
    /// non-default accounts).
    #[must_use]
    pub fn metastore(&self) -> Arc<dyn Metastore> {
        self.scoped_metastore.clone()
    }

    fn qualified(&self, display: &str) -> String {
        if self.is_default() {
            display.to_string()
        } else {
            format!("{}/{display}", self.account)
        }
    }

    async fn build_local(
        &self,
        display: &str,
        db: &RwObject<Database>,
    ) -> Result<Arc<CachingCatalog>> {
        let catalog = self
            .inner
            .build_catalog(
                display,
                &self.qualified(display),
                db,
                self.scoped_metastore.clone(),
            )
            .await?;
        let catalog = Arc::new(catalog);
        self.local.insert(display.to_string(), catalog.clone());
        Ok(catalog)
    }

    /// Resolve (building and caching if needed) the catalog `display`.
    /// Default account: pure delegation to the shared list. Other accounts:
    /// the per-view `local` map, falling back to the account's metastore
    /// database record.
    ///
    /// # Errors
    /// When the metastore lookup or provider build fails.
    pub async fn ensure_catalog(&self, display: &str) -> Result<Option<Arc<CachingCatalog>>> {
        if self.is_default() {
            return Ok(self
                .inner
                .catalogs
                .get(display)
                .map(|c| Arc::clone(c.value())));
        }
        if let Some(c) = self.local.get(display) {
            return Ok(Some(Arc::clone(c.value())));
        }
        let Some(db) = self
            .scoped_metastore
            .get_database(&display.to_string())
            .await
            .context(MetastoreSnafu)?
        else {
            return Ok(None);
        };
        Ok(Some(self.build_local(display, &db).await?))
    }

    /// Catalog names visible to this account (display names).
    pub async fn catalog_names_scoped(&self) -> Vec<String> {
        self.scoped_names()
    }

    fn scoped_names(&self) -> Vec<String> {
        if self.is_default() {
            return self.inner.catalog_names();
        }
        let prefix = format!("{}/", self.account);
        let mut names: Vec<String> = self
            .inner
            .catalog_keys()
            .into_iter()
            .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
            .collect();
        for entry in &self.local {
            if !names.contains(entry.key()) {
                names.push(entry.key().clone());
            }
        }
        names
    }

    /// `CREATE DATABASE` for this account.
    ///
    /// # Errors
    /// When the volume is missing, the database exists, or the build fails.
    pub async fn create_catalog(&self, name: &str, volume: &str) -> Result<()> {
        if self.is_default() {
            return self.inner.create_catalog(name, volume).await;
        }
        self.scoped_metastore
            .get_volume(&volume.to_string())
            .await
            .context(MetastoreSnafu)?
            .context(MissingVolumeSnafu {
                name: volume.to_string(),
            })?;
        // The scoped metastore stamps the account and qualifies the key.
        let database = Database {
            ident: name.to_string(),
            properties: None,
            volume: volume.to_string(),
            should_refresh: false,
            account: self.account.clone(),
        };
        let record = self
            .scoped_metastore
            .create_database(&name.to_string(), database)
            .await
            .context(MetastoreSnafu)?;
        self.build_local(name, &record).await?;
        Ok(())
    }

    /// `DROP DATABASE` for this account.
    ///
    /// # Errors
    /// When the catalog is unknown, is an S3 Tables catalog, or the metastore
    /// delete fails (the catalog then stays registered).
    pub async fn drop_catalog(&self, name: &str, cascade: bool) -> Result<()> {
        if self.is_default() {
            return self.inner.drop_catalog(name, cascade).await;
        }
        let Some(catalog) = self.ensure_catalog(name).await? else {
            return InvalidCacheSnafu {
                entity: "catalog",
                name,
            }
            .fail();
        };
        let result = match catalog.catalog_type {
            CatalogType::Embucket | CatalogType::Memory => self
                .scoped_metastore
                .delete_database(&name.to_string(), cascade)
                .await
                .context(MetastoreSnafu),
            CatalogType::S3tables => NotImplementedSnafu {
                feature: UnsupportedFeature::DropS3TablesDatabase,
                details: "Dropping S3 tables catalogs is not supported",
            }
            .fail(),
        };
        if result.is_ok() {
            self.local.remove(name);
        }
        result
    }

    /// (Re)register a catalog for an existing metastore database after
    /// `CLONE` / `UNDROP DATABASE`.
    ///
    /// # Errors
    /// When the database does not exist in the metastore or the build fails.
    pub async fn register_catalog_name(&self, name: &str) -> Result<()> {
        if self.is_default() {
            return self.inner.register_catalog(name).await;
        }
        if self.local.contains_key(name) {
            return Ok(());
        }
        match self.ensure_catalog(name).await? {
            Some(_) => Ok(()),
            None => InvalidCacheSnafu {
                entity: "database",
                name,
            }
            .fail(),
        }
    }
}

impl CatalogProviderList for AccountCatalogList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        if self.is_default() {
            // Back-compat: tmp_db / __tt registrations target the shared
            // list. Full trait qualification: an inherent async
            // `register_catalog` shadows the trait method.
            return <EmbucketCatalogList as CatalogProviderList>::register_catalog(
                &self.inner,
                name,
                catalog,
            );
        }
        let wrapped = CachingCatalog::new(catalog, name, None, self.inner.config.clone().into());
        self.local
            .insert(wrapped.name.clone(), Arc::new(wrapped))
            .map(|old| old as Arc<dyn CatalogProvider>)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.scoped_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if let Some(c) = self.local.get(name) {
            return Some(Arc::clone(c.value()) as Arc<dyn CatalogProvider>);
        }
        if self.is_default() {
            return <EmbucketCatalogList as CatalogProviderList>::catalog(&self.inner, name);
        }
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::catalog_list::CatalogListConfig;
    use catalog_metastore::{InMemoryMetastore, Volume, VolumeType};

    async fn setup() -> (Arc<dyn Metastore>, Arc<EmbucketCatalogList>) {
        let ms: Arc<dyn Metastore> = Arc::new(InMemoryMetastore::new());
        ms.create_volume(
            &"vol".to_string(),
            Volume::new("vol".into(), VolumeType::Memory),
        )
        .await
        .unwrap();
        let shared = Arc::new(EmbucketCatalogList::new(
            ms.clone(),
            CatalogListConfig::default(),
        ));
        (ms, shared)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn accounts_get_isolated_same_named_databases() {
        let (ms, shared) = setup().await;
        let default =
            AccountCatalogList::for_account(DEFAULT_DATABASE_ACCOUNT, shared.clone(), ms.clone())
                .await
                .unwrap();
        let acme = AccountCatalogList::for_account("acme", shared.clone(), ms.clone())
            .await
            .unwrap();
        let globex = AccountCatalogList::for_account("globex", shared.clone(), ms.clone())
            .await
            .unwrap();

        default.create_catalog("sales", "vol").await.unwrap();
        acme.create_catalog("sales", "vol").await.unwrap();
        globex.create_catalog("sales", "vol").await.unwrap();
        acme.create_catalog("only_acme", "vol").await.unwrap();
        // duplicate within an account still errors
        assert!(acme.create_catalog("sales", "vol").await.is_err());

        // The shared list only holds the default account's bare key.
        assert_eq!(shared.catalog_keys(), vec!["sales".to_string()]);

        let mut names = acme.catalog_names();
        names.sort();
        assert_eq!(names, vec!["only_acme".to_string(), "sales".to_string()]);
        assert_eq!(globex.catalog_names(), vec!["sales".to_string()]);
        assert_eq!(default.catalog_names(), vec!["sales".to_string()]);
        assert!(globex.catalog("only_acme").is_none());
        assert!(default.catalog("only_acme").is_none());
        assert!(acme.catalog("only_acme").is_some());

        // Each account's metastore view holds exactly its own record.
        assert_eq!(ms.list_databases().await.unwrap().len(), 4);
        assert!(
            ms.get_database(&"acme/sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ms.get_database(&"globex/sales".to_string())
                .await
                .unwrap()
                .is_some()
        );

        // A fresh session for the same account rehydrates from the metastore.
        let acme2 = AccountCatalogList::for_account("acme", shared.clone(), ms.clone())
            .await
            .unwrap();
        let mut names = acme2.catalog_names();
        names.sort();
        assert_eq!(names, vec!["only_acme".to_string(), "sales".to_string()]);
        assert!(acme2.catalog("sales").is_some());
        // ...and a never-created account sees nothing.
        let none = AccountCatalogList::for_account("nobody", shared.clone(), ms.clone())
            .await
            .unwrap();
        assert!(none.catalog_names().is_empty());

        // Dropping in one account leaves the others intact.
        acme.drop_catalog("sales", true).await.unwrap();
        assert!(acme.catalog("sales").is_none());
        assert!(globex.catalog("sales").is_some());
        assert!(default.catalog("sales").is_some());
        assert!(
            ms.get_database(&"globex/sales".to_string())
                .await
                .unwrap()
                .is_some()
        );
        // undrop path: re-register after the metastore op
        ms.delete_database(&"globex/sales".to_string(), true)
            .await
            .unwrap();
        ms.undrop_database(&"globex/sales".to_string())
            .await
            .unwrap();
        globex.local.remove("sales");
        globex.register_catalog_name("sales").await.unwrap();
        assert!(globex.catalog("sales").is_some());
        assert!(globex.register_catalog_name("missing").await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_registrations_stay_local_for_scoped_accounts() {
        use datafusion::catalog::MemoryCatalogProvider;
        let (ms, shared) = setup().await;
        let acme = AccountCatalogList::for_account("acme", shared.clone(), ms.clone())
            .await
            .unwrap();
        let default = AccountCatalogList::shared(shared.clone());
        acme.register_catalog("__tt".to_string(), Arc::new(MemoryCatalogProvider::new()));
        assert!(acme.catalog("__tt").is_some());
        assert!(shared.catalog("__tt").is_none());
        assert!(default.catalog("__tt").is_none());
        // default account registrations hit the shared list (back-compat)
        default.register_catalog("tmp_db".to_string(), Arc::new(MemoryCatalogProvider::new()));
        assert!(shared.catalog("tmp_db").is_some());
        assert!(acme.catalog("tmp_db").is_none());
    }
}
