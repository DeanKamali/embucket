use crate::server::error::CreateExecutorSnafu;
use crate::server::error::MetastoreConfigSnafu;
use crate::server::error::MetastoreStateSnafu;
use crate::server::error::Result;
use crate::server::server_models::RestApiConfig;
use api_snowflake_rest_sessions::session::SessionStore;
use catalog_metastore::InMemoryMetastore;
use catalog_metastore::Metastore;
use catalog_metastore::metastore_bootstrap_config::MetastoreBootstrapConfig;
use catalog_metastore::metastore_settings_config::MetastoreSettingsConfig;
use executor::service::CoreExecutionService;
use executor::utils::Config as ExecutionConfig;
use avalanche_account::store::AccountStore;
use snafu::ResultExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Duration;

pub struct CoreState {
    pub executor: Arc<CoreExecutionService>,
    pub metastore: Arc<InMemoryMetastore>,
    pub rest_api_config: RestApiConfig,
}

#[derive(Clone)]
pub enum MetastoreConfig {
    ConfigPath(PathBuf),
    ConfigJson(String),
    Env,
    DefaultConfig,
    None,
}

impl CoreState {
    pub async fn new(
        execution_cfg: ExecutionConfig,
        rest_api_config: RestApiConfig,
        metastore_settings_config: MetastoreSettingsConfig,
        metastore_bootstrap_config: MetastoreConfig,
    ) -> Result<Self> {
        let metastore = create_metastore(metastore_settings_config);
        Self::with_metastore(metastore, execution_cfg, rest_api_config, metastore_bootstrap_config)
            .await
    }

    /// Build `CoreState` around a pre-built metastore (design §23.0, M17.0):
    /// embucketd constructs the durable (journalled) metastore itself, since
    /// it owns the state-store backend.
    pub async fn with_metastore(
        metastore: Arc<InMemoryMetastore>,
        execution_cfg: ExecutionConfig,
        rest_api_config: RestApiConfig,
        metastore_bootstrap_config: MetastoreConfig,
    ) -> Result<Self> {
        apply_metastore_bootstrap_config(metastore.clone(), metastore_bootstrap_config).await?;
        let executor = create_executor(
            metastore.clone(),
            execution_cfg,
            rest_api_config.account.clone(),
        )
        .await?;
        Ok(Self {
            executor,
            metastore,
            rest_api_config,
        })
    }

    pub fn with_session_timeout(&self, session_timeout: Duration) -> Result<()> {
        tracing::info!(
            "With session timeout, by {} seconds",
            session_timeout.as_secs()
        );
        let session_store = SessionStore::new(self.executor.clone());
        tokio::spawn(async move {
            session_store
                .continuously_delete_expired(session_timeout)
                .await;
        });
        Ok(())
    }
}

#[must_use]
fn create_metastore(metastore_settings_config: MetastoreSettingsConfig) -> Arc<InMemoryMetastore> {
    Arc::new(InMemoryMetastore::new().with_settings_config(metastore_settings_config))
}

async fn apply_metastore_bootstrap_config(
    metastore: Arc<InMemoryMetastore>,
    metastore_bootstrap_config: MetastoreConfig,
) -> Result<()> {
    // Durability (design §23.0, M17.0): if the journal replay already
    // restored catalog objects, skip env/config seeding so a restart is a
    // true no-op instead of re-creating (and conflicting with) persisted
    // records.
    if !metastore.is_empty().await.context(MetastoreStateSnafu)? {
        tracing::info!("Metastore restored from journal; skipping bootstrap config");
        return Ok(());
    }
    match metastore_bootstrap_config {
        MetastoreConfig::Env => {
            tracing::info!("Bootstrapping metastore from environment");
            let config = MetastoreBootstrapConfig::load_from_env()
                .await
                .context(MetastoreConfigSnafu)?;
            config
                .apply(metastore.clone())
                .await
                .context(MetastoreConfigSnafu)?;
        }
        MetastoreConfig::ConfigPath(path) => {
            tracing::info!(
                path = %path.display(),
                "Bootstrapping metastore from config"
            );
            let config = MetastoreBootstrapConfig::load(&path)
                .await
                .context(MetastoreConfigSnafu)?;
            config
                .apply(metastore.clone())
                .await
                .context(MetastoreConfigSnafu)?;
        }
        MetastoreConfig::ConfigJson(data) => {
            tracing::info!("Bootstrapping metastore from config json");
            let config = MetastoreBootstrapConfig::load_from_json_data(&data)
                .await
                .context(MetastoreConfigSnafu)?;
            config
                .apply(metastore.clone())
                .await
                .context(MetastoreConfigSnafu)?;
        }
        MetastoreConfig::DefaultConfig => {
            tracing::info!("Bootstrapping metastore from default config");
            MetastoreBootstrapConfig::bootstrap()
                .apply(metastore.clone())
                .await
                .context(MetastoreConfigSnafu)?;
        }
        MetastoreConfig::None => {}
    }
    Ok(())
}

async fn create_executor(
    metastore: Arc<dyn Metastore>,
    execution_cfg: ExecutionConfig,
    account: Option<Arc<dyn AccountStore>>,
) -> Result<Arc<CoreExecutionService>> {
    tracing::info!("Creating execution service");
    let executor = CoreExecutionService::new(metastore, Arc::new(execution_cfg))
        .await
        .context(CreateExecutorSnafu)?;
    let executor = match account {
        Some(account) => executor.with_account(account),
        None => executor,
    };
    let executor = Arc::new(executor);
    // Automation scheduler (design §18.1-4, M11): in-process tokio loop for
    // due tasks and dynamic-table lag refreshes; exits with the service.
    executor::scheduler::spawn(&executor);
    tracing::info!("Execution service created");
    Ok(executor)
}
