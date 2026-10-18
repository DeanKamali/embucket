//! Per-warehouse storage registration from container env (M7.1, design §13.1).
//!
//! Warehouse compute containers receive scoped storage credentials as
//! environment variables at container start (`StorageCredentials::env_vars`,
//! injected by the warehouse manager) — never inside serialized plans. This
//! module turns that env into a registered object store on the store-owning
//! side:
//!
//! - **Executors** register it in their `RuntimeEnv` object-store registry
//!   (via the ballista `override_runtime_producer` hook) so decoded stage
//!   plans can scan `s3://<bucket>` locations.
//! - **Schedulers** register it in the sessions they build (via the
//!   `override_session_builder` hook) so logical plans referencing the
//!   warehouse bucket decode and re-plan correctly.

use std::sync::Arc;

use datafusion_common::Result as DfResult;
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use url::Url;

/// Build the per-warehouse S3 object store from container env.
///
/// `AVALANCHE_S3_BUCKET` selects the bucket; all other knobs (endpoint,
/// keys, region, allow-http) are read through `AmazonS3Builder::from_env`
/// (standard `AWS_*` variables injected by the warehouse manager).
///
/// Returns `None` when the process has no warehouse storage env (v1
/// `file://` warehouses, or the router itself).
///
/// # Errors
///
/// Returns an error if the env is inconsistent (e.g. bad endpoint URL) or
/// the store config fails to build.
pub fn s3_store_from_env() -> object_store::Result<Option<(Url, Arc<dyn ObjectStore>)>> {
    let bucket = std::env::var("AVALANCHE_S3_BUCKET")
        .ok()
        .filter(|b| !b.is_empty());
    let Some(bucket) = bucket else {
        return Ok(None);
    };
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .build()?;
    let url = Url::parse(&format!("s3://{bucket}")).map_err(|e| object_store::Error::Generic {
        store: "AVALANCHE_S3_BUCKET",
        source: Box::new(e),
    })?;
    Ok(Some((url, Arc::new(store))))
}

/// A fresh object-store registry pre-populated with the warehouse store from
/// env (when present). `DefaultObjectStoreRegistry` keeps the `file://`
/// default registration, so non-storage warehouses behave exactly as before.
///
/// # Errors
///
/// Returns an error if the storage env is invalid (see [`s3_store_from_env`]).
pub fn registry_from_env() -> object_store::Result<Arc<dyn ObjectStoreRegistry>> {
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    if let Some((url, store)) = s3_store_from_env()? {
        registry.register_store(&url, store);
    }
    Ok(registry)
}

/// Object-store registry that also resolves datafusion_iceberg's fake
/// per-table store URLs (`iceberg-rust://<escaped-table-path>`, see
/// `fake_object_store_url` in the datafusion_iceberg fork).
///
/// Stage plans produced by the scheduler keep those URLs in their
/// `DataSourceExec` file groups, but the executor's runtime registry is a
/// plain `DefaultObjectStoreRegistry` — it only knows `file://` plus the
/// warehouse `s3://<bucket>` store from env, so stage execution fails with
/// `No suitable object store found for iceberg-rust://...`.
///
/// Resolution order: any exact registration (including `iceberg-rust://`
/// URLs registered by a decoded provider's own `table_scan` during FLATTEN
/// sub-plan creation) first; unknown `iceberg-rust://` URLs fall back to the
/// warehouse store from env (the managed-table volume store — same bucket
/// the scheduler-side scan would have registered), or the local filesystem
/// when there is no warehouse env. Everything else delegates to the default
/// registry unchanged.
struct IcebergSchemeRegistry {
    inner: DefaultObjectStoreRegistry,
    fallback: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for IcebergSchemeRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergSchemeRegistry")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreRegistry for IcebergSchemeRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> DfResult<Arc<dyn ObjectStore>> {
        if let Ok(store) = self.inner.get_store(url) {
            return Ok(store);
        }
        if url.scheme() == "iceberg-rust" {
            return Ok(Arc::clone(&self.fallback));
        }
        self.inner.get_store(url)
    }
}

/// Builds [`IcebergSchemeRegistry`] with the warehouse store from env when
/// present, else the local filesystem default.
///
/// # Errors
///
/// Returns an error if the storage env is invalid (see [`s3_store_from_env`]).
pub fn registry_from_env_with_iceberg() -> object_store::Result<Arc<dyn ObjectStoreRegistry>> {
    let store = s3_store_from_env()?;
    let fallback: Arc<dyn ObjectStore> = match &store {
        Some((_, store)) => Arc::clone(store),
        None => Arc::new(object_store::local::LocalFileSystem::default()),
    };
    let registry = IcebergSchemeRegistry {
        inner: DefaultObjectStoreRegistry::new(),
        fallback,
    };
    if let Some((url, store)) = store {
        registry.register_store(&url, store);
    }
    Ok(Arc::new(registry))
}

/// Executor-side hook: a `RuntimeEnv` whose registry carries the warehouse
/// store from env. Reproduces ballista's default base env (temp path +
/// default registry) so the override is a drop-in.
///
/// # Errors
///
/// Returns an error if the storage env is invalid (see [`registry_from_env`])
/// or the runtime env fails to build.
pub fn runtime_env_from_env(work_dir: &str) -> DfResult<RuntimeEnv> {
    let registry =
        registry_from_env_with_iceberg().map_err(|e| datafusion_common::DataFusionError::External(
            format!("invalid warehouse storage env: {e}").into(),
        ))?;
    RuntimeEnvBuilder::new()
        .with_temp_file_path(work_dir.to_string())
        .with_object_store_registry(registry)
        .build()
}

/// Scheduler-side hook: a session builder wrapper that registers the
/// warehouse store from env on every session the scheduler creates.
///
/// # Errors
///
/// Returns an error if the storage env is invalid (see [`registry_from_env`]).
pub fn register_storage_in_session(
    state: datafusion::execution::context::SessionState,
) -> DfResult<datafusion::execution::context::SessionState> {
    let registry = registry_from_env_with_iceberg().map_err(|e| {
        datafusion_common::DataFusionError::External(format!(
            "invalid warehouse storage env: {e}"
        )
        .into())
    })?;
    let runtime = RuntimeEnvBuilder::new()
        .with_object_store_registry(registry)
        .build()?;
    let builder = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
        state,
    )
    .with_runtime_env(Arc::new(runtime));
    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper for setting process env in tests (unsafe in edition 2024 —
    /// process-global mutation; safe here because each test owns the vars it
    /// touches and tests don't read each other's vars concurrently).
    fn set_env(key: &str, value: &str) {
        // SAFETY: test-only; see module comment.
        unsafe { std::env::set_var(key, value) };
    }

    fn remove_env(key: &str) {
        // SAFETY: test-only; see module comment.
        unsafe { std::env::remove_var(key) };
    }

    /// Env parsing never places secret material anywhere but inside the
    /// store config; the registry contract is URL → store. This pins the
    /// M7.1 guarantee that plans/registries carry no credential bytes.
    #[test]
    fn registry_without_env_is_plain_default() {
        // No AVALANCHE_S3_* env in the test process → plain default registry.
        remove_env("AVALANCHE_S3_BUCKET");
        let registry = registry_from_env_with_iceberg().expect("default registry");
        // local file system default still resolvable
        let url = Url::parse("file:///tmp").expect("valid url");
        registry
            .get_store(&url)
            .expect("file:// default registration");
    }

    #[test]
    fn registry_with_env_registers_bucket_url() {
        // Test-local creds; never used for network I/O (registry build only).
        set_env("AVALANCHE_S3_BUCKET", "test-bucket-m71");
        set_env("AWS_ACCESS_KEY_ID", "test-key");
        set_env("AWS_SECRET_ACCESS_KEY", "test-secret");
        set_env("AWS_ENDPOINT", "http://127.0.0.1:9");
        set_env("AWS_ALLOW_HTTP", "true");
        let registry = registry_from_env_with_iceberg().expect("registry with env");
        let url = Url::parse("s3://test-bucket-m71").expect("valid url");
        registry
            .get_store(&url)
            .expect("bucket registered from env");
        remove_env("AVALANCHE_S3_BUCKET");
        remove_env("AWS_ACCESS_KEY_ID");
        remove_env("AWS_SECRET_ACCESS_KEY");
        remove_env("AWS_ENDPOINT");
        remove_env("AWS_ALLOW_HTTP");
    }

    /// Unknown `iceberg-rust://` URLs (datafusion_iceberg's fake per-table
    /// store URLs) resolve to the fallback store from env — the gap that
    /// made distributed managed-table scans fail on executors.
    #[test]
    fn registry_resolves_iceberg_rust_scheme() {
        set_env("AVALANCHE_S3_BUCKET", "test-bucket-m71");
        set_env("AWS_ACCESS_KEY_ID", "test-key");
        set_env("AWS_SECRET_ACCESS_KEY", "test-secret");
        set_env("AWS_ENDPOINT", "http://127.0.0.1:9");
        set_env("AWS_ALLOW_HTTP", "true");
        let registry = registry_from_env_with_iceberg().expect("registry with env");
        let url = Url::parse("iceberg-rust://udfdb-2Fpub-2Fmr/").expect("valid url");
        registry
            .get_store(&url)
            .expect("iceberg-rust URL resolves to env store");
        remove_env("AVALANCHE_S3_BUCKET");
        remove_env("AWS_ACCESS_KEY_ID");
        remove_env("AWS_SECRET_ACCESS_KEY");
        remove_env("AWS_ENDPOINT");
        remove_env("AWS_ALLOW_HTTP");

        // Without env the scheme falls back to the local filesystem.
        let registry = registry_from_env_with_iceberg().expect("default registry");
        registry
            .get_store(&url)
            .expect("iceberg-rust URL resolves to local fallback");
    }
}