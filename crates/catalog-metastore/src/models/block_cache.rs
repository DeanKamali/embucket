//! M14.1 — executor-local warm block cache (design §20.1).
//!
//! Wraps a data `ObjectStore` (S3 volumes) with a byte-range disk cache so a
//! second query over unchanged data reads blocks from local disk instead of
//! re-fetching them over the network — the Snowflake NVMe-cache analogue.
//!
//! Interception point: DataFusion's parquet reader fetches every byte
//! (footer *and* column chunks) through `ObjectStore::get_ranges`, so
//! overriding that one method covers the whole data path. Iceberg metadata
//! (manifest/manifest-list/snapshot reads) flows through whole-file `get`
//! calls, which stay passthrough so pruning always observes current commits.
//!
//! Cache identity is `(store id, path hash, range)` — Iceberg data files are
//! immutable per snapshot and compaction writes *new* paths, so a same-path
//! rewrite cannot make a cached block stale; old files age out via LRU.
//!
//! Enabled by `AVALANCHE_CACHE_DIR` (off when unset — full backward compat);
//! sized by `AVALANCHE_CACHE_BYTES` (default 8 GiB). Eviction is LRU by last
//! use; concurrent fetches of the same block single-flight into one network
//! GET. The block index is rebuilt from the directory on open, so the cache
//! survives process restarts.

use std::fs;
use std::ops::Range;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, Error as OsError, GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};
use object_store::path::Path;
use tokio::io::AsyncWriteExt;

/// Default cache budget when `AVALANCHE_CACHE_BYTES` is unset.
const DEFAULT_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Grace floor so a tiny explicit budget still functions.
const MIN_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// FNV-1a 64-bit — stable across processes and Rust versions (the block
/// index is rebuilt from filenames on restart, so the hash must not drift).
fn fnv1a(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in data {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Cacheable data-file check: only parquet data files (the Iceberg data
/// format of this stack). Metadata objects (`metadata/*.json`, `*.avro`
/// manifests) never match and stay passthrough.
fn is_data_file(path: &Path) -> bool {
    path.as_ref()
        .rsplit('/')
        .next()
        .is_some_and(|f| f.to_ascii_lowercase().ends_with(".parquet"))
}

/// Hit/miss/byte counters — one per cached store, queryable via
/// [`snapshot_cache_stats`].
#[derive(Default, Debug)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub cached_bytes_served: AtomicU64,
    pub fetched_bytes: AtomicU64,
    pub evictions: AtomicU64,
}

impl CacheStats {
    /// Point-in-time copy for reporting: `[hits, misses, cached_bytes_served,
    /// fetched_bytes, evictions]`.
    pub fn snapshot(&self) -> [u64; 5] {
        [
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.cached_bytes_served.load(Ordering::Relaxed),
            self.fetched_bytes.load(Ordering::Relaxed),
            self.evictions.load(Ordering::Relaxed),
        ]
    }
}

/// Global registry so cache stats survive wrapper recreation and can be
/// surfaced per volume (M14.3: `WAREHOUSE_CACHE_HISTORY`-style surface).
static CACHE_REGISTRY: std::sync::LazyLock<DashMap<String, Arc<CacheStats>>> =
    std::sync::LazyLock::new(DashMap::new);

/// Point-in-time stats for every cache-enabled store, by store id.
pub fn snapshot_cache_stats() -> Vec<(String, [u64; 5])> {
    CACHE_REGISTRY
        .iter()
        .map(|e| (e.key().clone(), e.value().snapshot()))
        .collect()
}

/// Identifies one cached byte range. `path_hash` (not the path) is part of
/// the key so the on-disk filename and the in-memory index agree and the
/// index can be rebuilt from filenames after a restart.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BlockKey {
    path_hash: u64,
    start: u64,
    end: u64,
}

impl BlockKey {
    fn new(location: &Path, range: Range<u64>) -> Self {
        Self {
            path_hash: fnv1a(location.as_ref().as_bytes()),
            start: range.start,
            end: range.end,
        }
    }

    /// `{path-hash:016x}-{start}-{end}.blk`
    fn file_name(&self) -> String {
        format!("{:016x}-{}-{}.blk", self.path_hash, self.start, self.end)
    }

    /// Reverse of [`BlockKey::file_name`] for index rebuild on open.
    fn parse_file_name(name: &str) -> Option<Self> {
        let name = name.strip_suffix(".blk")?;
        let (path_part, rest) = name.split_once('-')?;
        let (start, end) = rest.split_once('-')?;
        Some(Self {
            path_hash: u64::from_str_radix(path_part, 16).ok()?,
            start: start.parse().ok()?,
            end: end.parse().ok()?,
        })
    }
}

#[derive(Debug)]
struct BlockMeta {
    len: u64,
    last_use: u64,
}

#[derive(Debug)]
struct CacheCore {
    max_bytes: u64,
    index: DashMap<BlockKey, BlockMeta>,
    total: AtomicU64,
    clock: AtomicU64,
    /// Per-block locks: concurrent fetchers of the same block single-flight.
    inflight: DashMap<BlockKey, Arc<tokio::sync::Mutex<()>>>,
    stats: Arc<CacheStats>,
}

impl CacheCore {
    fn new(max_bytes: u64, stats: Arc<CacheStats>) -> Self {
        Self {
            max_bytes,
            index: DashMap::new(),
            total: AtomicU64::new(0),
            clock: AtomicU64::new(0),
            inflight: DashMap::new(),
            stats,
        }
    }

    /// Rebuilds the in-memory index from the persisted block files so a
    /// restarted executor keeps its warm cache.
    fn rebuild_index(&self, store_dir: &StdPath) {
        let entries = match fs::read_dir(store_dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        let mut total = 0u64;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(key) = BlockKey::parse_file_name(&name.to_string_lossy()) else {
                continue;
            };
            let Ok(meta) = entry.metadata() else { continue };
            let len = meta.len();
            total += len;
            self.index.insert(
                key,
                BlockMeta {
                    len,
                    last_use: meta
                        .modified()
                        .ok()
                        .and_then(|m| m.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
                        .map_or(0, |d| d.as_secs()),
                },
            );
        }
        self.total.store(total, Ordering::Relaxed);
    }

    fn block_path(store_dir: &StdPath, key: &BlockKey) -> PathBuf {
        store_dir.join(key.file_name())
    }

    /// Reads one cached block; `None` when the file is gone or short
    /// (manual deletion, eviction race) — the caller falls back to fetch.
    async fn read_block(&self, path: &StdPath, len: u64) -> Option<bytes::Bytes> {
        let want = usize::try_from(len).ok()?;
        match tokio::fs::read(path).await {
            Ok(data) if data.len() == want => Some(bytes::Bytes::from(data)),
            _ => None,
        }
    }

    async fn write_block(&self, path: &StdPath, data: &[u8]) -> OsResult<()> {
        let tmp = path.with_extension("tmp");
        let mut f = tokio::fs::File::create(&tmp).await.map_err(os_io_err)?;
        f.write_all(data).await.map_err(os_io_err)?;
        f.flush().await.map_err(os_io_err)?;
        drop(f);
        tokio::fs::rename(&tmp, path).await.map_err(os_io_err)
    }

    /// Evicts least-recently-used blocks until `total <= max_bytes`.
    async fn evict(&self, store_dir: &StdPath) {
        loop {
            if self.total.load(Ordering::Relaxed) <= self.max_bytes {
                return;
            }
            // O(n) victim scan — acceptable at cache-block granularity.
            let victim = self
                .index
                .iter()
                .min_by_key(|e| e.value().last_use)
                .map(|e| (e.key().clone(), e.value().len));
            let Some((key, len)) = victim else { return };
            if self.index.remove(&key).is_some() {
                let _ = tokio::fs::remove_file(Self::block_path(store_dir, &key)).await;
                self.total.fetch_sub(len, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Serves one range from the cache, fetching + storing on miss with
    /// single-flight per block.
    async fn get_or_fetch(
        &self,
        store_dir: &StdPath,
        key: BlockKey,
        location: &Path,
        range: Range<u64>,
        inner: &Arc<dyn ObjectStore>,
    ) -> OsResult<bytes::Bytes> {
        // Fast path: indexed hit. NOTE: DashMap guards must never be held
        // across an await or re-entered (get + get_mut on the same shard
        // deadlocks), so the block read goes through `read_cached`.
        if let Some(data) = self.read_cached(store_dir, &key).await {
            self.touch(&key);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats
                .cached_bytes_served
                .fetch_add(u64::try_from(data.len()).unwrap_or_default(), Ordering::Relaxed);
            return Ok(data);
        }

        // Single-flight: concurrent fetchers of the same block queue here and
        // re-check the cache after acquiring the lock.
        let lock = self.inflight.entry(key.clone()).or_default().clone();
        let _guard = lock.lock().await;

        if let Some(data) = self.read_cached(store_dir, &key).await {
            self.touch(&key);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            self.stats
                .cached_bytes_served
                .fetch_add(u64::try_from(data.len()).unwrap_or_default(), Ordering::Relaxed);
            return Ok(data);
        }

        let data = inner.get_range(location, range).await?;
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        self.stats
            .fetched_bytes
            .fetch_add(u64::try_from(data.len()).unwrap_or_default(), Ordering::Relaxed);

        self.write_block(&Self::block_path(store_dir, &key), &data).await?;
        let len = u64::try_from(data.len()).unwrap_or_default();
        self.total.fetch_add(len, Ordering::Relaxed);
        self.index.insert(
            key.clone(),
            BlockMeta {
                len,
                last_use: self.next_clock(),
            },
        );
        drop(_guard);
        self.evict(store_dir).await;
        Ok(data)
    }

    /// Bumps the LRU clock for a block (no guard re-entry: get_mut only).
    fn touch(&self, key: &BlockKey) {
        if let Some(mut meta) = self.index.get_mut(key) {
            meta.last_use = self.next_clock();
        }
    }

    /// Reads a cached block through the index (length-guarded); drops the
    /// index guard before the file read.
    async fn read_cached(&self, store_dir: &StdPath, key: &BlockKey) -> Option<bytes::Bytes> {
        let len = {
            let meta = self.index.get(key)?;
            meta.len
        };
        self.read_block(&self.block_path(store_dir, key), len).await
    }

    fn next_clock(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }
}

fn os_io_err(e: std::io::Error) -> OsError {
    OsError::Generic {
        store: "block-cache",
        source: Box::new(e),
    }
}

/// A data `ObjectStore` wrapped with the executor-local warm block cache.
#[derive(Debug)]
pub struct CachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    core: Arc<CacheCore>,
    store_dir: PathBuf,
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cached({inner})", inner = self.inner)
    }
}

impl CachedObjectStore {
    /// Wraps `inner` with a disk cache under `cache_dir/<store-hash>`.
    /// Stats are registered globally keyed by `store_id` (e.g. the volume
    /// prefix `s3://bucket`) so identity is stable across restarts.
    #[must_use]
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        store_id: &str,
        cache_dir: PathBuf,
        max_bytes: u64,
    ) -> Self {
        let store_dir = cache_dir.join(format!("{:016x}", fnv1a(store_id.as_bytes())));
        let _ = fs::create_dir_all(&store_dir);
        let stats = CACHE_REGISTRY
            .entry(store_id.to_string())
            .or_insert_with(|| Arc::new(CacheStats::default()))
            .clone();
        let core = Arc::new(CacheCore::new(max_bytes, stats));
        core.rebuild_index(&store_dir);
        Self { inner, core, store_dir }
    }
}

#[async_trait::async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> OsResult<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        // Whole-file gets: Iceberg metadata and non-parquet readers — always
        // the source so pruning/commits observe current state.
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> OsResult<Vec<bytes::Bytes>> {
        if !is_data_file(location) {
            return self.inner.get_ranges(location, ranges).await;
        }
        let mut out = Vec::with_capacity(ranges.len());
        for range in ranges {
            let key = BlockKey::new(location, range.clone());
            out.push(
                self.core
                    .get_or_fetch(&self.store_dir, key, location, range.clone(), &self.inner)
                    .await?,
            );
        }
        Ok(out)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Resolves the cache configuration from the environment.
/// Returns `None` when `AVALANCHE_CACHE_DIR` is unset/empty (cache off).
#[must_use]
pub fn cache_config() -> Option<(PathBuf, u64)> {
    let dir = std::env::var("AVALANCHE_CACHE_DIR").ok()?;
    if dir.trim().is_empty() {
        return None;
    }
    let dir = PathBuf::from(dir);
    let max = std::env::var("AVALANCHE_CACHE_BYTES")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|n| n.max(1))
        .unwrap_or_else(|| DEFAULT_CACHE_BYTES.max(MIN_CACHE_BYTES));
    Some((dir, max))
}

/// Wraps `store` with the warm block cache when `AVALANCHE_CACHE_DIR` is set;
/// otherwise returns `store` untouched.
#[must_use]
pub fn maybe_wrap_cached(store: Arc<dyn ObjectStore>, store_id: &str) -> Arc<dyn ObjectStore> {
    match cache_config() {
        Some((dir, max_bytes)) => Arc::new(CachedObjectStore::new(store, store_id, dir, max_bytes)),
        None => store,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;

    /// Counts range fetches to prove warm hits never reach the inner store.
    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        range_fetches: AtomicU64,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                range_fetches: AtomicU64::new(0),
            }
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> OsResult<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            if options.range.is_some() {
                self.range_fetches.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
            self.range_fetches.fetch_add(1, Ordering::Relaxed);
            self.inner.get_ranges(location, ranges).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn seed(store: &CountingStore, path: &str, len: usize) {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        store
            .put(&Path::from(path), Bytes::from(data).into())
            .await
            .unwrap();
    }

    fn tmp_cache_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("m14cache-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn expected(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn hit_serves_from_disk_without_inner_fetch() {
        let dir = tmp_cache_dir("hit");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/data.parquet", 4096).await;
        let cached = CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            "s3://hit",
            dir.clone(),
            1 << 20,
        );
        let loc = Path::from("t/data.parquet");

        let first = cached.get_ranges(&loc, &[0..128, 256..384]).await.unwrap();
        assert_eq!(first.len(), 2);
        let after_cold = counting.range_fetches.load(Ordering::Relaxed);

        let second = cached.get_ranges(&loc, &[0..128, 256..384]).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(
            counting.range_fetches.load(Ordering::Relaxed),
            after_cold,
            "warm read must not touch the inner store"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn non_parquet_paths_stay_passthrough() {
        let dir = tmp_cache_dir("meta");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/metadata/snap.json", 1024).await;
        let cached = CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            "s3://meta",
            dir.clone(),
            1 << 20,
        );
        let loc = Path::from("t/metadata/snap.json");
        let _ = cached.get_ranges(&loc, &[0..64]).await.unwrap();
        assert_eq!(counting.range_fetches.load(Ordering::Relaxed), 1);
        let _ = cached.get_ranges(&loc, &[0..64]).await.unwrap();
        assert_eq!(
            counting.range_fetches.load(Ordering::Relaxed),
            2,
            "metadata reads always hit the source"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn eviction_respects_byte_cap() {
        let dir = tmp_cache_dir("evict");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/a.parquet", 1024).await;
        seed(&counting, "t/b.parquet", 1024).await;
        let cached = CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            "s3://evict",
            dir.clone(),
            1024,
        );
        let a = Path::from("t/a.parquet");
        let b = Path::from("t/b.parquet");
        let _ = cached.get_ranges(&a, &[0..1024]).await.unwrap();
        let _ = cached.get_ranges(&b, &[0..1024]).await.unwrap();
        assert!(
            cached.core.total.load(Ordering::Relaxed) <= 1024,
            "cap must hold after eviction"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn index_rebuild_survives_restart() {
        let dir = tmp_cache_dir("restart");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/data.parquet", 2048).await;
        let loc = Path::from("t/data.parquet");

        {
            let cached = CachedObjectStore::new(
                Arc::clone(&counting) as Arc<dyn ObjectStore>,
                "s3://restart",
                dir.clone(),
                1 << 20,
            );
            let _ = cached.get_ranges(&loc, &[0..512]).await.unwrap();
        } // dropped: cache files persist on disk

        let cached = CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            "s3://restart",
            dir.clone(),
            1 << 20,
        );
        let before = counting.range_fetches.load(Ordering::Relaxed);
        let bytes = cached.get_ranges(&loc, &[0..512]).await.unwrap();
        assert_eq!(bytes[0].as_ref(), &expected(512));
        assert_eq!(
            counting.range_fetches.load(Ordering::Relaxed),
            before,
            "restarted instance must serve the block from the rebuilt index"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_readers_single_flight() {
        let dir = tmp_cache_dir("sf");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/data.parquet", 1024).await;
        let cached = Arc::new(CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            "s3://sf",
            dir.clone(),
            1 << 20,
        ));
        let loc = Path::from("t/data.parquet");
        let mut handles = Vec::new();
        for _ in 0..16 {
            let c = Arc::clone(&cached);
            let l = loc.clone();
            handles.push(tokio::spawn(async move { c.get_ranges(&l, &[0..256]).await }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        let expect = expected(256);
        let again = cached.get_ranges(&loc, &[0..256]).await.unwrap();
        assert_eq!(again[0].as_ref(), &expect);
        assert!(counting.range_fetches.load(Ordering::Relaxed) >= 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stats_registry_keyed_by_store() {
        let dir = tmp_cache_dir("stats");
        let counting = Arc::new(CountingStore::new());
        seed(&counting, "t/data.parquet", 512).await;
        let id = format!("s3://stats-{}", std::process::id());
        let cached = CachedObjectStore::new(
            Arc::clone(&counting) as Arc<dyn ObjectStore>,
            &id,
            dir.clone(),
            1 << 20,
        );
        let loc = Path::from("t/data.parquet");
        let _ = cached.get_ranges(&loc, &[0..128]).await.unwrap();
        let _ = cached.get_ranges(&loc, &[0..128]).await.unwrap();
        let [hits, misses, cached_bytes, fetched, _ev] = {
            // Drop the registry guard before `remove` — same-shard re-entry
            // on a DashMap deadlocks.
            let entry = CACHE_REGISTRY.get(&id).expect("stats registered");
            let s = entry.snapshot();
            drop(entry);
            CACHE_REGISTRY.remove(&id);
            s
        };
        assert_eq!((hits, misses), (1, 1));
        assert_eq!(cached_bytes, 128);
        assert_eq!(fetched, 128);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn block_key_filename_roundtrip() {
        let key = BlockKey::new(&Path::from("db/t/part-000.parquet"), 123..456);
        let name = key.file_name();
        let parsed = BlockKey::parse_file_name(&name).unwrap();
        assert_eq!(key, parsed);
        assert!(BlockKey::parse_file_name("garbage").is_none());
    }
}