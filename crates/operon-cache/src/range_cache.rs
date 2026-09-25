use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, RecoverMode,
};
use futures::{StreamExt, TryStreamExt};
use operon_store::{Store, StoreError};

/// Errors returned by [`RangeCache`].
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("range {start}..{end} out of bounds for {path} (size {size})")]
    OutOfRange {
        path: String,
        start: u64,
        end: u64,
        size: u64,
    },
    #[error("size mismatch for {path}: expected {expected} bytes, got {actual}")]
    SizeMismatch {
        path: String,
        expected: u64,
        actual: u64,
    },
}

/// NVMe (or any local directory) tier configuration.
///
/// The disk tier always starts cold: it never recovers blocks left behind by a
/// previous process. A directory reused across runs (or across a config change
/// such as a different `block_size`) is treated as empty rather than replayed,
/// so a stale on-disk block can never be served as if it matched the current
/// configuration.
#[derive(Clone, Debug)]
pub struct DiskConfig {
    pub dir: PathBuf,
    pub capacity_bytes: usize,
}

/// Configuration for [`RangeCache`].
#[derive(Clone, Debug)]
pub struct RangeCacheConfig {
    /// Block size in bytes. Reads are served in whole blocks.
    pub block_size: u64,
    /// RAM budget for cached blocks.
    pub memory_bytes: usize,
    /// Optional disk tier. `None` keeps the cache RAM-only.
    pub disk: Option<DiskConfig>,
}

impl Default for RangeCacheConfig {
    fn default() -> Self {
        Self {
            block_size: 1024 * 1024,
            memory_bytes: 256 * 1024 * 1024,
            disk: None,
        }
    }
}

/// Hit/miss counters, in blocks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub checksum_failures: u64,
    /// Lookups where the foyer cache itself returned an error (for example disk
    /// I/O on the disk tier). These are treated as misses and served from the
    /// store; they never fail the read.
    pub cache_errors: u64,
}

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    checksum_failures: AtomicU64,
    cache_errors: AtomicU64,
}

/// Read-through cache of byte ranges of immutable objects.
#[derive(Clone)]
pub struct RangeCache {
    store: Store,
    blocks: HybridCache<String, Bytes>,
    sizes: moka::future::Cache<String, u64>,
    block_size: u64,
    counters: Arc<Counters>,
}

impl std::fmt::Debug for RangeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RangeCache")
            .field("block_size", &self.block_size)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

const CRC_LEN: usize = 4;

/// Caps how many block fetches a single multi-block `read` drives concurrently,
/// so a large range request cannot open unbounded concurrent store/cache lookups.
const MAX_CONCURRENT_BLOCK_FETCHES: usize = 16;

impl RangeCache {
    pub async fn new(store: Store, config: RangeCacheConfig) -> Result<Self, CacheError> {
        if config.block_size == 0 {
            return Err(CacheError::Cache("block_size must be > 0".into()));
        }
        let memory = HybridCacheBuilder::new()
            .with_name("operon-range-cache")
            .memory(config.memory_bytes)
            .with_weighter(|key: &String, value: &Bytes| key.len() + value.len())
            .storage();
        let blocks = match &config.disk {
            Some(disk) => {
                let device = FsDeviceBuilder::new(&disk.dir)
                    .with_capacity(disk.capacity_bytes)
                    .build()
                    .map_err(|e| CacheError::Cache(e.to_string()))?;
                memory
                    .with_engine_config(BlockEngineConfig::new(device))
                    // The disk tier is immutable-block cache data, not a source of
                    // truth (see `DiskConfig`): never recover it on open, so a
                    // reused directory or a changed `block_size` can't resurrect a
                    // stale block under a key the current config would also use.
                    .with_recover_mode(RecoverMode::None)
                    .build()
                    .await
            }
            None => memory.build().await,
        }
        .map_err(|e| CacheError::Cache(e.to_string()))?;
        Ok(Self {
            store,
            blocks,
            sizes: moka::future::Cache::new(1_000_000),
            block_size: config.block_size,
            counters: Arc::default(),
        })
    }

    /// Size of the object at `path` (cached after the first lookup).
    pub async fn size(&self, path: &str) -> Result<u64, CacheError> {
        if let Some(size) = self.sizes.get(path).await {
            return Ok(size);
        }
        let size = self.store.head(path).await?.size;
        self.sizes.insert(path.to_string(), size).await;
        Ok(size)
    }

    /// Reads bytes `range` of the immutable object at `path`.
    pub async fn read(&self, path: &str, range: Range<u64>) -> Result<Bytes, CacheError> {
        let size = self.size(path).await?;
        self.read_sized(path, size, range)
            .await
            .map(|(bytes, _)| bytes)
    }

    /// Reads bytes `range` of the immutable object at `path`, whose `size`
    /// the caller already knows (for example a split's size from its
    /// manifest), so a cold read costs no HEAD: one GET per run of missing
    /// blocks.
    ///
    /// Every GET's response also carries the object's size, and a `size`
    /// that it contradicts is a [`CacheError::SizeMismatch`] before any
    /// block is cached. So is a `size` that contradicts the size already
    /// known, without a request. Once a GET has confirmed `size`, it is
    /// remembered for later [`Self::size`] and [`Self::read`] calls.
    pub async fn read_with_size(
        &self,
        path: &str,
        size: u64,
        range: Range<u64>,
    ) -> Result<Bytes, CacheError> {
        if let Some(known) = self.sizes.get(path).await
            && known != size
        {
            return Err(CacheError::SizeMismatch {
                path: path.to_string(),
                expected: size,
                actual: known,
            });
        }
        let (bytes, confirmed) = self.read_sized(path, size, range).await?;
        if confirmed {
            self.sizes.insert(path.to_string(), size).await;
        }
        Ok(bytes)
    }

    /// Reads bytes `range` of the object at `path`, which is `size` bytes:
    /// the cached blocks, then each run of missing blocks with one GET.
    /// Returns the bytes and whether a GET confirmed `size`.
    async fn read_sized(
        &self,
        path: &str,
        size: u64,
        range: Range<u64>,
    ) -> Result<(Bytes, bool), CacheError> {
        if range.start > range.end || range.end > size {
            return Err(CacheError::OutOfRange {
                path: path.to_string(),
                start: range.start,
                end: range.end,
                size,
            });
        }
        if range.start == range.end {
            return Ok((Bytes::new(), false));
        }
        let first = range.start / self.block_size;
        let last = (range.end - 1) / self.block_size;
        let mut blocks: Vec<Option<Bytes>> =
            futures::stream::iter((first..=last).map(|index| self.cached_block(path, index, size)))
                .buffered(MAX_CONCURRENT_BLOCK_FETCHES)
                .collect()
                .await;

        // The maximal runs of missing blocks, as block index ranges.
        let mut runs: Vec<Range<u64>> = Vec::new();
        for (index, block) in (first..=last).zip(&blocks) {
            if block.is_some() {
                continue;
            }
            match runs.last_mut() {
                Some(run) if run.end == index => run.end += 1,
                _ => runs.push(index..index + 1),
            }
        }
        let confirmed = !runs.is_empty();
        let fetched: Vec<(u64, Vec<Bytes>)> =
            futures::stream::iter(runs.into_iter().map(|run| async move {
                Ok::<_, CacheError>((run.start, self.fetch_run(path, run, size).await?))
            }))
            .buffered(MAX_CONCURRENT_BLOCK_FETCHES)
            .try_collect()
            .await?;
        for (run_start, run_blocks) in fetched {
            for (offset, block) in run_blocks.into_iter().enumerate() {
                blocks[(run_start - first) as usize + offset] = Some(block);
            }
        }

        let mut out = BytesMut::with_capacity((range.end - range.start) as usize);
        for (index, block) in (first..=last).zip(blocks) {
            let block = block.ok_or_else(|| CacheError::Cache("a block was not fetched".into()))?;
            let block_start = index * self.block_size;
            let from = range.start.saturating_sub(block_start) as usize;
            let to = (range.end - block_start).min(block.len() as u64) as usize;
            out.extend_from_slice(&block[from..to]);
        }
        Ok((out.freeze(), confirmed))
    }

    /// Forgets `path`: its size and its cached blocks, so that a deleted
    /// object is not reported as present (and an object written at the same
    /// path later is read afresh).
    pub async fn forget(&self, path: &str) {
        if let Some(size) = self.sizes.get(path).await {
            for index in 0..size.div_ceil(self.block_size) {
                self.blocks.remove(&block_key(path, index));
            }
        }
        self.sizes.invalidate(path).await;
    }

    /// Gracefully closes the cache, flushing any in-memory blocks to the disk
    /// tier (if configured) and waiting for pending disk writes to finish.
    ///
    /// Not required for correctness (the disk tier always starts cold, see
    /// [`DiskConfig`]), but recommended before process exit so a later reopen
    /// can reuse warm blocks instead of refetching them from the store.
    pub async fn close(&self) -> Result<(), CacheError> {
        self.blocks
            .close()
            .await
            .map_err(|e| CacheError::Cache(e.to_string()))
    }

    /// Current hit/miss counters.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            checksum_failures: self.counters.checksum_failures.load(Ordering::Relaxed),
            cache_errors: self.counters.cache_errors.load(Ordering::Relaxed),
        }
    }

    /// Block `index` of the `size`-byte object at `path`, if it is cached
    /// and intact. A block that fails its checksum, or whose length is not
    /// what this read expects (the object shrank after `size` was cached),
    /// is evicted; a foyer lookup error (for example disk I/O) is counted.
    /// Either way the block is then fetched like a miss.
    async fn cached_block(&self, path: &str, index: u64, size: u64) -> Option<Bytes> {
        let key = block_key(path, index);
        // `index` comes from a validated range, so `start < size`.
        let expected = self.block_size.min(size - index * self.block_size);
        let entry = match self.blocks.get(&key).await {
            Ok(entry) => entry?,
            Err(_) => {
                self.counters.cache_errors.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        match verify(entry.value()) {
            Some(data) if data.len() as u64 == expected => {
                self.counters.hits.fetch_add(1, Ordering::Relaxed);
                Some(data)
            }
            _ => {
                self.counters
                    .checksum_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.blocks.remove(&key);
                None
            }
        }
    }

    /// Fetches the blocks `run` of the `size`-byte object at `path` with one
    /// GET, checks the length and the object size the response reports, and
    /// caches each block.
    async fn fetch_run(
        &self,
        path: &str,
        run: Range<u64>,
        size: u64,
    ) -> Result<Vec<Bytes>, CacheError> {
        let start = run.start * self.block_size;
        let end = (run.end * self.block_size).min(size);
        self.counters
            .misses
            .fetch_add(run.end - run.start, Ordering::Relaxed);
        let (data, info) = self.store.get_range_with_info(path, start..end).await?;
        if data.len() as u64 != end - start {
            return Err(CacheError::SizeMismatch {
                path: path.to_string(),
                expected: end - start,
                actual: data.len() as u64,
            });
        }
        if info.size != size {
            return Err(CacheError::SizeMismatch {
                path: path.to_string(),
                expected: size,
                actual: info.size,
            });
        }
        let mut blocks = Vec::with_capacity((run.end - run.start) as usize);
        for index in run {
            let from = (index * self.block_size - start) as usize;
            let to = ((index + 1) * self.block_size).min(size) - start;
            let block = data.slice(from..to as usize);
            self.blocks.insert(block_key(path, index), seal(&block));
            blocks.push(block);
        }
        Ok(blocks)
    }

    #[cfg(test)]
    fn insert_raw_for_test(&self, path: &str, index: u64, value: Bytes) {
        self.blocks.insert(block_key(path, index), value);
    }
}

fn block_key(path: &str, index: u64) -> String {
    format!("{path}\u{0}{index}")
}

/// Appends a crc32c of `data`.
fn seal(data: &Bytes) -> Bytes {
    let mut sealed = BytesMut::with_capacity(data.len() + CRC_LEN);
    sealed.extend_from_slice(data);
    sealed.extend_from_slice(&crc32c::crc32c(data).to_le_bytes());
    sealed.freeze()
}

/// Returns the payload if its trailing crc32c matches.
fn verify(sealed: &Bytes) -> Option<Bytes> {
    let payload_len = sealed.len().checked_sub(CRC_LEN)?;
    let (payload, crc) = sealed.split_at(payload_len);
    let expected = u32::from_le_bytes(crc.try_into().ok()?);
    (crc32c::crc32c(payload) == expected).then(|| sealed.slice(..payload_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn corrupted_block_is_detected_evicted_and_refetched() {
        let store = Store::in_memory();
        store
            .put("obj", Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let cache = RangeCache::new(
            store,
            RangeCacheConfig {
                block_size: 4,
                memory_bytes: 1 << 20,
                disk: None,
            },
        )
        .await
        .unwrap();

        cache.insert_raw_for_test("obj", 0, Bytes::from_static(b"XXXX\0\0\0\0"));
        let data = cache.read("obj", 0..4).await.unwrap();

        assert_eq!(data, Bytes::from_static(b"abcd"));
        let stats = cache.stats();
        assert_eq!(stats.checksum_failures, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn seal_and_verify_round_trip() {
        let data = Bytes::from_static(b"hello");
        assert_eq!(verify(&seal(&data)), Some(data));
        assert_eq!(verify(&Bytes::from_static(b"ab")), None);
    }
}
