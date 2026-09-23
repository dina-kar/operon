use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder};
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
}

/// NVMe (or any local directory) tier configuration.
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
}

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    checksum_failures: AtomicU64,
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
        if range.start > range.end || range.end > size {
            return Err(CacheError::OutOfRange {
                path: path.to_string(),
                start: range.start,
                end: range.end,
                size,
            });
        }
        if range.start == range.end {
            return Ok(Bytes::new());
        }
        let first = range.start / self.block_size;
        let last = (range.end - 1) / self.block_size;
        let blocks = futures::future::try_join_all(
            (first..=last).map(|index| self.block(path, index, size)),
        )
        .await?;

        let mut out = BytesMut::with_capacity((range.end - range.start) as usize);
        for (index, block) in (first..=last).zip(blocks) {
            let block_start = index * self.block_size;
            let from = range.start.saturating_sub(block_start) as usize;
            let to = (range.end - block_start).min(block.len() as u64) as usize;
            out.extend_from_slice(&block[from..to]);
        }
        Ok(out.freeze())
    }

    /// Current hit/miss counters.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.counters.hits.load(Ordering::Relaxed),
            misses: self.counters.misses.load(Ordering::Relaxed),
            checksum_failures: self.counters.checksum_failures.load(Ordering::Relaxed),
        }
    }

    async fn block(&self, path: &str, index: u64, size: u64) -> Result<Bytes, CacheError> {
        let key = block_key(path, index);
        if let Some(entry) = self
            .blocks
            .get(&key)
            .await
            .map_err(|e| CacheError::Cache(e.to_string()))?
        {
            match verify(entry.value()) {
                Some(data) => {
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(data);
                }
                None => {
                    self.counters
                        .checksum_failures
                        .fetch_add(1, Ordering::Relaxed);
                    self.blocks.remove(&key);
                }
            }
        }
        self.counters.misses.fetch_add(1, Ordering::Relaxed);
        let start = index * self.block_size;
        let end = (start + self.block_size).min(size);
        let data = self.store.get_range(path, start..end).await?;
        self.blocks.insert(key, seal(&data));
        Ok(data)
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
