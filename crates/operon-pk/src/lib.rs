//! Primary-key indexes on SlateDB (design §03 §5).
//!
//! A [`PkIndex`] is one SlateDB database under the caller's prefix (by
//! convention `ns/<ns>/pk/<object_id>/`, design §01 §6), written by exactly
//! one writer: opening a [`PkIndex`] fences every earlier writer of the same
//! prefix, whose writes then fail with [`PkError::Fenced`] and never become
//! visible. A write returns only once it is durable in object storage.
//! [`PkReader`] is a read-only view that picks up new writes on
//! [`PkReader::refresh`].

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use operon_store::Store;
use slatedb::config::{DbReaderOptions, Settings};
use slatedb::db_cache::DbCache;
use slatedb::{CloseReason, Db, DbReader, DbReaderMode, ErrorKind, WriteBatch};

/// How a [`PkIndex`] writer runs.
#[derive(Clone)]
pub struct PkIndexConfig {
    /// How often buffered writes are flushed to the object store's WAL. A
    /// [`PkIndex::write`] waits for its own flush, so this bounds write
    /// latency. Default: SlateDB's (100 ms).
    pub flush_interval: Duration,
    /// A block cache shared by indexes, or `None` for none.
    pub cache: Option<Arc<dyn DbCache>>,
}

impl Default for PkIndexConfig {
    fn default() -> Self {
        Self {
            flush_interval: Settings::default()
                .flush_interval
                .unwrap_or(Duration::from_millis(100)),
            cache: None,
        }
    }
}

impl fmt::Debug for PkIndexConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkIndexConfig")
            .field("flush_interval", &self.flush_interval)
            .field("cache", &self.cache.is_some())
            .finish()
    }
}

/// Errors from a [`PkIndex`] or [`PkReader`].
#[derive(Debug, thiserror::Error)]
pub enum PkError {
    /// A newer writer opened the index: this one can no longer write, and its
    /// failed writes are not visible.
    #[error("fenced by a newer writer")]
    Fenced,
    /// The object store failed or is unavailable; retry later.
    #[error("object store: {0}")]
    Store(String),
    /// Stored data failed a check.
    #[error("corrupt index: {0}")]
    Corrupt(String),
    /// The index was closed.
    #[error("the index is closed")]
    Closed,
    #[error("{0}")]
    Other(String),
}

impl From<slatedb::Error> for PkError {
    fn from(err: slatedb::Error) -> Self {
        match err.kind() {
            ErrorKind::Closed(CloseReason::Fenced) => PkError::Fenced,
            ErrorKind::Closed(CloseReason::Clean) => PkError::Closed,
            ErrorKind::Unavailable => PkError::Store(err.to_string()),
            ErrorKind::Data => PkError::Corrupt(err.to_string()),
            _ => PkError::Other(err.to_string()),
        }
    }
}

/// SlateDB's view of the index: the store's backend under `path`.
fn scoped(store: &Store, path: &str) -> Result<Arc<dyn ObjectStore>, PkError> {
    let prefix = Path::parse(path.trim_end_matches('/'))
        .map_err(|e| PkError::Other(format!("invalid index path {path:?}: {e}")))?;
    if prefix.as_ref().is_empty() {
        return Err(PkError::Other(
            "an index needs a non-empty path".to_string(),
        ));
    }
    Ok(Arc::new(PrefixStore::new(store.inner().clone(), prefix)))
}

/// The root of the database inside its prefix store.
const DB_ROOT: &str = "db";

async fn scan(
    iter: Result<slatedb::DbIterator, slatedb::Error>,
    limit: usize,
) -> Result<Vec<(Bytes, Bytes)>, PkError> {
    let mut iter = iter?;
    let mut out = Vec::new();
    while out.len() < limit {
        match iter.next().await? {
            Some(kv) => out.push((kv.key, kv.value)),
            None => break,
        }
    }
    Ok(out)
}

/// The single writer of one primary-key index.
pub struct PkIndex {
    db: Db,
    path: String,
}

impl fmt::Debug for PkIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkIndex")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl PkIndex {
    /// Opens (or creates) the index at `path` as its writer. Any writer that
    /// had it open is fenced from now on.
    pub async fn open(store: &Store, path: &str, config: PkIndexConfig) -> Result<Self, PkError> {
        let settings = Settings {
            flush_interval: Some(config.flush_interval),
            ..Settings::default()
        };
        let mut builder = Db::builder(DB_ROOT, scoped(store, path)?).with_settings(settings);
        builder = match config.cache {
            Some(cache) => builder.with_db_cache(cache),
            None => builder.with_db_cache_disabled(),
        };
        let db = builder.build().await?;
        Ok(Self {
            db,
            path: path.to_string(),
        })
    }

    /// The value of `key`, if any.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, PkError> {
        Ok(self.db.get(key).await?)
    }

    /// Applies `batch` atomically (`None` deletes the key) and returns once
    /// it is durable. On [`PkError::Fenced`] nothing of it is visible to the
    /// newer writer or to readers.
    pub async fn write(&self, batch: Vec<(Bytes, Option<Bytes>)>) -> Result<(), PkError> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut write = WriteBatch::new();
        for (key, value) in batch {
            match value {
                Some(value) => write.put(key, value),
                None => write.delete(key),
            }
        }
        let handle = self.db.write(write).await?;
        handle.await_durable().await?;
        Ok(())
    }

    /// Up to `limit` entries whose keys start with `prefix`, in key order.
    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, PkError> {
        scan(self.db.scan_prefix(prefix, ..).await, limit).await
    }

    /// Flushes and closes the writer.
    pub async fn close(self) -> Result<(), PkError> {
        Ok(self.db.close().await?)
    }
}

/// A read-only view of a primary-key index. It sees what was durable when it
/// was opened or last refreshed (and, between refreshes, whatever SlateDB's
/// background polling picks up).
pub struct PkReader {
    reader: DbReader,
    store: Arc<dyn ObjectStore>,
    path: String,
}

impl fmt::Debug for PkReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkReader")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl PkReader {
    async fn open_reader(store: Arc<dyn ObjectStore>) -> Result<DbReader, PkError> {
        // `FollowLatest` never writes to the index (no reader checkpoints),
        // so a reader cannot interfere with the writer.
        Ok(DbReader::open(
            DB_ROOT,
            store,
            DbReaderMode::FollowLatest,
            DbReaderOptions::default(),
        )
        .await?)
    }

    /// Opens a read-only view of the index at `path`. The index must exist
    /// (a writer has opened it at least once).
    pub async fn open(store: &Store, path: &str) -> Result<Self, PkError> {
        let store = scoped(store, path)?;
        let reader = Self::open_reader(store.clone()).await?;
        Ok(Self {
            reader,
            store,
            path: path.to_string(),
        })
    }

    /// Picks up every write that was durable before this call.
    pub async fn refresh(&mut self) -> Result<(), PkError> {
        let fresh = Self::open_reader(self.store.clone()).await?;
        let stale = std::mem::replace(&mut self.reader, fresh);
        if let Err(err) = stale.close().await {
            tracing::debug!(path = %self.path, %err, "closing a stale index reader");
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, PkError> {
        Ok(self.reader.get(key).await?)
    }

    /// Up to `limit` entries whose keys start with `prefix`, in key order.
    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, PkError> {
        scan(self.reader.scan_prefix(prefix, ..).await, limit).await
    }

    pub async fn close(self) -> Result<(), PkError> {
        Ok(self.reader.close().await?)
    }
}
