//! Quickwit's [`Storage`] over `operon-store` and `operon-cache` (plan M1.1
//! Task 8 rule 5): split bundles are read through the [`RangeCache`] and
//! written create-only.

use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use operon_cache::{CacheError, RangeCache};
use operon_quickwit::shim::Uri;
use operon_quickwit::storage::{
    BulkDeleteError, DeleteFailure, OwnedBytes, PutPayload, SendableAsync, Storage, StorageError,
    StorageErrorKind, StorageResult,
};
use operon_store::{Store, StoreError};
use tokio::io::{AsyncRead, AsyncWriteExt};

/// A Quickwit [`Storage`] whose paths are relative to `root` in an Operon
/// [`Store`]. Ranged reads go through the [`RangeCache`]; writes are
/// create-only, and a write that finds identical bytes already there
/// succeeds (so a retry after a lost acknowledgement is harmless).
#[derive(Clone)]
pub struct OperonStorage {
    store: Store,
    cache: RangeCache,
    root: String,
    uri: Uri,
}

impl fmt::Debug for OperonStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperonStorage")
            .field("root", &self.root)
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

impl OperonStorage {
    /// A storage rooted at `root`, a store prefix ending with `/` (one is
    /// appended if it is missing; `""` is the store's root).
    pub fn new(store: Store, cache: RangeCache, root: &str) -> Self {
        let mut root = root.to_string();
        if !root.is_empty() && !root.ends_with('/') {
            root.push('/');
        }
        // The URI only names the storage in messages; Quickwit's `Protocol`
        // has no Operon scheme, and `ram` is the one that implies nothing
        // about a backend.
        let uri = Uri::from_str(&format!("ram:///operon/{root}"))
            .unwrap_or_else(|_| Uri::for_test("ram:///operon/"));
        Self {
            store,
            cache,
            root,
            uri,
        }
    }

    /// The store path of `path`.
    fn key(&self, path: &Path) -> StorageResult<String> {
        let path = path.to_str().ok_or_else(|| {
            StorageErrorKind::Internal
                .with_error(anyhow::anyhow!("path {} is not UTF-8", path.display()))
        })?;
        Ok(format!("{}{path}", self.root))
    }

    async fn get_bytes(&self, path: &Path) -> StorageResult<Bytes> {
        let key = self.key(path)?;
        let (bytes, _) = self.store.get(&key).await.map_err(store_error)?;
        Ok(bytes)
    }
}

/// `StoreError::NotFound` is `NotFound`, a retryable store error is `Io`,
/// and anything else is `Internal`.
fn store_error(err: StoreError) -> StorageError {
    let kind = match &err {
        StoreError::NotFound { .. } => StorageErrorKind::NotFound,
        err if err.is_retryable() => StorageErrorKind::Io,
        _ => StorageErrorKind::Internal,
    };
    kind.with_error(err)
}

fn cache_error(err: CacheError) -> StorageError {
    match err {
        CacheError::Store(err) => store_error(err),
        other => StorageErrorKind::Internal.with_error(other),
    }
}

fn owned(bytes: Bytes) -> OwnedBytes {
    OwnedBytes::new(Vec::from(bytes))
}

fn range_u64(range: Range<usize>) -> Range<u64> {
    range.start as u64..range.end as u64
}

#[async_trait]
impl Storage for OperonStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        let key = self.key(path)?;
        let data = Bytes::from(payload.read_all().await?.as_slice().to_vec());
        match self.store.put_if_absent(&key, data.clone()).await {
            Ok(_) => Ok(()),
            Err(StoreError::AlreadyExists { .. }) => {
                let (existing, _) = self.store.get(&key).await.map_err(store_error)?;
                if existing == data {
                    Ok(())
                } else {
                    Err(StorageErrorKind::Internal
                        .with_error(anyhow::anyhow!("{key} already exists with different bytes")))
                }
            }
            Err(err) => Err(store_error(err)),
        }
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let bytes = self.get_bytes(path).await?;
        output.write_all(&bytes).await?;
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let key = self.key(path)?;
        let bytes = self
            .cache
            .read(&key, range_u64(range))
            .await
            .map_err(cache_error)?;
        Ok(owned(bytes))
    }

    async fn get_slice_with_file_len(
        &self,
        path: &Path,
        file_len: u64,
        range: Range<usize>,
    ) -> StorageResult<OwnedBytes> {
        let key = self.key(path)?;
        let bytes = self
            .cache
            .read_with_size(&key, file_len, range_u64(range))
            .await
            .map_err(cache_error)?;
        Ok(owned(bytes))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let bytes = self.get_slice(path, range).await?;
        Ok(Box::new(std::io::Cursor::new(bytes)))
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        Ok(owned(self.get_bytes(path).await?))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        let key = self.key(path)?;
        self.store.delete(&key).await.map_err(store_error)?;
        // Otherwise the cached size would still report the file.
        self.cache.forget(&key).await;
        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let mut successes: Vec<PathBuf> = Vec::with_capacity(paths.len());
        for (at, path) in paths.iter().enumerate() {
            if let Err(error) = self.delete(path).await {
                let failure = DeleteFailure {
                    error: Some(error),
                    ..DeleteFailure::default()
                };
                return Err(BulkDeleteError {
                    error: None,
                    successes,
                    failures: [(path.to_path_buf(), failure)].into_iter().collect(),
                    unattempted: paths[at + 1..].iter().map(|p| p.to_path_buf()).collect(),
                });
            }
            successes.push(path.to_path_buf());
        }
        Ok(())
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        let key = self.key(path)?;
        self.cache.size(&key).await.map_err(cache_error)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}
