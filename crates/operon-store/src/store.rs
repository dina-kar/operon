use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use crate::error::{StoreError, map_err};

/// Opaque version of an object, used for compare-and-swap writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

/// Metadata about a stored object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub path: String,
    pub size: u64,
    pub version: ObjectVersion,
}

/// Handle to an object store. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
}

impl Store {
    /// Wraps an existing object store.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// An in-memory store, for tests and `operon dev`.
    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemory::new()))
    }

    /// Opens a store from a URL such as `s3://bucket/prefix`, `gs://bucket`,
    /// `az://container`, `file:///abs/dir` or `memory:///`.
    ///
    /// `options` are backend-specific keys (for example `aws_region`); credentials
    /// not given here are read from the environment by the backend.
    pub fn from_url<I, K, V>(url: &str, options: I) -> Result<Self, StoreError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let parsed = url::Url::parse(url).map_err(|e| StoreError::InvalidUrl(e.to_string()))?;
        if parsed.scheme() == "file" {
            let dir = parsed
                .to_file_path()
                .map_err(|()| StoreError::InvalidUrl(url.to_string()))?;
            std::fs::create_dir_all(&dir)
                .map_err(|e| StoreError::InvalidUrl(format!("{url}: {e}")))?;
            let fs = object_store::local::LocalFileSystem::new_with_prefix(&dir)
                .map_err(|e| map_err(url, e))?;
            return Ok(Self::new(Arc::new(fs)));
        }
        let (store, prefix) =
            object_store::parse_url_opts(&parsed, options).map_err(|e| map_err(url, e))?;
        let store: Arc<dyn ObjectStore> = if prefix.as_ref().is_empty() {
            Arc::from(store)
        } else {
            Arc::new(object_store::prefix::PrefixStore::new(store, prefix))
        };
        Ok(Self::new(store))
    }

    /// The underlying object store, for integrations that need it directly.
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    /// Writes `data` only if no object exists at `path`.
    ///
    /// Returns [`StoreError::AlreadyExists`] if an object is already present.
    pub async fn put_if_absent(
        &self,
        path: &str,
        data: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.put_with_mode(path, data, PutMode::Create).await
    }

    /// Writes `data` only if the current object at `path` has version `expected`.
    ///
    /// Returns [`StoreError::PreconditionFailed`] if the object changed, and
    /// [`StoreError::NotSupported`] on backends without conditional updates.
    pub async fn put_if_match(
        &self,
        path: &str,
        data: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, StoreError> {
        let mode = PutMode::Update(UpdateVersion {
            e_tag: expected.e_tag.clone(),
            version: expected.version.clone(),
        });
        self.put_with_mode(path, data, mode).await
    }

    /// Writes `data`, replacing any existing object.
    pub async fn put(&self, path: &str, data: Bytes) -> Result<ObjectVersion, StoreError> {
        self.put_with_mode(path, data, PutMode::Overwrite).await
    }

    async fn put_with_mode(
        &self,
        path: &str,
        data: Bytes,
        mode: PutMode,
    ) -> Result<ObjectVersion, StoreError> {
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        let result = self
            .inner
            .put_opts(&Path::from(path), PutPayload::from(data), opts)
            .await
            .map_err(|e| map_err(path, e))?;
        Ok(ObjectVersion {
            e_tag: result.e_tag,
            version: result.version,
        })
    }

    /// Reads a whole object.
    pub async fn get(&self, path: &str) -> Result<(Bytes, ObjectInfo), StoreError> {
        let result = self
            .inner
            .get(&Path::from(path))
            .await
            .map_err(|e| map_err(path, e))?;
        let info = to_info(&result.meta);
        let bytes = result.bytes().await.map_err(|e| map_err(path, e))?;
        Ok((bytes, info))
    }

    /// Reads bytes `range` of an object. An empty range returns empty bytes.
    pub async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes, StoreError> {
        if range.start >= range.end {
            return Ok(Bytes::new());
        }
        self.inner
            .get_range(&Path::from(path), range)
            .await
            .map_err(|e| map_err(path, e))
    }

    /// Reads object metadata.
    pub async fn head(&self, path: &str) -> Result<ObjectInfo, StoreError> {
        let meta = self
            .inner
            .head(&Path::from(path))
            .await
            .map_err(|e| map_err(path, e))?;
        Ok(to_info(&meta))
    }

    /// Deletes an object. Deleting a missing object succeeds.
    pub async fn delete(&self, path: &str) -> Result<(), StoreError> {
        match self.inner.delete(&Path::from(path)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(map_err(path, e)),
        }
    }

    /// Lists all objects under `prefix` (recursively), sorted by path.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, StoreError> {
        let prefix_path = Path::from(prefix);
        let prefix_arg = if prefix.is_empty() {
            None
        } else {
            Some(&prefix_path)
        };
        let mut infos: Vec<ObjectInfo> = self
            .inner
            .list(prefix_arg)
            .map_ok(|meta| to_info(&meta))
            .try_collect()
            .await
            .map_err(|e| map_err(prefix, e))?;
        infos.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(infos)
    }
}

fn to_info(meta: &object_store::ObjectMeta) -> ObjectInfo {
    ObjectInfo {
        path: meta.location.to_string(),
        size: meta.size,
        version: ObjectVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        },
    }
}
