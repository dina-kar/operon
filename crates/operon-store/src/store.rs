use std::sync::Arc;

use bytes::Bytes;
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
