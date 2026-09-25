// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-storage/src/ram_storage.rs); modified for Operon: RamStorageFactory removed with tests needing unvendored helpers; RamStorageBuilder derives Debug.

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::shim::uri::Uri;
use crate::storage::storage::SendableAsync;
use crate::storage::{BulkDeleteError, OwnedBytes, Storage, StorageErrorKind, StorageResult};

/// In Ram implementation of quickwit's storage.
///
/// This implementation is mostly useful in unit tests.
#[derive(Clone)]
pub struct RamStorage {
    uri: Uri,
    files: Arc<RwLock<HashMap<PathBuf, OwnedBytes>>>,
}

impl fmt::Debug for RamStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("RamStorage")
            .field("uri", &self.uri)
            .finish()
    }
}

impl Default for RamStorage {
    fn default() -> Self {
        Self {
            uri: Uri::for_test("ram:///"),
            files: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl RamStorage {
    /// Creates a [`RamStorageBuilder`]
    pub fn builder() -> RamStorageBuilder {
        RamStorageBuilder::default()
    }

    async fn put_data(&self, path: &Path, payload: OwnedBytes) {
        self.files.write().await.insert(path.to_path_buf(), payload);
    }

    async fn get_data(&self, path: &Path) -> Option<OwnedBytes> {
        self.files.read().await.get(path).cloned()
    }

    /// Returns the list of files that are present in the RamStorage.
    pub async fn list_files(&self) -> Vec<PathBuf> {
        self.files.read().await.keys().cloned().collect()
    }
}

#[async_trait]
impl Storage for RamStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn crate::storage::PutPayload>,
    ) -> crate::storage::StorageResult<()> {
        let payload_bytes = payload.read_all().await?;
        self.put_data(path, payload_bytes).await;
        Ok(())
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        output.write_all(&payload_bytes).await?;
        output.flush().await?;
        Ok(())
    }

    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes.slice(range.start..range.end))
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        let bytes = self.get_slice(path, range).await?;
        Ok(Box::new(Cursor::new(bytes)))
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.files.write().await.remove(path);
        Ok(())
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        let mut files = self.files.write().await;
        for &path in paths {
            files.remove(path);
        }
        Ok(())
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        let payload_bytes = self.get_data(path).await.ok_or_else(|| {
            StorageErrorKind::NotFound
                .with_error(anyhow::anyhow!("failed to find dest_path {:?}", path))
        })?;
        Ok(payload_bytes)
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        if let Some(file_bytes) = self.files.read().await.get(path) {
            Ok(file_bytes.len() as u64)
        } else {
            let err = anyhow::anyhow!("missing file `{}`", path.display());
            Err(StorageErrorKind::NotFound.with_error(err))
        }
    }
}

/// Builder to create a prepopulated [`RamStorage`]. This is mostly useful for tests.
#[derive(Debug, Default)]
pub struct RamStorageBuilder {
    files: HashMap<PathBuf, OwnedBytes>,
}

impl RamStorageBuilder {
    /// Adds a new file into the [`RamStorageBuilder`].
    pub fn put(mut self, path: &str, payload: &[u8]) -> Self {
        self.files
            .insert(PathBuf::from(path), OwnedBytes::new(payload.to_vec()));
        self
    }

    /// Finalizes the [`RamStorage`] creation.
    pub fn build(self) -> RamStorage {
        RamStorage {
            uri: Uri::for_test("ram:///"),
            files: Arc::new(RwLock::new(self.files)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_ram_storage_builder() -> anyhow::Result<()> {
        let storage = RamStorage::builder()
            .put("path1", b"path1_payload")
            .put("path2", b"path2_payload")
            .put("path1", b"path1_payloadb")
            .build();
        assert_eq!(
            &storage.get_all(Path::new("path1")).await?,
            &b"path1_payloadb"[..]
        );
        assert_eq!(
            &storage.get_all(Path::new("path2")).await?,
            &b"path2_payload"[..]
        );
        Ok(())
    }
}
