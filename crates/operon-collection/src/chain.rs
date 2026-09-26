//! The manifest chain (plan M1.1 Task 9, Ruling 12; overview §6.4, A6, A21):
//! the live manifest, which the pointer `collection/<cid>` names, and its
//! ancestors through `parent_manifest`.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use operon_common::meta::{Consistency, MetaStore, collection_pointer_key};
use operon_common::{CollectionId, NamespaceId};
use operon_store::{Store, StoreError};

use crate::error::CollectionError;
use crate::manifest::{CollectionManifest, decode_manifest};

/// Decoded manifests by path. Manifests are immutable, so an entry never
/// goes stale. Cheap to clone; clones share the cache.
#[derive(Clone)]
pub struct ManifestCache {
    cache: moka::sync::Cache<String, Arc<CollectionManifest>>,
}

impl fmt::Debug for ManifestCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManifestCache")
            .field("entries", &self.cache.entry_count())
            .finish_non_exhaustive()
    }
}

impl ManifestCache {
    /// A cache of at most `entries` manifests.
    pub fn new(entries: usize) -> Self {
        Self {
            cache: moka::sync::Cache::new(u64::try_from(entries).unwrap_or(u64::MAX)),
        }
    }

    /// The manifest at `path`, read from `store` on a miss. A missing object
    /// is `CollectionError::Store(StoreError::NotFound)`.
    pub async fn load(
        &self,
        store: &Store,
        path: &str,
    ) -> Result<Arc<CollectionManifest>, CollectionError> {
        if let Some(manifest) = self.cache.get(path) {
            return Ok(manifest);
        }
        let (bytes, _) = store.get(path).await?;
        let manifest = Arc::new(decode_manifest(&bytes).map_err(|err| match err {
            CollectionError::Corrupt(message) => {
                CollectionError::Corrupt(format!("{path}: {message}"))
            }
            other => other,
        })?);
        self.cache.insert(path.to_string(), manifest.clone());
        Ok(manifest)
    }
}

/// Loads the manifest the pointer of collection `cid` names, which must be
/// that collection's manifest at the pointer's version (rule 2).
pub(crate) async fn load_pointed(
    store: &Store,
    cache: &ManifestCache,
    cid: CollectionId,
    pointer_version: u64,
    path: &str,
) -> Result<Arc<CollectionManifest>, CollectionError> {
    let manifest = cache.load(store, path).await?;
    if manifest.version != pointer_version || manifest.collection_id != cid {
        return Err(CollectionError::Corrupt(format!(
            "{path} is version {} of collection {}, but the pointer of collection {cid} is at version {pointer_version}",
            manifest.version, manifest.collection_id
        )));
    }
    Ok(manifest)
}

/// The pointer `collection/<cid>` and its manifest; None before the first commit.
pub async fn live_manifest(
    meta: &dyn MetaStore,
    store: &Store,
    cache: &ManifestCache,
    ns: NamespaceId,
    cid: CollectionId,
    consistency: Consistency,
) -> Result<Option<(String, Arc<CollectionManifest>)>, CollectionError> {
    let key = collection_pointer_key(cid);
    let pointer = meta.pointer(consistency, ns, &key).await?;
    let Some(pointer) = pointer else {
        return Ok(None);
    };
    let manifest = load_pointed(store, cache, cid, pointer.version, &pointer.value).await?;
    Ok(Some((pointer.value, manifest)))
}

/// Ruling 12's retained set: `live` and its ancestors, newest first (rule 4).
///
/// The ancestor at distance *d* is kept iff `d <= keep_manifests` or the
/// manifest that superseded it (at distance *d* − 1) was created less than
/// `retention` before `clock_ms` (overview A21). The walk stops at the first
/// ancestor that is not kept, at the root, or at a parent that no longer
/// exists, so the result is a prefix of the chain that only shrinks as the
/// clock advances or commits arrive.
pub async fn retained_chain(
    store: &Store,
    cache: &ManifestCache,
    live: (String, Arc<CollectionManifest>),
    keep_manifests: usize,
    retention: Duration,
    clock_ms: u64,
) -> Result<Vec<(String, Arc<CollectionManifest>)>, CollectionError> {
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let mut chain = vec![live];
    loop {
        let (child_path, child) = &chain[chain.len() - 1];
        let Some(parent_path) = child.parent_manifest.clone() else {
            break;
        };
        let distance = chain.len();
        let young = child.created_at_ms.saturating_add(retention_ms) > clock_ms;
        if distance > keep_manifests && !young {
            break;
        }
        let parent = match cache.load(store, &parent_path).await {
            Ok(parent) => parent,
            Err(CollectionError::Store(StoreError::NotFound { .. })) => break,
            Err(err) => return Err(err),
        };
        if parent.version != child.parent_version || parent.collection_id != child.collection_id {
            return Err(CollectionError::Corrupt(format!(
                "{child_path} names parent {parent_path}, which is version {} of collection {}, not version {} of collection {}",
                parent.version, parent.collection_id, child.parent_version, child.collection_id
            )));
        }
        chain.push((parent_path, parent));
    }
    Ok(chain)
}
