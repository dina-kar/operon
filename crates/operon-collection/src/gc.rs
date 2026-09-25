//! Garbage-collection roots of collections (plan M1.1 Task 12, Rulings 2, 12
//! and 13; overview R19, A6, A15, A21): which objects under
//! `ns/<ns>/collections/` and `ns/<ns>/pk/` GC must keep.
//!
//! [`CollectionGcRoots`] keeps, for every collection, the manifests of its
//! [`retained_chain`] and everything they reference: splits, delete
//! bitmaps, PK deltas, dead letters and the files of every Lance version
//! they name. Lance's own cleanup is never run (it sees only mainline
//! manifests, Ruling 2), so Lance reachability is computed here from those
//! versions' manifests. Only Lance's known file classes are ever collected;
//! any other file under a dataset (the mainline version 1's manifest, and
//! any class a later Lance adds) is kept. A retained manifest's objects are
//! kept exactly as long as the manifest is, by the same retention rule and
//! the same metastore clock that [`CollectionSnapshot::open_version`] uses,
//! so a manifest that is retained is readable.
//!
//! [`PkGcRoots`] never touches a live collection's PK index (SlateDB
//! collects its own files); PK objects of an id the catalog does not know
//! go once they are `grace` old.
//!
//! A dropped collection's prefixes are retired (Ruling 13): both roots keep
//! them, and GC's pass 1 deletes them once the drop is `grace` old.
//!
//! [`CollectionSnapshot::open_version`]: crate::CollectionSnapshot::open_version

use std::collections::BTreeSet;

use async_trait::async_trait;
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::read_manifest_indexes;
use operon_common::{CollectionId, NamespaceId};
use operon_log::LogError;
use operon_log::gc::{GcRoots, object_time_ms};
use operon_meta::{
    Consistency, MetaClient, MetaState, Pointer, collection_pk_prefix, collection_pointer_key,
};
use operon_store::{ObjectInfo, Store};
use operon_worker::TaskError;
use uuid::Uuid;

use crate::chain::{load_pointed, retained_chain};
use crate::error::CollectionError;
use crate::paths::{lance_prefix, split_path};
use crate::snapshot::CollectionContext;

/// The GC root of `ns/<ns>/collections/`. Retention (`keep_manifests`,
/// `time_travel_retention`) comes from the context's `CollectionConfig`,
/// its one source; GC's own `keep_manifests` is not used.
#[derive(Clone, Debug)]
pub struct CollectionGcRoots {
    ctx: CollectionContext,
}

impl CollectionGcRoots {
    pub fn new(ctx: CollectionContext) -> Self {
        Self { ctx }
    }

    async fn reachable_objects(
        &self,
        meta: &MetaClient,
        store: &Store,
        ns: NamespaceId,
    ) -> Result<BTreeSet<String>, CollectionError> {
        let (collections, clock_ms): (Vec<(CollectionId, Option<Pointer>)>, u64) = meta
            .read(Consistency::Linearizable, |s| {
                let collections = s
                    .collections(ns)
                    .map(|c| (c.id, s.pointer(ns, &collection_pointer_key(c.id)).cloned()))
                    .collect();
                (collections, s.clock_ms())
            })
            .await?;
        let config = &self.ctx.config;
        let mut keep = BTreeSet::new();
        for (cid, pointer) in collections {
            let prefix = lance_prefix(ns, cid);
            let listed = store.list(&prefix).await?;
            let mut versions = BTreeSet::new();
            for info in &listed {
                let relative = &info.path[prefix.len()..];
                if !collectable_lance_file(relative) {
                    keep.insert(info.path.clone());
                }
                if is_mainline_manifest(relative) {
                    // Version 1, the only mainline version (Ruling 1): its
                    // manifest is kept above, its transaction file below.
                    versions.insert(1);
                }
            }
            if let Some(pointer) = pointer {
                let live = load_pointed(
                    store,
                    &self.ctx.manifests,
                    cid,
                    pointer.version,
                    &pointer.value,
                )
                .await?;
                let chain = retained_chain(
                    store,
                    &self.ctx.manifests,
                    (pointer.value, live),
                    config.keep_manifests,
                    config.time_travel_retention,
                    clock_ms,
                )
                .await?;
                for (path, manifest) in chain {
                    keep.insert(path);
                    for split in &manifest.splits {
                        keep.insert(split_path(ns, cid, split.ulid));
                        keep.extend(split.delete_bitmap.clone());
                    }
                    keep.extend(manifest.pk_delta.clone());
                    keep.extend(manifest.dead_letters.clone());
                    if manifest.lance_version != 0 {
                        versions.insert(manifest.lance_version);
                    }
                }
            }
            for version in versions {
                self.lance_files(ns, cid, version, &prefix, &listed, &mut keep)
                    .await?;
            }
        }
        Ok(keep)
    }

    /// Adds every file of Lance version `version` to `keep`: its manifest,
    /// its transaction file, every data and deletion file of its fragments,
    /// and every file (in `listed`, the listing of the dataset `prefix`) of
    /// every index it references, usable by this build or not.
    async fn lance_files(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        version: u64,
        prefix: &str,
        listed: &[ObjectInfo],
        keep: &mut BTreeSet<String>,
    ) -> Result<(), CollectionError> {
        let dataset = self.ctx.lance.open(ns, cid, version).await?;
        let location = dataset.manifest_location();
        keep.insert(location.path.to_string());
        let manifest = &dataset.manifest;
        if let Some(transaction) = &manifest.transaction_file {
            keep.insert(format!("{prefix}_transactions/{transaction}"));
        }
        for fragment in manifest.fragments.iter() {
            for file in fragment.referenced_lance_files() {
                keep.insert(format!("{prefix}data/{}", file.path));
            }
            if let Some(deletion) = &fragment.deletion_file {
                keep.insert(format!(
                    "{prefix}{}",
                    relative_deletion_file_path(fragment.id, deletion)
                ));
            }
        }
        // Not `load_indices`, which hides indexes this build cannot use.
        let object_store = dataset.object_store(None).await?;
        let indices = read_manifest_indexes(&object_store, location, manifest).await?;
        for index in indices {
            let dir = format!("{prefix}_indices/{}/", index.uuid);
            keep.extend(
                listed
                    .iter()
                    .filter(|info| info.path.starts_with(&dir))
                    .map(|info| info.path.clone()),
            );
        }
        Ok(())
    }
}

/// Whether `relative` (a path under a Lance dataset root) is in a file class
/// Operon's GC collects (Ruling 2): `data/*.lance`,
/// `_deletions/*.{arrow,bin}`, `_transactions/*.txn`, anything under
/// `_indices/<uuid>/`, and detached manifests `_versions/d<id>.manifest`.
/// Everything else is kept.
fn collectable_lance_file(relative: &str) -> bool {
    let mut parts = relative.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("data"), Some(name), None) => name.ends_with(".lance"),
        (Some("_deletions"), Some(name), None) => {
            name.ends_with(".arrow") || name.ends_with(".bin")
        }
        (Some("_transactions"), Some(name), None) => name.ends_with(".txn"),
        (Some("_indices"), Some(uuid), Some(_)) => Uuid::parse_str(uuid).is_ok(),
        (Some("_versions"), Some(name), None) => name
            .strip_prefix('d')
            .and_then(|rest| rest.strip_suffix(".manifest"))
            .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())),
        _ => false,
    }
}

/// Whether `relative` is a mainline (not detached) Lance manifest.
fn is_mainline_manifest(relative: &str) -> bool {
    relative
        .strip_prefix("_versions/")
        .and_then(|name| name.strip_suffix(".manifest"))
        .is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()))
}

/// The retired prefixes (paths ending in `/`) under `under`.
fn retired_prefixes(state: &MetaState, under: &str) -> Vec<String> {
    state
        .retired()
        .map(|(path, _)| path)
        .filter(|path| path.starts_with(under) && path.ends_with('/'))
        .map(str::to_string)
        .collect()
}

fn log_error(err: CollectionError) -> LogError {
    match err {
        CollectionError::Store(err) => LogError::from(err),
        CollectionError::Meta(err) => LogError::Meta(err),
        CollectionError::Corrupt(message) => LogError::Corrupt(message),
        other => LogError::Task(TaskError::failed(other)),
    }
}

#[async_trait]
impl GcRoots for CollectionGcRoots {
    fn prefix(&self) -> &str {
        "collections/"
    }

    /// Every collection's retained manifests and what they reference, and
    /// every Lance file outside the collectable classes, as of one
    /// `Linearizable` read (collections, pointers and the clock). Any read
    /// error fails the call, so GC skips the prefix this run.
    async fn reachable(
        &self,
        meta: &MetaClient,
        store: &Store,
        namespace: NamespaceId,
        _keep_manifests: usize,
    ) -> Result<BTreeSet<String>, LogError> {
        self.reachable_objects(meta, store, namespace)
            .await
            .map_err(log_error)
    }

    /// The retired prefixes of dropped collections: GC's pass 1 deletes them
    /// once their grace is over.
    async fn kept_prefixes(
        &self,
        meta: &MetaClient,
        _store: &Store,
        namespace: NamespaceId,
    ) -> Result<Vec<String>, LogError> {
        let under = format!("ns/{namespace}/collections/");
        Ok(meta
            .read(Consistency::Linearizable, |s| retired_prefixes(s, &under))
            .await?)
    }

    /// Lance names carry no ULID, so Lance files are aged by their
    /// modification time; every other object by the default.
    fn object_time_ms(&self, info: &ObjectInfo) -> u64 {
        if info.path.contains("/lance/") {
            info.last_modified_ms
        } else {
            object_time_ms(info)
        }
    }
}

/// The GC root of `ns/<ns>/pk/`. Nothing there is reachable object by
/// object: the PK index of every collection in the catalog is kept whole
/// (SlateDB collects its own files), as are the retired PK prefixes of
/// dropped collections (pass 1 deletes those); anything else, such as the
/// PK objects of an unknown id, goes once it is `grace` old.
#[derive(Clone, Copy, Debug, Default)]
pub struct PkGcRoots;

#[async_trait]
impl GcRoots for PkGcRoots {
    fn prefix(&self) -> &str {
        "pk/"
    }

    async fn reachable(
        &self,
        _meta: &MetaClient,
        _store: &Store,
        _namespace: NamespaceId,
        _keep_manifests: usize,
    ) -> Result<BTreeSet<String>, LogError> {
        Ok(BTreeSet::new())
    }

    async fn kept_prefixes(
        &self,
        meta: &MetaClient,
        _store: &Store,
        namespace: NamespaceId,
    ) -> Result<Vec<String>, LogError> {
        let under = format!("ns/{namespace}/pk/");
        Ok(meta
            .read(Consistency::Linearizable, |s| {
                let mut kept: Vec<String> = s
                    .collections(namespace)
                    .map(|c| collection_pk_prefix(namespace, c.id))
                    .collect();
                kept.extend(retired_prefixes(s, &under));
                kept
            })
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::{collectable_lance_file, is_mainline_manifest};

    #[test]
    fn only_known_lance_file_classes_are_collectable() {
        for path in [
            "data/0101.lance",
            "_deletions/3-7-12.arrow",
            "_deletions/3-7-12.bin",
            "_transactions/7-5b1c.txn",
            "_indices/67e55044-10b1-426f-9247-bb680e5fe0c8/index.idx",
            "_indices/67e55044-10b1-426f-9247-bb680e5fe0c8/part/aux.idx",
            "_versions/d9223372036854775809.manifest",
        ] {
            assert!(collectable_lance_file(path), "{path}");
        }
        for path in [
            "_versions/18446744073709551614.manifest",
            "_versions/.tmp-x.manifest",
            "_refs/x",
            "_latest.manifest",
            "data/0101/blob.blob",
            "data/0101.bin",
            "_indices/not-a-uuid/index.idx",
            "_indices/67e55044-10b1-426f-9247-bb680e5fe0c8",
            "_versions/d.manifest",
        ] {
            assert!(!collectable_lance_file(path), "{path}");
        }
        assert!(is_mainline_manifest(
            "_versions/18446744073709551614.manifest"
        ));
        assert!(!is_mainline_manifest(
            "_versions/d9223372036854775809.manifest"
        ));
    }
}
