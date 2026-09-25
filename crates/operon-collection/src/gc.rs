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
//! any class a later Lance adds) is kept. Retention follows the same rule
//! and the same metastore clock that [`CollectionSnapshot::open_version`]
//! uses, and a manifest's objects are deleted no earlier than the manifest:
//! a listed manifest that is not retained keeps its objects (not itself)
//! for as long as it is listed, so they go one run after it. A manifest that
//! exists is therefore always readable, even after a retention increase.
//! A collection whose objects cannot be read (a missing or corrupt
//! manifest, a Lance version GC cannot account for) is kept whole, and GC
//! goes on with the others.
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
use lance_table::format::{Fragment, RowDatasetVersionMeta, RowIdMeta};
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::io::manifest::read_manifest_indexes;
use operon_common::{CollectionId, NamespaceId};
use operon_log::LogError;
use operon_log::gc::{GcKeep, GcRoots, object_time_ms};
use operon_meta::{
    Consistency, MetaClient, MetaState, Pointer, collection_pk_prefix, collection_pointer_key,
    collection_prefix,
};
use operon_store::{ObjectInfo, Store, StoreError};
use operon_worker::TaskError;
use uuid::Uuid;

use crate::chain::{load_pointed, retained_chain};
use crate::error::CollectionError;
use crate::manifest::CollectionManifest;
use crate::paths::{lance_prefix, manifest_version, split_path};
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

    /// Every collection's objects to keep, and the prefixes to keep whole:
    /// the retired prefixes of dropped collections (GC's pass 1 deletes them
    /// once their grace is over), and the prefix of every collection whose
    /// objects cannot be read, so one bad collection does not stop GC of the
    /// others. Collections, pointers, the clock and the retired prefixes
    /// come from one `Linearizable` read. Nothing is carried between calls.
    async fn keep(
        &self,
        meta: &MetaClient,
        store: &Store,
        ns: NamespaceId,
    ) -> Result<GcKeep, CollectionError> {
        type Seen = (Vec<(CollectionId, Option<Pointer>)>, u64, Vec<String>);
        let under = format!("ns/{ns}/collections/");
        let (collections, clock_ms, retired): Seen = meta
            .read(Consistency::Linearizable, |s| {
                let collections = s
                    .collections(ns)
                    .map(|c| (c.id, s.pointer(ns, &collection_pointer_key(c.id)).cloned()))
                    .collect();
                (collections, s.clock_ms(), retired_prefixes(s, &under))
            })
            .await?;
        let mut keep = GcKeep {
            objects: BTreeSet::new(),
            prefixes: retired,
        };
        for (cid, pointer) in collections {
            let mut objects = BTreeSet::new();
            match self
                .collection_objects(store, ns, cid, pointer, clock_ms, &mut objects)
                .await
            {
                Ok(()) => keep.objects.extend(objects),
                Err(err) => {
                    let prefix = collection_prefix(ns, cid);
                    tracing::warn!(%prefix, %err, "gc keeps a collection whose objects it cannot read");
                    keep.prefixes.push(prefix);
                }
            }
        }
        Ok(keep)
    }

    /// Adds to `keep` what collection `cid` must keep:
    /// - every Lance file outside the collectable classes;
    /// - the manifests of its retained chain, everything they reference and
    ///   the files of every Lance version they name (and of version 1);
    /// - everything referenced by a listed manifest that is **not**
    ///   retained, but not that manifest itself. Such a manifest goes this
    ///   run and its objects the run after, so a manifest never outlives
    ///   what it references (a truncated page, a failed delete or a
    ///   cancelled run leaves the manifest, and then its objects too): a
    ///   later retention increase that walks back into it finds it whole.
    async fn collection_objects(
        &self,
        store: &Store,
        ns: NamespaceId,
        cid: CollectionId,
        pointer: Option<Pointer>,
        clock_ms: u64,
        keep: &mut BTreeSet<String>,
    ) -> Result<(), CollectionError> {
        let config = &self.ctx.config;
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
        let mut retained = BTreeSet::new();
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
                keep_referenced(ns, cid, &manifest, keep);
                if manifest.lance_version != 0 {
                    versions.insert(manifest.lance_version);
                }
                keep.insert(path.clone());
                retained.insert(path);
            }
        }
        // Listed manifests that are not retained: their objects only.
        let manifests_prefix = format!("{}manifests/", collection_prefix(ns, cid));
        let mut lingering = BTreeSet::new();
        for info in store.list(&manifests_prefix).await? {
            if retained.contains(&info.path) || manifest_version(&info.path).is_none() {
                continue;
            }
            let manifest = match self.ctx.manifests.load(store, &info.path).await {
                Ok(manifest) => manifest,
                // Deleted since the listing.
                Err(CollectionError::Store(StoreError::NotFound { .. })) => continue,
                Err(err) => return Err(err),
            };
            keep_referenced(ns, cid, &manifest, keep);
            if manifest.lance_version != 0 && !versions.contains(&manifest.lance_version) {
                lingering.insert(manifest.lance_version);
            }
        }
        for version in versions {
            self.lance_files(ns, cid, version, &prefix, &listed, keep)
                .await?;
        }
        for version in lingering {
            match self
                .lance_files(ns, cid, version, &prefix, &listed, keep)
                .await
            {
                // A Lance version whose manifest is already gone has no
                // files left to enumerate; no retained manifest names it.
                Ok(()) | Err(CollectionError::NotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Adds every file of Lance version `version` to `keep`: its manifest,
    /// its transaction file, every data and deletion file of its fragments,
    /// and every file (in `listed`, the listing of the dataset `prefix`) of
    /// every index it references, usable by this build or not. A fragment
    /// with row-id or row-version metadata in an external file is
    /// `Corrupt`: Operon never writes one, and GC cannot tell whether its
    /// file is kept, so it fails closed.
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
            if let Some(external) = external_metadata_file(fragment) {
                return Err(CollectionError::Corrupt(format!(
                    "fragment {} of lance version {version} of collection {cid} keeps metadata in the external file {external}",
                    fragment.id
                )));
            }
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

/// Adds the objects `manifest` references (not the manifest itself) to
/// `keep`: its splits, their delete bitmaps, its PK delta and dead letters.
fn keep_referenced(
    ns: NamespaceId,
    cid: CollectionId,
    manifest: &CollectionManifest,
    keep: &mut BTreeSet<String>,
) {
    for split in &manifest.splits {
        keep.insert(split_path(ns, cid, split.ulid));
        keep.extend(split.delete_bitmap.clone());
    }
    keep.extend(manifest.pk_delta.clone());
    keep.extend(manifest.dead_letters.clone());
}

/// The path of an external row-id or row-version metadata file of
/// `fragment`, if it has one.
fn external_metadata_file(fragment: &Fragment) -> Option<&str> {
    if let Some(RowIdMeta::External(file)) = &fragment.row_id_meta {
        return Some(&file.path);
    }
    [
        &fragment.last_updated_at_version_meta,
        &fragment.created_at_version_meta,
    ]
    .into_iter()
    .find_map(|meta| match meta {
        Some(RowDatasetVersionMeta::External(file)) => Some(file.path.as_str()),
        _ => None,
    })
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
    /// every Lance file outside the collectable classes; kept whole, the
    /// retired prefixes and every collection that cannot be read (see
    /// [`CollectionGcRoots`]). An error in the metastore read fails the
    /// call, so GC skips the prefix this run.
    async fn reachable(
        &self,
        meta: &MetaClient,
        store: &Store,
        namespace: NamespaceId,
        _keep_manifests: usize,
    ) -> Result<GcKeep, LogError> {
        self.keep(meta, store, namespace).await.map_err(log_error)
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
        meta: &MetaClient,
        _store: &Store,
        namespace: NamespaceId,
        _keep_manifests: usize,
    ) -> Result<GcKeep, LogError> {
        let under = format!("ns/{namespace}/pk/");
        let prefixes = meta
            .read(Consistency::Linearizable, |s| {
                let mut kept: Vec<String> = s
                    .collections(namespace)
                    .map(|c| collection_pk_prefix(namespace, c.id))
                    .collect();
                kept.extend(retired_prefixes(s, &under));
                kept
            })
            .await?;
        Ok(GcKeep {
            objects: BTreeSet::new(),
            prefixes,
        })
    }
}

#[cfg(test)]
mod tests {
    use lance_table::format::{ExternalFile, Fragment, RowDatasetVersionMeta, RowIdMeta};

    use super::{collectable_lance_file, external_metadata_file, is_mainline_manifest};

    fn external(path: &str) -> ExternalFile {
        ExternalFile {
            path: path.to_string(),
            offset: 0,
            size: 8,
        }
    }

    #[test]
    fn external_fragment_metadata_is_found() {
        let plain = Fragment::new(0);
        assert_eq!(external_metadata_file(&plain), None);
        let mut row_ids = Fragment::new(1);
        row_ids.row_id_meta = Some(RowIdMeta::External(external("_rowids/a.bin")));
        assert_eq!(external_metadata_file(&row_ids), Some("_rowids/a.bin"));
        let mut updated = Fragment::new(2);
        updated.last_updated_at_version_meta =
            Some(RowDatasetVersionMeta::External(external("v/b.bin")));
        assert_eq!(external_metadata_file(&updated), Some("v/b.bin"));
        let mut created = Fragment::new(3);
        created.created_at_version_meta =
            Some(RowDatasetVersionMeta::External(external("v/c.bin")));
        assert_eq!(external_metadata_file(&created), Some("v/c.bin"));
    }

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
