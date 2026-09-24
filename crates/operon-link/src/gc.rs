//! Garbage collection roots of link targets.

use std::collections::BTreeSet;

use async_trait::async_trait;
use operon_common::NamespaceId;
use operon_log::LogError;
use operon_log::gc::GcRoots;
use operon_meta::{Consistency, MetaClient};
use operon_store::Store;

use crate::counter::{COUNTER_KIND, Manifest, decode_manifest, manifest_path, pointer_key};

/// Tells garbage collection which objects under `ns/<ns>/links/` are
/// reachable: for every `counter` link, its live manifest, its last
/// `keep_manifests` ancestors, and every data file the live manifest lists
/// (manifests list all data files so far, so the ancestors' data is among
/// them). Everything else there is an orphan: the data and manifests of
/// commits that crashed or were fenced, old manifests, and objects of links
/// the catalog does not know.
#[derive(Clone, Copy, Debug, Default)]
pub struct LinkGcRoots;

#[async_trait]
impl GcRoots for LinkGcRoots {
    fn prefix(&self) -> &str {
        "links/"
    }

    async fn reachable(
        &self,
        meta: &MetaClient,
        store: &Store,
        namespace: NamespaceId,
        keep_manifests: usize,
    ) -> Result<BTreeSet<String>, LogError> {
        let links: Vec<(operon_meta::LinkId, Option<operon_meta::Pointer>)> = meta
            .read(Consistency::Linearizable, |s| {
                s.links(namespace)
                    .filter(|l| l.target.kind == COUNTER_KIND)
                    .map(|l| (l.id, s.pointer(namespace, &pointer_key(l.id)).cloned()))
                    .collect()
            })
            .await?;
        let keep = u64::try_from(keep_manifests).unwrap_or(u64::MAX);
        let mut reachable = BTreeSet::new();
        for (link, pointer) in links {
            let Some(pointer) = pointer else { continue };
            let (bytes, _) = store.get(&pointer.value).await?;
            let manifest: Manifest = decode_manifest(&pointer.value, &bytes)
                .map_err(|e| LogError::Corrupt(e.to_string()))?;
            reachable.extend(manifest.data);
            for version in pointer.version.saturating_sub(keep).max(1)..=pointer.version {
                reachable.insert(manifest_path(namespace, link, version));
            }
            reachable.insert(pointer.value);
        }
        Ok(reachable)
    }
}
