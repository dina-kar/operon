//! Garbage collection roots of link targets.

use std::collections::BTreeSet;

use async_trait::async_trait;
use operon_common::NamespaceId;
use operon_common::meta::{Consistency, MetaStore};
use operon_log::LogError;
use operon_log::gc::{GcKeep, GcRoots};
use operon_store::Store;

use crate::counter::{COUNTER_KIND, Manifest, decode_manifest, link_prefix, manifest_version};

/// Tells garbage collection which objects under `ns/<ns>/links/` are
/// reachable: for every `counter` link, its live manifest, its last
/// `keep_manifests` ancestors, and every data file the live manifest lists
/// (manifests list all data files so far, so the ancestors' data is among
/// them). Everything else there is an orphan: the data and manifests of
/// commits that crashed or were fenced, old manifests, and objects of links
/// the catalog does not know. Everything under a link of another target kind
/// is kept, since these roots cannot tell its objects apart (M0.4 review M2).
#[derive(Clone, Copy, Debug, Default)]
pub struct LinkGcRoots;

#[async_trait]
impl GcRoots for LinkGcRoots {
    fn prefix(&self) -> &str {
        "links/"
    }

    async fn reachable(
        &self,
        meta: &dyn MetaStore,
        store: &Store,
        namespace: NamespaceId,
        keep_manifests: usize,
    ) -> Result<GcKeep, LogError> {
        let links = meta
            .links_with_pointers(Consistency::Linearizable, namespace)
            .await?;
        let keep = u64::try_from(keep_manifests).unwrap_or(u64::MAX);
        let mut reachable = BTreeSet::new();
        for head in links {
            let link = head.link.id;
            if head.link.target.kind != COUNTER_KIND {
                let listed = store.list(&link_prefix(namespace, link)).await?;
                reachable.extend(listed.into_iter().map(|info| info.path));
                continue;
            }
            let Some(pointer) = head.pointer else {
                continue;
            };
            let (bytes, _) = store.get(&pointer.value).await?;
            let manifest: Manifest = decode_manifest(&pointer.value, &bytes)
                .map_err(|e| LogError::Corrupt(e.to_string()))?;
            reachable.extend(manifest.data);
            // The live manifest's last `keep` ancestors (and any orphan at
            // those versions, which is harmless to keep a little longer).
            let oldest = pointer.version.saturating_sub(keep).max(1);
            let manifests = store
                .list(&format!("{}manifests/", link_prefix(namespace, link)))
                .await?;
            reachable.extend(
                manifests
                    .into_iter()
                    .filter(|info| {
                        manifest_version(&info.path)
                            .is_some_and(|v| (oldest..=pointer.version).contains(&v))
                    })
                    .map(|info| info.path),
            );
            reachable.insert(pointer.value);
        }
        Ok(reachable.into())
    }
}
