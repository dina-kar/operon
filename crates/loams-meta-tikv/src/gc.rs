//! Garbage-collection reads and bookkeeping writes (design §20 §11.3). The
//! reads are one snapshot at a fresh TSO timestamp, and "the metastore clock
//! of the same state" is that timestamp's physical time. The retired set is
//! spread over 64 shards (`r/<shard>/<path>`), so its reads scan all of `r/`.
//!
//! `o/` rows: nothing in the R1 trait sets a GC claim (the claim protocol
//! arrives with R2's garbage collector), so the writes that make an object
//! reachable only check and lock the row, and `forget_objects` deletes it
//! with the retired entry, clearing any claim (row T5-5).

use std::collections::BTreeSet;

use loams_common::meta::{
    CollectionRoots, Fence, IndexEntry, MetaResult, Pointer, WAL_COMMIT_WINDOW_MS,
    collection_pointer_key,
};
use loams_common::{NamespaceId, StreamId};
use loams_tikv::{Snap, Tikv, TxnError};

use crate::catalog::{list_collections, list_streams};
use crate::keys::{self, WalGroup};
use crate::leases::check_fence;
use crate::{Reader, TikvMeta, fatal, load};

/// Objects `forget_objects` handles per transaction, and commit records
/// `prune_wal_commits` scans per transaction.
const BATCH: usize = 256;

/// Every retired path with its retirement time, in path order.
async fn retired_set(snap: &mut Snap) -> Result<Vec<(String, u64)>, TxnError> {
    let mut out = Vec::new();
    for (key, value) in snap.scan_prefix(keys::RETIRED).await? {
        if let Some(path) = keys::retired_path(&key) {
            out.push((path, keys::decode_u64("retirement", &value).map_err(fatal)?));
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Which of `paths` are retired, by one batch read.
async fn retired_among(r: &mut dyn Reader, paths: &[&str]) -> Result<BTreeSet<Vec<u8>>, TxnError> {
    let found = r
        .batch_get(paths.iter().map(|p| keys::retired(p)).collect())
        .await?;
    Ok(found.into_iter().map(|(k, _)| k).collect())
}

impl TikvMeta {
    pub(crate) async fn retired_expired_impl(&self, grace_ms: u64) -> MetaResult<Vec<String>> {
        self.read(move |snap| {
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                Ok(Ok(retired_set(snap)
                    .await?
                    .into_iter()
                    .filter(|(_, at)| at.saturating_add(grace_ms) <= clock_ms)
                    .map(|(path, _)| path)
                    .collect()))
            })
        })
        .await
    }

    /// One transaction per [`BATCH`] objects, each checking the fence; an
    /// empty list still checks it.
    pub(crate) async fn forget_objects_impl(
        &self,
        objects: Vec<String>,
        fence: Option<Fence>,
    ) -> MetaResult<u32> {
        let mut removed = 0u32;
        let batches: Vec<Vec<String>> = if objects.is_empty() {
            vec![Vec::new()]
        } else {
            objects.chunks(BATCH).map(<[String]>::to_vec).collect()
        };
        for batch in batches {
            let fence = fence.clone();
            removed = removed.saturating_add(
                self.write("meta.forget_objects", move |txn| {
                    let (batch, fence) = (batch.clone(), fence.clone());
                    Box::pin(async move {
                        if let Some(fence) = &fence
                            && let Err(e) = check_fence(txn, fence).await?
                        {
                            return Ok(Err(e));
                        }
                        let present = retired_among(
                            txn,
                            &batch.iter().map(String::as_str).collect::<Vec<_>>(),
                        )
                        .await?;
                        let mut removed = 0u32;
                        for object in &batch {
                            let key = keys::retired(object);
                            if present.contains(&key) {
                                txn.delete(&key).await?;
                                txn.delete(&keys::object_ref(object)).await?;
                                removed = removed.saturating_add(1);
                            }
                        }
                        Ok(Ok(removed))
                    })
                })
                .await
                .0?,
            );
        }
        Ok(removed)
    }

    /// One transaction per page of [`BATCH`] commit records, each checking
    /// the fence and stamped with its start timestamp.
    pub(crate) async fn prune_wal_commits_impl(&self, fence: Option<Fence>) -> MetaResult<u32> {
        self.reach_now().await;
        let (lo, hi) = keys::prefix_range(keys::WAL_RECORDS);
        let mut start = lo;
        let mut removed = 0u32;
        loop {
            let (fence, from, hi) = (fence.clone(), start.clone(), hi.clone());
            let (count, next) = self
                .write("meta.prune_wal_commits", move |txn| {
                    let (fence, from, hi) = (fence.clone(), from.clone(), hi.clone());
                    Box::pin(async move {
                        if let Some(fence) = &fence
                            && let Err(e) = check_fence(txn, fence).await?
                        {
                            return Ok(Err(e));
                        }
                        let clock_ms = Tikv::physical_ms(&txn.start_ts());
                        let page = txn.scan(&from, hi.as_deref(), BATCH).await?;
                        let next = (page.len() == BATCH)
                            .then(|| page.last().map(|(k, _)| after(k)))
                            .flatten();
                        let mut count = 0u32;
                        for (key, value) in page {
                            let record: WalGroup =
                                keys::decode("WAL commit record", &value).map_err(fatal)?;
                            let expires = record
                                .created_at_ms
                                .saturating_add(2 * WAL_COMMIT_WINDOW_MS);
                            if expires < clock_ms {
                                txn.delete(&key).await?;
                                count = count.saturating_add(1);
                            }
                        }
                        Ok(Ok((count, next)))
                    })
                })
                .await
                .0?;
            removed = removed.saturating_add(count);
            match next {
                Some(next) => start = next,
                None => return Ok(removed),
            }
        }
    }

    pub(crate) async fn orphan_wal_objects_impl(
        &self,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.read(move |snap| {
            let candidates = candidates.clone();
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                let old: Vec<&str> = candidates
                    .iter()
                    .filter(|(_, created)| created.saturating_add(min_age_ms) <= clock_ms)
                    .map(|(path, _)| path.as_str())
                    .collect();
                let retired = retired_among(snap, &old).await?;
                let live: BTreeSet<Vec<u8>> = snap
                    .batch_get(old.iter().map(|p| keys::wal_live(p)).collect::<Vec<_>>())
                    .await?
                    .into_iter()
                    .map(|(k, _)| k)
                    .collect();
                Ok(Ok(old
                    .into_iter()
                    .filter(|p| {
                        !live.contains(&keys::wal_live(p)) && !retired.contains(&keys::retired(p))
                    })
                    .take(limit)
                    .map(str::to_string)
                    .collect()))
            })
        })
        .await
    }

    /// Scans the index entries of every stream of the namespace (row T5-8).
    pub(crate) async fn orphan_segments_impl(
        &self,
        ns: NamespaceId,
        candidates: Vec<(String, u64)>,
        min_age_ms: u64,
        limit: usize,
    ) -> MetaResult<Vec<String>> {
        self.read(move |snap| {
            let candidates = candidates.clone();
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                let old: Vec<&str> = candidates
                    .iter()
                    .filter(|(_, created)| created.saturating_add(min_age_ms) <= clock_ms)
                    .map(|(path, _)| path.as_str())
                    .collect();
                if old.is_empty() {
                    return Ok(Ok(Vec::new()));
                }
                let retired = retired_among(snap, &old).await?;
                let mut kept: BTreeSet<String> = BTreeSet::new();
                for stream in list_streams(snap, Some(ns)).await? {
                    for (_, value) in snap.scan_prefix(&keys::index_entries(stream.id)).await? {
                        let entry: IndexEntry =
                            keys::decode("index entry", &value).map_err(fatal)?;
                        kept.insert(entry.object);
                    }
                }
                Ok(Ok(old
                    .into_iter()
                    .filter(|p| !kept.contains(*p) && !retired.contains(&keys::retired(p)))
                    .take(limit)
                    .map(str::to_string)
                    .collect()))
            })
        })
        .await
    }

    pub(crate) async fn segment_referenced_impl(
        &self,
        stream: StreamId,
        partition: u32,
        object: &str,
    ) -> MetaResult<bool> {
        let object = object.to_string();
        self.read(move |snap| {
            let object = object.clone();
            Box::pin(async move {
                if snap.get(&keys::retired(&object)).await?.is_some() {
                    return Ok(Ok(true));
                }
                for (_, value) in snap
                    .scan_prefix(&keys::partition_entries(stream, partition))
                    .await?
                {
                    let entry: IndexEntry = keys::decode("index entry", &value).map_err(fatal)?;
                    if entry.object == object {
                        return Ok(Ok(true));
                    }
                }
                Ok(Ok(false))
            })
        })
        .await
    }

    pub(crate) async fn collection_roots_impl(
        &self,
        ns: NamespaceId,
        under: &str,
    ) -> MetaResult<CollectionRoots> {
        let under = under.to_string();
        self.read(move |snap| {
            let under = under.clone();
            Box::pin(async move {
                let clock_ms = Tikv::physical_ms(snap.ts());
                let mut collections = Vec::new();
                for c in list_collections(snap, Some(ns)).await? {
                    let pointer: Option<Pointer> = load(
                        snap,
                        "pointer",
                        &keys::pointer(ns, &collection_pointer_key(c.id)),
                    )
                    .await?;
                    collections.push((c, pointer));
                }
                let retired_prefixes = retired_set(snap)
                    .await?
                    .into_iter()
                    .map(|(path, _)| path)
                    .filter(|path| path.starts_with(under.as_str()) && path.ends_with('/'))
                    .collect();
                Ok(Ok(CollectionRoots {
                    clock_ms,
                    collections,
                    retired_prefixes,
                }))
            })
        })
        .await
    }
}

/// The first key after `key`.
fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
