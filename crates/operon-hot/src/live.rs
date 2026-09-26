//! The live row ids of a manifest (plan M1.3 Task 6 rule 1; Ruling 2),
//! computed from the manifest alone: every split's `row_id_ranges` minus its
//! delete bitmap. Every live Lance row is exactly one live split doc (M1.1
//! Task 10 rule 8.5), so no Lance scan is needed per version.

use std::sync::Arc;

use operon_collection::{CollectionError, CollectionSnapshot, SplitRef};
use roaring::{RoaringBitmap, RoaringTreemap};

use crate::TierError;

/// The bytes of deleted-doc sets a [`DeletedDocsCache`] keeps.
const DELETED_CACHE_BYTES: u64 = 256 << 20;

/// Deleted-doc sets by delete-bitmap path (E2). Bitmaps are immutable, so a
/// set never changes; `live_rows` for each new manifest reads only the
/// bitmaps it has not seen. Bounded by the sets' serialized size.
#[derive(Clone)]
pub struct DeletedDocsCache {
    cache: moka::sync::Cache<String, Arc<RoaringBitmap>>,
}

impl std::fmt::Debug for DeletedDocsCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeletedDocsCache")
            .field("entries", &self.cache.entry_count())
            .finish()
    }
}

impl Default for DeletedDocsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DeletedDocsCache {
    pub fn new() -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .weigher(|path: &String, set: &Arc<RoaringBitmap>| {
                    let bytes = path.len() + set.serialized_size();
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .max_capacity(DELETED_CACHE_BYTES)
                .build(),
        }
    }

    /// The deleted docs of `split` at `snapshot`, cached by bitmap path.
    async fn deleted(
        &self,
        snapshot: &CollectionSnapshot,
        split: &SplitRef,
    ) -> Result<Arc<RoaringBitmap>, CollectionError> {
        let Some(path) = &split.delete_bitmap else {
            return Ok(Arc::new(RoaringBitmap::new()));
        };
        if let Some(hit) = self.cache.get(path) {
            return Ok(hit);
        }
        let set = Arc::new(snapshot.deleted_docs(split).await?);
        self.cache.insert(path.clone(), set.clone());
        Ok(set)
    }
}

/// The live row ids at `snapshot`'s manifest (Ruling 2): every split's ranges minus its deleted docs.
pub async fn live_rows(snapshot: &CollectionSnapshot) -> Result<RoaringTreemap, TierError> {
    live_rows_cached(snapshot, &DeletedDocsCache::new()).await
}

/// [`live_rows`], reading deleted-doc sets through `cache`.
pub async fn live_rows_cached(
    snapshot: &CollectionSnapshot,
    cache: &DeletedDocsCache,
) -> Result<RoaringTreemap, TierError> {
    let mut live = RoaringTreemap::new();
    for split in snapshot.splits() {
        for range in &split.row_id_ranges {
            live.insert_range(range.clone());
        }
        let deleted = cache.deleted(snapshot, split).await?;
        remove_deleted(&mut live, split, &deleted)?;
    }
    Ok(live)
}

/// Removes from `live` the row id of every doc of `deleted`: doc *d* is row
/// `range.start + (d − doc_base)` of the range holding it, with doc bases
/// running over the ranges in list (doc-id) order.
fn remove_deleted(
    live: &mut RoaringTreemap,
    split: &SplitRef,
    deleted: &RoaringBitmap,
) -> Result<(), TierError> {
    let mut ranges = split.row_id_ranges.iter();
    let (mut range, mut doc_base) = (ranges.next(), 0u64);
    for doc in deleted {
        let doc = u64::from(doc);
        loop {
            let Some(current) = range else {
                return Err(TierError::Collection(CollectionError::Corrupt(format!(
                    "split {}: deleted doc {doc} is past its {} docs",
                    split.ulid, split.doc_count
                ))));
            };
            let len = current.end.saturating_sub(current.start);
            if doc < doc_base + len {
                live.remove(current.start + (doc - doc_base));
                break;
            }
            doc_base += len;
            range = ranges.next();
        }
    }
    Ok(())
}
