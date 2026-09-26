//! Range tails of pinned reads (plan M1.2 Task 4 rule 3, Ruling 1): a tail
//! over exactly `(applied, token]` of a retained manifest, built once per
//! `(collection, manifest version, token)` and cached.
//!
//! Overflow fallbacks build their range tails with [`build_range_tail`]
//! directly: their upper bounds differ per read, so they are not cached.
//!
//! [`build_range_tail`]: super::build_range_tail

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use operon_common::CollectionId;

use super::TailSnapshot;

/// A range tail stays cached this long after its last read.
const IDLE: Duration = Duration::from_secs(300);

/// The key of a pinned range tail: the collection, the manifest version and
/// the token's text form.
pub type RangeKey = (CollectionId, u64, String);

/// Pinned range tails by [`RangeKey`].
#[derive(Clone, Debug)]
pub struct RangeTailCache {
    cache: moka::future::Cache<RangeKey, Arc<TailSnapshot>>,
}

impl RangeTailCache {
    /// At most `entries` range tails.
    pub fn new(entries: u64) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(entries)
                .time_to_idle(IDLE)
                .build(),
        }
    }

    /// The cached range tail of `key`, or the one `build` makes (concurrent
    /// callers of one key share one build). A failed build is not cached.
    pub async fn get_or_build<E, F>(&self, key: RangeKey, build: F) -> Result<Arc<TailSnapshot>, E>
    where
        E: Clone + Send + Sync + 'static,
        F: Future<Output = Result<Arc<TailSnapshot>, E>>,
    {
        self.cache
            .try_get_with(key, build)
            .await
            .map_err(|err| (*err).clone())
    }
}
