//! Background maintenance (plan M1.3 Tasks 1 and 2; design §03 §3.2, §09
//! §5): split merges ([`SplitMergeSource`]) and Lance compaction, worker
//! task sources at [`Priority::Compaction`](operon_worker::Priority), both
//! committed under the collection manifest by the same fenced,
//! freshness-checked pointer CAS as link apply, with rebase on `Conflict`.
//!
//! Every source that scans collections proposes them the same way
//! ([`CollectionPoller`], Task 1 rule 1): one `Local` read of every
//! collection with its pointer version; a collection is due when its
//! pointer moved since its last idle run, or that run is older than the
//! poll interval.

mod config;
pub(crate) mod merge;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use operon_common::meta::{Consistency, MetaError, MetaStore};
use operon_common::{CollectionId, NamespaceId};

pub use config::MaintenanceConfig;
pub use merge::{
    MERGE_TASK_PREFIX, MergePlan, SplitMergeSource, plan_merges, row_id_runs, schema_for_split,
};

/// A collection a poll found due: its namespace, id and pointer version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DueCollection {
    pub ns: NamespaceId,
    pub cid: CollectionId,
    pub version: u64,
}

/// The candidate rule every collection-scanning maintenance source shares
/// (Task 1 rule 1; Task 0 E60): M2's dirty sets replace this one type.
#[derive(Default)]
pub struct CollectionPoller {
    /// Per collection, the pointer version its last idle run saw and when.
    last_checked: Mutex<BTreeMap<CollectionId, (u64, Instant)>>,
}

impl fmt::Debug for CollectionPoller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let checked = self
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        f.debug_struct("CollectionPoller")
            .field("checked", &checked)
            .finish()
    }
}

impl CollectionPoller {
    pub fn new() -> Self {
        Self::default()
    }

    /// The collections with a pointer (one `Local` read of every collection)
    /// whose pointer version differs from the one their last idle run saw,
    /// or whose last idle run is older than `interval`. Forgets collections
    /// that no longer exist.
    pub async fn due(
        &self,
        meta: &dyn MetaStore,
        interval: Duration,
    ) -> Result<Vec<DueCollection>, MetaError> {
        let collections: Vec<DueCollection> = meta
            .collection_heads(Consistency::Local, None)
            .await?
            .into_iter()
            .filter_map(|head| {
                Some(DueCollection {
                    ns: head.collection.namespace,
                    cid: head.collection.id,
                    version: head.pointer?.version,
                })
            })
            .collect();
        let mut checked = self
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let live: BTreeSet<CollectionId> = collections.iter().map(|c| c.cid).collect();
        checked.retain(|cid, _| live.contains(cid));
        Ok(collections
            .into_iter()
            .filter(|c| {
                checked
                    .get(&c.cid)
                    .is_none_or(|(seen, at)| *seen != c.version || at.elapsed() >= interval)
            })
            .collect())
    }

    /// Records how a run of `cid` ended: an idle run at pointer `version`
    /// is not due again until the pointer moves or the interval passes; any
    /// other run (`None`) is due at the next poll.
    pub fn checked(&self, cid: CollectionId, idle_at: Option<u64>) {
        let mut checked = self
            .last_checked
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match idle_at {
            Some(version) => {
                checked.insert(cid, (version, Instant::now()));
            }
            None => {
                checked.remove(&cid);
            }
        }
    }
}
