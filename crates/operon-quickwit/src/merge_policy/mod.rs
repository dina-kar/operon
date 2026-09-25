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
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-indexing/src/merge_policy/mod.rs); modified for Operon: MergeTask, MergeSource, TrackedObject, MergePermit, the other policies and the settings constructors removed; SplitMetadata and SplitId from crate::shim; actor-based test helpers removed.

pub mod config;
mod stable_log_merge_policy;

use std::fmt;

pub use config::StableLogMergePolicyConfig;
use itertools::Itertools;
use serde::Serialize;
pub use stable_log_merge_policy::StableLogMergePolicy;
use tracing::{Span, info_span};

use crate::shim::{SplitId, SplitMaturity, SplitMetadata};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum MergeOperationType {
    Merge,
    DeleteAndMerge,
}

impl fmt::Display for MergeOperationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Serialize)]
pub struct MergeOperation {
    #[serde(skip_serializing)]
    pub merge_parent_span: Span,
    pub merge_split_id: SplitId,
    pub splits: Vec<SplitMetadata>,
    pub operation_type: MergeOperationType,
}

impl MergeOperation {
    pub fn new_merge_operation(splits: Vec<SplitMetadata>) -> Self {
        let merge_split_id = SplitId::new();
        let split_ids = splits.iter().map(|split| split.split_id()).collect_vec();
        let merge_parent_span = info_span!("merge", merge_split_id=%merge_split_id, split_ids=?split_ids, typ=%MergeOperationType::Merge);
        Self {
            merge_parent_span,
            merge_split_id,
            splits,
            operation_type: MergeOperationType::Merge,
        }
    }

    pub fn total_num_bytes(&self) -> u64 {
        self.splits
            .iter()
            .map(|split: &SplitMetadata| split.footer_offsets.end)
            .sum()
    }

    pub fn new_delete_and_merge_operation(split: SplitMetadata) -> Self {
        let merge_split_id = SplitId::new();
        let merge_parent_span = info_span!("delete", merge_split_id=%merge_split_id, split_ids=?split.split_id(), typ=%MergeOperationType::DeleteAndMerge);
        Self {
            merge_parent_span,
            merge_split_id,
            splits: vec![split],
            operation_type: MergeOperationType::DeleteAndMerge,
        }
    }

    pub fn splits_as_slice(&self) -> &[SplitMetadata] {
        self.splits.as_slice()
    }

    pub fn merge_level(&self) -> usize {
        self.splits
            .iter()
            .map(|s| s.num_merge_ops)
            .max()
            .unwrap_or(0)
    }
}

// The higher, the sooner we will execute the merge operation.
// A good merge operation:
// - strongly reduces the number of splits
// - is light.
pub fn compute_merge_score(num_splits: usize, total_num_bytes: u64) -> u64 {
    if total_num_bytes == 0 {
        // Silly corner case that should never happen.
        return u64::MAX;
    }
    // We will remove num_splits and add 1 merge split.
    let delta_num_splits = num_splits.saturating_sub(1) as u64;
    // Integer arithmetic to avoid `f64 are not ordered` silliness.
    (delta_num_splits << 48)
        .checked_div(total_num_bytes)
        .unwrap_or(1u64)
}

impl fmt::Debug for MergeOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Merge(operation_type={}, merged_split_id={},splits=[",
            self.operation_type, self.merge_split_id
        )?;
        for split in &self.splits {
            write!(f, "{},", split.split_id())?;
        }
        write!(f, "])")?;
        Ok(())
    }
}

/// A merge policy wraps the logic that decides what should be merged.
/// The SplitMetadata must be extracted from the splits `Vec`.
///
/// It is called by the merge planner whenever a new split is added.
pub trait MergePolicy: Send + Sync + fmt::Debug {
    /// Returns the list of merge operations that should be performed.
    fn operations(&self, splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation>;

    /// After the last indexing pipeline has been shutdown, quickwit
    /// finishes the ongoing merge operations, and eventually needs to shut it down.
    ///
    /// This method makes it possible to offer a last list of merge operations before
    /// really shutting down the merge policy.
    ///
    /// This is especially useful for users relying on a one-index-per-day scheme.
    fn finalize_operations(&self, _splits: &mut Vec<SplitMetadata>) -> Vec<MergeOperation> {
        Vec::new()
    }

    /// Returns split maturity.
    /// A split is either:
    /// - `Mature` if it does not undergo new merge operations.
    /// - or `Immature` with a `maturation_period` after which it becomes mature.
    fn split_maturity(&self, split_num_docs: usize, split_num_merge_ops: usize) -> SplitMaturity;

    /// Checks a bunch of properties specific to the given merge policy.
    /// This method is used in proptesting.
    ///
    /// - `merge_op` is a merge operation emitted by this merge policy.
    /// - `remaining_splits` is the list of remaining splits.
    #[cfg(test)]
    fn check_is_valid(&self, _merge_op: &MergeOperation, _remaining_splits: &[SplitMetadata]) {}
}

struct SplitShortDebug<'a>(&'a SplitMetadata);

impl fmt::Debug for SplitShortDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Split")
            .field("split_id", &self.0.split_id())
            .field("num_docs", &self.0.num_docs)
            .finish()
    }
}

fn splits_short_debug(splits: &[SplitMetadata]) -> Vec<SplitShortDebug<'_>> {
    splits.iter().map(SplitShortDebug).collect()
}

#[cfg(test)]
pub mod tests {

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as _, Hasher};
    use std::ops::RangeInclusive;

    use proptest::prelude::*;
    use rand::seq::SliceRandom;
    use time::OffsetDateTime;

    use super::*;

    #[test]
    fn test_score() {
        // Lighter merge at the same split count scores higher.
        assert!(compute_merge_score(10, 100_000_000) < compute_merge_score(10, 9_999_990));
        // More splits removed at the same total bytes scores higher.
        assert!(compute_merge_score(10, 100_000_000) > compute_merge_score(9, 100_000_000));
        // Equal `(delta_splits / total_bytes)` ratios yield equal scores.
        assert_eq!(
            // delta=8, 90M bytes.
            compute_merge_score(9, 90_000_000),
            // delta=4, 45M bytes (same 8/90M ratio).
            compute_merge_score(5, 45_000_000),
        );
    }

    fn pow_of_10(n: usize) -> usize {
        10usize.pow(n as u32)
    }

    prop_compose! {
        fn num_docs_around_power_of_ten()(
            pow_ten in 1usize..5usize,
            diff in -2isize..2isize
        ) -> usize {
            (pow_of_10(pow_ten) as isize + diff).max(1isize) as usize
        }
    }

    fn num_docs_strategy() -> impl Strategy<Value = usize> {
        prop_oneof![1usize..10_000_000usize, num_docs_around_power_of_ten()]
    }

    prop_compose! {
      fn split_strategy()
        (num_merge_ops in 0usize..5usize, start_timestamp in 1_664_000_000i64..1_665_000_000i64, average_time_delta in 100i64..120i64, delta_creation_date in 0u64..100_000u64, num_docs in num_docs_strategy()) -> SplitMetadata {
        let end_timestamp = start_timestamp + average_time_delta * pow_of_10(num_merge_ops) as i64;
        let create_timestamp: i64 = (end_timestamp as u64 + delta_creation_date) as i64;
        SplitMetadata {
            split_id: SplitId::new(),
            time_range: Some(start_timestamp..=end_timestamp),
            num_docs,
            create_timestamp,
            num_merge_ops,
            .. Default::default()
        }
      }
    }

    pub(crate) fn create_splits(
        merge_policy: &dyn MergePolicy,
        num_docs_vec: Vec<usize>,
    ) -> Vec<SplitMetadata> {
        let num_docs_with_timestamp = num_docs_vec
            .into_iter()
            // we give the same timestamp to all of them and rely on stable sort to keep the split
            // order.
            .map(|num_docs| (num_docs, (1630563067..=1630564067)))
            .collect();
        create_splits_with_timestamps(merge_policy, num_docs_with_timestamp)
    }

    fn create_splits_with_timestamps(
        merge_policy: &dyn MergePolicy,
        num_docs_vec: Vec<(usize, RangeInclusive<i64>)>,
    ) -> Vec<SplitMetadata> {
        num_docs_vec
            .into_iter()
            .enumerate()
            .map(|(split_ord, (num_docs, time_range))| {
                let create_timestamp = OffsetDateTime::now_utc().unix_timestamp();
                let time_to_maturity = merge_policy.split_maturity(num_docs, 0);
                SplitMetadata {
                    split_id: format!("split_{split_ord:02}").into(),
                    num_docs,
                    time_range: Some(time_range),
                    create_timestamp,
                    maturity: time_to_maturity,
                    ..Default::default()
                }
            })
            .collect()
    }

    // Creates a checksum for a given merge operation.
    // This does not take in account the merge split id,
    // and is split order independent.
    fn compute_checksum_op(op: &MergeOperation) -> u64 {
        let mut checksum = 0u64;
        for split in op.splits_as_slice() {
            let mut hasher = DefaultHasher::default();
            split.split_id.hash(&mut hasher);
            checksum ^= hasher.finish();
        }
        checksum
    }

    // Creates a checksum for a set of operations.
    // This checksum does not depend on the order of the merrge operations,
    // nor the merge split ids.
    fn compute_checksum_ops(ops: &[MergeOperation]) -> u64 {
        let mut checksum = 0u64;
        for op in ops {
            let op_checksum = compute_checksum_op(op);
            let mut hasher = DefaultHasher::default();
            hasher.write_u64(op_checksum);
            checksum ^= hasher.finish();
        }
        checksum
    }

    fn compare_merge_operations(left_ops: &[MergeOperation], right_ops: &[MergeOperation]) -> bool {
        compute_checksum_ops(left_ops) == compute_checksum_ops(right_ops)
    }

    pub(crate) fn proptest_merge_policy(merge_policy: &dyn MergePolicy) {
        proptest!(|(mut splits in prop::collection::vec(split_strategy(), 0..100))| {
            let mut cloned_splits = splits.clone();
            cloned_splits.shuffle(&mut rand::rng());

            let original_num_splits = splits.len();

            let mut operations: Vec<MergeOperation> = merge_policy.operations(&mut splits);
            let operations_after_shuffle = merge_policy.operations(&mut cloned_splits);
            assert!(compare_merge_operations(&operations[..],
                &operations_after_shuffle[..]),
                "Merge policy result should be independent from the original order.");

            let num_splits_in_merge: usize = operations.iter().map(|op| op.splits_as_slice().len()).sum();

            assert_eq!(
                num_splits_in_merge + splits.len(), original_num_splits,
                "Splits should not be lost."
            );

            // This property is not uninteresting but is currently not observed
            // in the stable log merge policy.
            // assert!(
            //     merge_policy.operations(&mut splits).is_empty(),
            //     "Merge policy are expected to return all available merge operations."
            // );
            let now_utc = OffsetDateTime::now_utc();
            for merge_op in &mut operations {
                assert_eq!(merge_op.operation_type, MergeOperationType::Merge,
                    "A merge policy should only emit Merge operations."
                );
                assert!(merge_op.splits_as_slice().len() >= 2,
            "Merge policies should not suggest merging a single split.");
                for split in merge_op.splits_as_slice() {
                    assert!(!split.is_mature(now_utc), "Merges should not contain mature splits.");
                }
                merge_policy.check_is_valid(merge_op, &splits[..]);
            }
        });
    }
}
