//! The stable-log merge policy plans over split metadata.

use std::time::Duration;

use operon_quickwit::merge_policy::{
    MergePolicy, StableLogMergePolicy, StableLogMergePolicyConfig,
};
use operon_quickwit::shim::consts::DEFAULT_SPLIT_NUM_DOCS_TARGET;
use operon_quickwit::shim::{SplitId, SplitMaturity, SplitMetadata};
use time::OffsetDateTime;

#[test]
fn ten_small_splits_merge_into_one() {
    let config = StableLogMergePolicyConfig::default();
    assert_eq!(
        config,
        StableLogMergePolicyConfig {
            min_level_num_docs: 100_000,
            merge_factor: 10,
            max_merge_factor: 12,
            maturation_period: Duration::from_secs(48 * 3600),
        }
    );
    let policy = StableLogMergePolicy::new(config, DEFAULT_SPLIT_NUM_DOCS_TARGET);
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let mut splits: Vec<SplitMetadata> = (0..10)
        .map(|i| SplitMetadata {
            split_id: SplitId::new(),
            num_docs: 1_000,
            time_range: Some(i * 100..=i * 100 + 99),
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_secs(48 * 3600),
            },
            create_timestamp: now,
            num_merge_ops: 0,
            footer_offsets: 0..100,
        })
        .collect();
    let mut expected_ids: Vec<String> = splits.iter().map(|s| s.split_id().to_string()).collect();

    let operations = policy.operations(&mut splits);

    assert_eq!(operations.len(), 1);
    let mut merged_ids: Vec<String> = operations[0]
        .splits_as_slice()
        .iter()
        .map(|s| s.split_id().to_string())
        .collect();
    merged_ids.sort();
    expected_ids.sort();
    assert_eq!(merged_ids, expected_ids);
    assert!(splits.is_empty());
}
