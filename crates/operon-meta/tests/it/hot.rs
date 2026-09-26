//! Per-collection hot configuration and the lease prefix query, on
//! `MetaState` directly (M1.3 Task 4, Rulings 7, 12 and 20).

use operon_common::schema::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec};
use operon_common::{CollectionId, NamespaceId};
use operon_meta::{
    ApplyError, Command, HotConfig, MetaState, Reply, snapshot_bytes, snapshot_round_trip,
    state_from_snapshot_bytes,
};

const NS: NamespaceId = NamespaceId(1);

const VECTORS: HotConfig = HotConfig {
    vectors: true,
    text: false,
    fragments: false,
};
const TEXT_AND_FRAGMENTS: HotConfig = HotConfig {
    vectors: false,
    text: true,
    fragments: true,
};

fn schema() -> CollectionSchema {
    CollectionSchema::new(
        vec![FieldSpec {
            name: "title".to_string(),
            source_path: "title".to_string(),
            kind: FieldKind::Keyword,
            indexed: true,
            fast: false,
            ignore_malformed: false,
        }],
        vec![],
        DynamicMapping::Ignore,
    )
}

/// A namespace with collections `a` (id 1) and `b` (id 2).
fn state() -> MetaState {
    let mut state = MetaState::default();
    state
        .apply(Command::CreateNamespace {
            name: "acme".to_string(),
        })
        .expect("namespace");
    for name in ["a", "b"] {
        state
            .apply(Command::CreateCollection {
                namespace: NS,
                name: name.to_string(),
                schema: schema(),
                partitions: 1,
            })
            .expect("collection");
    }
    state
}

fn set(collection: u64, hot: HotConfig) -> Command {
    Command::SetCollectionHot {
        collection: CollectionId(collection),
        hot,
    }
}

fn assert_valid(state: &MetaState) {
    let violations = state.check_invariants();
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn set_collection_hot_is_retry_safe() {
    let mut state = state();
    assert_eq!(state.collection_hot(CollectionId(1)), HotConfig::default());
    assert_eq!(state.apply(set(1, VECTORS)), Ok(Reply::CollectionHotSet));
    let once = state.clone();
    assert_eq!(state.apply(set(1, VECTORS)), Ok(Reply::CollectionHotSet));
    assert_eq!(state, once);
    assert_eq!(state.collection_hot(CollectionId(1)), VECTORS);
    assert_eq!(state.collection_hot(CollectionId(2)), HotConfig::default());
    assert_valid(&state);
}

#[test]
fn an_all_false_config_removes_the_entry() {
    let mut state = state();
    let before = state.clone();
    state.apply(set(1, TEXT_AND_FRAGMENTS)).expect("set");
    assert_eq!(state.hot_collections().count(), 1);
    state.apply(set(1, HotConfig::default())).expect("clear");
    assert_eq!(state.hot_collections().count(), 0);
    assert_eq!(state.collection_hot(CollectionId(1)), HotConfig::default());
    // Nothing of the setting is left: the state equals the one before.
    assert_eq!(state, before);
    // Clearing again is a no-op.
    state
        .apply(set(1, HotConfig::default()))
        .expect("clear again");
    assert_eq!(state, before);
    assert_valid(&state);
}

#[test]
fn setting_hot_on_a_missing_collection_is_not_found() {
    let mut state = state();
    assert_eq!(
        state.apply(set(7, VECTORS)),
        Err(ApplyError::CollectionNotFound(CollectionId(7)))
    );
}

#[test]
fn dropping_a_collection_forgets_its_hot_config() {
    let mut state = state();
    state.apply(set(1, VECTORS)).expect("set a");
    state.apply(set(2, TEXT_AND_FRAGMENTS)).expect("set b");
    state
        .apply(Command::DropCollection {
            namespace: NS,
            name: "a".to_string(),
            now_ms: 10,
        })
        .expect("drop");
    assert_eq!(state.collection_hot(CollectionId(1)), HotConfig::default());
    assert_eq!(
        state.hot_collections().collect::<Vec<_>>(),
        vec![(CollectionId(2), TEXT_AND_FRAGMENTS)]
    );
    assert_valid(&state);
    assert_eq!(
        state.apply(set(1, VECTORS)),
        Err(ApplyError::CollectionNotFound(CollectionId(1)))
    );
}

#[test]
fn leases_with_prefix_lists_only_that_prefix() {
    let mut state = state();
    for key in ["node/1", "node/2", "nodes", "task/x"] {
        state
            .apply(Command::AcquireLease {
                key: key.to_string(),
                owner: "owner".to_string(),
                ttl_ms: 1_000,
                now_ms: 100,
            })
            .expect("acquire");
    }
    state
        .apply(Command::ReleaseLease {
            key: "node/2".to_string(),
            owner: "owner".to_string(),
            epoch: 1,
        })
        .expect("release");
    let listed: Vec<(&str, Option<&str>)> = state
        .leases_with_prefix("node/")
        .map(|(key, lease)| (key, lease.owner.as_deref()))
        .collect();
    assert_eq!(listed, [("node/1", Some("owner")), ("node/2", None)]);
    assert_eq!(state.leases_with_prefix("node").count(), 3);
    assert_eq!(state.leases_with_prefix("").count(), 4);
    assert_eq!(state.leases_with_prefix("t").count(), 1);
    assert_eq!(state.leases_with_prefix("zzz").count(), 0);
}

#[test]
fn rejected_hot_commands_leave_the_state_unchanged() {
    let mut state = state();
    state.apply(set(1, VECTORS)).expect("set");
    let before = state.clone();
    for command in [
        set(0, VECTORS),
        set(3, TEXT_AND_FRAGMENTS),
        set(99, HotConfig::default()),
    ] {
        assert!(state.apply(command).is_err());
        assert_eq!(state, before);
    }
}

#[test]
fn a_snapshot_with_a_hot_entry_round_trips() {
    let mut state = state();
    state.apply(set(2, TEXT_AND_FRAGMENTS)).expect("set");
    let bytes = snapshot_bytes(&state).expect("encode");
    assert_eq!(&bytes[8..12], &6u32.to_le_bytes());
    let decoded = state_from_snapshot_bytes(&bytes).expect("decode");
    assert_eq!(decoded, state);
    assert_eq!(decoded.collection_hot(CollectionId(2)), TEXT_AND_FRAGMENTS);
    assert_eq!(snapshot_round_trip(&state).expect("round trip"), state);
    // Cleared again, the state is written as version 5.
    state.apply(set(2, HotConfig::default())).expect("clear");
    let bytes = snapshot_bytes(&state).expect("encode");
    assert_eq!(&bytes[8..12], &5u32.to_le_bytes());
    assert_eq!(state_from_snapshot_bytes(&bytes).expect("decode"), state);
}
