use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;

use futures::stream;
use openraft::entry::RaftEntry;
use openraft::impls::leader_id_adv::LeaderId;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{EntryOf, LogIdOf};
use openraft::{BasicNode, LogId, Membership};
use operon_common::NamespaceId;
use operon_meta::{Command, LocalDb, SnapshotData, StateMachineStore, TypeConfig, WalClass};
use operon_store::{Fault, FaultyStore, Op, Store};
use tempfile::TempDir;

const PREFIX: &str = "meta/snapshots";

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
    LogId::new(LeaderId { term, node_id: 1 }, index)
}

fn membership_entry(index: u64) -> EntryOf<TypeConfig> {
    let voters = BTreeSet::from([1]);
    let nodes = BTreeMap::from([(1, BasicNode::default())]);
    let membership = Membership::new(vec![voters], nodes).expect("membership");
    EntryOf::<TypeConfig>::new_membership(log_id(1, index), membership)
}

fn command_entry(index: u64, command: Command) -> EntryOf<TypeConfig> {
    EntryOf::<TypeConfig>::new_normal(log_id(1, index), command)
}

fn create_namespace(index: u64, name: &str) -> EntryOf<TypeConfig> {
    command_entry(
        index,
        Command::CreateNamespace {
            name: name.to_string(),
        },
    )
}

async fn apply(sm: &mut StateMachineStore, entries: Vec<EntryOf<TypeConfig>>) {
    let items = entries.into_iter().map(|entry| Ok((entry, None)));
    sm.apply(stream::iter(items)).await.expect("apply");
}

async fn open(node_id: u64, dir: &TempDir, store: &Store) -> StateMachineStore {
    let db = LocalDb::open(dir.path()).expect("open local db");
    StateMachineStore::open(node_id, store.clone(), PREFIX, db)
        .await
        .expect("open state machine")
}

async fn snapshot_paths(store: &Store, node_id: u64) -> Vec<String> {
    let listed = store
        .list(&format!("{PREFIX}/{node_id}/"))
        .await
        .expect("list");
    listed.into_iter().map(|o| o.path).collect()
}

fn namespace_names(sm: &StateMachineStore) -> Vec<String> {
    sm.read(|s| s.namespaces().map(|n| n.name.clone()).collect())
}

#[tokio::test]
async fn applied_entries_update_state_and_applied_log_id() {
    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let mut sm = open(1, &dir, &store).await;

    apply(
        &mut sm,
        vec![
            membership_entry(0),
            create_namespace(1, "acme"),
            command_entry(
                2,
                Command::CreateStream {
                    namespace: NamespaceId(1),
                    name: "events".to_string(),
                    partitions: 2,
                    class: WalClass::Standard,
                    retention: operon_meta::Retention::default(),
                },
            ),
        ],
    )
    .await;

    assert_eq!(namespace_names(&sm), ["acme"]);
    assert!(sm.read(|s| s.stream_by_name(NamespaceId(1), "events").is_some()));
    let (last_applied, membership) = sm.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 2)));
    assert_eq!(membership.log_id(), &Some(log_id(1, 0)));
    assert_eq!(membership.voter_ids().collect::<Vec<_>>(), [1]);
}

#[tokio::test]
async fn a_rejected_command_still_advances_the_applied_log_id() {
    let dir = TempDir::new().unwrap();
    let mut sm = open(1, &dir, &Store::in_memory()).await;
    apply(
        &mut sm,
        vec![create_namespace(1, "acme"), create_namespace(2, "acme")],
    )
    .await;

    assert_eq!(namespace_names(&sm), ["acme"]);
    assert_eq!(sm.applied_state().await.unwrap().0, Some(log_id(1, 2)));
}

#[tokio::test]
async fn snapshots_go_to_object_storage_and_are_reloaded_on_open() {
    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;

    let snapshot = sm.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 1)));
    assert_eq!(
        snapshot_paths(&store, 1).await,
        ["meta/snapshots/1/00000000000000000001-00000000000000000001.snap"]
    );

    drop(sm);
    let mut reopened = open(1, &dir, &store).await;
    assert_eq!(namespace_names(&reopened), ["acme"]);
    let (last_applied, membership) = reopened.applied_state().await.unwrap();
    assert_eq!(last_applied, Some(log_id(1, 1)));
    assert_eq!(membership.voter_ids().collect::<Vec<_>>(), [1]);
}

#[tokio::test]
async fn a_new_snapshot_replaces_and_deletes_the_previous_one() {
    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    sm.build_snapshot().await.unwrap();
    apply(&mut sm, vec![create_namespace(2, "globex")]).await;
    sm.build_snapshot().await.unwrap();

    assert_eq!(
        snapshot_paths(&store, 1).await,
        ["meta/snapshots/1/00000000000000000001-00000000000000000002.snap"]
    );
    let current = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta.last_log_id, Some(log_id(1, 2)));
}

#[tokio::test]
async fn installing_a_snapshot_replaces_state_and_survives_reopen() {
    let store = Store::in_memory();
    let (dir1, dir2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut leader = open(1, &dir1, &store).await;
    apply(
        &mut leader,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    let snapshot = leader.build_snapshot().await.unwrap();

    let mut follower = open(2, &dir2, &store).await;
    apply(&mut follower, vec![create_namespace(1, "stale")]).await;
    follower
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();

    assert_eq!(namespace_names(&follower), ["acme"]);
    assert_eq!(
        follower.applied_state().await.unwrap().0,
        Some(log_id(1, 1))
    );
    assert_eq!(snapshot_paths(&store, 2).await.len(), 1);

    drop(follower);
    let reopened = open(2, &dir2, &store).await;
    assert_eq!(namespace_names(&reopened), ["acme"]);
}

#[tokio::test]
async fn corrupt_or_mismatched_snapshots_are_rejected() {
    let store = Store::in_memory();
    let (dir1, dir2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut leader = open(1, &dir1, &store).await;
    apply(
        &mut leader,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    let snapshot = leader.build_snapshot().await.unwrap();
    let good = snapshot.snapshot.into_inner();

    let mut follower = open(2, &dir2, &store).await;
    let mut flipped = good.clone();
    let middle = flipped.len() / 2;
    flipped[middle] ^= 0x01;
    for bad in [
        flipped,
        good[..good.len() - 1].to_vec(),
        b"garbage".to_vec(),
        vec![],
    ] {
        let err = follower
            .install_snapshot(&snapshot.meta, SnapshotData::new(bad))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
    }

    let mut other_meta = snapshot.meta.clone();
    other_meta.last_log_id = Some(log_id(1, 9));
    let err = follower
        .install_snapshot(&other_meta, SnapshotData::new(good))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");

    assert!(namespace_names(&follower).is_empty());
    assert!(snapshot_paths(&store, 2).await.is_empty());
}

#[tokio::test]
async fn opening_fails_if_the_current_snapshot_is_corrupt() {
    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    sm.build_snapshot().await.unwrap();
    drop(sm);

    let path = snapshot_paths(&store, 1).await.remove(0);
    let (data, _) = store.get(&path).await.unwrap();
    let mut corrupt = data.to_vec();
    corrupt[20] ^= 0xff;
    store.put(&path, corrupt.into()).await.unwrap();

    let db = LocalDb::open(dir.path()).unwrap();
    let err = StateMachineStore::open(1, store.clone(), PREFIX, db)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}

#[tokio::test]
async fn opening_fails_if_the_current_snapshot_is_missing() {
    let dir = TempDir::new().unwrap();
    let store = Store::in_memory();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    sm.build_snapshot().await.unwrap();
    drop(sm);

    // Deleted from the bucket (by a lifecycle rule or a mistaken cleanup): the
    // node must refuse to start rather than come up empty.
    for path in snapshot_paths(&store, 1).await {
        store.delete(&path).await.unwrap();
    }
    let db = LocalDb::open(dir.path()).unwrap();
    assert!(
        StateMachineStore::open(1, store.clone(), PREFIX, db)
            .await
            .is_err()
    );
}

/// A store that fails the operations queued on the returned `FaultyStore`.
fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

#[tokio::test]
async fn failed_snapshot_uploads_are_retried() {
    let dir = TempDir::new().unwrap();
    let (faulty, store) = faulty_store();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;

    // openraft stops the node on a snapshot error, so transient store
    // failures must not surface.
    for _ in 0..3 {
        faulty.inject(Op::Put, Fault::Error);
    }
    let snapshot = sm.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 1)));
    assert_eq!(faulty.calls(Op::Put), 4);
    assert_eq!(snapshot_paths(&store, 1).await.len(), 1);

    // A lost acknowledgement is retried the same way; the rewrite is harmless.
    apply(&mut sm, vec![create_namespace(2, "globex")]).await;
    faulty.inject(Op::Put, Fault::ErrorAfterApply);
    sm.build_snapshot().await.unwrap();
    assert_eq!(faulty.calls(Op::Put), 6);
    drop(sm);
    assert_eq!(
        namespace_names(&open(1, &dir, &store).await),
        ["acme", "globex"]
    );
}

#[tokio::test]
async fn the_current_snapshot_is_served_from_memory() {
    let dir = TempDir::new().unwrap();
    let (faulty, store) = faulty_store();
    let mut sm = open(1, &dir, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    sm.build_snapshot().await.unwrap();
    apply(&mut sm, vec![create_namespace(2, "globex")]).await;
    let built = sm.build_snapshot().await.unwrap();
    drop(sm);
    let mut sm = open(1, &dir, &store).await;
    let gets = faulty.calls(Op::Get);

    // A build that replaces the snapshot deletes its object, possibly while a
    // follower's request for it is being served. Serving from memory means
    // the object's absence (or a failing store) cannot fail the request.
    for path in snapshot_paths(&store, 1).await {
        store.delete(&path).await.unwrap();
    }
    faulty.inject(Op::Get, Fault::Error);
    let current = sm.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, built.meta);
    assert_eq!(current.snapshot.into_inner(), built.snapshot.into_inner());
    assert_eq!(faulty.calls(Op::Get), gets);
}

#[tokio::test]
async fn installing_a_snapshot_older_than_the_current_one_fails_and_changes_nothing() {
    let store = Store::in_memory();
    let (dir1, dir2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut leader = open(1, &dir1, &store).await;
    apply(
        &mut leader,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    let older = leader.build_snapshot().await.unwrap();
    apply(&mut leader, vec![create_namespace(2, "globex")]).await;
    let newer = leader.build_snapshot().await.unwrap();

    let mut follower = open(2, &dir2, &store).await;
    follower
        .install_snapshot(&newer.meta, newer.snapshot)
        .await
        .unwrap();
    let paths = snapshot_paths(&store, 2).await;
    let err = follower
        .install_snapshot(&older.meta, older.snapshot)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");

    assert_eq!(namespace_names(&follower), ["acme", "globex"]);
    assert_eq!(
        follower.applied_state().await.unwrap().0,
        Some(log_id(1, 2))
    );
    assert_eq!(snapshot_paths(&store, 2).await, paths);
    let current = follower.get_current_snapshot().await.unwrap().unwrap();
    assert_eq!(current.meta, newer.meta);
    drop(follower);
    assert_eq!(
        namespace_names(&open(2, &dir2, &store).await),
        ["acme", "globex"]
    );
}

#[tokio::test]
async fn opening_fails_if_the_current_snapshot_is_not_the_one_the_pointer_names() {
    let store = Store::in_memory();
    let (dir1, dir2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut sm = open(1, &dir1, &store).await;
    apply(
        &mut sm,
        vec![membership_entry(0), create_namespace(1, "acme")],
    )
    .await;
    sm.build_snapshot().await.unwrap();
    drop(sm);

    // A valid, checksummed snapshot of another log position at the pointer's
    // path, as a second cluster sharing the bucket could write.
    let mut other = open(2, &dir2, &Store::in_memory()).await;
    apply(
        &mut other,
        vec![
            membership_entry(0),
            create_namespace(1, "other"),
            create_namespace(2, "cluster"),
        ],
    )
    .await;
    let foreign = other.build_snapshot().await.unwrap();
    let path = snapshot_paths(&store, 1).await.remove(0);
    store
        .put(&path, foreign.snapshot.into_inner().into())
        .await
        .unwrap();

    let db = LocalDb::open(dir1.path()).unwrap();
    let err = StateMachineStore::open(1, store.clone(), PREFIX, db)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
}
