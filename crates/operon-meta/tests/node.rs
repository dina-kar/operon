use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_common::{NamespaceId, StreamId};
use operon_meta::{
    ApplyError, Consistency, Fence, ManualClock, MetaConfig, MetaError, MetaNode, RaftStatus,
    Router, WalChunk, WalClass,
};
use operon_store::{Fault, FaultyStore, Op, Store};
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(10);

fn config(dir: &TempDir, store: &Store) -> MetaConfig {
    MetaConfig::new(1, dir.path(), store.clone())
}

async fn start(config: MetaConfig) -> MetaNode {
    let node = MetaNode::start(config, &Router::new())
        .await
        .expect("start node");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(WAIT).await.expect("leader");
    node
}

/// A store that fails the operations queued on the returned `FaultyStore`.
fn faulty_store() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Store::in_memory().inner().clone()));
    (faulty.clone(), Store::new(faulty))
}

fn namespace_names(state: &operon_meta::MetaState) -> Vec<String> {
    state.namespaces().map(|n| n.name.clone()).collect()
}

/// Waits until `node`'s status satisfies `check`.
async fn wait_for_status(node: &MetaNode, check: impl Fn(RaftStatus) -> bool) -> RaftStatus {
    let deadline = Instant::now() + WAIT;
    loop {
        let status = node.status();
        if check(status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "status never matched: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn chunk(stream: StreamId, records: u32) -> WalChunk {
    WalChunk {
        stream,
        partition: 0,
        records,
        byte_range: 0..64,
        max_timestamp_ms: 0,
    }
}

#[tokio::test]
async fn a_single_node_serves_writes_and_reads() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = start(config(&dir, &store)).await;
    assert_eq!(node.current_leader().await, Some(1));

    let ns = node.create_namespace("acme").await.unwrap();
    let stream = node
        .create_stream(ns, "events", 2, WalClass::Standard)
        .await
        .unwrap();
    assert_eq!(
        node.commit_wal("wal/1.wal", 0, vec![chunk(stream, 10)])
            .await
            .unwrap(),
        [0]
    );
    assert_eq!(
        node.commit_wal("wal/2.wal", 0, vec![chunk(stream, 5)])
            .await
            .unwrap(),
        [10]
    );
    // A retried commit gets its original offsets.
    assert_eq!(
        node.commit_wal("wal/1.wal", 0, vec![chunk(stream, 10)])
            .await
            .unwrap(),
        [0]
    );

    for consistency in [Consistency::Linearizable, Consistency::Local] {
        let next = node
            .read(consistency, |s| {
                s.partition(stream, 0).map(|p| p.next_offset())
            })
            .await
            .unwrap();
        assert_eq!(next, Some(15));
    }
}

#[tokio::test]
async fn rejected_commands_come_back_as_rejected_errors() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = start(config(&dir, &store)).await;
    let ns = node.create_namespace("acme").await.unwrap();

    let err = node.create_namespace("acme").await.unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::NamespaceExists(id)) if id == ns),
        "{err:?}"
    );
    let err = node
        .create_stream(NamespaceId(99), "events", 1, WalClass::Standard)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::NamespaceNotFound(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn leases_are_stamped_with_the_node_clock() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let clock = Arc::new(ManualClock::new(1_000));
    let mut config = config(&dir, &store);
    config.clock = clock.clone();
    let node = start(config).await;
    let ns = node.create_namespace("acme").await.unwrap();
    let ttl = Duration::from_secs(10);

    let first = node.acquire_lease("task/a", "w1", ttl).await.unwrap();
    assert_eq!((first.epoch, first.deadline_ms), (1, 11_000));
    let err = node.acquire_lease("task/a", "w2", ttl).await.unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::LeaseHeld { .. })),
        "{err:?}"
    );

    clock.advance(ttl);
    let second = node.acquire_lease("task/a", "w2", ttl).await.unwrap();
    assert_eq!((second.epoch, second.deadline_ms), (2, 21_000));
    let err = node.renew_lease("task/a", "w1", 1, ttl).await.unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::LeaseLost { .. })),
        "{err:?}"
    );

    let fence = |epoch| {
        Some(Fence {
            lease: "task/a".to_string(),
            epoch,
        })
    };
    let err = node
        .cas_pointer(ns, "manifest", None, "m/1.pb", fence(1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaError::Rejected(ApplyError::Fenced { .. })),
        "{err:?}"
    );
    assert_eq!(
        node.cas_pointer(ns, "manifest", None, "m/1.pb", fence(2))
            .await
            .unwrap(),
        1
    );
    node.release_lease("task/a", "w2", 2).await.unwrap();
}

#[tokio::test]
async fn an_uninitialized_node_refuses_writes_and_linearizable_reads() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = MetaNode::start(config(&dir, &store), &Router::new())
        .await
        .unwrap();

    let err = node.create_namespace("acme").await.unwrap_err();
    assert!(
        matches!(err, MetaError::NotLeader { leader: None }),
        "{err:?}"
    );
    let err = node
        .read(Consistency::Linearizable, |s| s.namespaces().count())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaError::NotLeader { leader: None }),
        "{err:?}"
    );
    let count = node
        .read(Consistency::Local, |s| s.namespaces().count())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn initialize_is_idempotent_and_checks_membership() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = start(config(&dir, &store)).await;
    node.initialize([1]).await.unwrap();
    node.create_namespace("acme").await.unwrap();
    // Initializing an existing cluster with other voters is a mistake, not a no-op.
    let err = node.initialize([1, 2]).await.unwrap_err();
    assert!(matches!(err, MetaError::Config(_)), "{err:?}");
    node.initialize([1]).await.unwrap();

    let (dir2, store2) = (TempDir::new().unwrap(), Store::in_memory());
    let other = MetaNode::start(MetaConfig::new(5, dir2.path(), store2), &Router::new())
        .await
        .unwrap();
    let err = other.initialize([1, 2]).await.unwrap_err();
    assert!(matches!(err, MetaError::Config(_)), "{err:?}");
}

#[tokio::test]
async fn state_survives_a_restart() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = start(config(&dir, &store)).await;
    node.create_namespace("acme").await.unwrap();
    node.shutdown().await.unwrap();
    drop(node);

    // Recovery re-applies the log inside `start`: a local read right away,
    // before any leader is elected, already sees the write.
    let node = MetaNode::start(config(&dir, &store), &Router::new())
        .await
        .unwrap();
    let names = node
        .read(Consistency::Local, namespace_names)
        .await
        .unwrap();
    assert_eq!(names, ["acme"]);
    node.wait_for_leader(WAIT).await.unwrap();
    let names = node
        .read(Consistency::Linearizable, namespace_names)
        .await
        .unwrap();
    assert_eq!(names, ["acme"]);
    assert_eq!(
        node.create_namespace("globex").await.unwrap(),
        NamespaceId(2)
    );
}

#[tokio::test]
async fn shutdown_releases_the_local_database_even_while_handles_remain() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let node = start(config(&dir, &store)).await;
    node.create_namespace("acme").await.unwrap();
    let clone = node.clone();
    node.shutdown().await.unwrap();

    // `clone` is still alive, yet the node restarts on the same directory.
    let restarted = start(config(&dir, &store)).await;
    assert_eq!(
        restarted.create_namespace("globex").await.unwrap(),
        NamespaceId(2)
    );
    let err = clone.create_namespace("zombie").await.unwrap_err();
    assert!(matches!(err, MetaError::Unavailable(_)), "{err:?}");
}

#[tokio::test]
async fn state_survives_a_restart_after_snapshot_and_log_purge() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let mut cfg = config(&dir, &store);
    cfg.logs_after_snapshot = 0;
    let node = start(cfg.clone()).await;
    let ns = node.create_namespace("acme").await.unwrap();
    let stream = node
        .create_stream(ns, "events", 1, WalClass::Standard)
        .await
        .unwrap();
    node.commit_wal("wal/1.wal", 0, vec![chunk(stream, 7)])
        .await
        .unwrap();
    node.snapshot().await.unwrap();
    let status = node.status();
    assert_eq!(status.leader, Some(1));
    assert!(status.snapshot.is_some() && status.snapshot == status.last_applied);
    // The purge follows the snapshot asynchronously.
    let snapshotted = wait_for_status(&node, |s| s.purged >= s.snapshot).await;
    assert_eq!(snapshotted.purged, status.snapshot);
    // Entries after the snapshot are recovered from the log.
    node.commit_wal("wal/2.wal", 0, vec![chunk(stream, 3)])
        .await
        .unwrap();

    let snapshots = store.list("meta/snapshots/1/").await.unwrap();
    assert_eq!(snapshots.len(), 1, "{snapshots:?}");
    let before = node.status();
    node.shutdown().await.unwrap();
    drop(node);

    // Right after `start`, before any leader: the local state already holds
    // the snapshot plus the re-applied log tail, and the status reports it.
    let node = MetaNode::start(cfg, &Router::new()).await.unwrap();
    let status = node.status();
    assert_eq!(
        (status.last_applied, status.snapshot, status.purged),
        (
            before.last_applied,
            snapshotted.snapshot,
            snapshotted.purged
        ),
        "{status:?}"
    );
    let next_offset = |s: &operon_meta::MetaState| s.partition(stream, 0).map(|p| p.next_offset());
    assert_eq!(
        node.read(Consistency::Local, next_offset).await.unwrap(),
        Some(10)
    );
    node.wait_for_leader(WAIT).await.unwrap();
    assert_eq!(
        node.read(Consistency::Linearizable, next_offset)
            .await
            .unwrap(),
        Some(10)
    );
    assert_eq!(
        node.commit_wal("wal/3.wal", 0, vec![chunk(stream, 1)])
            .await
            .unwrap(),
        [10]
    );
}

#[tokio::test]
async fn snapshots_are_taken_automatically_every_n_entries() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let mut cfg = config(&dir, &store);
    cfg.snapshot_every = 10;
    let node = start(cfg).await;
    for i in 0..25 {
        node.create_namespace(&format!("ns{i}")).await.unwrap();
    }

    let mut found = false;
    for _ in 0..100 {
        if !store.list("meta/snapshots/1/").await.unwrap().is_empty() {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(found, "no snapshot written after 25 entries");
}

#[tokio::test]
async fn a_node_keeps_serving_through_failed_snapshot_uploads() {
    let dir = TempDir::new().unwrap();
    let (faulty, store) = faulty_store();
    let node = start(config(&dir, &store)).await;
    node.create_namespace("acme").await.unwrap();

    // openraft stops the node for good on a snapshot error; the state machine
    // must ride out a store that fails for a while.
    for _ in 0..3 {
        faulty.inject(Op::Put, Fault::Error);
    }
    let snapshot = tokio::spawn({
        let node = node.clone();
        async move { node.snapshot().await }
    });
    while faulty.calls(Op::Put) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Writes and reads go on while the upload is being retried.
    node.create_namespace("globex").await.unwrap();
    snapshot.await.unwrap().unwrap();
    assert_eq!(faulty.calls(Op::Put), 4);
    assert_eq!(store.list("meta/snapshots/1/").await.unwrap().len(), 1);

    node.create_namespace("initech").await.unwrap();
    node.snapshot().await.unwrap();
    let names = node
        .read(Consistency::Linearizable, namespace_names)
        .await
        .unwrap();
    assert_eq!(names, ["acme", "globex", "initech"]);
}

#[tokio::test]
async fn snapshot_waits_out_an_in_flight_build_that_covers_less() {
    let dir = TempDir::new().unwrap();
    let (faulty, store) = faulty_store();
    let mut cfg = config(&dir, &store);
    cfg.snapshot_every = 5;
    let node = start(cfg).await;

    // Slow down the automatic build (retrying failed uploads) so that it is
    // still in flight, covering fewer entries, when `snapshot` is called.
    for _ in 0..4 {
        faulty.inject(Op::Put, Fault::Error);
    }
    for i in 0..5 {
        node.create_namespace(&format!("ns{i}")).await.unwrap();
    }
    while faulty.calls(Op::Put) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Too few entries for openraft to start another automatic build after
    // this one: only `snapshot` itself can get the snapshot to cover them.
    for i in 5..7 {
        node.create_namespace(&format!("ns{i}")).await.unwrap();
    }
    let applied = node.status().last_applied;

    node.snapshot().await.unwrap();
    let status = node.status();
    assert!(
        status.snapshot >= applied,
        "{status:?}, applied {applied:?}"
    );
}

#[tokio::test]
async fn shutdown_cuts_short_a_snapshot_upload_being_retried() {
    let dir = TempDir::new().unwrap();
    let (faulty, store) = faulty_store();
    let mut cfg = config(&dir, &store);
    cfg.request_timeout = Duration::from_secs(2);
    let node = start(cfg.clone()).await;
    node.create_namespace("acme").await.unwrap();

    for _ in 0..10_000 {
        faulty.inject(Op::Put, Fault::Error);
    }
    let snapshot = tokio::spawn({
        let node = node.clone();
        async move { node.snapshot().await }
    });
    while faulty.calls(Op::Put) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    node.shutdown().await.unwrap();
    assert!(snapshot.await.unwrap().is_err());

    let node = start(cfg).await;
    let names = node
        .read(Consistency::Linearizable, namespace_names)
        .await
        .unwrap();
    assert_eq!(names, ["acme"]);
}

#[tokio::test]
async fn a_node_without_local_state_refuses_to_start_over_existing_snapshots() {
    let store = Store::in_memory();
    let dir = TempDir::new().unwrap();
    let node = start(config(&dir, &store)).await;
    node.create_namespace("acme").await.unwrap();
    node.snapshot().await.unwrap();
    node.shutdown().await.unwrap();

    // The same node on a replaced (empty) data directory would start an empty
    // metastore, reissue ids and later overwrite the snapshots. So would any
    // other node id pointed at this bucket.
    for id in [1, 2] {
        let empty = TempDir::new().unwrap();
        let err = MetaNode::start(
            MetaConfig::new(id, empty.path(), store.clone()),
            &Router::new(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, MetaError::Config(msg) if msg.contains("snapshot")),
            "{err:?}"
        );
    }

    // An operator who knows the snapshots are stale can override the check.
    let empty = TempDir::new().unwrap();
    let mut cfg = MetaConfig::new(1, empty.path(), store.clone());
    cfg.allow_fresh_start_with_existing_snapshots = true;
    let fresh = start(cfg).await;
    assert_eq!(
        fresh
            .read(Consistency::Local, |s| s.namespaces().count())
            .await
            .unwrap(),
        0
    );
    fresh.shutdown().await.unwrap();

    // The original data directory still starts, with its state.
    let node = start(config(&dir, &store)).await;
    let names = node
        .read(Consistency::Linearizable, namespace_names)
        .await
        .unwrap();
    assert_eq!(names, ["acme"]);
}

#[tokio::test]
async fn invalid_configs_are_rejected() {
    let (dir, store) = (TempDir::new().unwrap(), Store::in_memory());
    let mut cfg = config(&dir, &store);
    cfg.snapshot_every = 0;
    let err = MetaNode::start(cfg, &Router::new()).await.unwrap_err();
    assert!(matches!(err, MetaError::Config(_)), "{err:?}");
}
