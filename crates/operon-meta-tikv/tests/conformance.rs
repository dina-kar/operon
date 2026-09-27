//! The `MetaStore` conformance suite against the TiKV metastore (R1 plan
//! Task 4): the cases whose methods are all Task 4's (row R3; Task 5 runs
//! all 53), plus the TiKV-specific clock and id-block tests. Every test
//! needs a cluster and skips without `OPERON_TEST_PD`.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use operon_common::meta::{Consistency, MetaStore};
use operon_meta_conformance::{Backend, Faults, Instance};
use operon_meta_tikv::TikvMeta;
use operon_tikv::testing::{self, TEST_META, TestCluster};
use operon_tikv::{Fault, FaultPlan, FaultPoint, Tikv};

/// Handles per case: three, as for a three-node backend.
const HANDLES: usize = 3;

/// One fresh metastore per case: a random root in `loam_test_meta`, three
/// `TikvMeta` handles on it (each with its own `tikv-client`), and a fault
/// adapter.
struct TikvBackend;

#[async_trait]
impl Backend for TikvBackend {
    async fn start(&self) -> Instance {
        let cluster = testing::cluster()
            .await
            .expect("unavailable() said a cluster is configured");
        let (clients, plans) =
            open_handles(&cluster, HANDLES, operon_meta_tikv::DEFAULT_ID_BLOCK).await;
        Instance {
            clients: clients
                .into_iter()
                .map(|m| Arc::new(m) as Arc<dyn MetaStore>)
                .collect(),
            faults: Some(Arc::new(LoseAcks(plans))),
            guard: Box::new(()),
        }
    }

    fn unavailable(&self) -> Option<String> {
        std::env::var(testing::PD_ENV)
            .map_or(true, |v| v.trim().is_empty())
            .then(|| testing::PD_ENV.to_string())
    }
}

/// `count` handles on one new random root, each with its own lost-ack plan.
async fn open_handles(
    cluster: &TestCluster,
    count: usize,
    id_block: u64,
) -> (Vec<TikvMeta>, Vec<Arc<LoseAck>>) {
    let config = cluster.config(TEST_META);
    let mut metas = Vec::new();
    let mut plans = Vec::new();
    for _ in 0..count {
        let plan = Arc::new(LoseAck::default());
        let tikv = Tikv::connect(config.clone())
            .await
            .expect("connect to the test keyspace")
            .with_faults(plan.clone());
        let meta = TikvMeta::open_on(tikv, id_block, operon_meta_tikv::DEFAULT_POLL)
            .await
            .expect("open the metastore");
        metas.push(meta);
        plans.push(plan);
    }
    (metas, plans)
}

/// A one-shot lost acknowledgement: the next commit of the handle succeeds,
/// then reports an unknown outcome, which the runner resolves through the
/// commit token (row R3).
#[derive(Default)]
struct LoseAck {
    armed: AtomicBool,
}

impl FaultPlan for LoseAck {
    fn at(&self, _op: &str, point: FaultPoint, _attempt: u32) -> Option<Fault> {
        (point == FaultPoint::AfterCommit && self.armed.swap(false, Ordering::SeqCst))
            .then_some(Fault::LoseAck)
    }
}

/// The suite's fault hooks: `lose_next_ack` arms a handle's [`LoseAck`];
/// `disturb` and `heal` do nothing until Task 6's nemesis.
struct LoseAcks(Vec<Arc<LoseAck>>);

#[async_trait]
impl Faults for LoseAcks {
    fn lose_next_ack(&self, client: usize) {
        self.0[client].armed.store(true, Ordering::SeqCst);
    }

    async fn disturb(&self, _seed: u64) {}

    async fn heal(&self) {}
}

mod suite {
    operon_meta_conformance::metastore_conformance!(super::TikvBackend; cases = [
        // catalog
        namespace_create_then_lookup,
        namespace_create_retry_reports_namespace_exists,
        stream_create_validates_names_partitions_and_reserved_prefix,
        set_retention_round_trips,
        link_create_lookup_and_retry,
        lists_are_ordered_by_id,
        // leases
        acquire_renew_release,
        a_held_lease_refuses_another_owner,
        renew_at_a_stale_epoch_is_lease_lost,
        reacquire_after_expiry_keeps_the_epoch,
        a_ttl_above_the_limit_is_invalid,
        leases_with_prefix_lists_only_that_prefix,
        // pointers
        cas_create_then_update,
        cas_mismatch_carries_the_current_pointer,
        a_fenced_cas_is_refused,
        a_collection_pointer_needs_its_collection,
        a_key_over_the_limit_is_invalid,
        // collections
        create_collection_makes_its_stream_and_link,
        create_collection_retry_is_collection_exists,
        schema_updates_are_additive_and_versioned,
        aliases_apply_atomically_and_resolve,
        collection_for_link_finds_the_implicit_link,
        // hot
        collection_hot_defaults_and_set_is_retry_safe,
        collection_hot_needs_the_collection_in_its_namespace,
        a_dropped_collection_forgets_its_hot_config,
        // changes
        changes_wake_a_waiter_armed_before_a_write,
        is_ready_after_start,
        a_linearizable_read_through_another_client_sees_an_acknowledged_write,
        // faults
        a_lost_ack_on_create_collection_returns_the_same_ids,
        a_lost_ack_on_cas_reports_a_mismatch_with_the_callers_value,
        // linearizable
        concurrent_cas_on_two_keys_is_linearizable,
    ]);
}

/// `clock_ms` is the physical part of a fresh TSO timestamp: between two TSO
/// fetches around it, and never behind `now_ms`'s anchor.
#[tokio::test]
async fn clock_is_tso_physical() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let (metas, _) = open_handles(&cluster, 1, 10).await;
    let meta = &metas[0];
    let tikv = meta.tikv();
    for _ in 0..20 {
        let before = Tikv::physical_ms(&tikv.now().await.expect("tso"));
        let clock = meta
            .clock_ms(Consistency::Linearizable)
            .await
            .expect("clock");
        let after = Tikv::physical_ms(&tikv.now().await.expect("tso"));
        assert!(
            before <= clock && clock <= after,
            "{before} <= {clock} <= {after}"
        );
        // now_ms never trails the TSO this handle has seen.
        assert!(meta.now_ms() >= after, "now_ms {} < {after}", meta.now_ms());
    }
    // now_ms is monotonic and keeps up with wall time between fetches.
    let mut last = meta.now_ms();
    for _ in 0..100 {
        let now = meta.now_ms();
        assert!(now >= last);
        last = now;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(meta.now_ms() >= last + 50);
}

/// Ids come from per-handle blocks: two handles interleave from different
/// blocks, a handle that stops leaves the rest of its block unused, and no
/// id is ever handed out twice, also under concurrent creates.
#[tokio::test]
async fn id_blocks_leave_gaps_but_never_repeat() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    const BLOCK: u64 = 4;
    let config = cluster.config(TEST_META);
    let open = || async {
        let tikv = Tikv::connect(config.clone()).await.expect("connect");
        TikvMeta::open_on(tikv, BLOCK, operon_meta_tikv::DEFAULT_POLL)
            .await
            .expect("open")
    };
    let a = open().await;
    let b = open().await;
    let mut ids = Vec::new();
    for i in 0..6 {
        ids.push(
            a.create_namespace(&format!("ids-a-{i}"))
                .await
                .expect("a")
                .0,
        );
        ids.push(
            b.create_namespace(&format!("ids-b-{i}"))
                .await
                .expect("b")
                .0,
        );
    }
    // `a` took block 1..5 then 9..13; `b` took 5..9 then 13..17.
    let from_a: Vec<u64> = ids.iter().step_by(2).copied().collect();
    let from_b: Vec<u64> = ids.iter().skip(1).step_by(2).copied().collect();
    assert_eq!(from_a, [1, 2, 3, 4, 9, 10]);
    assert_eq!(from_b, [5, 6, 7, 8, 13, 14]);
    // A new handle takes a fresh block: 11, 12 and 15, 16 stay unused.
    drop(a);
    let c = open().await;
    ids.push(c.create_namespace("ids-c-0").await.expect("c").0);
    assert_eq!(ids.last(), Some(&17));

    // Concurrent creates through fresh handles never repeat an id.
    let mut tasks = Vec::new();
    for h in 0..4 {
        let meta = open().await;
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            for i in 0..10 {
                got.push(
                    meta.create_namespace(&format!("ids-par-{h}-{i}"))
                        .await
                        .expect("create")
                        .0,
                );
            }
            got
        }));
    }
    for task in tasks {
        ids.extend(task.await.expect("join"));
    }
    let unique: BTreeSet<u64> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "an id repeated: {ids:?}");
    // The namespaces list agrees, in id order.
    let listed: Vec<u64> = c
        .namespaces(Consistency::Linearizable)
        .await
        .expect("list")
        .into_iter()
        .map(|n| n.id.0)
        .collect();
    assert_eq!(listed, unique.into_iter().collect::<Vec<_>>());
}
