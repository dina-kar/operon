//! The node registry (plan M1.3 Task 10 rule 1; Ruling 12): `node/<id>`
//! leases on a single-node metastore with a manual clock.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_common::meta::{Consistency, MetaStore};
use operon_hot::{NodeDescriptor, NodeRegistry, RegistryConfig, Roles};
use operon_meta::{ManualClock, SystemClock};
use ulid::Ulid;

use crate::common::{Meta, WAIT};

const CONFIG: RegistryConfig = RegistryConfig {
    lease_ttl: Duration::from_secs(10),
    renew_every: Duration::from_millis(50),
    refresh_every: Duration::from_millis(50),
};

fn node(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        node_id: id,
        incarnation: Ulid::from_parts(id, u128::from(id)),
        addr: SocketAddr::from(([127, 0, 0, 1], 7000 + id as u16)),
        roles: Roles::parse("query,gateway").expect("roles"),
        zone: format!("z{}", id % 2),
    }
}

async fn meta() -> (Meta, Arc<ManualClock>, Arc<dyn MetaStore>) {
    use operon_meta::Clock as _;
    let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
    let meta = Meta::start(clock.clone()).await;
    let store: Arc<dyn MetaStore> = Arc::new(meta.client.clone());
    (meta, clock, store)
}

fn ids(registry: &NodeRegistry) -> Vec<u64> {
    registry.live().iter().map(|n| n.node_id).collect()
}

async fn eventually(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_registers_and_is_live() {
    let (meta, _, store) = meta().await;
    let one = NodeRegistry::register(store.clone(), node(1), CONFIG)
        .await
        .expect("register");
    assert_eq!(one.me(), &node(1));
    assert_eq!(*one.live(), vec![node(1)]);
    let two = NodeRegistry::register(store.clone(), node(2), CONFIG)
        .await
        .expect("register");
    assert_eq!(*two.live(), vec![node(1), node(2)]);
    eventually("node 1 sees node 2", || ids(&one) == vec![1, 2]).await;
    one.deregister().await;
    two.deregister().await;
    meta.node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_node_leaves_the_live_set() {
    let (meta, clock, store) = meta().await;
    let one = NodeRegistry::register(store.clone(), node(1), CONFIG)
        .await
        .expect("register");
    let two = NodeRegistry::register(store.clone(), node(2), CONFIG)
        .await
        .expect("register");
    eventually("node 2 sees node 1", || ids(&two) == vec![1, 2]).await;
    // Node 1 stops renewing (it died); node 2 keeps renewing.
    one.stop();
    clock.advance(CONFIG.lease_ttl + Duration::from_secs(1));
    eventually("node 1 leaves the live set", || ids(&two) == vec![2]).await;
    two.deregister().await;
    meta.node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_node_waits_for_its_old_lease() {
    let (meta, clock, store) = meta().await;
    let first = NodeRegistry::register(store.clone(), node(1), CONFIG)
        .await
        .expect("register");
    first.stop();
    let second = NodeDescriptor {
        incarnation: Ulid::from_parts(99, 99),
        ..node(1)
    };
    let pending = tokio::spawn(NodeRegistry::register(
        store.clone(),
        second.clone(),
        CONFIG,
    ));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(!pending.is_finished(), "registered over a held lease");
    clock.advance(CONFIG.lease_ttl + Duration::from_millis(1));
    let registry = tokio::time::timeout(WAIT, pending)
        .await
        .expect("registered after the old lease expired")
        .expect("join")
        .expect("register");
    assert_eq!(*registry.live(), vec![second.clone()]);
    let lease = store
        .lease(Consistency::Local, "node/1")
        .await
        .expect("lease")
        .expect("a lease");
    assert_eq!(lease.owner, Some(second.encode()));

    // A lease held by someone else past 2 × ttl: registration gives up.
    registry.deregister().await;
    store
        .acquire_lease("node/1", "squatter", Duration::from_secs(3600))
        .await
        .expect("squat");
    let third = NodeDescriptor {
        incarnation: Ulid::from_parts(100, 100),
        ..node(1)
    };
    let pending = tokio::spawn(NodeRegistry::register(store.clone(), third, CONFIG));
    tokio::time::sleep(Duration::from_millis(200)).await;
    clock.advance(CONFIG.lease_ttl * 2 + Duration::from_secs(1));
    let err = tokio::time::timeout(WAIT, pending)
        .await
        .expect("gave up")
        .expect("join")
        .expect_err("still held");
    assert!(err.to_string().contains("still registered"), "{err}");
    meta.node.shutdown().await.expect("shutdown");
}

#[test]
fn descriptors_round_trip() {
    for descriptor in [
        node(1),
        NodeDescriptor {
            roles: Roles::all(),
            zone: String::new(),
            addr: "[::1]:9000".parse().expect("addr"),
            ..node(2)
        },
        NodeDescriptor {
            zone: "eu-west-1a;odd".to_string(),
            ..node(3)
        },
    ] {
        let encoded = descriptor.encode();
        assert!(encoded.starts_with("v1;"), "{encoded}");
        assert_eq!(
            NodeDescriptor::decode(descriptor.node_id, &encoded).expect("decode"),
            descriptor
        );
    }
    assert_eq!(
        node(1).encode(),
        format!(
            "v1;{};127.0.0.1:7001;query,gateway;z1",
            Ulid::from_parts(1, 1)
        )
    );
    for bad in [
        "",
        "v2;01J0000000000000000000000;127.0.0.1:1;query;",
        "v1;nope;127.0.0.1:1;query;",
        "v1;01J0000000000000000000000;host:1;query;",
        "v1;01J0000000000000000000000;127.0.0.1:1;cook;",
        "v1;01J0000000000000000000000;127.0.0.1:1;query",
    ] {
        assert!(NodeDescriptor::decode(1, bad).is_err(), "{bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_descriptor_is_skipped() {
    let (meta, _, store) = meta().await;
    let ttl = Duration::from_secs(60);
    store
        .acquire_lease("node/9", "garbage", ttl)
        .await
        .expect("lease");
    store
        .acquire_lease("node/x", &node(8).encode(), ttl)
        .await
        .expect("lease");
    let one = NodeRegistry::register(store.clone(), node(1), CONFIG)
        .await
        .expect("register");
    one.refresh().await.expect("refresh");
    assert_eq!(ids(&one), vec![1]);
    one.deregister().await;
    meta.node.shutdown().await.expect("shutdown");
}

#[test]
fn roles_parse_and_display() {
    let roles = Roles::parse("query,meta").expect("roles");
    assert!(roles.meta && roles.query && !roles.log && !roles.worker && !roles.gateway);
    assert_eq!(roles.to_string(), "meta,query");
    assert_eq!(Roles::all().to_string(), "meta,log,query,worker,gateway");
    assert_eq!(
        Roles::parse("gateway,worker,log")
            .expect("roles")
            .to_string(),
        "log,worker,gateway"
    );
    for bad in ["cook", "", "query,", "meta,cook"] {
        assert!(Roles::parse(bad).is_err(), "{bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deregister_releases_the_lease() {
    let (meta, _, store) = meta().await;
    let one = NodeRegistry::register(store.clone(), node(1), CONFIG)
        .await
        .expect("register");
    let two = NodeRegistry::register(store.clone(), node(2), CONFIG)
        .await
        .expect("register");
    one.deregister().await;
    let lease = store
        .lease(Consistency::Local, "node/1")
        .await
        .expect("lease")
        .expect("a lease");
    assert_eq!(lease.owner, None, "released");
    eventually("node 1 leaves the live set", || ids(&two) == vec![2]).await;
    // A new incarnation registers at once.
    let again = NodeRegistry::register(
        store.clone(),
        NodeDescriptor {
            incarnation: Ulid::from_parts(5, 5),
            ..node(1)
        },
        CONFIG,
    )
    .await
    .expect("register");
    again.deregister().await;
    two.deregister().await;
    meta.node.shutdown().await.expect("shutdown");
}
