//! Rendezvous placement (plan M1.3 Task 10 rules 2 and 3; Ruling 13; E61,
//! E62).

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use operon_common::{CollectionId, NamespaceId};
use operon_hot::{
    NodeDescriptor, NodeRegistry, PlacementImpl, PlacementKey, Roles, owners, ranking,
    rendezvous_score,
};
use operon_query::placement::{Owner, Placement};
use ulid::Ulid;

fn node_in(id: u64, zone: &str, roles: &str) -> NodeDescriptor {
    NodeDescriptor {
        node_id: id,
        incarnation: Ulid::from_parts(id, 0),
        addr: SocketAddr::from(([10, 0, 0, id as u8], 7000)),
        roles: Roles::parse(roles).expect("roles"),
        zone: zone.to_string(),
    }
}

fn node(id: u64) -> NodeDescriptor {
    node_in(id, "", "query")
}

fn nodes(ids: impl IntoIterator<Item = u64>) -> Vec<NodeDescriptor> {
    ids.into_iter().map(node).collect()
}

fn owner_id(ns: u64, cid: u64, nodes: &[NodeDescriptor]) -> Option<u64> {
    owners(NamespaceId(ns), CollectionId(cid), nodes, 1)
        .first()
        .map(|n| n.node_id)
}

#[test]
fn rendezvous_matches_the_reference_scores() {
    let table: [((u64, u64), [u64; 3], u64); 3] = [
        (
            (1, 1),
            [0xfdcc645eadfda102, 0x2a7341371209acbc, 0xbcd6a95e708d42b3],
            1,
        ),
        (
            (1, 2),
            [0x0b2804e5c63d39ef, 0xb3bdd0f3688baa5a, 0x281def79323288c6],
            2,
        ),
        (
            (7, 42),
            [0xa68db0d40775b4b2, 0xd1f8ac66b948f2a8, 0x344e5428d4f20577],
            2,
        ),
    ];
    let three = nodes(1..=3);
    for ((ns, cid), scores, owner) in table {
        let key = PlacementKey::collection(NamespaceId(ns), CollectionId(cid));
        for (i, score) in scores.into_iter().enumerate() {
            assert_eq!(
                rendezvous_score(&key, i as u64 + 1),
                score,
                "({ns}, {cid}) node {}",
                i + 1
            );
        }
        assert_eq!(owner_id(ns, cid, &three), Some(owner), "({ns}, {cid})");
    }
    // A shard changes the hash input (E61).
    let key = PlacementKey::collection(NamespaceId(1), CollectionId(1));
    let shard = PlacementKey {
        shard: Some(0),
        ..key
    };
    assert_ne!(rendezvous_score(&key, 1), rendezvous_score(&shard, 1));
}

#[test]
fn owners_are_deterministic_and_skip_non_query_nodes() {
    let mut all = nodes(1..=4);
    all.push(node_in(5, "", "meta,log,worker,gateway"));
    let mut reversed = all.clone();
    reversed.reverse();
    for cid in 0..500 {
        let a = owner_id(1, cid, &all);
        assert_eq!(a, owner_id(1, cid, &reversed), "cid {cid}");
        assert_ne!(a, Some(5), "a non-query node owns cid {cid}");
        // The full ranking lists every query node once, in score order.
        let key = PlacementKey::collection(NamespaceId(1), CollectionId(cid));
        let ranked: Vec<u64> = ranking(&key, &all, 1).iter().map(|n| n.node_id).collect();
        assert_eq!(ranked.len(), 4);
        let scores: Vec<u64> = ranked
            .iter()
            .map(|id| rendezvous_score(&key, *id))
            .collect();
        assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{scores:?}");
        assert_eq!(Some(ranked[0]), a);
    }
    assert_eq!(owner_id(1, 1, &[node_in(5, "", "gateway")]), None);
}

#[test]
fn adding_a_node_moves_only_its_keys() {
    let five = nodes(1..=5);
    let six = nodes(1..=6);
    let mut moved = 0;
    for cid in 0..10_000 {
        let before = owner_id(3, cid, &five);
        let after = owner_id(3, cid, &six);
        if before != after {
            assert_eq!(after, Some(6), "cid {cid} moved to an old node");
            moved += 1;
        }
    }
    let share = f64::from(moved) / 10_000.0;
    assert!((0.12..=0.22).contains(&share), "{share}");
}

#[test]
fn removing_a_node_moves_only_its_keys() {
    let six = nodes(1..=6);
    let five: Vec<_> = six.iter().filter(|n| n.node_id != 3).cloned().collect();
    let mut moved = 0;
    for cid in 0..10_000 {
        let before = owner_id(3, cid, &six);
        let after = owner_id(3, cid, &five);
        if before != after {
            assert_eq!(before, Some(3), "cid {cid} moved off a surviving node");
            moved += 1;
        }
    }
    let share = f64::from(moved) / 10_000.0;
    assert!((0.12..=0.22).contains(&share), "{share}");
}

#[test]
fn replicas_spread_across_zones() {
    let six: Vec<_> = (1..=6)
        .map(|id| node_in(id, ["a", "b", "c"][(id % 3) as usize], "query"))
        .collect();
    for cid in 0..1_000 {
        let chosen = owners(NamespaceId(1), CollectionId(cid), &six, 3);
        assert_eq!(chosen.len(), 3);
        let zones: BTreeSet<&str> = chosen.iter().map(|n| n.zone.as_str()).collect();
        assert_eq!(zones.len(), 3, "cid {cid}: {chosen:?}");
        // The first owner is the top-scored node.
        assert_eq!(chosen[0].node_id, owner_id(1, cid, &six).expect("owner"));
    }
    // More replicas than zones: every zone first, then by score.
    let chosen = owners(NamespaceId(1), CollectionId(1), &six, 5);
    let zones: BTreeSet<&str> = chosen[..3].iter().map(|n| n.zone.as_str()).collect();
    assert_eq!((chosen.len(), zones.len()), (5, 3));
}

/// The first cid of namespace 1 that `id` owns among `nodes`.
fn owned_by(id: u64, nodes: &[NodeDescriptor]) -> CollectionId {
    (0..)
        .map(CollectionId)
        .find(|cid| owner_id(1, cid.0, nodes) == Some(id))
        .expect("some cid")
}

#[test]
fn a_suspect_node_is_skipped_until_it_recovers() {
    let all = nodes(1..=3);
    let registry = NodeRegistry::fixed(node(1), all.clone());
    let placement = PlacementImpl::with_suspect_for(registry, 1, Duration::from_millis(300));
    let cid = owned_by(2, &all);
    let remote_2 = Owner::Remote {
        node_id: 2,
        addr: node(2).addr,
    };
    assert_eq!(placement.owner(NamespaceId(1), cid), remote_2);
    placement.mark_unreachable(2);
    assert!(placement.is_suspect(2));
    assert_ne!(placement.owner(NamespaceId(1), cid), remote_2);
    // Its keys go to the next node of the ranking.
    let key = PlacementKey::collection(NamespaceId(1), cid);
    let next = ranking(&key, &all, 1)[1].node_id;
    assert_eq!(placement.owners_of(&key)[0].node_id, next);
    std::thread::sleep(Duration::from_millis(350));
    assert!(!placement.is_suspect(2));
    assert_eq!(placement.owner(NamespaceId(1), cid), remote_2);

    // Every candidate suspect: they are used again.
    placement.mark_unreachable(1);
    placement.mark_unreachable(2);
    placement.mark_unreachable(3);
    assert_eq!(placement.owner(NamespaceId(1), cid), remote_2);
}

#[test]
fn no_query_node_means_local() {
    let me = node_in(1, "", "gateway");
    let registry = NodeRegistry::fixed(me.clone(), vec![me.clone(), node_in(2, "", "worker")]);
    let placement = PlacementImpl::new(registry.clone(), 1);
    for cid in 0..50 {
        assert_eq!(
            placement.owner(NamespaceId(1), CollectionId(cid)),
            Owner::Local
        );
    }
    // A query node joins: a gateway without `query` forwards everything.
    registry.set_live(vec![me, node(3)]);
    assert_eq!(
        placement.owner(NamespaceId(1), CollectionId(7)),
        Owner::Remote {
            node_id: 3,
            addr: node(3).addr
        }
    );
    // A standalone query node owns everything.
    let alone = PlacementImpl::new(NodeRegistry::standalone(node(4)), 1);
    assert_eq!(alone.owner(NamespaceId(1), CollectionId(7)), Owner::Local);
}
