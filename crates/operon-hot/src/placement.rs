//! Placement (plan M1.3 Task 10 rules 2 and 3; Ruling 13; E61, E62): which
//! live `query` node owns a resource, by rendezvous hashing over the node
//! registry. Ownership is a soft hint (§18 §5.3): a read that reaches a node
//! that does not own its collection is served there, cold.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use operon_common::{CollectionId, NamespaceId};
use operon_query::placement::{Owner, Placement};
use xxhash_rust::xxh3::xxh3_64;

use crate::registry::{NodeDescriptor, NodeRegistry};

/// What kind of resource a [`PlacementKey`] names (D75). M1 places only
/// collections; M2 adds stream partitions and consumer groups, M3 graphs,
/// M4 tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ResourceKind {
    Collection,
}

impl ResourceKind {
    /// The byte a non-collection kind (or a sharded key) appends to the hash
    /// input.
    fn tag(self) -> u8 {
        match self {
            ResourceKind::Collection => 0,
        }
    }
}

/// What the one router places (D75, E61): `(ns, kind, id[, shard])`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlacementKey {
    pub ns: NamespaceId,
    pub kind: ResourceKind,
    pub id: u64,
    pub shard: Option<u32>,
}

impl PlacementKey {
    /// The key of a whole collection.
    pub fn collection(ns: NamespaceId, cid: CollectionId) -> Self {
        Self {
            ns,
            kind: ResourceKind::Collection,
            id: cid.0,
            shard: None,
        }
    }
}

/// `xxh3_64(ns BE ‖ id BE ‖ node BE)` for an unsharded collection (Ruling
/// 13's golden scores); any other kind, or a shard, appends the kind byte
/// and the shard (BE; `0` without one).
pub fn rendezvous_score(key: &PlacementKey, node_id: u64) -> u64 {
    let mut input = Vec::with_capacity(29);
    input.extend_from_slice(&key.ns.0.to_be_bytes());
    input.extend_from_slice(&key.id.to_be_bytes());
    input.extend_from_slice(&node_id.to_be_bytes());
    if key.kind != ResourceKind::Collection || key.shard.is_some() {
        input.push(key.kind.tag());
        input.extend_from_slice(&key.shard.unwrap_or(0).to_be_bytes());
    }
    xxh3_64(&input)
}

/// The whole rendezvous ranking of `key` over the `query` nodes of `nodes`
/// (E62): by `(score desc, node_id asc)`, except that the first
/// `replication` places take the best-ranked node of a zone not yet chosen
/// until every zone is used; the rest follow in score order.
pub fn ranking<'a>(
    key: &PlacementKey,
    nodes: &'a [NodeDescriptor],
    replication: usize,
) -> Vec<&'a NodeDescriptor> {
    let mut rest: Vec<(u64, &NodeDescriptor)> = nodes
        .iter()
        .filter(|node| node.roles.query)
        .map(|node| (rendezvous_score(key, node.node_id), node))
        .collect();
    rest.sort_by(|(a, na), (b, nb)| b.cmp(a).then(na.node_id.cmp(&nb.node_id)));
    let zones: BTreeSet<&str> = rest.iter().map(|(_, node)| node.zone.as_str()).collect();
    let mut out = Vec::with_capacity(rest.len());
    let mut used: BTreeSet<&str> = BTreeSet::new();
    while out.len() < replication && !rest.is_empty() {
        let pick = if used.len() < zones.len() {
            rest.iter()
                .position(|(_, node)| !used.contains(node.zone.as_str()))
                .unwrap_or(0)
        } else {
            0
        };
        let (_, node) = rest.remove(pick);
        used.insert(node.zone.as_str());
        out.push(node);
    }
    out.extend(rest.into_iter().map(|(_, node)| node));
    out
}

/// Ruling 13, pure: the first `replication` of [`ranking`] for a collection.
pub fn owners(
    ns: NamespaceId,
    cid: CollectionId,
    nodes: &[NodeDescriptor],
    replication: usize,
) -> Vec<&NodeDescriptor> {
    let mut ranked = ranking(&PlacementKey::collection(ns, cid), nodes, replication);
    ranked.truncate(replication);
    ranked
}

/// How long a node that failed a forward is skipped (Ruling 13).
pub const SUSPECT_FOR: Duration = Duration::from_secs(5);

/// Rendezvous placement over the node registry.
#[derive(Debug)]
pub struct PlacementImpl {
    registry: Arc<NodeRegistry>,
    replication: usize,
    suspects: Mutex<BTreeMap<u64, Instant>>,
    suspect_for: Duration,
}

impl PlacementImpl {
    /// Suspects are skipped for [`SUSPECT_FOR`].
    pub fn new(registry: Arc<NodeRegistry>, replication: usize) -> Self {
        Self::with_suspect_for(registry, replication, SUSPECT_FOR)
    }

    pub fn with_suspect_for(
        registry: Arc<NodeRegistry>,
        replication: usize,
        suspect_for: Duration,
    ) -> Self {
        Self {
            registry,
            replication: replication.max(1),
            suspects: Mutex::new(BTreeMap::new()),
            suspect_for,
        }
    }

    pub fn registry(&self) -> &Arc<NodeRegistry> {
        &self.registry
    }

    /// The owners of a collection (Ruling 13) among the live nodes, suspects
    /// included.
    pub fn owners(&self, ns: NamespaceId, cid: CollectionId) -> Vec<NodeDescriptor> {
        let live = self.registry.live();
        owners(ns, cid, &live, self.replication)
            .into_iter()
            .cloned()
            .collect()
    }

    /// The owners of `key` after skipping suspects (rule 3): the first
    /// `replication` non-suspect nodes of the ranking, or the first
    /// `replication` of the whole ranking when that leaves none.
    pub fn owners_of(&self, key: &PlacementKey) -> Vec<NodeDescriptor> {
        let live = self.registry.live();
        let ranked = ranking(key, &live, self.replication);
        let suspects = self.fresh_suspects();
        let mut chosen: Vec<NodeDescriptor> = ranked
            .iter()
            .filter(|node| !suspects.contains(&node.node_id))
            .take(self.replication)
            .map(|node| (*node).clone())
            .collect();
        if chosen.is_empty() {
            chosen = ranked.into_iter().take(self.replication).cloned().collect();
        }
        chosen
    }

    /// Skips `node_id` for `suspect_for` from now.
    pub fn mark_unreachable(&self, node_id: u64) {
        self.suspects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node_id, Instant::now());
    }

    /// Whether `node_id` is suspect now.
    pub fn is_suspect(&self, node_id: u64) -> bool {
        self.fresh_suspects().contains(&node_id)
    }

    /// The suspects whose mark is younger than `suspect_for`; older marks
    /// are forgotten.
    fn fresh_suspects(&self) -> BTreeSet<u64> {
        let mut suspects = self.suspects.lock().unwrap_or_else(PoisonError::into_inner);
        let suspect_for = self.suspect_for;
        suspects.retain(|_, since| since.elapsed() < suspect_for);
        suspects.keys().copied().collect()
    }

    /// Rule 3 for any key: `Local` if this node is among the owners, or no
    /// query node is live; else the first owner.
    pub fn owner_of(&self, key: &PlacementKey) -> Owner {
        let owners = self.owners_of(key);
        let me = self.registry.me().node_id;
        match owners.first() {
            None => Owner::Local,
            Some(_) if owners.iter().any(|node| node.node_id == me) => Owner::Local,
            Some(first) => Owner::Remote {
                node_id: first.node_id,
                addr: first.addr,
            },
        }
    }
}

impl Placement for PlacementImpl {
    fn owner(&self, ns: NamespaceId, cid: CollectionId) -> Owner {
        self.owner_of(&PlacementKey::collection(ns, cid))
    }
}

/// Every collection is owned by this node: `operon dev` and `standalone`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AlwaysLocal;

impl Placement for AlwaysLocal {
    fn owner(&self, _: NamespaceId, _: CollectionId) -> Owner {
        Owner::Local
    }
}
