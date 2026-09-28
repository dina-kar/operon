//! Budgets (plan M1.3 Task 7 rule 3; Ruling 18): which hot structures a
//! node keeps when NVMe, RAM or the number of open artifacts runs out. Pure
//! functions over [`Resident`] lists; the tier applies their plans.
//!
//! A candidate `X` may evict `Y` iff `Y.class < X.class`, or the classes are
//! equal, `Y`'s namespace is over its fair share or is `X`'s, and `Y`'s heat
//! per byte is below `X`'s. Evictions are taken in the order (class, over
//! share first, heat per byte, key), skipping those that free nothing still
//! short, until `X` fits; when it cannot fit, nothing is evicted and the
//! answer is [`TierError::OverBudget`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use operon_common::{CollectionId, NamespaceId};

use crate::TierError;

/// Why a structure is hot. `Promoted < Pinned`: a promoted structure is
/// evicted before any pinned one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HotClass {
    /// Promoted by heat (a promotion lease, or `warm`).
    Promoted,
    /// Pinned by the catalog configuration or `--hot-pin-all`.
    Pinned,
}

/// A kind of budgeted structure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StructureKind {
    /// A loaded HNSW artifact (counts against `max_artifacts`).
    Hnsw,
    /// A pinned split.
    Split,
    /// A delta index.
    Delta,
}

/// One budgeted structure, resident or asking to be.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resident {
    pub namespace: NamespaceId,
    pub collection: CollectionId,
    pub kind: StructureKind,
    /// The column of an artifact or delta, the ULID of a split.
    pub id: String,
    pub nvme_bytes: u64,
    pub ram_bytes: u64,
    pub class: HotClass,
    /// The collection's heat; the tier raises a pinned structure's heat to
    /// at least `promote_min_hits`.
    pub heat: u32,
}

/// What a node may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budget {
    pub nvme_bytes: u64,
    pub ram_bytes: u64,
    /// The most [`StructureKind::Hnsw`] residents (Ruling 18).
    pub max_artifacts: usize,
}

impl Resident {
    fn bytes(&self) -> u128 {
        u128::from(self.nvme_bytes) + u128::from(self.ram_bytes)
    }

    fn key(&self) -> (NamespaceId, CollectionId, StructureKind, &str) {
        (self.namespace, self.collection, self.kind, &self.id)
    }

    fn artifacts(&self) -> usize {
        usize::from(self.kind == StructureKind::Hnsw)
    }
}

/// `hpb(a)` against `hpb(b)`, where `hpb = heat / max(1, nvme + ram)`,
/// compared exactly by cross-multiplication.
fn hpb_cmp(a: &Resident, b: &Resident) -> Ordering {
    let left = u128::from(a.heat) * b.bytes().max(1);
    let right = u128::from(b.heat) * a.bytes().max(1);
    left.cmp(&right)
}

/// NVMe bytes, RAM bytes and artifacts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Totals {
    nvme: u128,
    ram: u128,
    artifacts: usize,
}

impl Totals {
    fn of<'a>(items: impl IntoIterator<Item = &'a Resident>) -> Self {
        let mut totals = Totals::default();
        for item in items {
            totals.add(item);
        }
        totals
    }

    fn add(&mut self, item: &Resident) {
        self.nvme += u128::from(item.nvme_bytes);
        self.ram += u128::from(item.ram_bytes);
        self.artifacts += item.artifacts();
    }

    fn sub(&mut self, item: &Resident) {
        self.nvme -= u128::from(item.nvme_bytes);
        self.ram -= u128::from(item.ram_bytes);
        self.artifacts -= item.artifacts();
    }

    fn fits(&self, budget: &Budget) -> bool {
        self.nvme <= u128::from(budget.nvme_bytes)
            && self.ram <= u128::from(budget.ram_bytes)
            && self.artifacts <= budget.max_artifacts
    }

    /// Whether evicting `item` frees something `budget` is still short of.
    fn helps(&self, item: &Resident, budget: &Budget) -> bool {
        (self.nvme > u128::from(budget.nvme_bytes) && item.nvme_bytes > 0)
            || (self.ram > u128::from(budget.ram_bytes) && item.ram_bytes > 0)
            || (self.artifacts > budget.max_artifacts && item.artifacts() > 0)
    }
}

/// The namespaces over their fair share (`budget / n` of NVMe or of RAM,
/// with `n` the namespaces of `resident` and `extra`).
fn over_share(
    resident: &[Resident],
    extra: Option<&Resident>,
    budget: &Budget,
) -> BTreeSet<NamespaceId> {
    let mut usage: BTreeMap<NamespaceId, Totals> = BTreeMap::new();
    for item in resident {
        usage.entry(item.namespace).or_default().add(item);
    }
    let mut namespaces: BTreeSet<NamespaceId> = usage.keys().copied().collect();
    if let Some(extra) = extra {
        namespaces.insert(extra.namespace);
    }
    let n = namespaces.len().max(1) as u128;
    let (nvme_share, ram_share) = (
        u128::from(budget.nvme_bytes) / n,
        u128::from(budget.ram_bytes) / n,
    );
    usage
        .into_iter()
        .filter(|(_, totals)| totals.nvme > nvme_share || totals.ram > ram_share)
        .map(|(ns, _)| ns)
        .collect()
}

/// Rule 3's eviction order over `candidates` (indexes into `resident`).
fn eviction_order(resident: &[Resident], candidates: &mut [usize], over: &BTreeSet<NamespaceId>) {
    candidates.sort_by(|&a, &b| {
        let (ya, yb) = (&resident[a], &resident[b]);
        ya.class
            .cmp(&yb.class)
            .then_with(|| {
                over.contains(&yb.namespace)
                    .cmp(&over.contains(&ya.namespace))
            })
            .then_with(|| hpb_cmp(ya, yb))
            .then_with(|| ya.key().cmp(&yb.key()))
    });
}

/// Whether `candidate` may evict `victim` (rule 3), given the namespaces
/// over their share.
pub fn may_evict(victim: &Resident, candidate: &Resident, over: &BTreeSet<NamespaceId>) -> bool {
    match victim.class.cmp(&candidate.class) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => {
            (over.contains(&victim.namespace) || victim.namespace == candidate.namespace)
                && hpb_cmp(victim, candidate) == Ordering::Less
        }
    }
}

/// The namespaces of `resident` over their share when `candidate` asks to
/// join (what [`may_evict`] is judged against by [`plan_admission`]).
pub fn over_share_with(
    resident: &[Resident],
    candidate: &Resident,
    budget: &Budget,
) -> BTreeSet<NamespaceId> {
    over_share(resident, Some(candidate), budget)
}

/// Rule 3: indexes of `resident` to evict so that `candidate` fits, or `Err(TierError::OverBudget)`.
pub fn plan_admission(
    resident: &[Resident],
    candidate: &Resident,
    budget: &Budget,
) -> Result<Vec<usize>, TierError> {
    let mut totals = Totals::of(resident);
    totals.add(candidate);
    if totals.fits(budget) {
        return Ok(Vec::new());
    }
    let over_budget = || {
        TierError::OverBudget(format!(
            "{} {}/{} {} ({} NVMe bytes, {} RAM bytes) does not fit",
            match candidate.kind {
                StructureKind::Hnsw => "artifact",
                StructureKind::Split => "split",
                StructureKind::Delta => "delta",
            },
            candidate.namespace,
            candidate.collection,
            candidate.id,
            candidate.nvme_bytes,
            candidate.ram_bytes
        ))
    };
    if !Totals::of([candidate]).fits(budget) {
        return Err(over_budget());
    }
    let over = over_share(resident, Some(candidate), budget);
    let mut order: Vec<usize> = (0..resident.len())
        .filter(|&i| may_evict(&resident[i], candidate, &over))
        .collect();
    eviction_order(resident, &mut order, &over);
    let mut evicted = Vec::new();
    for i in order {
        if totals.fits(budget) {
            break;
        }
        if totals.helps(&resident[i], budget) {
            totals.sub(&resident[i]);
            evicted.push(i);
        }
    }
    match totals.fits(budget) {
        true => Ok(evicted),
        false => Err(over_budget()),
    }
}

/// Rule 3: evictions that bring `resident` back under `budget` (after a budget change).
pub fn plan_shrink(resident: &[Resident], budget: &Budget) -> Vec<usize> {
    let mut totals = Totals::of(resident);
    if totals.fits(budget) {
        return Vec::new();
    }
    let over = over_share(resident, None, budget);
    let mut order: Vec<usize> = (0..resident.len()).collect();
    eviction_order(resident, &mut order, &over);
    let mut evicted = Vec::new();
    for i in order {
        if totals.fits(budget) {
            break;
        }
        if totals.helps(&resident[i], budget) {
            totals.sub(&resident[i]);
            evicted.push(i);
        }
    }
    evicted
}
