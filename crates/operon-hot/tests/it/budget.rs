//! Budgets and heat (plan M1.3 Task 7 rules 3 and 4): pure admission and
//! shrink plans, and the count-min heat sketch.

use std::collections::BTreeMap;

use operon_common::{CollectionId, NamespaceId};
use operon_hot::{
    Budget, HeatSketch, HotClass, Resident, StructureKind, TierError, may_evict, over_share_with,
    plan_admission, plan_shrink,
};
use proptest::prelude::*;

fn item(ns: u64, cid: u64, kind: StructureKind, id: &str, nvme: u64, heat: u32) -> Resident {
    Resident {
        namespace: NamespaceId(ns),
        collection: CollectionId(cid),
        kind,
        id: id.to_string(),
        nvme_bytes: nvme,
        ram_bytes: 0,
        class: HotClass::Pinned,
        heat,
    }
}

fn split(ns: u64, cid: u64, id: &str, nvme: u64, heat: u32) -> Resident {
    item(ns, cid, StructureKind::Split, id, nvme, heat)
}

fn budget(nvme: u64) -> Budget {
    Budget {
        nvme_bytes: nvme,
        ram_bytes: u64::MAX,
        max_artifacts: 32,
    }
}

fn ids(resident: &[Resident], plan: &[usize]) -> Vec<String> {
    plan.iter().map(|&i| resident[i].id.clone()).collect()
}

#[test]
fn lowest_heat_per_byte_is_demoted_first() {
    // One namespace: every resident may be evicted by a hotter candidate.
    let resident = vec![
        split(1, 1, "a", 100, 50), // 0.5 per byte
        split(1, 2, "b", 100, 10), // 0.1
        split(1, 3, "c", 50, 10),  // 0.2
    ];
    let candidate = split(1, 4, "x", 120, 100);
    let plan = plan_admission(&resident, &candidate, &budget(300)).expect("fits");
    // 250 + 120 = 370: 70 bytes must go; b (lowest) frees 100.
    assert_eq!(ids(&resident, &plan), ["b"]);
    let plan =
        plan_admission(&resident, &split(1, 4, "x", 200, 1_000), &budget(300)).expect("fits");
    // 450: 150 must go: b, then c.
    assert_eq!(ids(&resident, &plan), ["b", "c"]);
    // A colder candidate evicts nothing and does not fit.
    let cold = split(1, 4, "x", 200, 1);
    assert!(matches!(
        plan_admission(&resident, &cold, &budget(300)),
        Err(TierError::OverBudget(_))
    ));
}

#[test]
fn a_namespace_over_its_share_is_demoted_before_others() {
    // Three namespaces (the candidate's included) share 400 bytes: 133
    // each. Namespace 1 holds 300.
    let resident = vec![
        split(1, 1, "a", 150, 10),
        split(1, 2, "b", 150, 10),
        split(2, 3, "c", 50, 1), // colder, but within its share
    ];
    let candidate = split(3, 4, "x", 100, 100);
    let over = over_share_with(&resident, &candidate, &budget(400));
    assert!(over.contains(&NamespaceId(1)) && !over.contains(&NamespaceId(2)));
    let plan = plan_admission(&resident, &candidate, &budget(400)).expect("fits");
    // 450 → 400: namespace 1 goes first although c is colder per byte.
    assert_eq!(ids(&resident, &plan), ["a"]);
    // c is in another namespace, within its share: never evicted by x.
    assert!(!may_evict(&resident[2], &candidate, &over));
}

#[test]
fn promoted_structures_go_before_pinned_ones() {
    let mut hot_promoted = split(1, 1, "promoted", 100, 1_000);
    hot_promoted.class = HotClass::Promoted;
    let resident = vec![split(1, 2, "pinned", 100, 1), hot_promoted];
    let candidate = split(1, 3, "x", 100, 2);
    let plan = plan_admission(&resident, &candidate, &budget(200)).expect("fits");
    assert_eq!(ids(&resident, &plan), ["promoted"]);
    // A promoted candidate never evicts a pinned structure.
    let mut promoted = split(1, 3, "x", 150, u32::MAX);
    promoted.class = HotClass::Promoted;
    let only_pinned = vec![split(1, 2, "pinned", 100, 1)];
    assert!(plan_admission(&only_pinned, &promoted, &budget(200)).is_err());
    assert!(HotClass::Promoted < HotClass::Pinned);
}

#[test]
fn an_oversized_pin_reports_over_budget() {
    let resident = vec![split(1, 1, "a", 10, 1)];
    let huge = split(1, 2, "x", 1_001, u32::MAX);
    match plan_admission(&resident, &huge, &budget(1_000)) {
        Err(TierError::OverBudget(message)) => assert!(message.contains("x"), "{message}"),
        other => panic!("{other:?}"),
    }
    // Nothing is evicted when a candidate cannot fit even after every
    // allowed eviction.
    let resident = vec![split(1, 1, "a", 500, 1), split(1, 2, "b", 400, u32::MAX)];
    let big = split(1, 3, "x", 700, 1_000);
    assert!(plan_admission(&resident, &big, &budget(1_000)).is_err());
}

#[test]
fn max_loaded_artifacts_is_enforced() {
    let limit = Budget {
        nvme_bytes: u64::MAX,
        ram_bytes: u64::MAX,
        max_artifacts: 2,
    };
    let resident = vec![
        item(1, 1, StructureKind::Hnsw, "_vector_0", 10, 5),
        item(1, 2, StructureKind::Hnsw, "_vector_0", 10, 50),
        item(1, 1, StructureKind::Delta, "_vector_0", 1, 1),
        split(1, 1, "s", 1, 1),
    ];
    let candidate = item(1, 3, StructureKind::Hnsw, "_vector_0", 10, 20);
    let plan = plan_admission(&resident, &candidate, &limit).expect("fits");
    // Only an artifact frees a slot: the coldest artifact goes, not the
    // colder delta or split.
    assert_eq!(plan, vec![0]);
    // Deltas and splits do not count against the limit.
    let delta = item(1, 3, StructureKind::Delta, "_vector_0", 10, 1);
    assert_eq!(
        plan_admission(&resident, &delta, &limit).expect("fits"),
        Vec::<usize>::new()
    );
    let none = Budget {
        max_artifacts: 0,
        ..limit
    };
    assert!(plan_admission(&[], &candidate, &none).is_err());
    assert_eq!(
        plan_shrink(
            &resident,
            &Budget {
                max_artifacts: 1,
                ..limit
            }
        ),
        vec![0]
    );
}

#[test]
fn plan_shrink_restores_the_budget() {
    let mut promoted = split(2, 9, "p", 100, 1_000);
    promoted.class = HotClass::Promoted;
    let resident = vec![
        split(1, 1, "a", 100, 10),
        split(1, 2, "b", 100, 50),
        promoted,
        split(2, 3, "c", 100, 20),
    ];
    assert!(plan_shrink(&resident, &budget(400)).is_empty());
    let plan = plan_shrink(&resident, &budget(250));
    // Promoted first, then the coldest per byte among the pinned.
    assert_eq!(ids(&resident, &plan), ["p", "a"]);
    let everything = plan_shrink(&resident, &budget(0));
    assert_eq!(everything.len(), resident.len());
    let ram = Budget {
        nvme_bytes: u64::MAX,
        ram_bytes: 0,
        max_artifacts: 32,
    };
    // Nothing holds RAM: nothing to evict for it.
    assert!(plan_shrink(&resident, &ram).is_empty());
}

fn arb_resident() -> impl Strategy<Value = Resident> {
    (
        1u64..4,
        1u64..6,
        0u8..3,
        0u64..200,
        0u64..100,
        any::<bool>(),
        0u32..300,
        0u32..1_000,
    )
        .prop_map(|(ns, cid, kind, nvme, ram, pinned, heat, id)| Resident {
            namespace: NamespaceId(ns),
            collection: CollectionId(cid),
            kind: match kind {
                0 => StructureKind::Hnsw,
                1 => StructureKind::Split,
                _ => StructureKind::Delta,
            },
            id: format!("s{id}"),
            nvme_bytes: nvme,
            ram_bytes: ram,
            class: match pinned {
                true => HotClass::Pinned,
                false => HotClass::Promoted,
            },
            heat,
        })
}

fn totals(items: &[&Resident]) -> (u64, u64, usize) {
    items.iter().fold((0, 0, 0), |(n, r, a), item| {
        (
            n + item.nvme_bytes,
            r + item.ram_bytes,
            a + usize::from(item.kind == StructureKind::Hnsw),
        )
    })
}

proptest! {
    #[test]
    fn admission_never_exceeds_the_budget(
        resident in proptest::collection::vec(arb_resident(), 0..12),
        candidate in arb_resident(),
        nvme in 0u64..1_500,
        ram in 0u64..800,
        max_artifacts in 0usize..5,
    ) {
        let budget = Budget { nvme_bytes: nvme, ram_bytes: ram, max_artifacts };
        // Start from a resident set that fits.
        let shrink = plan_shrink(&resident, &budget);
        let resident: Vec<Resident> = resident
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !shrink.contains(i))
            .map(|(_, r)| r)
            .collect();
        let before = totals(&resident.iter().collect::<Vec<_>>());
        prop_assert!(before.0 <= nvme && before.1 <= ram && before.2 <= max_artifacts);
        let over = over_share_with(&resident, &candidate, &budget);
        match plan_admission(&resident, &candidate, &budget) {
            Ok(plan) => {
                let mut kept: Vec<&Resident> = resident
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !plan.contains(i))
                    .map(|(_, r)| r)
                    .collect();
                kept.push(&candidate);
                let (n, r, a) = totals(&kept);
                prop_assert!(n <= nvme && r <= ram && a <= max_artifacts);
                for &i in &plan {
                    prop_assert!(may_evict(&resident[i], &candidate, &over), "{:?} evicted {:?}", candidate, resident[i]);
                }
                let mut unique = plan.clone();
                unique.sort_unstable();
                unique.dedup();
                prop_assert_eq!(unique.len(), plan.len());
            }
            Err(err) => {
                prop_assert!(matches!(err, TierError::OverBudget(_)));
                // Even evicting everything it may evict would not do.
                let kept: Vec<&Resident> = resident
                    .iter()
                    .filter(|r| !may_evict(r, &candidate, &over))
                    .chain(std::iter::once(&candidate))
                    .collect();
                let (n, r, a) = totals(&kept);
                prop_assert!(n > nvme || r > ram || a > max_artifacts);
            }
        }
    }

    #[test]
    fn the_sketch_never_undercounts(hits in proptest::collection::vec((0u64..3, 0u64..40), 0..600)) {
        let sketch = HeatSketch::new();
        let mut exact: BTreeMap<(u64, u64), u32> = BTreeMap::new();
        for (ns, cid) in &hits {
            sketch.record(NamespaceId(*ns), CollectionId(*cid));
            *exact.entry((*ns, *cid)).or_default() += 1;
        }
        for ((ns, cid), count) in exact {
            let estimate = sketch.estimate(NamespaceId(ns), CollectionId(cid));
            prop_assert!(estimate >= count.min(255), "{ns}/{cid}: {estimate} < {count}");
        }
    }
}

#[test]
fn decay_halves_estimates() {
    let sketch = HeatSketch::new();
    let (ns, cid) = (NamespaceId(1), CollectionId(7));
    for _ in 0..100 {
        sketch.record(ns, cid);
    }
    assert_eq!(sketch.estimate(ns, cid), 100);
    sketch.decay();
    assert_eq!(sketch.estimate(ns, cid), 50);
    sketch.decay();
    assert_eq!(sketch.estimate(ns, cid), 25);
    for _ in 0..8 {
        sketch.decay();
    }
    assert_eq!(sketch.estimate(ns, cid), 0);
    // Saturates at 255 instead of wrapping.
    for _ in 0..300 {
        sketch.record(ns, cid);
    }
    assert_eq!(sketch.estimate(ns, cid), 255);
    assert_eq!(sketch.estimate(NamespaceId(1), CollectionId(8)), 0);
}
