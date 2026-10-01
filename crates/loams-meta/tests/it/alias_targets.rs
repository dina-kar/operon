//! Aliases with several members (M1.5 Task 0a, Rulings 9 and 22), on
//! `MetaState` directly: `UpdateAliasTargets`, the changed M1.1 commands,
//! the two alias maps and their queries.

use loams_common::schema::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec};
use loams_common::{CollectionId, NamespaceId};
use loams_meta::{
    AliasAction, AliasTargetAction, AliasTargets, ApplyError, Command, MetaState, NameTarget,
    Reply, snapshot_bytes, snapshot_round_trip,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const NS: NamespaceId = NamespaceId(1);

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
        Vec::new(),
        DynamicMapping::Ignore,
    )
}

fn create_command(name: &str) -> Command {
    Command::CreateCollection {
        namespace: NS,
        name: name.to_string(),
        schema: schema(),
        partitions: 1,
    }
}

/// Namespace 1 with the collections `names`, ids 1, 2, … in order.
fn state_with(names: &[&str]) -> MetaState {
    let mut state = MetaState::default();
    state
        .apply(Command::CreateNamespace {
            name: "acme".to_string(),
        })
        .expect("namespace");
    for name in names {
        state.apply(create_command(name)).expect("collection");
    }
    state
}

fn add(alias: &str, collection: &str, is_write_index: Option<bool>) -> AliasTargetAction {
    AliasTargetAction::Add {
        alias: alias.to_string(),
        collection: collection.to_string(),
        is_write_index,
    }
}

fn remove(alias: &str, collection: &str) -> AliasTargetAction {
    AliasTargetAction::Remove {
        alias: alias.to_string(),
        collection: collection.to_string(),
    }
}

fn remove_alias(alias: &str) -> AliasTargetAction {
    AliasTargetAction::RemoveAlias {
        alias: alias.to_string(),
    }
}

fn targets(actions: Vec<AliasTargetAction>) -> Command {
    Command::UpdateAliasTargets {
        namespace: NS,
        actions,
    }
}

fn drop_command(name: &str) -> Command {
    Command::DropCollection {
        namespace: NS,
        name: name.to_string(),
        now_ms: 0,
    }
}

fn apply(state: &mut MetaState, command: Command) {
    assert_eq!(
        state.apply(command.clone()),
        Ok(Reply::AliasesUpdated),
        "{command}"
    );
    assert_eq!(state.check_invariants(), Vec::<String>::new(), "{command}");
}

fn members(pairs: &[(u64, Option<bool>)]) -> AliasTargets {
    AliasTargets {
        members: pairs
            .iter()
            .map(|(id, w)| (CollectionId(*id), *w))
            .collect(),
    }
}

/// An alias's members as `(collection name, setting)` and its write target id.
type Resolved = (Vec<(String, Option<bool>)>, Option<u64>);

/// The alias's members and write target through `resolve_name`.
fn resolved(state: &MetaState, alias: &str) -> Option<Resolved> {
    match state.resolve_name(NS, alias)? {
        NameTarget::Alias {
            members,
            write_target,
        } => Some((
            members.into_iter().map(|(c, w)| (c.name, w)).collect(),
            write_target.map(|id| id.0),
        )),
        NameTarget::Collection(c) => panic!("{alias} resolved to collection {}", c.name),
    }
}

fn named(pairs: &[(&str, Option<bool>)]) -> Vec<(String, Option<bool>)> {
    pairs.iter().map(|(n, w)| (n.to_string(), *w)).collect()
}

/// The version in a snapshot's header.
fn snapshot_version(state: &MetaState) -> u32 {
    let bytes = snapshot_bytes(state).expect("snapshot");
    u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]])
}

/// Whether `alias` is in the M1.1 map: it resolves through
/// `resolve_collection` and snapshots do not need format 7 for it.
fn in_m1_1_map(state: &MetaState, alias: &str) -> bool {
    let mut probe = state.clone();
    // Drop every other alias so the snapshot version speaks for this one.
    let others: Vec<String> = state
        .alias_targets(NS)
        .map(|(a, _)| a.to_string())
        .filter(|a| a != alias)
        .collect();
    for other in others {
        probe
            .apply(targets(vec![remove_alias(&other)]))
            .expect("remove");
    }
    probe.resolve_name(NS, alias).is_some() && snapshot_version(&probe) == 5
}

/// The LangChain cache fixture `es_env_fx` and what `test_cache.py` does
/// with it (Ruling 9).
#[test]
fn alias_targets_follow_the_langchain_cache_fixture() {
    let mut state = state_with(&["test_index1", "test_index2", "test_index3"]);
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index1", None)]),
    );
    assert!(in_m1_1_map(&state, "test_alias"));
    assert_eq!(
        resolved(&state, "test_alias"),
        Some((named(&[("test_index1", None)]), Some(1)))
    );
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index2", Some(true))]),
    );
    assert_eq!(snapshot_version(&state), 7);
    assert_eq!(
        resolved(&state, "test_alias"),
        Some((
            named(&[("test_index1", None), ("test_index2", Some(true))]),
            Some(2)
        ))
    );
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index2", Some(false))]),
    );
    assert_eq!(resolved(&state, "test_alias").unwrap().1, None);
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index1", Some(true))]),
    );
    assert_eq!(resolved(&state, "test_alias").unwrap().1, Some(1));
    apply(
        &mut state,
        targets(vec![remove("test_alias", "test_index2")]),
    );
    // One member set to true stays in the multi-target map.
    assert_eq!(
        state.alias_targets(NS).collect::<Vec<_>>(),
        vec![("test_alias", members(&[(1, Some(true))]))]
    );
    assert_eq!(snapshot_version(&state), 7);
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index2", None)]),
    );
    apply(
        &mut state,
        targets(vec![add("test_alias", "test_index3", None)]),
    );
    assert_eq!(
        resolved(&state, "test_alias"),
        Some((
            named(&[
                ("test_index1", Some(true)),
                ("test_index2", None),
                ("test_index3", None)
            ]),
            Some(1)
        ))
    );
}

#[test]
fn a_single_unset_member_lives_in_the_m1_1_map() {
    let mut state = state_with(&["a", "b"]);
    // Through either command.
    apply(&mut state, targets(vec![add("x", "a", None)]));
    state
        .apply(Command::UpdateAliases {
            namespace: NS,
            actions: vec![AliasAction::Create {
                alias: "y".to_string(),
                collection: "a".to_string(),
            }],
        })
        .expect("update aliases");
    assert!(in_m1_1_map(&state, "x"));
    assert!(in_m1_1_map(&state, "y"));
    assert_eq!(snapshot_version(&state), 5);
    // And after a `Remove` leaves one unset member.
    apply(&mut state, targets(vec![add("x", "b", Some(false))]));
    assert_eq!(snapshot_version(&state), 7);
    apply(&mut state, targets(vec![remove("x", "b")]));
    assert!(in_m1_1_map(&state, "x"));
    assert_eq!(snapshot_version(&state), 5);
    // A setting of false on the only member is not the M1.1 form.
    apply(&mut state, targets(vec![add("x", "a", Some(false))]));
    assert_eq!(snapshot_version(&state), 7);
    assert_eq!(resolved(&state, "x").unwrap().1, None);
}

#[test]
fn two_write_targets_are_refused_and_nothing_changes() {
    let mut state = state_with(&["b", "a"]);
    apply(&mut state, targets(vec![add("x", "a", None)]));
    let before = state.clone();
    assert_eq!(
        state.apply(targets(vec![
            add("x", "b", Some(true)),
            add("x", "a", Some(true)),
        ])),
        Err(ApplyError::InvalidArgument(
            "alias [x] has more than one write index [a,b]".to_string()
        ))
    );
    assert_eq!(state, before);
    // A later action of the same command can clear the conflict.
    apply(
        &mut state,
        targets(vec![
            add("x", "b", Some(true)),
            add("x", "a", Some(true)),
            add("x", "b", None),
        ]),
    );
    assert_eq!(resolved(&state, "x").unwrap().1, Some(2));
}

#[test]
fn too_many_members_is_invalid() {
    let names: Vec<String> = (0..101).map(|i| format!("c{i:03}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut state = state_with(&refs);
    apply(
        &mut state,
        targets(refs[..100].iter().map(|c| add("x", c, None)).collect()),
    );
    let before = state.clone();
    assert_eq!(
        state.apply(targets(vec![add("x", refs[100], None)])),
        Err(ApplyError::InvalidArgument(
            "alias [x] would name 101 collections; the limit is 100".to_string()
        ))
    );
    assert_eq!(state, before);
    // More than 100 actions is the `UpdateAliases` limit and wording.
    assert_eq!(
        state.apply(targets(refs.iter().map(|c| add("y", c, None)).collect())),
        Err(ApplyError::InvalidArgument(
            "an alias update takes 1..=100 actions, got 101".to_string()
        ))
    );
    assert!(matches!(
        state.apply(targets(Vec::new())),
        Err(ApplyError::InvalidArgument(_))
    ));
}

#[test]
fn add_names_collections_not_aliases() {
    let mut state = state_with(&["a", "b"]);
    apply(&mut state, targets(vec![add("x", "a", None)]));
    let before = state.clone();
    assert_eq!(
        state.apply(targets(vec![add("y", "x", None)])),
        Err(ApplyError::UnknownCollection("x".to_string()))
    );
    assert_eq!(
        state.apply(targets(vec![add("b", "a", None)])),
        Err(ApplyError::NameTaken("b".to_string()))
    );
    for reserved in ["_y", "bad/name", ""] {
        assert!(
            matches!(
                state.apply(targets(vec![add(reserved, "a", None)])),
                Err(ApplyError::InvalidArgument(_))
            ),
            "{reserved:?}"
        );
    }
    assert_eq!(
        state.apply(Command::UpdateAliasTargets {
            namespace: NamespaceId(9),
            actions: vec![add("y", "a", None)],
        }),
        Err(ApplyError::NamespaceNotFound(NamespaceId(9)))
    );
    assert_eq!(state, before);
}

#[test]
fn update_aliases_create_repoints_a_multi_target_alias_to_one_collection() {
    let mut state = state_with(&["a", "b", "c"]);
    apply(
        &mut state,
        targets(vec![add("x", "a", None), add("x", "b", Some(true))]),
    );
    state
        .apply(Command::UpdateAliases {
            namespace: NS,
            actions: vec![AliasAction::Create {
                alias: "x".to_string(),
                collection: "c".to_string(),
            }],
        })
        .expect("re-point");
    assert_eq!(state.check_invariants(), Vec::<String>::new());
    assert_eq!(
        state.resolve_collection(NS, "x").map(|c| c.name.as_str()),
        Some("c")
    );
    assert!(in_m1_1_map(&state, "x"));
    assert_eq!(snapshot_version(&state), 5);
}

#[test]
fn update_aliases_delete_removes_a_multi_target_alias() {
    let mut state = state_with(&["a", "b"]);
    apply(
        &mut state,
        targets(vec![add("x", "a", None), add("x", "b", None)]),
    );
    state
        .apply(Command::UpdateAliases {
            namespace: NS,
            actions: vec![AliasAction::Delete {
                alias: "x".to_string(),
            }],
        })
        .expect("delete");
    assert_eq!(state.check_invariants(), Vec::<String>::new());
    assert_eq!(state.resolve_name(NS, "x"), None);
    assert_eq!(state.aliases(NS).count(), 0);
}

#[test]
fn drop_removes_the_collection_from_every_alias() {
    let mut state = state_with(&["a", "b", "c"]);
    apply(
        &mut state,
        targets(vec![
            // Dropping b leaves a, unset: the M1.1 map, a is the write target.
            add("p", "a", None),
            add("p", "b", Some(true)),
            // Dropping b leaves nothing: the alias goes.
            add("q", "b", None),
            // Dropping b leaves a set to false: no write target.
            add("r", "a", Some(false)),
            add("r", "b", Some(true)),
            // Dropping b leaves two members.
            add("s", "a", None),
            add("s", "b", None),
            add("s", "c", None),
        ]),
    );
    state.apply(drop_command("b")).expect("drop");
    assert_eq!(state.check_invariants(), Vec::<String>::new());
    assert!(in_m1_1_map(&state, "p"));
    assert_eq!(
        resolved(&state, "p"),
        Some((named(&[("a", None)]), Some(1)))
    );
    assert_eq!(resolved(&state, "q"), None);
    assert_eq!(
        resolved(&state, "r"),
        Some((named(&[("a", Some(false))]), None))
    );
    assert_eq!(
        resolved(&state, "s"),
        Some((named(&[("a", None), ("c", None)]), None))
    );
    // Dropping a's last aliases' members.
    state.apply(drop_command("a")).expect("drop");
    state.apply(drop_command("c")).expect("drop");
    assert_eq!(state.check_invariants(), Vec::<String>::new());
    assert_eq!(state.aliases(NS).count(), 0);
    assert_eq!(snapshot_version(&state), 5);
}

#[test]
fn create_collection_refuses_a_multi_target_alias_name() {
    let mut state = state_with(&["a", "b"]);
    apply(
        &mut state,
        targets(vec![add("x", "a", None), add("x", "b", None)]),
    );
    let before = state.clone();
    assert_eq!(
        state.apply(create_command("x")),
        Err(ApplyError::NameTaken("x".to_string()))
    );
    assert_eq!(state, before);
}

#[test]
fn remove_of_a_missing_alias_or_member_is_a_no_op() {
    let mut state = state_with(&["a", "b"]);
    apply(&mut state, targets(vec![add("x", "a", None)]));
    let before = state.clone();
    apply(
        &mut state,
        targets(vec![
            remove("nope", "a"),
            remove("x", "b"),
            remove("x", "missing"),
            remove_alias("nope"),
        ]),
    );
    assert_eq!(state, before);
    // Removing and re-adding in one command is idempotent too.
    apply(
        &mut state,
        targets(vec![remove_alias("x"), add("x", "a", None)]),
    );
    assert_eq!(state, before);
}

#[test]
fn resolve_name_aliases_and_resolve_collection_cover_both_maps() {
    let mut state = state_with(&["a", "b"]);
    apply(
        &mut state,
        targets(vec![
            add("one", "b", None),
            add("set", "a", Some(true)),
            add("many", "b", None),
            add("many", "a", Some(false)),
        ]),
    );
    // A collection name.
    assert!(matches!(
        state.resolve_name(NS, "a"),
        Some(NameTarget::Collection(c)) if c.id == CollectionId(1)
    ));
    assert_eq!(state.resolve_name(NS, "nothing"), None);
    assert_eq!(state.resolve_name(NamespaceId(2), "a"), None);
    // resolve_collection: a collection, or an alias's only member.
    let resolve = |name: &str| state.resolve_collection(NS, name).map(|c| c.id.0);
    assert_eq!(resolve("a"), Some(1));
    assert_eq!(resolve("one"), Some(2));
    assert_eq!(resolve("set"), Some(1));
    assert_eq!(resolve("many"), None);
    // Members by id, both maps.
    assert_eq!(
        resolved(&state, "many"),
        Some((named(&[("a", Some(false)), ("b", None)]), None))
    );
    assert_eq!(
        state.aliases(NS).collect::<Vec<_>>(),
        vec![
            ("many", CollectionId(1)),
            ("many", CollectionId(2)),
            ("one", CollectionId(2)),
            ("set", CollectionId(1)),
        ]
    );
    assert_eq!(
        state.alias_targets(NS).collect::<Vec<_>>(),
        vec![
            ("many", members(&[(1, Some(false)), (2, None)])),
            ("one", members(&[(2, None)])),
            ("set", members(&[(1, Some(true))])),
        ]
    );
}

const COLLECTION_NAMES: [&str; 4] = ["c0", "c1", "c2", "c3"];
const ALIAS_NAMES: [&str; 5] = ["a0", "a1", "a2", "a3", "c0"];

fn random_command(rng: &mut ChaCha8Rng) -> Command {
    let collection = |rng: &mut ChaCha8Rng| COLLECTION_NAMES[rng.random_range(0..4)].to_string();
    let alias = |rng: &mut ChaCha8Rng| ALIAS_NAMES[rng.random_range(0..5)].to_string();
    match rng.random_range(0..10u32) {
        0 | 1 => create_command(&collection(rng)),
        2 => drop_command(&collection(rng)),
        3 | 4 => Command::UpdateAliases {
            namespace: NS,
            actions: (0..rng.random_range(1..=2))
                .map(|_| match rng.random_bool(0.7) {
                    true => AliasAction::Create {
                        alias: alias(rng),
                        collection: collection(rng),
                    },
                    false => AliasAction::Delete { alias: alias(rng) },
                })
                .collect(),
        },
        _ => targets(
            (0..rng.random_range(1..=4))
                .map(|_| match rng.random_range(0..10u32) {
                    0..=5 => {
                        let is_write_index = match rng.random_range(0..3u32) {
                            0 => None,
                            1 => Some(false),
                            _ => Some(true),
                        };
                        add(&alias(rng), &collection(rng), is_write_index)
                    }
                    6..=8 => remove(&alias(rng), &collection(rng)),
                    _ => remove_alias(&alias(rng)),
                })
                .collect(),
        ),
    }
}

/// Seeded random commands over both alias commands, creates and drops keep
/// each alias in its canonical map, and the snapshot is version 7 exactly
/// while the multi-target map is non-empty.
#[test]
fn random_alias_commands_keep_the_canonical_form() {
    for seed in 1..=3u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut state = state_with(&[]);
        let mut applied = 0;
        for step in 0..2_000 {
            let command = random_command(&mut rng);
            let before = state.clone();
            match state.apply(command.clone()) {
                Ok(_) => applied += 1,
                Err(_) => assert_eq!(state, before, "seed {seed} step {step}: {command}"),
            }
            assert_eq!(
                state.check_invariants(),
                Vec::<String>::new(),
                "seed {seed} step {step}: {command}"
            );
            assert_eq!(
                snapshot_round_trip(&state).expect("round trip"),
                state,
                "seed {seed} step {step}"
            );
            // Collections never have hot configuration here, so the
            // version is 7 iff some alias is not exactly one unset member.
            let multi = state
                .alias_targets(NS)
                .any(|(_, t)| !(t.members.len() == 1 && t.members.values().all(Option::is_none)));
            assert_eq!(
                snapshot_version(&state),
                if multi { 7 } else { 5 },
                "seed {seed} step {step}: {command}"
            );
        }
        assert!(
            applied > 1_000,
            "seed {seed}: only {applied} commands applied"
        );
    }
}
