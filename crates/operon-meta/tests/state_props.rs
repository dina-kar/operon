//! Property test: random command sequences over the collection catalog keep
//! every invariant, rejected commands change nothing, and the final state
//! survives a snapshot round trip.

use operon_common::schema::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, SparseModifier,
    SparseVectorSpec, VectorElement, VectorIndexSpec, VectorSpec,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_meta::{
    AliasAction, Command, MetaState, Retention, WalChunk, WalClass, collection_pointer_key,
    snapshot_round_trip,
};
use proptest::collection::vec;
use proptest::prelude::*;

const NAMESPACES: [&str; 2] = ["acme", "globex"];
const STREAMS: [&str; 3] = ["events", "_x", "_collection.a.1"];
const COLLECTIONS: [&str; 3] = ["a", "b", "c"];
/// Collection names plus one that is only ever an alias.
const NAMES: [&str; 4] = ["a", "b", "c", "x"];
const ALIASES: [&str; 3] = ["x", "y", "a"];

fn keyword(name: &str) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: name.to_string(),
        kind: FieldKind::Keyword,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

fn base(dim: u32) -> CollectionSchema {
    CollectionSchema::new(
        vec![keyword("title")],
        vec![VectorSpec {
            name: String::new(),
            dim,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::Auto,
            hnsw: HnswParams::default(),
            quantization: None,
        }],
        DynamicMapping::Ignore,
    )
}

fn with_fields(mut schema: CollectionSchema, names: &[&str]) -> CollectionSchema {
    schema.fields.extend(names.iter().map(|n| keyword(n)));
    schema
}

/// The two schemas collections are created with.
fn creation_schema(i: usize) -> CollectionSchema {
    base(if i == 0 { 4 } else { 8 })
}

/// Schema updates: additive over one creation schema or an earlier update,
/// or incompatible with every one.
fn update_schema(i: usize) -> CollectionSchema {
    match i {
        0 => base(4),
        1 => with_fields(base(4), &["tag"]),
        2 => with_fields(base(4), &["tag", "more"]),
        3 => with_fields(base(8), &["tag"]),
        4 => {
            let mut removed = base(4);
            removed.fields.clear();
            removed
        }
        _ => base(4).with_sparse_vectors(vec![SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::Idf,
        }]),
    }
}

fn namespace() -> impl Strategy<Value = NamespaceId> {
    (1u64..=3).prop_map(NamespaceId)
}

fn alias_action() -> impl Strategy<Value = AliasAction> {
    prop_oneof![
        (0..ALIASES.len(), 0..NAMES.len()).prop_map(|(a, c)| AliasAction::Create {
            alias: ALIASES[a].to_string(),
            collection: NAMES[c].to_string(),
        }),
        (0..ALIASES.len()).prop_map(|a| AliasAction::Delete {
            alias: ALIASES[a].to_string(),
        }),
    ]
}

fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        1 => (0..NAMESPACES.len()).prop_map(|n| Command::CreateNamespace {
            name: NAMESPACES[n].to_string(),
        }),
        1 => (namespace(), 0..STREAMS.len(), 1u32..=3).prop_map(|(namespace, n, partitions)| {
            Command::CreateStream {
                namespace,
                name: STREAMS[n].to_string(),
                partitions,
                class: WalClass::Standard,
                retention: Retention::default(),
            }
        }),
        3 => (namespace(), 0..COLLECTIONS.len(), 1u32..=3, 0usize..2).prop_map(
            |(namespace, n, partitions, s)| Command::CreateCollection {
                namespace,
                name: COLLECTIONS[n].to_string(),
                schema: creation_schema(s),
                partitions,
            }
        ),
        2 => (namespace(), 0..NAMES.len(), 0u64..1_000_000).prop_map(|(namespace, n, now_ms)| {
            Command::DropCollection {
                namespace,
                name: NAMES[n].to_string(),
                now_ms,
            }
        }),
        2 => (1u64..=6, 0u64..4, 0usize..6).prop_map(|(id, expected_version, s)| {
            Command::UpdateCollectionSchema {
                collection: CollectionId(id),
                expected_version,
                schema: update_schema(s),
            }
        }),
        2 => (namespace(), vec(alias_action(), 0..=3))
            .prop_map(|(namespace, actions)| Command::UpdateAliases { namespace, actions }),
        3 => (0u32..1_000, 1u64..=8, 0u32..3, 0u64..1_000_000).prop_map(
            |(object, stream, partition, created_at_ms)| Command::CommitWal {
                object: format!("w{object}"),
                created_at_ms,
                chunks: vec![WalChunk {
                    stream: StreamId(stream),
                    partition,
                    records: 1,
                    byte_range: 0..10,
                    max_timestamp_ms: 0,
                }],
            }
        ),
        1 => (1u64..=8, proptest::option::of(1u64..=1_000)).prop_map(|(stream, max_bytes)| {
            Command::SetRetention {
                stream: StreamId(stream),
                retention: Retention {
                    max_age_ms: None,
                    max_bytes,
                },
            }
        }),
        2 => (namespace(), 1u64..=4, proptest::option::of(1u64..=3)).prop_map(
            |(namespace, id, expected)| Command::CasPointer {
                namespace,
                key: collection_pointer_key(CollectionId(id)),
                expected,
                value: "manifest".to_string(),
                fence: None,
                fresh: None,
            }
        ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn collection_commands_keep_the_invariants(commands in vec(command(), 60)) {
        let mut state = MetaState::default();
        state
            .apply(Command::CreateNamespace { name: "acme".to_string() })
            .expect("namespace");
        for command in commands {
            let before = state.clone();
            let result = state.apply(command.clone());
            if result.is_err() {
                prop_assert_eq!(&state, &before, "{} was rejected but changed the state", command);
            }
            let violations = state.check_invariants();
            prop_assert!(violations.is_empty(), "after {}: {:?}", command, violations);
        }
        prop_assert_eq!(snapshot_round_trip(&state).expect("round trip"), state);
    }
}
