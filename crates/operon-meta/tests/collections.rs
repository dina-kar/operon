//! The collection catalog, on `MetaState` directly and through a client.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use operon_common::schema::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, VectorElement,
    VectorIndexSpec, VectorSpec,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_meta::{
    AliasAction, ApplyError, COLLECTION_KIND, Command, LinkId, MetaClient, MetaClientConfig,
    MetaConfig, MetaNode, MetaState, Reply, Retention, Router, SystemClock, TargetRef, WalChunk,
    WalClass, collection_pk_prefix, collection_pointer_key, collection_prefix, implicit_name,
};
use operon_store::Store;
use tempfile::TempDir;

const NS: NamespaceId = NamespaceId(1);
const OTHER_NS: NamespaceId = NamespaceId(2);
const RESERVED: &str = "names starting with '_' are reserved for implicit objects";

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

fn schema() -> CollectionSchema {
    CollectionSchema::new(
        vec![keyword("title")],
        vec![VectorSpec {
            name: String::new(),
            dim: 4,
            distance: Distance::Cosine,
            element: VectorElement::F32,
            index: VectorIndexSpec::Auto,
            hnsw: HnswParams::default(),
            quantization: None,
        }],
        DynamicMapping::Ignore,
    )
}

/// `schema()` with one more field.
fn extended() -> CollectionSchema {
    let mut schema = schema();
    schema.fields.push(keyword("tag"));
    schema
}

/// Namespaces 1 (`acme`, with stream 1 `events`) and 2 (`globex`).
fn state() -> MetaState {
    let mut state = MetaState::default();
    for name in ["acme", "globex"] {
        state
            .apply(Command::CreateNamespace {
                name: name.to_string(),
            })
            .expect("namespace");
    }
    state
        .apply(Command::CreateStream {
            namespace: NS,
            name: "events".to_string(),
            partitions: 1,
            class: WalClass::Standard,
            retention: Retention::default(),
        })
        .expect("stream");
    state
}

fn create_command(
    namespace: NamespaceId,
    name: &str,
    schema: CollectionSchema,
    partitions: u32,
) -> Command {
    Command::CreateCollection {
        namespace,
        name: name.to_string(),
        schema,
        partitions,
    }
}

fn create(state: &mut MetaState, name: &str, partitions: u32) -> (CollectionId, StreamId, LinkId) {
    match state.apply(create_command(NS, name, schema(), partitions)) {
        Ok(Reply::CollectionCreated { id, stream, link }) => (id, stream, link),
        other => panic!("expected CollectionCreated, got {other:?}"),
    }
}

fn drop_command(name: &str, now_ms: u64) -> Command {
    Command::DropCollection {
        namespace: NS,
        name: name.to_string(),
        now_ms,
    }
}

fn update_schema(collection: u64, expected_version: u64, schema: CollectionSchema) -> Command {
    Command::UpdateCollectionSchema {
        collection: CollectionId(collection),
        expected_version,
        schema,
    }
}

fn alias(alias: &str, collection: &str) -> AliasAction {
    AliasAction::Create {
        alias: alias.to_string(),
        collection: collection.to_string(),
    }
}

fn unalias(alias: &str) -> AliasAction {
    AliasAction::Delete {
        alias: alias.to_string(),
    }
}

fn aliases(actions: Vec<AliasAction>) -> Command {
    Command::UpdateAliases {
        namespace: NS,
        actions,
    }
}

fn cas_command(namespace: NamespaceId, key: &str) -> Command {
    Command::CasPointer {
        namespace,
        key: key.to_string(),
        expected: None,
        value: "manifest".to_string(),
        fence: None,
        fresh: None,
    }
}

fn commit(stream: StreamId, object: &str, partitions: &[u32]) -> Command {
    Command::CommitWal {
        object: object.to_string(),
        created_at_ms: 0,
        chunks: partitions
            .iter()
            .map(|&partition| WalChunk {
                stream,
                partition,
                records: 1,
                byte_range: 0..10,
                max_timestamp_ms: 0,
            })
            .collect(),
    }
}

fn alias_list(state: &MetaState, namespace: NamespaceId) -> Vec<(String, CollectionId)> {
    state
        .aliases(namespace)
        .map(|(a, id)| (a.to_string(), id))
        .collect()
}

#[test]
fn create_makes_the_collection_its_stream_and_link_at_once() {
    let mut state = state();
    let (id, stream, link) = create(&mut state, "docs", 3);
    assert_eq!(
        (id, stream, link),
        (CollectionId(1), StreamId(2), LinkId(1))
    );
    assert_eq!(implicit_name("docs", id), "_collection.docs.1");
    assert_eq!(collection_pointer_key(id), "collection/1");
    assert_eq!(collection_prefix(NS, id), "ns/1/collections/1/");
    assert_eq!(collection_pk_prefix(NS, id), "ns/1/pk/collection-1/");

    let collection = state.collection(id).unwrap();
    assert_eq!(
        (
            collection.namespace,
            collection.name.as_str(),
            collection.partitions,
            collection.stream,
            collection.link
        ),
        (NS, "docs", 3, stream, link)
    );
    assert_eq!(collection.schema, schema());
    assert_eq!(state.collection_by_name(NS, "docs"), Some(collection));
    assert_eq!(state.collection_by_name(OTHER_NS, "docs"), None);
    assert_eq!(state.collection_for_link(link), Some(collection));
    assert_eq!(state.collection_for_link(LinkId(9)), None);
    assert_eq!(state.collections(NS).count(), 1);
    assert_eq!(state.collections(OTHER_NS).count(), 0);
    assert_eq!(state.all_collections().count(), 1);

    let s = state.stream_by_name(NS, "_collection.docs.1").unwrap();
    assert_eq!(
        (s.id, s.partitions, s.class, s.retention),
        (stream, 3, WalClass::Standard, Retention::default())
    );
    for partition in 0..3 {
        assert_eq!(state.partition(stream, partition).unwrap().next_offset(), 0);
    }
    let l = state.link_by_name(NS, "_collection.docs.1").unwrap();
    assert_eq!((l.id, l.source), (link, stream));
    assert_eq!(
        l.target,
        TargetRef {
            kind: COLLECTION_KIND.to_string(),
            name: "docs".to_string()
        }
    );
    assert!(l.options.is_empty());
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

#[test]
fn create_retried_with_the_same_schema_returns_collection_exists() {
    let mut state = state();
    let (id, _, _) = create(&mut state, "docs", 2);
    let before = state.clone();
    assert_eq!(
        state.apply(create_command(NS, "docs", schema(), 2)),
        Err(ApplyError::CollectionExists(id))
    );
    assert_eq!(state, before);
    // The same name in another namespace is another collection.
    assert!(matches!(
        state.apply(create_command(OTHER_NS, "docs", schema(), 2)),
        Ok(Reply::CollectionCreated {
            id: CollectionId(2),
            ..
        })
    ));
}

#[test]
fn create_with_a_different_schema_is_name_taken() {
    let mut state = state();
    create(&mut state, "docs", 2);
    let taken = Err(ApplyError::NameTaken("docs".to_string()));
    assert_eq!(
        state.apply(create_command(NS, "docs", extended(), 2)),
        taken
    );
    assert_eq!(state.apply(create_command(NS, "docs", schema(), 3)), taken);
    // An alias holds its name too.
    state.apply(aliases(vec![alias("latest", "docs")])).unwrap();
    assert_eq!(
        state.apply(create_command(NS, "latest", schema(), 2)),
        Err(ApplyError::NameTaken("latest".to_string()))
    );
}

#[test]
fn a_collection_name_that_would_overflow_its_stream_name_is_refused() {
    let mut state = state();
    let before = state.clone();
    assert!(matches!(
        state.apply(create_command(NS, &"a".repeat(223), schema(), 1)),
        Err(ApplyError::InvalidArgument(_))
    ));
    assert_eq!(state, before);
    let (id, _, _) = create(&mut state, &"a".repeat(222), 1);
    assert!(
        state
            .stream_by_name(NS, &implicit_name(&"a".repeat(222), id))
            .is_some()
    );
}

#[test]
fn create_stream_and_create_link_refuse_underscore_names() {
    let mut state = state();
    let before = state.clone();
    let reserved = Err(ApplyError::InvalidArgument(RESERVED.to_string()));
    assert_eq!(
        state.apply(Command::CreateStream {
            namespace: NS,
            name: "_x".to_string(),
            partitions: 1,
            class: WalClass::Standard,
            retention: Retention::default(),
        }),
        reserved
    );
    assert_eq!(
        state.apply(Command::CreateLink {
            namespace: NS,
            name: "_x".to_string(),
            source: StreamId(1),
            target: TargetRef {
                kind: "counter".to_string(),
                name: "x".to_string(),
            },
            options: BTreeMap::new(),
        }),
        reserved
    );
    assert_eq!(
        state.apply(create_command(NS, "_docs", schema(), 1)),
        reserved
    );
    assert_eq!(state, before);
}

#[test]
fn create_link_refuses_a_collection_target() {
    let mut state = state();
    let before = state.clone();
    let result = state.apply(Command::CreateLink {
        namespace: NS,
        name: "into-docs".to_string(),
        source: StreamId(1),
        target: TargetRef {
            kind: COLLECTION_KIND.to_string(),
            name: "docs".to_string(),
        },
        options: BTreeMap::new(),
    });
    assert!(
        matches!(result, Err(ApplyError::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(state, before);
}

#[test]
fn create_link_refuses_an_implicit_stream_as_source() {
    let mut state = state();
    let (_, stream, _) = create(&mut state, "docs", 1);
    let before = state.clone();
    let result = state.apply(Command::CreateLink {
        namespace: NS,
        name: "tap".to_string(),
        source: stream,
        target: TargetRef {
            kind: "counter".to_string(),
            name: "tap".to_string(),
        },
        options: BTreeMap::new(),
    });
    assert!(
        matches!(result, Err(ApplyError::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(state, before);
}

#[test]
fn set_retention_refuses_an_implicit_stream() {
    let mut state = state();
    let (_, stream, _) = create(&mut state, "docs", 1);
    let before = state.clone();
    let result = state.apply(Command::SetRetention {
        stream,
        retention: Retention {
            max_age_ms: Some(1),
            max_bytes: None,
        },
    });
    assert!(
        matches!(result, Err(ApplyError::InvalidArgument(_))),
        "{result:?}"
    );
    assert_eq!(state, before);
    // A user stream still takes one.
    assert!(
        state
            .apply(Command::SetRetention {
                stream: StreamId(1),
                retention: Retention::default(),
            })
            .is_ok()
    );
}

#[test]
fn drop_frees_the_name_and_recreate_gets_a_new_id() {
    let mut state = state();
    let (first, old_stream, old_link) = create(&mut state, "docs", 2);
    assert_eq!(first, CollectionId(1));
    assert_eq!(
        state.apply(drop_command("docs", 10)),
        Ok(Reply::CollectionDropped(Some(first)))
    );
    assert_eq!(state.collection(first), None);
    assert_eq!(state.collection_by_name(NS, "docs"), None);
    assert_eq!(state.stream(old_stream), None);
    assert_eq!(state.partition(old_stream, 0), None);
    assert_eq!(state.link(old_link), None);
    assert_eq!(state.clock_ms(), 10);

    let (second, stream, _) = create(&mut state, "docs", 2);
    assert_eq!(second, CollectionId(2));
    assert_ne!(stream, old_stream);
    assert!(state.stream_by_name(NS, "_collection.docs.2").is_some());
    assert!(state.stream_by_name(NS, "_collection.docs.1").is_none());
    assert!(state.link_by_name(NS, "_collection.docs.1").is_none());
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

#[test]
fn drop_retires_the_streams_objects_and_both_prefixes() {
    let mut state = state();
    let (id, stream, _) = create(&mut state, "docs", 2);
    state.apply(commit(stream, "w1", &[0, 1])).unwrap();
    state.apply(commit(stream, "w2", &[0])).unwrap();
    // An object shared with another stream stays live for that stream.
    let events = StreamId(1);
    let shared = Command::CommitWal {
        object: "w3".to_string(),
        created_at_ms: 0,
        chunks: vec![
            WalChunk {
                stream,
                partition: 1,
                records: 1,
                byte_range: 0..10,
                max_timestamp_ms: 0,
            },
            WalChunk {
                stream: events,
                partition: 0,
                records: 1,
                byte_range: 10..20,
                max_timestamp_ms: 0,
            },
        ],
    };
    state.apply(shared).unwrap();
    assert_eq!(state.retired().count(), 0);

    assert_eq!(
        state.apply(drop_command("docs", 5_000)),
        Ok(Reply::CollectionDropped(Some(id)))
    );
    let retired: Vec<(&str, u64)> = state.retired().collect();
    assert_eq!(
        retired,
        [
            ("ns/1/collections/1/", 5_000),
            ("ns/1/pk/collection-1/", 5_000),
            ("w1", 5_000),
            ("w2", 5_000),
        ]
    );
    assert_eq!(state.wal_live_chunks("w1"), None);
    assert_eq!(state.wal_live_chunks("w3"), Some(1));
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

#[test]
fn drop_of_a_missing_collection_is_none() {
    let mut state = state();
    assert_eq!(
        state.apply(drop_command("docs", 0)),
        Ok(Reply::CollectionDropped(None))
    );
    let (id, _, _) = create(&mut state, "docs", 1);
    state.apply(aliases(vec![alias("latest", "docs")])).unwrap();
    // An alias is not a collection.
    let before = state.clone();
    assert_eq!(
        state.apply(drop_command("latest", 0)),
        Ok(Reply::CollectionDropped(None))
    );
    assert_eq!(state, before);
    assert_eq!(
        state.apply(drop_command("docs", 0)),
        Ok(Reply::CollectionDropped(Some(id)))
    );
    // A retry after a lost acknowledgement.
    assert_eq!(
        state.apply(drop_command("docs", 0)),
        Ok(Reply::CollectionDropped(None))
    );
}

#[test]
fn drop_removes_aliases_to_it() {
    let mut state = state();
    create(&mut state, "docs", 1);
    let (other, _, _) = create(&mut state, "other", 1);
    state
        .apply(aliases(vec![
            alias("a", "docs"),
            alias("b", "other"),
            alias("c", "docs"),
        ]))
        .unwrap();
    state.apply(drop_command("docs", 0)).unwrap();
    assert_eq!(alias_list(&state, NS), [("b".to_string(), other)]);
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

#[test]
fn cas_on_a_dropped_collections_pointer_is_refused() {
    let mut state = state();
    let (id, _, _) = create(&mut state, "docs", 1);
    let key = collection_pointer_key(id);
    // Only in the collection's namespace.
    assert_eq!(
        state.apply(cas_command(OTHER_NS, &key)),
        Err(ApplyError::CollectionNotFound(id))
    );
    assert_eq!(
        state.apply(cas_command(NS, &key)),
        Ok(Reply::PointerSet { version: 1 })
    );
    state.apply(drop_command("docs", 0)).unwrap();
    assert_eq!(state.pointer(NS, &key), None);
    let before = state.clone();
    assert_eq!(
        state.apply(cas_command(NS, &key)),
        Err(ApplyError::CollectionNotFound(CollectionId(1)))
    );
    assert!(matches!(
        state.apply(cas_command(NS, "collection/01")),
        Err(ApplyError::InvalidArgument(_))
    ));
    assert_eq!(state, before);
}

#[test]
fn schema_update_is_additive_and_bumps_the_version() {
    let mut state = state();
    let (id, _, _) = create(&mut state, "docs", 1);
    assert_eq!(
        state.apply(update_schema(1, 1, extended())),
        Ok(Reply::SchemaUpdated { version: 2 })
    );
    let stored = &state.collection(id).unwrap().schema;
    assert_eq!(stored.version, 2);
    assert!(stored.same_ignoring_version(&extended()));
    let mut again = extended();
    again.fields.push(keyword("more"));
    assert_eq!(
        state.apply(update_schema(1, 2, again)),
        Ok(Reply::SchemaUpdated { version: 3 })
    );
}

#[test]
fn a_racing_schema_update_gets_schema_version_mismatch() {
    let mut state = state();
    let (id, _, _) = create(&mut state, "docs", 1);
    state.apply(update_schema(1, 1, extended())).unwrap();
    let mut other = schema();
    other.fields.push(keyword("other"));
    let before = state.clone();
    assert_eq!(
        state.apply(update_schema(1, 1, other)),
        Err(ApplyError::SchemaVersionMismatch {
            collection: id,
            current: 2
        })
    );
    assert_eq!(state, before);
}

#[test]
fn a_retried_schema_update_succeeds() {
    let mut state = state();
    let (id, _, _) = create(&mut state, "docs", 1);
    let reply = Ok(Reply::SchemaUpdated { version: 2 });
    assert_eq!(state.apply(update_schema(1, 1, extended())), reply);
    let before = state.clone();
    assert_eq!(state.apply(update_schema(1, 1, extended())), reply);
    assert_eq!(state, before);
    assert_eq!(state.collection(id).unwrap().schema.version, 2);
}

#[test]
fn an_incompatible_schema_update_is_refused_and_changes_nothing() {
    let mut state = state();
    create(&mut state, "docs", 1);
    let before = state.clone();
    let mut removed = schema();
    removed.fields.clear();
    assert!(matches!(
        state.apply(update_schema(1, 1, removed)),
        Err(ApplyError::IncompatibleSchema(_))
    ));
    assert_eq!(state, before);
}

#[test]
fn aliases_apply_atomically() {
    let mut state = state();
    create(&mut state, "docs", 1);
    let before = state.clone();
    assert_eq!(
        state.apply(aliases(vec![alias("a", "docs"), alias("b", "missing")])),
        Err(ApplyError::UnknownCollection("missing".to_string()))
    );
    assert_eq!(state.resolve_collection(NS, "a"), None);
    assert_eq!(state, before);
}

#[test]
fn an_alias_cannot_shadow_a_collection() {
    let mut state = state();
    create(&mut state, "docs", 1);
    create(&mut state, "other", 1);
    let before = state.clone();
    assert_eq!(
        state.apply(aliases(vec![alias("other", "docs")])),
        Err(ApplyError::NameTaken("other".to_string()))
    );
    // An alias names a collection, not another alias.
    state.apply(aliases(vec![alias("a", "docs")])).unwrap();
    assert_eq!(
        state.apply(aliases(vec![alias("b", "a")])),
        Err(ApplyError::UnknownCollection("a".to_string()))
    );
    state.apply(aliases(vec![unalias("a")])).unwrap();
    assert_eq!(state, before);
}

#[test]
fn deleting_a_missing_alias_is_a_noop() {
    let mut state = state();
    create(&mut state, "docs", 1);
    let before = state.clone();
    assert_eq!(
        state.apply(aliases(vec![unalias("nope")])),
        Ok(Reply::AliasesUpdated)
    );
    assert_eq!(state, before);
}

#[test]
fn resolve_collection_follows_aliases() {
    let mut state = state();
    let (docs, _, _) = create(&mut state, "docs", 1);
    let (other, _, _) = create(&mut state, "other", 1);
    state.apply(aliases(vec![alias("latest", "docs")])).unwrap();
    let id = |state: &MetaState, name: &str| state.resolve_collection(NS, name).map(|c| c.id);
    assert_eq!(id(&state, "docs"), Some(docs));
    assert_eq!(id(&state, "latest"), Some(docs));
    assert_eq!(id(&state, "missing"), None);
    assert_eq!(state.resolve_collection(OTHER_NS, "latest"), None);
    // Creating an existing alias re-points it; later actions see earlier ones.
    state
        .apply(aliases(vec![
            alias("latest", "other"),
            alias("tmp", "docs"),
            unalias("tmp"),
        ]))
        .unwrap();
    assert_eq!(id(&state, "latest"), Some(other));
    assert_eq!(alias_list(&state, NS), [("latest".to_string(), other)]);
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

#[test]
fn rejected_commands_leave_the_state_unchanged() {
    let mut state = state();
    create(&mut state, "docs", 2);
    state.apply(aliases(vec![alias("latest", "docs")])).unwrap();
    state.apply(drop_command("gone", 1_000)).unwrap();
    let mut invalid = schema();
    invalid.fields.push(keyword("_bad"));
    let mut version_two = schema();
    version_two.version = 2;
    let mut removed = schema();
    removed.vectors.clear();
    let rejected = [
        create_command(NamespaceId(9), "docs", schema(), 1),
        create_command(NS, "bad/name", schema(), 1),
        create_command(NS, "_docs", schema(), 1),
        create_command(NS, &"a".repeat(223), schema(), 1),
        create_command(NS, "new", schema(), 0),
        create_command(NS, "new", schema(), 10_001),
        create_command(NS, "new", invalid.clone(), 1),
        create_command(NS, "new", version_two, 1),
        create_command(NS, "docs", schema(), 2),
        create_command(NS, "docs", extended(), 2),
        create_command(NS, "latest", schema(), 2),
        update_schema(9, 1, extended()),
        update_schema(1, 1, invalid),
        update_schema(1, 2, extended()),
        update_schema(1, 1, removed),
        Command::UpdateAliases {
            namespace: NamespaceId(9),
            actions: vec![alias("a", "docs")],
        },
        aliases(vec![]),
        aliases((0..101).map(|i| alias(&format!("a{i}"), "docs")).collect()),
        aliases(vec![alias("bad/name", "docs")]),
        aliases(vec![alias("_a", "docs")]),
        aliases(vec![unalias("latest"), alias("docs", "docs")]),
        aliases(vec![unalias("latest"), alias("a", "missing")]),
        cas_command(NS, "collection/7"),
        cas_command(NS, "collection/x"),
        Command::CreateStream {
            namespace: NS,
            name: "_collection.docs.9".to_string(),
            partitions: 1,
            class: WalClass::Standard,
            retention: Retention::default(),
        },
        // The implicit stream (2) keeps its retention and takes no user links.
        Command::SetRetention {
            stream: StreamId(2),
            retention: Retention {
                max_age_ms: Some(1),
                max_bytes: None,
            },
        },
        Command::CreateLink {
            namespace: NS,
            name: "tap".to_string(),
            source: StreamId(2),
            target: TargetRef {
                kind: "counter".to_string(),
                name: "tap".to_string(),
            },
            options: BTreeMap::new(),
        },
    ];
    for command in rejected {
        let before = state.clone();
        let result = state.apply(command.clone());
        assert!(result.is_err(), "{command}: {result:?}");
        assert_eq!(state, before, "{command} changed the state");
        assert_eq!(state.clock_ms(), 1_000);
    }
    assert_eq!(state.check_invariants(), Vec::<String>::new());
}

async fn single_node() -> (TempDir, MetaNode, MetaClient) {
    let dir = TempDir::new().expect("temp dir");
    let node = MetaNode::start(
        MetaConfig::new(1, dir.path(), Store::in_memory()),
        &Router::new(),
    )
    .await
    .expect("start");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(Duration::from_secs(10))
        .await
        .expect("leader");
    let client = MetaClient::new(
        node.clone(),
        vec![],
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    (dir, node, client)
}

#[tokio::test]
async fn a_create_collection_with_a_lost_ack_returns_the_same_ids() {
    let (_dir, node, client) = single_node().await;
    let ns = client.create_namespace("acme").await.unwrap();
    client.inject_lost_ack();
    let created = client
        .create_collection(ns, "docs", schema(), 2)
        .await
        .unwrap();
    assert_eq!(created, (CollectionId(1), StreamId(1), LinkId(1)));
    // Schema updates and alias updates are retry-safe as well.
    client.inject_lost_ack();
    assert_eq!(
        client
            .update_collection_schema(CollectionId(1), 1, extended())
            .await
            .unwrap(),
        2
    );
    client.inject_lost_ack();
    client
        .update_aliases(ns, vec![alias("latest", "docs")])
        .await
        .unwrap();
    let resolved = client
        .read(operon_meta::Consistency::Local, |s| {
            s.resolve_collection(ns, "latest").map(|c| c.id)
        })
        .await
        .unwrap();
    assert_eq!(resolved, Some(CollectionId(1)));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_drop_with_a_lost_ack_leaves_the_collection_dropped() {
    let (_dir, node, client) = single_node().await;
    let ns = client.create_namespace("acme").await.unwrap();
    let (id, stream, _) = client
        .create_collection(ns, "docs", schema(), 1)
        .await
        .unwrap();
    assert_eq!(client.drop_collection(ns, "missing").await.unwrap(), None);
    client.inject_lost_ack();
    // The first attempt dropped it; the retry finds nothing to drop.
    assert_eq!(client.drop_collection(ns, "docs").await.unwrap(), None);
    let (collection, implicit) = client
        .read(operon_meta::Consistency::Local, |s| {
            (s.collection(id).cloned(), s.stream(stream).cloned())
        })
        .await
        .unwrap();
    assert_eq!((collection, implicit), (None, None));
    let (again, _, _) = client
        .create_collection(ns, "docs", schema(), 1)
        .await
        .unwrap();
    assert_eq!(again, CollectionId(2));
    node.shutdown().await.unwrap();
}
