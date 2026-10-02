//! Collections: the catalog, aliases, schema evolution, composite reads.

use std::collections::BTreeMap;

use loams_common::meta::{
    AliasAction, AliasTargetAction, AliasTargets, ApplyError, COLLECTION_KIND, Collection,
    CollectionHead, Consistency, MAX_COLLECTION_NAME_LEN, MetaStore, NameTarget, Pointer,
    Retention, TargetRef, WalClass, collection_pk_prefix, collection_pointer_key,
    collection_prefix, implicit_name,
};
use loams_common::{CollectionId, NamespaceId};

use super::{cas, chunk, commit, keyword, namespace, rejected, retired, schema, stamp, stream};
use crate::Backend;

const L: Consistency = Consistency::Linearizable;

fn create(alias: &str, collection: &str) -> AliasAction {
    AliasAction::Create {
        alias: alias.to_string(),
        collection: collection.to_string(),
    }
}

pub async fn create_collection_makes_its_stream_and_link(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-create").await;
    let (cid, sid, lid) = meta
        .create_collection(ns, "docs", schema(), 3)
        .await
        .expect("create collection");
    let expected = Collection {
        id: cid,
        namespace: ns,
        name: "docs".to_string(),
        schema: schema(),
        partitions: 3,
        stream: sid,
        link: lid,
    };
    let reader = db.last();
    assert_eq!(
        reader.collection(L, cid).await.expect("read"),
        Some(expected.clone())
    );
    assert_eq!(
        reader
            .resolve_collection(L, ns, "docs")
            .await
            .expect("read"),
        Some(expected.clone())
    );
    assert_eq!(
        reader.collections(L, Some(ns)).await.expect("read"),
        vec![expected.clone()]
    );
    let implicit = implicit_name("docs", cid);
    let stream = reader.stream(L, sid).await.expect("read").expect("stream");
    assert_eq!(stream.name, implicit);
    assert_eq!(stream.namespace, ns);
    assert_eq!(stream.partitions, 3);
    assert_eq!(stream.class, WalClass::Standard);
    assert_eq!(stream.retention, Retention::default());
    let link = reader
        .link_by_name(L, ns, &implicit)
        .await
        .expect("read")
        .expect("link");
    assert_eq!(link.id, lid);
    assert_eq!(link.source, sid);
    assert_eq!(
        link.target,
        TargetRef {
            kind: COLLECTION_KIND.to_string(),
            name: "docs".to_string(),
        }
    );
    assert_eq!(link.options, BTreeMap::new());
    assert_eq!(
        reader
            .resolve_collection(L, ns, "missing")
            .await
            .expect("read"),
        None
    );
}

pub async fn create_collection_retry_is_collection_exists(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-retry").await;
    let (cid, _, _) = meta
        .create_collection(ns, "docs", schema(), 2)
        .await
        .expect("create collection");
    assert_eq!(
        rejected(db.last().create_collection(ns, "docs", schema(), 2).await),
        ApplyError::CollectionExists(cid)
    );
    // The same name with another partition count or schema is taken.
    assert_eq!(
        rejected(meta.create_collection(ns, "docs", schema(), 3).await),
        ApplyError::NameTaken("docs".to_string())
    );
    let mut wider = schema();
    wider.fields.push(keyword("tag"));
    assert_eq!(
        rejected(meta.create_collection(ns, "docs", wider, 2).await),
        ApplyError::NameTaken("docs".to_string())
    );
    let mut versioned = schema();
    versioned.version = 2;
    for (name, schema, partitions) in [
        ("versioned".to_string(), versioned, 1),
        ("_reserved".to_string(), schema(), 1),
        ("x".repeat(MAX_COLLECTION_NAME_LEN + 1), schema(), 1),
        ("no-partitions".to_string(), schema(), 0),
    ] {
        assert!(
            matches!(
                rejected(meta.create_collection(ns, &name, schema, partitions).await),
                ApplyError::InvalidArgument(_)
            ),
            "{name:?}"
        );
    }
    let missing = NamespaceId(ns.0 + 1000);
    assert_eq!(
        rejected(meta.create_collection(missing, "docs", schema(), 2).await),
        ApplyError::NamespaceNotFound(missing)
    );
    assert_eq!(db.last().collections(L, None).await.expect("read").len(), 1);
}

pub async fn drop_frees_the_name_and_retires_both_prefixes(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-drop").await;
    let (cid, sid, lid) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    let key = collection_pointer_key(cid);
    meta.cas_pointer(cas(ns, &key, None, "coll-drop/m-1"))
        .await
        .into_result()
        .expect("cas");
    meta.update_aliases(ns, vec![create("live", "docs")])
        .await
        .expect("alias");
    assert_eq!(
        db.last().drop_collection(ns, "docs").await.expect("drop"),
        Some(cid)
    );
    let reader = db.last();
    assert_eq!(reader.collection(L, cid).await.expect("read"), None);
    assert_eq!(
        reader
            .resolve_collection(L, ns, "live")
            .await
            .expect("read"),
        None
    );
    assert_eq!(reader.aliases(L, ns).await.expect("read"), Vec::new());
    assert_eq!(reader.stream(L, sid).await.expect("read"), None);
    assert_eq!(
        reader
            .link_by_name(L, ns, &implicit_name("docs", cid))
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        reader.collection_for_link(L, lid).await.expect("read"),
        None
    );
    assert_eq!(reader.pointer(L, ns, &key).await.expect("read"), None);
    let retired = retired(reader).await;
    assert!(retired.contains(&collection_prefix(ns, cid)), "{retired:?}");
    assert!(
        retired.contains(&collection_pk_prefix(ns, cid)),
        "{retired:?}"
    );
    // A retry finds nothing to drop.
    assert_eq!(meta.drop_collection(ns, "docs").await.expect("drop"), None);
    // The name is free again, for a new collection.
    let (again, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create again");
    assert_ne!(again, cid);
}

pub async fn schema_updates_are_additive_and_versioned(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-schema").await;
    let (cid, _, _) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    let mut extended = schema();
    extended.fields.push(keyword("tag"));
    assert_eq!(
        meta.update_collection_schema(cid, 1, extended.clone())
            .await
            .expect("update"),
        2
    );
    let stored = db
        .last()
        .collection(L, cid)
        .await
        .expect("read")
        .expect("collection")
        .schema;
    assert_eq!(stored.version, 2);
    assert_eq!(stored.fields, extended.fields);
    // A retry finds the same schema at the next version.
    assert_eq!(
        db.last()
            .update_collection_schema(cid, 1, extended.clone())
            .await
            .expect("retry"),
        2
    );
    let mut other = extended.clone();
    other.fields.push(keyword("other"));
    assert_eq!(
        rejected(meta.update_collection_schema(cid, 1, other.clone()).await),
        ApplyError::SchemaVersionMismatch {
            collection: cid,
            current: 2
        }
    );
    // Removing a field is not additive.
    assert!(matches!(
        rejected(meta.update_collection_schema(cid, 2, schema()).await),
        ApplyError::IncompatibleSchema(_)
    ));
    assert_eq!(
        meta.update_collection_schema(cid, 2, other)
            .await
            .expect("update"),
        3
    );
    let missing = CollectionId(cid.0 + 1000);
    assert_eq!(
        rejected(meta.update_collection_schema(missing, 1, schema()).await),
        ApplyError::CollectionNotFound(missing)
    );
}

pub async fn aliases_apply_atomically_and_resolve(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-aliases").await;
    let (a, _, _) = meta
        .create_collection(ns, "docs-a", schema(), 1)
        .await
        .expect("create a");
    let (b, _, _) = meta
        .create_collection(ns, "docs-b", schema(), 1)
        .await
        .expect("create b");
    let reader = db.last();
    let resolved = |alias: &'static str| async move {
        reader
            .resolve_collection(L, ns, alias)
            .await
            .expect("read")
            .map(|c| c.id)
    };
    meta.update_aliases(ns, vec![create("live", "docs-a")])
        .await
        .expect("alias");
    assert_eq!(resolved("live").await, Some(a));
    // All or nothing.
    assert_eq!(
        rejected(
            meta.update_aliases(ns, vec![create("next", "docs-b"), create("bad", "missing")])
                .await
        ),
        ApplyError::UnknownCollection("missing".to_string())
    );
    assert_eq!(resolved("next").await, None);
    assert_eq!(
        rejected(
            meta.update_aliases(ns, vec![create("docs-a", "docs-b")])
                .await
        ),
        ApplyError::NameTaken("docs-a".to_string())
    );
    // A collection named like an alias is refused too.
    assert_eq!(
        rejected(meta.create_collection(ns, "live", schema(), 1).await),
        ApplyError::NameTaken("live".to_string())
    );
    // Re-pointing, and deleting a missing alias, in one update.
    db.last()
        .update_aliases(
            ns,
            vec![
                create("live", "docs-b"),
                create("alpha", "docs-a"),
                AliasAction::Delete {
                    alias: "nothing".to_string(),
                },
            ],
        )
        .await
        .expect("update");
    assert_eq!(resolved("live").await, Some(b));
    assert_eq!(
        db.last().aliases(L, ns).await.expect("read"),
        vec![("alpha".to_string(), a), ("live".to_string(), b)]
    );
    // Retrying the same update changes nothing.
    meta.update_aliases(ns, vec![create("live", "docs-b")])
        .await
        .expect("retry");
    meta.update_aliases(
        ns,
        vec![AliasAction::Delete {
            alias: "alpha".to_string(),
        }],
    )
    .await
    .expect("delete");
    assert_eq!(
        db.last().aliases(L, ns).await.expect("read"),
        vec![("live".to_string(), b)]
    );
    assert!(matches!(
        rejected(meta.update_aliases(ns, Vec::new()).await),
        ApplyError::InvalidArgument(_)
    ));
    let missing = NamespaceId(ns.0 + 1000);
    assert_eq!(
        rejected(
            meta.update_aliases(missing, vec![create("x", "docs-a")])
                .await
        ),
        ApplyError::NamespaceNotFound(missing)
    );
}

pub async fn collection_head_reads_pointer_bounds_and_clock(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-head").await;
    let (cid, sid, _) = meta
        .create_collection(ns, "docs", schema(), 2)
        .await
        .expect("create collection");
    commit(meta, "coll-head/wal-1", vec![chunk(sid, 1, 3, 0..30)]).await;
    commit(meta, "coll-head/wal-2", vec![chunk(sid, 1, 2, 0..20)]).await;
    meta.trim_partition(sid, 1, 3, None).await.expect("trim");
    meta.cas_pointer(cas(ns, &collection_pointer_key(cid), None, "coll-head/m-1"))
        .await
        .into_result()
        .expect("cas");
    stamp(meta).await;
    let reader = db.last();
    let clock = reader.clock_ms(L).await.expect("clock");
    let head = reader
        .collection_head(L, cid)
        .await
        .expect("read")
        .expect("head");
    let collection = reader
        .collection(L, cid)
        .await
        .expect("read")
        .expect("collection");
    // The clock never goes backwards, so the head's clock is not earlier
    // than a clock read before it. It may be later: a backend whose clock
    // is a TSO (TiKV) advances it with time, not only with writes (R1 plan
    // row T4-14).
    assert!(
        head.clock_ms >= clock,
        "head clock {} < {clock}",
        head.clock_ms
    );
    assert!(clock > 0);
    assert_eq!(
        head,
        CollectionHead {
            collection,
            pointer: Some(Pointer {
                version: 1,
                value: "coll-head/m-1".to_string(),
            }),
            log_start_offsets: vec![0, 3],
            high_watermarks: vec![0, 5],
            clock_ms: head.clock_ms,
        }
    );
    // The same head from the lists, at a clock not earlier than its own.
    for heads in [
        reader.collection_heads(L, Some(ns)).await.expect("read"),
        reader.collection_heads(L, None).await.expect("read"),
    ] {
        let [listed] = heads.as_slice() else {
            panic!("expected one head, got {heads:?}");
        };
        assert!(listed.clock_ms >= head.clock_ms);
        assert_eq!(
            *listed,
            CollectionHead {
                clock_ms: listed.clock_ms,
                ..head.clone()
            }
        );
    }
    assert_eq!(
        reader
            .collection_head(L, CollectionId(cid.0 + 1000))
            .await
            .expect("read"),
        None
    );
    // Without a commit yet, the head has no pointer.
    let (fresh, _, _) = meta
        .create_collection(ns, "empty", schema(), 1)
        .await
        .expect("create collection");
    let head = reader
        .collection_head(L, fresh)
        .await
        .expect("read")
        .expect("head");
    assert_eq!(head.pointer, None);
    assert_eq!(head.log_start_offsets, vec![0]);
    assert_eq!(head.high_watermarks, vec![0]);
}

pub async fn collection_for_link_finds_the_implicit_link(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let ns = namespace(meta, "coll-link").await;
    let (cid, _, lid) = meta
        .create_collection(ns, "docs", schema(), 1)
        .await
        .expect("create collection");
    let found = db
        .last()
        .collection_for_link(L, lid)
        .await
        .expect("read")
        .expect("collection");
    assert_eq!(found.id, cid);
    assert_eq!(found.link, lid);
    let s = stream(meta, ns, "events", 1).await;
    let counter = meta
        .create_link(
            ns,
            "counts",
            s,
            TargetRef {
                kind: "counter".to_string(),
                name: "docs".to_string(),
            },
            BTreeMap::new(),
        )
        .await
        .expect("create link");
    assert_eq!(
        db.last()
            .collection_for_link(L, counter)
            .await
            .expect("read"),
        None
    );
    assert_eq!(
        db.last()
            .collection_for_link(L, loams_common::meta::LinkId(lid.0 + 1000))
            .await
            .expect("read"),
        None
    );
}

// ----- Aliases with several members (M1.5 Task 0a) -----

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

fn targets(pairs: &[(CollectionId, Option<bool>)]) -> AliasTargets {
    AliasTargets {
        members: pairs.iter().copied().collect(),
    }
}

/// An alias's members by id and its write target.
type AliasView = (Vec<(CollectionId, Option<bool>)>, Option<CollectionId>);

/// Alias `name` through `resolve_name`, `Linearizable`; panics when `name`
/// is a collection.
async fn alias_of(meta: &dyn MetaStore, ns: NamespaceId, name: &str) -> Option<AliasView> {
    match meta.resolve_name(L, ns, name).await.expect("read")? {
        NameTarget::Alias {
            members,
            write_target,
        } => Some((
            members.into_iter().map(|(c, w)| (c.id, w)).collect(),
            write_target,
        )),
        NameTarget::Collection(c) => panic!("{name} is collection {}", c.id),
    }
}

/// Collections `names` in a fresh namespace; their ids in order.
async fn fresh_collections(
    meta: &dyn MetaStore,
    ns_name: &str,
    names: &[&str],
) -> (NamespaceId, Vec<CollectionId>) {
    let ns = namespace(meta, ns_name).await;
    let mut ids = Vec::new();
    for name in names {
        let (id, _, _) = meta
            .create_collection(ns, name, schema(), 1)
            .await
            .expect("create collection");
        ids.push(id);
    }
    (ns, ids)
}

pub async fn alias_targets_apply_atomically_and_resolve(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-atomic", &["t1", "t2", "t3"]).await;
    meta.update_alias_targets(ns, vec![add("al", "t1", None), add("al", "t2", None)])
        .await
        .expect("alias");
    let reader = db.last();
    assert_eq!(
        alias_of(reader, ns, "al").await,
        Some((vec![(ids[0], None), (ids[1], None)], None))
    );
    // All or nothing: the unknown collection refuses the whole command.
    assert_eq!(
        rejected(
            meta.update_alias_targets(ns, vec![add("al", "t3", None), add("al", "missing", None)])
                .await
        ),
        ApplyError::UnknownCollection("missing".to_string())
    );
    assert_eq!(
        reader.alias_targets(L, ns).await.expect("read"),
        vec![("al".to_string(), targets(&[(ids[0], None), (ids[1], None)]))]
    );
    // An alias names collections, not aliases, and never a collection's name.
    assert_eq!(
        rejected(
            meta.update_alias_targets(ns, vec![add("other", "al", None)])
                .await
        ),
        ApplyError::UnknownCollection("al".to_string())
    );
    assert_eq!(
        rejected(
            meta.update_alias_targets(ns, vec![add("t3", "t1", None)])
                .await
        ),
        ApplyError::NameTaken("t3".to_string())
    );
    assert!(matches!(
        rejected(meta.update_alias_targets(ns, Vec::new()).await),
        ApplyError::InvalidArgument(_)
    ));
    let missing = NamespaceId(ns.0 + 1000);
    assert_eq!(
        rejected(
            meta.update_alias_targets(missing, vec![add("x", "t1", None)])
                .await
        ),
        ApplyError::NamespaceNotFound(missing)
    );
    // Applying the same command twice leaves the same state.
    for _ in 0..2 {
        meta.update_alias_targets(ns, vec![add("al", "t3", None), remove("al", "t1")])
            .await
            .expect("retry-safe");
    }
    assert_eq!(
        alias_of(reader, ns, "al").await,
        Some((vec![(ids[1], None), (ids[2], None)], None))
    );
    assert_eq!(
        reader.aliases(L, ns).await.expect("read"),
        vec![("al".to_string(), ids[1]), ("al".to_string(), ids[2])]
    );
    // resolve_name: a collection, and nothing.
    assert!(matches!(
        reader.resolve_name(L, ns, "t1").await.expect("read"),
        Some(NameTarget::Collection(c)) if c.id == ids[0]
    ));
    assert_eq!(
        reader.resolve_name(L, ns, "none").await.expect("read"),
        None
    );
}

pub async fn alias_write_target_follows_elasticsearch_rules(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-write", &["i1", "i2", "i3"]).await;
    let reader = db.last();
    let steps: Vec<(Vec<AliasTargetAction>, Option<CollectionId>)> = vec![
        // The only member, unset, is the write target.
        (vec![add("al", "i1", None)], Some(ids[0])),
        // The member set to true.
        (vec![add("al", "i2", Some(true))], Some(ids[1])),
        // Set to false: none.
        (vec![add("al", "i2", Some(false))], None),
        (vec![add("al", "i1", Some(true))], Some(ids[0])),
        // Two members, neither set to true.
        (vec![remove("al", "i1"), add("al", "i3", None)], None),
        // One member set to false is no write target either.
        (vec![remove("al", "i3")], None),
    ];
    for (actions, want) in steps {
        meta.update_alias_targets(ns, actions.clone())
            .await
            .expect("update");
        let (_, write_target) = alias_of(reader, ns, "al").await.expect("alias");
        assert_eq!(write_target, want, "after {actions:?}");
    }
    assert_eq!(
        alias_of(reader, ns, "al").await,
        Some((vec![(ids[1], Some(false))], None))
    );
}

pub async fn an_alias_with_two_write_targets_is_refused(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, _) = fresh_collections(meta, "at-two", &["b", "a"]).await;
    assert_eq!(
        rejected(
            meta.update_alias_targets(
                ns,
                vec![add("al", "b", Some(true)), add("al", "a", Some(true))]
            )
            .await
        ),
        ApplyError::InvalidArgument("alias [al] has more than one write index [a,b]".to_string())
    );
    assert_eq!(
        db.last().resolve_name(L, ns, "al").await.expect("read"),
        None
    );
    assert_eq!(db.last().aliases(L, ns).await.expect("read"), Vec::new());
}

pub async fn single_target_aliases_read_back_through_both_alias_apis(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-single", &["s1", "s2"]).await;
    meta.update_aliases(ns, vec![create("old", "s1")])
        .await
        .expect("M1.1 alias");
    meta.update_alias_targets(ns, vec![add("new", "s2", None)])
        .await
        .expect("M1.5 alias");
    let reader = db.last();
    assert_eq!(
        reader.aliases(L, ns).await.expect("read"),
        vec![("new".to_string(), ids[1]), ("old".to_string(), ids[0])]
    );
    assert_eq!(
        reader.alias_targets(L, ns).await.expect("read"),
        vec![
            ("new".to_string(), targets(&[(ids[1], None)])),
            ("old".to_string(), targets(&[(ids[0], None)])),
        ]
    );
    for (name, id) in [("old", ids[0]), ("new", ids[1])] {
        assert_eq!(
            reader
                .resolve_collection(L, ns, name)
                .await
                .expect("read")
                .map(|c| c.id),
            Some(id)
        );
        assert_eq!(
            alias_of(reader, ns, name).await,
            Some((vec![(id, None)], Some(id)))
        );
    }
}

pub async fn update_aliases_repoints_a_multi_target_alias(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-repoint", &["r1", "r2", "r3"]).await;
    meta.update_alias_targets(ns, vec![add("al", "r1", None), add("al", "r2", Some(true))])
        .await
        .expect("alias");
    meta.update_aliases(ns, vec![create("al", "r3")])
        .await
        .expect("re-point");
    let reader = db.last();
    assert_eq!(
        reader.aliases(L, ns).await.expect("read"),
        vec![("al".to_string(), ids[2])]
    );
    assert_eq!(
        reader
            .resolve_collection(L, ns, "al")
            .await
            .expect("read")
            .map(|c| c.id),
        Some(ids[2])
    );
    meta.update_alias_targets(ns, vec![add("al", "r1", None)])
        .await
        .expect("grow");
    meta.update_aliases(
        ns,
        vec![AliasAction::Delete {
            alias: "al".to_string(),
        }],
    )
    .await
    .expect("delete");
    assert_eq!(reader.resolve_name(L, ns, "al").await.expect("read"), None);
    // The freed name can hold a collection.
    meta.create_collection(ns, "al", schema(), 1)
        .await
        .expect("create over the freed name");
}

pub async fn drop_removes_a_collection_from_every_alias(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-drop", &["a", "b", "c"]).await;
    meta.update_alias_targets(
        ns,
        vec![
            add("p", "a", None),
            add("p", "b", Some(true)),
            add("q", "b", None),
            add("r", "a", Some(false)),
            add("r", "b", Some(true)),
            add("s", "a", None),
            add("s", "b", None),
            add("s", "c", None),
        ],
    )
    .await
    .expect("aliases");
    // A multi-target alias holds its name against collections.
    assert_eq!(
        rejected(meta.create_collection(ns, "s", schema(), 1).await),
        ApplyError::NameTaken("s".to_string())
    );
    assert_eq!(
        meta.drop_collection(ns, "b").await.expect("drop"),
        Some(ids[1])
    );
    let reader = db.last();
    // p: one unset member left, which becomes the write target.
    assert_eq!(
        alias_of(reader, ns, "p").await,
        Some((vec![(ids[0], None)], Some(ids[0])))
    );
    // q: its last member gone, so is the alias.
    assert_eq!(alias_of(reader, ns, "q").await, None);
    // r: one member set to false, no write target.
    assert_eq!(
        alias_of(reader, ns, "r").await,
        Some((vec![(ids[0], Some(false))], None))
    );
    assert_eq!(
        alias_of(reader, ns, "s").await,
        Some((vec![(ids[0], None), (ids[2], None)], None))
    );
    assert_eq!(
        reader.aliases(L, ns).await.expect("read"),
        vec![
            ("p".to_string(), ids[0]),
            ("r".to_string(), ids[0]),
            ("s".to_string(), ids[0]),
            ("s".to_string(), ids[2]),
        ]
    );
}

pub async fn resolve_collection_is_none_for_a_multi_target_alias(backend: &dyn Backend) {
    let db = backend.start().await;
    let meta = db.first();
    let (ns, ids) = fresh_collections(meta, "at-resolve", &["x1", "x2"]).await;
    meta.update_alias_targets(ns, vec![add("al", "x1", None), add("al", "x2", Some(true))])
        .await
        .expect("alias");
    let reader = db.last();
    assert_eq!(
        reader.resolve_collection(L, ns, "al").await.expect("read"),
        None
    );
    // One member left, set to true: resolve_collection finds it again.
    meta.update_alias_targets(ns, vec![remove("al", "x1")])
        .await
        .expect("shrink");
    assert_eq!(
        reader
            .resolve_collection(L, ns, "al")
            .await
            .expect("read")
            .map(|c| c.id),
        Some(ids[1])
    );
}
