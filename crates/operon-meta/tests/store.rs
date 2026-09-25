//! `MetaStore` for the openraft `MetaClient` (M1.2a plan, Task 4): every
//! trait read against one state-machine closure, the GC queries, the tracked
//! writes, the change watch, readiness and the `Arc<dyn MetaStore>`
//! conversions.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_common::meta::{
    AliasAction, ApplyError, Collection, CollectionHead, CollectionRoots, Freshness, Lease, Link,
    LinkHead, LinkId, MetaError, MetaStore, Namespace, PartitionBounds, PartitionIndex, Pointer,
    PointerCas, Retention, SegmentSwap, Stream, StreamState, TargetRef, WalChunk, WalClass,
    WalCommit, collection_pointer_key, implicit_name, link_pointer_key,
};
use operon_common::schema::{
    CollectionSchema, Distance, DynamicMapping, FieldKind, FieldSpec, HnswParams, VectorElement,
    VectorIndexSpec, VectorSpec,
};
use operon_common::{CollectionId, NamespaceId, StreamId};
use operon_meta::{
    Consistency, ManualClock, MetaClient, MetaClientConfig, MetaConfig, MetaNode, MetaState,
    Router, SystemClock,
};
use operon_store::Store;
use tempfile::TempDir;

const WAIT: Duration = Duration::from_secs(10);
/// The client clock's start, in ms since the epoch.
const T0: u64 = 10_000_000;

struct Single {
    _dir: TempDir,
    node: MetaNode,
    client: MetaClient,
    clock: Arc<ManualClock>,
}

impl Single {
    fn meta(&self) -> &dyn MetaStore {
        &self.client
    }

    /// Moves the client clock to `now_ms` and the metastore clock with it (a
    /// stamped write that changes nothing else).
    async fn tick(&self, now_ms: u64) {
        self.clock.set(now_ms);
        self.meta().prune_wal_commits(None).await.expect("prune");
    }

    async fn shutdown(self) {
        self.node.shutdown().await.expect("shutdown");
    }
}

/// A single node, and a client whose clock is a `ManualClock` at `T0`.
async fn single() -> Single {
    let dir = TempDir::new().expect("temp dir");
    let node = MetaNode::start(
        MetaConfig::new(1, dir.path(), Store::in_memory()),
        &Router::new(),
    )
    .await
    .expect("start");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(WAIT).await.expect("leader");
    let clock = Arc::new(ManualClock::new(T0));
    let client = MetaClient::new(
        node.clone(),
        Vec::new(),
        clock.clone(),
        MetaClientConfig::default(),
    );
    Single {
        _dir: dir,
        node,
        client,
        clock,
    }
}

fn schema() -> CollectionSchema {
    CollectionSchema::new(
        vec![FieldSpec {
            name: "title".to_string(),
            source_path: "title".to_string(),
            kind: FieldKind::Keyword,
            indexed: true,
            fast: true,
            ignore_malformed: false,
        }],
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

fn chunk(stream: StreamId, partition: u32, records: u32, bytes: std::ops::Range<u64>) -> WalChunk {
    WalChunk {
        stream,
        partition,
        records,
        byte_range: bytes,
        max_timestamp_ms: 1_000,
    }
}

fn target(kind: &str, name: &str) -> TargetRef {
    TargetRef {
        kind: kind.to_string(),
        name: name.to_string(),
    }
}

/// Commits `object`, created now by the client clock; returns the offsets.
async fn commit(meta: &dyn MetaStore, object: &str, chunks: Vec<WalChunk>) -> Vec<u64> {
    let commit = WalCommit {
        object: object.to_string(),
        created_at_ms: meta.now_ms(),
        chunks,
    };
    meta.commit_wal(commit).await.into_result().expect("commit")
}

/// Swaps the WAL entries `replaces` of a partition for `segment`.
async fn swap(
    meta: &dyn MetaStore,
    stream: StreamId,
    partition: u32,
    replaces: &[(u64, &str)],
    segment: &str,
) {
    let swap = SegmentSwap {
        stream,
        partition,
        replaces: replaces.iter().map(|(b, o)| (*b, o.to_string())).collect(),
        segment: segment.to_string(),
        byte_range: 0..90,
        max_timestamp_ms: 1_000,
        fence: None,
        fresh: Freshness {
            created_at_ms: meta.now_ms(),
            max_age_ms: 60_000,
        },
    };
    meta.swap_segment(swap).await.into_result().expect("swap");
}

fn cas(namespace: NamespaceId, key: &str, expected: Option<u64>, value: &str) -> PointerCas {
    PointerCas {
        namespace,
        key: key.to_string(),
        expected,
        value: value.to_string(),
        fence: None,
        fresh: None,
    }
}

async fn set_pointer(
    meta: &dyn MetaStore,
    namespace: NamespaceId,
    key: &str,
    expected: Option<u64>,
    value: &str,
) -> u64 {
    meta.cas_pointer(cas(namespace, key, expected, value))
        .await
        .into_result()
        .expect("cas")
}

/// The index entries of a partition, straight from the state machine.
async fn all_entries(
    client: &MetaClient,
    stream: StreamId,
    partition: u32,
) -> Vec<operon_common::meta::IndexEntry> {
    client
        .read(Consistency::Local, |s| {
            s.partition(stream, partition)
                .map(|state| state.entries().cloned().collect())
                .unwrap_or_default()
        })
        .await
        .expect("read")
}

// ----- Every trait read against one closure -----

/// The arguments of every read the composite test makes.
struct Queries {
    namespace_names: Vec<&'static str>,
    namespaces: Vec<NamespaceId>,
    streams: Vec<StreamId>,
    stream_names: Vec<(NamespaceId, String)>,
    link_names: Vec<(NamespaceId, &'static str)>,
    index: Vec<(StreamId, u32, u64, Option<u64>)>,
    leases: Vec<&'static str>,
    pointers: Vec<(NamespaceId, String)>,
    collections: Vec<CollectionId>,
    resolve: Vec<(NamespaceId, &'static str)>,
    links: Vec<LinkId>,
    graces: Vec<u64>,
    wal_candidates: Vec<(String, u64)>,
    wal_ages: Vec<(u64, usize)>,
    segment_candidates: Vec<(String, u64)>,
    segment_ages: Vec<(NamespaceId, u64, usize)>,
    referenced: Vec<(StreamId, u32, &'static str)>,
    roots: Vec<(NamespaceId, String)>,
}

impl Queries {
    /// Each namespace, then `None` for all of them.
    fn scopes(&self) -> Vec<Option<NamespaceId>> {
        self.namespaces
            .iter()
            .copied()
            .map(Some)
            .chain([None])
            .collect()
    }
}

/// What every read returned.
#[derive(Debug, PartialEq)]
struct Reads {
    clock_ms: u64,
    namespace_by_name: Vec<Option<Namespace>>,
    namespaces: Vec<Namespace>,
    stream: Vec<Option<Stream>>,
    stream_by_name: Vec<Option<Stream>>,
    streams: Vec<Vec<Stream>>,
    stream_state: Vec<Option<StreamState>>,
    link_by_name: Vec<Option<Link>>,
    links: Vec<Vec<Link>>,
    links_with_pointers: Vec<Vec<LinkHead>>,
    partition_index: Vec<Option<PartitionIndex>>,
    lease: Vec<Option<Lease>>,
    pointer: Vec<Option<Pointer>>,
    collection: Vec<Option<Collection>>,
    resolve_collection: Vec<Option<Collection>>,
    collection_for_link: Vec<Option<Collection>>,
    collections: Vec<Vec<Collection>>,
    aliases: Vec<Vec<(String, CollectionId)>>,
    collection_head: Vec<Option<CollectionHead>>,
    collection_heads: Vec<Vec<CollectionHead>>,
    retired_expired: Vec<Vec<String>>,
    orphan_wal_objects: Vec<Vec<String>>,
    orphan_segments: Vec<Vec<String>>,
    segment_referenced: Vec<bool>,
    collection_roots: Vec<CollectionRoots>,
}

/// Every trait read, at `consistency` where the method takes one.
async fn trait_reads(meta: &dyn MetaStore, c: Consistency, q: &Queries) -> Reads {
    let mut r = Reads {
        clock_ms: meta.clock_ms(c).await.expect("read"),
        namespace_by_name: Vec::new(),
        namespaces: meta.namespaces(c).await.expect("read"),
        stream: Vec::new(),
        stream_by_name: Vec::new(),
        streams: Vec::new(),
        stream_state: Vec::new(),
        link_by_name: Vec::new(),
        links: Vec::new(),
        links_with_pointers: Vec::new(),
        partition_index: Vec::new(),
        lease: Vec::new(),
        pointer: Vec::new(),
        collection: Vec::new(),
        resolve_collection: Vec::new(),
        collection_for_link: Vec::new(),
        collections: Vec::new(),
        aliases: Vec::new(),
        collection_head: Vec::new(),
        collection_heads: Vec::new(),
        retired_expired: Vec::new(),
        orphan_wal_objects: Vec::new(),
        orphan_segments: Vec::new(),
        segment_referenced: Vec::new(),
        collection_roots: Vec::new(),
    };
    for name in &q.namespace_names {
        r.namespace_by_name
            .push(meta.namespace_by_name(c, name).await.expect("read"));
    }
    for &id in &q.streams {
        r.stream.push(meta.stream(c, id).await.expect("read"));
        r.stream_state
            .push(meta.stream_state(c, id).await.expect("read"));
    }
    for (ns, name) in &q.stream_names {
        r.stream_by_name
            .push(meta.stream_by_name(c, *ns, name).await.expect("read"));
    }
    for scope in q.scopes() {
        r.streams.push(meta.streams(c, scope).await.expect("read"));
        r.links.push(meta.links(c, scope).await.expect("read"));
        r.collections
            .push(meta.collections(c, scope).await.expect("read"));
        r.collection_heads
            .push(meta.collection_heads(c, scope).await.expect("read"));
    }
    for &ns in &q.namespaces {
        r.links_with_pointers
            .push(meta.links_with_pointers(c, ns).await.expect("read"));
        r.aliases.push(meta.aliases(c, ns).await.expect("read"));
    }
    for (ns, name) in &q.link_names {
        r.link_by_name
            .push(meta.link_by_name(c, *ns, name).await.expect("read"));
    }
    for &(stream, partition, from, max) in &q.index {
        r.partition_index.push(
            meta.partition_index(c, stream, partition, from, max)
                .await
                .expect("read"),
        );
    }
    for key in &q.leases {
        r.lease.push(meta.lease(c, key).await.expect("read"));
    }
    for (ns, key) in &q.pointers {
        r.pointer
            .push(meta.pointer(c, *ns, key).await.expect("read"));
    }
    for &id in &q.collections {
        r.collection
            .push(meta.collection(c, id).await.expect("read"));
        r.collection_head
            .push(meta.collection_head(c, id).await.expect("read"));
    }
    for (ns, name) in &q.resolve {
        r.resolve_collection
            .push(meta.resolve_collection(c, *ns, name).await.expect("read"));
    }
    for &link in &q.links {
        r.collection_for_link
            .push(meta.collection_for_link(c, link).await.expect("read"));
    }
    for &grace in &q.graces {
        r.retired_expired
            .push(meta.retired_expired(grace).await.expect("read"));
    }
    for &(age, limit) in &q.wal_ages {
        r.orphan_wal_objects.push(
            meta.orphan_wal_objects(q.wal_candidates.clone(), age, limit)
                .await
                .expect("read"),
        );
    }
    for &(ns, age, limit) in &q.segment_ages {
        r.orphan_segments.push(
            meta.orphan_segments(ns, q.segment_candidates.clone(), age, limit)
                .await
                .expect("read"),
        );
    }
    for &(stream, partition, object) in &q.referenced {
        r.segment_referenced.push(
            meta.segment_referenced(stream, partition, object)
                .await
                .expect("read"),
        );
    }
    for (ns, under) in &q.roots {
        r.collection_roots
            .push(meta.collection_roots(*ns, under).await.expect("read"));
    }
    r
}

/// The same reads, computed from `MetaState` queries in one closure.
fn state_reads(s: &MetaState, q: &Queries) -> Reads {
    let head = |c: &Collection| {
        let bound = |f: fn(&operon_meta::PartitionState) -> u64| -> Vec<u64> {
            (0..c.partitions)
                .map(|p| s.partition(c.stream, p).map_or(0, f))
                .collect()
        };
        CollectionHead {
            collection: c.clone(),
            pointer: s
                .pointer(c.namespace, &collection_pointer_key(c.id))
                .cloned(),
            log_start_offsets: bound(operon_meta::PartitionState::log_start_offset),
            high_watermarks: bound(operon_meta::PartitionState::high_watermark),
            clock_ms: s.clock_ms(),
        }
    };
    let retired: Vec<&str> = s.retired().map(|(p, _)| p).collect();
    Reads {
        clock_ms: s.clock_ms(),
        namespace_by_name: q
            .namespace_names
            .iter()
            .map(|n| s.namespace_by_name(n).cloned())
            .collect(),
        namespaces: s.namespaces().cloned().collect(),
        stream: q.streams.iter().map(|&id| s.stream(id).cloned()).collect(),
        stream_by_name: q
            .stream_names
            .iter()
            .map(|(ns, n)| s.stream_by_name(*ns, n).cloned())
            .collect(),
        streams: q
            .scopes()
            .into_iter()
            .map(|scope| match scope {
                Some(ns) => s.streams(ns).cloned().collect(),
                None => s.all_streams().cloned().collect(),
            })
            .collect(),
        stream_state: q
            .streams
            .iter()
            .map(|&id| {
                let stream = s.stream(id)?;
                Some(StreamState {
                    stream: stream.clone(),
                    partitions: (0..stream.partitions)
                        .map(|p| {
                            s.partition(id, p).map(|ps| PartitionBounds {
                                log_start_offset: ps.log_start_offset(),
                                high_watermark: ps.high_watermark(),
                                bytes: ps.bytes(),
                            })
                        })
                        .collect(),
                })
            })
            .collect(),
        link_by_name: q
            .link_names
            .iter()
            .map(|(ns, n)| s.link_by_name(*ns, n).cloned())
            .collect(),
        links: q
            .scopes()
            .into_iter()
            .map(|scope| match scope {
                Some(ns) => s.links(ns).cloned().collect(),
                None => s.all_links().cloned().collect(),
            })
            .collect(),
        links_with_pointers: q
            .namespaces
            .iter()
            .map(|&ns| {
                s.links(ns)
                    .map(|l| LinkHead {
                        link: l.clone(),
                        pointer: s.pointer(ns, &link_pointer_key(l.id)).cloned(),
                    })
                    .collect()
            })
            .collect(),
        partition_index: q
            .index
            .iter()
            .map(|&(stream, partition, from, max)| {
                s.stream(stream)?;
                let ps = s.partition(stream, partition)?;
                let mut entries = Vec::new();
                let mut bytes = 0;
                for e in ps.entries_from(from) {
                    if max.is_some_and(|max| !entries.is_empty() && bytes >= max) {
                        break;
                    }
                    bytes += e.byte_range.end - e.byte_range.start;
                    entries.push(e.clone());
                }
                Some(PartitionIndex::new(
                    ps.log_start_offset(),
                    ps.next_offset(),
                    ps.bytes(),
                    entries,
                ))
            })
            .collect(),
        lease: q.leases.iter().map(|k| s.lease(k).cloned()).collect(),
        pointer: q
            .pointers
            .iter()
            .map(|(ns, k)| s.pointer(*ns, k).cloned())
            .collect(),
        collection: q
            .collections
            .iter()
            .map(|&id| s.collection(id).cloned())
            .collect(),
        resolve_collection: q
            .resolve
            .iter()
            .map(|(ns, n)| s.resolve_collection(*ns, n).cloned())
            .collect(),
        collection_for_link: q
            .links
            .iter()
            .map(|&l| s.collection_for_link(l).cloned())
            .collect(),
        collections: q
            .scopes()
            .into_iter()
            .map(|scope| match scope {
                Some(ns) => s.collections(ns).cloned().collect(),
                None => s.all_collections().cloned().collect(),
            })
            .collect(),
        aliases: q
            .namespaces
            .iter()
            .map(|&ns| s.aliases(ns).map(|(a, id)| (a.to_string(), id)).collect())
            .collect(),
        collection_head: q
            .collections
            .iter()
            .map(|&id| s.collection(id).map(&head))
            .collect(),
        collection_heads: q
            .scopes()
            .into_iter()
            .map(|scope| match scope {
                Some(ns) => s.collections(ns).map(&head).collect(),
                None => s.all_collections().map(&head).collect(),
            })
            .collect(),
        retired_expired: q
            .graces
            .iter()
            .map(|&grace| {
                s.retired()
                    .filter(|(_, at)| at.saturating_add(grace) <= s.clock_ms())
                    .map(|(p, _)| p.to_string())
                    .collect()
            })
            .collect(),
        orphan_wal_objects: q
            .wal_ages
            .iter()
            .map(|&(age, limit)| {
                q.wal_candidates
                    .iter()
                    .filter(|(p, created)| {
                        created.saturating_add(age) <= s.clock_ms()
                            && s.wal_live_chunks(p).is_none()
                            && !retired.contains(&p.as_str())
                    })
                    .map(|(p, _)| p.clone())
                    .take(limit)
                    .collect()
            })
            .collect(),
        orphan_segments: q
            .segment_ages
            .iter()
            .map(|&(ns, age, limit)| {
                let indexed = |p: &str| {
                    s.streams(ns).any(|st| {
                        (0..st.partitions).any(|part| {
                            s.partition(st.id, part)
                                .is_some_and(|ps| ps.entries().any(|e| e.object == p))
                        })
                    })
                };
                q.segment_candidates
                    .iter()
                    .filter(|(p, created)| {
                        created.saturating_add(age) <= s.clock_ms()
                            && !retired.contains(&p.as_str())
                            && !indexed(p)
                    })
                    .map(|(p, _)| p.clone())
                    .take(limit)
                    .collect()
            })
            .collect(),
        segment_referenced: q
            .referenced
            .iter()
            .map(|&(stream, partition, object)| {
                retired.contains(&object)
                    || s.partition(stream, partition)
                        .is_some_and(|ps| ps.entries().any(|e| e.object == object))
            })
            .collect(),
        collection_roots: q
            .roots
            .iter()
            .map(|(ns, under)| CollectionRoots {
                clock_ms: s.clock_ms(),
                collections: s
                    .collections(*ns)
                    .map(|c| {
                        let key = collection_pointer_key(c.id);
                        (c.clone(), s.pointer(*ns, &key).cloned())
                    })
                    .collect(),
                retired_prefixes: retired
                    .iter()
                    .filter(|p| p.starts_with(under.as_str()) && p.ends_with('/'))
                    .map(|p| p.to_string())
                    .collect(),
            })
            .collect(),
    }
}

#[tokio::test]
async fn composite_reads_equal_one_state_machine_closure() {
    let single = single().await;
    let meta = single.meta();

    // Two namespaces; streams of each class, one with retention.
    let a = meta.create_namespace("acme").await.unwrap();
    let b = meta.create_namespace("globex").await.unwrap();
    let retention = Retention {
        max_age_ms: Some(60_000),
        max_bytes: Some(1_000),
    };
    let std_s = meta
        .create_stream(a, "std", 3, WalClass::Standard, retention)
        .await
        .unwrap();
    let exp = meta
        .create_stream(a, "exp", 1, WalClass::Express, Retention::default())
        .await
        .unwrap();
    let quo = meta
        .create_stream(b, "quo", 2, WalClass::Quorum, Retention::default())
        .await
        .unwrap();

    // WAL commits over several partitions, a segment swap, a mid-entry trim.
    let seg = format!("ns/{a}/streams/{std_s}/0/a.seg");
    commit(
        meta,
        "wal/1.wal",
        vec![
            chunk(std_s, 0, 10, 0..100),
            chunk(std_s, 1, 5, 100..150),
            chunk(exp, 0, 3, 150..180),
        ],
    )
    .await;
    commit(
        meta,
        "wal/2.wal",
        vec![
            chunk(std_s, 0, 10, 0..200),
            chunk(std_s, 2, 4, 200..240),
            chunk(quo, 1, 7, 240..310),
        ],
    )
    .await;
    commit(meta, "wal/3.wal", vec![chunk(std_s, 0, 10, 0..50)]).await;
    swap(meta, std_s, 0, &[(0, "wal/1.wal")], &seg).await;
    assert_eq!(meta.trim_partition(std_s, 0, 15, None).await.unwrap(), 15);

    // Links of kinds `counter` and `other`, with `link/<id>` pointers.
    let counts = meta
        .create_link(
            a,
            "counts",
            std_s,
            target("counter", "counts"),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let options = BTreeMap::from([("mode".to_string(), "fast".to_string())]);
    let mirror = meta
        .create_link(a, "mirror", exp, target("other", "mirror"), options)
        .await
        .unwrap();
    let far = meta
        .create_link(b, "far", quo, target("counter", "far"), BTreeMap::new())
        .await
        .unwrap();
    let counts_key = link_pointer_key(counts);
    set_pointer(meta, a, &counts_key, None, "ns/1/links/1/m-1").await;
    set_pointer(meta, a, &counts_key, Some(1), "ns/1/links/1/m-2").await;
    set_pointer(meta, b, &link_pointer_key(far), None, "ns/2/links/3/m-1").await;

    // Collections, an alias, one dropped collection.
    let (docs, docs_stream, docs_link) = meta
        .create_collection(a, "docs", schema(), 2)
        .await
        .unwrap();
    let (notes, _, notes_link) = meta
        .create_collection(a, "notes", schema(), 1)
        .await
        .unwrap();
    let (items, _, items_link) = meta
        .create_collection(b, "items", schema(), 1)
        .await
        .unwrap();
    meta.update_aliases(
        a,
        vec![AliasAction::Create {
            alias: "latest".to_string(),
            collection: "docs".to_string(),
        }],
    )
    .await
    .unwrap();
    commit(meta, "wal/4.wal", vec![chunk(docs_stream, 1, 6, 0..60)]).await;
    let docs_key = collection_pointer_key(docs);
    set_pointer(meta, a, &docs_key, None, "ns/1/collections/1/m-1").await;
    assert_eq!(meta.drop_collection(a, "notes").await.unwrap(), Some(notes));

    // Held and released leases.
    let ttl = Duration::from_secs(60);
    meta.acquire_lease("task/held", "w1", ttl).await.unwrap();
    let grant = meta
        .acquire_lease("task/released", "w2", ttl)
        .await
        .unwrap();
    meta.release_lease("task/released", "w2", grant.epoch)
        .await
        .unwrap();

    let queries = Queries {
        namespace_names: vec!["acme", "globex", "nope"],
        namespaces: vec![a, b],
        streams: vec![std_s, exp, quo, docs_stream, StreamId(99)],
        stream_names: vec![
            (a, "std".to_string()),
            (a, "exp".to_string()),
            (b, "quo".to_string()),
            (b, "std".to_string()),
            (a, implicit_name("docs", docs)),
        ],
        link_names: vec![(a, "counts"), (a, "mirror"), (b, "far"), (a, "far")],
        index: vec![
            (std_s, 0, 0, None),
            (std_s, 0, 12, Some(1)),
            (std_s, 0, 0, Some(150)),
            (std_s, 1, 0, None),
            (std_s, 2, 3, Some(0)),
            (std_s, 5, 0, None),
            (exp, 0, 0, None),
            (docs_stream, 1, 3, Some(10)),
            (StreamId(99), 0, 0, None),
        ],
        leases: vec!["task/held", "task/released", "task/none"],
        pointers: vec![
            (a, counts_key.clone()),
            (b, link_pointer_key(far)),
            (a, docs_key.clone()),
            (a, collection_pointer_key(notes)),
            (b, counts_key.clone()),
        ],
        collections: vec![docs, notes, items, CollectionId(99)],
        resolve: vec![
            (a, "docs"),
            (a, "latest"),
            (a, "notes"),
            (b, "items"),
            (b, "latest"),
        ],
        links: vec![
            counts,
            mirror,
            far,
            docs_link,
            notes_link,
            items_link,
            LinkId(99),
        ],
        graces: vec![0, 1, u64::MAX],
        wal_candidates: vec![
            ("wal/1.wal".to_string(), 0),
            ("wal/9.wal".to_string(), 0),
            (seg.clone(), 0),
            ("wal/8.wal".to_string(), T0),
            ("wal/7.wal".to_string(), 0),
        ],
        wal_ages: vec![(0, 10), (1_000, 10), (0, 1)],
        segment_candidates: vec![
            (seg.clone(), 0),
            ("wal/2.wal".to_string(), 0),
            (format!("ns/{a}/streams/{std_s}/0/x.seg"), 0),
            ("wal/1.wal".to_string(), T0),
        ],
        segment_ages: vec![(a, 0, 10), (b, 0, 10), (a, 1_000, 10), (a, 0, 1)],
        referenced: vec![
            (std_s, 0, "ns/1/streams/1/0/a.seg"),
            (std_s, 0, "wal/2.wal"),
            (std_s, 1, "wal/2.wal"),
            (std_s, 1, "wal/1.wal"),
            (std_s, 0, "wal/9.wal"),
            (StreamId(99), 0, "wal/9.wal"),
        ],
        roots: vec![
            (a, format!("ns/{a}/collections/")),
            (a, format!("ns/{a}/pk/")),
            (b, format!("ns/{b}/collections/")),
            (a, String::new()),
        ],
    };

    let expected = single
        .client
        .read(Consistency::Local, |s| state_reads(s, &queries))
        .await
        .unwrap();
    // The session left something to compare in every composite read.
    assert_eq!(expected.retired_expired[0].len(), 3, "{expected:?}");
    assert!(expected.links_with_pointers[0][0].pointer.is_some());
    assert!(
        expected.collection_head[0]
            .as_ref()
            .unwrap()
            .pointer
            .is_some()
    );
    assert_eq!(expected.collection_roots[0].retired_prefixes.len(), 1);
    assert_eq!(
        expected.partition_index[0]
            .as_ref()
            .unwrap()
            .log_start_offset(),
        15
    );
    for consistency in [Consistency::Local, Consistency::Linearizable] {
        let reads = trait_reads(meta, consistency, &queries).await;
        assert_eq!(reads, expected, "{consistency:?}");
    }
    single.shutdown().await;
}

// ----- The offset index -----

/// Stream 1 of namespace `acme` (one partition) with three 10-record, 100-byte
/// entries at offsets 0, 10 and 20.
async fn three_entries(single: &Single) -> StreamId {
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.expect("namespace");
    let stream = meta
        .create_stream(ns, "events", 1, WalClass::Standard, Retention::default())
        .await
        .expect("stream");
    for i in 0..3 {
        commit(
            meta,
            &format!("wal/{i}.wal"),
            vec![chunk(stream, 0, 10, 0..100)],
        )
        .await;
    }
    stream
}

fn bases(index: &PartitionIndex) -> Vec<u64> {
    index.entries().map(|e| e.base_offset).collect()
}

#[tokio::test]
async fn partition_index_pages_like_the_fetch_plan() {
    let single = single().await;
    let stream = three_entries(&single).await;
    let meta = single.meta();
    let page = |from, max| meta.partition_index(Consistency::Local, stream, 0, from, max);

    // A budget smaller than the first entry still returns that entry.
    let index = page(0, Some(10)).await.unwrap().unwrap();
    assert_eq!(bases(&index), [0]);
    // A budget that ends inside an entry includes that entry.
    let index = page(0, Some(150)).await.unwrap().unwrap();
    assert_eq!(bases(&index), [0, 10]);
    // A budget met exactly stops there.
    let index = page(0, Some(200)).await.unwrap().unwrap();
    assert_eq!(bases(&index), [0, 10]);
    // No budget: every entry from the offset, starting with the one holding it.
    let index = page(0, None).await.unwrap().unwrap();
    assert_eq!(bases(&index), [0, 10, 20]);
    let index = page(15, None).await.unwrap().unwrap();
    assert_eq!(bases(&index), [10, 20]);
    // The bounds are the whole partition's, whatever the page.
    let index = page(15, Some(1)).await.unwrap().unwrap();
    assert_eq!(bases(&index), [10]);
    assert_eq!(
        (
            index.log_start_offset(),
            index.next_offset(),
            index.high_watermark()
        ),
        (0, 30, 30)
    );
    assert_eq!(index.bytes(), 300);
    // From the high watermark: no entries.
    let index = page(30, None).await.unwrap().unwrap();
    assert_eq!(index.entries().count(), 0);
    single.shutdown().await;
}

#[tokio::test]
async fn partition_index_from_zero_equals_every_entry_after_a_mid_entry_trim() {
    let single = single().await;
    let stream = three_entries(&single).await;
    let meta = single.meta();
    assert_eq!(meta.trim_partition(stream, 0, 15, None).await.unwrap(), 15);
    let index = meta
        .partition_index(Consistency::Local, stream, 0, 0, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(index.log_start_offset(), 15);
    assert_eq!(bases(&index), [10, 20]);
    let entries = all_entries(&single.client, stream, 0).await;
    assert_eq!(index.clone().into_entries(), entries);
    assert_eq!(index.bytes(), 200);
    single.shutdown().await;
}

#[tokio::test]
async fn partition_index_of_an_unknown_partition_or_stream_is_none() {
    let single = single().await;
    let stream = three_entries(&single).await;
    let meta = single.meta();
    for c in [Consistency::Local, Consistency::Linearizable] {
        let unknown_partition = meta.partition_index(c, stream, 1, 0, None).await.unwrap();
        assert_eq!(unknown_partition, None);
        let unknown_stream = meta
            .partition_index(c, StreamId(99), 0, 0, None)
            .await
            .unwrap();
        assert_eq!(unknown_stream, None);
    }
    single.shutdown().await;
}

// ----- Garbage collection queries -----

#[tokio::test]
async fn orphan_wal_objects_follows_gc_pass_two() {
    let single = single().await;
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let stream = meta
        .create_stream(ns, "events", 1, WalClass::Standard, Retention::default())
        .await
        .unwrap();
    commit(meta, "wal/live.wal", vec![chunk(stream, 0, 10, 0..100)]).await;
    commit(meta, "wal/ret.wal", vec![chunk(stream, 0, 10, 0..100)]).await;
    // Its only chunk is segmented: the object is retired.
    swap(
        meta,
        stream,
        0,
        &[(10, "wal/ret.wal")],
        "ns/1/streams/1/0/s.seg",
    )
    .await;
    single.tick(T0 + 5_000).await;

    let candidates: Vec<(String, u64)> = [
        ("wal/orphan-2.wal", T0),
        ("wal/young.wal", T0 + 4_500),
        ("wal/live.wal", T0),
        ("wal/orphan-1.wal", T0),
        ("wal/ret.wal", T0),
        ("wal/orphan-3.wal", T0),
    ]
    .into_iter()
    .map(|(p, t)| (p.to_string(), t))
    .collect();
    let orphans = meta
        .orphan_wal_objects(candidates.clone(), 1_000, 10)
        .await
        .unwrap();
    assert_eq!(
        orphans,
        ["wal/orphan-2.wal", "wal/orphan-1.wal", "wal/orphan-3.wal"]
    );
    let capped = meta
        .orphan_wal_objects(candidates.clone(), 1_000, 2)
        .await
        .unwrap();
    assert_eq!(capped, ["wal/orphan-2.wal", "wal/orphan-1.wal"]);
    // Younger than `min_age_ms` by the metastore clock: kept.
    let too_young = meta
        .orphan_wal_objects(candidates, 6_000, 10)
        .await
        .unwrap();
    assert!(too_young.is_empty(), "{too_young:?}");
    single.shutdown().await;
}

#[tokio::test]
async fn orphan_segments_follows_gc_pass_three() {
    let single = single().await;
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let stream = meta
        .create_stream(ns, "events", 1, WalClass::Standard, Retention::default())
        .await
        .unwrap();
    for i in 0..3 {
        commit(
            meta,
            &format!("wal/{i}.wal"),
            vec![chunk(stream, 0, 10, 0..100)],
        )
        .await;
    }
    let seg = |name: &str| format!("ns/{ns}/streams/{stream}/0/{name}.seg");
    swap(meta, stream, 0, &[(0, "wal/0.wal")], &seg("retired")).await;
    // Trims the first segment: it is retired.
    meta.trim_partition(stream, 0, 10, None).await.unwrap();
    swap(meta, stream, 0, &[(10, "wal/1.wal")], &seg("indexed")).await;
    single.tick(T0 + 5_000).await;

    let candidates = vec![
        (seg("indexed"), T0),
        (seg("retired"), T0),
        (seg("young"), T0 + 4_500),
        (seg("orphan"), T0),
    ];
    let orphans = meta
        .orphan_segments(ns, candidates, 1_000, 10)
        .await
        .unwrap();
    assert_eq!(orphans, [seg("orphan")]);
    single.shutdown().await;
}

#[tokio::test]
async fn retired_expired_uses_the_metastore_clock() {
    let single = single().await;
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let stream = meta
        .create_stream(ns, "events", 1, WalClass::Standard, Retention::default())
        .await
        .unwrap();
    commit(meta, "wal/a.wal", vec![chunk(stream, 0, 10, 0..100)]).await;
    let t = T0 + 1_000;
    single.clock.set(t);
    // Retires `wal/a.wal` at metastore clock `t`.
    swap(
        meta,
        stream,
        0,
        &[(0, "wal/a.wal")],
        "ns/1/streams/1/0/a.seg",
    )
    .await;
    let grace = 10_000;
    single.tick(t + grace - 1).await;
    assert!(meta.retired_expired(grace).await.unwrap().is_empty());
    single.tick(t + grace).await;
    assert_eq!(meta.retired_expired(grace).await.unwrap(), ["wal/a.wal"]);
    single.shutdown().await;
}

#[tokio::test]
async fn segment_referenced_sees_index_entries_and_retired_objects() {
    let single = single().await;
    let stream = three_entries(&single).await;
    let meta = single.meta();
    let seg = "ns/1/streams/1/0/s.seg";
    // `wal/0.wal` is retired, `s.seg` indexed, `wal/1.wal` still a WAL entry.
    swap(meta, stream, 0, &[(0, "wal/0.wal")], seg).await;
    assert!(meta.segment_referenced(stream, 0, seg).await.unwrap());
    assert!(
        meta.segment_referenced(stream, 0, "wal/0.wal")
            .await
            .unwrap()
    );
    assert!(
        meta.segment_referenced(stream, 0, "wal/1.wal")
            .await
            .unwrap()
    );
    assert!(
        !meta
            .segment_referenced(stream, 0, "wal/9.wal")
            .await
            .unwrap()
    );
    // Indexed only in partition 0 of stream 1.
    assert!(!meta.segment_referenced(StreamId(99), 0, seg).await.unwrap());
    // Retired objects count for any partition.
    assert!(
        meta.segment_referenced(StreamId(99), 0, "wal/0.wal")
            .await
            .unwrap()
    );
    single.shutdown().await;
}

#[tokio::test]
async fn collection_head_carries_pointer_bounds_and_clock() {
    let single = single().await;
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let (cid, stream, _) = meta
        .create_collection(ns, "docs", schema(), 2)
        .await
        .unwrap();
    let head = meta
        .collection_head(Consistency::Linearizable, cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.pointer, None);
    assert_eq!(head.log_start_offsets, [0, 0]);
    assert_eq!(head.high_watermarks, [0, 0]);

    commit(meta, "wal/1.wal", vec![chunk(stream, 1, 5, 0..50)]).await;
    let key = collection_pointer_key(cid);
    set_pointer(meta, ns, &key, None, "ns/1/collections/1/m-1").await;
    single.clock.set(T0 + 777);
    assert_eq!(meta.trim_partition(stream, 1, 2, None).await.unwrap(), 2);
    let head = meta
        .collection_head(Consistency::Linearizable, cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.collection.id, cid);
    assert_eq!(
        head.pointer,
        Some(Pointer {
            version: 1,
            value: "ns/1/collections/1/m-1".to_string()
        })
    );
    assert_eq!(head.log_start_offsets, [0, 2]);
    assert_eq!(head.high_watermarks, [0, 5]);
    assert_eq!(head.clock_ms, T0 + 777);
    assert_eq!(
        head.clock_ms,
        meta.clock_ms(Consistency::Local).await.unwrap()
    );
    let heads = meta
        .collection_heads(Consistency::Local, Some(ns))
        .await
        .unwrap();
    assert_eq!(heads, [head]);

    meta.drop_collection(ns, "docs").await.unwrap();
    assert_eq!(
        meta.collection_head(Consistency::Local, cid).await.unwrap(),
        None
    );
    single.shutdown().await;
}

#[tokio::test]
async fn collection_roots_lists_only_retired_prefixes_under_the_path() {
    let single = single().await;
    let meta = single.meta();
    let a = meta.create_namespace("acme").await.unwrap();
    let b = meta.create_namespace("globex").await.unwrap();
    let (gone, _, _) = meta
        .create_collection(a, "gone", schema(), 1)
        .await
        .unwrap();
    let (kept, _, _) = meta
        .create_collection(a, "kept", schema(), 1)
        .await
        .unwrap();
    let (other, _, _) = meta
        .create_collection(b, "other", schema(), 1)
        .await
        .unwrap();
    let key = collection_pointer_key(kept);
    set_pointer(meta, a, &key, None, "ns/1/collections/2/m-1").await;
    meta.drop_collection(a, "gone").await.unwrap();
    meta.drop_collection(b, "other").await.unwrap();
    // A retired object (not a prefix) under the collections path.
    let stream = meta
        .create_stream(a, "events", 1, WalClass::Standard, Retention::default())
        .await
        .unwrap();
    commit(meta, "wal/1.wal", vec![chunk(stream, 0, 10, 0..100)]).await;
    let stray = format!("ns/{a}/collections/stray.seg");
    swap(meta, stream, 0, &[(0, "wal/1.wal")], &stray).await;
    meta.trim_partition(stream, 0, 10, None).await.unwrap();
    let clock_ms = meta.clock_ms(Consistency::Linearizable).await.unwrap();

    let roots = meta
        .collection_roots(a, &format!("ns/{a}/collections/"))
        .await
        .unwrap();
    assert_eq!(roots.clock_ms, clock_ms);
    let kept_collection = meta
        .collection(Consistency::Local, kept)
        .await
        .unwrap()
        .unwrap();
    let pointer = meta.pointer(Consistency::Local, a, &key).await.unwrap();
    assert!(pointer.is_some());
    assert_eq!(roots.collections, [(kept_collection, pointer)]);
    assert_eq!(
        roots.retired_prefixes,
        [format!("ns/{a}/collections/{gone}/")]
    );
    let pk = meta
        .collection_roots(a, &format!("ns/{a}/pk/"))
        .await
        .unwrap();
    assert_eq!(
        pk.retired_prefixes,
        [format!("ns/{a}/pk/collection-{gone}/")]
    );
    let theirs = meta
        .collection_roots(b, &format!("ns/{b}/collections/"))
        .await
        .unwrap();
    assert!(theirs.collections.is_empty());
    assert_eq!(
        theirs.retired_prefixes,
        [format!("ns/{b}/collections/{other}/")]
    );
    single.shutdown().await;
}

// ----- Tracked writes -----

#[tokio::test]
async fn tracked_writes_report_an_earlier_unknown_attempt() {
    let single = single().await;
    let meta = single.meta();
    let ns = meta.create_namespace("acme").await.unwrap();
    let stream = meta
        .create_stream(ns, "events", 2, WalClass::Standard, Retention::default())
        .await
        .unwrap();
    let wal = WalCommit {
        object: "wal/1.wal".to_string(),
        created_at_ms: T0,
        chunks: vec![chunk(stream, 0, 10, 0..100), chunk(stream, 1, 3, 100..130)],
    };

    single.client.inject_lost_ack();
    let first = meta.commit_wal(wal.clone()).await;
    assert!(first.earlier_unknown);
    let offsets = first.result.unwrap();
    assert_eq!(offsets, [0, 0]);
    let again = meta.commit_wal(wal).await;
    assert!(!again.earlier_unknown);
    assert_eq!(again.result.unwrap(), offsets);

    single.client.inject_lost_ack();
    let lost = meta.cas_pointer(cas(ns, "p", None, "mine")).await;
    assert!(lost.earlier_unknown);
    let own = Pointer {
        version: 1,
        value: "mine".to_string(),
    };
    match lost.result {
        Err(MetaError::Rejected(ApplyError::VersionMismatch { current })) => {
            assert_eq!(current, Some(own));
        }
        other => panic!("expected VersionMismatch, got {other:?}"),
    }

    // Without an injected loss, nothing earlier is unknown.
    let next = meta.cas_pointer(cas(ns, "p", Some(1), "next")).await;
    assert!(!next.earlier_unknown);
    assert_eq!(next.result.unwrap(), 2);
    let plain = meta
        .commit_wal(WalCommit {
            object: "wal/2.wal".to_string(),
            created_at_ms: T0,
            chunks: vec![chunk(stream, 0, 1, 0..10)],
        })
        .await;
    assert!(!plain.earlier_unknown);
    assert_eq!(plain.result.unwrap(), [10]);
    let rejected = meta.cas_pointer(cas(ns, "p", Some(1), "stale")).await;
    assert!(!rejected.earlier_unknown);
    assert!(matches!(
        rejected.result,
        Err(MetaError::Rejected(ApplyError::VersionMismatch { .. }))
    ));
    single.shutdown().await;
}

// ----- Changes and readiness -----

#[tokio::test]
async fn watch_changes_is_armed_at_creation() {
    let single = single().await;
    let meta = single.meta();
    let mut changes = meta.watch_changes();
    // Applied before anyone waits: the watch still sees it.
    meta.create_namespace("acme").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), changes.changed())
        .await
        .expect("the watch saw the change")
        .expect("the metastore runs");
    single.shutdown().await;
}

#[tokio::test]
async fn watch_changes_ends_when_the_node_stops() {
    let single = single().await;
    let mut changes = single.meta().watch_changes();
    single.node.shutdown().await.expect("shutdown");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let result = tokio::time::timeout(remaining, changes.changed())
            .await
            .expect("the watch ended");
        // Spurious wake-ups are allowed; the stop must follow.
        if result.is_err() {
            break;
        }
    }
    assert!(changes.changed().await.is_err());
}

#[tokio::test]
async fn is_ready_once_the_single_node_leads() {
    let dir = TempDir::new().expect("temp dir");
    let node = MetaNode::start(
        MetaConfig::new(1, dir.path(), Store::in_memory()),
        &Router::new(),
    )
    .await
    .expect("start");
    let client = MetaClient::new(
        node.clone(),
        Vec::new(),
        Arc::new(SystemClock),
        MetaClientConfig::default(),
    );
    let meta: &dyn MetaStore = &client;
    assert!(!meta.is_ready(), "not initialized: no leader");
    node.initialize([1]).await.expect("initialize");
    node.wait_for_leader(WAIT).await.expect("leader");
    let deadline = Instant::now() + WAIT;
    while !meta.is_ready() {
        assert!(Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_metaclient_converts_into_a_shared_store() {
    let single = single().await;
    let owned: Arc<dyn MetaStore> = single.client.clone().into();
    let borrowed: Arc<dyn MetaStore> = (&single.client).into();
    let ns = owned.create_namespace("acme").await.unwrap();
    assert_eq!(
        borrowed
            .namespace_by_name(Consistency::Local, "acme")
            .await
            .unwrap()
            .map(|n| n.id),
        Some(ns)
    );
    // A loss injected on the concrete client is seen through both objects.
    single.client.inject_lost_ack();
    let lost = borrowed.cas_pointer(cas(ns, "p", None, "v")).await;
    assert!(lost.earlier_unknown);
    single.client.inject_lost_ack();
    let lost = owned.cas_pointer(cas(ns, "q", None, "v")).await;
    assert!(lost.earlier_unknown);
    single.shutdown().await;
}

// ----- Three nodes -----

struct Cluster {
    _router: Router,
    _dirs: Vec<TempDir>,
    nodes: Vec<MetaNode>,
}

impl Cluster {
    async fn start() -> Self {
        let router = Router::new();
        let store = Store::in_memory();
        let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("temp dir")).collect();
        let mut nodes = Vec::new();
        for (id, dir) in (1..=3).zip(&dirs) {
            let config = MetaConfig::new(id, dir.path(), store.clone());
            nodes.push(MetaNode::start(config, &router).await.expect("start"));
        }
        nodes[0].initialize([1, 2, 3]).await.expect("initialize");
        for node in &nodes {
            node.wait_for_leader(Duration::from_secs(20))
                .await
                .expect("leader");
        }
        Self {
            _router: router,
            _dirs: dirs,
            nodes,
        }
    }

    /// A client whose local node is `local`, with the others as peers.
    fn client(&self, local: usize) -> MetaClient {
        let peers = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != local)
            .map(|(_, n)| n.clone())
            .collect();
        MetaClient::new(
            self.nodes[local].clone(),
            peers,
            Arc::new(SystemClock),
            MetaClientConfig::default(),
        )
    }

    async fn shutdown(self) {
        for node in &self.nodes {
            node.shutdown().await.expect("shutdown");
        }
    }
}

#[tokio::test]
async fn trait_reads_through_a_follower_client_are_linearizable_when_asked() {
    let cluster = Cluster::start().await;
    let writer: Arc<dyn MetaStore> = cluster.client(0).into();
    let reader: Arc<dyn MetaStore> = cluster.client(2).into();
    let ns = writer.create_namespace("acme").await.unwrap();
    let mut version = None;
    for i in 0..20 {
        let value = format!("v{i}");
        let written = writer
            .cas_pointer(cas(ns, "p", version, &value))
            .await
            .into_result()
            .unwrap();
        version = Some(written);
        let read = reader
            .pointer(Consistency::Linearizable, ns, "p")
            .await
            .unwrap();
        assert_eq!(
            read,
            Some(Pointer {
                version: written,
                value,
            }),
            "iteration {i}"
        );
    }
    cluster.shutdown().await;
}
