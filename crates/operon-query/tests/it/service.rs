//! `CollectionService` (plan M1.2 Task 9): collections, schemas, aliases,
//! writes with existence reporting, atomic validation and dynamic mapping,
//! versions, pins, forwarding and the hot switch.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::common::{Fixture, doc, field, obj, patch, tail_schema, upsert, vector};
use operon_collection::{
    CollectionSchema, ConsistencyToken, DocOp, Document, DynamicMapping, FieldKind, PatchMode,
    PrimaryKey, SparseModifier, SparseVector, SparseVectorSpec, partition_of,
};
use operon_common::meta::{AliasAction, Consistency, MetaStore};
use operon_common::{CollectionId, NamespaceId};
use operon_query::hot::{self, HotAnn, HotStatus, HotTier, HotUsed, RequestHot};
use operon_query::placement::{Owner, Placement, RemoteReads};
use operon_query::{
    CREATED_AT_ANNOTATION, CollectionInfo, CollectionService, FieldValue, Fusion, OpResult,
    Projection, Query, ReadConsistency, Retriever, SearchRequest, SearchResponse, ServiceConfig,
    ServiceError, SourceFilter, StoredDoc, TotalHits, TotalRelation, WriteOptions,
};
use serde_json::json;

const NS: &str = "acme";
const DOCS: &str = "docs";

fn u(pk: u64) -> PrimaryKey {
    PrimaryKey::U64(pk)
}

fn not_found(name: &str) -> ServiceError {
    ServiceError::NotFound {
        kind: "collection",
        name: name.to_string(),
    }
}

fn reported() -> WriteOptions {
    WriteOptions {
        report_existence: true,
        atomic: false,
    }
}

fn atomic() -> WriteOptions {
    WriteOptions {
        report_existence: false,
        atomic: true,
    }
}

/// The whole source and vector `v`.
fn everything() -> Projection {
    Projection {
        source: SourceFilter::All,
        vectors: vec!["v".to_string()],
        fields: Vec::new(),
    }
}

fn term(field: &str, value: &str) -> Query {
    Query::Term {
        field: field.to_string(),
        value: FieldValue::Str(value.to_string()),
    }
}

/// A text search for `query` over `collection`, 100 hits.
fn text_search(collection: &str, query: Query) -> SearchRequest {
    let mut request = SearchRequest::new(collection);
    request.retrievers = vec![Retriever::Text { query, k: 100 }];
    request.limit = 100;
    request
}

fn hit_keys(response: &SearchResponse) -> Vec<PrimaryKey> {
    response.hits.iter().map(|hit| hit.pk.clone()).collect()
}

fn with_upsert(pk: u64, source: serde_json::Value, upsert_doc: Document) -> DocOp {
    DocOp::Patch {
        pk: u(pk),
        mode: PatchMode::MergeDeep,
        source: obj(source),
        delete_keys: Vec::new(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: Some(upsert_doc),
    }
}

fn deleting_keys(pk: u64, keys: &[&str]) -> DocOp {
    DocOp::Patch {
        pk: u(pk),
        mode: PatchMode::MergeDeep,
        source: serde_json::Map::new(),
        delete_keys: keys.iter().map(|k| k.to_string()).collect(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: None,
    }
}

/// A fixture with collection `acme/docs` of [`tail_schema`] (4 partitions).
async fn with_docs() -> (Fixture, Arc<CollectionService>, CollectionInfo) {
    with_docs_configured(ServiceConfig::default()).await
}

async fn with_docs_configured(
    config: ServiceConfig,
) -> (Fixture, Arc<CollectionService>, CollectionInfo) {
    let f = Fixture::start_with(config).await;
    let service = f.service();
    let info = service
        .create_collection(NS, DOCS, tail_schema(), None)
        .await
        .expect("create the collection");
    (f, service, info)
}

/// Every partition's high watermark of `cid`.
async fn high_watermarks(f: &Fixture, cid: CollectionId) -> Vec<u64> {
    f.meta
        .client
        .collection_head(Consistency::Linearizable, cid)
        .await
        .expect("read")
        .expect("the collection exists")
        .high_watermarks
}

async fn count(service: &CollectionService, name: &str) -> Result<u64, ServiceError> {
    service.count(NS, name, None, ReadConsistency::Strong).await
}

// ----- namespaces and collections -----

#[tokio::test]
async fn create_collection_creates_the_namespace() {
    let f = Fixture::start().await;
    let service = f.service();
    let before = f
        .meta
        .client
        .namespace_by_name(Consistency::Linearizable, "fresh")
        .await
        .expect("read");
    assert!(before.is_none());

    let info = service
        .create_collection("fresh", DOCS, tail_schema(), None)
        .await
        .expect("create");
    assert_eq!(info.namespace, "fresh");
    assert_eq!(info.name, DOCS);
    assert_eq!(info.partitions, 4, "Ruling 20");
    assert_eq!(info.schema.version, 1);
    assert_eq!(info.manifest_version, 0);
    assert_eq!(info.live_doc_count, 0);
    assert_eq!(info.link_lag_records, 0);
    assert!(info.aliases.is_empty());
    assert!(info.created_at_ms > 0);
    assert_eq!(
        info.schema.annotations.get(CREATED_AT_ANNOTATION),
        Some(&info.created_at_ms.to_string())
    );
    let namespace = f
        .meta
        .client
        .namespace_by_name(Consistency::Linearizable, "fresh")
        .await
        .expect("read")
        .expect("the namespace was created");
    assert_eq!(namespace.name, "fresh");

    service.ensure_namespace("fresh").await.expect("idempotent");
    assert!(matches!(
        service.ensure_namespace("not a name").await,
        Err(ServiceError::InvalidArgument(_))
    ));
    f.shutdown().await;
}

#[tokio::test]
async fn reads_in_an_absent_namespace_behave_as_empty() {
    let f = Fixture::start().await;
    let service = f.service();
    let ns = "nowhere";
    let strong = ReadConsistency::Strong;
    assert_eq!(
        service
            .get(ns, DOCS, &[u(1)], &Projection::default(), strong.clone())
            .await,
        Err(not_found(DOCS))
    );
    assert_eq!(
        service
            .search(ns, SearchRequest::new(DOCS))
            .await
            .map(|_| ()),
        Err(not_found(DOCS))
    );
    assert_eq!(
        service.count(ns, DOCS, None, strong.clone()).await,
        Err(not_found(DOCS))
    );
    assert_eq!(
        service
            .scroll(ns, DOCS, None, None, 10, &Projection::default(), strong)
            .await,
        Err(not_found(DOCS))
    );
    assert_eq!(
        service.get_collection(ns, DOCS).await.map(|_| ()),
        Err(not_found(DOCS))
    );
    assert_eq!(service.versions(ns, DOCS).await, Err(not_found(DOCS)));
    assert_eq!(
        service.pin(ns, DOCS).await.map(|_| ()),
        Err(not_found(DOCS))
    );
    assert_eq!(
        service
            .write(
                ns,
                DOCS,
                vec![upsert(1, json!({}))],
                WriteOptions::default()
            )
            .await
            .map(|_| ()),
        Err(not_found(DOCS))
    );
    assert_eq!(
        service
            .add_fields(ns, DOCS, Vec::new(), Vec::new(), BTreeMap::new())
            .await
            .map(|_| ()),
        Err(not_found(DOCS))
    );
    assert_eq!(service.list_collections(ns).await, Ok(Vec::new()));
    assert_eq!(service.drop_collection(ns, DOCS).await, Ok(false));
    assert_eq!(
        service
            .update_aliases(
                ns,
                vec![AliasAction::Delete {
                    alias: "a".to_string()
                }]
            )
            .await,
        Ok(())
    );
    assert_eq!(
        service
            .update_aliases(
                ns,
                vec![AliasAction::Create {
                    alias: "a".to_string(),
                    collection: DOCS.to_string()
                }]
            )
            .await,
        Err(not_found(DOCS))
    );
    // Nothing above created the namespace.
    let namespace = f
        .meta
        .client
        .namespace_by_name(Consistency::Linearizable, ns)
        .await
        .expect("read");
    assert!(namespace.is_none());
    f.shutdown().await;
}

#[tokio::test]
async fn create_is_retry_safe_and_a_different_schema_is_already_exists() {
    let (f, service, first) = with_docs().await;
    // A later retry carries a newer creation annotation, so the metastore
    // answers `NameTaken`; the service compares without it.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let again = service
        .create_collection(NS, DOCS, tail_schema(), None)
        .await
        .expect("a retry succeeds");
    assert_eq!(again.id, first.id);
    assert_eq!(again.created_at_ms, first.created_at_ms);
    let same_partitions = service
        .create_collection(NS, DOCS, tail_schema(), Some(4))
        .await
        .expect("the same partition count succeeds");
    assert_eq!(same_partitions.id, first.id);

    assert_eq!(
        service
            .create_collection(NS, DOCS, tail_schema(), Some(2))
            .await
            .map(|_| ()),
        Err(ServiceError::AlreadyExists(DOCS.to_string()))
    );
    let mut other = tail_schema();
    other.fields.push(field("extra", FieldKind::Keyword));
    assert_eq!(
        service
            .create_collection(NS, DOCS, other, None)
            .await
            .map(|_| ()),
        Err(ServiceError::AlreadyExists(DOCS.to_string()))
    );
    assert!(matches!(
        service
            .create_collection(NS, "_reserved", tail_schema(), None)
            .await,
        Err(ServiceError::InvalidArgument(_))
    ));
    let listed = service.list_collections(NS).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, first.id);
    f.shutdown().await;
}

#[tokio::test]
async fn drop_frees_the_name_and_stops_the_tail() {
    let (f, service, info) = with_docs().await;
    service
        .write(
            NS,
            DOCS,
            (0..3).map(|n| upsert(n, json!({"n": n}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write");
    assert_eq!(count(&service, DOCS).await, Ok(3));
    let tail = service
        .reads()
        .tail(info.id)
        .expect("the strong read started the tail");

    assert_eq!(service.drop_collection(NS, DOCS).await, Ok(true));
    assert!(tail.is_stopped());
    assert!(service.reads().tail(info.id).is_none());
    assert_eq!(service.drop_collection(NS, DOCS).await, Ok(false));
    assert_eq!(count(&service, DOCS).await, Err(not_found(DOCS)));
    assert_eq!(
        service.get_collection(NS, DOCS).await.map(|_| ()),
        Err(not_found(DOCS))
    );

    let mut other = tail_schema();
    other.fields.push(field("extra", FieldKind::Keyword));
    let recreated = service
        .create_collection(NS, DOCS, other, Some(2))
        .await
        .expect("the name is free");
    assert_ne!(recreated.id, info.id);
    assert_eq!(recreated.partitions, 2);
    assert_eq!(count(&service, DOCS).await, Ok(0));
    f.shutdown().await;
}

#[tokio::test]
async fn get_collection_reports_manifest_version_and_link_lag() {
    let (f, service, _) = with_docs().await;
    let ops: Vec<DocOp> = (0..7).map(|n| upsert(n, json!({"t": "seven"}))).collect();
    service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
    let before = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(before.link_lag_records, 7);
    assert_eq!(before.manifest_version, 0);
    assert_eq!(before.live_doc_count, 0);

    f.settle().await;
    let after = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(after.link_lag_records, 0);
    assert!(after.manifest_version > before.manifest_version);
    assert_eq!(after.live_doc_count, 7);
    assert!(after.size_bytes > 0);
    f.shutdown().await;
}

#[tokio::test]
async fn versions_list_the_retained_manifests_in_order() {
    let (f, service, _) = with_docs().await;
    assert_eq!(service.versions(NS, DOCS).await, Ok(Vec::new()));
    for n in 0..3 {
        service
            .write(
                NS,
                DOCS,
                vec![upsert(n, json!({"n": n}))],
                WriteOptions::default(),
            )
            .await
            .expect("write");
        f.settle().await;
    }
    let versions = service.versions(NS, DOCS).await.expect("versions");
    assert_eq!(
        versions.iter().map(|v| v.version).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        versions
            .iter()
            .map(|v| v.live_doc_count)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        versions
            .iter()
            .all(|v| v.lance_version != 0 && v.size_bytes > 0)
    );
    assert!(
        versions
            .windows(2)
            .all(|w| w[0].created_at_ms <= w[1].created_at_ms)
    );
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.manifest_version, 3);
    f.shutdown().await;
}

// ----- writes -----

#[tokio::test]
async fn report_existence_answers_every_result() {
    use OpResult::{Created, Deleted, Noop, NotFound, Updated};
    let (f, service, _) = with_docs().await;
    // (setup, request, expected results); case `c` uses keys `100 c + 1`
    // (`a`) and `100 c + 2` (`b`).
    type Case = (Vec<DocOp>, Vec<DocOp>, Vec<OpResult>);
    let cases: Vec<Case> = (0u64..12)
        .map(|c| {
            let a = 100 * c + 1;
            let b = 100 * c + 2;
            match c {
                0 => (vec![], vec![upsert(a, json!({"n": 1}))], vec![Created]),
                1 => (
                    vec![upsert(a, json!({"n": 1}))],
                    vec![upsert(a, json!({"n": 2}))],
                    vec![Updated],
                ),
                // An equal upsert still writes a new version (P32).
                2 => (
                    vec![upsert(a, json!({"n": 1}))],
                    vec![upsert(a, json!({"n": 1}))],
                    vec![Updated],
                ),
                3 => (
                    vec![upsert(a, json!({"n": 1}))],
                    vec![DocOp::Delete(u(a))],
                    vec![Deleted],
                ),
                4 => (vec![], vec![DocOp::Delete(u(a))], vec![NotFound]),
                5 => (
                    vec![upsert(a, json!({"n": 1}))],
                    vec![patch(a, json!({"n": 2}))],
                    vec![Updated],
                ),
                6 => (
                    vec![upsert(a, json!({"n": 1, "tag": "x"}))],
                    vec![patch(a, json!({"n": 1}))],
                    vec![Noop],
                ),
                7 => (vec![], vec![patch(a, json!({"n": 1}))], vec![NotFound]),
                8 => (
                    vec![],
                    vec![with_upsert(a, json!({"n": 1}), doc(a, json!({"n": 5})))],
                    vec![Created],
                ),
                9 => (
                    vec![],
                    vec![
                        upsert(a, json!({"n": 1})),
                        patch(a, json!({"n": 2})),
                        DocOp::Delete(u(a)),
                        patch(a, json!({"n": 3})),
                    ],
                    vec![Created, Updated, Deleted, NotFound],
                ),
                10 => (
                    vec![upsert(a, json!({"n": 1}))],
                    vec![
                        DocOp::Delete(u(a)),
                        upsert(a, json!({"n": 1})),
                        patch(a, json!({"n": 1})),
                    ],
                    vec![Deleted, Created, Noop],
                ),
                _ => (
                    vec![upsert(a, json!({"n": 1})), upsert(b, json!({"n": 2}))],
                    vec![
                        // The upsert document is ignored for a present key.
                        with_upsert(a, json!({"n": 1}), doc(a, json!({"n": 9}))),
                        DocOp::Delete(u(b)),
                        DocOp::Delete(u(b)),
                        deleting_keys(a, &["missing"]),
                    ],
                    vec![Noop, Deleted, NotFound, Noop],
                ),
            }
        })
        .collect();
    // Half the setups are durable, half still in the tail.
    let (durable, tail): (Vec<_>, Vec<_>) = cases.iter().enumerate().partition(|(c, _)| c % 2 == 0);
    for (setups, settle) in [(durable, true), (tail, false)] {
        let ops: Vec<DocOp> = setups
            .into_iter()
            .flat_map(|(_, (setup, _, _))| setup.clone())
            .collect();
        service
            .write(NS, DOCS, ops, WriteOptions::default())
            .await
            .expect("setup");
        if settle {
            f.settle().await;
        }
    }
    for (c, (_, ops, expected)) in cases.iter().enumerate() {
        let result = service
            .write(NS, DOCS, ops.clone(), reported())
            .await
            .expect("write");
        assert_eq!(&result.results, expected, "case {c}");
        assert!(result.positions.iter().all(Option::is_some), "case {c}");
    }
    // The stored state is what the results describe.
    let docs = service
        .get(
            NS,
            DOCS,
            &[u(901), u(1001), u(1101), u(1102)],
            &everything(),
            ReadConsistency::Strong,
        )
        .await
        .expect("get");
    assert!(docs[0].is_none(), "case 9 deleted a");
    assert_eq!(
        docs[1].as_ref().and_then(|d| d.source.clone()),
        Some(obj(json!({"n": 1})))
    );
    assert_eq!(
        docs[2].as_ref().and_then(|d| d.source.clone()),
        Some(obj(json!({"n": 1})))
    );
    assert!(docs[3].is_none());
    f.shutdown().await;
}

#[tokio::test]
async fn an_atomic_write_rejects_the_whole_request() {
    let (f, service, info) = with_docs().await;
    let before = high_watermarks(&f, info.id).await;
    let mut ops: Vec<DocOp> = (0..10).map(|n| upsert(n, json!({"n": n}))).collect();
    ops[7] = upsert(7, json!({"n": "not a number"}));
    match service.write(NS, DOCS, ops, atomic()).await {
        Err(ServiceError::SchemaViolation { field, message }) => {
            assert_eq!(field, "n");
            assert!(message.starts_with("op 7: "), "{message}");
        }
        other => panic!("expected a schema violation, got {other:?}"),
    }
    assert_eq!(
        high_watermarks(&f, info.id).await,
        before,
        "nothing written"
    );
    assert_eq!(count(&service, DOCS).await, Ok(0));

    // Key, delete_keys and upsert-key errors are invalid arguments.
    let mut ops: Vec<DocOp> = (0..4).map(|n| upsert(n, json!({}))).collect();
    ops[2] = deleting_keys(2, &["a..b"]);
    match service.write(NS, DOCS, ops, atomic()).await {
        Err(ServiceError::InvalidArgument(message)) => {
            assert!(message.starts_with("op 2: delete_keys: "), "{message}");
        }
        other => panic!("expected an invalid argument, got {other:?}"),
    }
    let ops = vec![
        upsert(0, json!({})),
        with_upsert(1, json!({}), doc(2, json!({}))),
    ];
    match service.write(NS, DOCS, ops, atomic()).await {
        Err(ServiceError::InvalidArgument(message)) => {
            assert!(message.starts_with("op 1: "), "{message}");
        }
        other => panic!("expected an invalid argument, got {other:?}"),
    }
    let ops = vec![DocOp::Delete(PrimaryKey::Str(String::new()))];
    assert!(matches!(
        service.write(NS, DOCS, ops, atomic()).await,
        Err(ServiceError::InvalidArgument(message)) if message.starts_with("op 0: ")
    ));
    assert_eq!(
        high_watermarks(&f, info.id).await,
        before,
        "nothing written"
    );

    // A valid atomic request is written whole.
    let ops: Vec<DocOp> = (0..10).map(|n| upsert(n, json!({"n": n}))).collect();
    let result = service.write(NS, DOCS, ops, atomic()).await.expect("write");
    assert!(result.results.iter().all(|r| *r == OpResult::Accepted));
    assert_eq!(count(&service, DOCS).await, Ok(10));
    f.shutdown().await;
}

#[tokio::test]
async fn a_non_atomic_write_reports_rejected_ops_and_writes_the_rest() {
    let (f, service, _) = with_docs().await;
    let ops = vec![
        upsert(1, json!({"n": 1})),
        upsert(2, json!({"n": "bad"})),
        // The rejected upsert left key 2 absent.
        patch(2, json!({"n": 3})),
        upsert(3, json!({"n": 3})),
    ];
    let result = service
        .write(NS, DOCS, ops.clone(), reported())
        .await
        .expect("write");
    assert_eq!(result.results[0], OpResult::Created);
    assert!(matches!(
        &result.results[1],
        OpResult::Rejected(ServiceError::SchemaViolation { field, .. }) if field == "n"
    ));
    assert_eq!(result.results[2], OpResult::NotFound);
    assert_eq!(result.results[3], OpResult::Created);
    assert!(result.positions[0].is_some());
    assert!(result.positions[1].is_none());
    assert!(result.positions[2].is_some());
    assert!(result.positions[3].is_some());
    let docs = service
        .get(
            NS,
            DOCS,
            &[u(1), u(2), u(3)],
            &Projection::default(),
            ReadConsistency::AtLeast(result.token.clone()),
        )
        .await
        .expect("get");
    assert!(docs[0].is_some() && docs[1].is_none() && docs[2].is_some());

    // Without existence reporting, accepted ops are `Accepted`.
    let result = service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
    assert_eq!(result.results[0], OpResult::Accepted);
    assert!(matches!(result.results[1], OpResult::Rejected(_)));
    assert_eq!(result.results[2], OpResult::Accepted);
    f.shutdown().await;
}

#[tokio::test]
async fn accepted_ops_are_consecutive_per_partition() {
    let (f, service, info) = with_docs().await;
    service
        .write(
            NS,
            DOCS,
            (100..110).map(|n| upsert(n, json!({}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("an earlier write");
    let ops: Vec<DocOp> = (0..30).map(|n| upsert(n, json!({"n": n}))).collect();
    let result = service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
    let mut by_partition: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for (n, position) in result.positions.iter().enumerate() {
        let position = position.expect("written");
        assert_eq!(
            position.partition,
            partition_of(&u(n as u64), info.partitions)
        );
        by_partition
            .entry(position.partition)
            .or_default()
            .push(position.seq_no);
    }
    assert_eq!(by_partition.len(), 4, "30 keys reach every partition");
    for (partition, seq_nos) in &by_partition {
        assert!(
            seq_nos.windows(2).all(|w| w[1] == w[0] + 1),
            "partition {partition}: {seq_nos:?}"
        );
    }
    f.shutdown().await;
}

#[tokio::test]
async fn dynamic_map_mode_adds_fields_before_writing() {
    let f = Fixture::start().await;
    let service = f.service();
    let schema = CollectionSchema::new(
        vec![field(
            "t",
            FieldKind::Text {
                analyzer: "standard".to_string(),
                positions: true,
            },
        )],
        Vec::new(),
        DynamicMapping::Map,
    );
    service
        .create_collection(NS, DOCS, schema, None)
        .await
        .expect("create");
    service
        .write(
            NS,
            DOCS,
            vec![upsert(1, json!({"t": "hello", "color": "red", "size": 3}))],
            atomic(),
        )
        .await
        .expect("write");
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.schema.version, 2);
    assert!(info.schema.field("color").is_some());
    assert!(info.schema.field("color.keyword").is_some());
    assert!(info.schema.field("size").is_some());

    // Searchable by the new fields at once.
    let response = service
        .search(NS, text_search(DOCS, term("color.keyword", "red")))
        .await
        .expect("search");
    assert_eq!(hit_keys(&response), vec![u(1)]);
    let size = Query::Term {
        field: "size".to_string(),
        value: FieldValue::I64(3),
    };
    assert_eq!(
        service
            .count(NS, DOCS, Some(size), ReadConsistency::Strong)
            .await,
        Ok(1)
    );

    // Nothing new to map: the schema stays.
    service
        .write(
            NS,
            DOCS,
            vec![upsert(2, json!({"t": "again", "color": "blue"}))],
            WriteOptions::default(),
        )
        .await
        .expect("write");
    let info = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(info.schema.version, 2);
    f.shutdown().await;
}

#[tokio::test]
async fn add_fields_is_additive_idempotent_and_not_backfilled() {
    let (f, service, _) = with_docs().await;
    service
        .write(
            NS,
            DOCS,
            vec![upsert(1, json!({"t": "old", "color": "red"}))],
            WriteOptions::default(),
        )
        .await
        .expect("write");
    f.settle().await;

    let color = field("color", FieldKind::Keyword);
    let annotations = BTreeMap::from([("operon.owner".to_string(), "qa".to_string())]);
    let schema = service
        .add_fields(
            NS,
            DOCS,
            vec![color.clone()],
            Vec::new(),
            annotations.clone(),
        )
        .await
        .expect("add a field");
    assert_eq!(schema.version, 2);
    assert_eq!(schema.field("color"), Some(&color));
    assert_eq!(
        schema.annotations.get("operon.owner"),
        Some(&"qa".to_string())
    );
    let again = service
        .add_fields(NS, DOCS, vec![color], Vec::new(), annotations)
        .await
        .expect("a retry succeeds");
    assert_eq!(again, schema, "idempotent");

    assert_eq!(
        service
            .add_fields(
                NS,
                DOCS,
                vec![field("color", FieldKind::I64)],
                Vec::new(),
                BTreeMap::new()
            )
            .await,
        Err(ServiceError::AlreadyExists("field color".to_string()))
    );
    assert_eq!(
        service
            .add_fields(NS, DOCS, Vec::new(), vec![vector("v", 4)], BTreeMap::new())
            .await,
        Err(ServiceError::AlreadyExists("field v".to_string()))
    );
    assert!(matches!(
        service
            .add_fields(
                NS,
                DOCS,
                vec![field("_bad", FieldKind::Keyword)],
                Vec::new(),
                BTreeMap::new()
            )
            .await,
        Err(ServiceError::InvalidArgument(_))
    ));

    service
        .write(
            NS,
            DOCS,
            vec![upsert(2, json!({"t": "new", "color": "red"}))],
            WriteOptions::default(),
        )
        .await
        .expect("write");
    // The old document is not re-indexed (A4); the new one matches, from
    // the tail and then from its split.
    for settle in [false, true] {
        if settle {
            f.settle().await;
        }
        let response = service
            .search(NS, text_search(DOCS, term("color", "red")))
            .await
            .expect("search");
        assert_eq!(hit_keys(&response), vec![u(2)], "settled: {settle}");
        assert_eq!(
            service
                .count(
                    NS,
                    DOCS,
                    Some(term("color", "red")),
                    ReadConsistency::Strong
                )
                .await,
            Ok(1)
        );
    }
    f.shutdown().await;
}

#[tokio::test]
async fn aliases_resolve_everywhere_and_map_errors() {
    let (f, service, info) = with_docs().await;
    service
        .create_collection(NS, "other", tail_schema(), None)
        .await
        .expect("create other");
    service
        .update_aliases(
            NS,
            vec![AliasAction::Create {
                alias: "a".to_string(),
                collection: DOCS.to_string(),
            }],
        )
        .await
        .expect("create the alias");

    service
        .write(
            NS,
            "a",
            (1..=3)
                .map(|n| upsert(n, json!({"t": "via alias"})))
                .collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write through the alias");
    let strong = ReadConsistency::Strong;
    let docs = service
        .get(NS, "a", &[u(1)], &Projection::default(), strong.clone())
        .await
        .expect("get");
    assert!(docs[0].is_some());
    let response = service
        .search(NS, SearchRequest::new("a"))
        .await
        .expect("search");
    assert_eq!(hit_keys(&response), vec![u(1), u(2), u(3)]);
    assert_eq!(count(&service, "a").await, Ok(3));
    let (page, next) = service
        .scroll(NS, "a", None, None, 10, &Projection::default(), strong)
        .await
        .expect("scroll");
    assert_eq!(page.len(), 3);
    assert!(next.is_none());
    let through = service.get_collection(NS, "a").await.expect("info");
    assert_eq!(through.id, info.id);
    assert_eq!(through.name, DOCS);
    assert_eq!(through.aliases, vec!["a".to_string()]);
    let pin = service.pin(NS, "a").await.expect("pin");
    assert_eq!((pin.collection, pin.name.as_str()), (info.id, DOCS));
    assert_eq!(service.versions(NS, "a").await, Ok(Vec::new()));
    // The catalog cache lists collections and aliases, sorted.
    let deadline = std::time::Instant::now() + crate::common::WAIT;
    while service.catalog().names(NS) != ["a", DOCS, "other"] {
        assert!(
            std::time::Instant::now() < deadline,
            "catalog names: {:?}",
            service.catalog().names(NS)
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(service.catalog().names("nowhere").is_empty());

    // Errors.
    assert_eq!(
        service
            .update_aliases(
                NS,
                vec![AliasAction::Create {
                    alias: DOCS.to_string(),
                    collection: "other".to_string()
                }]
            )
            .await,
        Err(ServiceError::AlreadyExists(DOCS.to_string()))
    );
    assert_eq!(
        service
            .update_aliases(
                NS,
                vec![AliasAction::Create {
                    alias: "b".to_string(),
                    collection: "missing".to_string()
                }]
            )
            .await,
        Err(not_found("missing"))
    );
    assert_eq!(
        service
            .create_collection(NS, "a", tail_schema(), None)
            .await
            .map(|_| ()),
        Err(ServiceError::AlreadyExists("a".to_string()))
    );
    // An alias is not dropped as a collection.
    assert_eq!(service.drop_collection(NS, "a").await, Ok(false));
    assert_eq!(count(&service, DOCS).await, Ok(3));

    service
        .update_aliases(
            NS,
            vec![AliasAction::Delete {
                alias: "a".to_string(),
            }],
        )
        .await
        .expect("delete the alias");
    assert_eq!(count(&service, "a").await, Err(not_found("a")));
    f.shutdown().await;
}

// ----- forwarding -----

#[derive(Debug)]
struct RemoteOwner;

impl Placement for RemoteOwner {
    fn owner(&self, _: NamespaceId, _: CollectionId) -> Owner {
        Owner::Remote {
            node_id: 2,
            addr: SocketAddr::from(([127, 0, 0, 1], 1)),
        }
    }
}

/// Records every forwarded call (method, collection name, the hot switch it
/// saw) and answers with canned values or `fail`.
#[derive(Debug, Default)]
struct Recording {
    calls: Mutex<Vec<(&'static str, String, Option<bool>)>>,
    fail: Mutex<Option<ServiceError>>,
}

const CANNED: u64 = 424_242;

impl Recording {
    fn record(&self, method: &'static str, name: &str) -> Result<(), ServiceError> {
        let hot = hot::current().map(|hot| hot.enabled);
        self.calls
            .lock()
            .expect("lock")
            .push((method, name.to_string(), hot));
        match self.fail.lock().expect("lock").clone() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn methods(&self) -> Vec<(&'static str, String)> {
        self.calls
            .lock()
            .expect("lock")
            .iter()
            .map(|(method, name, _)| (*method, name.clone()))
            .collect()
    }
}

#[async_trait::async_trait]
impl RemoteReads for Recording {
    async fn search(
        &self,
        _: &Owner,
        _: &str,
        req: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        self.record("search", &req.collection)?;
        Ok(SearchResponse {
            hits: Vec::new(),
            total: Some(TotalHits {
                value: CANNED,
                relation: TotalRelation::Eq,
            }),
            aggregations: None,
            groups: None,
            read_token: ConsistencyToken::default(),
            hot_used: Default::default(),
        })
    }

    async fn get(
        &self,
        _: &Owner,
        _: &str,
        name: &str,
        pks: Vec<PrimaryKey>,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        self.record("get", name)?;
        Ok(vec![None; pks.len()])
    }

    async fn count(
        &self,
        _: &Owner,
        _: &str,
        name: &str,
        _: Option<Query>,
        _: ReadConsistency,
    ) -> Result<u64, ServiceError> {
        self.record("count", name)?;
        Ok(CANNED)
    }

    async fn scroll(
        &self,
        _: &Owner,
        _: &str,
        name: &str,
        _: Option<Query>,
        _: Option<PrimaryKey>,
        _: usize,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        self.record("scroll", name)?;
        Ok((Vec::new(), Some(u(CANNED))))
    }
}

/// Collection `docs` with alias `a` and documents 1..=3, owned by a remote
/// node that `Recording` stands for.
async fn remotely_owned() -> (
    Fixture,
    Arc<CollectionService>,
    CollectionInfo,
    Arc<Recording>,
) {
    let config = ServiceConfig {
        max_get_keys: 5,
        ..ServiceConfig::default()
    };
    let (f, service, info) = with_docs_configured(config).await;
    service
        .update_aliases(
            NS,
            vec![AliasAction::Create {
                alias: "a".to_string(),
                collection: DOCS.to_string(),
            }],
        )
        .await
        .expect("alias");
    service
        .write(
            NS,
            DOCS,
            (1..=3).map(|n| upsert(n, json!({"t": "doc"}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write");
    let recording = Arc::new(Recording::default());
    service.set_placement(Arc::new(RemoteOwner), recording.clone());
    (f, service, info, recording)
}

#[tokio::test]
async fn a_remote_owner_forwards_every_read() {
    let (f, service, info, recording) = remotely_owned().await;
    let strong = ReadConsistency::Strong;
    let response = service
        .search(NS, SearchRequest::new("a"))
        .await
        .expect("search");
    assert_eq!(response.total.map(|t| t.value), Some(CANNED));
    let docs = service
        .get(
            NS,
            "a",
            &[u(1), u(2)],
            &Projection::default(),
            strong.clone(),
        )
        .await
        .expect("get");
    assert_eq!(docs, vec![None, None], "the remote's answer");
    assert_eq!(count(&service, "a").await, Ok(CANNED));
    let (_, next) = service
        .scroll(
            NS,
            "a",
            None,
            None,
            10,
            &Projection::default(),
            strong.clone(),
        )
        .await
        .expect("scroll");
    assert_eq!(next, Some(u(CANNED)));
    assert_eq!(
        recording.methods(),
        vec![
            ("search", DOCS.to_string()),
            ("get", DOCS.to_string()),
            ("count", DOCS.to_string()),
            ("scroll", DOCS.to_string()),
        ],
        "every read is forwarded with the resolved name"
    );
    // The local planner never ran: no local read view started a tail.
    assert!(service.reads().tail(info.id).is_none());

    // The transport sees the request's hot switch.
    let off = RequestHot {
        enabled: false,
        used: HotUsed::default(),
    };
    hot::scope(off, service.count(NS, DOCS, None, strong.clone()))
        .await
        .expect("count");
    let seen: Vec<Option<bool>> = recording
        .calls
        .lock()
        .expect("lock")
        .iter()
        .map(|(_, _, hot)| *hot)
        .collect();
    assert_eq!(
        seen,
        vec![Some(true); 4]
            .into_iter()
            .chain([Some(false)])
            .collect::<Vec<_>>()
    );

    // Validation runs before forwarding.
    let calls = recording.methods().len();
    let keys: Vec<PrimaryKey> = (0..6).map(u).collect();
    assert!(matches!(
        service
            .get(NS, "a", &keys, &Projection::default(), strong)
            .await,
        Err(ServiceError::InvalidArgument(_))
    ));
    let mut huge = SearchRequest::new("a");
    huge.limit = usize::MAX / 2;
    assert!(matches!(
        service.search(NS, huge).await,
        Err(ServiceError::InvalidArgument(_))
    ));
    assert_eq!(recording.methods().len(), calls);
    f.shutdown().await;
}

#[tokio::test]
async fn an_unavailable_remote_owner_falls_back_to_a_local_read() {
    let (f, service, _, recording) = remotely_owned().await;
    let strong = ReadConsistency::Strong;
    let local_search = hit_keys(
        &service
            .search_local(NS, SearchRequest::new("a"))
            .await
            .expect("search"),
    );
    assert_eq!(local_search, vec![u(1), u(2), u(3)]);
    for failure in [
        ServiceError::Unavailable("the owner is down".to_string()),
        ServiceError::Timeout,
    ] {
        *recording.fail.lock().expect("lock") = Some(failure.clone());
        let response = service
            .search(NS, SearchRequest::new("a"))
            .await
            .expect("search");
        assert_eq!(hit_keys(&response), local_search, "{failure:?}");
        let docs = service
            .get(
                NS,
                "a",
                &[u(1), u(9)],
                &Projection::default(),
                strong.clone(),
            )
            .await
            .expect("get");
        assert!(docs[0].is_some() && docs[1].is_none(), "{failure:?}");
        assert_eq!(count(&service, "a").await, Ok(3), "{failure:?}");
        let (page, next) = service
            .scroll(
                NS,
                "a",
                None,
                None,
                2,
                &Projection::default(),
                strong.clone(),
            )
            .await
            .expect("scroll");
        assert_eq!(
            page.iter().map(|d| d.pk.clone()).collect::<Vec<_>>(),
            vec![u(1), u(2)],
            "{failure:?}"
        );
        assert_eq!(next, Some(u(2)), "{failure:?}");
    }
    assert_eq!(
        recording.methods().len(),
        8,
        "every read asked the owner first"
    );

    let refusal = ServiceError::InvalidArgument("the owner refuses".to_string());
    *recording.fail.lock().expect("lock") = Some(refusal.clone());
    assert_eq!(
        service
            .search(NS, SearchRequest::new("a"))
            .await
            .map(|_| ()),
        Err(refusal.clone())
    );
    assert_eq!(
        service
            .get(NS, "a", &[u(1)], &Projection::default(), strong.clone())
            .await,
        Err(refusal.clone())
    );
    assert_eq!(count(&service, "a").await, Err(refusal.clone()));
    assert_eq!(
        service
            .scroll(NS, "a", None, None, 2, &Projection::default(), strong)
            .await,
        Err(refusal)
    );
    f.shutdown().await;
}

// ----- the hot switch -----

/// Counts every call of the hot-tier contract.
#[derive(Debug, Default)]
struct CountingHot {
    ann: AtomicUsize,
    split_file: AtomicUsize,
    record_access: AtomicUsize,
    status: AtomicUsize,
}

impl CountingHot {
    fn calls(&self) -> usize {
        self.ann.load(Ordering::SeqCst)
            + self.split_file.load(Ordering::SeqCst)
            + self.record_access.load(Ordering::SeqCst)
            + self.status.load(Ordering::SeqCst)
    }
}

impl HotTier for CountingHot {
    fn ann(&self, _: NamespaceId, _: CollectionId, _: &str, _: u64) -> Option<Arc<dyn HotAnn>> {
        self.ann.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn split_file(
        &self,
        _: NamespaceId,
        _: CollectionId,
        _: ulid::Ulid,
    ) -> Option<std::path::PathBuf> {
        self.split_file.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn record_access(&self, _: NamespaceId, _: CollectionId) {
        self.record_access.fetch_add(1, Ordering::SeqCst);
    }

    fn status(&self, _: NamespaceId, _: CollectionId) -> HotStatus {
        self.status.fetch_add(1, Ordering::SeqCst);
        HotStatus::default()
    }
}

#[tokio::test]
async fn operon_hot_off_uses_no_hot_tier() {
    let (f, service, _) = with_docs().await;
    let ops: Vec<DocOp> = (0..20)
        .map(|n| {
            let mut document = doc(n, json!({"t": format!("word{} common", n % 3)}));
            document
                .vectors
                .insert("v".to_string(), vec![1.0, n as f32, 0.5]);
            DocOp::Upsert(document)
        })
        .collect();
    service
        .write(NS, DOCS, ops, WriteOptions::default())
        .await
        .expect("write");
    f.settle().await;
    let counting = Arc::new(CountingHot::default());
    service.set_hot_tier(counting.clone());
    let request = || {
        let mut request = SearchRequest::new(DOCS);
        request.fusion = Some(Fusion::Rrf { k: 60 });
        request.retrievers = vec![
            Retriever::Text {
                query: Query::Match {
                    field: "t".to_string(),
                    text: "common".to_string(),
                    operator: Default::default(),
                    minimum_should_match: None,
                    fuzziness: None,
                    analyzer: None,
                },
                k: 10,
            },
            Retriever::Vector {
                field: "v".to_string(),
                query: vec![1.0, 2.0, 0.5],
                k: 10,
                params: Default::default(),
                filter: None,
            },
        ];
        request
    };

    let off = RequestHot {
        enabled: false,
        used: HotUsed::default(),
    };
    let response = hot::scope(off.clone(), service.search(NS, request()))
        .await
        .expect("search");
    assert!(!response.hits.is_empty());
    assert_eq!(counting.calls(), 0, "no call reached the hot tier");
    assert!(off.used.kinds().is_empty());

    // With the switch on, the same search reaches it.
    let on = RequestHot {
        enabled: true,
        used: HotUsed::default(),
    };
    let hot_response = hot::scope(on, service.search(NS, request()))
        .await
        .expect("search");
    assert_eq!(counting.record_access.load(Ordering::SeqCst), 1);
    assert_eq!(hot_response.hits, response.hits, "exact paths agree");
    f.shutdown().await;
}

// ----- pins -----

#[tokio::test]
async fn pin_returns_the_current_manifest_and_high_watermarks() {
    let (f, service, info) = with_docs().await;
    service
        .write(
            NS,
            DOCS,
            (0..5).map(|n| upsert(n, json!({}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write");
    f.settle().await;
    service
        .write(
            NS,
            DOCS,
            (5..7).map(|n| upsert(n, json!({}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write");

    let pin = service.pin(NS, DOCS).await.expect("pin");
    let current = service.get_collection(NS, DOCS).await.expect("info");
    assert_eq!(pin.collection, info.id);
    assert_eq!(pin.name, DOCS);
    assert!(pin.manifest_version > 0);
    assert_eq!(pin.manifest_version, current.manifest_version);
    let hwm = high_watermarks(&f, info.id).await;
    let expected = ConsistencyToken(
        (0..info.partitions)
            .map(|p| (info.stream, p, hwm[p as usize]))
            .collect(),
    );
    assert_eq!(pin.token.clone().normalized(), expected.normalized());

    service
        .write(
            NS,
            DOCS,
            (7..10).map(|n| upsert(n, json!({}))).collect(),
            WriteOptions::default(),
        )
        .await
        .expect("write");
    assert_eq!(
        service.count(NS, DOCS, None, pin.consistency()).await,
        Ok(7)
    );
    assert_eq!(count(&service, DOCS).await, Ok(10));
    f.shutdown().await;
}

// ----- sparse vectors -----

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("a sparse vector")
}

#[tokio::test]
async fn sparse_collections_round_trip_through_the_service() {
    let f = Fixture::start().await;
    let service = f.service();
    let schema = CollectionSchema::new(
        vec![field("tag", FieldKind::Keyword)],
        vec![vector("d", 2)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![
        SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::Idf,
        },
        SparseVectorSpec {
            name: "t".to_string(),
            modifier: SparseModifier::None,
        },
    ]);
    let info = service
        .create_collection(NS, DOCS, schema, None)
        .await
        .expect("create");
    let s = sparse(&[1, 2], &[0.5, 0.25]);
    let mut document = doc(1, json!({"tag": "x"}));
    document.vectors.insert("d".to_string(), vec![1.0, 0.0]);
    document.sparse_vectors.insert("s".to_string(), s.clone());
    document
        .sparse_vectors
        .insert("t".to_string(), sparse(&[7], &[1.0]));
    let result = service
        .write(NS, DOCS, vec![DocOp::Upsert(document.clone())], reported())
        .await
        .expect("write");
    assert_eq!(result.results, vec![OpResult::Created]);

    let only_s = Projection {
        source: SourceFilter::All,
        vectors: vec!["s".to_string()],
        fields: Vec::new(),
    };
    let got = service
        .get(NS, DOCS, &[u(1)], &only_s, ReadConsistency::Strong)
        .await
        .expect("get")
        .remove(0)
        .expect("present");
    assert!(got.vectors.is_empty());
    assert_eq!(
        got.sparse_vectors,
        BTreeMap::from([("s".to_string(), s.clone())])
    );

    let sparse_patch = |value: SparseVector| DocOp::Patch {
        pk: u(1),
        mode: PatchMode::MergeDeep,
        source: serde_json::Map::new(),
        delete_keys: Vec::new(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::from([("s".to_string(), Some(value))]),
        upsert: None,
    };
    let changed = sparse(&[1, 2], &[0.5, 0.3]);
    let result = service
        .write(
            NS,
            DOCS,
            vec![
                // P32: an equal upsert, sparse vectors included, is a write.
                DocOp::Upsert(document),
                sparse_patch(s),
                sparse_patch(changed.clone()),
            ],
            reported(),
        )
        .await
        .expect("write");
    assert_eq!(
        result.results,
        vec![OpResult::Updated, OpResult::Noop, OpResult::Updated]
    );
    let got = service
        .get(NS, DOCS, &[u(1)], &only_s, ReadConsistency::Strong)
        .await
        .expect("get")
        .remove(0)
        .expect("present");
    assert_eq!(got.sparse_vectors.get("s"), Some(&changed));

    // Sparse vectors are fixed at creation (A26).
    let schema = service
        .add_fields(
            NS,
            DOCS,
            vec![field("extra", FieldKind::Keyword)],
            Vec::new(),
            BTreeMap::new(),
        )
        .await
        .expect("add a field");
    assert_eq!(schema.sparse_vectors, info.schema.sparse_vectors);
    let mut next = schema.clone();
    next.sparse_vectors.push(SparseVectorSpec {
        name: "u".to_string(),
        modifier: SparseModifier::None,
    });
    let err = f
        .meta
        .client
        .update_collection_schema(info.id, schema.version, next)
        .await
        .expect_err("a new sparse vector is refused");
    assert!(matches!(
        ServiceError::from(err),
        ServiceError::InvalidArgument(_)
    ));
    f.shutdown().await;
}
