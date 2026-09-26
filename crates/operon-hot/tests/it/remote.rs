//! Read forwarding (plan M1.3 Task 10 rules 4–6): M1.2's `CollectionService`
//! over the fixture, with an axum server in the test process that hosts
//! `serve_forwarded` for a receiving service.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::{Path, State};
use bytes::Bytes;
use operon_collection::{
    CollectionConfig, CollectionSchema, CollectionWriter, ConsistencyToken, DocOp, Document,
    DynamicMapping, FieldKind, PrimaryKey,
};
use operon_common::meta::HotConfig;
use operon_hot::{
    ForwardStats, NodeDescriptor, NodeRegistry, PlacementImpl, READS_PATH, RemoteReadsConfig,
    RemoteReadsImpl, Roles, from_wire, owners, serve_forwarded, to_wire,
};
use operon_query::error::ServiceError;
use operon_query::hot::{self, HotKind, HotUsed, RequestHot};
use operon_query::placement::{Owner, RemoteReads};
use operon_query::{
    CollectionService, Projection, Query, ReadConsistency, ScrollPage, SearchRequest,
    SearchResponse, ServiceConfig, StoredDoc,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::common::{DIM, Elsewhere, Fixture, field, vector, vector_of};

const NS: &str = "acme";
const DOCS: &str = "docs";
const WORDS: [&str; 4] = ["alpha", "beta", "gamma", "delta"];

fn schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field(
                "body",
                FieldKind::Text {
                    analyzer: "standard".to_string(),
                    positions: true,
                },
            ),
        ],
        vec![vector(DIM)],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

fn doc(k: u64) -> DocOp {
    let body = format!(
        "{} {} doc{k}",
        WORDS[(k % 4) as usize],
        WORDS[(k / 4 % 4) as usize]
    );
    let source = match json!({"tag": format!("t{}", k % 3), "n": k as i64, "body": body}) {
        Value::Object(map) => map,
        _ => unreachable!("an object"),
    };
    DocOp::Upsert(Document {
        pk: PrimaryKey::U64(k),
        source,
        vectors: BTreeMap::from([(String::new(), vector_of(k, DIM))]),
        sparse_vectors: BTreeMap::new(),
    })
}

/// 60 documents in two commits, and 5 more left in the log tail.
async fn fixture() -> Fixture {
    let f = Fixture::start_with(schema(), CollectionConfig::default()).await;
    f.commit((0..30).map(doc).collect()).await;
    f.commit((30..60).map(doc).collect()).await;
    f.write_to(f.coll, (60..65).map(doc).collect()).await;
    f
}

fn service(f: &Fixture) -> Arc<CollectionService> {
    CollectionService::new(
        f.ctx.clone(),
        CollectionWriter::new(f.meta.client.clone(), f.writer.clone()),
        f.reader.clone(),
        ServiceConfig::default(),
    )
}

#[derive(Clone)]
struct Receiver {
    service: Arc<CollectionService>,
    stats: Arc<ForwardStats>,
}

async fn handle(
    State(r): State<Receiver>,
    Path(op): Path<String>,
    body: Bytes,
) -> (http::StatusCode, Bytes) {
    serve_forwarded(&r.service, &op, body, &r.stats).await
}

/// Serves `serve_forwarded` for `service` on `127.0.0.1:0`.
async fn serve(service: Arc<CollectionService>) -> (SocketAddr, Arc<ForwardStats>, JoinHandle<()>) {
    let stats = Arc::new(ForwardStats::default());
    let app = axum::Router::new()
        .route(&format!("{READS_PATH}{{op}}"), axum::routing::post(handle))
        .with_state(Receiver {
            service,
            stats: stats.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, stats, task)
}

fn descriptor(id: u64, addr: SocketAddr) -> NodeDescriptor {
    NodeDescriptor {
        node_id: id,
        incarnation: Ulid::from_parts(id, 0),
        addr,
        roles: Roles::parse("query,gateway").expect("roles"),
        zone: String::new(),
    }
}

/// A placement on node 1 in which another query node at `addr` owns the
/// fixture's collection.
fn remote_placement(f: &Fixture, addr: SocketAddr) -> (Arc<PlacementImpl>, u64) {
    let me = descriptor(1, "127.0.0.1:9".parse().expect("addr"));
    let other = (2..)
        .find(|id| {
            let nodes = [me.clone(), descriptor(*id, addr)];
            owners(f.ns, f.cid, &nodes, 1)[0].node_id == *id
        })
        .expect("an owner id");
    let registry = NodeRegistry::fixed(me.clone(), vec![me, descriptor(other, addr)]);
    (Arc::new(PlacementImpl::new(registry, 1)), other)
}

/// A service on node 1 that forwards the collection's reads to `addr`.
fn sender(
    f: &Fixture,
    addr: SocketAddr,
) -> (
    Arc<CollectionService>,
    Arc<PlacementImpl>,
    Arc<ForwardStats>,
    u64,
) {
    let (placement, owner) = remote_placement(f, addr);
    let stats = Arc::new(ForwardStats::default());
    let remote = RemoteReadsImpl::new(
        placement.clone(),
        stats.clone(),
        RemoteReadsConfig::default(),
    )
    .expect("client");
    let service = service(f);
    service.set_placement(placement.clone(), Arc::new(remote));
    (service, placement, stats, owner)
}

fn search(body: Value) -> SearchRequest {
    let mut body = body;
    body["collection"] = json!(DOCS);
    serde_json::from_value(body).expect("a search request")
}

fn searches() -> Vec<SearchRequest> {
    let q = vector_of(7, DIM);
    vec![
        search(json!({})),
        search(json!({"limit": 100})),
        search(
            json!({"retrievers": [{"text": {"query": {"match": {"field": "body", "text": "alpha"}}, "k": 20}}]}),
        ),
        search(json!({
            "retrievers": [{"text": {"query": {"match": {"field": "body", "text": "beta gamma"}}, "k": 20}}],
            "filter": {"term": {"field": "tag", "value": "t1"}}
        })),
        search(json!({"filter": {"range": {"field": "n", "gte": 10, "lt": 30}}, "limit": 50})),
        search(
            json!({"retrievers": [{"vector": {"field": "", "query": q, "k": 5, "params": {"exact": true}}}]}),
        ),
        search(json!({
            "retrievers": [{"vector": {"field": "", "query": q, "k": 5, "params": {"exact": true},
                "filter": {"term": {"field": "tag", "value": "t2"}}}}]
        })),
        search(json!({"aggregations": {"tags": {"terms": {"field": "tag"}}}, "limit": 0})),
        search(json!({"sort": [{"field": {"field": "n", "order": "desc"}}], "limit": 7})),
        search(json!({"offset": 5, "limit": 5})),
        search(
            json!({"filter": {"term": {"field": "tag", "value": "t0"}}, "select": {"source": "none"}}),
        ),
    ]
}

fn u(k: u64) -> PrimaryKey {
    PrimaryKey::U64(k)
}

async fn eq_json<T: serde::Serialize>(what: &str, a: &T, b: &T) {
    assert_eq!(
        serde_json::to_value(a).expect("json"),
        serde_json::to_value(b).expect("json"),
        "{what}"
    );
}

/// The 9 get, count and scroll requests; runs each through both services
/// and compares results and read tokens.
async fn compare_other_reads(routed: &CollectionService, local: &CollectionService) -> usize {
    let strong = ReadConsistency::Strong;
    let all = Projection::default();
    let none: Projection = serde_json::from_value(json!({"source": "none"})).expect("projection");
    let mut n = 0;
    for (pks, select) in [
        (vec![u(1), u(2), u(61)], &all),
        (vec![u(3), u(999)], &all),
        (vec![u(4)], &none),
    ] {
        let a = routed
            .get_with_token(NS, DOCS, &pks, select, strong.clone())
            .await
            .expect("get");
        let b = local
            .get_local_with_token(NS, DOCS, &pks, select, strong.clone())
            .await
            .expect("get");
        eq_json("get", &a.0, &b.0).await;
        assert_eq!(a.1, b.1, "get token");
        n += 1;
    }
    let filters: [Option<Query>; 3] = [
        None,
        Some(serde_json::from_value(json!({"term": {"field": "tag", "value": "t1"}})).expect("q")),
        Some(serde_json::from_value(json!({"range": {"field": "n", "gte": 50}})).expect("q")),
    ];
    for filter in filters.clone() {
        let a = routed
            .count_with_token(NS, DOCS, filter.clone(), strong.clone())
            .await
            .expect("count");
        let b = local
            .count_local_with_token(NS, DOCS, filter, strong.clone())
            .await
            .expect("count");
        assert_eq!(a, b, "count");
        n += 1;
    }
    for (filter, after, limit) in [
        (None, None, 10),
        (None, Some(u(20)), 15),
        (filters[1].clone(), None, 100),
    ] {
        let a: ScrollPage = routed
            .scroll_with_token(
                NS,
                DOCS,
                filter.clone(),
                after.clone(),
                limit,
                &all,
                strong.clone(),
            )
            .await
            .expect("scroll");
        let b = local
            .scroll_local_with_token(NS, DOCS, filter, after, limit, &all, strong.clone())
            .await
            .expect("scroll");
        eq_json("scroll", &a.0, &b.0).await;
        assert_eq!(a.1, b.1, "scroll token");
        n += 1;
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwarded_search_get_count_scroll_equal_local_results() {
    let f = fixture().await;
    let receiver = service(&f);
    let (addr, received, server) = serve(receiver.clone()).await;
    let (routed, _, sent_stats, _) = sender(&f, addr);
    let local = service(&f);
    let mut n = 0;
    for request in searches() {
        let a: SearchResponse = routed.search(NS, request.clone()).await.expect("search");
        let b = local
            .search_local(NS, request.clone())
            .await
            .expect("search");
        eq_json(&format!("{request:?}"), &a, &b).await;
        n += 1;
    }
    n += compare_other_reads(&routed, &local).await;
    assert_eq!(n, 20);
    assert_eq!(received.snapshot().forwarded_in, 20);
    let sent = sent_stats.snapshot();
    assert_eq!((sent.forwarded_out, sent.fallbacks_signalled), (20, 0));
    // An error answer is the owner's error, not a fallback.
    let bad =
        search(json!({"retrievers": [{"vector": {"field": "nope", "query": [1.0], "k": 1}}]}));
    let a = routed.search(NS, bad.clone()).await.expect_err("an error");
    let b = local.search_local(NS, bad).await.expect_err("an error");
    assert_eq!(a, b);
    assert_eq!(received.snapshot().forwarded_in, 21);
    assert_eq!(sent_stats.snapshot().fallbacks_signalled, 0);
    server.abort();
    f.shutdown().await;
}

/// Counts the reads a service tries to forward; they all fail over to local.
#[derive(Debug, Default)]
struct Counting(AtomicUsize);

impl Counting {
    fn hit(&self) -> ServiceError {
        self.0.fetch_add(1, Ordering::SeqCst);
        ServiceError::Unavailable("counting only".to_string())
    }
}

#[async_trait::async_trait]
impl RemoteReads for Counting {
    async fn search(
        &self,
        _: &Owner,
        _: &str,
        _: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        Err(self.hit())
    }

    async fn get(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Vec<PrimaryKey>,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<(Vec<Option<StoredDoc>>, ConsistencyToken), ServiceError> {
        Err(self.hit())
    }

    async fn count(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Option<Query>,
        _: ReadConsistency,
    ) -> Result<(u64, ConsistencyToken), ServiceError> {
        Err(self.hit())
    }

    async fn scroll(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Option<Query>,
        _: Option<PrimaryKey>,
        _: usize,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<ScrollPage, ServiceError> {
        Err(self.hit())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_read_is_never_forwarded_again() {
    let f = fixture().await;
    let receiver = service(&f);
    // The receiver's own routing says another node owns the collection.
    let counting = Arc::new(Counting::default());
    receiver.set_placement(Arc::new(Elsewhere), counting.clone());
    let (addr, received, server) = serve(receiver).await;
    let (routed, _, _, _) = sender(&f, addr);
    let local = service(&f);
    for request in searches().into_iter().take(3) {
        let a = routed.search(NS, request.clone()).await.expect("search");
        let b = local.search_local(NS, request).await.expect("search");
        eq_json("search", &a, &b).await;
    }
    compare_other_reads(&routed, &local).await;
    assert_eq!(received.snapshot().forwarded_in, 12);
    assert_eq!(
        counting.0.load(Ordering::SeqCst),
        0,
        "a forwarded read was forwarded again"
    );
    server.abort();
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_fall_back_when_the_owner_is_unreachable() {
    let f = fixture().await;
    // A closed port.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr")
    };
    let (routed, placement, stats, owner) = sender(&f, closed);
    let local = service(&f);
    let request = searches().remove(2);
    let a = routed.search(NS, request.clone()).await.expect("search");
    let b = local
        .search_local(NS, request.clone())
        .await
        .expect("search");
    eq_json("fallback search", &a, &b).await;
    assert!(placement.is_suspect(owner));
    let counters = stats.snapshot();
    assert_eq!(
        (counters.forwarded_out, counters.fallbacks_signalled),
        (1, 1)
    );
    // The next reads go local without a connect attempt.
    routed.search(NS, request).await.expect("search");
    assert_eq!(
        routed.count(NS, DOCS, None, ReadConsistency::Strong).await,
        Ok(65)
    );
    assert_eq!(stats.snapshot(), counters);
    f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hot_flag_is_forwarded() {
    let f = fixture().await;
    f.pin(
        f.cid,
        HotConfig {
            vectors: false,
            text: true,
            fragments: false,
        },
    )
    .await;
    let tier = Arc::new(f.tier().await);
    let report = tier.reconcile_once().await.expect("reconcile");
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    let receiver = service(&f);
    receiver.set_hot_tier(tier.clone());
    let (addr, _, server) = serve(receiver).await;
    let (routed, _, _, _) = sender(&f, addr);
    let text = searches().remove(2);

    let off = RequestHot {
        enabled: false,
        used: HotUsed::default(),
    };
    let before = tier.counters();
    let response = hot::scope(off.clone(), routed.search(NS, text.clone()))
        .await
        .expect("search");
    assert!(response.hot_used.is_empty(), "{:?}", response.hot_used);
    assert!(off.used.kinds().is_empty());
    assert_eq!(
        tier.counters(),
        before,
        "the hot tier served a read with hot off"
    );

    let on = RequestHot {
        enabled: true,
        used: HotUsed::default(),
    };
    let response = hot::scope(on.clone(), routed.search(NS, text))
        .await
        .expect("search");
    assert!(
        response.hot_used.contains(&HotKind::Splits),
        "{:?}",
        response.hot_used
    );
    assert!(
        on.used.kinds().contains(&HotKind::Splits),
        "{:?}",
        on.used.kinds()
    );
    assert!(tier.counters().split_files_served > before.split_files_served);
    server.abort();
    tier.shutdown().await;
    f.shutdown().await;
}

#[test]
fn errors_round_trip_as_service_errors() {
    let mut errors: Vec<ServiceError> = operon_query::error::NOT_FOUND_KINDS
        .iter()
        .map(|kind| ServiceError::NotFound {
            kind,
            name: format!("a {kind}"),
        })
        .collect();
    errors.extend([
        ServiceError::AlreadyExists("x".to_string()),
        ServiceError::InvalidArgument("bad: really".to_string()),
        ServiceError::SchemaViolation {
            field: "f".to_string(),
            message: "not a number".to_string(),
        },
        ServiceError::Unavailable("down".to_string()),
        ServiceError::Timeout,
        ServiceError::Internal("oops".to_string()),
    ]);
    for err in errors {
        assert_eq!(from_wire(to_wire(&err)), err);
    }
    // An unknown not-found kind reads back as "object"; garbage is internal.
    assert_eq!(
        from_wire(json!({"error": "not_found", "kind": "spoon", "name": "s"})),
        ServiceError::NotFound {
            kind: "object",
            name: "s".to_string()
        }
    );
    assert!(matches!(from_wire(json!(42)), ServiceError::Internal(_)));
}
