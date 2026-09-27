//! The universal query compiled to the search IR (plan M1.4 Task 7):
//! nearest, sparse nearest, prefetch, fusion (E3), rescore, the ancestor
//! filters and the unsupported kinds.

use operon_collection::{CollectionSchema, Distance, PrimaryKey, SparseVector};
use operon_qdrant::filter::{and, compile_filter};
use operon_qdrant::model::collections::CreateCollection;
use operon_qdrant::model::filter::Filter;
use operon_qdrant::model::query::{QueryRequest, SearchParams};
use operon_qdrant::query::{Example, QueryPlan, ResolvedExamples, ScoreKind, compile_query};
use operon_qdrant::schema::schema_from_create;
use operon_qdrant::scoring::ann_params;
use operon_qdrant::{GatewayError, QdrantConfig};
use operon_query::{AnnParams, Fusion, Query, Retriever, SearchRequest, SparseParams};
use serde_json::{Value, json};

/// Dense `""` (Cosine, 2), `e` (Euclid, 2) and `d` (Dot, 2); sparse `s`
/// (IDF) and `t` (no modifier).
fn schema() -> CollectionSchema {
    let body = json!({
        "vectors": {
            "": {"size": 2, "distance": "Cosine"},
            "e": {"size": 2, "distance": "Euclid"},
            "d": {"size": 2, "distance": "Dot"}
        },
        "sparse_vectors": {"s": {"modifier": "idf"}, "t": {}}
    });
    let req: CreateCollection = serde_json::from_value(body.clone()).expect("parses");
    schema_from_create("c", &req, &body).expect("schema")
}

fn request(body: Value) -> QueryRequest {
    serde_json::from_value(body.clone()).unwrap_or_else(|e| panic!("{body}: {e}"))
}

fn compile_with(body: Value, examples: &ResolvedExamples) -> Result<QueryPlan, GatewayError> {
    compile_query(
        &request(body),
        "c",
        &schema(),
        examples,
        &QdrantConfig::default(),
    )
}

fn compile(body: Value) -> Result<QueryPlan, GatewayError> {
    compile_with(body, &ResolvedExamples::default())
}

fn ir(body: Value) -> (SearchRequest, operon_qdrant::query::PostProcess) {
    match compile(body.clone()).unwrap_or_else(|e| panic!("{body}: {e}")) {
        QueryPlan::Ir { request, post } => (request, post),
    }
}

fn filter(v: Value) -> Query {
    let f: Filter = serde_json::from_value(v).expect("filter");
    compile_filter(&f, &schema()).expect("compiles")
}

fn bad(body: Value) -> String {
    match compile(body.clone()) {
        Err(GatewayError::BadRequest(m)) => m,
        other => panic!("{body}: not a BadRequest: {other:?}"),
    }
}

fn unsupported(body: Value) -> String {
    match compile(body.clone()) {
        Err(GatewayError::Unsupported(m)) => m,
        other => panic!("{body}: not Unsupported: {other:?}"),
    }
}

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("sparse")
}

#[test]
fn nearest_compiles_to_one_vector_retriever() {
    let (req, post) = ir(json!({
        "query": [3.0, 4.0],
        "filter": {"must": [{"key": "k", "match": {"value": 1}}]},
        "limit": 5,
        "offset": 2,
        "score_threshold": 0.5
    }));
    assert_eq!(
        req.retrievers,
        vec![Retriever::Vector {
            field: String::new(),
            // Normalized for Cosine (Ruling 8).
            query: vec![0.6, 0.8],
            k: 7,
            params: AnnParams::default(),
            filter: None,
        }]
    );
    assert_eq!(req.fusion, None);
    assert_eq!(
        req.filter,
        Some(filter(
            json!({"must": [{"key": "k", "match": {"value": 1}}]})
        ))
    );
    assert_eq!((req.offset, req.limit), (0, 7));
    assert_eq!(req.score_threshold, None);
    assert!(req.sort.is_empty());
    assert_eq!(req.collection, "c");
    assert_eq!(post.kind, ScoreKind::Distance);
    assert_eq!(post.distance, Distance::Cosine);
    assert_eq!((post.offset, post.limit, post.threshold), (2, 5, Some(0.5)));
    // Query with_payload defaults to false: no source is read.
    assert_eq!(req.select.source, operon_query::SourceFilter::None);
    // The `{nearest}` form and `using` compile the same way.
    let (named, post) = ir(json!({"query": {"nearest": [1.0, 2.0]}, "using": "e"}));
    assert_eq!(
        named.retrievers,
        vec![Retriever::Vector {
            field: "e".into(),
            query: vec![1.0, 2.0],
            k: 10,
            params: AnnParams::default(),
            filter: None,
        }]
    );
    assert_eq!(post.distance, Distance::Euclid);
}

#[test]
fn search_params_map_to_ann_params() {
    let params: SearchParams = serde_json::from_value(
        json!({"hnsw_ef": 128, "exact": true, "quantization": {"oversampling": 2.0}}),
    )
    .expect("params");
    assert_eq!(
        ann_params(Some(&params)),
        AnnParams {
            exact: true,
            ef: Some(128),
            oversampling: Some(2.0),
            ..AnnParams::default()
        }
    );
    assert_eq!(ann_params(None), AnnParams::default());
    let (req, _) = ir(json!({"query": [1.0, 0.0], "params": {"hnsw_ef": 64}}));
    assert!(matches!(
        &req.retrievers[0],
        Retriever::Vector { params, .. } if params.ef == Some(64) && !params.exact
    ));
}

fn two_prefetches() -> Value {
    json!([
        {"query": [1.0, 0.0], "using": "d", "limit": 3},
        {"query": {"indices": [1], "values": [1.0]}, "using": "t", "limit": 4}
    ])
}

fn fused(req: &SearchRequest) -> (&Vec<Retriever>, &Fusion, usize) {
    match req.retrievers.as_slice() {
        [Retriever::Fused { inputs, fusion, k }] => (inputs, fusion, *k),
        other => panic!("not one Fused: {other:?}"),
    }
}

#[test]
fn rrf_k_is_shifted_by_one() {
    let (req, post) = ir(json!({"prefetch": two_prefetches(), "query": {"fusion": "rrf"}}));
    assert_eq!(fused(&req).1, &Fusion::Rrf { k: 1 });
    assert_eq!(post.kind, ScoreKind::Fusion);
    let (req, _) = ir(json!({"prefetch": two_prefetches(), "query": {"rrf": {"k": 60}}}));
    assert_eq!(fused(&req).1, &Fusion::Rrf { k: 59 });
    let (req, _) = ir(json!({"prefetch": two_prefetches(), "query": {"rrf": {}}}));
    assert_eq!(fused(&req).1, &Fusion::Rrf { k: 1 });
    let (req, _) = ir(json!({"prefetch": two_prefetches(), "query": {"fusion": "dbsf"}}));
    assert_eq!(fused(&req).1, &Fusion::Dbsf);
    assert_eq!(
        bad(json!({"prefetch": two_prefetches(), "query": {"rrf": {"k": 0}}})),
        "k must be at least 1"
    );
}

#[test]
fn root_fusion_is_one_fused_retriever_over_offset_plus_limit() {
    // E3: a root fusion is `[Fused { k: offset + limit }]`, never top-level
    // retrievers with a `fusion`, which would cut at the largest prefetch k.
    let (req, _) = ir(json!({
        "prefetch": two_prefetches(),
        "query": {"fusion": "rrf"},
        "limit": 10,
        "offset": 5
    }));
    let (inputs, _, k) = fused(&req);
    assert_eq!(k, 15);
    assert_eq!(inputs.len(), 2);
    assert_eq!(req.fusion, None);
    assert_eq!((req.offset, req.limit), (0, 15));
    // One prefetch is still wrapped.
    let (req, _) = ir(json!({
        "prefetch": {"query": [1.0, 0.0], "using": "d"},
        "query": {"fusion": "dbsf"}
    }));
    assert_eq!(fused(&req).0.len(), 1);
}

#[test]
fn nested_prefetch_compiles_to_fused_in_fused() {
    let (req, _) = ir(json!({
        "prefetch": [
            {"prefetch": two_prefetches(), "query": {"fusion": "dbsf"}, "limit": 6},
            {"query": [0.0, 1.0], "using": "e", "limit": 2}
        ],
        "query": {"fusion": "rrf"},
        "limit": 3
    }));
    let (inputs, fusion, k) = fused(&req);
    assert_eq!((fusion, k), (&Fusion::Rrf { k: 1 }, 3));
    match &inputs[0] {
        Retriever::Fused { inputs, fusion, k } => {
            assert_eq!((fusion, *k, inputs.len()), (&Fusion::Dbsf, 6, 2));
            assert!(matches!(&inputs[0], Retriever::Vector { field, k: 3, .. } if field == "d"));
            assert!(matches!(&inputs[1], Retriever::Sparse { field, k: 4, .. } if field == "t"));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(&inputs[1], Retriever::Vector { field, k: 2, .. } if field == "e"));
}

#[test]
fn prefetch_then_nearest_is_rescore() {
    let (req, post) = ir(json!({
        "prefetch": {"query": [1.0, 0.0], "using": "d", "limit": 20},
        "query": [3.0, 4.0],
        "using": "e",
        "limit": 5
    }));
    assert_eq!(
        req.retrievers,
        vec![Retriever::Rescore {
            input: Box::new(Retriever::Vector {
                field: "d".into(),
                query: vec![1.0, 0.0],
                k: 20,
                params: AnnParams::default(),
                filter: None,
            }),
            field: "e".into(),
            query: vec![3.0, 4.0],
            k: 5,
        }]
    );
    assert_eq!(
        (post.kind, post.distance),
        (ScoreKind::Distance, Distance::Euclid)
    );
    // Several prefetches under a rescore are unioned with k = Σ child k.
    let (req, _) = ir(json!({"prefetch": two_prefetches(), "query": [1.0, 1.0], "using": "d"}));
    match &req.retrievers[0] {
        Retriever::Rescore { input, .. } => match input.as_ref() {
            Retriever::Fused { inputs, fusion, k } => {
                assert_eq!((inputs.len(), fusion, *k), (2, &Fusion::Rrf { k: 1 }, 7));
            }
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
    // A prefetch with its own prefetch and a nearest query rescores too.
    let (req, _) = ir(json!({
        "prefetch": {"prefetch": {"query": [1.0, 0.0], "using": "d"}, "query": [0.0, 1.0], "using": "e", "limit": 4},
        "query": {"fusion": "rrf"}
    }));
    assert!(matches!(
        &fused(&req).0[0],
        Retriever::Rescore { field, k: 4, .. } if field == "e"
    ));
}

#[test]
fn ancestor_filters_reach_the_leaves() {
    let fa = json!({"must": [{"key": "a", "match": {"value": 1}}]});
    let fb = json!({"must": [{"key": "b", "match": {"value": 2}}]});
    let fc = json!({"must": [{"key": "c", "match": {"value": 3}}]});
    let (req, _) = ir(json!({
        "prefetch": {
            "filter": fa,
            "prefetch": [
                {"query": [1.0, 0.0], "using": "d", "filter": fb},
                {"query": {"indices": [1], "values": [1.0]}, "using": "t"}
            ],
            "query": {"fusion": "rrf"}
        },
        "query": {"fusion": "rrf"},
        "filter": fc
    }));
    // The root filter is the request's filter, which the IR applies to
    // every leaf.
    assert_eq!(req.filter, Some(filter(fc)));
    let (outer, _, _) = fused(&req);
    let Retriever::Fused { inputs, .. } = &outer[0] else {
        panic!("{outer:?}")
    };
    match &inputs[0] {
        Retriever::Vector { filter: f, .. } => {
            assert_eq!(f, &and(Some(filter(fa.clone())), Some(filter(fb))));
        }
        other => panic!("{other:?}"),
    }
    match &inputs[1] {
        Retriever::Sparse { filter: f, .. } => assert_eq!(f, &Some(filter(fa))),
        other => panic!("{other:?}"),
    }
}

#[test]
fn unsupported_query_kinds_are_501() {
    let cases = [
        (json!({"query": {"order_by": "x"}}), "order_by"),
        (json!({"query": {"formula": {"sum": []}}}), "formula"),
        (json!({"query": {"sample": "random"}}), "sample"),
        (
            json!({"query": {"relevance_feedback": {"target": [1.0, 0.0]}}}),
            "relevance_feedback",
        ),
        (
            json!({"prefetch": two_prefetches(), "query": {"rrf": {"weights": [1.0, 2.0]}}}),
            "weighted RRF",
        ),
        (
            json!({"prefetch": {"query": [1.0, 0.0], "score_threshold": 0.5}, "query": {"fusion": "rrf"}}),
            "prefetch score_threshold",
        ),
        (
            json!({"query": {"recommend": {"positive": [[1.0]]}}, "using": "s"}),
            "sparse recommend",
        ),
        (
            json!({"query": {"discover": {"target": [1.0], "context": []}}, "using": "s"}),
            "sparse discover",
        ),
        (
            json!({"query": {"context": []}, "using": "t"}),
            "sparse context",
        ),
        (
            json!({"query": {"nearest": {"indices": [1], "values": [1.0]}, "mmr": {}}, "using": "s"}),
            "sparse mmr",
        ),
        (
            json!({"prefetch": {"query": [1.0, 0.0], "using": "d"}, "query": {"indices": [1], "values": [1.0]}, "using": "s"}),
            "sparse rescoring",
        ),
        (
            json!({"prefetch": two_prefetches(), "query": {"recommend": {"positive": [[1.0, 0.0]]}}}),
            "prefetch under recommend",
        ),
        (
            json!({"prefetch": {"query": {"nearest": [1.0, 0.0], "mmr": {}}}, "query": {"fusion": "rrf"}}),
            "mmr inside a prefetch",
        ),
        (json!({"query": [1.0, 0.0], "shard_key": "a"}), "shard_key"),
        (json!({"query": [[1.0, 0.0], [0.0, 1.0]]}), "multivectors"),
        (
            json!({"query": {"text": "hello", "model": "m"}}),
            "inference objects",
        ),
    ];
    for (body, feature) in cases {
        assert_eq!(unsupported(body.clone()), feature, "{body}");
    }
}

#[test]
fn sparse_using_compiles_to_a_sparse_retriever() {
    let (req, post) = ir(json!({"query": {"indices": [5, 1], "values": [0.5, 2.0]}, "using": "s"}));
    assert_eq!(
        req.retrievers,
        vec![Retriever::Sparse {
            field: "s".into(),
            query: sparse(&[1, 5], &[2.0, 0.5]),
            k: 10,
            filter: None,
            params: SparseParams::default(),
        }]
    );
    assert_eq!(
        (post.kind, post.distance),
        (ScoreKind::Distance, Distance::Dot)
    );
    let corpus = json!({"must": [{"key": "g", "match": {"value": 1}}]});
    let (req, _) = ir(json!({
        "query": {"nearest": {"indices": [1], "values": [1.0]}},
        "using": "s",
        "params": {"idf": {"corpus": corpus}, "hnsw_ef": 5, "exact": true}
    }));
    assert!(matches!(
        &req.retrievers[0],
        Retriever::Sparse { params, .. } if params.idf_corpus == Some(filter(corpus.clone()))
    ));
    let (req, _) = ir(json!({
        "query": {"indices": [1], "values": [1.0]}, "using": "s", "params": {"idf": "global"}
    }));
    assert!(matches!(
        &req.retrievers[0],
        Retriever::Sparse { params, .. } if params.idf_corpus.is_none()
    ));
    let idf = "search param `idf` requires a sparse vector with the `idf` modifier";
    assert_eq!(
        bad(
            json!({"query": {"indices": [1], "values": [1.0]}, "using": "t", "params": {"idf": "global"}})
        ),
        idf
    );
    assert_eq!(
        bad(json!({"query": [1.0, 0.0], "using": "d", "params": {"idf": "global"}})),
        idf
    );
    assert_eq!(
        bad(json!({"query": [1.0, 0.0], "using": "s"})),
        "Vector s is a sparse vector"
    );
    assert_eq!(
        bad(json!({"query": {"indices": [1], "values": [1.0]}, "using": "d"})),
        "Vector d is a dense vector"
    );
    assert_eq!(
        bad(json!({"query": [1.0, 0.0], "using": "nope"})),
        "Not existing vector name error: nope"
    );
}

#[test]
fn a_hybrid_prefetch_compiles_to_fused_dense_and_sparse() {
    // LangChain's HYBRID body (`lc110:langchain_qdrant/qdrant.py:560-630`).
    let f = json!({"must": [{"key": "metadata.page", "match": {"value": 1}}]});
    let (req, post) = ir(json!({
        "prefetch": [
            {"using": "", "query": [1.0, 1.0], "filter": f, "limit": 4},
            {"using": "s", "query": {"indices": [1, 2], "values": [1.0, 1.0]}, "filter": f, "limit": 4}
        ],
        "query": {"fusion": "rrf"},
        "limit": 4,
        "offset": 0,
        "with_payload": true,
        "with_vector": false
    }));
    let (inputs, fusion, k) = fused(&req);
    assert_eq!((fusion, k), (&Fusion::Rrf { k: 1 }, 4));
    assert!(matches!(
        &inputs[0],
        Retriever::Vector { field, filter: Some(q), k: 4, .. } if field.is_empty() && *q == filter(f.clone())
    ));
    assert!(matches!(
        &inputs[1],
        Retriever::Sparse { field, filter: Some(q), k: 4, .. } if field == "s" && *q == filter(f.clone())
    ));
    assert_eq!(post.kind, ScoreKind::Fusion);
    assert_eq!(req.select.source, operon_query::SourceFilter::All);
}

#[test]
fn fusion_without_prefetch_is_400() {
    assert_eq!(
        bad(json!({"query": {"fusion": "rrf"}})),
        "Fusion query requires prefetch"
    );
    assert_eq!(
        bad(json!({"prefetch": {"query": {"fusion": "dbsf"}}, "query": {"fusion": "rrf"}})),
        "Fusion query requires prefetch"
    );
}

#[test]
fn root_without_query_and_two_prefetches_is_400() {
    // Qdrant refuses prefetches without a query at every level
    // (`qdrant:lib/collection/src/operations/universal_query/collection_query.rs`
    // `validation`, row T8-1), with one prefetch as with several.
    let merge = "A query is needed to merge the prefetches. Can't have prefetches without defining a query.";
    assert_eq!(bad(json!({"prefetch": two_prefetches()})), merge);
    assert_eq!(
        bad(json!({"prefetch": {"query": [1.0, 0.0], "using": "e"}})),
        merge
    );
    assert_eq!(
        bad(json!({
            "prefetch": {"prefetch": {"query": [1.0, 0.0], "using": "d"}, "limit": 3},
            "query": {"fusion": "rrf"}
        })),
        merge
    );
    // A leaf prefetch without a query is Qdrant's scroll, which the IR
    // cannot feed into a parent query.
    assert_eq!(
        unsupported(json!({"prefetch": {"filter": {"must": []}}, "query": {"fusion": "rrf"}})),
        "a prefetch without a query"
    );
    // No query and no prefetch: filter order, score 0.0.
    let (req, post) = ir(json!({"filter": {"must": [{"key": "a", "match": {"value": 1}}]}}));
    assert!(req.retrievers.is_empty());
    assert_eq!(post.kind, ScoreKind::Filter);
    assert_eq!(
        bad(json!({"query": [1.0, 0.0], "limit": 0})),
        "limit must be at least 1"
    );
}

#[test]
fn qdrant_validation_rules_hold_at_every_level() {
    // `score_threshold` needs a query (row T8-2).
    let threshold = "A query is needed to use the score_threshold. Can't have score_threshold without defining a query.";
    assert_eq!(bad(json!({"score_threshold": 0.5})), threshold);
    assert_eq!(
        bad(json!({"prefetch": {"score_threshold": 0.5}, "query": {"fusion": "rrf"}})),
        threshold
    );
    // A fusion takes no `using` (row T8-3); `""` is the default.
    let using = "Fusion queries cannot be combined with the 'using' field.";
    assert_eq!(
        bad(json!({"prefetch": two_prefetches(), "query": {"fusion": "rrf"}, "using": "d"})),
        using
    );
    assert_eq!(
        bad(json!({
            "prefetch": {"prefetch": two_prefetches(), "query": {"rrf": {}}, "using": "d"},
            "query": {"fusion": "dbsf"}
        })),
        using
    );
    let (req, _) = ir(json!({"prefetch": two_prefetches(), "query": {"fusion": "rrf"}, "using": ""}));
    assert_eq!(fused(&req).1, &Fusion::Rrf { k: 1 });
    // Qdrant's order: prefetches first, then the threshold.
    assert_eq!(
        bad(json!({"prefetch": two_prefetches(), "score_threshold": 0.5})),
        "A query is needed to merge the prefetches. Can't have prefetches without defining a query."
    );
}

#[test]
fn ids_use_the_resolved_examples_and_are_excluded() {
    let mut examples = ResolvedExamples::default();
    examples.vectors.insert(
        ("c".into(), String::new(), PrimaryKey::U64(7)),
        Example::Dense(vec![3.0, 4.0]),
    );
    examples.vectors.insert(
        ("c".into(), "s".into(), PrimaryKey::U64(8)),
        Example::Sparse(sparse(&[3], &[1.0])),
    );
    examples.exclude.insert(PrimaryKey::U64(7));
    let plan = compile_with(json!({"query": 7}), &examples).expect("compiles");
    let QueryPlan::Ir { request, post } = plan;
    assert!(matches!(
        &request.retrievers[0],
        Retriever::Vector { query, .. } if *query == vec![0.6, 0.8]
    ));
    assert_eq!(
        post.exclude.iter().collect::<Vec<_>>(),
        [&PrimaryKey::U64(7)]
    );
    assert_eq!(
        request.filter,
        operon_qdrant::filter::exclude_ids(None, &[PrimaryKey::U64(7)])
    );
    let QueryPlan::Ir { request, .. } =
        compile_with(json!({"query": {"nearest": 8}, "using": "s"}), &examples).expect("ok");
    assert!(matches!(
        &request.retrievers[0],
        Retriever::Sparse { query, .. } if *query == sparse(&[3], &[1.0])
    ));
}
