//! Task 8: search request compilation.

use loams_collection::{Distance, PrimaryKey};
use loams_es::EsError;
use loams_es::doc::SourceFilter;
use loams_es::http::Params;
use loams_es::mapping::EsSimilarity;
use loams_es::search::{EsScore, SEARCH_PARAMS, SearchParams, SearchPlan, compile, parse_params};
use loams_query::{
    AnnParams, FieldValue, Fusion, MissingOrder, Query, Retriever, SearchRequest, SortKey,
    SortOrder, SourceFilter as IrSourceFilter, TrackTotalHits,
};
use serde_json::{Value, json};

use crate::fixture::{NOW_MS, assert_error, view};

fn plan(body: Value) -> SearchPlan {
    plan_with(body, &SearchParams::default())
}

fn plan_with(body: Value, params: &SearchParams) -> SearchPlan {
    compile(&view(), &body, params, NOW_MS).unwrap_or_else(|e| panic!("{body}: {e}"))
}

fn plan_err(body: Value) -> EsError {
    compile(&view(), &body, &SearchParams::default(), NOW_MS).expect_err("refused")
}

fn single(plan: SearchPlan) -> SearchRequest {
    match plan {
        SearchPlan::Single { request, .. } => request,
        other => panic!("not Single: {other:?}"),
    }
}

fn params(query: &str) -> SearchParams {
    let p = Params::parse(Some(query), "/i/_search", SEARCH_PARAMS).expect("params");
    parse_params(&p).expect("parse_params")
}

fn term(field: &str, value: FieldValue) -> Query {
    Query::Term {
        field: field.to_string(),
        value,
    }
}

fn not_exists(field: &str) -> Query {
    Query::Bool {
        must: vec![],
        should: vec![],
        must_not: vec![Query::Exists {
            field: field.to_string(),
        }],
        filter: vec![],
        minimum_should_match: None,
    }
}

fn should(parts: Vec<Query>) -> Query {
    Query::Bool {
        must: vec![],
        should: parts,
        must_not: vec![],
        filter: vec![],
        minimum_should_match: None,
    }
}

fn filter(parts: Vec<Query>) -> Query {
    Query::Bool {
        must: vec![],
        should: vec![],
        must_not: vec![],
        filter: parts,
        minimum_should_match: None,
    }
}

fn match_foo() -> Value {
    json!({"bool": {"must": [{"match": {"text": {"query": "foo"}}}], "filter": []}})
}

fn knn_body(k: usize, num_candidates: usize) -> Value {
    json!({"field": "vector", "filter": [], "k": k, "num_candidates": num_candidates,
        "query_vector": [1.0, 0.0, 0.0]})
}

#[test]
fn a_langchain_knn_body_compiles_to_one_vector_retriever() {
    // C14.
    let compiled = plan_with(
        json!({"knn": knn_body(4, 50), "size": 4, "_source": true}),
        &params("_source_includes=metadata,text"),
    );
    let SearchPlan::Single { request, render } = compiled else {
        panic!("not Single");
    };
    assert_eq!(
        request.retrievers,
        vec![Retriever::Vector {
            field: "vector".to_string(),
            query: vec![1.0, 0.0, 0.0],
            k: 4,
            params: AnnParams {
                ef: Some(50),
                ..AnnParams::default()
            },
            filter: None,
        }]
    );
    assert_eq!((request.offset, request.limit), (0, 4));
    assert_eq!(request.fusion, None);
    assert_eq!(render.score, EsScore::Knn(EsSimilarity::Cosine));
    assert!(render.scores_visible);
    assert_eq!(render.source.includes, vec!["metadata", "text"]);
    // Only the vectors the source filter keeps are fetched.
    assert_eq!(request.select.source, IrSourceFilter::All);
    assert!(request.select.vectors.is_empty());
    let request = single(plan_with(
        json!({"knn": knn_body(4, 50)}),
        &SearchParams::default(),
    ));
    assert_eq!(request.select.vectors, vec!["dot", "l2", "v2", "vector"]);
    let request = single(plan(json!({"knn": knn_body(4, 50), "_source": false})));
    assert_eq!(request.select.source, IrSourceFilter::None);
    assert!(request.select.vectors.is_empty());
}

#[test]
fn knn_thresholds_are_in_engine_space() {
    let with = |field: &str, extra: Value| {
        let mut knn = json!({"field": field, "k": 3, "query_vector": [1.0, 0.0, 0.0]});
        knn.as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        single(plan(json!({"knn": knn}))).score_threshold
    };
    assert_eq!(with("vector", json!({"similarity": 0.5})), Some(0.5));
    assert_eq!(with("l2", json!({"similarity": 2.0})), Some(-2.0));
    let min = |field: &str, s: f32| {
        let body = json!({"knn": {"field": field, "k": 3, "query_vector": [1.0, 0.0, 0.0]},
            "min_score": s});
        single(plan(body)).score_threshold
    };
    assert_eq!(min("vector", 0.75), Some(0.5));
    assert_eq!(min("l2", 0.2), Some(-2.0));
    assert_eq!(min("dot", 0.75), Some(0.5));
    // `min_score` is in boosted ES score space (Task 9): with boost 2 a
    // min_score of 1.5 is an ES score of 0.75, cosine 0.5.
    let body = json!({"knn": {"field": "vector", "k": 3, "query_vector": [1.0, 0.0, 0.0],
        "boost": 2.0}, "min_score": 1.5});
    let SearchPlan::Single { request, render } = plan(body) else {
        panic!("not Single");
    };
    assert_eq!(request.score_threshold, Some(0.5));
    assert_eq!(render.boost, 2.0);
}

#[test]
fn legacy_rank_rrf_compiles_like_the_rrf_retriever() {
    let knn = knn_body(3, 10);
    let legacy = plan(json!({"query": match_foo(), "knn": knn, "rank": {"rrf": {}}, "size": 3}));
    let retriever = plan(json!({"retriever": {"rrf": {"retrievers": [
        {"standard": {"query": match_foo()}},
        {"knn": knn}
    ]}}, "size": 3}));
    assert_eq!(legacy, retriever);
    let SearchPlan::Single { request, render } = legacy else {
        panic!("not Single");
    };
    assert_eq!(request.fusion, Some(Fusion::Rrf { k: 60 }));
    assert_eq!(render.score, EsScore::Rrf);
    assert!(matches!(
        request.retrievers[0],
        Retriever::Text { k: 10, .. }
    ));
    assert!(matches!(
        request.retrievers[1],
        Retriever::Vector { k: 3, .. }
    ));
    assert_eq!((request.offset, request.limit), (0, 3));
    // `window_size` is the legacy name of `rank_window_size`.
    let legacy = single(plan(json!({"query": match_foo(), "knn": knn,
        "rank": {"rrf": {"rank_constant": 5, "window_size": 20}}})));
    assert_eq!(legacy.fusion, Some(Fusion::Rrf { k: 5 }));
    assert!(matches!(
        legacy.retrievers[0],
        Retriever::Text { k: 20, .. }
    ));
    assert_error(
        &plan_err(json!({"query": match_foo(), "rank": {"rrf": {}}})),
        400,
        "illegal_argument_exception",
        "[rank] requires a minimum of [2] result sets",
    );
    assert_error(
        &plan_err(json!({"query": match_foo(), "knn": knn, "rank": {"rescorer": {}}})),
        400,
        "illegal_argument_exception",
        "Loams does not support [rank.rescorer]",
    );
}

#[test]
fn rrf_parameters_are_carried() {
    // C23.
    let request = single(plan(json!({"retriever": {"rrf": {"retrievers": [
        {"standard": {"query": match_foo()}},
        {"knn": knn_body(4, 10)}
    ], "rank_constant": 1, "rank_window_size": 5}}, "size": 3})));
    assert_eq!(request.fusion, Some(Fusion::Rrf { k: 1 }));
    assert!(matches!(
        request.retrievers[0],
        Retriever::Text { k: 5, .. }
    ));
    assert!(matches!(
        request.retrievers[1],
        Retriever::Vector { k: 4, .. }
    ));
    // The rrf filter reaches every child; a nested rrf is fused.
    let request = single(plan(json!({"retriever": {"rrf": {"retrievers": [
        {"standard": {"query": {"match_all": {}}}},
        {"rrf": {"retrievers": [{"knn": knn_body(3, 10)}], "rank_constant": 7}}
    ], "filter": {"term": {"session_id": "s"}}}}})));
    let f = term("session_id", FieldValue::Str("s".to_string()));
    assert_eq!(
        request.retrievers[0],
        Retriever::Text {
            query: Query::Bool {
                must: vec![Query::MatchAll],
                should: vec![],
                must_not: vec![],
                filter: vec![f.clone()],
                minimum_should_match: None,
            },
            k: 10,
        }
    );
    let Retriever::Fused { inputs, fusion, .. } = &request.retrievers[1] else {
        panic!("fused");
    };
    assert_eq!(fusion, &Fusion::Rrf { k: 7 });
    assert!(matches!(&inputs[0], Retriever::Vector { filter: Some(q), .. } if *q == f));
    assert_error(
        &plan_err(
            json!({"retriever": {"rrf": {"retrievers": [{"knn": knn_body(3, 10)}],
            "rank_constant": 0}}}),
        ),
        400,
        "illegal_argument_exception",
        "[rank_constant] must be greater or equal to [1]",
    );
    assert_error(
        &plan_err(
            json!({"retriever": {"rrf": {"retrievers": [{"knn": knn_body(3, 10)}],
            "rank_window_size": 5}}, "size": 6}),
        ),
        400,
        "illegal_argument_exception",
        "[rank_window_size] must be greater than or equal to [from + size]",
    );
    assert_error(
        &plan_err(json!({"retriever": {"standard": {}}, "query": {"match_all": {}}})),
        400,
        "illegal_argument_exception",
        "cannot specify [retriever] and [query]",
    );
    assert_error(
        &plan_err(json!({"retriever": {"standard": {}}, "knn": knn_body(3, 10)})),
        400,
        "illegal_argument_exception",
        "cannot specify [retriever] and [knn]",
    );
    assert_error(
        &plan_err(json!({"retriever": {"linear": {}}})),
        400,
        "illegal_argument_exception",
        "Loams does not support [retriever.linear]",
    );
}

#[test]
fn query_plus_knn_compiles_to_hybrid_sum() {
    // C24.
    let SearchPlan::HybridSum {
        text,
        vectors,
        count_filter,
        render,
    } = plan(json!({"knn": knn_body(4, 10), "query": match_foo(), "size": 3, "from": 2}))
    else {
        panic!("not HybridSum");
    };
    let (text, weight) = text.expect("text part");
    assert_eq!(weight, 1.0);
    assert!(matches!(text.retrievers[0], Retriever::Text { k: 9, .. }));
    assert_eq!(text.limit, 9);
    assert_eq!(vectors.len(), 1);
    let (vector, similarity, boost) = &vectors[0];
    assert!(matches!(
        vector.retrievers[0],
        Retriever::Vector { k: 4, .. }
    ));
    assert_eq!((*similarity, *boost), (EsSimilarity::Cosine, 1.0));
    assert_eq!(render.score, EsScore::Sum);
    assert_eq!((render.from, render.size), (2, 3));
    let Query::Bool { should, .. } = count_filter else {
        panic!("count filter");
    };
    assert_eq!(should.len(), 2);
    assert_eq!(should[1], Query::Ids(vec![]));
    // Two knn searches and no query.
    let SearchPlan::HybridSum { text, vectors, .. } =
        plan(json!({"knn": [knn_body(2, 10), knn_body(3, 10)]}))
    else {
        panic!("not HybridSum");
    };
    assert!(text.is_none());
    assert_eq!(vectors.len(), 2);
    // Thresholds have no meaning after the sum (Ruling 12).
    let mut knn = knn_body(2, 10);
    knn["similarity"] = json!(0.5);
    assert_error(
        &plan_err(json!({"knn": knn, "query": match_foo()})),
        400,
        "illegal_argument_exception",
        "Loams does not support",
    );
    assert_error(
        &plan_err(json!({"knn": knn_body(2, 10), "query": match_foo(), "min_score": 1})),
        400,
        "illegal_argument_exception",
        "Loams does not support",
    );
}

#[test]
fn query_vector_builder_is_refused() {
    // C26.
    let error = plan_err(
        json!({"knn": {"field": "vector", "k": 3, "num_candidates": 10,
        "query_vector_builder": {"text_embedding": {"model_id": "m", "model_text": "t"}}}}),
    );
    assert_eq!(
        (error.status, error.kind.as_str()),
        (400, "illegal_argument_exception")
    );
    assert!(error.reason.contains("ML"), "{}", error.reason);
}

fn script_body(source: &str, query: Value) -> Value {
    json!({"query": {"script_score": {"query": query,
        "script": {"source": source, "params": {"query_vector": [1.0, 0.0, 0.0]}}}}})
}

#[test]
fn script_score_compiles_to_exact_search_with_a_metric() {
    for (source, distance, function) in [
        (
            "cosineSimilarity(params.query_vector, 'vector') + 1.0",
            Distance::Cosine,
            loams_es::dsl::ScriptFunction::CosinePlusOne,
        ),
        (
            "1 / (1 + l2norm(params.query_vector, 'vector'))",
            Distance::Euclid,
            loams_es::dsl::ScriptFunction::InverseOnePlusL2,
        ),
        (
            "double value = dotProduct(params.query_vector, 'vector'); return sigmoid(1, Math.E, -value);",
            Distance::Dot,
            loams_es::dsl::ScriptFunction::SigmoidDot,
        ),
    ] {
        let SearchPlan::ScriptScore {
            request,
            function: f,
            count_filter,
            render,
        } = plan(script_body(source, json!({"match_all": {}})))
        else {
            panic!("not ScriptScore");
        };
        assert_eq!(f, function);
        let Retriever::Vector {
            params, filter, k, ..
        } = &request.retrievers[0]
        else {
            panic!("vector");
        };
        assert!(params.exact);
        assert_eq!(params.distance, Some(distance));
        assert_eq!(filter, &Some(Query::MatchAll));
        assert_eq!(*k, 10);
        assert_eq!(count_filter, Query::MatchAll);
        assert_eq!(render.score, EsScore::Script(function));
    }
    // min_score is the inverse of the function, over the boosted score.
    let mut body = script_body(
        "cosineSimilarity(params.query_vector, 'vector') + 1.0",
        json!({"match_all": {}}),
    );
    body["query"]["script_score"]["min_score"] = json!(3.0);
    body["query"]["script_score"]["boost"] = json!(2.0);
    let SearchPlan::ScriptScore {
        request, render, ..
    } = plan(body)
    else {
        panic!("not ScriptScore");
    };
    assert_eq!(request.score_threshold, Some(0.5));
    assert_eq!(render.boost, 2.0);
}

#[test]
fn field_sort_hides_scores_and_filters() {
    // C35's shape.
    let SearchPlan::Single { request, render } = plan_with(
        json!({"query": {"term": {"session_id": "s"}}, "size": 100}),
        &params("sort=created_at:asc"),
    ) else {
        panic!("not Single");
    };
    assert!(request.retrievers.is_empty());
    assert_eq!(
        request.filter,
        Some(term("session_id", FieldValue::Str("s".into())))
    );
    assert!(!render.scores_visible);
    let key = SortKey::Field {
        field: "created_at".to_string(),
        order: SortOrder::Asc,
        missing: MissingOrder::Last,
    };
    assert_eq!(render.sort_keys, vec![key.clone()]);
    assert_eq!(render.user_sort_len, 1);
    assert_eq!(
        request.sort,
        vec![
            key,
            SortKey::Pk {
                order: SortOrder::Asc
            }
        ]
    );
    // The body forms.
    let render = plan(
        json!({"sort": [{"metadata.page": {"order": "desc", "missing": "_first",
        "mode": "max"}}, "_doc"]}),
    )
    .render()
    .clone();
    assert_eq!(
        render.sort_keys,
        vec![
            SortKey::Field {
                field: "metadata.page".to_string(),
                order: SortOrder::Desc,
                missing: MissingOrder::First,
            },
            SortKey::Pk {
                order: SortOrder::Asc
            }
        ]
    );
    // `_score` first keeps scores and the retriever, and a `_doc` after it.
    let SearchPlan::Single { request, render } =
        plan(json!({"query": match_foo(), "sort": ["_score", "_doc"]}))
    else {
        panic!("not Single");
    };
    assert!(render.scores_visible);
    assert_eq!(request.retrievers.len(), 1);
    assert_eq!(
        request.sort,
        vec![
            SortKey::Score {
                order: SortOrder::Desc
            },
            SortKey::Pk {
                order: SortOrder::Asc
            }
        ]
    );
    assert_eq!(render.user_sort_len, 2);
    // A field after `_score` is refused: the engine breaks score ties by PK
    // only (PR #74 review).
    let err = plan_err(json!({"query": match_foo(), "sort": ["_score", {"created_at": "desc"}]}));
    assert_eq!(
        err.reason,
        "Loams does not support [sort on a field after _score] (Elasticsearch API Phase A)"
    );
    // An unmapped key with `unmapped_type` is dropped.
    let render = plan(json!({"sort": [{"nope": {"unmapped_type": "long"}}]}))
        .render()
        .clone();
    assert!(render.sort_keys.is_empty());
    assert_error(
        &plan_err(json!({"sort": ["nope"]})),
        400,
        "search_phase_execution_exception",
        "all shards failed",
    );
    assert_eq!(
        plan_err(json!({"sort": ["nope"]})).to_body()["error"]["root_cause"][0]["reason"],
        "No mapping found for [nope] in order to sort on"
    );
    for body in [
        json!({"sort": [{"created_at": {"mode": "avg"}}]}),
        json!({"sort": [{"created_at": {"missing": 0}}]}),
        json!({"sort": [{"_doc": "desc"}]}),
        json!({"sort": ["created_at", "_score"]}),
        json!({"sort": ["created_at"], "track_scores": true}),
        json!({"sort": ["created_at"], "knn": knn_body(3, 10)}),
    ] {
        assert_error(
            &plan_err(body),
            400,
            "illegal_argument_exception",
            "Loams does not support",
        );
    }
}

#[test]
fn sorting_on_a_text_field_is_the_fielddata_error() {
    for field in ["text", "llm_output"] {
        let error = plan_err(json!({"sort": [field]}));
        assert_error(
            &error,
            400,
            "search_phase_execution_exception",
            "all shards failed",
        );
        let body = error.to_body();
        let root = &body["error"]["root_cause"][0];
        assert_eq!(root["type"], "illegal_argument_exception");
        assert!(
            root["reason"]
                .as_str()
                .expect("reason")
                .starts_with("Text fields are not optimised")
        );
    }
}

#[test]
fn a_size_of_10000_is_allowed_and_10001_is_not() {
    // C37.
    let request = single(plan(
        json!({"query": {"bool": {"must": [{"terms": {"_id": ["a"]}}]}},
        "size": 10000}),
    ));
    assert_eq!(request.limit, 10_000);
    let error = plan_err(json!({"from": 1, "size": 10000}));
    assert_error(
        &error,
        400,
        "search_phase_execution_exception",
        "all shards failed",
    );
    assert!(
        error.to_body()["error"]["root_cause"][0]["reason"]
            .as_str()
            .expect("reason")
            .starts_with("Result window is too large, from + size must be less than or equal to: [10000] but was [10001].")
    );
    // URL values override the body (Ruling 20).
    let request = single(plan_with(
        json!({"from": 5, "size": 5}),
        &params("from=1&size=2"),
    ));
    assert_eq!((request.offset, request.limit), (1, 2));
}

#[test]
fn search_after_prefix_builds_a_lexicographic_filter() {
    let range = |field: &str, gt: Option<FieldValue>, lt: Option<FieldValue>| Query::Range {
        field: field.to_string(),
        gt,
        gte: None,
        lt,
        lte: None,
    };
    let date = FieldValue::Date(1_727_172_672_000_000);
    let request = single(plan(json!({"sort": [{"created_at": "asc"}],
        "search_after": [1727172672000_i64]})));
    assert_eq!(
        request.filter,
        Some(should(vec![
            range("created_at", Some(date.clone()), None),
            not_exists("created_at")
        ]))
    );
    // Two keys: (a > v) OR (a == v AND b beyond w), with the query.
    let request = single(plan(json!({"query": {"term": {"flag": true}},
        "sort": [{"created_at": "asc"}, {"session_id": {"order": "desc", "missing": "_first"}}],
        "search_after": ["2024-09-24T10:11:12Z", "m"]})));
    let first = should(vec![
        range("created_at", Some(date.clone()), None),
        not_exists("created_at"),
    ]);
    let second = filter(vec![
        term("created_at", date.clone()),
        range("session_id", None, Some(FieldValue::Str("m".into()))),
    ]);
    assert_eq!(
        request.filter,
        Some(filter(vec![
            term("flag", FieldValue::Bool(true)),
            should(vec![first, second])
        ]))
    );
    // A missing value echoed back: only later keys can move past it.
    let request = single(plan(json!({"sort": ["created_at", "session_id"],
        "search_after": [null, "m"]})));
    assert_eq!(
        request.filter,
        Some(filter(vec![
            not_exists("created_at"),
            should(vec![
                range("session_id", Some(FieldValue::Str("m".into())), None),
                not_exists("session_id")
            ])
        ]))
    );
    // A shard failure in ES (row T11-3).
    let e = plan_err(json!({"sort": ["created_at"], "search_after": [1, 2]}));
    assert_error(
        &e,
        400,
        "search_phase_execution_exception",
        "all shards failed",
    );
    assert_eq!(
        e.to_body()["error"]["root_cause"][0]["reason"],
        "search_after has 2 value(s) but sort has 1."
    );
    assert_error(
        &plan_err(json!({"sort": ["created_at"], "search_after": [1], "from": 3})),
        400,
        "action_request_validation_exception",
        "Validation Failed: 1: [from] parameter must be set to 0 when [search_after] is used;",
    );
}

#[test]
fn search_after_on_score_is_refused() {
    for body in [
        json!({"search_after": [1.2]}),
        json!({"sort": ["_score"], "search_after": [1.2]}),
    ] {
        assert_error(
            &plan_err(body),
            400,
            "illegal_argument_exception",
            "Loams does not support [search_after on _score] (Elasticsearch API Phase A)",
        );
    }
}

#[test]
fn track_total_hits_forms() {
    let track = |body: Value| single(plan(body)).track_total_hits;
    assert_eq!(track(json!({})), TrackTotalHits::UpTo(10_000));
    assert_eq!(
        track(json!({"track_total_hits": true})),
        TrackTotalHits::Exact
    );
    assert_eq!(
        track(json!({"track_total_hits": false})),
        TrackTotalHits::None
    );
    assert_eq!(
        track(json!({"track_total_hits": 100})),
        TrackTotalHits::UpTo(100)
    );
    assert_eq!(track(json!({"track_total_hits": -1})), TrackTotalHits::None);
    let request = single(plan_with(
        json!({"track_total_hits": 5}),
        &params("track_total_hits=true"),
    ));
    assert_eq!(request.track_total_hits, TrackTotalHits::Exact);
    assert_error(
        &plan_err(json!({"track_total_hits": -2})),
        400,
        "illegal_argument_exception",
        "[track_total_hits] parameter must be positive or equals to -1",
    );
}

#[test]
fn unknown_body_keys_are_parsing_errors() {
    assert_error(
        &plan_err(json!({"querry": {}})),
        400,
        "parsing_exception",
        "Unknown key for a START_OBJECT in [querry].",
    );
    assert_error(
        &plan_err(json!({"sizee": 1})),
        400,
        "parsing_exception",
        "Unknown key for a VALUE_NUMBER in [sizee].",
    );
    assert_error(
        &plan_err(json!({"collapse": {"field": "session_id"}})),
        400,
        "illegal_argument_exception",
        "Loams does not support [collapse] (Elasticsearch API Phase A)",
    );
    assert_error(
        &plan_err(json!({"explain": true})),
        400,
        "illegal_argument_exception",
        "Loams does not support [explain]",
    );
    plan(
        json!({"explain": false, "profile": false, "timeout": "1s", "stats": ["a"],
        "version": true, "seq_no_primary_term": true}),
    );
}

#[test]
fn phase_b_body_keys_are_refused() {
    for key in ["aggs", "aggregations", "highlight", "pit"] {
        assert_error(
            &plan_err(json!({key: {}})),
            400,
            "illegal_argument_exception",
            &format!("Loams does not support [{key}] (Elasticsearch API Phase A)"),
        );
    }
}

#[test]
fn query_only_and_url_query_compile_to_one_text_retriever() {
    let SearchPlan::Single { request, render } = plan(
        json!({"query": {"match": {"text": "foo"}}, "from": 2, "size": 3, "min_score": 0.5,
            "version": true}),
    ) else {
        panic!("not Single");
    };
    assert!(matches!(
        request.retrievers[0],
        Retriever::Text { k: 5, .. }
    ));
    assert_eq!((request.offset, request.limit), (2, 3));
    assert_eq!(request.score_threshold, Some(0.5));
    assert_eq!(render.score, EsScore::Bm25);
    assert!(render.version);
    // No query at all: match_all.
    let request = single(plan(json!({})));
    assert_eq!(
        request.retrievers,
        vec![Retriever::Text {
            query: Query::MatchAll,
            k: 10
        }]
    );
    // `q` replaces the body's query (Ruling 20).
    let request = single(plan_with(
        json!({"query": {"match_none": {}}}),
        &params("q=title:foo&df=txt&default_operator=AND"),
    ));
    assert_eq!(
        request.retrievers,
        vec![Retriever::Text {
            query: Query::QueryString {
                query: "title:foo".to_string(),
                default_fields: vec!["txt".to_string()],
                default_operator: loams_query::BoolOperator::And,
            },
            k: 10
        }]
    );
    // `ids` from the caches (C49).
    let request = single(plan(
        json!({"query": {"ids": {"values": ["a"]}}, "size": 1}),
    ));
    assert_eq!(
        request.retrievers,
        vec![Retriever::Text {
            query: Query::Ids(vec![PrimaryKey::Str("a".into())]),
            k: 1
        }]
    );
}

#[test]
fn search_params_follow_es() {
    let p = params(
        "from=1&size=2&sort=a:asc,b&_source=false&track_total_hits=7&q=x&df=t&default_operator=and&rest_total_hits_as_int=true&ignore_unavailable=true&allow_no_indices=false&track_scores=true&search_type=dfs_query_then_fetch&preference=_local&routing=r&request_cache=false&timeout=1s",
    );
    assert_eq!(
        p,
        SearchParams {
            from: Some(1),
            size: Some(2),
            sort: Some(vec!["a:asc".to_string(), "b".to_string()]),
            source: Some(SourceFilter {
                enabled: false,
                includes: vec![],
                excludes: vec![],
            }),
            track_total_hits: Some(json!(7)),
            q: Some("x".to_string()),
            df: Some("t".to_string()),
            default_operator: Some(loams_query::BoolOperator::And),
            rest_total_hits_as_int: true,
            ignore_unavailable: true,
            allow_no_indices: false,
            track_scores: Some(true),
        }
    );
    assert!(params("").allow_no_indices);
    let refused = Params::parse(Some("typed_keys=true"), "/i/_search", SEARCH_PARAMS)
        .expect_err("typed_keys");
    assert_eq!(
        refused.reason,
        "request [/i/_search] contains unrecognized parameter: [typed_keys]"
    );
    let p = Params::parse(Some("search_type=scan"), "/i/_search", SEARCH_PARAMS).expect("params");
    assert!(parse_params(&p).is_err());
}
