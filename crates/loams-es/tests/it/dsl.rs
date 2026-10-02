//! Task 7: the Query DSL → the search IR.

use loams_collection::PrimaryKey;
use loams_es::EsError;
use loams_es::dsl::{
    KnnSpec, ParsedQuery, QueryContext, Rounding, ScriptFunction, coerce, parse_date_math,
    parse_filter_list, parse_knn, parse_leaf, parse_query, recognise_script,
};
use loams_es::mapping::IndexView;
use loams_query::{BoolOperator, FieldValue, Fuzziness, MultiMatchKind, Query};
use serde_json::{Value, json};

use crate::fixture::{NOW_MS, assert_error, view};

fn ctx(view: &IndexView) -> QueryContext<'_> {
    QueryContext::new(view, NOW_MS)
}

fn leaf(v: Value) -> Query {
    let view = view();
    parse_leaf(&v, &ctx(&view)).unwrap_or_else(|e| panic!("{v}: {e}"))
}

fn leaf_err(v: Value) -> EsError {
    let view = view();
    parse_leaf(&v, &ctx(&view)).expect_err("refused")
}

fn top(v: Value) -> ParsedQuery {
    let view = view();
    parse_query(&v, &ctx(&view)).unwrap_or_else(|e| panic!("{v}: {e}"))
}

fn term(field: &str, value: FieldValue) -> Query {
    Query::Term {
        field: field.to_string(),
        value,
    }
}

fn s(v: &str) -> FieldValue {
    FieldValue::Str(v.to_string())
}

fn bool_q(must: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>, filter: Vec<Query>) -> Query {
    Query::Bool {
        must,
        should,
        must_not,
        filter,
        minimum_should_match: None,
    }
}

fn exists(field: &str) -> Query {
    Query::Exists {
        field: field.to_string(),
    }
}

fn boost(query: Query, boost: f32) -> Query {
    Query::Boost {
        query: Box::new(query),
        boost,
    }
}

/// The µs of an RFC 3339 instant.
fn us(text: &str) -> i64 {
    let at = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .expect("rfc3339");
    (at.unix_timestamp_nanos() / 1000) as i64
}

#[test]
fn every_phase_a_leaf_parses() {
    let matched = |text: &str| Query::Match {
        field: "text".to_string(),
        text: text.to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    };
    let rows: Vec<(Value, Query)> = vec![
        (json!({"match_all": {}}), Query::MatchAll),
        (
            json!({"match_all": {"boost": 2}}),
            boost(Query::MatchAll, 2.0),
        ),
        (json!({"match_none": {}}), Query::MatchNone),
        (json!({"match": {"text": "foo bar"}}), matched("foo bar")),
        (
            json!({"match": {"text": {"query": "foo", "_name": "n"}}}),
            matched("foo"),
        ),
        (
            json!({"match": {"text": {"query": "foo", "operator": "AND",
                "minimum_should_match": "75%", "fuzziness": "AUTO", "analyzer": "english",
                "lenient": true, "prefix_length": 0, "max_expansions": 50,
                "fuzzy_transpositions": true, "zero_terms_query": "none", "boost": 3}}}),
            boost(
                Query::Match {
                    field: "text".to_string(),
                    text: "foo".to_string(),
                    operator: BoolOperator::And,
                    minimum_should_match: Some("75%".to_string()),
                    fuzziness: Some(Fuzziness::Auto),
                    analyzer: Some("english".to_string()),
                },
                3.0,
            ),
        ),
        (
            json!({"match": {"session_id": "s1"}}),
            term("session_id", s("s1")),
        ),
        (
            json!({"match": {"metadata.page": "3"}}),
            term("metadata.page", FieldValue::I64(3)),
        ),
        (
            json!({"match": {"metadata.page": {"query": "x", "lenient": true}}}),
            Query::MatchNone,
        ),
        (
            json!({"match_phrase": {"text": "foo bar"}}),
            Query::MatchPhrase {
                field: "text".to_string(),
                text: "foo bar".to_string(),
                slop: 0,
            },
        ),
        (
            json!({"match_phrase": {"text": {"query": "foo bar", "slop": 2}}}),
            Query::MatchPhrase {
                field: "text".to_string(),
                text: "foo bar".to_string(),
                slop: 2,
            },
        ),
        (
            json!({"match_phrase": {"session_id": "s1"}}),
            term("session_id", s("s1")),
        ),
        (
            json!({"multi_match": {"query": "q", "fields": ["title^2.5", "txt"]}}),
            Query::MultiMatch {
                fields: vec![("title".to_string(), 2.5), ("txt".to_string(), 1.0)],
                text: "q".to_string(),
                kind: MultiMatchKind::BestFields,
                operator: BoolOperator::Or,
                tie_breaker: None,
            },
        ),
        (
            json!({"multi_match": {"query": "q", "fields": "t*", "type": "most_fields"}}),
            Query::MultiMatch {
                fields: vec![
                    ("text".to_string(), 1.0),
                    ("text.keyword".to_string(), 1.0),
                    ("title".to_string(), 1.0),
                    ("txt".to_string(), 1.0),
                ],
                text: "q".to_string(),
                kind: MultiMatchKind::MostFields,
                operator: BoolOperator::Or,
                tie_breaker: None,
            },
        ),
        (
            json!({"term": {"session_id": "s1"}}),
            term("session_id", s("s1")),
        ),
        (
            json!({"term": {"session_id": {"value": "s1", "boost": 2, "case_insensitive": false}}}),
            boost(term("session_id", s("s1")), 2.0),
        ),
        (
            json!({"term": {"flag": "true"}}),
            term("flag", FieldValue::Bool(true)),
        ),
        (
            json!({"term": {"score": "1.5"}}),
            term("score", FieldValue::F64(1.5)),
        ),
        (
            json!({"term": {"created_at": 1727172672000_i64}}),
            term("created_at", FieldValue::Date(1_727_172_672_000_000)),
        ),
        (
            json!({"term": {"created_at": "2026-09-24"}}),
            Query::Range {
                field: "created_at".to_string(),
                gt: None,
                gte: Some(FieldValue::Date(us("2026-09-24T00:00:00Z"))),
                lt: None,
                lte: Some(FieldValue::Date(us("2026-09-24T23:59:59.999Z"))),
            },
        ),
        (
            json!({"terms": {"session_id": ["a", 1], "boost": 2}}),
            boost(
                Query::Terms {
                    field: "session_id".to_string(),
                    values: vec![s("a"), s("1")],
                },
                2.0,
            ),
        ),
        (
            json!({"ids": {"values": ["a", "b"]}}),
            Query::Ids(vec![
                PrimaryKey::Str("a".into()),
                PrimaryKey::Str("b".into()),
            ]),
        ),
        (
            json!({"range": {"metadata.page": {"gte": 1, "lt": "5"}}}),
            Query::Range {
                field: "metadata.page".to_string(),
                gt: None,
                gte: Some(FieldValue::I64(1)),
                lt: Some(FieldValue::I64(5)),
                lte: None,
            },
        ),
        (
            json!({"range": {"metadata.page": {"gt": 1.5}}}),
            Query::Range {
                field: "metadata.page".to_string(),
                gt: Some(FieldValue::F64(1.5)),
                gte: None,
                lt: None,
                lte: None,
            },
        ),
        (
            json!({"range": {"metadata.page": {"from": 1, "to": 4, "include_upper": false}}}),
            Query::Range {
                field: "metadata.page".to_string(),
                gt: None,
                gte: Some(FieldValue::I64(1)),
                lt: Some(FieldValue::I64(4)),
                lte: None,
            },
        ),
        (
            json!({"range": {"created_at": {"gt": "now-1d/d", "lte": "now/d",
                "format": "strict_date_optional_time||epoch_millis", "time_zone": "UTC",
                "relation": "intersects"}}}),
            Query::Range {
                field: "created_at".to_string(),
                gt: Some(FieldValue::Date(us("2026-09-23T23:59:59.999Z"))),
                gte: None,
                lt: None,
                lte: Some(FieldValue::Date(us("2026-09-24T23:59:59.999Z"))),
            },
        ),
        (json!({"exists": {"field": "text"}}), exists("text")),
        (
            json!({"prefix": {"session_id": "ab"}}),
            Query::Prefix {
                field: "session_id".to_string(),
                value: "ab".to_string(),
            },
        ),
        (
            json!({"prefix": {"session_id": {"value": "ab", "rewrite": "constant_score"}}}),
            Query::Prefix {
                field: "session_id".to_string(),
                value: "ab".to_string(),
            },
        ),
        (
            json!({"wildcard": {"session_id": {"wildcard": "a?c*"}}}),
            Query::Wildcard {
                field: "session_id".to_string(),
                pattern: "a?c*".to_string(),
            },
        ),
        (
            json!({"fuzzy": {"session_id": "abc"}}),
            Query::Fuzzy {
                field: "session_id".to_string(),
                value: "abc".to_string(),
                fuzziness: Fuzziness::Auto,
            },
        ),
        (
            json!({"fuzzy": {"text": {"value": "abc", "fuzziness": 1, "prefix_length": 0,
                "max_expansions": 50, "transpositions": true}}}),
            Query::Fuzzy {
                field: "text".to_string(),
                value: "abc".to_string(),
                fuzziness: Fuzziness::Edits(1),
            },
        ),
        (
            json!({"query_string": {"query": "a AND b"}}),
            Query::QueryString {
                query: "a AND b".to_string(),
                default_fields: vec![
                    "text".to_string(),
                    "metadata.author".to_string(),
                    "title".to_string(),
                    "txt".to_string(),
                ],
                default_operator: BoolOperator::Or,
            },
        ),
        (
            json!({"query_string": {"query": "a", "default_field": "title",
                "default_operator": "and", "lenient": true}}),
            Query::QueryString {
                query: "a".to_string(),
                default_fields: vec!["title".to_string()],
                default_operator: BoolOperator::And,
            },
        ),
        (
            json!({"constant_score": {"filter": {"term": {"session_id": "s"}}, "boost": 1.5}}),
            Query::ConstantScore {
                query: Box::new(term("session_id", s("s"))),
                score: 1.5,
            },
        ),
        (
            json!({"bool": {"must": [{"match_all": {}}], "should": {"match_none": {}},
                "must_not": [], "filter": [{"exists": {"field": "flag"}}],
                "minimum_should_match": 1, "adjust_pure_negative": true}}),
            Query::Bool {
                must: vec![Query::MatchAll],
                should: vec![Query::MatchNone],
                must_not: Vec::new(),
                filter: vec![exists("flag")],
                minimum_should_match: Some("1".to_string()),
            },
        ),
    ];
    for (input, expected) in rows {
        assert_eq!(leaf(input.clone()), expected, "{input}");
    }
}

#[test]
fn leaf_parameters_outside_phase_a_are_refused() {
    for (input, reason) in [
        (
            json!({"term": {"session_id": {"value": "a", "case_insensitive": true}}}),
            "Loams does not support",
        ),
        (
            json!({"match": {"text": {"query": "a", "prefix_length": 2}}}),
            "Loams does not support",
        ),
        (
            json!({"match": {"text": {"query": "a", "minimum_should_match": "3<90%"}}}),
            "Loams does not support",
        ),
        (
            json!({"match": {"text": {"query": "a", "zero_terms_query": "all"}}}),
            "Loams does not support",
        ),
        (
            json!({"multi_match": {"query": "a", "type": "cross_fields"}}),
            "Loams does not support",
        ),
        (
            json!({"multi_match": {"query": "a", "fuzziness": "AUTO"}}),
            "Loams does not support",
        ),
        (
            json!({"terms": {"session_id": {"index": "i", "id": "1", "path": "p"}}}),
            "Loams does not support",
        ),
        (
            json!({"range": {"created_at": {"gte": "now", "time_zone": "+01:00"}}}),
            "Loams does not support",
        ),
        (
            json!({"range": {"created_at": {"gte": "now", "relation": "WITHIN"}}}),
            "Loams does not support",
        ),
        (
            json!({"fuzzy": {"text": {"value": "a", "transpositions": false}}}),
            "Loams does not support",
        ),
        (
            json!({"query_string": {"query": "a", "phrase_slop": 1}}),
            "Loams does not support",
        ),
    ] {
        assert_error(&leaf_err(input), 400, "illegal_argument_exception", reason);
    }
    assert_error(
        &leaf_err(json!({"match": {"text": {"query": "a", "bogus": 1}}})),
        400,
        "parsing_exception",
        "[match] query does not support [bogus]",
    );
    assert_error(
        &leaf_err(json!({"match": {"text": {"query": "a", "analyzer": "french"}}})),
        400,
        "illegal_argument_exception",
        "[match] analyzer [french] not found",
    );
    // Both lower bounds: the last key wins, as in ES (row T11-3).
    assert_eq!(
        leaf(json!({"range": {"metadata.page": {"gt": 1, "gte": 2}}})),
        Query::Range {
            field: "metadata.page".to_string(),
            gt: None,
            gte: Some(FieldValue::I64(2)),
            lt: None,
            lte: None,
        }
    );
    assert_eq!(
        leaf(json!({"range": {"metadata.page": {"lte": 9, "lt": 5, "from": 1}}})),
        Query::Range {
            field: "metadata.page".to_string(),
            gt: None,
            gte: Some(FieldValue::I64(1)),
            lt: Some(FieldValue::I64(5)),
            lte: None,
        }
    );
    let many: Vec<u32> = (0..65_537).collect();
    assert_error(
        &leaf_err(json!({"terms": {"session_id": many}})),
        400,
        "illegal_argument_exception",
        "The number of terms [65537] used in the Terms Query request has exceeded the allowed \
         maximum of [65536].",
    );
}

#[test]
fn query_shape_errors_use_es_texts() {
    assert_error(
        &leaf_err(json!({"match": {}, "term": {}})),
        400,
        "parsing_exception",
        "[match] malformed query, expected [END_OBJECT] but found [FIELD_NAME]",
    );
    assert_error(
        &leaf_err(json!({})),
        400,
        "parsing_exception",
        "query malformed, empty clause found",
    );
    assert_error(
        &leaf_err(json!({"nope": {}})),
        400,
        "parsing_exception",
        "unknown query [nope]",
    );
    let mut deep = json!({"match_all": {}});
    for _ in 0..30 {
        deep = json!({"bool": {"must": [deep]}});
    }
    assert_error(
        &leaf_err(deep),
        400,
        "illegal_argument_exception",
        "The nested depth of the query exceeds the maximum nested depth",
    );
    let mut ok = json!({"match_all": {}});
    for _ in 0..29 {
        ok = json!({"bool": {"must": [ok]}});
    }
    leaf(ok);
    assert_error(
        &leaf_err(json!({"match_all": {"boost": -1}})),
        400,
        "illegal_argument_exception",
        "negative [boost] are not allowed.",
    );
}

#[test]
fn multi_match_carries_the_tie_breaker() {
    let q = leaf(
        json!({"multi_match": {"query": "what is it", "type": "best_fields",
        "fields": ["title", "txt"], "tie_breaker": 0.5}}),
    );
    assert_eq!(
        q,
        Query::MultiMatch {
            fields: vec![("title".to_string(), 1.0), ("txt".to_string(), 1.0)],
            text: "what is it".to_string(),
            kind: MultiMatchKind::BestFields,
            operator: BoolOperator::Or,
            tie_breaker: Some(0.5),
        }
    );
    assert_error(
        &leaf_err(json!({"multi_match": {"query": "q", "fields": ["title"], "tie_breaker": 1.5}})),
        400,
        "parsing_exception",
        "[multi_match] [tie_breaker]",
    );
    // No `fields`: every searchable text and keyword field.
    let Query::MultiMatch { fields, .. } = leaf(json!({"multi_match": {"query": "q"}})) else {
        panic!("multi_match");
    };
    let names: Vec<&str> = fields.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(
        names,
        [
            "text",
            "text.keyword",
            "metadata.author",
            "metadata.author.keyword",
            "session_id",
            "title",
            "txt"
        ]
    );
}

#[test]
fn a_string_term_matches_a_long_field() {
    assert_eq!(
        leaf(json!({"term": {"metadata.page": "1"}})),
        term("metadata.page", FieldValue::I64(1))
    );
    assert_eq!(
        leaf(json!({"term": {"metadata.page": 0}})),
        term("metadata.page", FieldValue::I64(0))
    );
    assert_eq!(
        leaf(json!({"term": {"metadata.page": 2.0}})),
        term("metadata.page", FieldValue::I64(2))
    );
    // A fractional value stays a float, which no long matches.
    assert_eq!(
        leaf(json!({"term": {"metadata.page": 2.5}})),
        term("metadata.page", FieldValue::F64(2.5))
    );
    assert_error(
        &leaf_err(json!({"term": {"metadata.page": "abc"}})),
        400,
        "query_shard_exception",
        "failed to create query: For input string: \"abc\"",
    );
}

#[test]
fn a_keyword_subfield_term_filters_knn() {
    let view = view();
    let filter = parse_filter_list(
        &json!([{"bool": {"must": [{"term": {"metadata.author.keyword": {"value": "Stephen King"}}}]}}]),
        &ctx(&view),
    )
    .expect("filter");
    assert_eq!(
        filter,
        Some(bool_q(
            vec![term("metadata.author.keyword", s("Stephen King"))],
            vec![],
            vec![],
            vec![]
        ))
    );
    assert_eq!(
        parse_filter_list(&json!([]), &ctx(&view)).expect("empty"),
        None
    );
    assert_eq!(
        parse_filter_list(
            &json!([{"term": {"session_id": "a"}}, {"exists": {"field": "flag"}}]),
            &ctx(&view)
        )
        .expect("two"),
        Some(bool_q(
            vec![],
            vec![],
            vec![],
            vec![term("session_id", s("a")), exists("flag")]
        ))
    );
}

#[test]
fn wildcard_on_a_text_field_matches_terms() {
    assert_eq!(
        leaf(json!({"wildcard": {"metadata.author": "stephe*"}})),
        Query::Wildcard {
            field: "metadata.author".to_string(),
            pattern: "stephe*".to_string(),
        }
    );
    assert_error(
        &leaf_err(json!({"wildcard": {"metadata.page": "1*"}})),
        400,
        "query_shard_exception",
        "failed to create query: Can only use wildcard queries on keyword and text fields - not \
         on [metadata.page] which is of type [long]",
    );
}

#[test]
fn term_on_id_is_ids() {
    let ids = |v: &[&str]| Query::Ids(v.iter().map(|s| PrimaryKey::Str(s.to_string())).collect());
    assert_eq!(leaf(json!({"term": {"_id": "k"}})), ids(&["k"]));
    assert_eq!(
        leaf(json!({"terms": {"_id": ["a", "b"]}})),
        ids(&["a", "b"])
    );
    assert_eq!(leaf(json!({"ids": {"values": ["a", 7]}})), ids(&["a", "7"]));
    assert_eq!(leaf(json!({"match": {"_id": "k"}})), ids(&["k"]));
    assert_eq!(leaf(json!({"exists": {"field": "_id"}})), Query::MatchAll);
}

#[test]
fn unmapped_fields_match_nothing() {
    for q in [
        json!({"match": {"nope": "x"}}),
        json!({"term": {"nope": "x"}}),
        json!({"terms": {"nope": ["x"]}}),
        json!({"range": {"nope": {"gte": 1}}}),
        json!({"prefix": {"nope": "x"}}),
        json!({"exists": {"field": "nope"}}),
    ] {
        assert_eq!(leaf(q.clone()), Query::MatchNone, "{q}");
    }
    assert_eq!(
        leaf(json!({"exists": {"field": "metadata"}})),
        bool_q(
            vec![],
            vec![
                exists("metadata.page"),
                exists("metadata.author"),
                exists("metadata.author.keyword")
            ],
            vec![],
            vec![]
        )
    );
}

#[test]
fn unindexed_fields_refuse_queries() {
    // T2-2: an unindexed text and a binary are unindexed keywords in the
    // schema; the ES type decides.
    for field in ["llm_output", "vector_dump"] {
        for q in [
            json!({"term": {field: "x"}}),
            json!({"match": {field: "x"}}),
            json!({"prefix": {field: "x"}}),
            json!({"range": {field: {"gte": "a"}}}),
            json!({"multi_match": {"query": "x", "fields": [field]}}),
        ] {
            // ES's texts (row T11-3): the binary mapper's own.
            let why = if field == "vector_dump" {
                "Binary fields do not support searching".to_string()
            } else {
                format!("Cannot search on field [{field}] since it is not indexed.")
            };
            assert_error(
                &leaf_err(q.clone()),
                400,
                "query_shard_exception",
                &format!("failed to create query: {why}"),
            );
        }
        // Neither keeps norms nor doc values in ES: `exists` finds nothing.
        assert_eq!(leaf(json!({"exists": {"field": field}})), Query::MatchNone);
    }
    // Wildcard expansion skips them.
    let Query::MultiMatch { fields, .. } =
        leaf(json!({"multi_match": {"query": "x", "fields": ["*"]}}))
    else {
        panic!("multi_match");
    };
    assert!(
        fields
            .iter()
            .all(|(f, _)| f != "llm_output" && f != "vector_dump")
    );
}

#[test]
fn flattened_paths_keep_the_json_type() {
    // E11: a flattened path keeps the JSON value's own type, so numeric
    // range bounds compare numerically (O-M15-2).
    assert_eq!(
        leaf(json!({"term": {"labels.priority": 2}})),
        term("labels.priority", FieldValue::I64(2))
    );
    assert_eq!(
        leaf(json!({"term": {"labels.priority": "2"}})),
        term("labels.priority", s("2"))
    );
    assert_eq!(
        leaf(json!({"range": {"labels.priority": {"gt": 1.5, "lte": 10}}})),
        Query::Range {
            field: "labels.priority".to_string(),
            gt: Some(FieldValue::F64(1.5)),
            gte: None,
            lt: None,
            lte: Some(FieldValue::I64(10)),
        }
    );
    let view = view();
    assert_eq!(
        coerce(&view, "labels.x", &json!(true), NOW_MS).expect("coerce"),
        Some(FieldValue::Bool(true))
    );
    assert_eq!(
        coerce(&view, "nope", &json!(1), NOW_MS).expect("coerce"),
        None
    );
    assert_eq!(
        coerce(&view, "session_id", &json!(1.5), NOW_MS).expect("coerce"),
        Some(s("1.5"))
    );
    assert_eq!(
        coerce(&view, "flag", &json!("false"), NOW_MS).expect("coerce"),
        Some(FieldValue::Bool(false))
    );
    assert_eq!(
        coerce(
            &view,
            "created_at",
            &json!("2026-09-24T10:11:12.345Z"),
            NOW_MS
        )
        .expect("coerce"),
        Some(FieldValue::Date(NOW_MS * 1000))
    );
}

#[test]
fn bool_accepts_single_clauses_and_boost() {
    assert_eq!(
        leaf(json!({"bool": {"must": {"match_all": {}}, "boost": 2}})),
        boost(bool_q(vec![Query::MatchAll], vec![], vec![], vec![]), 2.0)
    );
    assert_eq!(
        leaf(json!({"bool": {}})),
        bool_q(vec![], vec![], vec![], vec![])
    );
}

fn knn_spec(field: &str, k: usize, num_candidates: usize) -> KnnSpec {
    KnnSpec {
        field: field.to_string(),
        query_vector: vec![1.0, 0.0, 0.0],
        k,
        num_candidates,
        filter: None,
        similarity: None,
        boost: 1.0,
    }
}

#[test]
fn knn_defaults_and_bounds() {
    let view = view();
    let knn = |v: Value| parse_knn(&v, &ctx(&view), 10);
    assert_eq!(
        knn(json!({"field": "vector", "query_vector": [1, 0, 0]})).expect("knn"),
        vec![knn_spec("vector", 10, 15)]
    );
    assert_eq!(
        knn(
            json!([{"field": "vector", "query_vector": [1, 0, 0], "k": 4, "num_candidates": 50,
            "filter": [], "similarity": 0.5, "boost": 2}])
        )
        .expect("knn"),
        vec![KnnSpec {
            similarity: Some(0.5),
            boost: 2.0,
            ..knn_spec("vector", 4, 50)
        }]
    );
    let err = |v: Value| knn(v).expect_err("refused");
    assert_error(
        &err(json!({"field": "vector", "query_vector": [1, 0, 0], "k": 60, "num_candidates": 50})),
        400,
        "illegal_argument_exception",
        "[num_candidates] cannot be less than [k]",
    );
    assert_error(
        &err(
            json!({"field": "vector", "query_vector": [1, 0, 0], "k": 1, "num_candidates": 10001}),
        ),
        400,
        "illegal_argument_exception",
        "[num_candidates] cannot exceed [10000]",
    );
    assert_error(
        &err(json!({"field": "vector", "query_vector": [1, 0, 0], "k": 0})),
        400,
        "illegal_argument_exception",
        "[k] must be greater than 0",
    );
    assert_error(
        &err(json!({"field": "vector", "query_vector": [1, 0]})),
        400,
        "illegal_argument_exception",
        "the query vector has a different number of dimensions [2] than the document vectors [3]",
    );
    assert_error(
        &err(json!({"field": "vector", "query_vector": [0, 0, 0]})),
        400,
        "illegal_argument_exception",
        "The [cosine] similarity does not support vectors with zero magnitude.",
    );
    assert_error(
        &err(json!({"field": "dot", "query_vector": [1, 1, 0]})),
        400,
        "query_shard_exception",
        "failed to create query: The [dot_product] similarity can only be used with unit-length \
         vectors. Preview of invalid vector: [1.0, 1.0, 0.0]",
    );
    assert_error(
        &err(json!({"field": "v2", "query_vector": [1, 0, 0]})),
        400,
        "illegal_argument_exception",
        "to perform knn search on field [v2], its mapping must have [index] set to [true]",
    );
    assert_error(
        &err(json!({"field": "nope", "query_vector": [1, 0, 0]})),
        400,
        "illegal_argument_exception",
        "field [nope] does not exist in the mapping",
    );
    assert_error(
        &err(json!({"field": "vector", "query_vector_builder": {"text_embedding": {}}})),
        400,
        "illegal_argument_exception",
        "Loams does not support [query_vector_builder] (no scripting or ML inference)",
    );
}

#[test]
fn knn_is_allowed_only_at_the_top() {
    let knn = json!({"knn": {"field": "vector", "query_vector": [1, 0, 0], "k": 3}});
    let parsed = top(json!({"bool": {
        "should": [{"match": {"text": "foo"}}, knn],
        "filter": [{"term": {"session_id": "s"}}]
    }}));
    assert_eq!(
        parsed.knn,
        vec![KnnSpec {
            filter: Some(term("session_id", s("s"))),
            ..knn_spec("vector", 3, 5)
        }]
    );
    assert_eq!(
        parsed.query,
        Some(bool_q(
            vec![],
            vec![Query::Match {
                field: "text".to_string(),
                text: "foo".to_string(),
                operator: BoolOperator::Or,
                minimum_should_match: None,
                fuzziness: None,
                analyzer: None,
            }],
            vec![],
            vec![term("session_id", s("s"))]
        ))
    );
    // The whole query.
    let parsed = top(knn.clone());
    assert_eq!(parsed.query, None);
    assert_eq!(parsed.knn, vec![knn_spec("vector", 3, 5)]);
    // Only knn in `should`: nothing else to score.
    let parsed = top(json!({"bool": {"should": [knn]}}));
    assert_eq!(parsed.query, None);
    assert_eq!(parsed.knn.len(), 1);
    let view = view();
    for q in [
        json!({"bool": {"must": [knn]}}),
        json!({"bool": {"filter": [knn]}}),
        json!({"constant_score": {"filter": knn}}),
        json!({"bool": {"should": [{"bool": {"should": [knn]}}]}}),
    ] {
        assert_error(
            &parse_query(&q, &ctx(&view)).expect_err("refused"),
            400,
            "illegal_argument_exception",
            "[knn] queries are only supported as the top-level query or a top-level [bool] \
             should clause (Loams Phase A)",
        );
    }
}

/// C18, C20 and C21 exactly as elasticsearch-py 8.19's strategies build them.
const COSINE: &str = "cosineSimilarity(params.query_vector, 'vector') + 1.0";
const L2: &str = "1 / (1 + l2norm(params.query_vector, 'vector'))";
const SIGMOID: &str = "\n            double value = dotProduct(params.query_vector, 'vector');\n            return sigmoid(1, Math.E, -value);\n            ";
const MIP: &str = "\n            double value = dotProduct(params.query_vector, 'vector');\n            if (dotProduct < 0) {\n                return 1 / (1 + -1 * dotProduct);\n            }\n            return dotProduct + 1;\n            ";

#[test]
fn script_sources_are_recognised_modulo_whitespace() {
    for (source, function) in [
        (COSINE, ScriptFunction::CosinePlusOne),
        (
            "cosineSimilarity(params.query_vector, doc['vector']) + 1.0",
            ScriptFunction::CosinePlusOne,
        ),
        (L2, ScriptFunction::InverseOnePlusL2),
        (SIGMOID, ScriptFunction::SigmoidDot),
    ] {
        assert_eq!(
            recognise_script(source).expect("recognised"),
            (function, "vector".to_string())
        );
        let doubled = source.replace(' ', "  ");
        assert_eq!(
            recognise_script(&doubled).expect("recognised"),
            (function, "vector".to_string())
        );
    }
    // The whole script_score query.
    let parsed = top(json!({"script_score": {
        "query": {"bool": {"filter": [{"term": {"metadata.page": 0}}]}},
        "script": {"source": COSINE, "params": {"query_vector": [1, 0, 0]}},
        "min_score": 1.5,
        "boost": 2
    }}));
    let script = parsed.script.expect("script");
    assert_eq!(parsed.query, None);
    assert_eq!(script.function, ScriptFunction::CosinePlusOne);
    assert_eq!(script.field, "vector");
    assert_eq!(script.query_vector, vec![1.0, 0.0, 0.0]);
    assert_eq!(
        script.filter,
        bool_q(
            vec![],
            vec![],
            vec![],
            vec![term("metadata.page", FieldValue::I64(0))]
        )
    );
    assert_eq!((script.min_score, script.boost), (Some(1.5), 2.0));
    // An unindexed vector may be scored exactly (C6).
    let parsed = top(
        json!({"script_score": {"query": {"match_all": {}}, "script": {
        "source": "cosineSimilarity(params.query_vector, 'v2') + 1.0",
        "params": {"query_vector": [1, 0, 0]}}}}),
    );
    assert_eq!(parsed.script.expect("script").filter, Query::MatchAll);
}

#[test]
fn the_broken_mip_script_is_a_compile_error() {
    let error = recognise_script(MIP).expect_err("refused");
    assert_error(&error, 400, "script_exception", "compile error");
    let body = error.to_body();
    assert_eq!(
        body["error"]["caused_by"]["reason"],
        "cannot resolve symbol [dotProduct]"
    );
    assert_eq!(
        body["error"]["caused_by"]["type"],
        "illegal_argument_exception"
    );
    assert_eq!(body["error"]["lang"], "painless");
    assert!(body["error"]["script_stack"].is_array());
}

#[test]
fn an_unknown_script_is_refused() {
    let arbitrary = "Loams does not support arbitrary scripts";
    assert_error(
        &recognise_script("doc['x'].value * 2").expect_err("refused"),
        400,
        "illegal_argument_exception",
        arbitrary,
    );
    let view = view();
    for script in [
        json!({"source": COSINE, "params": {"query_vector": [1, 0, 0], "other": 1}}),
        json!({"source": COSINE}),
        json!({"id": "stored"}),
    ] {
        let q = json!({"script_score": {"query": {"match_all": {}}, "script": script}});
        assert_error(
            &parse_query(&q, &ctx(&view)).expect_err("refused"),
            400,
            "illegal_argument_exception",
            arbitrary,
        );
    }
    let q = json!({"script_score": {"query": {"match_all": {}}, "script": {
        "source": COSINE, "lang": "expression", "params": {"query_vector": [1, 0, 0]}}}});
    assert_error(
        &parse_query(&q, &ctx(&view)).expect_err("refused"),
        400,
        "illegal_argument_exception",
        "script_lang not supported [expression]",
    );
}

#[test]
fn date_math_rounds_per_bound() {
    let at = |expr: &str, round| parse_date_math(expr, NOW_MS, round).expect(expr);
    assert_eq!(at("now", Rounding::Down), NOW_MS * 1000);
    assert_eq!(at("now/d", Rounding::Down), us("2026-09-24T00:00:00Z"));
    assert_eq!(at("now/d", Rounding::Up), us("2026-09-24T23:59:59.999Z"));
    assert_eq!(at("now-1M/M", Rounding::Down), us("2026-08-01T00:00:00Z"));
    assert_eq!(at("now-1M/M", Rounding::Up), us("2026-08-31T23:59:59.999Z"));
    assert_eq!(
        at("2026-01-31||+1M", Rounding::Down),
        us("2026-02-28T00:00:00Z")
    );
    assert_eq!(at("now/w", Rounding::Down), us("2026-09-21T00:00:00Z"));
    assert_eq!(
        at("now+1h-30m/m", Rounding::Down),
        us("2026-09-24T10:41:00Z")
    );
    assert_eq!(at("now-1y/y", Rounding::Down), us("2025-01-01T00:00:00Z"));
    // A bare date rounds up its time of day only: a missing month or day
    // stays 1, as ES's round-up parser fills it (row T11-3).
    assert_eq!(at("2026-09", Rounding::Up), us("2026-09-01T23:59:59.999Z"));
    assert_eq!(at("2026", Rounding::Up), us("2026-01-01T23:59:59.999Z"));
    assert_eq!(
        at("2026-09-24", Rounding::Up),
        us("2026-09-24T23:59:59.999Z")
    );
    assert_eq!(
        at("2026-09-24T10:11", Rounding::Up),
        us("2026-09-24T10:11:59.999Z")
    );
    assert_eq!(
        at("2026-09-24T10:11:12.5Z", Rounding::Up),
        us("2026-09-24T10:11:12.5Z")
    );
    assert_eq!(
        at("2026-09-24T12:00:00+02:00", Rounding::Down),
        us("2026-09-24T10:00:00Z")
    );
    assert_eq!(at("1727172672000", Rounding::Down), 1_727_172_672_000_000);
    assert_error(
        &parse_date_math("yesterday", NOW_MS, Rounding::Down).expect_err("refused"),
        400,
        "parse_exception",
        "failed to parse date field [yesterday] with format [strict_date_optional_time||epoch_millis]",
    );
    assert_error(
        &parse_date_math("now+1x", NOW_MS, Rounding::Down).expect_err("refused"),
        400,
        "parse_exception",
        "unit [x] not supported for date math [+1x]",
    );
    // Amounts that overflow are parse errors, not panics (PR #74 review).
    for expr in [
        "now+9223372036854775807m",
        "now-99999999999999999d",
        "now+9223372036854775807w",
        "now+9223372036854775807h",
        "now+9223372036854775807M",
        "now-9223372036854775807y",
    ] {
        let err = parse_date_math(expr, NOW_MS, Rounding::Down).expect_err(expr);
        assert_eq!(
            (err.status, err.kind.as_str()),
            (400, "parse_exception"),
            "{expr}"
        );
    }
}

#[test]
fn phase_b_queries_are_refused_with_one_reason() {
    for q in [
        json!({"nested": {"path": "p", "query": {"match_all": {}}}}),
        json!({"function_score": {"query": {"match_all": {}}}}),
        json!({"regexp": {"session_id": "a.*"}}),
        json!({"script": {"script": "true"}}),
        json!({"sparse_vector": {"field": "f"}}),
    ] {
        let error = leaf_err(q.clone());
        assert_error(
            &error,
            400,
            "illegal_argument_exception",
            "Loams does not support",
        );
    }
}
