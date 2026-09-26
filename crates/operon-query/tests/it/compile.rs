//! The query compiler (plan M1.2 Task 2): every IR variant against one split
//! of 12 fixed documents, the JSON path rules (M1.4 S4, S5, S11), exact date
//! and decimal bounds, ES's scoring conventions (M1.5) and warmup-free
//! validation errors.

use std::collections::BTreeSet;
use std::sync::Arc;

use operon_collection::{
    CollectionSchema, Document, DynamicMapping, FieldKind, FieldSpec, PrimaryKey, check_document,
    tantivy_layout, to_tantivy_doc,
};
use operon_query::text::{
    CompileMode, QueryCompiler, fuzziness_edits, highlight_terms, parse_minimum_should_match,
    query_tokenizers,
};
use operon_query::{Fuzziness, Query, ServiceError};
use operon_quickwit::query::tokenizers::TokenizerManager;
use operon_quickwit::storage::RamStorage;
use operon_text::{build_split, open_split, warm_up_all};
use serde_json::{Value, json};
use tantivy::collector::{DocSetCollector, TopDocs};
use tantivy::schema::Schema;
use tantivy::{Searcher, TantivyDocument};

const U0: &str = "0a0a0a0a-0000-4000-8000-000000000000";
const U1: &str = "1b1b1b1b-0000-4000-8000-000000000001";
const U3: &str = "3d3d3d3d-0000-4000-8000-000000000003";
const U8: &str = "8e8e8e8e-0000-4000-8000-000000000008";
const U10: &str = "afafafaf-0000-4000-8000-00000000000a";

fn field(name: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.to_string(),
        source_path: format!("f.{name}"),
        fast: !matches!(kind, FieldKind::Text { .. }),
        kind,
        indexed: true,
        ignore_malformed: false,
    }
}

fn text(name: &str, positions: bool) -> FieldSpec {
    field(
        name,
        FieldKind::Text {
            analyzer: "english".to_string(),
            positions,
        },
    )
}

fn payload() -> FieldSpec {
    FieldSpec {
        name: "payload".to_string(),
        source_path: String::new(),
        kind: FieldKind::Json,
        indexed: true,
        fast: true,
        ignore_malformed: false,
    }
}

/// `title` Text english, `tag` Keyword fast, `n` I64 fast, `x` F64 fast,
/// `b` Bool fast, `d` Date fast, `u` Uuid, and `payload` Json over the
/// whole source. Plain fields read `f.<name>`, so the JSON path tests own
/// the top-level keys.
fn schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            text("title", true),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field("x", FieldKind::F64),
            field("b", FieldKind::Bool),
            field("d", FieldKind::Date),
            field("u", FieldKind::Uuid),
            payload(),
        ],
        vec![],
        DynamicMapping::Ignore,
    );
    schema.validate().expect("valid schema");
    schema
}

/// The 12 documents: pk i is doc id i.
fn sources() -> Vec<Value> {
    vec![
        json!({"f": {"title": "The quick brown fox", "tag": "alpha", "n": 1, "x": 1.5, "b": true, "d": 1000, "u": U0},
               "n": 1, "k": [1, 2]}),
        json!({"f": {"title": "Quick brown dogs", "tag": "beta", "n": 2, "x": 2.0, "b": false, "d": 2000, "u": U1},
               "n": "1", "k": null}),
        json!({"f": {"title": "lazy dogs sleeping", "tag": "gamma", "n": 3, "x": -0.5, "b": true, "d": 1500},
               "n": 1.0, "k": []}),
        json!({"f": {"title": "the fox jumps over the dog", "tag": "alpha", "n": 10, "x": 10.25, "b": false, "d": 86_400_000, "u": U3},
               "n": 1.5, "k": [1, null]}),
        json!({"f": {"title": "alpha beta", "tag": "alpha", "n": -7, "x": 0.0},
               "tags": ["a", "b"], "body": "the quick brown fox"}),
        json!({"f": {"title": "running runners run", "tag": "delta", "n": 100, "x": 99.9, "b": true, "d": 1_700_000_000_000_i64},
               "o": {"k": [1, 2]}, "when": "2024-01-02"}),
        json!({"f": {"title": "foxes and dogs"}, "when": "yesterday"}),
        json!({"f": {"title": "Brown sugar", "tag": "Alpha", "n": 0, "x": 3.0, "b": false, "d": 999}}),
        json!({"f": {"tag": "beta", "n": 5, "x": 5.5, "b": true, "d": 1_000_000, "u": U8}, "status": "open"}),
        json!({"f": {"title": "quick quick quick", "tag": "a.b*c", "n": 42, "x": 42.0, "b": false, "d": 1001},
               "body": "lazy cat"}),
        json!({"f": {"title": "tempo", "tag": "7", "n": 7, "x": 7.0, "b": true, "d": 3000, "u": U10}, "tags": ["c"]}),
        json!({"f": {"tag": "zeta", "n": i64::MAX, "x": 1e10, "b": false, "d": 0},
               "big": 18_446_744_073_709_551_615_u64}),
    ]
}

fn document(pk: u64, source: Value) -> Document {
    let Value::Object(source) = source else {
        panic!("not an object");
    };
    Document {
        pk: PrimaryKey::U64(pk),
        source,
        vectors: Default::default(),
        sparse_vectors: Default::default(),
    }
}

/// A warm split of `sources` under `schema` (row id = pk = doc id).
async fn split(schema: &CollectionSchema, sources: Vec<Value>) -> Searcher {
    let layout = tantivy_layout(schema);
    let docs: Vec<TantivyDocument> = sources
        .into_iter()
        .enumerate()
        .map(|(i, source)| {
            let doc = document(i as u64, source);
            let extracted = check_document(schema, &doc).expect("valid document");
            to_tantivy_doc(&layout, schema, &doc, &extracted, i as u64)
        })
        .collect();
    let built = build_split(layout.schema, docs).expect("split builds");
    let storage = RamStorage::builder().put("s.split", &built.bytes).build();
    let index = open_split(
        Arc::new(storage),
        "s.split",
        built.bytes.len() as u64,
        built.footer_range,
    )
    .await
    .expect("split opens");
    warm_up_all(&index).await.expect("split warms")
}

struct Fixture {
    schema: CollectionSchema,
    searcher: Searcher,
    tokenizers: TokenizerManager,
}

impl Fixture {
    async fn new() -> Self {
        let schema = schema();
        let searcher = split(&schema, sources()).await;
        Self {
            schema,
            searcher,
            tokenizers: query_tokenizers(),
        }
    }

    fn split_schema(&self) -> Schema {
        self.searcher.schema().clone()
    }

    /// The pks `query` matches, in both modes (which must agree).
    fn hits(&self, query: &Query) -> Result<BTreeSet<u64>, ServiceError> {
        hits_in(&self.schema, &self.searcher, &self.tokenizers, query)
    }

    /// (pk, score) of every match, scored.
    fn scores(&self, query: &Query) -> Vec<(u64, f32)> {
        let split = self.split_schema();
        let compiler = QueryCompiler::new(&self.schema, &split, &self.tokenizers);
        let compiled = compiler
            .compile(query, CompileMode::Scoring)
            .expect("compiles");
        let top = self
            .searcher
            .search(&*compiled.query, &TopDocs::with_limit(100).order_by_score())
            .expect("search");
        let mut out: Vec<(u64, f32)> = top
            .into_iter()
            .map(|(score, address)| (u64::from(address.doc_id), score))
            .collect();
        out.sort_by_key(|(pk, _)| *pk);
        out
    }
}

fn hits_in(
    schema: &CollectionSchema,
    searcher: &Searcher,
    tokenizers: &TokenizerManager,
    query: &Query,
) -> Result<BTreeSet<u64>, ServiceError> {
    let split = searcher.schema().clone();
    let compiler = QueryCompiler::new(schema, &split, tokenizers);
    let mut sets = Vec::new();
    for mode in [CompileMode::Scoring, CompileMode::Filter] {
        let compiled = compiler.compile(query, mode)?;
        assert!(compiled.warmup.required_terms.is_empty());
        let docs = searcher
            .search(&*compiled.query, &DocSetCollector)
            .unwrap_or_else(|err| panic!("{query:?} searches: {err}"));
        sets.push(
            docs.into_iter()
                .map(|address| u64::from(address.doc_id))
                .collect::<BTreeSet<u64>>(),
        );
    }
    assert_eq!(
        sets[0], sets[1],
        "{query:?}: both modes match the same docs"
    );
    Ok(sets.remove(0))
}

fn q(value: Value) -> Query {
    serde_json::from_value(value.clone()).unwrap_or_else(|err| panic!("{value}: {err}"))
}

fn set(pks: &[u64]) -> BTreeSet<u64> {
    pks.iter().copied().collect()
}

const ALL: [u64; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

#[tokio::test]
async fn every_variant_matches_the_expected_docs() {
    let fx = Fixture::new().await;
    let upper_u0 = U0.to_uppercase();
    let rows: Vec<(Value, &[u64])> = vec![
        // match_all, match_none
        (json!("match_all"), &ALL),
        (json!("match_none"), &[]),
        // match
        (
            json!({"match": {"field": "title", "text": "quick"}}),
            &[0, 1, 9],
        ),
        (
            json!({"match": {"field": "title", "text": "Quick Fox"}}),
            &[0, 1, 3, 6, 9],
        ),
        (
            json!({"match": {"field": "title", "text": "quick fox", "operator": "and"}}),
            &[0],
        ),
        (
            json!({"match": {"field": "title", "text": "quick fox dogs", "minimum_should_match": "2"}}),
            &[0, 1, 3, 6],
        ),
        (
            json!({"match": {"field": "title", "text": "quikc", "fuzziness": "auto"}}),
            &[0, 1, 9],
        ),
        (json!({"match": {"field": "title", "text": "the"}}), &[]),
        (
            json!({"match": {"field": "tag", "text": "alpha"}}),
            &[0, 3, 4],
        ),
        (json!({"match": {"field": "n", "text": "10"}}), &[3]),
        (json!({"match": {"field": "u", "text": upper_u0}}), &[0]),
        (
            json!({"match": {"field": "payload.body", "text": "Quick Fox"}}),
            &[4],
        ),
        // match_phrase
        (
            json!({"match_phrase": {"field": "title", "text": "quick brown"}}),
            &[0, 1],
        ),
        (
            json!({"match_phrase": {"field": "title", "text": "brown quick"}}),
            &[],
        ),
        (
            json!({"match_phrase": {"field": "title", "text": "fox jumps"}}),
            &[3],
        ),
        (
            json!({"match_phrase": {"field": "title", "text": "quick fox", "slop": 1}}),
            &[0],
        ),
        (
            json!({"match_phrase": {"field": "payload.body", "text": "quick brown"}}),
            &[4],
        ),
        // multi_match
        (
            json!({"multi_match": {"fields": [["title", 1.0], ["tag", 1.0]], "text": "alpha"}}),
            &[0, 3, 4],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0], ["tag", 2.0]], "text": "alpha", "kind": "most_fields"}}),
            &[0, 3, 4],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0], ["payload.body", 1.0]], "text": "quick fox", "kind": "cross_fields", "operator": "and"}}),
            &[0, 4],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0], ["tag", 1.0]], "text": "alpha beta", "kind": "cross_fields"}}),
            &[4],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0]], "text": "quick brown", "kind": "phrase"}}),
            &[0, 1],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0]], "text": "quick bro", "kind": "phrase_prefix"}}),
            &[0, 1],
        ),
        (
            json!({"multi_match": {"fields": [["title", 1.0]], "text": "sle", "kind": "phrase_prefix"}}),
            &[2],
        ),
        // term: text and keyword (coercion row 1)
        (
            json!({"term": {"field": "tag", "value": "alpha"}}),
            &[0, 3, 4],
        ),
        (json!({"term": {"field": "tag", "value": 7}}), &[10]),
        (json!({"term": {"field": "tag", "value": 7.0}}), &[10]),
        (json!({"term": {"field": "tag", "value": true}}), &[]),
        (
            json!({"term": {"field": "tag", "value": {"date": "1970-01-01T00:00:01Z"}}}),
            &[],
        ),
        (
            json!({"term": {"field": "title", "value": "fox"}}),
            &[0, 3, 6],
        ),
        (json!({"term": {"field": "title", "value": "Fox"}}), &[]),
        // term: uuid (row 2)
        (json!({"term": {"field": "u", "value": U1}}), &[1]),
        (json!({"term": {"field": "u", "value": "not-a-uuid"}}), &[]),
        (json!({"term": {"field": "u", "value": 5}}), &[]),
        // term: long (row 3)
        (json!({"term": {"field": "n", "value": 10}}), &[3]),
        (json!({"term": {"field": "n", "value": "10"}}), &[3]),
        (json!({"term": {"field": "n", "value": "10.0"}}), &[3]),
        (json!({"term": {"field": "n", "value": 1.5}}), &[]),
        (
            json!({"term": {"field": "n", "value": 9_223_372_036_854_775_808_u64}}),
            &[],
        ),
        (
            json!({"term": {"field": "n", "value": 9_223_372_036_854_775_807_u64}}),
            &[11],
        ),
        // term: double (row 4)
        (json!({"term": {"field": "x", "value": 2}}), &[1]),
        (json!({"term": {"field": "x", "value": "10.25"}}), &[3]),
        (json!({"term": {"field": "x", "value": 7.0}}), &[10]),
        // term: boolean (row 5)
        (
            json!({"term": {"field": "b", "value": true}}),
            &[0, 2, 5, 8, 10],
        ),
        (
            json!({"term": {"field": "b", "value": "false"}}),
            &[1, 3, 7, 9, 11],
        ),
        // term: date (row 6)
        (
            json!({"term": {"field": "d", "value": {"date": "1970-01-01T00:00:01Z"}}}),
            &[0],
        ),
        (json!({"term": {"field": "d", "value": 2000}}), &[1]),
        (
            json!({"term": {"field": "d", "value": "1970-01-01T00:00:01.500Z"}}),
            &[2],
        ),
        (
            json!({"term": {"field": "d", "value": {"date": "1970-01-01T00:00:01.000500Z"}}}),
            &[],
        ),
        (
            json!({"term": {"field": "d", "value": 18_446_744_073_709_551_615_u64}}),
            &[],
        ),
        // term: _id and JSON paths
        (json!({"term": {"field": "_id", "value": 3}}), &[3]),
        (json!({"term": {"field": "payload.n", "value": 1}}), &[0, 2]),
        (json!({"term": {"field": "payload.n", "value": "1"}}), &[1]),
        (
            json!({"term": {"field": "payload.n", "value": 1.0}}),
            &[0, 2],
        ),
        (json!({"term": {"field": "payload.n", "value": 1.5}}), &[3]),
        (
            json!({"term": {"field": "payload.tags", "value": "a"}}),
            &[4],
        ),
        (json!({"term": {"field": "payload.o.k", "value": 2}}), &[5]),
        (
            json!({"term": {"field": "payload.big", "value": 18_446_744_073_709_551_615_u64}}),
            &[11],
        ),
        (
            json!({"term": {"field": "payload.when", "value": {"date": "2024-01-02T00:00:00Z"}}}),
            &[5],
        ),
        // terms
        (
            json!({"terms": {"field": "tag", "values": ["alpha", "beta"]}}),
            &[0, 1, 3, 4, 8],
        ),
        (
            json!({"terms": {"field": "n", "values": [1, 2, 1.5, "3"]}}),
            &[0, 1, 2],
        ),
        (
            json!({"terms": {"field": "d", "values": [{"date": "1970-01-01T00:00:01Z"}, 2000]}}),
            &[0, 1],
        ),
        (
            json!({"terms": {"field": "_id", "values": [1, 2]}}),
            &[1, 2],
        ),
        (
            json!({"terms": {"field": "payload.n", "values": [1, "1"]}}),
            &[0, 1, 2],
        ),
        // range
        (
            json!({"range": {"field": "n", "gte": 2, "lt": 10}}),
            &[1, 2, 8, 10],
        ),
        (
            json!({"range": {"field": "n", "gt": 1.5, "lte": 3}}),
            &[1, 2],
        ),
        (
            json!({"range": {"field": "n", "gte": "5"}}),
            &[3, 5, 8, 9, 10, 11],
        ),
        (
            json!({"range": {"field": "n", "gt": 9_223_372_036_854_775_808_u64}}),
            &[],
        ),
        (
            json!({"range": {"field": "n", "lte": 18_446_744_073_709_551_615_u64}}),
            &[0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11],
        ),
        (
            json!({"range": {"field": "x", "gt": 0, "lt": 10}}),
            &[0, 1, 7, 8, 10],
        ),
        (
            json!({"range": {"field": "d", "gte": {"date": "1970-01-01T00:00:01Z"}, "lt": 2000}}),
            &[0, 2, 9],
        ),
        (
            json!({"range": {"field": "d", "lte": "1970-01-01T00:00:01Z"}}),
            &[0, 7, 11],
        ),
        (
            json!({"range": {"field": "tag", "gte": "b", "lt": "d"}}),
            &[1, 8],
        ),
        (
            json!({"range": {"field": "title", "gte": "s"}}),
            &[2, 7, 10],
        ),
        (
            json!({"range": {"field": "payload.n", "gte": 0.5}}),
            &[0, 2, 3],
        ),
        (
            json!({"range": {"field": "payload.when", "gte": {"date": "2024-01-01T00:00:00Z"}}}),
            &[5],
        ),
        (
            json!({"range": {"field": "payload.tags", "gte": "b"}}),
            &[4, 10],
        ),
        // exists, is_null, is_empty, values_count
        (
            json!({"exists": {"field": "title"}}),
            &[0, 1, 2, 3, 4, 5, 6, 7, 9, 10],
        ),
        (
            json!({"exists": {"field": "tag"}}),
            &[0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11],
        ),
        (json!({"exists": {"field": "payload.k"}}), &[0, 3]),
        (json!({"exists": {"field": "payload"}}), &ALL),
        (json!({"exists": {"field": "nosuch"}}), &[]),
        (json!({"is_null": {"field": "payload.k"}}), &[1, 3]),
        (
            json!({"is_empty": {"field": "payload.k"}}),
            &[1, 2, 4, 5, 6, 7, 8, 9, 10, 11],
        ),
        (json!({"is_empty": {"field": "tag"}}), &[6]),
        (
            json!({"values_count": {"field": "payload.k", "gte": 2}}),
            &[0],
        ),
        (
            json!({"values_count": {"field": "payload.k", "lte": 0}}),
            &[1, 2, 4, 5, 6, 7, 8, 9, 10, 11],
        ),
        (
            json!({"values_count": {"field": "payload.k", "gte": 1, "lte": 1}}),
            &[3],
        ),
        // prefix, wildcard, fuzzy
        (
            json!({"prefix": {"field": "title", "value": "Qui"}}),
            &[0, 1, 9],
        ),
        (
            json!({"prefix": {"field": "tag", "value": "alpha"}}),
            &[0, 3, 4],
        ),
        (json!({"prefix": {"field": "tag", "value": "a.b"}}), &[9]),
        (json!({"prefix": {"field": "tag", "value": "a.b*"}}), &[9]),
        (
            json!({"prefix": {"field": "tag", "value": "x#&~-\"@<>"}}),
            &[],
        ),
        (
            json!({"prefix": {"field": "payload.status", "value": "op"}}),
            &[8],
        ),
        (
            json!({"wildcard": {"field": "tag", "pattern": "a*a"}}),
            &[0, 3, 4],
        ),
        (
            json!({"wildcard": {"field": "tag", "pattern": "?eta"}}),
            &[1, 8, 11],
        ),
        (
            json!({"wildcard": {"field": "title", "pattern": "BRO*"}}),
            &[0, 1, 7],
        ),
        (
            json!({"wildcard": {"field": "payload.tags", "pattern": "?"}}),
            &[4, 10],
        ),
        (
            json!({"fuzzy": {"field": "title", "value": "fux", "fuzziness": "auto"}}),
            &[0, 3, 6],
        ),
        (
            json!({"fuzzy": {"field": "tag", "value": "alphx", "fuzziness": 1}}),
            &[0, 3, 4],
        ),
        (
            json!({"fuzzy": {"field": "tag", "value": "gamma", "fuzziness": 0}}),
            &[2],
        ),
        // ids, query_string
        (json!({"ids": [0, 5]}), &[0, 5]),
        (json!({"ids": ["nope"]}), &[]),
        (
            json!({"query_string": {"query": "quick fox", "default_fields": ["title"]}}),
            &[0, 1, 3, 6, 9],
        ),
        (
            json!({"query_string": {"query": "quick fox", "default_fields": ["title"], "default_operator": "and"}}),
            &[0],
        ),
        (
            json!({"query_string": {"query": "tag:beta", "default_fields": ["title"]}}),
            &[1, 8],
        ),
        (
            json!({"query_string": {"query": "quick", "default_fields": ["nosuch"]}}),
            &[],
        ),
        // bool, boost, constant_score
        (
            json!({"bool": {"must": [{"term": {"field": "tag", "value": "alpha"}}], "must_not": [{"term": {"field": "n", "value": 10}}]}}),
            &[0, 4],
        ),
        (
            json!({"bool": {"should": [{"term": {"field": "tag", "value": "alpha"}}, {"term": {"field": "tag", "value": "beta"}}]}}),
            &[0, 1, 3, 4, 8],
        ),
        (
            json!({"bool": {"should": [{"term": {"field": "tag", "value": "alpha"}}, {"term": {"field": "b", "value": true}}, {"term": {"field": "n", "value": 1}}], "minimum_should_match": "2"}}),
            &[0],
        ),
        (
            json!({"bool": {"must_not": [{"exists": {"field": "tag"}}]}}),
            &[6],
        ),
        (
            json!({"bool": {"filter": [{"range": {"field": "n", "gte": 5}}]}}),
            &[3, 5, 8, 9, 10, 11],
        ),
        (json!({"bool": {}}), &ALL),
        (
            json!({"bool": {"must": [{"match": {"field": "title", "text": "dogs"}}], "should": [{"term": {"field": "b", "value": true}}]}}),
            &[1, 2, 3, 6],
        ),
        (
            json!({"boost": {"query": {"term": {"field": "tag", "value": "alpha"}}, "boost": 2.0}}),
            &[0, 3, 4],
        ),
        (
            json!({"constant_score": {"query": {"match": {"field": "title", "text": "fox"}}, "score": 5.0}}),
            &[0, 3, 6],
        ),
    ];
    assert!(rows.len() >= 60, "{} rows", rows.len());
    let mut failures = Vec::new();
    for (query, expected) in &rows {
        match fx.hits(&q(query.clone())) {
            Ok(hits) if hits == set(expected) => {}
            Ok(hits) => failures.push(format!("{query}: got {hits:?}, expected {expected:?}")),
            Err(err) => failures.push(format!("{query}: {err}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // The error cells of the coercion table, and the other invalid queries.
    let errors = [
        json!({"term": {"field": "payload", "value": "x"}}),
        json!({"term": {"field": "n", "value": "abc"}}),
        json!({"term": {"field": "n", "value": true}}),
        json!({"term": {"field": "n", "value": {"date": "1970-01-01T00:00:01Z"}}}),
        json!({"term": {"field": "x", "value": "abc"}}),
        json!({"term": {"field": "x", "value": false}}),
        json!({"term": {"field": "b", "value": 1}}),
        json!({"term": {"field": "b", "value": "yes"}}),
        json!({"term": {"field": "d", "value": 1.5}}),
        json!({"term": {"field": "d", "value": "not a date"}}),
        json!({"match": {"field": "title", "text": "a b c", "minimum_should_match": "3<90%"}}),
        json!({"range": {"field": "n", "gt": 1, "gte": 2}}),
        json!({"prefix": {"field": "n", "value": "1"}}),
        json!({"values_count": {"field": "title", "gte": 1}}),
        json!({"query_string": {"query": "title:(", "default_fields": ["title"]}}),
        json!({"match": {"field": "title", "text": "a", "analyzer": "nosuch"}}),
    ];
    for query in errors {
        match fx.hits(&q(query.clone())) {
            Err(ServiceError::InvalidArgument(_)) => {}
            other => panic!("{query}: expected InvalidArgument, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn json_paths_are_type_strict() {
    let fx = Fixture::new().await;
    // payload.n: 0 → 1, 1 → "1", 2 → 1.0, 3 → 1.5. Tantivy indexes the
    // integral float 1.0 as the integer 1, so numbers compare by value; a
    // string never matches a number.
    let n1 = fx
        .hits(&q(json!({"term": {"field": "payload.n", "value": 1}})))
        .unwrap();
    assert_eq!(n1, set(&[0, 2]));
    let s1 = fx
        .hits(&q(json!({"term": {"field": "payload.n", "value": "1"}})))
        .unwrap();
    assert_eq!(s1, set(&[1]));
    let half = fx
        .hits(&q(json!({"range": {"field": "payload.n", "gte": 0.5}})))
        .unwrap();
    assert!(half.contains(&0) && half.contains(&3), "{half:?}");
    assert!(!half.contains(&1), "a string never matches a numeric range");
    let tags = fx
        .hits(&q(json!({"term": {"field": "payload.tags", "value": "a"}})))
        .unwrap();
    assert_eq!(tags, set(&[4]));
    let nested = fx
        .hits(&q(json!({"term": {"field": "payload.o.k", "value": 2}})))
        .unwrap();
    assert_eq!(nested, set(&[5]));
}

#[tokio::test]
async fn json_paths_support_text_and_date_conditions() {
    let fx = Fixture::new().await;
    let hits = |query: Value| fx.hits(&q(query)).unwrap();
    assert!(hits(json!({"match": {"field": "payload.body", "text": "Quick Fox"}})).contains(&4));
    assert!(
        hits(json!({"match_phrase": {"field": "payload.body", "text": "quick brown"}}))
            .contains(&4)
    );
    assert!(
        !hits(json!({"match_phrase": {"field": "payload.body", "text": "brown quick"}}))
            .contains(&4)
    );
    let when =
        hits(json!({"range": {"field": "payload.when", "gte": {"date": "2024-01-01T00:00:00Z"}}}));
    assert!(when.contains(&5), "2024-01-02 is after 2024-01-01");
    assert!(!when.contains(&6), "\"yesterday\" is not a date");
}

#[tokio::test]
async fn is_empty_and_is_null_follow_qdrant() {
    let fx = Fixture::new().await;
    let hits = |query: Value| fx.hits(&q(query)).unwrap();
    // payload.k: 0 → [1, 2], 1 → null, 2 → [], 3 → [1, null], 4 → absent.
    let empty = hits(json!({"is_empty": {"field": "payload.k"}}));
    for pk in [4, 1, 2] {
        assert!(empty.contains(&pk), "{pk}: {empty:?}");
    }
    assert!(!empty.contains(&0) && !empty.contains(&3));
    let null = hits(json!({"is_null": {"field": "payload.k"}}));
    assert!(null.contains(&1) && null.contains(&3), "{null:?}");
    assert!(!null.contains(&4), "an absent key is not null");
    assert!(hits(json!({"values_count": {"field": "payload.k", "lte": 0}})).contains(&4));
    assert!(hits(json!({"values_count": {"field": "payload.k", "gte": 2}})).contains(&0));
}

#[tokio::test]
async fn date_bounds_round_to_milliseconds_exactly() {
    let fx = Fixture::new().await;
    // Doc 0 has d = 1_000 ms.
    let date = |us: i64| operon_query::FieldValue::Date(us);
    let range = |gt, gte, lt| Query::Range {
        field: "d".to_string(),
        gt,
        gte,
        lt,
        lte: None,
    };
    let has0 = |query: Query| fx.hits(&query).unwrap().contains(&0);
    assert!(has0(range(Some(date(999_999)), None, None)));
    assert!(!has0(range(Some(date(1_000_000)), None, None)));
    assert!(!has0(range(None, Some(date(1_000_001)), None)));
    assert!(has0(range(None, None, Some(date(1_000_001)))));
    let term = Query::Term {
        field: "d".to_string(),
        value: date(1_000_500),
    };
    assert!(fx.hits(&term).unwrap().is_empty());
}

#[tokio::test]
async fn a_decimal_bound_on_a_long_field_rounds_like_es() {
    let fx = Fixture::new().await;
    // Docs 0 and 1 have n = 1 and n = 2.
    let among = |range: Value| {
        fx.hits(&q(json!({"bool": {"must": [
            {"terms": {"field": "n", "values": [1, 2]}}, range]}})))
            .unwrap()
    };
    assert_eq!(
        among(json!({"range": {"field": "n", "gt": 1.5}})),
        set(&[1])
    );
    assert_eq!(
        among(json!({"range": {"field": "n", "lt": 1.5}})),
        set(&[0])
    );
    assert_eq!(
        among(json!({"range": {"field": "n", "gte": 2.0}})),
        set(&[1])
    );
}

#[test]
fn minimum_should_match_forms() {
    assert_eq!(parse_minimum_should_match("2", 4).unwrap(), 2);
    assert_eq!(parse_minimum_should_match("-1", 4).unwrap(), 3);
    assert_eq!(parse_minimum_should_match("75%", 4).unwrap(), 3);
    assert_eq!(parse_minimum_should_match("-25%", 4).unwrap(), 3);
    assert_eq!(parse_minimum_should_match("9", 4).unwrap(), 4);
    assert_eq!(parse_minimum_should_match("-9", 4).unwrap(), 0);
    assert!(matches!(
        parse_minimum_should_match("3<90%", 4),
        Err(ServiceError::InvalidArgument(_))
    ));
}

#[test]
fn fuzziness_auto_follows_es() {
    assert_eq!(fuzziness_edits(Fuzziness::Auto, "ab"), 0);
    assert_eq!(fuzziness_edits(Fuzziness::Auto, "abc"), 1);
    assert_eq!(fuzziness_edits(Fuzziness::Auto, "abcde"), 1);
    assert_eq!(fuzziness_edits(Fuzziness::Auto, "abcdef"), 2);
    assert_eq!(fuzziness_edits(Fuzziness::Edits(5), "a"), 2);
}

#[tokio::test]
async fn a_pure_must_not_matches_the_rest_with_score_zero() {
    let fx = Fixture::new().await;
    let scores = fx.scores(&q(json!({"bool": {"must_not": [
        {"term": {"field": "tag", "value": "alpha"}}]}})));
    let pks: BTreeSet<u64> = scores.iter().map(|(pk, _)| *pk).collect();
    assert_eq!(pks, set(&[1, 2, 5, 6, 7, 8, 9, 10, 11]));
    assert!(scores.iter().all(|(_, score)| *score == 0.0), "{scores:?}");
}

#[tokio::test]
async fn a_filter_only_bool_scores_zero() {
    let fx = Fixture::new().await;
    let scores = fx.scores(&q(json!({"bool": {"filter": [
        {"match": {"field": "title", "text": "fox"}}]}})));
    assert_eq!(scores.len(), 3);
    assert!(scores.iter().all(|(_, score)| *score == 0.0), "{scores:?}");
}

#[tokio::test]
async fn match_all_scores_one() {
    let fx = Fixture::new().await;
    let scores = fx.scores(&Query::MatchAll);
    assert_eq!(scores.len(), 12);
    assert!(scores.iter().all(|(_, score)| *score == 1.0), "{scores:?}");
    // Constant leaves score 1.0 too, and boosts multiply.
    let boosted = fx.scores(&q(
        json!({"boost": {"query": {"range": {"field": "n", "gte": 42}}, "boost": 3.0}}),
    ));
    assert_eq!(boosted, vec![(5, 3.0), (9, 3.0), (11, 3.0)]);
    let constant = fx.scores(&q(json!({"constant_score": {"query": {"match": {"field": "title", "text": "fox"}}, "score": 5.0}})));
    assert!(
        constant.iter().all(|(_, score)| *score == 5.0),
        "{constant:?}"
    );
}

#[tokio::test]
async fn multi_match_best_fields_uses_the_tie_breaker() {
    let fx = Fixture::new().await;
    // Doc 4 has "alpha" in both its title and its tag.
    let score_of = |query: Value| {
        fx.scores(&q(query))
            .into_iter()
            .find(|(pk, _)| *pk == 4)
            .map(|(_, score)| score)
            .expect("doc 4 matches")
    };
    let title = score_of(json!({"match": {"field": "title", "text": "alpha"}}));
    let tag = score_of(json!({"match": {"field": "tag", "text": "alpha"}}));
    let (s1, s2) = if title > tag {
        (title, tag)
    } else {
        (tag, title)
    };
    assert!(s1 > s2, "distinct single-field scores: {s1} {s2}");
    let fused = score_of(
        json!({"multi_match": {"fields": [["title", 1.0], ["tag", 1.0]],
        "text": "alpha", "tie_breaker": 0.3}}),
    );
    assert!(
        (fused - (s1 + 0.3 * s2)).abs() < 1e-6,
        "{fused} vs {s1} + 0.3 * {s2}"
    );
}

#[tokio::test]
async fn an_unknown_field_matches_nothing_and_is_empty_matches_all() {
    let fx = Fixture::new().await;
    for query in [
        json!({"term": {"field": "nosuch", "value": "x"}}),
        json!({"match": {"field": "nosuch", "text": "x"}}),
        json!({"range": {"field": "nosuch", "gte": 1}}),
        json!({"exists": {"field": "nosuch"}}),
        json!({"prefix": {"field": "nosuch", "value": "x"}}),
    ] {
        assert!(fx.hits(&q(query.clone())).unwrap().is_empty(), "{query}");
    }
    let all = fx
        .hits(&q(json!({"is_empty": {"field": "nosuch"}})))
        .unwrap();
    assert_eq!(all, set(&ALL));
}

#[tokio::test]
async fn a_split_written_before_a_field_existed_matches_nothing_on_it() {
    let current = schema();
    let mut old = current.clone();
    old.fields.retain(|spec| spec.name != "tag");
    let searcher = split(
        &old,
        vec![
            json!({"f": {"title": "old fox", "tag": "alpha", "n": 1}}),
            json!({"f": {"title": "old dog", "tag": "beta", "n": 2}}),
        ],
    )
    .await;
    let tokenizers = query_tokenizers();
    let hits = |query: Value| hits_in(&current, &searcher, &tokenizers, &q(query)).unwrap();
    assert!(hits(json!({"term": {"field": "tag", "value": "alpha"}})).is_empty());
    assert!(hits(json!({"exists": {"field": "tag"}})).is_empty());
    assert_eq!(hits(json!({"is_empty": {"field": "tag"}})), set(&[0, 1]));
    // The fields the split has still work.
    assert_eq!(hits(json!({"term": {"field": "n", "value": 2}})), set(&[1]));
}

#[test]
fn a_phrase_on_a_field_without_positions_is_invalid() {
    let schema = CollectionSchema::new(vec![text("body", false)], vec![], DynamicMapping::Ignore);
    let split = tantivy_layout(&schema).schema;
    let tokenizers = query_tokenizers();
    let compiler = QueryCompiler::new(&schema, &split, &tokenizers);
    let phrase = q(json!({"match_phrase": {"field": "body", "text": "quick brown"}}));
    match compiler.compile(&phrase, CompileMode::Scoring) {
        Err(ServiceError::InvalidArgument(message)) => {
            assert_eq!(message, "field body was indexed without positions");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[test]
fn is_null_on_a_plain_field_is_invalid() {
    let schema = schema();
    let split = tantivy_layout(&schema).schema;
    let tokenizers = query_tokenizers();
    let compiler = QueryCompiler::new(&schema, &split, &tokenizers);
    match compiler.compile(
        &q(json!({"is_null": {"field": "title"}})),
        CompileMode::Filter,
    ) {
        Err(ServiceError::InvalidArgument(message)) => {
            assert_eq!(message, "is_null is supported on JSON field paths only");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[tokio::test]
async fn query_string_honours_the_default_operator() {
    let fx = Fixture::new().await;
    let query = |operator: &str| {
        q(
            json!({"query_string": {"query": "quick fox", "default_fields": ["title"],
            "default_operator": operator}}),
        )
    };
    assert_eq!(fx.hits(&query("or")).unwrap(), set(&[0, 1, 3, 6, 9]));
    assert_eq!(fx.hits(&query("and")).unwrap(), set(&[0]));
}

#[test]
fn highlight_terms_skip_must_not() {
    let schema = schema();
    let tokenizers = query_tokenizers();
    let query = q(json!({"bool": {
        "must": [{"match": {"field": "title", "text": "Quick foxes"}}],
        "must_not": [{"match": {"field": "title", "text": "lazy"}}],
        "should": [{"match": {"field": "payload.body", "text": "Brown"}},
                   {"term": {"field": "tag", "value": "alpha"}}]}}));
    let terms = highlight_terms(&schema, &query, &tokenizers);
    let expected: Vec<(String, String)> = [
        ("title", "quick"),
        ("title", "fox"),
        ("payload.body", "brown"),
    ]
    .into_iter()
    .map(|(f, t)| (f.to_string(), t.to_string()))
    .collect();
    assert_eq!(terms, expected);
}

#[tokio::test]
async fn warmup_names_what_the_query_reads() {
    let fx = Fixture::new().await;
    let split = fx.split_schema();
    let compiler = QueryCompiler::new(&fx.schema, &split, &fx.tokenizers);
    let scored = compiler
        .compile(
            &q(json!({"match": {"field": "title", "text": "quick"}})),
            CompileMode::Scoring,
        )
        .unwrap();
    assert!(!scored.constant_score);
    assert!(scored.warmup.field_norms);
    let names: BTreeSet<String> = scored
        .warmup
        .fast_fields
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert!(
        names.contains("_rowid") && names.contains("_pk"),
        "{names:?}"
    );
    let title = split.get_field("title").unwrap();
    assert_eq!(scored.warmup.terms_grouped_by_field[&title].len(), 1);

    let filtered = compiler
        .compile(
            &q(json!({"match": {"field": "title", "text": "quick"}})),
            CompileMode::Filter,
        )
        .unwrap();
    assert!(filtered.constant_score && !filtered.warmup.field_norms);

    let exists = compiler
        .compile(
            &q(json!({"exists": {"field": "payload.k"}})),
            CompileMode::Filter,
        )
        .unwrap();
    assert!(
        exists
            .warmup
            .fast_fields
            .iter()
            .any(|f| f.name == "payload.k" && f.with_subfields)
    );

    let phrase = compiler
        .compile(
            &q(json!({"match_phrase": {"field": "title", "text": "quick brown"}})),
            CompileMode::Scoring,
        )
        .unwrap();
    assert!(
        phrase.warmup.terms_grouped_by_field[&title]
            .values()
            .all(|positions| *positions)
    );
}
