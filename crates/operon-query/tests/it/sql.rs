//! SQL (plan M1.2 Task 10): the DataFusion catalog, `CollectionProvider`
//! and the search table functions (Ruling 23, D56).

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{Fixture, field, obj, vector};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DictionaryArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int64Array,
    LargeStringArray, ListArray, MapBuilder, RecordBatch, StringArray, StringBuilder,
    StringViewArray, StructArray, TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Int32Type, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, ScalarUDF};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use operon_collection::{
    CollectionSchema, DocOp, Document, DynamicMapping, FieldKind, PrimaryKey, SparseModifier,
    SparseVector, SparseVectorSpec,
};
use operon_common::meta::AliasAction;
use operon_query::sql::{
    CollectionProvider, RetrieverDescriptorUdf, SqlResult, rows_to_json, run_read_only,
};
use operon_query::{
    AnnParams, BoolOperator, CollectionService, FieldValue, Fusion, Query, Retriever,
    SearchRequest, ServiceConfig, ServiceError, SqlConfig, WriteOptions,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

const NS: &str = "acme";
const UUID: &str = "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e";

fn text_field(name: &str) -> operon_collection::FieldSpec {
    field(
        name,
        FieldKind::Text {
            analyzer: "standard".to_string(),
            positions: true,
        },
    )
}

/// The M1.6 fixture's `kb`: `body`, `tenant`, `n` and `embedding` (dim 3).
fn kb_schema() -> CollectionSchema {
    CollectionSchema::new(
        vec![
            text_field("body"),
            field("tenant", FieldKind::Keyword),
            field("n", FieldKind::I64),
        ],
        vec![vector("embedding", 3)],
        DynamicMapping::Ignore,
    )
}

fn document(pk: PrimaryKey, source: Value, embedding: Option<[f32; 3]>) -> DocOp {
    let mut vectors = BTreeMap::new();
    if let Some(embedding) = embedding {
        vectors.insert("embedding".to_string(), embedding.to_vec());
    }
    DocOp::Upsert(Document {
        pk,
        source: obj(source),
        vectors,
        sparse_vectors: BTreeMap::new(),
    })
}

/// The bytes of a hyphenated UUID.
fn uuid_bytes(text: &str) -> [u8; 16] {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).expect("hex");
    }
    out
}

/// The M1.6 fixture's step 10 documents.
fn kb_docs() -> Vec<DocOp> {
    let uuid = uuid_bytes(UUID);
    vec![
        document(
            PrimaryKey::U64(1),
            json!({"body": "refund policy", "tenant": "a", "n": 1}),
            Some([1.0, 0.0, 0.0]),
        ),
        document(
            PrimaryKey::U64(2),
            json!({"body": "shipping times", "tenant": "a", "n": 2}),
            Some([0.9, 0.1, 0.0]),
        ),
        document(
            PrimaryKey::U64(3),
            json!({"body": "refund window", "tenant": "b", "n": 3}),
            Some([0.0, 0.0, 1.0]),
        ),
        document(PrimaryKey::U64(u64::MAX), json!({"tenant": "c"}), None),
        document(
            PrimaryKey::Str("k-str".to_string()),
            json!({"tenant": "c"}),
            None,
        ),
        document(PrimaryKey::Uuid(uuid), json!({"tenant": "c"}), None),
    ]
}

async fn write(service: &CollectionService, name: &str, ops: Vec<DocOp>) {
    service
        .write(NS, name, ops, WriteOptions::default())
        .await
        .expect("write");
}

/// A fixture with `kb` holding the M1.6 documents, applied.
async fn with_kb() -> (Fixture, Arc<CollectionService>) {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, "kb", kb_schema(), Some(2))
        .await
        .expect("create kb");
    write(&service, "kb", kb_docs()).await;
    f.settle().await;
    (f, service)
}

async fn sql(service: &CollectionService, query: &str) -> Result<SqlResult, ServiceError> {
    let ctx = service.sql_context(NS);
    run_read_only(&ctx, query, &service.config().sql).await
}

async fn rows(service: &CollectionService, query: &str) -> Vec<Value> {
    let result = sql(service, query).await.expect(query);
    match rows_to_json(&result)["rows"].take() {
        Value::Array(rows) => rows,
        other => panic!("rows: {other}"),
    }
}

fn invalid(result: Result<SqlResult, ServiceError>) -> String {
    match result {
        Err(ServiceError::InvalidArgument(message)) => message,
        other => panic!(
            "expected InvalidArgument, got {:?}",
            other.map(|r| r.batches)
        ),
    }
}

/// `_id` and `_score` of every row.
fn ids_and_scores(result: &SqlResult) -> Vec<(String, f32)> {
    let mut out = Vec::new();
    for batch in &result.batches {
        let ids = batch
            .column_by_name("_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("_id");
        let scores = batch
            .column_by_name("_score")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            .expect("_score");
        for row in 0..batch.num_rows() {
            out.push((ids.value(row).to_string(), scores.value(row)));
        }
    }
    out
}

fn id_text(pk: &PrimaryKey) -> String {
    match pk {
        PrimaryKey::U64(n) => n.to_string(),
        PrimaryKey::Str(s) => s.clone(),
        PrimaryKey::Uuid(_) => UUID.to_string(),
    }
}

async fn searched(service: &CollectionService, request: SearchRequest) -> Vec<(String, f32)> {
    service
        .search(NS, request)
        .await
        .expect("search")
        .hits
        .iter()
        .map(|hit| (id_text(&hit.pk), hit.score))
        .collect()
}

fn matching(field: &str, text: &str) -> Query {
    Query::Match {
        field: field.to_string(),
        text: text.to_string(),
        operator: BoolOperator::Or,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    }
}

fn tenant(value: &str) -> Query {
    Query::Term {
        field: "tenant".to_string(),
        value: FieldValue::Str(value.to_string()),
    }
}

fn request(retrievers: Vec<Retriever>, limit: usize) -> SearchRequest {
    let mut request = SearchRequest::new("kb");
    request.retrievers = retrievers;
    request.limit = limit;
    request
}

fn vector_retriever(k: usize, exact: bool, filter: Option<Query>) -> Retriever {
    Retriever::Vector {
        field: "embedding".to_string(),
        query: vec![1.0, 0.0, 0.0],
        k,
        params: AnnParams {
            exact,
            ..AnnParams::default()
        },
        filter,
    }
}

fn text_retriever(k: usize, filter: Option<Query>) -> Retriever {
    let query = match filter {
        None => matching("body", "refund"),
        Some(filter) => Query::Bool {
            must: vec![matching("body", "refund")],
            should: vec![],
            must_not: vec![],
            filter: vec![filter],
            minimum_should_match: None,
        },
    };
    Retriever::Text { query, k }
}

// ----- tests -----

#[tokio::test]
async fn select_count_star_counts_durable_and_tail_rows() {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, "kb", kb_schema(), Some(2))
        .await
        .expect("create kb");
    let docs = kb_docs();
    write(&service, "kb", docs[..3].to_vec()).await;
    f.settle().await;
    // Two more in the tail: the link does not run again.
    write(&service, "kb", docs[3..5].to_vec()).await;
    let result = sql(&service, "SELECT count(*) AS n FROM kb")
        .await
        .expect("count");
    assert_eq!(
        rows_to_json(&result),
        json!({"columns":[{"name":"n","type":"Int64"}],"rows":[[5]],"truncated":false})
    );
    f.shutdown().await;
}

fn typed_schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![
            text_field("body"),
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field("x", FieldKind::F64),
            field("flag", FieldKind::Bool),
            field("at", FieldKind::Date),
            field("id", FieldKind::Uuid),
            field("meta", FieldKind::Json),
        ],
        vec![vector("", 2), vector("named", 3)],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    schema.validate().expect("valid schema");
    schema
}

#[tokio::test]
async fn typed_columns_come_from_source() {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, "typed", typed_schema(), Some(2))
        .await
        .expect("create");
    let source = json!({
        "body": "hello world",
        "tag": "red",
        "n": "42",
        "x": 2.5,
        "flag": "true",
        "at": "2024-01-02T03:04:05.678Z",
        "id": UUID.to_uppercase(),
        "meta": {"a": [1, {"b": null}]},
    });
    let full = DocOp::Upsert(Document {
        pk: PrimaryKey::Str("doc-1".to_string()),
        source: obj(source.clone()),
        vectors: BTreeMap::from([
            ("".to_string(), vec![0.5, -0.5]),
            ("named".to_string(), vec![1.0, 2.0, 3.0]),
        ]),
        sparse_vectors: BTreeMap::from([(
            "s".to_string(),
            SparseVector::new(vec![1, 5], vec![0.5, 0.25]).expect("sparse"),
        )]),
    });
    let bare = DocOp::Upsert(Document {
        pk: PrimaryKey::U64(7),
        source: obj(json!({"other": 1})),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    });
    write(&service, "typed", vec![full]).await;
    f.settle().await;
    // One durable row and one tail row.
    write(&service, "typed", vec![bare]).await;
    let result = sql(
        &service,
        "SELECT _id, _source, _partition, body, tag, n, x, flag, at, id, meta, _vector, named, s \
         FROM typed ORDER BY _id",
    )
    .await
    .expect("select");
    let out = rows_to_json(&result);
    let got = out["rows"].as_array().expect("rows");
    assert_eq!(got.len(), 2);
    let doc = &got[1];
    assert_eq!(doc[0], json!("doc-1"));
    let stored: Value = serde_json::from_str(doc[1].as_str().expect("_source")).expect("json");
    assert_eq!(stored, source);
    let partition = operon_collection::partition_of(&PrimaryKey::Str("doc-1".to_string()), 2);
    assert_eq!(doc[2], json!(partition));
    assert_eq!(
        doc.as_array().expect("row")[3..],
        [
            json!("hello world"),
            json!("red"),
            json!(42),
            json!(2.5),
            json!(true),
            json!("2024-01-02T03:04:05.678Z"),
            json!(UUID),
            json!(r#"{"a":[1,{"b":null}]}"#),
            json!([0.5, -0.5]),
            json!([1.0, 2.0, 3.0]),
            json!({"indices": [1, 5], "values": [0.5, 0.25]}),
        ]
    );
    // The tail row: every field and vector column null.
    let bare = &got[0];
    assert_eq!(bare[0], json!("7"));
    assert!(
        bare.as_array().expect("row")[3..]
            .iter()
            .all(Value::is_null)
    );
    // The column types of rule 2.
    let types: Vec<String> = result
        .schema
        .fields()
        .iter()
        .map(|field| field.data_type().to_string())
        .collect();
    assert_eq!(types[0], DataType::Utf8.to_string());
    assert_eq!(
        result.schema.field_with_name("at").expect("at").data_type(),
        &DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Millisecond,
            Some("UTC".into())
        )
    );
    assert_eq!(
        result
            .schema
            .field_with_name("named")
            .expect("named")
            .data_type(),
        &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3)
    );
    // `_seq_no` is the offset that wrote the document.
    let seq = rows(&service, "SELECT _seq_no FROM typed WHERE _id = 'doc-1'").await;
    assert_eq!(seq, vec![json!([0])]);
    f.shutdown().await;
}

#[tokio::test]
async fn a_multi_valued_field_exposes_its_first_value() {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, "typed", typed_schema(), Some(2))
        .await
        .expect("create");
    write(
        &service,
        "typed",
        vec![DocOp::Upsert(Document {
            pk: PrimaryKey::U64(1),
            source: obj(json!({"tag": ["b", "a"], "n": [null, 5, 3], "x": []})),
            vectors: BTreeMap::new(),
            sparse_vectors: BTreeMap::new(),
        })],
    )
    .await;
    let got = rows(&service, "SELECT tag, n, x, _source FROM typed").await;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], json!("b"));
    assert_eq!(got[0][1], json!(5));
    assert_eq!(got[0][2], Value::Null);
    let source: Value = serde_json::from_str(got[0][3].as_str().expect("_source")).expect("json");
    assert_eq!(source["tag"], json!(["b", "a"]));
    // A pushed-down filter matches on any value, and DataFusion keeps SQL's
    // first-value semantics.
    assert!(
        rows(&service, "SELECT _id FROM typed WHERE tag = 'a'")
            .await
            .is_empty()
    );
    assert_eq!(
        rows(&service, "SELECT _id FROM typed WHERE tag = 'b'").await,
        vec![json!(["1"])]
    );
    f.shutdown().await;
}

fn pushdown_schema() -> CollectionSchema {
    CollectionSchema::new(
        vec![
            field("tag", FieldKind::Keyword),
            field("n", FieldKind::I64),
            field("x", FieldKind::F64),
            field("flag", FieldKind::Bool),
        ],
        vec![],
        DynamicMapping::Ignore,
    )
}

fn pushdown_source(pk: u64, version: u64) -> Value {
    let mut source = serde_json::Map::new();
    let v = pk + version;
    if !pk.is_multiple_of(7) {
        let n = if pk.is_multiple_of(4) {
            json!([v % 10, (v * 3) % 10])
        } else {
            json!(v % 10)
        };
        source.insert("n".to_string(), n);
    }
    if !pk.is_multiple_of(9) {
        let tag = if pk.is_multiple_of(6) {
            json!([format!("t{}", v % 5), "t9"])
        } else {
            json!(format!("t{}", v % 5))
        };
        source.insert("tag".to_string(), tag);
    }
    source.insert("x".to_string(), json!(v as f64 / 4.0));
    if !pk.is_multiple_of(11) {
        source.insert("flag".to_string(), json!(v.is_multiple_of(2)));
    }
    Value::Object(source)
}

fn atom(rng: &mut ChaCha8Rng) -> String {
    let v = rng.random_range(0..10);
    let w = rng.random_range(0..10);
    match rng.random_range(0..16) {
        0 => format!("n = {v}"),
        1 => format!("n > {v}"),
        2 => format!("n <= {v}"),
        3 => format!("{v} < n"),
        4 => format!("n BETWEEN {} AND {}", v.min(w), v.max(w)),
        5 => format!("n IN ({v}, {w}, 11)"),
        6 => "n IS NULL".to_string(),
        7 => "n IS NOT NULL".to_string(),
        8 => format!("tag = 't{}'", v % 5),
        9 => format!("tag IN ('t{}', 't9')", v % 5),
        10 => format!("tag > 't{}'", v % 5),
        11 => "tag IS NULL".to_string(),
        12 => format!("x >= {}", v as f64 / 2.0),
        13 => format!("x < {v}"),
        14 => format!("flag = {}", v % 2 == 0),
        _ => format!("_id = '{}'", v * 3),
    }
}

fn clause(rng: &mut ChaCha8Rng, depth: u32) -> String {
    if depth == 0 {
        return atom(rng);
    }
    match rng.random_range(0..4) {
        0 => atom(rng),
        1 => format!(
            "({} AND {})",
            clause(rng, depth - 1),
            clause(rng, depth - 1)
        ),
        2 => format!("({} OR {})", clause(rng, depth - 1), clause(rng, depth - 1)),
        _ => format!("NOT ({})", clause(rng, depth - 1)),
    }
}

async fn sorted_rows(ctx: &SessionContext, query: &str, config: &SqlConfig) -> Vec<String> {
    let result = run_read_only(ctx, query, config).await.expect(query);
    let mut rows: Vec<String> = rows_to_json(&result)["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(Value::to_string)
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn pushed_down_filters_equal_unpushed_results() {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(NS, "pd", pushdown_schema(), Some(2))
        .await
        .expect("create");
    let ops: Vec<DocOp> = (0..30)
        .map(|pk| document(PrimaryKey::U64(pk), pushdown_source(pk, 0), None))
        .collect();
    write(&service, "pd", ops).await;
    f.settle().await;
    // The tail: new documents, updates and deletes.
    let mut tail: Vec<DocOp> = (30..36)
        .map(|pk| document(PrimaryKey::U64(pk), pushdown_source(pk, 0), None))
        .collect();
    tail.extend(
        (0..5).map(|pk| document(PrimaryKey::U64(pk * 5), pushdown_source(pk * 5, 1), None)),
    );
    tail.extend([3, 8, 13].map(|pk| DocOp::Delete(PrimaryKey::U64(pk))));
    write(&service, "pd", tail).await;

    let config = service.config().sql.clone();
    let pushed = service.sql_context(NS);
    let provider = pushed.table_provider("pd").await.expect("pd");
    let provider = (provider.as_ref() as &dyn Any)
        .downcast_ref::<CollectionProvider>()
        .expect("a collection provider");
    let unpushed = SessionContext::new();
    unpushed
        .register_table("pd", Arc::new(provider.without_pushdown()))
        .expect("register");

    // The filter reaches the scan.
    let plan = pushed
        .sql("SELECT _id FROM pd WHERE n = 3")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    assert!(shown.contains("filtered=true"), "{shown}");

    let mut rng = ChaCha8Rng::seed_from_u64(10);
    for _ in 0..20 {
        let clause = clause(&mut rng, 2);
        let query = format!("SELECT _id, tag, n, x, flag FROM pd WHERE {clause}");
        assert_eq!(
            sorted_rows(&pushed, &query, &config).await,
            sorted_rows(&unpushed, &query, &config).await,
            "{query}"
        );
    }
    f.shutdown().await;
}

#[tokio::test]
async fn vector_search_text_search_and_hybrid_search_match_the_ir() {
    let (f, service) = with_kb().await;
    let filter = r#"'{"term": {"field": "tenant", "value": "a"}}'"#;
    let cases: Vec<(String, SearchRequest)> = vec![
        (
            format!("vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 5, {filter}, true)"),
            request(vec![vector_retriever(5, true, Some(tenant("a")))], 5),
        ),
        (
            "vector_search('kb', [1.0, 0.0, 0.0])".to_string(),
            request(vec![vector_retriever(1_000, false, None)], 1_000),
        ),
        (
            "vector_search('kb', '[1.0, 0.0, 0.0]', NULL, 2)".to_string(),
            request(vec![vector_retriever(2, false, None)], 2),
        ),
        (
            format!("text_search('kb', 'refund', 'body', 5, {filter})"),
            request(vec![text_retriever(5, Some(tenant("a")))], 5),
        ),
        (
            "text_search('kb', 'refund')".to_string(),
            request(vec![text_retriever(1_000, None)], 1_000),
        ),
        (
            "hybrid_search('kb', 'embedding', [1.0, 0.0, 0.0], 'body', 'refund', 10)".to_string(),
            SearchRequest {
                fusion: Some(Fusion::Rrf { k: 60 }),
                ..request(
                    vec![vector_retriever(10, false, None), text_retriever(10, None)],
                    10,
                )
            },
        ),
        (
            format!(
                "hybrid_search('kb', 'embedding', [1.0, 0.0, 0.0], 'body', 'refund', 10, 'dbsf', NULL, {filter})"
            ),
            SearchRequest {
                fusion: Some(Fusion::Dbsf),
                filter: Some(tenant("a")),
                ..request(
                    vec![vector_retriever(10, false, None), text_retriever(10, None)],
                    10,
                )
            },
        ),
    ];
    for (call, ir) in cases {
        let expected = searched(&service, ir).await;
        assert!(!expected.is_empty(), "{call}");
        let result = sql(&service, &format!("SELECT _id, _score FROM {call}"))
            .await
            .expect(&call);
        assert_eq!(ids_and_scores(&result), expected, "{call}");
    }
    // Every column of rule 2 after `_id` and `_score`.
    let hit = rows(
        &service,
        "SELECT _id, _score, _source, _seq_no, _partition, body, tenant, n, embedding \
         FROM text_search('kb', 'refund', 'body', 1)",
    )
    .await;
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0][0], json!("1"));
    assert_eq!(hit[0][5], json!("refund policy"));
    assert_eq!(hit[0][6], json!("a"));
    assert_eq!(hit[0][7], json!(1));
    assert_eq!(hit[0][8], json!([1.0, 0.0, 0.0]));
    assert!(hit[0][3].is_u64() && hit[0][4].is_u64(), "{:?}", hit[0]);
    f.shutdown().await;
}

#[tokio::test]
async fn omitted_fields_need_a_single_candidate() {
    let (f, service) = with_kb().await;
    let schema = CollectionSchema::new(
        vec![text_field("title"), text_field("body")],
        vec![vector("a", 3), vector("b", 3)],
        DynamicMapping::Ignore,
    );
    service
        .create_collection(NS, "kb2", schema, None)
        .await
        .expect("create kb2");
    let message = invalid(
        sql(
            &service,
            "SELECT * FROM vector_search('kb2', [1.0, 0.0, 0.0])",
        )
        .await,
    );
    assert!(
        message.contains("vector_search on kb2 needs a field: it has 2 vectors"),
        "{message}"
    );
    let message = invalid(sql(&service, "SELECT * FROM text_search('kb2', 'x')").await);
    assert!(
        message.contains("text_search on kb2 needs a field: it has 2 text fields"),
        "{message}"
    );
    f.shutdown().await;
}

#[test]
fn retriever_descriptors_fold_to_literals() {
    // Ruling 23's nested calls: DataFusion 54's `ExprSimplifier` evaluates
    // the immutable descriptor UDF of literal arguments (`make_array`
    // included) into a Utf8 literal.
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::new_from_impl(RetrieverDescriptorUdf::new(
        "vector_search",
    )));
    let expr = ctx
        .parse_sql_expr(
            "vector_search('kb', [1.0, 0.0, 0.0], NULL, 10)",
            &DFSchema::empty(),
        )
        .expect("parse");
    let simplifier = ExprSimplifier::new(Default::default());
    let Expr::Literal(ScalarValue::Utf8(Some(text)), _) = simplifier.simplify(expr).expect("fold")
    else {
        panic!("the descriptor did not fold");
    };
    let descriptor: Value = serde_json::from_str(&text).expect("json");
    assert_eq!(descriptor["collection"], json!("kb"));
    assert_eq!(descriptor["retriever"]["vector"]["field"], Value::Null);
    assert_eq!(descriptor["retriever"]["vector"]["k"], json!(10));
    assert_eq!(
        descriptor["retriever"]["vector"]["query"],
        json!([1.0, 0.0, 0.0])
    );
}

#[tokio::test]
async fn rrf_fuses_nested_search_calls_like_the_ir() {
    let (f, service) = with_kb().await;
    let other = kb_schema();
    service
        .create_collection(NS, "other", other, None)
        .await
        .expect("create other");
    let expected = searched(
        &service,
        SearchRequest {
            fusion: Some(Fusion::Rrf { k: 60 }),
            ..request(
                vec![vector_retriever(10, false, None), text_retriever(10, None)],
                3,
            )
        },
    )
    .await;
    let ids: Vec<&str> = expected.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["1", "3", "2"]);
    let fused = |tail: &str| {
        format!(
            "SELECT _id, _score FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), \
             text_search('kb', 'refund', 'body', 10){tail}) LIMIT 3"
        )
    };
    for tail in ["", ", 60.0", ", 60"] {
        let result = sql(&service, &fused(tail)).await.expect(tail);
        assert_eq!(ids_and_scores(&result), expected, "{tail}");
    }
    // Omitted fields resolve against kb's schema, and descriptors may also
    // come as their JSON text.
    let result = sql(
        &service,
        "SELECT _id, _score FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], NULL, 10), \
         text_search('kb', 'refund', NULL, 10)) LIMIT 3",
    )
    .await
    .expect("omitted fields");
    assert_eq!(ids_and_scores(&result), expected);
    let result = sql(
        &service,
        r#"SELECT _id, _score FROM rrf('{"collection": "kb", "retriever": {"vector": {"field": "embedding", "query": [1.0, 0.0, 0.0], "k": 10}}}', text_search('kb', 'refund', 'body', 10)) LIMIT 3"#,
    )
    .await
    .expect("a descriptor's JSON text");
    assert_eq!(ids_and_scores(&result), expected);
    let message = invalid(sql(&service, &fused(", 60.5")).await);
    assert!(
        message.contains("rrf's k must be a whole number"),
        "{message}"
    );
    let message = invalid(
        sql(
            &service,
            "SELECT * FROM rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), \
             text_search('other', 'refund', 'body', 10))",
        )
        .await,
    );
    assert!(
        message.contains("rrf fuses retrievers of one collection, got kb and other"),
        "{message}"
    );
    f.shutdown().await;
}

#[tokio::test]
async fn rerank_is_reserved_for_m3() {
    let (f, service) = with_kb().await;
    let message = invalid(
        sql(
            &service,
            "SELECT * FROM rerank(rrf(vector_search('kb', [1.0, 0.0, 0.0], 'embedding', 10), \
             text_search('kb', 'refund', 'body', 10)))",
        )
        .await,
    );
    assert!(message.contains("rerank arrives in M3"), "{message}");
    f.shutdown().await;
}

#[tokio::test]
async fn a_non_constant_table_function_argument_is_refused() {
    let (f, service) = with_kb().await;
    let message = invalid(
        sql(
            &service,
            "SELECT * FROM text_search('kb', CAST(random() AS VARCHAR))",
        )
        .await,
    );
    assert!(
        message.contains("argument 2 of text_search must be a constant"),
        "{message}"
    );
    f.shutdown().await;
}

#[tokio::test]
async fn ddl_and_dml_are_refused() {
    let (f, service) = with_kb().await;
    for statement in [
        "CREATE TABLE t (a INT)",
        "INSERT INTO kb SELECT * FROM kb",
        "COPY (SELECT 1) TO 'out.csv'",
        "SET datafusion.execution.batch_size = 10",
    ] {
        let message = invalid(sql(&service, statement).await);
        if statement.starts_with("CREATE") || statement.starts_with("SET") {
            assert!(
                message.starts_with("only read-only queries are allowed"),
                "{statement}: {message}"
            );
        }
    }
    // Nothing was created.
    assert_eq!(
        rows(&service, "SELECT count(*) FROM kb").await,
        vec![json!([6])]
    );
    f.shutdown().await;
}

#[tokio::test]
async fn tables_resolve_by_name_alias_and_qualified_name() {
    let (f, service) = with_kb().await;
    service
        .update_aliases(
            NS,
            vec![AliasAction::Create {
                alias: "kb_alias".to_string(),
                collection: "kb".to_string(),
            }],
        )
        .await
        .expect("alias");
    let ctx = service.sql_context(NS);
    // Created after the context exists.
    service
        .create_collection(NS, "later", kb_schema(), None)
        .await
        .expect("create later");
    let config = service.config().sql.clone();
    for (query, n) in [
        ("SELECT count(*) FROM kb", 6),
        ("SELECT count(*) FROM collections.kb", 6),
        ("SELECT count(*) FROM \"acme\".collections.kb", 6),
        ("SELECT count(*) FROM acme.collections.kb_alias", 6),
        ("SELECT count(*) FROM kb_alias", 6),
        ("SELECT count(*) FROM later", 0),
    ] {
        let result = run_read_only(&ctx, query, &config).await.expect(query);
        assert_eq!(rows_to_json(&result)["rows"], json!([[n]]), "{query}");
    }
    let names = rows(
        &service,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'collections' ORDER BY table_name",
    )
    .await;
    assert_eq!(
        names,
        vec![json!(["kb"]), json!(["kb_alias"]), json!(["later"])]
    );
    let message = invalid(run_read_only(&ctx, "SELECT * FROM missing", &config).await);
    assert!(message.contains("missing"), "{message}");
    f.shutdown().await;
}

#[tokio::test]
async fn results_are_capped_and_marked_truncated() {
    let f = Fixture::start_with(ServiceConfig {
        sql: SqlConfig {
            max_rows: 4,
            timeout: Duration::from_secs(30),
            batch_rows: 2,
        },
        ..ServiceConfig::default()
    })
    .await;
    let service = f.service();
    service
        .create_collection(NS, "kb", kb_schema(), Some(2))
        .await
        .expect("create kb");
    write(&service, "kb", kb_docs()).await;
    f.settle().await;
    let result = sql(&service, "SELECT _id FROM kb").await.expect("select");
    let out = rows_to_json(&result);
    assert_eq!(out["rows"].as_array().expect("rows").len(), 4);
    assert_eq!(out["truncated"], json!(true));
    let result = sql(&service, "SELECT _id FROM kb LIMIT 4")
        .await
        .expect("limit");
    assert_eq!(rows_to_json(&result)["truncated"], json!(false));
    let result = sql(&service, "SELECT _id FROM kb WHERE n >= 2")
        .await
        .expect("where");
    assert_eq!(rows_to_json(&result)["truncated"], json!(false));
    f.shutdown().await;
}

#[test]
fn rows_to_json_formats_every_type() {
    let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
    ]);
    let fixed = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![Some(vec![Some(0.5), Some(1.5)]), None],
        2,
    );
    let strukt = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
        ),
    ]);
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Array::builder(2));
    map.keys().append_value("k");
    map.values().append_value(7);
    map.append(true).expect("map");
    map.append(false).expect("map");
    let dictionary: DictionaryArray<Int32Type> = vec![Some("red"), None].into_iter().collect();
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("bool", Arc::new(BooleanArray::from(vec![Some(true), None]))),
        ("i8", Arc::new(Int8Array::from(vec![Some(-8), None]))),
        (
            "i64",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
        ),
        (
            "u64",
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
        ),
        (
            "f32",
            Arc::new(Float32Array::from(vec![Some(0.1), Some(f32::NAN)])),
        ),
        (
            "f64",
            Arc::new(Float64Array::from(vec![Some(2.5), Some(f64::INFINITY)])),
        ),
        (
            "dec",
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None])
                    .with_precision_and_scale(10, 2)
                    .expect("decimal"),
            ),
        ),
        ("utf8", Arc::new(StringArray::from(vec![Some("a"), None]))),
        (
            "large",
            Arc::new(LargeStringArray::from(vec![Some("b"), None])),
        ),
        (
            "view",
            Arc::new(StringViewArray::from(vec![Some("c"), None])),
        ),
        (
            "bin",
            Arc::new(BinaryArray::from(vec![Some(b"hi".as_slice()), None])),
        ),
        ("d32", Arc::new(Date32Array::from(vec![Some(19_723), None]))),
        (
            "d64",
            Arc::new(Date64Array::from(vec![Some(1_704_153_600_000), None])),
        ),
        (
            "ts_ms",
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(1_704_164_645_678), None])
                    .with_timezone("UTC"),
            ),
        ),
        (
            "ts_ns",
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_164_645_000_000_001),
                None,
            ])),
        ),
        ("list", Arc::new(list)),
        ("fixed", Arc::new(fixed)),
        ("struct", Arc::new(strukt)),
        ("map", Arc::new(map.finish())),
        ("dict", Arc::new(dictionary)),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .expect("batch");
    let out = rows_to_json(&SqlResult {
        schema,
        batches: vec![batch],
        truncated: false,
    });
    assert_eq!(
        out["columns"][0],
        json!({"name": "bool", "type": "Boolean"})
    );
    assert_eq!(out["columns"][3], json!({"name": "u64", "type": "UInt64"}));
    assert_eq!(
        out["rows"][0],
        json!([
            true,
            -8,
            i64::MIN,
            u64::MAX,
            0.1,
            2.5,
            "123.45",
            "a",
            "b",
            "c",
            "aGk=",
            "2024-01-01",
            "2024-01-02",
            "2024-01-02T03:04:05.678Z",
            "2024-01-02T03:04:05.000000001Z",
            [1, null, 3],
            [0.5, 1.5],
            {"a": 1, "b": "x"},
            [{"key": "k", "value": 7}],
            "red",
        ])
    );
    assert_eq!(
        out["rows"][1],
        json!([
            null, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, {"a": null, "b": "y"}, null, null,
        ])
    );
    assert_eq!(out["truncated"], json!(false));
}
