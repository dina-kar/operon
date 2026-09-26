//! Flight `DoPut` bulk ingest over an in-process server (plan M1.2 Task 13).

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, Float32Builder, ListBuilder, StringBuilder, StructBuilder, UInt32Builder,
};
use arrow_array::types::Float32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray, TimestampMillisecondArray, UInt32Array, UInt64Array, UnionArray,
};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandStatementIngest, SqlInfo, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use arrow_flight::{FlightClient, FlightDescriptor};
use arrow_schema::{DataType, Field};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::{StreamExt, TryStreamExt};
use operon_collection::{ConsistencyToken, PrimaryKey, partition_of};
use operon_common::CollectionId;
use operon_common::meta::implicit_name;
use operon_query::flight::NAMESPACE_METADATA;
use operon_query::flight_ingest::PutAck;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tonic::Code;
use tonic::transport::Channel;

use crate::common::{Native, TOKEN};

// ----- clients -----

async fn channel(api: &Native) -> Channel {
    let addr = api.server.flight_sql_addr().expect("flight sql listens");
    Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect")
}

async fn flight(api: &Native, ns: &str) -> FlightClient {
    let mut client = FlightClient::new(channel(api).await);
    client.add_header(NAMESPACE_METADATA, ns).expect("header");
    client
}

async fn flight_sql(api: &Native, ns: &str) -> FlightSqlServiceClient<Channel> {
    let mut client = FlightSqlServiceClient::new(channel(api).await);
    client.set_header(NAMESPACE_METADATA, ns);
    client
}

/// A path-descriptor put of `batches`: the acknowledgements received, then
/// the error that ended the put, if any.
async fn put(
    client: &mut FlightClient,
    path: &[&str],
    batches: Vec<RecordBatch>,
    max_message: Option<usize>,
) -> (Vec<PutAck>, Option<FlightError>) {
    let descriptor = FlightDescriptor::new_path(path.iter().map(|p| p.to_string()).collect());
    let mut encoder = FlightDataEncoderBuilder::new().with_flight_descriptor(Some(descriptor));
    if let Some(size) = max_message {
        encoder = encoder.with_max_flight_data_size(size);
    }
    let data = encoder.build(futures::stream::iter(batches.into_iter().map(Ok)));
    let mut responses = match client.do_put(data).await {
        Ok(responses) => responses,
        Err(err) => return (Vec::new(), Some(err)),
    };
    let mut acks = Vec::new();
    while let Some(response) = responses.next().await {
        match response {
            Ok(result) => {
                acks.push(serde_json::from_slice(&result.app_metadata).expect("a PutAck"))
            }
            Err(err) => return (acks, Some(err)),
        }
    }
    (acks, None)
}

/// An ingest of `batches` with `cmd`.
async fn ingest(
    client: &mut FlightSqlServiceClient<Channel>,
    cmd: CommandStatementIngest,
    batches: Vec<RecordBatch>,
) -> Result<i64, FlightError> {
    client
        .execute_ingest(cmd, futures::stream::iter(batches.into_iter().map(Ok)))
        .await
}

fn ingest_cmd(
    table: &str,
    options: Option<(TableNotExistOption, TableExistsOption)>,
) -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: options.map(|(if_not_exist, if_exists)| TableDefinitionOptions {
            if_not_exist: if_not_exist as i32,
            if_exists: if_exists as i32,
        }),
        table: table.to_string(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
    }
}

fn code(err: &FlightError) -> Code {
    match err {
        FlightError::Tonic(status) => status.code(),
        other => panic!("expected a status, got {other:?}"),
    }
}

fn message(err: &FlightError) -> String {
    match err {
        FlightError::Tonic(status) => status.message().to_string(),
        other => panic!("expected a status, got {other:?}"),
    }
}

async fn count(api: &Native, ns: &str, table: &str) -> i64 {
    let mut client = flight_sql(api, ns).await;
    let info = client
        .execute(format!("SELECT count(*) AS c FROM {table}"), None)
        .await
        .expect("count");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .expect("do_get")
        .try_collect()
        .await
        .expect("rows");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64")
        .value(0)
}

// ----- data -----

/// `body` (text), vector `v` (dim 4) and sparse vector `s`.
fn kb_schema(dynamic: &str) -> Value {
    json!({
        "fields": [
            {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false}
        ],
        "vectors": [{"name": "v", "dim": 4, "distance": "cosine"}],
        "sparse_vectors": [{"name": "s", "modifier": "none"}],
        "dynamic": dynamic,
        "max_fields": 1000
    })
}

/// Creates `kb` in `ns` (2 partitions); returns its id.
async fn create_kb(api: &Native, ns: &str, dynamic: &str) -> CollectionId {
    let body = api
        .post(
            &format!("/v1/namespaces/{ns}/collections"),
            json!({"name": "kb", "schema": kb_schema(dynamic), "partitions": 2}),
        )
        .await
        .expect(StatusCode::CREATED);
    CollectionId::from_str(&body["id"].to_string()).expect("id")
}

fn sparse_column(rows: impl Iterator<Item = (u32, f32)>) -> ArrayRef {
    let mut indices = ListBuilder::new(UInt32Builder::new());
    let mut values = ListBuilder::new(Float32Builder::new());
    for (index, value) in rows {
        indices.values().append_value(index);
        indices.append(true);
        values.values().append_value(value);
        values.append(true);
    }
    let indices: ListArray = indices.finish();
    let values: ListArray = values.finish();
    Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("indices", indices.data_type().clone(), true)),
            Arc::new(indices) as ArrayRef,
        ),
        (
            Arc::new(Field::new("values", values.data_type().clone(), true)),
            Arc::new(values) as ArrayRef,
        ),
    ]))
}

/// Rows `ids` of `kb`: `body` "row {i} {word}", `v` as a `List<Float32>`
/// (row `bad_row` with 3 elements instead of 4) and `s`.
fn kb_batch(ids: std::ops::Range<u64>, word: &str, bad_row: Option<u64>) -> RecordBatch {
    let bodies: Vec<String> = ids.clone().map(|i| format!("row {i} {word}")).collect();
    let vectors = ListArray::from_iter_primitive::<Float32Type, _, _>(ids.clone().map(|i| {
        let dim = if Some(i) == bad_row { 3 } else { 4 };
        Some(
            (0..dim)
                .map(|j| Some(if j == 0 { 1.0 } else { i as f32 }))
                .collect::<Vec<_>>(),
        )
    }));
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("_id", Arc::new(UInt64Array::from_iter_values(ids.clone()))),
        ("body", Arc::new(StringArray::from(bodies))),
        ("v", Arc::new(vectors)),
        ("s", sparse_column(ids.map(|i| ((i % 10) as u32, 1.0)))),
    ];
    RecordBatch::try_from_iter(columns).expect("batch")
}

fn token(ack: &PutAck) -> ConsistencyToken {
    ConsistencyToken::from_str(&ack.token).expect("a token")
}

/// Whether `later` covers every offset of `earlier`.
fn covers(later: &ConsistencyToken, earlier: &ConsistencyToken) -> bool {
    earlier
        .0
        .iter()
        .all(|&(s, p, o)| later.offset(s, p).is_some_and(|l| l >= o))
}

/// The documents of `ids` (`null` for a missing one).
async fn get(api: &Native, ns: &str, ids: Vec<u64>, token: Option<&str>) -> Vec<Value> {
    let headers: Vec<(&str, &str)> = token.map(|t| (TOKEN, t)).into_iter().collect();
    let body = api
        .post_with(
            &format!("/v1/namespaces/{ns}/collections/kb/documents/get"),
            &headers,
            json!({"ids": ids}),
        )
        .await
        .expect(StatusCode::OK);
    body["documents"].as_array().expect("documents").clone()
}

/// The primary keys of a text query for `word` on `field`.
async fn search(api: &Native, ns: &str, collection: &str, field: &str, word: &str) -> Vec<Value> {
    let body = api
        .post(
            &format!("/v1/namespaces/{ns}/query"),
            json!({"collection": collection, "retrievers": [
                {"text": {"query": {"match": {"field": field, "text": word, "operator": "or",
                          "minimum_should_match": null, "fuzziness": null, "analyzer": null}}, "k": 10}}
            ], "limit": 10}),
        )
        .await
        .expect(StatusCode::OK);
    body["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["pk"].clone())
        .collect()
}

/// The high watermark of every partition of stream `name`.
async fn high_watermarks(api: &Native, ns: &str, name: &str) -> Vec<u64> {
    let body = api
        .get(&format!("/v1/namespaces/{ns}/streams/{name}"))
        .await
        .expect(StatusCode::OK);
    body["partitions"]
        .as_array()
        .expect("partitions")
        .iter()
        .map(|p| p["high_watermark"].as_u64().expect("hw"))
        .collect()
}

// ----- collections -----

#[tokio::test]
async fn do_put_into_a_collection_is_searchable_at_once() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    let (acks, err) = put(
        &mut client,
        &["collections", "kb"],
        vec![
            kb_batch(0..1000, "apple", None),
            kb_batch(1000..2000, "apple", None),
            kb_batch(2000..2500, "zebra", None),
        ],
        None,
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(
        acks.iter().map(|a| (a.batch, a.rows)).collect::<Vec<_>>(),
        [(0, 1000), (1, 1000), (2, 500)]
    );
    assert!(acks.iter().all(|a| a.offsets.is_empty()));
    for pair in acks.windows(2) {
        assert!(covers(&token(&pair[1]), &token(&pair[0])), "{pair:?}");
    }
    // No token: strong reads see the put.
    let hits = search(&api, "w", "kb", "body", "zebra").await;
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|pk| (2000..2500).contains(&pk.as_u64().unwrap()))
    );
    assert_eq!(count(&api, "w", "kb").await, 2500);
    api.shutdown().await;
}

#[tokio::test]
async fn the_last_put_token_serves_at_least_reads() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    let (acks, err) = put(
        &mut client,
        &["collections", "kb"],
        vec![
            kb_batch(0..1000, "a", None),
            kb_batch(1000..2000, "b", None),
            kb_batch(2000..2500, "c", None),
        ],
        None,
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    let last = acks.last().expect("acks").token.clone();
    let docs = get(&api, "w", (0..2500).collect(), Some(&last)).await;
    assert_eq!(docs.len(), 2500);
    assert!(docs.iter().all(|doc| !doc.is_null()));
    api.shutdown().await;
}

#[tokio::test]
async fn a_bad_batch_fails_after_earlier_batches_are_acknowledged() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    let (acks, err) = put(
        &mut client,
        &["collections", "kb"],
        vec![
            kb_batch(0..1000, "a", None),
            kb_batch(1000..2000, "b", Some(1007)),
            kb_batch(2000..2500, "c", None),
        ],
        None,
    )
    .await;
    assert_eq!(acks.len(), 1);
    let err = err.expect("the put fails");
    assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    let text = message(&err);
    assert!(text.contains("batch 1 row 7"), "{text}");
    assert!(
        text.ends_with("(1000 rows were written before the failure)"),
        "{text}"
    );
    let docs = get(&api, "w", (0..1000).collect(), None).await;
    assert!(docs.iter().all(|doc| !doc.is_null()));
    let docs = get(&api, "w", (1000..2500).collect(), None).await;
    assert!(docs.iter().all(Value::is_null));
    api.shutdown().await;
}

#[tokio::test]
async fn a_bad_row_fails_only_its_chunk() {
    // Chunks of 400 rows: rows 0..800 are written, the chunk of row 900 is
    // not.
    let api = Native::start_with(|config| {
        config.flight_sql = Some(([127, 0, 0, 1], 0).into());
        config.flight.put_chunk_rows = 400;
    })
    .await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    let (acks, err) = put(
        &mut client,
        &["collections", "kb"],
        vec![kb_batch(0..1000, "a", Some(900))],
        None,
    )
    .await;
    assert!(acks.is_empty());
    let text = message(&err.expect("the put fails"));
    assert!(text.contains("batch 0 row 900"), "{text}");
    assert!(
        text.ends_with("(800 rows were written before the failure)"),
        "{text}"
    );
    let docs = get(&api, "w", (0..1000).collect(), None).await;
    assert!(docs[..800].iter().all(|doc| !doc.is_null()));
    assert!(docs[800..].iter().all(Value::is_null));
    api.shutdown().await;
}

#[tokio::test]
async fn a_bad_schema_writes_nothing() {
    let api = Native::start_flight().await;
    let cid = create_kb(&api, "w", "ignore").await;
    let stream = implicit_name("kb", cid);
    let before = high_watermarks(&api, "w", &stream).await;
    let mut client = flight(&api, "w").await;
    let no_id = RecordBatch::try_from_iter(vec![(
        "body",
        Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
    )])
    .expect("batch");
    let (acks, err) = put(&mut client, &["collections", "kb"], vec![no_id], None).await;
    assert!(acks.is_empty());
    let err = err.expect("refused");
    assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    assert!(message(&err).contains("_id"), "{err}");
    assert_eq!(high_watermarks(&api, "w", &stream).await, before);
    // An unknown path, and a missing collection.
    let (_, err) = put(
        &mut client,
        &["tables", "kb"],
        vec![kb_batch(0..1, "a", None)],
        None,
    )
    .await;
    assert_eq!(code(&err.expect("refused")), Code::InvalidArgument);
    let (_, err) = put(
        &mut client,
        &["collections", "nope"],
        vec![kb_batch(0..1, "a", None)],
        None,
    )
    .await;
    assert_eq!(code(&err.expect("refused")), Code::NotFound);
    assert_eq!(high_watermarks(&api, "w", &stream).await, before);
    api.shutdown().await;
}

#[tokio::test]
async fn flight_sql_ingest_appends_to_a_collection() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "ignore").await;
    create_kb(&api, "other", "ignore").await;
    let mut client = flight_sql(&api, "w").await;
    let append = Some((TableNotExistOption::Fail, TableExistsOption::Append));
    let rows = ingest(
        &mut client,
        ingest_cmd("kb", append),
        vec![
            kb_batch(0..200, "apple", None),
            kb_batch(200..300, "zebra", None),
        ],
    )
    .await
    .expect("ingest");
    assert_eq!(rows, 300);
    let hits = search(&api, "w", "kb", "body", "zebra").await;
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|pk| (200..300).contains(&pk.as_u64().unwrap()))
    );
    assert_eq!(count(&api, "w", "kb").await, 300);
    // `catalog` names the namespace.
    let cmd = CommandStatementIngest {
        catalog: Some("other".to_string()),
        ..ingest_cmd("kb", append)
    };
    let rows = ingest(&mut client, cmd, vec![kb_batch(0..50, "x", None)])
        .await
        .expect("ingest");
    assert_eq!(rows, 50);
    assert_eq!(count(&api, "other", "kb").await, 50);
    assert_eq!(count(&api, "w", "kb").await, 300);
    api.shutdown().await;
}

/// `_id`, `title` (a string) and `emb` (`FixedSizeList<Float32, 4>`).
fn fresh_batch(ids: std::ops::Range<u64>) -> RecordBatch {
    let titles: Vec<String> = ids.clone().map(|i| format!("hello{i}")).collect();
    let emb = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        ids.clone()
            .map(|i| Some(vec![Some(1.0), Some(i as f32), Some(0.0), Some(0.0)])),
        4,
    );
    RecordBatch::try_from_iter(vec![
        (
            "_id",
            Arc::new(UInt64Array::from_iter_values(ids)) as ArrayRef,
        ),
        ("title", Arc::new(StringArray::from(titles)) as ArrayRef),
        ("emb", Arc::new(emb) as ArrayRef),
    ])
    .expect("batch")
}

#[tokio::test]
async fn ingest_table_options_follow_the_table() {
    use TableExistsOption as E;
    use TableNotExistOption as N;
    let api = Native::start_flight().await;
    let mut client = flight_sql(&api, "w").await;
    // ADBC `create` on a missing collection creates it and loads it.
    let create = Some((N::Create, E::Fail));
    let rows = ingest(
        &mut client,
        ingest_cmd("fresh", create),
        vec![fresh_batch(0..5)],
    )
    .await
    .expect("create");
    assert_eq!(rows, 5);
    let info = api
        .get("/v1/namespaces/w/collections/fresh")
        .await
        .expect(StatusCode::OK);
    let schema = &info["schema"];
    assert_eq!(schema["vectors"][0]["name"], "emb", "{schema}");
    assert_eq!(schema["vectors"][0]["dim"], 4);
    assert_eq!(schema["vectors"][0]["distance"], "cosine");
    assert_eq!(schema["dynamic"], "map");
    let names: BTreeSet<String> = schema["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .map(|f| f["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains("title"), "{names:?}");
    assert_eq!(
        search(&api, "w", "fresh", "title", "hello3").await,
        [json!(3)]
    );
    // ... and answers AlreadyExists on the existing one.
    let err = ingest(
        &mut client,
        ingest_cmd("fresh", create),
        vec![fresh_batch(5..6)],
    )
    .await
    .expect_err("exists");
    assert_eq!(code(&err), Code::AlreadyExists, "{err}");
    assert_eq!(count(&api, "w", "fresh").await, 5);
    // `create_append` loads a missing and an existing collection.
    let create_append = Some((N::Create, E::Append));
    for ids in [0..3, 3..7] {
        ingest(
            &mut client,
            ingest_cmd("both", create_append),
            vec![fresh_batch(ids)],
        )
        .await
        .expect("create_append");
    }
    assert_eq!(count(&api, "w", "both").await, 7);
    // `append` on a missing collection.
    let err = ingest(
        &mut client,
        ingest_cmd("nope", Some((N::Fail, E::Append))),
        vec![fresh_batch(0..1)],
    )
    .await
    .expect_err("missing");
    assert_eq!(code(&err), Code::NotFound, "{err}");
    // `replace`, `temporary` and `transaction_id`.
    for cmd in [
        ingest_cmd("fresh", Some((N::Create, E::Replace))),
        CommandStatementIngest {
            temporary: true,
            ..ingest_cmd("fresh", create_append)
        },
        CommandStatementIngest {
            transaction_id: Some(bytes::Bytes::from_static(b"t")),
            ..ingest_cmd("fresh", create_append)
        },
    ] {
        let err = ingest(&mut client, cmd, vec![fresh_batch(0..1)])
            .await
            .expect_err("refused");
        assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    }
    // `create` of a missing stream.
    let cmd = CommandStatementIngest {
        schema: Some("streams".to_string()),
        ..ingest_cmd("events", create)
    };
    let err = ingest(&mut client, cmd, vec![stream_batch(&[b"k"], None)])
        .await
        .expect_err("refused");
    assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    assert!(message(&err).contains("create streams through the native API"));
    api.shutdown().await;
}

#[tokio::test]
async fn dynamic_mapping_applies_to_do_put() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "map").await;
    let mut client = flight(&api, "w").await;
    let batch = RecordBatch::try_from_iter(vec![
        ("_id", Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef),
        (
            "color",
            Arc::new(StringArray::from(vec!["red", "blue"])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let (acks, err) = put(&mut client, &["collections", "kb"], vec![batch], None).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(acks.len(), 1);
    let info = api
        .get("/v1/namespaces/w/collections/kb")
        .await
        .expect(StatusCode::OK);
    assert!(
        info["schema"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["name"] == "color"),
        "{info}"
    );
    let body = api
        .post(
            "/v1/namespaces/w/query",
            json!({"collection": "kb", "retrievers": [], "fusion": null,
                   "filter": {"term": {"field": "color", "value": "red"}}, "limit": 10}),
        )
        .await
        .expect(StatusCode::OK);
    let pks: Vec<Value> = body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["pk"].clone())
        .collect();
    assert_eq!(pks, [json!(1)], "{body}");
    api.shutdown().await;
}

// ----- streams -----

/// `key` (binary) and `value` (utf8) rows, with a `partition` column when
/// given.
fn stream_batch(keys: &[&[u8]], partitions: Option<Vec<u32>>) -> RecordBatch {
    let values: Vec<String> = (0..keys.len()).map(|i| format!("v{i}")).collect();
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        (
            "key",
            Arc::new(arrow_array::BinaryArray::from(keys.to_vec())) as ArrayRef,
        ),
        ("value", Arc::new(StringArray::from(values)) as ArrayRef),
    ];
    if let Some(partitions) = partitions {
        columns.push((
            "partition",
            Arc::new(UInt32Array::from(partitions)) as ArrayRef,
        ));
    }
    RecordBatch::try_from_iter(columns).expect("batch")
}

async fn create_stream(api: &Native, ns: &str, name: &str, partitions: u32) {
    let reply = api.post("/v1/namespaces", json!({"name": ns})).await;
    assert!(
        reply.status == StatusCode::CREATED || reply.status == StatusCode::CONFLICT,
        "{:?}",
        reply.body
    );
    api.post(
        &format!("/v1/namespaces/{ns}/streams"),
        json!({"name": name, "partitions": partitions}),
    )
    .await
    .expect(StatusCode::CREATED);
}

/// The records of partition `p` of `ns/name` from offset 0.
async fn records(api: &Native, ns: &str, name: &str, p: u32) -> Vec<Value> {
    let body = api
        .get(&format!(
            "/v1/namespaces/{ns}/streams/{name}/partitions/{p}/records?offset=0"
        ))
        .await
        .expect(StatusCode::OK);
    body["records"].as_array().expect("records").clone()
}

#[tokio::test]
async fn do_put_into_a_stream_appends_records() {
    let api = Native::start_flight().await;
    create_stream(&api, "w", "events", 2).await;
    let mut client = flight(&api, "w").await;
    // key, value, headers, timestamp and partition.
    let mut headers = ListBuilder::new(StructBuilder::from_fields(
        vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Binary, true),
        ],
        0,
    ));
    for row in 0..4 {
        let entries = headers.values();
        entries
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value(format!("h{row}"));
        entries
            .field_builder::<BinaryBuilder>(1)
            .unwrap()
            .append_value(format!("x{row}"));
        entries.append(true);
        headers.append(true);
    }
    let batch = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(arrow_array::BinaryArray::from(vec![
                b"1".as_slice(),
                b"2",
                b"3",
                b"4",
            ])) as ArrayRef,
        ),
        ("headers", Arc::new(headers.finish()) as ArrayRef),
        (
            "timestamp",
            Arc::new(TimestampMillisecondArray::from(vec![
                1_000, 2_000, 3_000, 4_000,
            ])) as ArrayRef,
        ),
        (
            "partition",
            Arc::new(UInt32Array::from(vec![1, 0, 1, 1])) as ArrayRef,
        ),
    ])
    .expect("batch");
    let (acks, err) = put(&mut client, &["streams", "events"], vec![batch], None).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(acks.len(), 1);
    let offsets: Vec<(u32, u64, u64)> = acks[0]
        .offsets
        .iter()
        .map(|o| (o.partition, o.base_offset, o.last_offset))
        .collect();
    assert_eq!(offsets, [(0, 0, 0), (1, 0, 2)]);
    token(&acks[0]);
    let b64 = |s: &str| json!(BASE64.encode(s));
    let one = records(&api, "w", "events", 1).await;
    let got: Vec<(Value, Value, Value, Value)> = one
        .iter()
        .map(|r| {
            (
                r["key"].clone(),
                r["value"].clone(),
                r["headers"].clone(),
                r["timestamp_ms"].clone(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [
            (
                b64("a"),
                b64("1"),
                json!([{"key": "h0", "value": BASE64.encode("x0")}]),
                json!(1000)
            ),
            (
                b64("c"),
                b64("3"),
                json!([{"key": "h2", "value": BASE64.encode("x2")}]),
                json!(3000)
            ),
            (
                b64("d"),
                b64("4"),
                json!([{"key": "h3", "value": BASE64.encode("x3")}]),
                json!(4000)
            ),
        ]
    );
    let zero = records(&api, "w", "events", 0).await;
    assert_eq!(zero.len(), 1);
    assert_eq!(zero[0]["key"], b64("b"));
    // An ingest into `streams` without a partition column goes by key hash
    // (on canonical keys, exactly `partition_of`).
    let pks: Vec<PrimaryKey> = (10..20).map(PrimaryKey::U64).collect();
    let keys: Vec<Vec<u8>> = pks.iter().map(PrimaryKey::canonical).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let mut sql = flight_sql(&api, "w").await;
    let cmd = CommandStatementIngest {
        schema: Some("streams".to_string()),
        ..ingest_cmd("events", None)
    };
    let rows = ingest(&mut sql, cmd, vec![stream_batch(&key_refs, None)])
        .await
        .expect("ingest");
    assert_eq!(rows, 10);
    for p in 0..2 {
        let got: Vec<Value> = records(&api, "w", "events", p)
            .await
            .into_iter()
            .skip(if p == 0 { 1 } else { 3 })
            .map(|r| r["key"].clone())
            .collect();
        let expected: Vec<Value> = pks
            .iter()
            .filter(|pk| partition_of(pk, 2) == p)
            .map(|pk| json!(BASE64.encode(pk.canonical())))
            .collect();
        assert_eq!(got, expected, "partition {p}");
    }
    api.shutdown().await;
}

#[tokio::test]
async fn do_put_onto_an_implicit_stream_checks_partitions() {
    let api = Native::start_flight().await;
    let cid = create_kb(&api, "w", "ignore").await;
    let stream = implicit_name("kb", cid);
    let before = high_watermarks(&api, "w", &stream).await;
    let mut client = flight(&api, "w").await;
    let pk = PrimaryKey::U64(42);
    let wrong = 1 - partition_of(&pk, 2);
    let right_pk = PrimaryKey::U64(43);
    let right = partition_of(&right_pk, 2);
    let keys = [right_pk.canonical(), pk.canonical()];
    let batch = stream_batch(
        &[keys[0].as_slice(), keys[1].as_slice()],
        Some(vec![right, wrong]),
    );
    let (acks, err) = put(&mut client, &["streams", &stream], vec![batch], None).await;
    assert!(acks.is_empty());
    let err = err.expect("refused");
    assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    assert!(message(&err).contains("batch 0 row 1"), "{err}");
    assert_eq!(high_watermarks(&api, "w", &stream).await, before);
    api.shutdown().await;
}

#[tokio::test]
async fn a_message_over_the_limit_is_refused() {
    let api = Native::start_with(|config| {
        config.flight_sql = Some(([127, 0, 0, 1], 0).into());
        config.flight.max_message_bytes = 1 << 20;
    })
    .await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    // 2 MiB of bodies in one message.
    let big = "x".repeat(2 << 20);
    let batch = RecordBatch::try_from_iter(vec![
        ("_id", Arc::new(UInt64Array::from(vec![1])) as ArrayRef),
        ("body", Arc::new(StringArray::from(vec![big])) as ArrayRef),
    ])
    .expect("batch");
    let (acks, err) = put(
        &mut client,
        &["collections", "kb"],
        vec![batch],
        Some(usize::MAX),
    )
    .await;
    assert!(acks.is_empty());
    assert!(err.is_some());
    assert_eq!(count(&api, "w", "kb").await, 0);
    api.shutdown().await;
}

#[tokio::test]
async fn sql_info_declares_bulk_ingestion() {
    let api = Native::start_flight().await;
    let mut client = flight_sql(&api, "w").await;
    let info = client
        .get_sql_info(vec![
            SqlInfo::FlightSqlServerBulkIngestion,
            SqlInfo::FlightSqlServerIngestTransactionsSupported,
            SqlInfo::FlightSqlServerReadOnly,
        ])
        .await
        .expect("sql info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .expect("do_get")
        .try_collect()
        .await
        .expect("rows");
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("names");
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("values");
    let flag = |info: SqlInfo| {
        let row = (0..names.len())
            .find(|&r| names.value(r) == info as u32)
            .expect("declared");
        values
            .value(row)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("a bool")
            .value(0)
    };
    assert!(flag(SqlInfo::FlightSqlServerBulkIngestion));
    assert!(!flag(SqlInfo::FlightSqlServerIngestTransactionsSupported));
    assert!(flag(SqlInfo::FlightSqlServerReadOnly));
    api.shutdown().await;
}

#[tokio::test]
async fn a_stalled_put_does_not_hang_shutdown() {
    let api = Native::start_flight().await;
    create_kb(&api, "w", "ignore").await;
    let mut client = flight(&api, "w").await;
    // One batch, then a client that never sends another message nor ends
    // its stream.
    let descriptor = FlightDescriptor::new_path(vec!["collections".into(), "kb".into()]);
    let first = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .build(futures::stream::iter([Ok(kb_batch(0..10, "apple", None))]));
    let data = first.chain(futures::stream::pending());
    let mut responses = client.do_put(data).await.expect("the put starts");
    let ack = responses
        .next()
        .await
        .expect("an acknowledgement")
        .expect("the first batch is written");
    let ack: PutAck = serde_json::from_slice(&ack.app_metadata).expect("a PutAck");
    assert_eq!((ack.batch, ack.rows), (0, 10));

    // The put waits for its next message; shutdown stops it at once instead
    // of waiting for the client (the #25 review): well within the 10 s grace
    // after which the server would abort the calls in flight.
    tokio::time::timeout(std::time::Duration::from_secs(8), api.shutdown())
        .await
        .expect("shutdown does not wait for the stalled put");
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(response) = responses.next().await {
            if response.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the put's answer stream ends");
}
