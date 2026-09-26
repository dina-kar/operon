//! Arrow Flight SQL over an in-process server (plan M1.2 Task 12).

use std::collections::BTreeSet;
use std::str::FromStr;
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{Any, CommandGetDbSchemas, CommandGetTables, TicketStatementQuery};
use arrow_flight::{FlightInfo, IpcMessage};
use arrow_schema::Schema;
use futures::TryStreamExt;
use operon_collection::ConsistencyToken;
use operon_query::flight::{NAMESPACE_METADATA, decode_ticket};
use prost::Message;
use reqwest::StatusCode;
use serde_json::json;
use tonic::Code;
use tonic::transport::Channel;

use crate::common::{Native, TOKEN, kb, kb_schema};

/// A Flight SQL client of `api` in namespace `ns` (none: the default).
async fn client(api: &Native, ns: Option<&str>) -> FlightSqlServiceClient<Channel> {
    let addr = api.server.flight_sql_addr().expect("flight sql listens");
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect");
    let mut client = FlightSqlServiceClient::new(channel);
    if let Some(ns) = ns {
        client.set_header(NAMESPACE_METADATA, ns);
    }
    client
}

/// Every batch of every endpoint of `info`.
async fn fetch(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<Vec<RecordBatch>, FlightError> {
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.expect("a ticket");
        let stream = client.do_get(ticket).await?;
        batches.extend(stream.try_collect::<Vec<_>>().await?);
    }
    Ok(batches)
}

/// Runs `sql` and collects its batches.
async fn query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, FlightError> {
    let info = client.execute(sql.to_string(), None).await?;
    fetch(client, info).await
}

fn code(err: &FlightError) -> Code {
    match err {
        FlightError::Tonic(status) => status.code(),
        other => panic!("expected a status, got {other:?}"),
    }
}

/// Column `name` of `batches` as strings (`None` for nulls).
fn strings(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).expect("column");
            let column = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("a string column");
            (0..column.len())
                .map(|i| (!column.is_null(i)).then(|| column.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Column `name` of `batches` as i64s (`None` for nulls).
fn i64s(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).expect("column");
            let column = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column");
            (0..column.len())
                .map(|i| (!column.is_null(i)).then(|| column.value(i)))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn count(client: &mut FlightSqlServiceClient<Channel>, table: &str) -> i64 {
    let batches = query(client, &format!("SELECT count(*) AS c FROM {table}"))
        .await
        .expect("count");
    i64s(&batches, "c")[0].expect("a count")
}

#[tokio::test]
async fn a_statement_returns_arrow_batches() {
    let api = Native::start_flight().await;
    kb(&api, "w").await;
    let mut client = client(&api, Some("w")).await;
    let info = client
        .execute("SELECT _id, n FROM kb ORDER BY n".to_string(), None)
        .await
        .expect("flight info");
    assert_eq!(info.total_records, -1);
    assert_eq!(info.endpoint.len(), 1);
    assert!(info.endpoint[0].location.is_empty(), "no location");
    let schema = Schema::try_from(info.clone()).expect("schema");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["_id", "n"]);
    let batches = fetch(&mut client, info).await.expect("rows");
    // Strong: the documents written just before are read (from the tail).
    let ids = strings(&batches, "_id");
    let ns = i64s(&batches, "n");
    assert_eq!(ns, [Some(1), Some(2), Some(3), None, None, None]);
    assert_eq!(
        ids[..3],
        [
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string())
        ]
    );
    let rest: BTreeSet<String> = ids[3..].iter().flatten().cloned().collect();
    assert_eq!(
        rest,
        BTreeSet::from([
            "18446744073709551615".to_string(),
            "k-str".to_string(),
            crate::common::UUID.to_string(),
        ])
    );
    api.shutdown().await;
}

#[tokio::test]
async fn a_collection_created_just_before_is_queryable() {
    // Carry of Task 10: the search table functions plan against the catalog
    // cache, which Flight SQL refreshes before planning.
    let api = Native::start_flight().await;
    // Without its refresh task the cache learns of `fresh` only from the
    // refresh before planning.
    api.server.collections().catalog().stop();
    let mut client = client(&api, Some("w")).await;
    api.post(
        "/v1/namespaces/w/collections",
        json!({"name": "fresh", "schema": kb_schema(), "partitions": 1}),
    )
    .await
    .expect(StatusCode::CREATED);
    api.post(
        "/v1/namespaces/w/collections/fresh/documents",
        json!({"ops": [
            {"upsert": {"id": 1, "source": {"body": "refund policy", "n": 1}}},
            {"upsert": {"id": 2, "source": {"body": "shipping times", "n": 2}}}
        ]}),
    )
    .await
    .expect(StatusCode::OK);
    let batches = query(
        &mut client,
        "SELECT _id FROM text_search('fresh', 'refund', 'body', 10)",
    )
    .await
    .expect("text_search");
    assert_eq!(strings(&batches, "_id"), [Some("1".to_string())]);
    assert_eq!(count(&mut client, "fresh").await, 2);
    api.shutdown().await;
}

#[tokio::test]
async fn the_namespace_comes_from_metadata() {
    let api = Native::start_flight().await;
    kb(&api, "a").await;
    api.post(
        "/v1/namespaces/b/collections",
        json!({"name": "kb", "schema": kb_schema(), "partitions": 1}),
    )
    .await
    .expect(StatusCode::CREATED);
    api.post(
        "/v1/namespaces/b/collections/kb/documents",
        json!({"ops": [
            {"upsert": {"id": 1, "source": {"n": 1}}},
            {"upsert": {"id": 2, "source": {"n": 2}}}
        ]}),
    )
    .await
    .expect(StatusCode::OK);
    assert_eq!(count(&mut client(&api, Some("a")).await, "kb").await, 6);
    assert_eq!(count(&mut client(&api, Some("b")).await, "kb").await, 2);
    // Without the metadata the namespace is `default`, which has no kb.
    let err = query(&mut client(&api, None).await, "SELECT count(*) FROM kb")
        .await
        .expect_err("no kb in default");
    assert_eq!(code(&err), Code::InvalidArgument, "{err}");
    api.shutdown().await;
}

#[tokio::test]
async fn a_consistency_token_in_metadata_is_honoured() {
    let api = Native::start_with(|config| {
        config.flight_sql = Some(([127, 0, 0, 1], 0).into());
        config.query.read.consistency_wait = Duration::from_millis(500);
    })
    .await;
    let token = kb(&api, "w").await;
    // The token travels in the statement ticket.
    let mut with_token = client(&api, Some("w")).await;
    with_token.set_header(TOKEN, &token);
    let info = with_token
        .execute("SELECT count(*) AS c FROM kb".to_string(), None)
        .await
        .expect("flight info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let any = Any::decode(ticket.ticket.clone()).expect("any");
    let statement: TicketStatementQuery = any.unpack().expect("unpack").expect("a statement");
    let decoded = decode_ticket(&statement.statement_handle).expect("our ticket");
    assert_eq!(decoded.namespace, "w");
    assert_eq!(decoded.token.as_deref(), Some(token.as_str()));
    let batches = fetch(&mut with_token, info).await.expect("rows");
    assert_eq!(i64s(&batches, "c"), [Some(6)]);
    // A token past everything written waits, then times out: AtLeast.
    let mut ahead = ConsistencyToken::from_str(&token).expect("token");
    for item in &mut ahead.0 {
        item.2 += 1_000;
    }
    let mut waiting = client(&api, Some("w")).await;
    waiting.set_header(TOKEN, ahead.to_string());
    let err = query(&mut waiting, "SELECT count(*) FROM kb")
        .await
        .expect_err("times out");
    assert_eq!(code(&err), Code::DeadlineExceeded, "{err}");
    // A token that does not parse reads Strong.
    let mut garbled = client(&api, Some("w")).await;
    garbled.set_header(TOKEN, "c1:nope");
    assert_eq!(count(&mut garbled, "kb").await, 6);
    api.shutdown().await;
}

#[tokio::test]
async fn catalogs_schemas_and_tables_are_listed() {
    let api = Native::start_flight().await;
    kb(&api, "a").await;
    kb(&api, "b").await;
    api.post(
        "/v1/namespaces/b/collections",
        json!({"name": "notes", "schema": kb_schema(), "partitions": 1}),
    )
    .await
    .expect(StatusCode::CREATED);
    let mut client = client(&api, None).await;
    // Catalogs: the namespaces, sorted.
    let info = client.get_catalogs().await.expect("catalogs");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert_eq!(
        strings(&batches, "catalog_name"),
        [Some("a".to_string()), Some("b".to_string())]
    );
    // Schemas: `collections` per namespace, filtered by catalog and pattern.
    let schemas = |catalog: Option<&str>, pattern: Option<&str>| CommandGetDbSchemas {
        catalog: catalog.map(str::to_string),
        db_schema_filter_pattern: pattern.map(str::to_string),
    };
    let info = client
        .get_db_schemas(schemas(None, None))
        .await
        .expect("schemas");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert_eq!(
        strings(&batches, "catalog_name"),
        [Some("a".to_string()), Some("b".to_string())]
    );
    assert_eq!(
        strings(&batches, "db_schema_name"),
        [
            Some("collections".to_string()),
            Some("collections".to_string())
        ]
    );
    let info = client
        .get_db_schemas(schemas(Some("b"), Some("coll%")))
        .await
        .expect("schemas");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert_eq!(strings(&batches, "catalog_name"), [Some("b".to_string())]);
    let info = client
        .get_db_schemas(schemas(None, Some("x%")))
        .await
        .expect("schemas");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert!(strings(&batches, "catalog_name").is_empty());
    // Tables: every collection.
    let tables =
        |catalog: Option<&str>, table: Option<&str>, include_schema: bool| CommandGetTables {
            catalog: catalog.map(str::to_string),
            db_schema_filter_pattern: None,
            table_name_filter_pattern: table.map(str::to_string),
            table_types: vec![],
            include_schema,
        };
    let info = client
        .get_tables(tables(None, None, false))
        .await
        .expect("tables");
    let batches = fetch(&mut client, info).await.expect("rows");
    // `catalog.schema.table:type` per row.
    let rows: Vec<String> = {
        let columns = ["catalog_name", "db_schema_name", "table_name", "table_type"]
            .map(|name| strings(&batches, name));
        (0..columns[2].len())
            .map(|i| {
                let [c, s, t, k] = [0, 1, 2, 3].map(|j| columns[j][i].clone().expect("set"));
                format!("{c}.{s}.{t}:{k}")
            })
            .collect()
    };
    assert_eq!(
        rows,
        [
            "a.collections.kb:TABLE",
            "b.collections.kb:TABLE",
            "b.collections.notes:TABLE"
        ]
    );
    assert!(batches[0].column_by_name("table_schema").is_none());
    // With `include_schema`: the collection's Arrow schema (Task 10 rule 2).
    let info = client
        .get_tables(tables(Some("a"), Some("kb"), true))
        .await
        .expect("tables");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let column = batches[0]
        .column_by_name("table_schema")
        .expect("table_schema");
    let bytes = column
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .expect("binary")
        .value(0)
        .to_vec();
    let listed = Schema::try_from(IpcMessage(bytes.into())).expect("an IPC schema");
    let info = api
        .server
        .collections()
        .get_collection("a", "kb")
        .await
        .expect("kb");
    let expected = operon_query::sql::collection_arrow_schema(&info.schema);
    assert_eq!(&listed, expected.as_ref());
    // Table types.
    let info = client.get_table_types().await.expect("table types");
    let batches = fetch(&mut client, info).await.expect("rows");
    assert_eq!(strings(&batches, "table_type"), [Some("TABLE".to_string())]);
    api.shutdown().await;
}

#[tokio::test]
async fn ddl_over_flight_is_invalid_argument() {
    let api = Native::start_flight().await;
    kb(&api, "w").await;
    let mut client = client(&api, Some("w")).await;
    for statement in [
        "CREATE TABLE t (x INT)",
        "DROP TABLE kb",
        "INSERT INTO kb (n) VALUES (1)",
    ] {
        let err = client
            .execute(statement.to_string(), None)
            .await
            .expect_err(statement);
        assert_eq!(code(&err), Code::InvalidArgument, "{statement}: {err}");
    }
    // Prepared statements are not served.
    let err = client
        .prepare("SELECT 1".to_string(), None)
        .await
        .expect_err("no prepared statements");
    assert_eq!(code(&err), Code::Unimplemented, "{err}");
    // The table is untouched.
    assert_eq!(count(&mut client, "kb").await, 6);
    api.shutdown().await;
}
