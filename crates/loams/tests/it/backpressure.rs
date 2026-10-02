//! Write backpressure over the native API (plan M1.3 Task 15, D86): an
//! in-process server with small budgets and the link paused.

use std::time::Duration;

use loams::api::{BACKPRESSURE_HEADER, UNAPPLIED_BYTES_HEADER, UNAPPLIED_RECORDS_HEADER};
use loams::{Server, ServerConfig};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::common::{Native, Reply};

const DOCS: &str = "/v1/namespaces/bp/collections/docs/documents";

/// A server whose link never commits within a test (a small batch waits an
/// hour) and whose budget is `records` unapplied records, measured afresh
/// before every admission.
async fn paused(records: u64) -> Native {
    let api = Native::start_with(|config| {
        config.link.batch_interval = Duration::from_secs(3600);
        config.link.batch_records = 1_000_000;
        config.query.backpressure.max_unapplied_records = records;
        config.query.backpressure.refresh_interval = Duration::ZERO;
    })
    .await;
    api.post(
        "/v1/namespaces/bp/collections",
        json!({
            "name": "docs",
            "schema": {
                "fields": [{"name": "t", "source_path": "t", "kind": "keyword", "indexed": true, "fast": false}],
                "vectors": [],
                "sparse_vectors": [],
                "dynamic": "ignore",
                "max_fields": 1000
            },
            "partitions": 2
        }),
    )
    .await
    .expect(StatusCode::CREATED);
    api
}

fn ops(keys: std::ops::Range<u64>) -> Value {
    let ops: Vec<Value> = keys
        .map(|i| json!({"upsert": {"id": i, "source": {"t": format!("k{i}")}}}))
        .collect();
    json!({"ops": ops})
}

fn backlog(reply: &Reply) -> (u64, u64) {
    let number = |name: &str| -> u64 {
        reply
            .header(name)
            .unwrap_or_else(|| panic!("no {name} header"))
            .parse()
            .expect("a number")
    };
    (
        number(UNAPPLIED_RECORDS_HEADER),
        number(UNAPPLIED_BYTES_HEADER),
    )
}

#[tokio::test]
async fn a_throttled_write_gets_429_retry_after_and_backlog_headers() {
    let api = paused(3).await;
    api.post(DOCS, ops(0..3)).await.expect(StatusCode::OK);
    let reply = api.post(DOCS, ops(3..4)).await;
    assert_eq!(
        reply.status,
        StatusCode::TOO_MANY_REQUESTS,
        "{}",
        reply.body
    );
    let retry: u64 = reply
        .header("retry-after")
        .expect("Retry-After")
        .parse()
        .expect("an integer");
    assert!((1..=30).contains(&retry), "{retry}");
    assert_eq!(reply.body["error"], json!("resource_exhausted"));
    let ms = reply.body["retry_after_ms"]
        .as_u64()
        .expect("retry_after_ms");
    assert_eq!(ms.div_ceil(1000), retry);
    let (records, bytes) = backlog(&reply);
    assert_eq!(records, 3);
    assert!(bytes > 0);
    let info = api
        .get("/v1/namespaces/bp/collections/docs")
        .await
        .expect(StatusCode::OK);
    assert_eq!(info["backpressure"]["state"], json!("throttling"));
    assert_eq!(info["unapplied_bytes"], json!(bytes));
    api.shutdown().await;
}

#[tokio::test]
async fn every_write_response_carries_the_backlog_headers() {
    let api = paused(100).await;
    let first = api.post(DOCS, ops(0..4)).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(backlog(&first), (0, 0), "measured before its own records");
    let second = api.post(DOCS, ops(4..5)).await;
    assert_eq!(second.status, StatusCode::OK);
    let (records, bytes) = backlog(&second);
    assert_eq!(records, 4);
    assert!(bytes > 0);
    api.shutdown().await;
}

#[tokio::test]
async fn loams_backpressure_off_admits_a_bulk_write() {
    let api = paused(2).await;
    api.post(DOCS, ops(0..2)).await.expect(StatusCode::OK);
    assert_eq!(
        api.post(DOCS, ops(2..3)).await.status,
        StatusCode::TOO_MANY_REQUESTS
    );
    let off = [(BACKPRESSURE_HEADER, "off")];
    // Backlogs 2, 4 and 6 are under 4 × 2; 8 is not.
    for i in 0..3 {
        let reply = api.post_with(DOCS, &off, ops(10 + 2 * i..12 + 2 * i)).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        assert_eq!(backlog(&reply).0, 2 + 2 * i);
    }
    let reply = api.post_with(DOCS, &off, ops(20..22)).await;
    assert_eq!(reply.status, StatusCode::TOO_MANY_REQUESTS);
    api.shutdown().await;
}

#[tokio::test]
async fn an_unknown_backpressure_value_is_400() {
    let api = paused(2).await;
    let reply = api
        .post_with(DOCS, &[(BACKPRESSURE_HEADER, "maybe")], ops(0..1))
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.body["error"], json!("invalid_argument"));
    api.shutdown().await;
}

#[tokio::test]
async fn a_config_with_the_byte_budget_over_half_the_tail_is_refused() {
    let dir = TempDir::new().expect("temp dir");
    let mut bad = ServerConfig::new(dir.path());
    bad.listen = ([127, 0, 0, 1], 0).into();
    bad.query.tail.max_bytes = 1 << 20;
    bad.query.backpressure.max_unapplied_bytes = (1 << 19) + 1;
    let err = Server::start(bad.clone()).await.expect_err("refused");
    let loams::ServerError::Config(message) = &err else {
        panic!("expected a config error, got {err:?}");
    };
    assert!(message.contains("--max-unapplied-bytes"), "{message}");
    // Exactly half is fine.
    bad.query.backpressure.max_unapplied_bytes = 1 << 19;
    bad.validate().expect("half the tail");
    for edit in [
        (|c: &mut ServerConfig| c.query.backpressure.max_unapplied_records = 0)
            as fn(&mut ServerConfig),
        |c| c.query.backpressure.override_factor = 0,
        |c| c.query.backpressure.min_retry_after = Duration::from_secs(60),
    ] {
        let mut config = bad.clone();
        edit(&mut config);
        assert!(config.validate().is_err());
    }
}
