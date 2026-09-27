//! `search` end to end (plan M1.6 Task 8 rules 1–2), over the Task 1
//! scenario's `kb` collection.

use operon_mcp::McpConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::Mcp;

fn hit_ids(result: &Value) -> Vec<Value> {
    result["structuredContent"]["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("hits: {result}"))
        .iter()
        .map(|h| h["id"].clone())
        .collect()
}

async fn search(mcp: &Mcp, args: Value) -> Value {
    let mut args = args;
    args["collection"] = json!("kb");
    mcp.call(None, "search", args).await
}

fn assert_tool_error(result: &Value, code: &str) {
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(result["structuredContent"]["error"], code, "{result}");
}

#[tokio::test]
async fn search_by_text_finds_matching_documents() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = search(&mcp, json!({"query": "refund"})).await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(hit_ids(&result), [json!(1), json!(3)]);
    let out = &result["structuredContent"];
    assert!(
        out["read_token"].as_str().unwrap().starts_with("v1:"),
        "{out}"
    );
    assert_eq!(out["truncated"], false);
    assert_eq!(out["hits"][0]["source"]["body"], "refund policy");
    // Naming the text field gives the same hits; a non-text field is refused.
    let named = search(&mcp, json!({"query": "refund", "fields": ["body"]})).await;
    assert_eq!(hit_ids(&named), [json!(1), json!(3)]);
    let keyword = search(&mcp, json!({"query": "refund", "fields": ["tenant"]})).await;
    assert_tool_error(&keyword, "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_by_vector_ranks_by_similarity() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = search(&mcp, json!({"vector": [1.0, 0.0, 0.0], "limit": 2})).await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(hit_ids(&result), [json!(1), json!(2)]);
    let named = search(
        &mcp,
        json!({"vector": [1.0, 0.0, 0.0], "vector_field": "embedding", "limit": 2}),
    )
    .await;
    assert_eq!(hit_ids(&named), [json!(1), json!(2)]);
    let unknown = search(
        &mcp,
        json!({"vector": [1.0, 0.0, 0.0], "vector_field": "nope"}),
    )
    .await;
    assert_tool_error(&unknown, "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_hybrid_fuses_with_rrf() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = search(
        &mcp,
        json!({"query": "refund", "vector": [1.0, 0.0, 0.0], "limit": 3}),
    )
    .await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(hit_ids(&result), [json!(1), json!(3), json!(2)]);
    // The native response's `performance` block (M1.6 Task 10, owner
    // ruling on Task 8's question).
    let p = &result["structuredContent"]["performance"];
    assert!(p["server_total_ms"].as_f64().is_some(), "{result}");
    let kinds: Vec<&str> = p["rows_scanned"]
        .as_array()
        .unwrap_or_else(|| panic!("rows_scanned: {result}"))
        .iter()
        .map(|rows| rows["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["text", "vector"], "{p}");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_filter_is_applied() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let tenant = search(&mcp, json!({"query": "refund", "filter": {"tenant": "b"}})).await;
    assert_eq!(hit_ids(&tenant), [json!(3)], "{tenant}");
    let range = search(
        &mcp,
        json!({"vector": [1.0, 0.0, 0.0], "filter": {"n": {"gte": 2}}}),
    )
    .await;
    assert_eq!(hit_ids(&range), [json!(2), json!(3)], "{range}");
    let bad = search(
        &mcp,
        json!({"query": "refund", "filter": {"n": {"near": 2}}}),
    )
    .await;
    assert_tool_error(&bad, "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_without_query_or_vector_is_a_tool_error() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    assert_tool_error(&search(&mcp, json!({})).await, "invalid_argument");
    assert_tool_error(
        &search(&mcp, json!({"query": "   "})).await,
        "invalid_argument",
    );
    assert_tool_error(
        &search(&mcp, json!({"query": "refund", "limit": 0})).await,
        "invalid_argument",
    );
    assert_tool_error(
        &search(&mcp, json!({"query": "refund", "limit": 101})).await,
        "invalid_argument",
    );
    let missing = mcp
        .call(None, "search", json!({"collection": "nope", "query": "x"}))
        .await;
    assert_tool_error(&missing, "not_found");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_vector_of_the_wrong_dimension_is_a_tool_error() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = search(&mcp, json!({"vector": [1.0, 0.0]})).await;
    assert_tool_error(&result, "invalid_argument");
    let message = result["structuredContent"]["message"].as_str().unwrap();
    assert_eq!(message, "vector has 2 dimensions; `embedding` has 3");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_output_is_capped() {
    let mcp = Mcp::start(McpConfig {
        // Room for the performance block (Task 10) and a few hits.
        max_output_bytes: 4096,
        ..McpConfig::default()
    })
    .await;
    mcp.kb("default").await;
    let body = format!("refund {}", "x".repeat(493));
    let ops: Vec<Value> = (100..120)
        .map(|id| {
            json!({"upsert": {"id": id, "source": {"body": body, "tenant": "d"}, "vectors": {}, "sparse_vectors": {}}})
        })
        .collect();
    let (status, answer) = mcp
        .native(
            Method::POST,
            "/v1/namespaces/default/collections/kb/documents",
            Some(json!({"ops": ops, "report_existence": false})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    let result = search(
        &mcp,
        json!({"query": "refund", "filter": {"tenant": "d"}, "limit": 20}),
    )
    .await;
    assert_eq!(result["isError"], false, "{result}");
    let out = &result["structuredContent"];
    assert_eq!(out["truncated"], true, "{out}");
    let hits = out["hits"].as_array().unwrap().len();
    assert!(hits > 0 && hits < 20, "{hits}");
    // The whole result (text and structured copy) fits (Ruling 13).
    assert!(
        result.to_string().len() <= 4096,
        "{}",
        result.to_string().len()
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn search_select_limits_the_source() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = search(&mcp, json!({"query": "refund", "select": ["tenant"]})).await;
    let hits = result["structuredContent"]["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2, "{result}");
    for hit in hits {
        let source = hit["source"].as_object().expect("source");
        assert_eq!(source.keys().collect::<Vec<_>>(), ["tenant"], "{hit}");
    }
    mcp.server.shutdown().await.unwrap();
}
