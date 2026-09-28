//! `list_collections` and `get_documents` end to end (plan M1.6 Task 7),
//! over the Task 1 scenario's `kb` collection.

use operon_mcp::McpConfig;
use serde_json::{Value, json};

use crate::harness::Mcp;

fn ids(result: &Value) -> Vec<Value> {
    result["structuredContent"]["documents"]
        .as_array()
        .expect("documents")
        .iter()
        .map(|d| d["id"].clone())
        .collect()
}

#[tokio::test]
async fn list_collections_describes_fields_and_vectors() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = mcp.call(None, "list_collections", json!({})).await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(
        result["structuredContent"],
        json!({"collections": [{
            "name": "kb",
            "fields": [
                {"name": "body", "kind": "text"},
                {"name": "tenant", "kind": "keyword"},
                {"name": "n", "kind": "i64"}
            ],
            "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
            "live_doc_count": 6
        }], "truncated": false})
    );
    // The same JSON as text (Ruling 8).
    let text = result["content"][0]["text"].as_str().expect("text");
    assert_eq!(
        serde_json::from_str::<Value>(text).unwrap(),
        result["structuredContent"]
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn list_collections_on_an_empty_namespace_is_empty() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let result = mcp
        .call(Some("nobody"), "list_collections", json!({}))
        .await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(
        result["structuredContent"],
        json!({"collections": [], "truncated": false})
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn namespace_header_overrides_the_configured_namespace() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("a").await;
    let args = json!({"collection": "kb", "ids": [1]});
    let missing = mcp.call(None, "get_documents", args.clone()).await;
    assert_eq!(missing["isError"], true, "{missing}");
    assert_eq!(missing["structuredContent"]["error"], "not_found");
    let found = mcp.call(Some("a"), "get_documents", args).await;
    assert_eq!(found["isError"], false, "{found}");
    assert_eq!(found["structuredContent"]["documents"][0]["found"], true);
    let listed = mcp.call(Some("a"), "list_collections", json!({})).await;
    assert_eq!(listed["structuredContent"]["collections"][0]["name"], "kb");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn get_documents_returns_found_and_missing_in_order() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": [3, 999, 1]}),
        )
        .await;
    assert_eq!(result["isError"], false, "{result}");
    let out = &result["structuredContent"];
    let found: Vec<&Value> = out["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| &d["found"])
        .collect();
    assert_eq!(found, [&json!(true), &json!(false), &json!(true)]);
    assert_eq!(ids(&result), [json!(3), json!(999), json!(1)]);
    assert_eq!(out["documents"][0]["source"]["body"], "refund window");
    assert_eq!(out["documents"][1]["source"], Value::Null);
    assert_eq!(out["truncated"], false);

    let selected = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": [1], "select": ["tenant"]}),
        )
        .await;
    assert_eq!(
        selected["structuredContent"]["documents"][0]["source"],
        json!({"tenant": "a"})
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn get_documents_accepts_u64_string_and_uuid_ids() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let special = json!([
        18446744073709551615u64,
        "k-str",
        {"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"}
    ]);
    let result = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": special}),
        )
        .await;
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(Value::Array(ids(&result)), special);
    for doc in result["structuredContent"]["documents"].as_array().unwrap() {
        assert_eq!(doc["found"], true, "{doc}");
        assert_eq!(doc["source"]["tenant"], "c");
    }
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn get_documents_refuses_too_many_ids() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let too_many: Vec<u64> = (0..101).collect();
    let result = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": too_many}),
        )
        .await;
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(result["structuredContent"]["error"], "invalid_argument");
    let none = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": []}),
        )
        .await;
    assert_eq!(none["structuredContent"]["error"], "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn bad_arguments_are_refused() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    // Arguments that do not deserialize are refused by rmcp itself: 3.4.1
    // answers a tool result with `isError` and its own message (row T7-3).
    for (tool, args) in [
        ("get_documents", json!({"collection": "kb", "ids": [true]})),
        ("list_collections", json!({"extra": 1})),
    ] {
        let result = mcp.call(None, tool, args).await;
        assert_eq!(result["isError"], true, "{result}");
        let text = result["content"][0]["text"].as_str().unwrap_or_default();
        assert!(
            text.starts_with("failed to deserialize parameters"),
            "{result}"
        );
    }
    let bad_token = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": [1], "consistency_token": "nope"}),
        )
        .await;
    assert_eq!(bad_token["structuredContent"]["error"], "invalid_argument");
    let bad_uuid = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": [{"uuid": "x"}]}),
        )
        .await;
    assert_eq!(bad_uuid["structuredContent"]["error"], "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

/// The whole tool result, as the JSON-RPC answer carries it (the text
/// content and `structuredContent`), is at most `max_output_bytes`
/// (Ruling 13; PR #98 review).
fn assert_result_fits(result: &Value, max: usize) {
    let size = result.to_string().len();
    assert!(size <= max, "result is {size} bytes, over {max}: {result}");
    let text = result["content"][0]["text"].as_str().expect("text");
    assert_eq!(
        serde_json::from_str::<Value>(text).expect("the text is JSON"),
        result["structuredContent"]
    );
}

#[tokio::test]
async fn list_collections_output_is_capped() {
    let mcp = Mcp::start(McpConfig {
        max_output_bytes: 1024,
        ..McpConfig::default()
    })
    .await;
    for i in 0..10 {
        let body = json!({
            "name": format!("c{i:02}"),
            "schema": {
                "fields": [{"name": "body", "kind": "keyword"}],
                "vectors": [], "sparse_vectors": [], "dynamic": "ignore", "max_fields": 1000
            }
        });
        let (status, answer) = mcp
            .native(
                reqwest::Method::POST,
                "/v1/namespaces/default/collections",
                Some(body),
            )
            .await;
        assert_eq!(status, reqwest::StatusCode::CREATED, "{answer}");
    }
    let result = mcp.call(None, "list_collections", json!({})).await;
    assert_eq!(result["isError"], false, "{result}");
    let out = &result["structuredContent"];
    assert_eq!(out["truncated"], true, "{out}");
    let kept = out["collections"].as_array().unwrap();
    assert!(!kept.is_empty() && kept.len() < 10, "{out}");
    assert_eq!(kept[0]["name"], "c00");
    assert_result_fits(&result, 1024);
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn get_documents_output_is_capped_with_its_text() {
    let mcp = Mcp::start(McpConfig {
        max_output_bytes: 2048,
        ..McpConfig::default()
    })
    .await;
    mcp.kb("default").await;
    let body = "x".repeat(300);
    let ops: Vec<Value> = (100..110)
        .map(|id| {
            json!({"upsert": {"id": id, "source": {"body": body}, "vectors": {}, "sparse_vectors": {}}})
        })
        .collect();
    let (status, answer) = mcp
        .native(
            reqwest::Method::POST,
            "/v1/namespaces/default/collections/kb/documents",
            Some(json!({"ops": ops, "report_existence": false})),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{answer}");
    let ids: Vec<u64> = (100..110).collect();
    let result = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "kb", "ids": ids}),
        )
        .await;
    assert_eq!(result["isError"], false, "{result}");
    let out = &result["structuredContent"];
    assert_eq!(out["truncated"], true, "{out}");
    let kept = out["documents"].as_array().unwrap().len();
    assert!(kept > 0 && kept < 10, "{kept}");
    assert_result_fits(&result, 2048);
    mcp.server.shutdown().await.unwrap();
}
