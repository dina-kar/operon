//! `memory_write` end to end (plan M1.6 Task 8 rules 4–5, Ruling 9).

use std::time::{Duration, Instant};

use operon_mcp::McpConfig;
use serde_json::{Value, json};

use crate::harness::Mcp;

fn out(result: &Value) -> &Value {
    assert_eq!(result["isError"], false, "{result}");
    &result["structuredContent"]
}

async fn collection(mcp: &Mcp, name: &str) -> Vec<Value> {
    let listed = mcp.call(None, "list_collections", json!({})).await;
    listed["structuredContent"]["collections"]
        .as_array()
        .expect("collections")
        .iter()
        .filter(|c| c["name"] == name)
        .cloned()
        .collect()
}

#[tokio::test]
async fn memory_write_creates_the_memory_collection_on_first_use() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let first = mcp
        .call(None, "memory_write", json!({"text": "first memory"}))
        .await;
    let first = out(&first);
    assert_eq!(first["collection"], "memories");
    assert_eq!(first["created_collection"], true);
    assert!(
        first["consistency_token"]
            .as_str()
            .unwrap()
            .starts_with("v1:")
    );
    assert_eq!(first["id"].as_str().unwrap().len(), 26, "a ULID: {first}");

    let listed = collection(&mcp, "memories").await;
    assert_eq!(listed.len(), 1);
    let fields: Vec<(&str, &str)> = listed[0]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["name"].as_str().unwrap(), f["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(
        fields,
        [
            ("text", "text"),
            ("tags", "keyword"),
            ("author", "keyword"),
            ("created_at", "date"),
            ("metadata", "json")
        ]
    );
    assert_eq!(listed[0]["vectors"], json!([]));

    let second = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "second memory", "tags": ["a"], "author": "me", "metadata": {"k": 1}}),
        )
        .await;
    assert_eq!(out(&second)["created_collection"], false);
    let id = out(&second)["id"].clone();
    let got = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "memories", "ids": [id]}),
        )
        .await;
    let source = &out(&got)["documents"][0]["source"];
    assert_eq!(source["text"], "second memory");
    assert_eq!(source["tags"], json!(["a"]));
    assert_eq!(source["author"], "me");
    assert_eq!(source["metadata"], json!({"k": 1}));
    assert!(
        source["created_at"].as_str().unwrap().ends_with('Z'),
        "{source}"
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_write_then_search_finds_it() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.call(None, "memory_write", json!({"text": "lunch is at noon"}))
        .await;
    let written = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "the deploy key rotates on Fridays"}),
        )
        .await;
    let id = out(&written)["id"].clone();
    let token = out(&written)["consistency_token"].clone();
    let found = mcp
        .call(
            None,
            "search",
            json!({"collection": "memories", "query": "deploy key", "consistency_token": token}),
        )
        .await;
    assert_eq!(out(&found)["hits"][0]["id"], id, "{found}");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_write_with_a_vector_adds_the_embedding_field() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let plain = mcp
        .call(None, "memory_write", json!({"text": "no vector yet"}))
        .await;
    out(&plain);
    let with = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "with a vector", "vector": [1.0, 0.0, 0.0]}),
        )
        .await;
    out(&with);
    let listed = collection(&mcp, "memories").await;
    assert_eq!(
        listed[0]["vectors"],
        json!([{"name": "embedding", "dim": 3, "distance": "cosine"}])
    );
    let wrong = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "wrong size", "vector": [1.0, 0.0, 0.0, 0.0]}),
        )
        .await;
    assert_eq!(wrong["isError"], true, "{wrong}");
    assert_eq!(wrong["structuredContent"]["error"], "schema_violation");
    assert_eq!(wrong["structuredContent"]["field"], "embedding");

    // A collection created by a write with a vector has it from the start.
    let fresh = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "v", "collection": "vectors", "vector": [0.5, 0.5]}),
        )
        .await;
    assert_eq!(out(&fresh)["created_collection"], true);
    let listed = collection(&mcp, "vectors").await;
    assert_eq!(listed[0]["vectors"][0]["dim"], 2);
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_write_with_an_id_replaces_the_memory() {
    let mcp = Mcp::start(McpConfig::default()).await;
    for text in ["first text", "second text"] {
        let written = mcp
            .call(None, "memory_write", json!({"text": text, "id": "m-1"}))
            .await;
        assert_eq!(out(&written)["id"], "m-1");
    }
    let got = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "memories", "ids": ["m-1"]}),
        )
        .await;
    assert_eq!(out(&got)["documents"][0]["source"]["text"], "second text");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let listed = collection(&mcp, "memories").await;
        if listed[0]["live_doc_count"] == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "{listed:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_write_into_a_non_memory_collection_is_a_tool_error() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = mcp
        .call(
            None,
            "memory_write",
            json!({"text": "x", "collection": "kb"}),
        )
        .await;
    assert_eq!(result["isError"], true, "{result}");
    assert_eq!(
        result["structuredContent"]["error"],
        "not_a_memory_collection"
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn memory_write_refuses_bad_arguments() {
    let mcp = Mcp::start(McpConfig::default()).await;
    for args in [
        json!({"text": ""}),
        json!({"text": "x", "id": ""}),
        json!({"text": "x", "tags": [""]}),
        json!({"text": "x", "tags": vec!["t"; 65]}),
        json!({"text": "x", "author": "a".repeat(257)}),
        json!({"text": "x", "vector": []}),
    ] {
        let result = mcp.call(None, "memory_write", args.clone()).await;
        assert_eq!(result["isError"], true, "{args}: {result}");
        assert_eq!(
            result["structuredContent"]["error"], "invalid_argument",
            "{args}"
        );
    }
    assert!(collection(&mcp, "memories").await.is_empty());
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_first_writes_share_one_collection() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let writes = (0..8).map(|i| {
        mcp.call(
            None,
            "memory_write",
            json!({"text": format!("memory {i}"), "collection": "fresh", "id": format!("m{i}")}),
        )
    });
    let results = futures::future::join_all(writes).await;
    let created = results
        .iter()
        .filter(|r| out(r)["created_collection"] == true)
        .count();
    // At least one: the metastore answers an identical concurrent create as
    // a retry of the same command (`CollectionExists`), which
    // `create_collection_owned` counts as created (row T8-3).
    assert!(created >= 1, "{results:?}");
    assert_eq!(collection(&mcp, "fresh").await.len(), 1);
    let ids: Vec<String> = (0..8).map(|i| format!("m{i}")).collect();
    let got = mcp
        .call(
            None,
            "get_documents",
            json!({"collection": "fresh", "ids": ids}),
        )
        .await;
    for doc in out(&got)["documents"].as_array().unwrap() {
        assert_eq!(doc["found"], true, "{doc}");
    }
    mcp.server.shutdown().await.unwrap();
}
