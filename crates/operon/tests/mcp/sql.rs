//! `sql` end to end (plan M1.6 Task 8 rule 3, rows E18 and E19).

use operon_mcp::McpConfig;
use serde_json::json;

use crate::harness::Mcp;

#[tokio::test]
async fn sql_select_returns_rows() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let result = mcp
        .call(
            None,
            "sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await;
    assert_eq!(result["isError"], false, "{result}");
    let out = &result["structuredContent"];
    assert_eq!(out["rows"], json!([{"n": 6}]), "{out}");
    assert_eq!(out["columns"], json!([{"name": "n", "data_type": "Int64"}]));
    assert_eq!(out["row_count"], 1);
    assert_eq!(out["truncated"], false);

    let capped = mcp
        .call(
            None,
            "sql",
            json!({"query": "SELECT n FROM kb WHERE n IS NOT NULL ORDER BY n", "max_rows": 2}),
        )
        .await;
    let out = &capped["structuredContent"];
    assert_eq!(out["rows"], json!([{"n": 1}, {"n": 2}]), "{out}");
    assert_eq!(out["truncated"], true);
    let zero = mcp
        .call(None, "sql", json!({"query": "SELECT 1", "max_rows": 0}))
        .await;
    assert_eq!(zero["structuredContent"]["error"], "invalid_argument");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn sql_refuses_ddl_and_dml() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    for query in [
        "DROP TABLE kb",
        "INSERT INTO kb (body) VALUES ('x')",
        "CREATE TABLE t AS SELECT 1",
    ] {
        let result = mcp.call(None, "sql", json!({"query": query})).await;
        assert_eq!(result["isError"], true, "{query}: {result}");
        assert_eq!(
            result["structuredContent"]["error"], "invalid_argument",
            "{query}: {result}"
        );
    }
    let after = mcp
        .call(
            None,
            "sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await;
    assert_eq!(after["structuredContent"]["rows"], json!([{"n": 6}]));
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn sql_reads_the_namespace_of_the_header() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("a").await;
    let missing = mcp
        .call(
            None,
            "sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await;
    assert_eq!(missing["isError"], true, "{missing}");
    let found = mcp
        .call(
            Some("a"),
            "sql",
            json!({"query": "SELECT count(*) AS n FROM kb"}),
        )
        .await;
    assert_eq!(found["structuredContent"]["rows"], json!([{"n": 6}]));
    mcp.server.shutdown().await.unwrap();
}
