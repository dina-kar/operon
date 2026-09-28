//! The MCP transport and protocol (plan M1.6 Task 7; Review Focus 5):
//! stateless 2026-07-28 requests, legacy clients, strict mode, `Host` and
//! `Origin` checks, the listener and the rmcp client.

use operon_mcp::McpConfig;
use reqwest::{Method, StatusCode};
use rmcp::model::{CallToolRequestParams, ClientConfig, ProtocolVersion};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientLifecycleMode, ClientServiceExt};
use serde_json::{Value, json};

use crate::harness::Mcp;

/// Every tool, sorted (Ruling 8).
const TOOLS: [&str; 5] = [
    "get_documents",
    "list_collections",
    "memory_write",
    "search",
    "sql",
];

fn tool_names(result: &Value) -> Vec<String> {
    let mut names: Vec<String> = result["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|t| t["name"].as_str().expect("name").to_string())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn tools_list_needs_no_initialize_and_no_session() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let (status, headers, body) = mcp.rpc(None, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let content_type = headers["content-type"].to_str().unwrap();
    assert!(
        content_type.starts_with("application/json"),
        "{content_type}"
    );
    assert!(headers.get("mcp-session-id").is_none());
    assert_eq!(tool_names(&body["result"]), TOOLS);
    assert_eq!(body["result"]["ttlMs"], 600_000);
    assert_eq!(body["result"]["cacheScope"], "public");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn server_discover_advertises_the_supported_versions() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let (status, _, body) = mcp.rpc(None, "server/discover", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result = &body["result"];
    assert_eq!(
        result["supportedVersions"],
        json!(["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"])
    );
    assert_eq!(result["ttlMs"], 600_000);
    assert_eq!(result["cacheScope"], "public");
    assert!(result["capabilities"]["tools"].is_object(), "{result}");
    assert!(
        result["instructions"]
            .as_str()
            .unwrap()
            .starts_with("Operon database tools.")
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn legacy_initialize_is_answered_statelessly() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let accept = ("accept", "application/json, text/event-stream");
    let initialize = mcp.request(
        "initialize",
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "legacy", "version": "0"}
        }),
    );
    let (status, headers, body) = mcp.post(&[accept], &initialize).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["protocolVersion"], "2025-11-25", "{body}");
    assert!(headers.get("mcp-session-id").is_none());

    let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
    let (status, headers, _) = mcp
        .post(
            &[accept, ("mcp-protocol-version", "2025-11-25")],
            &initialized,
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(headers.get("mcp-session-id").is_none());

    let legacy = [accept, ("mcp-protocol-version", "2025-11-25")];
    let list = mcp.request("tools/list", json!({}));
    let (status, headers, body) = mcp.post(&legacy, &list).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(headers.get("mcp-session-id").is_none());
    assert_eq!(tool_names(&body["result"]), TOOLS);
    assert!(body["result"].get("ttlMs").is_none(), "{body}");
    assert!(body["result"].get("cacheScope").is_none(), "{body}");

    let call = mcp.request(
        "tools/call",
        json!({"name": "list_collections", "arguments": {}}),
    );
    let (status, _, body) = mcp.post(&legacy, &call).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["isError"], false, "{body}");
    assert_eq!(
        body["result"]["structuredContent"]["collections"][0]["name"],
        "kb"
    );
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_stateless_refuses_requests_without_metadata() {
    let mcp = Mcp::start(McpConfig {
        strict_stateless: true,
        ..McpConfig::default()
    })
    .await;
    let accept = ("accept", "application/json, text/event-stream");
    let list = mcp.request("tools/list", json!({}));
    let (status, _, body) = mcp.post(&[accept], &list).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], -32020, "{body}");

    let initialize = mcp.request(
        "initialize",
        json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "legacy", "version": "0"}
        }),
    );
    let (_, _, body) = mcp.post(&[accept], &initialize).await;
    assert_eq!(body["error"]["code"], -32022, "{body}");

    let (status, _, body) = mcp.rpc(None, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(tool_names(&body["result"]), TOOLS);
    let (_, _, body) = mcp.rpc(None, "server/discover", json!({})).await;
    assert_eq!(body["result"]["supportedVersions"], json!(["2026-07-28"]));
    mcp.server.shutdown().await.unwrap();
}

/// Calls `tool` with `args` through `client` and returns its structured
/// content, asserting the call succeeded.
async fn client_call(
    client: &rmcp::Peer<rmcp::RoleClient>,
    tool: &'static str,
    args: Value,
) -> Value {
    let result = client
        .call_tool(
            CallToolRequestParams::new(tool)
                .with_arguments(args.as_object().expect("object").clone()),
        )
        .await
        .unwrap_or_else(|err| panic!("{tool}: {err}"));
    let content = result.structured_content.expect("structured");
    assert_eq!(result.is_error, Some(false), "{tool}: {content}");
    content
}

/// Task 8: the Discover-mode client (`server/discover`, never
/// `initialize`; Review Focus 5) lists and calls all five tools.
#[tokio::test]
async fn rmcp_client_calls_every_tool_over_streamable_http() {
    let mcp = Mcp::start(McpConfig::default()).await;
    mcp.kb("default").await;
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(mcp.mcp.clone()),
    );
    let client = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .expect("client starts with server/discover");
    let tools = client.list_tools(None).await.expect("tools/list");
    let mut names: Vec<String> = tools.tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(names, TOOLS);

    let listed = client_call(&client, "list_collections", json!({})).await;
    assert_eq!(listed["collections"][0]["name"], "kb");

    let got = client_call(
        &client,
        "get_documents",
        json!({"collection": "kb", "ids": [1]}),
    )
    .await;
    assert_eq!(got["documents"][0]["found"], true, "{got}");
    assert_eq!(got["documents"][0]["source"]["body"], "refund policy");

    let found = client_call(
        &client,
        "search",
        json!({"collection": "kb", "query": "refund"}),
    )
    .await;
    assert_eq!(found["hits"][0]["id"], 1, "{found}");

    let rows = client_call(
        &client,
        "sql",
        json!({"query": "SELECT count(*) AS n FROM kb"}),
    )
    .await;
    assert_eq!(rows["rows"], json!([{"n": 6}]), "{rows}");

    let written = client_call(
        &client,
        "memory_write",
        json!({"text": "the client wrote this"}),
    )
    .await;
    assert_eq!(written["collection"], "memories", "{written}");
    assert_eq!(written["created_collection"], true, "{written}");
    client.cancel().await.expect("cancel");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_foreign_host_header_is_refused() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let list = mcp.request("tools/list", json!({"_meta": crate::harness::meta()}));
    let headers = [
        ("accept", "application/json, text/event-stream"),
        ("mcp-protocol-version", crate::harness::VERSION),
        ("mcp-method", "tools/list"),
        ("host", "evil.example"),
    ];
    let (status, _, _) = mcp.post(&headers, &list).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_browser_origin_is_refused() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let mut headers = vec![
        ("accept", "application/json, text/event-stream"),
        ("mcp-protocol-version", crate::harness::VERSION),
        ("mcp-method", "tools/list"),
    ];
    let list = mcp.request("tools/list", json!({"_meta": crate::harness::meta()}));
    let (status, _, body) = mcp.post(&headers, &list).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    headers.push(("origin", "http://evil.example"));
    let (status, _, _) = mcp.post(&headers, &list).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn tool_annotations_are_advertised() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let (_, _, body) = mcp.rpc(None, "tools/list", json!({})).await;
    let tools = body["result"]["tools"].as_array().expect("tools");
    let titles = [
        ("search", "Search a collection"),
        ("sql", "Read-only SQL"),
        ("memory_write", "Write a memory"),
        ("list_collections", "List collections"),
        ("get_documents", "Get documents"),
    ];
    for (name, title) in titles {
        let tool = tools
            .iter()
            .find(|t| t["name"] == name)
            .unwrap_or_else(|| panic!("{name} listed"));
        let annotations = &tool["annotations"];
        assert_eq!(annotations["title"], title, "{tool}");
        assert_eq!(annotations["openWorldHint"], false, "{tool}");
        if name == "memory_write" {
            assert_eq!(annotations["readOnlyHint"], false, "{tool}");
            assert_eq!(annotations["destructiveHint"], true, "{tool}");
            assert_eq!(annotations["idempotentHint"], false, "{tool}");
        } else {
            assert_eq!(annotations["readOnlyHint"], true, "{tool}");
        }
        assert!(tool.get("outputSchema").is_none(), "{tool}");
        assert_eq!(tool["inputSchema"]["additionalProperties"], false, "{tool}");
    }
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_mcp_disables_the_endpoint() {
    let mcp = Mcp::start_with(None).await;
    assert_eq!(mcp.server.mcp_addr(), None);
    assert!(mcp.mcp.is_empty());
    let (status, body) = mcp.native(Method::POST, "/mcp", Some(json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found", "{body}");
    mcp.server.shutdown().await.unwrap();
}

#[tokio::test]
async fn mcp_is_not_served_on_the_native_listener() {
    let mcp = Mcp::start(McpConfig::default()).await;
    let native = mcp.server.local_addr();
    let served = mcp.server.mcp_addr().expect("mcp listener");
    assert_ne!(native, served);
    let request = mcp.request("tools/list", json!({"_meta": crate::harness::meta()}));
    let (status, body) = mcp.native(Method::POST, "/mcp", Some(request)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "not_found", "{body}");
    assert!(body["message"].is_string(), "{body}");
    mcp.server.shutdown().await.unwrap();
}
