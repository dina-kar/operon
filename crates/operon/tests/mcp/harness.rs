//! The MCP test fixture (plan M1.6 Task 7 rule 13, Ruling 19): an
//! in-process `Server` with the MCP endpoint on its own ephemeral listener,
//! a native REST client for setting up collections, and a raw JSON-RPC
//! client.

#![allow(dead_code)] // Each suite uses its own subset.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use operon::{McpServerConfig, Server, ServerConfig};
use operon_mcp::McpConfig;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

/// The protocol version the fixture speaks.
pub const VERSION: &str = "2026-07-28";

/// An in-process server with MCP.
pub struct Mcp {
    pub server: Server,
    /// `http://<native addr>`.
    pub base: String,
    /// `http://<mcp addr>/mcp`.
    pub mcp: String,
    pub http: reqwest::Client,
    next_id: AtomicU64,
    _dir: TempDir,
}

/// The `_meta` of a 2026-07-28 request.
pub fn meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "operon-test", "version": "0"},
    })
}

/// The `kb` collection of the Task 1 scenario (`sdks/fixtures`).
pub fn kb_body() -> Value {
    json!({
        "name": "kb",
        "schema": {
            "fields": [
                {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
                {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true},
                {"name": "n", "source_path": "n", "kind": "i64", "indexed": true, "fast": true}
            ],
            "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
            "sparse_vectors": [],
            "dynamic": "ignore",
            "max_fields": 1000
        },
        "partitions": 2
    })
}

/// The scenario's six upserts (step `write_docs`).
pub fn kb_docs() -> Value {
    let doc = |id: Value, source: Value, vectors: Value| json!({"upsert": {"id": id, "source": source, "vectors": vectors, "sparse_vectors": {}}});
    json!({
        "ops": [
            doc(json!(1), json!({"body": "refund policy", "tenant": "a", "n": 1}), json!({"embedding": [1.0, 0.0, 0.0]})),
            doc(json!(2), json!({"body": "shipping times", "tenant": "a", "n": 2}), json!({"embedding": [0.9, 0.1, 0.0]})),
            doc(json!(3), json!({"body": "refund window", "tenant": "b", "n": 3}), json!({"embedding": [0.0, 0.0, 1.0]})),
            doc(json!(18446744073709551615u64), json!({"tenant": "c"}), json!({})),
            doc(json!("k-str"), json!({"tenant": "c"}), json!({})),
            doc(json!({"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"}), json!({"tenant": "c"}), json!({})),
        ],
        "report_existence": false
    })
}

/// A server config with fast background work, native on an ephemeral
/// port and no MCP.
pub fn base_config(dir: &TempDir) -> ServerConfig {
    let any = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut config = ServerConfig::new(dir.path());
    config.listen = any;
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    config.link.batch_interval = Duration::ZERO;
    config
}

impl Mcp {
    pub async fn start(mcp: McpConfig) -> Self {
        Self::start_with(Some(mcp)).await
    }

    /// With `mcp` `None`, the server has no MCP listener.
    pub async fn start_with(mcp: Option<McpConfig>) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = base_config(&dir);
        config.mcp = mcp.map(|mcp| McpServerConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            mcp,
        });
        let path = config
            .mcp
            .as_ref()
            .map(|m| m.mcp.path.clone())
            .unwrap_or_else(|| "/mcp".to_string());
        let server = Server::start(config).await.expect("start");
        let base = format!("http://{}", server.local_addr());
        let mcp = match server.mcp_addr() {
            Some(addr) => format!("http://{addr}{path}"),
            None => String::new(),
        };
        Self {
            server,
            base,
            mcp,
            http: reqwest::Client::new(),
            next_id: AtomicU64::new(1),
            _dir: dir,
        }
    }

    /// A native REST call.
    pub async fn native(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("send");
        let status = response.status();
        let text = response.text().await.expect("body");
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    /// Creates `kb` in `ns`, writes the scenario's six documents and waits
    /// until the collection counts them.
    pub async fn kb(&self, ns: &str) {
        let (status, body) = self
            .native(
                Method::POST,
                &format!("/v1/namespaces/{ns}/collections"),
                Some(kb_body()),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = self
            .native(
                Method::POST,
                &format!("/v1/namespaces/{ns}/collections/kb/documents"),
                Some(kb_docs()),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (_, info) = self
                .native(
                    Method::GET,
                    &format!("/v1/namespaces/{ns}/collections/kb"),
                    None,
                )
                .await;
            if info["live_doc_count"] == 6 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "kb never counted 6 documents: {info}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// POSTs `body` to the MCP endpoint with exactly `headers` (plus the
    /// JSON content type).
    pub async fn post(
        &self,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut request = self
            .http
            .post(&self.mcp)
            .header("content-type", "application/json")
            .body(body.to_string());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = request.send().await.expect("send");
        let status = response.status();
        let headers = response.headers().clone();
        let text = response.text().await.expect("body");
        (
            status,
            headers,
            serde_json::from_str(&text).unwrap_or(Value::Null),
        )
    }

    /// A JSON-RPC request body with a fresh id.
    pub fn request(&self, method: &str, params: Value) -> Value {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
    }

    /// A full 2026-07-28 request (rule 13): the protocol headers, `_meta`
    /// in `params`, and `Operon-Namespace` when `ns` is given.
    pub async fn rpc(
        &self,
        ns: Option<&str>,
        method: &str,
        params: Value,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut params = params;
        if !params.is_object() {
            params = json!({});
        }
        params["_meta"] = meta();
        let name = params["name"].as_str().map(str::to_string);
        let mut headers = vec![
            ("accept", "application/json, text/event-stream"),
            ("mcp-protocol-version", VERSION),
            ("mcp-method", method),
        ];
        if method == "tools/call"
            && let Some(name) = &name
        {
            headers.push(("mcp-name", name.as_str()));
        }
        if let Some(ns) = ns {
            headers.push(("operon-namespace", ns));
        }
        let body = self.request(method, params);
        self.post(&headers, &body).await
    }

    /// `tools/call` `tool` with `args`; returns the JSON-RPC `result`.
    pub async fn call(&self, ns: Option<&str>, tool: &str, args: Value) -> Value {
        let (status, _, body) = self
            .rpc(ns, "tools/call", json!({"name": tool, "arguments": args}))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.get("error").is_none(), "{body}");
        body["result"].clone()
    }
}
