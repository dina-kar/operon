//! An in-process server and a small JSON client for the native API tests.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use operon::{Server, ServerConfig};
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

/// The token header.
pub const TOKEN: &str = "operon-consistency-token";

/// A response: status, headers and JSON body (`null` when not JSON).
#[derive(Debug)]
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Value,
}

impl Reply {
    /// The value of header `name`.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// Asserts the status and returns the body.
    pub fn expect(self, status: StatusCode) -> Value {
        assert_eq!(self.status, status, "{}", self.body);
        self.body
    }
}

/// An in-process server with fast background work, and a client for it.
pub struct Native {
    pub server: Server,
    pub base: String,
    pub http: reqwest::Client,
    _dir: TempDir,
}

impl Native {
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = ServerConfig::new(dir.path());
        config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.log.flush_interval = Duration::from_millis(20);
        config.worker_poll_interval = Duration::from_millis(50);
        config.link.batch_interval = Duration::ZERO;
        let server = Server::start(config).await.expect("start");
        Self {
            base: format!("http://{}", server.local_addr()),
            server,
            http: reqwest::Client::new(),
            _dir: dir,
        }
    }

    pub async fn call(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> Reply {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("send");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.json().await.unwrap_or(Value::Null);
        Reply {
            status,
            headers,
            body,
        }
    }

    pub async fn post(&self, path: &str, body: Value) -> Reply {
        self.call(Method::POST, path, &[], Some(body)).await
    }

    pub async fn post_with(&self, path: &str, headers: &[(&str, &str)], body: Value) -> Reply {
        self.call(Method::POST, path, headers, Some(body)).await
    }

    pub async fn get(&self, path: &str) -> Reply {
        self.call(Method::GET, path, &[], None).await
    }

    pub async fn delete(&self, path: &str) -> Reply {
        self.call(Method::DELETE, path, &[], None).await
    }

    pub async fn shutdown(self) {
        self.server.shutdown().await.expect("shutdown");
    }
}

/// The M1.6 fixture's `kb` schema: `body` (text), `tenant` (keyword, fast),
/// `n` (i64, fast) and `embedding` (dim 3, cosine). Dynamic mapping is
/// `ignore`, not the encoder default `strict`: step 14 patches in `meta.x`,
/// which a strict schema refuses.
pub fn kb_schema() -> Value {
    json!({
        "fields": [
            {"name": "body", "source_path": "body", "kind": {"text": {"analyzer": "standard", "positions": true}}, "indexed": true, "fast": false},
            {"name": "tenant", "source_path": "tenant", "kind": "keyword", "indexed": true, "fast": true},
            {"name": "n", "source_path": "n", "kind": "i64", "indexed": true, "fast": true}
        ],
        "vectors": [{"name": "embedding", "dim": 3, "distance": "cosine"}],
        "sparse_vectors": [],
        "dynamic": "ignore",
        "max_fields": 1000
    })
}

/// The M1.6 fixture's uuid id.
pub const UUID: &str = "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e";

/// The M1.6 fixture step 10 upserts.
pub fn kb_docs() -> Value {
    let upsert = |id: Value, source: Value, embedding: Value| json!({"upsert": {"id": id, "source": source, "vectors": embedding}});
    json!({
        "ops": [
            upsert(json!(1), json!({"body": "refund policy", "tenant": "a", "n": 1}), json!({"embedding": [1.0, 0.0, 0.0]})),
            upsert(json!(2), json!({"body": "shipping times", "tenant": "a", "n": 2}), json!({"embedding": [0.9, 0.1, 0.0]})),
            upsert(json!(3), json!({"body": "refund window", "tenant": "b", "n": 3}), json!({"embedding": [0.0, 0.0, 1.0]})),
            upsert(json!(18446744073709551615u64), json!({"tenant": "c"}), json!({})),
            upsert(json!("k-str"), json!({"tenant": "c"}), json!({})),
            upsert(json!({"uuid": UUID}), json!({"tenant": "c"}), json!({})),
        ],
        "report_existence": false
    })
}

/// Creates `kb` in namespace `ns` (2 partitions) and writes the fixture's
/// documents; returns the write's token.
pub async fn kb(api: &Native, ns: &str) -> String {
    api.post(
        &format!("/v1/namespaces/{ns}/collections"),
        json!({"name": "kb", "schema": kb_schema(), "partitions": 2}),
    )
    .await
    .expect(StatusCode::CREATED);
    let reply = api
        .post(
            &format!("/v1/namespaces/{ns}/collections/kb/documents"),
            kb_docs(),
        )
        .await;
    let body = reply.expect(StatusCode::OK);
    body["token"].as_str().expect("token").to_string()
}
