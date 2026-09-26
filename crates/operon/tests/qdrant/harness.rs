//! The Qdrant gateway's test harness (plan M1.4 Task 2): an in-process
//! `Server` with both gateway listeners on ephemeral ports, REST helpers,
//! the generated tonic clients, and the `operon dev` binary.

#![allow(dead_code)] // Each suite uses its own subset.

use std::io::BufRead;
use std::net::SocketAddr;
use std::time::Duration;

use operon::{Server, ServerConfig};
use operon_collection::{CollectionSchema, DynamicMapping};
use operon_qdrant::QdrantConfig;
use operon_qdrant::proto::qdrant::collections_client::CollectionsClient;
use operon_qdrant::proto::qdrant::points_client::PointsClient;
use operon_qdrant::proto::qdrant::qdrant_client::QdrantClient;
use operon_qdrant::proto::qdrant::snapshots_client::SnapshotsClient;
use reqwest::{Method, StatusCode};
use serde_json::Value;
use tempfile::TempDir;
use tonic::transport::Channel;

/// An in-process server with the Qdrant gateway.
pub struct Qd {
    pub server: Server,
    /// `http://<REST addr>`.
    pub rest: String,
    /// `http://<gRPC addr>` (a tonic endpoint).
    pub grpc: String,
    pub http: reqwest::Client,
    _dir: TempDir,
}

impl Qd {
    pub async fn start() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let any = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut config = ServerConfig::new(dir.path());
        config.listen = any;
        config.log.flush_interval = Duration::from_millis(20);
        config.worker_poll_interval = Duration::from_millis(50);
        config.link.batch_interval = Duration::ZERO;
        config.qdrant = Some(QdrantConfig {
            rest_listen: any,
            grpc_listen: any,
            ..QdrantConfig::default()
        });
        let server = Server::start(config).await.expect("start");
        let rest = format!("http://{}", server.qdrant_rest_addr().expect("REST"));
        let grpc = format!("http://{}", server.qdrant_grpc_addr().expect("gRPC"));
        Self {
            server,
            rest,
            grpc,
            http: reqwest::Client::new(),
            _dir: dir,
        }
    }

    /// Creates `name` in `ns` through the collection service (the Qdrant
    /// create route is Task 3's).
    pub async fn create_raw(&self, ns: &str, name: &str) {
        let schema = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
        self.server
            .collections()
            .create_collection(ns, name, schema, None)
            .await
            .expect("create collection");
    }

    /// Sends `body` (JSON when given) with `headers`; the answer's status
    /// and body (JSON, or the text as a JSON string).
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let mut request = self.http.request(method, format!("{}{path}", self.rest));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        read(request.send().await.expect("send")).await
    }

    pub async fn put(&self, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.send(Method::PUT, path, body, &[]).await
    }

    pub async fn post(&self, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.send(Method::POST, path, body, &[]).await
    }

    pub async fn patch(&self, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.send(Method::PATCH, path, body, &[]).await
    }

    pub async fn get(&self, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.send(Method::GET, path, body, &[]).await
    }

    pub async fn delete(&self, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        self.send(Method::DELETE, path, body, &[]).await
    }

    /// A POST of raw bytes as `application/json`.
    pub async fn post_raw(&self, path: &str, body: Vec<u8>) -> (StatusCode, Value) {
        let response = self
            .http
            .post(format!("{}{path}", self.rest))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .expect("send");
        read(response).await
    }

    pub async fn channel(&self) -> Channel {
        Channel::from_shared(self.grpc.clone())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect")
    }

    pub async fn points(&self) -> PointsClient<Channel> {
        PointsClient::new(self.channel().await)
    }

    pub async fn collections(&self) -> CollectionsClient<Channel> {
        CollectionsClient::new(self.channel().await)
    }

    pub async fn snapshots(&self) -> SnapshotsClient<Channel> {
        SnapshotsClient::new(self.channel().await)
    }

    pub async fn qdrant(&self) -> QdrantClient<Channel> {
        QdrantClient::new(self.channel().await)
    }

    /// `operon dev` with the gateway on ephemeral ports (or `--no-qdrant`),
    /// read up to its `operon listening on` line.
    pub fn dev(qdrant: bool) -> Dev {
        let dir = TempDir::new().expect("temp dir");
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_operon"));
        command.args(["dev", "--listen", "127.0.0.1:0", "--no-flight-sql"]);
        if qdrant {
            command.args([
                "--qdrant-listen",
                "127.0.0.1:0",
                "--qdrant-grpc-listen",
                "127.0.0.1:0",
            ]);
        } else {
            command.arg("--no-qdrant");
        }
        let mut child = command
            .arg("--data-dir")
            .arg(dir.path())
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn operon dev");
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = std::io::BufReader::new(stdout).lines();
        let mut seen = Vec::new();
        loop {
            let line = lines
                .next()
                .expect("operon exited before listening")
                .expect("read stdout");
            let done = line.starts_with("operon listening on ");
            seen.push(line);
            if done {
                break;
            }
        }
        // Keep draining stdout so the process never blocks on a full pipe.
        std::thread::spawn(move || for _ in lines {});
        Dev {
            child,
            lines: seen,
            _dir: dir,
        }
    }
}

async fn read(response: reqwest::Response) -> (StatusCode, Value) {
    let status = response.status();
    let text = response.text().await.expect("body");
    let body = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, body)
}

/// A running `operon dev`; killed on drop.
pub struct Dev {
    child: std::process::Child,
    /// Its stdout up to and including `operon listening on …`.
    pub lines: Vec<String>,
    _dir: TempDir,
}

impl Dev {
    /// The address after `prefix` on the first line that starts with it.
    pub fn addr(&self, prefix: &str) -> Option<String> {
        self.lines
            .iter()
            .find_map(|line| line.strip_prefix(prefix).map(str::to_string))
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
