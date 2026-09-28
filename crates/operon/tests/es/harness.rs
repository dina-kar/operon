//! The Elasticsearch gateway's test harness (plan M1.5 Task 1): an
//! in-process `Server` with the gateway on an ephemeral port, REST helpers,
//! and the `operon dev` binary.

#![allow(dead_code)] // Each suite uses its own subset.

use std::io::BufRead;
use std::net::SocketAddr;
use std::time::Duration;

use operon::{Server, ServerConfig};
use operon_collection::{CollectionSchema, DynamicMapping};
use operon_es::EsConfig;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use serde_json::Value;
use tempfile::TempDir;

/// An in-process server with the Elasticsearch gateway.
pub struct Es {
    pub server: Server,
    /// `http://<ES addr>`.
    pub base: String,
    pub http: reqwest::Client,
    _dir: TempDir,
}

/// An answer: status, headers and body (JSON, or the text as a JSON
/// string; `Null` when empty).
#[derive(Debug)]
pub struct Answer {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Value,
    pub text: String,
}

impl Answer {
    /// A header's value, `""` when absent.
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
    }

    /// Asserts the ES envelope: `status`, `type` and (when given) `reason`.
    pub fn assert_error(&self, status: u16, kind: &str, reason: Option<&str>) {
        assert_eq!(self.status.as_u16(), status, "{}", self.text);
        assert_eq!(self.body["status"], status, "{}", self.text);
        assert_eq!(self.body["error"]["type"], kind, "{}", self.text);
        assert_eq!(
            self.body["error"]["root_cause"][0]["type"], kind,
            "{}",
            self.text
        );
        if let Some(reason) = reason {
            assert_eq!(self.body["error"]["reason"], reason, "{}", self.text);
        }
    }
}

impl Es {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    /// [`Es::start`], with `configure` applied to the config last.
    pub async fn start_with(configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let any = SocketAddr::from(([127, 0, 0, 1], 0));
        let mut config = ServerConfig::new(dir.path());
        config.listen = any;
        config.log.flush_interval = Duration::from_millis(20);
        config.worker_poll_interval = Duration::from_millis(50);
        config.link.batch_interval = Duration::ZERO;
        config.es = Some(EsConfig {
            listen: any,
            ..EsConfig::default()
        });
        configure(&mut config);
        let server = Server::start(config).await.expect("start");
        let base = format!("http://{}", server.es_addr().expect("ES"));
        Self {
            server,
            base,
            http: reqwest::Client::new(),
            _dir: dir,
        }
    }

    /// Creates `name` in `ns` through the collection service (the ES
    /// create route is Task 3's).
    pub async fn create_raw(&self, ns: &str, name: &str, partitions: Option<u32>) {
        let schema = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
        self.server
            .collections()
            .create_collection(ns, name, schema, partitions)
            .await
            .expect("create collection");
    }

    /// Sends `body` (raw bytes, with its content type) and `headers`.
    pub async fn send_raw(
        &self,
        method: Method,
        path: &str,
        body: Option<(&str, Vec<u8>)>,
        headers: &[(&str, &str)],
    ) -> Answer {
        let mut request = self.http.request(method, format!("{}{path}", self.base));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        if let Some((content_type, bytes)) = body {
            request = request.header("content-type", content_type).body(bytes);
        }
        read(request.send().await.expect("send")).await
    }

    /// Sends `body` as JSON when given.
    pub async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> Answer {
        let body = body.map(|b| ("application/json", b.to_string().into_bytes()));
        self.send_raw(method, path, body, headers).await
    }

    pub async fn get(&self, path: &str) -> Answer {
        self.send(Method::GET, path, None, &[]).await
    }

    pub async fn head(&self, path: &str) -> Answer {
        self.send(Method::HEAD, path, None, &[]).await
    }

    pub async fn post(&self, path: &str, body: Value) -> Answer {
        self.send(Method::POST, path, Some(body), &[]).await
    }

    /// `PUT`, with a JSON body when given.
    pub async fn put(&self, path: &str, body: Option<Value>) -> Answer {
        self.send(Method::PUT, path, body, &[]).await
    }

    pub async fn delete(&self, path: &str) -> Answer {
        self.send(Method::DELETE, path, None, &[]).await
    }

    /// Asserts a 200 answer and returns it.
    pub fn ok(answer: Answer) -> Answer {
        assert_eq!(answer.status, StatusCode::OK, "{}", answer.text);
        answer
    }

    /// `operon dev` with the gateway on an ephemeral port (or `--no-es`),
    /// read up to its `operon listening on` line.
    pub fn dev(es: bool) -> Dev {
        let dir = TempDir::new().expect("temp dir");
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_operon"));
        command.args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--no-flight-sql",
            "--no-qdrant",
        ]);
        if es {
            command.args(["--es-listen", "127.0.0.1:0"]);
        } else {
            command.arg("--no-es");
        }
        command.arg("--no-mcp");
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

async fn read(response: reqwest::Response) -> Answer {
    let status = response.status();
    let headers = response.headers().clone();
    let text = response.text().await.expect("body");
    let body = if text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).unwrap_or(Value::String(text.clone()))
    };
    Answer {
        status,
        headers,
        body,
        text,
    }
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
