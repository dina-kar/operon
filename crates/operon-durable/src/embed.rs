//! [`DurableServer`]: Resonate built from [`registry`](crate::registry),
//! started and stopped inside the host process.
//!
//! It never calls `resonate_base::run`, so it installs no tracing subscriber,
//! no signal handler and no panic hook; the HTTP gateway answers a handler
//! panic with 500 (`abort_on_panic` is always `false`).

use std::fmt;
use std::fs::{File, OpenOptions, TryLockError};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use resonate_base::{Options, Running};
use resonate_plugin::types::RequestEnvelope;
use resonate_plugin::{ResonateServer, WorkerPlugin};
use serde_json::{Map, Value};

use crate::config::{self, DurableConfig, DurableStore};
use crate::error::DurableError;
use crate::listen;
use crate::registry;

/// The protocol version Loam's in-process calls speak.
pub const PROTOCOL_VERSION: &str = "2026-04-01";

/// The lock file beside a SQLite store; one process holds it exclusively.
pub const LOCK_FILE: &str = "durable.lock";

/// How long a stop waits for the listener's port to be free again.
const PORT_RELEASE: Duration = Duration::from_secs(2);

/// The embedded Resonate server: the built plugins, their listener, and the
/// store lock. Stop it with [`stop`](Self::stop); dropping it without a stop
/// leaves the listener task running until the runtime ends.
pub struct DurableServer {
    running: Running,
    listen: SocketAddr,
    node_id: String,
    shutdown_timeout: Duration,
    next_corr: AtomicU64,
    lock: Option<File>,
}

impl fmt::Debug for DurableServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableServer")
            .field("listen", &self.listen)
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl DurableServer {
    /// Check the address, lock the store, build and start the server.
    pub async fn start(config: DurableConfig, node_id: &str) -> Result<Self, DurableError> {
        Self::start_with_plugins(config, node_id, &[]).await
    }

    /// [`start`](Self::start) with `extra` worker plugins in the registry: a
    /// test hook for injecting routes onto the durable listener.
    #[doc(hidden)]
    pub async fn start_with_plugins(
        config: DurableConfig,
        node_id: &str,
        extra: &[&'static WorkerPlugin],
    ) -> Result<Self, DurableError> {
        listen::check_loopback(config.listen)?;
        let registry = registry::with_workers(extra);
        registry.check().map_err(|errors| {
            DurableError::Config(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        })?;
        #[cfg(not(feature = "mysql"))]
        if let DurableStore::Mysql { .. } = config.store {
            return Err(DurableError::Config(
                "this build has no MySQL durable store (the durable-mysql feature is off)".into(),
            ));
        }
        let configuration = config::configuration(&config, &registry::carried(&registry))?;
        let lock = match &config.store {
            DurableStore::Sqlite { path } => Some(lock_store(path)?),
            DurableStore::Mysql { .. } => None,
        };
        listen::probe(config.listen)?;
        let options = Options::default().default_server("server_sqlite");
        let running = resonate_base::build(&registry, &configuration, &options)
            .map_err(DurableError::Config)?;
        if let Err(e) = running.start(config.debug).await {
            // The port was free a moment ago: something took it in between.
            if e.contains("cannot bind") {
                tracing::warn!(addr = %config.listen, "the durable port was taken after the probe");
                return Err(DurableError::Bind {
                    addr: config.listen,
                    source: e,
                });
            }
            return Err(DurableError::Start(e));
        }
        tracing::info!(
            addr = %config.listen,
            "the durable API on http://{} is unauthenticated; it accepts loopback connections only (D111)",
            config.listen
        );
        if config.push {
            tracing::warn!(
                "--durable-push: the durable server delivers to any http:// or https:// \
                 resonate:target a caller names, a server-side request forgery risk"
            );
        }
        Ok(Self {
            running,
            listen: config.listen,
            node_id: node_id.to_string(),
            shutdown_timeout: config.shutdown_timeout,
            next_corr: AtomicU64::new(0),
            lock,
        })
    }

    /// The server, for an in-process caller such as Loam's SDK network.
    pub fn server(&self) -> Arc<dyn ResonateServer> {
        Arc::clone(self.running.server())
    }

    /// The listener's address.
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    /// The node this server runs on.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// One protocol request, in process: `{"kind": …, "data": …}`, with an
    /// optional `head` (its `corrId` and `version` are filled in when absent).
    ///
    /// A 2xx answer is the whole response envelope (`kind`, `head` with
    /// `status`, `data`). Any other status is [`DurableError::Protocol`] with
    /// the response's `data`; no answer at all is
    /// [`DurableError::Unavailable`].
    pub async fn process(&self, req: Value) -> Result<Value, DurableError> {
        let Value::Object(mut req) = req else {
            return Err(DurableError::Config(
                "a durable request is a JSON object".into(),
            ));
        };
        let head = req
            .entry("head")
            .or_insert_with(|| Value::Object(Map::new()));
        let Value::Object(head) = head else {
            return Err(DurableError::Config(
                "a request head is a JSON object".into(),
            ));
        };
        if !head.contains_key("corrId") {
            let n = self.next_corr.fetch_add(1, Ordering::Relaxed);
            head.insert(
                "corrId".into(),
                Value::String(format!("loam-{}-{n}", self.node_id)),
            );
        }
        head.entry("version")
            .or_insert_with(|| Value::String(PROTOCOL_VERSION.into()));
        let envelope: RequestEnvelope = serde_json::from_value(Value::Object(req))
            .map_err(|e| DurableError::Config(format!("a malformed durable request: {e}")))?;
        let response = self
            .running
            .server()
            .process(&envelope)
            .await
            .map_err(|e| DurableError::Unavailable(e.to_string()))?;
        let status = response.head.status;
        let response = serde_json::to_value(&response)
            .map_err(|e| DurableError::Unavailable(format!("an unreadable response: {e}")))?;
        if (200..300).contains(&status) {
            Ok(response)
        } else {
            Err(DurableError::Protocol {
                status: u16::try_from(status).unwrap_or(500),
                body: response.get("data").cloned().unwrap_or(Value::Null),
            })
        }
    }

    /// Whether the server can serve right now (its store answers).
    pub async fn ready(&self) -> bool {
        self.running.server().ready().await
    }

    /// Drain and stop, then wait until the listener's port is free (at most
    /// 2 s). The store lock is released last.
    pub async fn stop(self) {
        self.running.stop(self.shutdown_timeout).await;
        let Self {
            running,
            listen,
            lock,
            ..
        } = self;
        drop(running);
        if !listen::wait_free(listen, PORT_RELEASE).await {
            tracing::warn!(addr = %listen, "the durable port is still bound after stop");
        }
        drop(lock);
    }
}

/// Create the store's directory and take `durable.lock` in it exclusively.
fn lock_store(path: &Path) -> Result<File, DurableError> {
    let dir: PathBuf = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    };
    std::fs::create_dir_all(&dir).map_err(|e| {
        DurableError::Start(format!(
            "cannot create the durable store directory {}: {e}",
            dir.display()
        ))
    })?;
    let lock_path = dir.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .map_err(|e| DurableError::Start(format!("cannot open {}: {e}", lock_path.display())))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(DurableError::Start(format!(
            "the durable store {} is in use by another process",
            path.display()
        ))),
        Err(TryLockError::Error(e)) => Err(DurableError::Start(format!(
            "cannot lock {}: {e}",
            lock_path.display()
        ))),
    }
}
