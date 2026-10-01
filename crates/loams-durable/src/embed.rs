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

use resonate_base::{Options, Registry, Running};
use resonate_plugin::types::RequestEnvelope;
use resonate_plugin::{Configuration, ResonateServer, WorkerPlugin};
use serde_json::{Map, Value};

use crate::config::{self, DurableConfig, DurableStore, Mode};
use crate::error::DurableError;
use crate::inproc::{self, InProcWorker};
use crate::listen;
use crate::registry;

/// The protocol version Loams's in-process calls speak.
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
    inproc: Option<Arc<InProcWorker>>,
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
        let instance = inproc::next_instance();
        let (registry, configuration) = prepare(&config, extra, Mode::Serve, Some(instance))?;
        let lock = match &config.store {
            DurableStore::Sqlite { path } => Some(lock_store(path)?),
            DurableStore::Mysql { .. } => None,
            DurableStore::Tikv { .. } => None,
        };
        // Serving never migrates a MySQL store: an empty database would
        // otherwise get Resonate's schema on the first start (D1 Task 4).
        #[cfg(feature = "mysql")]
        if let DurableStore::Mysql { url, tls } = &config.store {
            crate::mysql::ensure_crypto_provider(*tls);
            crate::mysql::check_schema(url, *tls).await?;
        }
        listen::probe(config.listen)?;
        let options = Options::default().default_server("server_sqlite");
        let built = resonate_base::build(&registry, &configuration, &options);
        // Taken whatever `build` said, so a failed build parks nothing.
        let inproc = inproc::take(instance);
        let running = built.map_err(|e| DurableError::Config(scrub_for(&config.store, &e)))?;
        if let Err(e) = running.start(config.debug).await {
            // The port was free a moment ago: something took it in between.
            if e.contains("cannot bind") {
                tracing::warn!(addr = %config.listen, "the durable port was taken after the probe");
                return Err(DurableError::Bind {
                    addr: config.listen,
                    source: e,
                });
            }
            return Err(DurableError::Start(explain_start(&config.store, &e)));
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
            inproc,
        })
    }

    /// `loams durable migrate`: open `store` once with Resonate's migrations
    /// on (`migrate = true`), with no listener, and stop. An empty database
    /// gets the schema; an up-to-date one is left as it is. The database
    /// itself must exist.
    pub async fn migrate(store: DurableStore) -> Result<(), DurableError> {
        let config = DurableConfig::new(store);
        let (registry, configuration) = prepare(&config, &[], Mode::Migrate, None)?;
        #[cfg(feature = "mysql")]
        if let DurableStore::Mysql { tls, .. } = &config.store {
            crate::mysql::ensure_crypto_provider(*tls);
        }
        let lock = match &config.store {
            DurableStore::Sqlite { path } => Some(lock_store(path)?),
            DurableStore::Mysql { .. } => None,
            DurableStore::Tikv { .. } => None,
        };
        let options = Options::default().default_server("server_sqlite");
        let running = resonate_base::build(&registry, &configuration, &options)
            .map_err(|e| DurableError::Config(scrub_for(&config.store, &e)))?;
        running
            .start(false)
            .await
            .map_err(|e| DurableError::Start(scrub_for(&config.store, &e)))?;
        running.stop(config.shutdown_timeout).await;
        drop(running);
        drop(lock);
        tracing::info!(store = %config.store, "the durable store is migrated");
        Ok(())
    }

    /// The server, for an in-process caller such as Loams's SDK network.
    pub fn server(&self) -> Arc<dyn ResonateServer> {
        Arc::clone(self.running.server())
    }

    /// The `worker_inproc` worker Loams's runtime subscribes to (D1 Task 6).
    pub fn inproc(&self) -> Option<Arc<InProcWorker>> {
        self.inproc.clone()
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
                Value::String(format!("loams-{}-{n}", self.node_id)),
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

/// The registry (with `extra` workers) and the configuration for `config`.
fn prepare(
    config: &DurableConfig,
    extra: &[&'static WorkerPlugin],
    mode: Mode,
    inproc: Option<u64>,
) -> Result<(Registry, Configuration), DurableError> {
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
    #[cfg(not(feature = "tikv"))]
    if let DurableStore::Tikv { .. } = config.store {
        return Err(DurableError::Config(
            "this build has no native TiKV durable store (the durable-tikv feature is off)".into(),
        ));
    }
    let configuration = config::configuration(config, &registry::carried(&registry), mode, inproc)
        .map_err(|e| match e {
            DurableError::Config(message) => {
                DurableError::Config(scrub_for(&config.store, &message))
            }
            other => other,
        })?;
    Ok((registry, configuration))
}

/// `message` without the store's password.
fn scrub_for(store: &DurableStore, message: &str) -> String {
    match store {
        DurableStore::Sqlite { .. } => message.to_string(),
        DurableStore::Mysql { url, .. } => config::scrub(message, url),
        DurableStore::Tikv { .. } => message.to_string(),
    }
}

/// A start failure's message, without the store's password, and naming
/// `loams durable migrate` when the schema is the problem.
fn explain_start(store: &DurableStore, message: &str) -> String {
    let message = scrub_for(store, message);
    if !matches!(store, DurableStore::Mysql { .. }) {
        return message;
    }
    if message.contains("different checksum") {
        format!(
            "{message}\n\nThen run 'loams durable migrate' against the new database before \
             serving it."
        )
    } else if message.contains("schema:") {
        format!("{message}; run 'loams durable migrate' first")
    } else {
        message
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
