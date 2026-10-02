//! `loams-apps-mock`: a stateful mock of the Loams app protos (design §37
//! §12, AP0 Task 5).
//!
//! It serves `loams.instance.v1`, `loams.devices.v1`, `loams.approvals.v1`,
//! `loams.operations.v1` and `loams.notifications.v1` over Connect, gRPC and
//! gRPC-Web on one loopback listener (connect-rust), with seed data
//! ([`seed::Seed::demo`]). The console, the desktop and the phone apps
//! develop and test against it until the server side (AP4) exists.
//!
//! **Credentials are fake and documented as such.** A request carries
//! `Authorization: Bearer mock-access-<principal id>` for a fresh session
//! (authenticated now) or `Bearer mock-stale-<principal id>` for a session
//! authenticated 10 minutes ago, which exercises the step-up rule of AP0
//! Ruling 7. Nothing here is a secret, and [`serve`] refuses to bind a
//! non-loopback address (D111).
//!
//! What is real: the approval decision rules ([`acceptance`]), revisions,
//! idempotency keys, and watch streams with snapshot, changes, heartbeats
//! and resume cursors (AP0 Ruling 3). What is stubbed (answers
//! `unimplemented` with reason `not_implemented`): decision-proof
//! verification, push targets, notification preferences, test
//! notifications, operation cancel, and the YAML scenario runner (AP0
//! Ruling 9); see the crate README for the list.

pub mod acceptance;
mod auth;
mod cors;
pub mod seed;
mod services;
mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, bail};
use connectrpc::{ConnectError, ErrorCode, ErrorDetail, Router};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The generated messages, service traits and (with the `client` feature)
/// clients of the app protos.
#[allow(missing_debug_implementations, missing_docs, unreachable_pub)]
pub mod proto {
    connectrpc::include_generated!();
}

pub use seed::Seed;

/// The default listener: beside `loams-console-mock` (8081) and the MCP
/// listener (8083).
pub const DEFAULT_LISTEN: &str = "127.0.0.1:8084";

/// How often every `Watch*` stream sends a heartbeat (§37 §8.3).
pub const HEARTBEAT: Duration = Duration::from_secs(15);

/// How long [`MockHandle::stop`] waits for open connections.
const STOP_GRACE: Duration = Duration::from_millis(500);

/// How the mock runs.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// A loopback address; port 0 picks a free port.
    pub listen: SocketAddr,
    /// The data the mock starts with.
    pub seed: Seed,
    /// The heartbeat period of every watch stream (tests shorten it).
    pub heartbeat: Duration,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN.parse().expect("valid default address"),
            seed: Seed::demo(),
            heartbeat: HEARTBEAT,
        }
    }
}

/// A running mock.
#[derive(Debug)]
pub struct MockHandle {
    /// The bound address (the real port when `listen` asked for port 0).
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl MockHandle {
    /// The base URL clients use, for example `http://127.0.0.1:8084`.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stops the listener. Open watch streams never end by themselves, so
    /// connections still open after a short grace period are dropped.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if tokio::time::timeout(STOP_GRACE, &mut self.task)
            .await
            .is_err()
        {
            self.task.abort();
        }
    }
}

/// Binds the listener and serves the mock in the background.
///
/// # Errors
///
/// Refuses a non-loopback `listen` address (D111) and fails if the address
/// cannot be bound.
pub async fn serve(config: MockConfig) -> anyhow::Result<MockHandle> {
    if !config.listen.ip().is_loopback() {
        bail!(
            "loams-apps-mock binds loopback only (D111); refusing {}",
            config.listen
        );
    }
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let addr = listener.local_addr()?;
    let app = router(&config).into_axum_service();
    let app = axum::Router::new()
        .fallback_service(app)
        .layer(axum::middleware::from_fn(cors::cors));
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = rx.await;
        });
        if let Err(err) = server.await {
            tracing::error!(%err, "loams-apps-mock stopped");
        }
    });
    Ok(MockHandle {
        addr,
        shutdown: Some(tx),
        task,
    })
}

/// The Connect router with every app service registered.
fn router(config: &MockConfig) -> Router {
    use proto::loams::approvals::v1::ApprovalServiceExt as _;
    use proto::loams::devices::v1::DeviceServiceExt as _;
    use proto::loams::instance::v1::InstanceServiceExt as _;
    use proto::loams::notifications::v1::NotificationServiceExt as _;
    use proto::loams::operations::v1::OperationsServiceExt as _;

    let store = Arc::new(store::Store::new(config.seed.clone(), config.heartbeat));
    let router = Router::new();
    let router = Arc::new(services::Instance(store.clone())).register(router);
    let router = Arc::new(services::Approvals(store.clone())).register(router);
    let router = Arc::new(services::Devices(store.clone())).register(router);
    let router = Arc::new(services::Operations(store.clone())).register(router);
    Arc::new(services::Notifications(store)).register(router)
}

/// A Connect error carrying a `loams.errors.v1.ErrorInfo` with a stable
/// `reason` (AP0 Ruling 6).
pub(crate) fn refuse(code: ErrorCode, reason: &str, message: impl Into<String>) -> ConnectError {
    let info = proto::loams::errors::v1::ErrorInfo {
        reason: reason.to_owned(),
        ..Default::default()
    };
    ConnectError::new(code, message).with_detail(ErrorDetail::from_message(
        "loams.errors.v1.ErrorInfo",
        &info,
    ))
}

/// The answer of every stubbed RPC until AP4 (or a later AP0 task) builds it.
pub(crate) fn not_implemented(rpc: &str) -> ConnectError {
    refuse(
        ErrorCode::Unimplemented,
        "not_implemented",
        format!("{rpc} is not implemented in loams-apps-mock yet"),
    )
}
