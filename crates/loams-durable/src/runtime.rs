//! [`DurableRuntime`]: Loams's own durable functions, on the Resonate Rust SDK
//! over [`InProcNetwork`] (D141, D1 Task 6).
//!
//! The SDK instance joins group `loams` as process `<node_id>`, with the
//! addresses `inproc://uni@loams/<node_id>` and `inproc://any@loams/<node_id>`.
//! Tasks reach it through the embedded server's `worker_inproc`, and its
//! requests go to the server in process: no HTTP either way.
//!
//! `Resonate::new` reads `RESONATE_TOKEN` from the environment even with a
//! custom network (T0-10). The in-process network drops that token from
//! every request, so it is harmless here.

use std::sync::Arc;
use std::time::Duration;

use resonate_sdk::resonate::{Resonate, ResonateConfig};

use crate::embed::DurableServer;
use crate::error::DurableError;
use crate::inproc::{GROUP, InProcNetwork};

/// How long [`DurableRuntime::start`] waits for the network to subscribe.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How the runtime runs its tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeOptions {
    /// The task lease: a task held by a runtime that stops heartbeating is
    /// handed out again after this long. The SDK heartbeats every `ttl / 2`.
    pub ttl: Duration,
}

impl Default for RuntimeOptions {
    /// A 60 s lease (D1 Task 6 semantics 4).
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(60),
        }
    }
}

/// The SDK instance for group `loams` on one node.
pub struct DurableRuntime {
    sdk: Resonate,
    network: Arc<InProcNetwork>,
}

impl std::fmt::Debug for DurableRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableRuntime")
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

impl DurableRuntime {
    /// Join group `loams` on `server` as process `node_id`, with the default
    /// options. Register functions through [`sdk`](Self::sdk) afterwards; a
    /// task that arrives before its function is registered is released and
    /// handed out again, so prefer [`start_with`](Self::start_with).
    pub async fn start(server: &DurableServer, node_id: &str) -> Result<Self, DurableError> {
        Self::start_with(server, node_id, RuntimeOptions::default(), |_| Ok(())).await
    }

    /// [`start`](Self::start) with `options`, calling `register` on the SDK
    /// before the runtime subscribes, so no task reaches it before its
    /// function exists.
    pub async fn start_with<F>(
        server: &DurableServer,
        node_id: &str,
        options: RuntimeOptions,
        register: F,
    ) -> Result<Self, DurableError>
    where
        F: FnOnce(&Resonate) -> resonate_sdk::error::Result<()>,
    {
        let worker = server.inproc().ok_or_else(|| {
            DurableError::Config("this durable server has no in-process worker".into())
        })?;
        let ttl = u64::try_from(options.ttl.as_millis())
            .map_err(|_| DurableError::Config("the runtime ttl is too large".into()))?;
        let network = Arc::new(InProcNetwork::new(
            Arc::downgrade(&server.server()),
            worker,
            node_id,
        ));
        let mut ready = network.ready();
        // `new` spawns `network.start()` itself; that start waits for `arm`.
        let sdk = Resonate::new(ResonateConfig {
            network: Some(Arc::clone(&network) as Arc<dyn resonate_sdk::network::Network>),
            group: Some(GROUP.to_string()),
            pid: Some(node_id.to_string()),
            ttl: Some(ttl),
            ..ResonateConfig::default()
        });
        let runtime = Self { sdk, network };
        if let Err(e) = register(&runtime.sdk) {
            runtime.stop().await;
            return Err(DurableError::Start(format!(
                "registering a durable function failed: {e}"
            )));
        }
        runtime.network.arm();
        match tokio::time::timeout(READY_TIMEOUT, ready.wait_for(|ready| *ready)).await {
            Ok(Ok(_)) => Ok(runtime),
            _ => {
                runtime.stop().await;
                Err(DurableError::Start(
                    "the in-process durable network did not start".into(),
                ))
            }
        }
    }

    /// The SDK: register Loams's functions here and run them.
    pub fn sdk(&self) -> &Resonate {
        &self.sdk
    }

    /// Stop taking tasks: unsubscribe, stop heartbeating, and fail every
    /// later request. A task still running is abandoned as in a crash; the
    /// server hands it out again once its lease lapses.
    pub async fn stop(self) {
        if let Err(e) = self.sdk.stop().await {
            tracing::warn!(error = %e, "the durable runtime did not stop cleanly");
        }
    }
}
