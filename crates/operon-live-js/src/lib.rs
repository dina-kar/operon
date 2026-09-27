//! Loam Live's server functions in QuickJS (design §20 §6, D120; R1 plan
//! Task 13).
//!
//! A [`Bundle`] is one ES module whose exports are built with `query` and
//! `mutation` from `loam:server`: an object export `m` maps each function
//! property `f` to the path `m:f`. Its functions are
//! [`Function`](operon_live::Function)s, so the [`Runner`](operon_live::Runner)
//! runs them like the system functions: `ctx.db` calls go through the
//! function's `LiveTxn` and land in its read set.
//!
//! - **Values:** `bigint` ↔ `I64`, number ↔ `F64`, `ArrayBuffer` (or a
//!   `Uint8Array`) ↔ `Bytes`, strings, booleans, `null`, arrays and plain
//!   objects.
//! - **Determinism (§6.2):** `Date.now()` and `new Date()` are the start
//!   timestamp's ms; `Math.random` is a ChaCha8 stream seeded from the start
//!   timestamp and the request id; `crypto.getRandomValues` and
//!   `crypto.randomUUID` throw `DeterminismError`; there are no timers, no
//!   `fetch` and no WebAssembly. Document ids come from the host.
//! - **One context per call (semantics 2a):** each call runs in a fresh
//!   context from a warm pool and the context is dropped afterwards, so
//!   module state never carries over; built-ins are frozen before the bundle
//!   is evaluated.
//! - **Limits (semantics 3):** the CPU limit ([`JsConfig::cpu_limit`]) and
//!   the memory limit ([`JsConfig::memory_limit`]) end a call with
//!   `FunctionTimeout` and `FunctionOutOfMemory`.
//! - **Storage errors reach the runner:** a `ctx.db` call that fails with a
//!   storage error or an exceeded limit ends the call with that error at
//!   once; the function's `catch` never sees it, so the runner can rerun a
//!   conflicted mutation.
//!
//! [`JsEngine`] is the [`Engine`] `operon` gives Loam Live for `Deploy`.

mod host;
mod limits;
mod runtime;

use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;
use operon_live::deploy::{Deployment, Engine};
use operon_live::{FnKind, Function, LiveError, LiveTxn, LiveValue};

pub use host::{Host, Invocation, is_fatal};
pub use limits::{DEFAULT_CONTEXTS, DEFAULT_CPU_LIMIT, DEFAULT_MEMORY_LIMIT, JsConfig};
pub use runtime::{BUNDLE_MODULE, SERVER_MODULE};

use crate::host::TxnHost;
use crate::runtime::Pool;

/// A loaded bundle: its warm contexts and its functions. Cheap to share
/// through its functions, which keep it alive.
pub struct Bundle {
    pool: Arc<Pool>,
}

impl fmt::Debug for Bundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bundle")
            .field("functions", &self.pool.functions())
            .finish_non_exhaustive()
    }
}

impl Bundle {
    /// Evaluates `source` in a fresh context (which validates it: a syntax
    /// error, an import other than `loam:server`, a throwing or too slow
    /// top level, or a function exported without a module is
    /// [`LiveError::InvalidArgument`]) and starts the bundle's workers.
    /// Blocks while the first context warms.
    pub fn load(source: &str, config: JsConfig) -> Result<Self, LiveError> {
        Ok(Bundle {
            pool: Arc::new(Pool::start(source, &config)?),
        })
    }

    /// Every function, as `("module:export", kind)`, sorted by path.
    pub fn functions(&self) -> Vec<(String, FnKind)> {
        self.pool.functions().to_vec()
    }

    /// The function at `path`.
    pub fn function(&self, path: &str) -> Option<Arc<dyn Function>> {
        let kind = self
            .pool
            .functions()
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, kind)| *kind)?;
        Some(Arc::new(JsFunction {
            pool: self.pool.clone(),
            path: path.to_string(),
            kind,
        }))
    }

    /// Runs the function at `path` with `args`, answering its `ctx.db`
    /// calls with `host`, outside any transaction (tests and embedders; the
    /// runner calls [`function`](Self::function)'s `call`).
    pub async fn invoke(
        &self,
        path: &str,
        host: &mut dyn Host,
        invocation: Invocation,
        args: LiveValue,
    ) -> Result<LiveValue, LiveError> {
        let kind = self
            .pool
            .functions()
            .iter()
            .find(|(p, _)| p == path)
            .map(|(_, kind)| *kind)
            .ok_or_else(|| {
                LiveError::NotFound(format!("function {path:?} is not in the bundle"))
            })?;
        self.pool.invoke(path, kind, host, invocation, args).await
    }
}

impl Deployment for Bundle {
    fn functions(&self) -> Vec<(String, FnKind)> {
        Bundle::functions(self)
    }

    fn function(&self, path: &str) -> Option<Arc<dyn Function>> {
        Bundle::function(self, path)
    }
}

/// A function of a bundle.
struct JsFunction {
    pool: Arc<Pool>,
    path: String,
    kind: FnKind,
}

impl Function for JsFunction {
    fn name(&self) -> &str {
        &self.path
    }

    fn kind(&self) -> FnKind {
        self.kind
    }

    fn call<'a>(
        &'a self,
        txn: &'a mut LiveTxn<'_>,
        args: LiveValue,
    ) -> BoxFuture<'a, Result<LiveValue, LiveError>> {
        Box::pin(async move {
            let invocation = Invocation::of(txn);
            let mut host = TxnHost { txn };
            self.pool
                .invoke(&self.path, self.kind, &mut host, invocation, args)
                .await
        })
    }
}

/// The QuickJS [`Engine`]: loads bundles with its [`JsConfig`].
#[derive(Debug, Clone, Default)]
pub struct JsEngine {
    pub config: JsConfig,
}

impl JsEngine {
    /// An engine with `config`.
    pub fn new(config: JsConfig) -> Self {
        JsEngine { config }
    }
}

impl Engine for JsEngine {
    fn load(&self, source: &str) -> Result<Arc<dyn Deployment>, LiveError> {
        Ok(Arc::new(Bundle::load(source, self.config.clone())?))
    }
}
