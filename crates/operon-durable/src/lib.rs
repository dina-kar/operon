//! Durable execution for Operon (D1, design §21): the Resonate server
//! embedded in the `operon` process.
//!
//! [`DurableServer`] builds Resonate from its plugins ([`registry`]) with a
//! configuration made from Loam's flags alone ([`DurableConfig`]), serves the
//! durable API on a loopback listener (127.0.0.1:8001 by default, D138), and
//! answers in-process protocol calls ([`DurableServer::process`]). The store
//! is SQLite for `operon dev` and `standalone`, and a MySQL-protocol database
//! (TiDB) with the `mysql` feature (D139).
//!
//! [`DurableRuntime`] runs Loam's own durable functions on the Resonate Rust
//! SDK over [`InProcNetwork`], which reaches the server through its
//! `worker_inproc` plugin with no socket (D141).
//!
//! [`Operations`] is the operations API (D146): an operation is a durable
//! promise, submitted with an optional idempotency key and then polled.
//!
//! The embed leaves its host alone: no tracing subscriber, no signal handler,
//! no panic hook, and a handler panic answers 500.

mod config;
mod embed;
mod error;
mod ids;
pub mod inproc;
mod listen;
#[cfg(feature = "mysql")]
mod mysql;
pub mod ops;
mod registry;
mod runtime;

pub use config::{
    DEFAULT_DATABASE, DEFAULT_LISTEN, DEFAULT_RETRY_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT,
    DurableConfig, DurableStore, MysqlTls, PROTECTED, redact_url,
};
pub use embed::{DurableClient, DurableServer, LOCK_FILE, PROTOCOL_VERSION};
pub use error::DurableError;
pub use ids::{OPERATION_PREFIX, OperationId};
pub use inproc::{InProcNetwork, InProcWorker};
pub use listen::{is_loopback, parse_listen};
pub use ops::{
    OpInput, Operation, OperationError, OperationKinds, OperationState, Operations, OpsConfig,
    OpsError,
};
pub use registry::registry;
pub use resonate_plugin::ResonateServer;
pub use runtime::{DurableRuntime, RuntimeOptions};
