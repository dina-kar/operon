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
//! The embed leaves its host alone: no tracing subscriber, no signal handler,
//! no panic hook, and a handler panic answers 500.

mod config;
mod embed;
mod error;
pub mod inproc;
mod listen;
mod registry;

pub use config::{
    DEFAULT_LISTEN, DEFAULT_RETRY_TIMEOUT, DEFAULT_SHUTDOWN_TIMEOUT, DurableConfig, DurableStore,
    MysqlTls, PROTECTED,
};
pub use embed::{DurableServer, LOCK_FILE, PROTOCOL_VERSION};
pub use error::DurableError;
pub use listen::{is_loopback, parse_listen};
pub use registry::registry;
pub use resonate_plugin::ResonateServer;
