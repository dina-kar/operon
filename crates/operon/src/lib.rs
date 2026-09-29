//! The Operon server: one process running the metastore, the log, the range
//! cache and the background loops, serving the native HTTP API
//! (design §10 §1: `operon dev` and `operon standalone`).

pub mod api;
pub mod cluster;
mod meta_backend;
#[cfg(feature = "pgwire")]
pub mod pg;
mod server;

pub use meta_backend::{MetaBackend, NO_TIKV_FEATURE, TIKV_SCHEME};
pub use server::{ClusterConfig, Server, ServerConfig, ServerError};
