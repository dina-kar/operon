//! The Operon server: one process running the metastore, the log, the range
//! cache and the background loops, serving the native HTTP API
//! (design §10 §1: `operon dev` and `operon standalone`).

pub mod api;
mod server;

pub use server::{Server, ServerConfig, ServerError};
