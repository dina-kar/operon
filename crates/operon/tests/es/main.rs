//! The Elasticsearch gateway's integration tests (plan M1.5, row E3): one
//! binary, with a module per task's suite.

#[cfg(feature = "es")]
mod admin;
#[cfg(feature = "es")]
mod docs;
#[cfg(feature = "es")]
mod harness;
#[cfg(feature = "es")]
mod http;
