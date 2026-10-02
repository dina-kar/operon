//! `loams-specview`: watch the Loams router's TLA+ model checks and Rust tests
//! run in the browser (design §31, as-built tooling note in §11).
//!
//! The event model, the TLC and test-output parsers and the run model are
//! plain Rust, shared by the server and the wasm frontend. The server is behind
//! the `server` feature (default); the Leptos frontend is behind `web`.

pub mod event;
pub mod model;
pub mod testout;
pub mod tla_value;
pub mod tlc;
pub mod viewmodel;

#[cfg(feature = "server")]
pub mod runner;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "web")]
pub mod ui;
