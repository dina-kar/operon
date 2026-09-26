//! The Qdrant gateway's integration tests (plan M1.4). Task 0 pins the
//! M1.2 contracts the gateway relies on; Task 2 serves the listeners;
//! later tasks add the gateway's own suites here.

mod contract;
#[cfg(feature = "qdrant")]
mod harness;
#[cfg(feature = "qdrant")]
mod service;
