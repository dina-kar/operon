//! The Qdrant gateway's integration tests (plan M1.4). Task 0 pins the
//! M1.2 contracts the gateway relies on; Task 2 serves the listeners;
//! later tasks add the gateway's own suites here.

#[cfg(feature = "qdrant")]
mod collections;
mod contract;
#[cfg(feature = "qdrant")]
mod filters;
#[cfg(feature = "qdrant")]
mod groups;
#[cfg(feature = "qdrant")]
mod harness;
#[cfg(feature = "qdrant")]
mod query;
#[cfg(feature = "qdrant")]
mod reads;
#[cfg(feature = "qdrant")]
mod scored;
#[cfg(feature = "qdrant")]
mod service;
#[cfg(feature = "qdrant")]
mod writes;
