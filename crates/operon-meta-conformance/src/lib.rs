//! The backend-agnostic conformance suite of Operon's
//! [`MetaStore`](operon_common::meta::MetaStore) trait (M1.2a plan, Ruling
//! 14; design §12 §2 item 5), and the linearizability checker its histories
//! and the simulation's are checked with.
//!
//! A backend crate implements [`Backend`] (a fresh, empty metastore per
//! case, with optional [`Faults`]) and expands the suite in one of its test
//! files:
//!
//! ```ignore
//! mod single_node {
//!     operon_meta_conformance::metastore_conformance!(super::SingleNode);
//! }
//! ```
//!
//! That expands one `#[tokio::test]` per entry of [`CASES`], so the backend
//! crate needs `tokio` with its `macros` and `rt-multi-thread` features. Each
//! case asserts the exact [`ApplyError`](operon_common::meta::ApplyError)
//! variants the trait documents, uses its own names and keys, and must end
//! within [`CASE_LIMIT`]. A case that needs fault injection prints
//! `skipped: <case> needs fault injection` and passes when the backend has
//! none.

mod backend;
pub mod linearizability;
mod macros;
pub mod suite;

use std::future::Future;
use std::time::Duration;

pub use backend::{Backend, Faults, Instance};
pub use suite::CASES;

/// The longest a case may run.
pub const CASE_LIMIT: Duration = Duration::from_secs(60);

/// Runs `case`, failing it if it takes longer than [`CASE_LIMIT`].
#[doc(hidden)]
pub async fn bounded(name: &str, case: impl Future<Output = ()>) {
    if tokio::time::timeout(CASE_LIMIT, case).await.is_err() {
        panic!("{name} did not finish within {CASE_LIMIT:?}");
    }
}
