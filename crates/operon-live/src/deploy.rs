//! The functions an app serves, and `Deploy` (design §20 §6; R1 plan
//! Tasks 12–13).
//!
//! In Task 12 an app serves the built-in system functions only
//! ([`system`](crate::system)), and `Deploy` answers `UNIMPLEMENTED`; Task 13
//! adds QuickJS bundles, their deployment pointer and the deploy gate.

use std::sync::Arc;

use crate::{Function, LiveError, system};

/// The function `name` as the app serves it: a system function, else
/// [`LiveError::NotFound`].
pub fn resolve(name: &str) -> Result<Arc<dyn Function>, LiveError> {
    system::lookup(name)
        .ok_or_else(|| LiveError::NotFound(format!("function {name:?} is not deployed")))
}

/// Why `Deploy` is refused until Task 13.
pub const DEPLOY_UNIMPLEMENTED: &str =
    "Deploy is not available yet: function bundles arrive with R1 Task 13";
