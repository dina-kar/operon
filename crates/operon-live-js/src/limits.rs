//! CPU and memory limits (R1 plan Task 13 semantics 3): [`JsConfig`] and
//! the [`Budget`] the interrupt handler checks.
//!
//! The CPU limit counts the time a call spends running JavaScript on its
//! worker thread; time spent waiting for host calls (TiKV reads and
//! writes) does not count, since the mutation deadline bounds those. The
//! memory limit is QuickJS's per-runtime limit; every pooled context has
//! its own runtime (row T13-2).

use std::cell::Cell;
use std::time::{Duration, Instant};

/// The QuickJS engine's settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsConfig {
    /// The memory limit of each context's runtime, bundle included
    /// (64 MiB).
    pub memory_limit: usize,
    /// How long one call may run JavaScript (1 s), not counting host calls.
    /// Evaluating the bundle in a fresh context has the same limit.
    pub cpu_limit: Duration,
    /// The contexts kept warm per bundle, each on its own thread with its
    /// own runtime: at most this many calls run at once (4).
    pub contexts: usize,
}

/// [`JsConfig::memory_limit`]'s default.
pub const DEFAULT_MEMORY_LIMIT: usize = 64 * 1024 * 1024;
/// [`JsConfig::cpu_limit`]'s default.
pub const DEFAULT_CPU_LIMIT: Duration = Duration::from_secs(1);
/// [`JsConfig::contexts`]'s default.
pub const DEFAULT_CONTEXTS: usize = 4;

/// The JavaScript stack limit of a runtime; worker threads get
/// [`WORKER_STACK`] of native stack, well above it.
pub(crate) const JS_STACK: usize = 1024 * 1024;
/// The native stack of a worker thread.
pub(crate) const WORKER_STACK: usize = 8 * 1024 * 1024;

impl Default for JsConfig {
    fn default() -> Self {
        JsConfig {
            memory_limit: DEFAULT_MEMORY_LIMIT,
            cpu_limit: DEFAULT_CPU_LIMIT,
            contexts: DEFAULT_CONTEXTS,
        }
    }
}

/// The running time of the current call on one worker, which the runtime's
/// interrupt handler checks.
#[derive(Debug, Default)]
pub(crate) struct Budget {
    limit: Cell<Duration>,
    used: Cell<Duration>,
    since: Cell<Option<Instant>>,
    fired: Cell<bool>,
    abort: Cell<bool>,
}

impl Budget {
    /// Starts counting a call (or an evaluation) with `limit`.
    pub(crate) fn arm(&self, limit: Duration) {
        self.limit.set(limit);
        self.used.set(Duration::ZERO);
        self.fired.set(false);
        self.abort.set(false);
        self.since.set(Some(Instant::now()));
    }

    /// Stops counting.
    pub(crate) fn disarm(&self) {
        self.pause();
    }

    /// Stops the clock while the call waits for a host call.
    pub(crate) fn pause(&self) {
        if let Some(since) = self.since.take() {
            self.used.set(self.used.get() + since.elapsed());
        }
    }

    /// Restarts the clock.
    pub(crate) fn resume(&self) {
        self.since.set(Some(Instant::now()));
    }

    /// Makes the interrupt handler stop the running JavaScript.
    pub(crate) fn abort(&self) {
        self.abort.set(true);
    }

    /// Whether the CPU limit stopped the call.
    pub(crate) fn fired(&self) -> bool {
        self.fired.get()
    }

    /// The limit being counted against.
    pub(crate) fn limit(&self) -> Duration {
        self.limit.get()
    }

    /// The interrupt handler: `true` stops the running JavaScript with an
    /// uncatchable error.
    pub(crate) fn interrupt(&self) -> bool {
        if self.abort.get() {
            return true;
        }
        if let Some(since) = self.since.get()
            && self.used.get() + since.elapsed() > self.limit.get()
        {
            self.fired.set(true);
            return true;
        }
        false
    }
}
