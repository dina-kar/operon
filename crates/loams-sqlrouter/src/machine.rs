//! The sans-I/O machine seam (§31 §7.1, D313). Every protocol is a
//! [`Machine`]: it consumes one input, returns the commands to execute and
//! never blocks or does I/O. Drivers supply time and randomness through
//! [`Ctx`]: tokio adapters in production, the seeded scheduler in simulation.
//! Network, disk and spawning do not exist inside a machine; they are
//! commands in its output.

use rand_core::Rng;

use crate::trace::TraceSink;

/// Milliseconds on the driver's clock: real or simulated.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Millis(pub u64);

impl Millis {
    /// `self + ms`, saturating at `u64::MAX` instead of wrapping.
    pub fn saturating_add(self, ms: u64) -> Millis {
        Millis(self.0.saturating_add(ms))
    }
}

/// What a machine may use besides its input: the time, a random source and
/// the spec-event sink. All three come from the driver.
pub struct Ctx<'a> {
    /// The driver's current time; the same for every output of one `on` call.
    pub now: Millis,
    /// The driver's random source: seeded in simulation, OS-seeded in production.
    pub rng: &'a mut dyn Rng,
    /// Where the machine reports each transition as a spec event.
    pub trace: &'a mut dyn TraceSink,
}

impl std::fmt::Debug for Ctx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("now", &self.now)
            .finish_non_exhaustive()
    }
}

/// A protocol as a deterministic state machine: the same inputs, time and
/// random draws always give the same outputs.
pub trait Machine {
    /// An event the driver delivers: a command result, a timer, an observation.
    type Input;
    /// A command for the driver to execute (a CAS, a reload, a fence, …).
    type Output;
    /// Consume one input and return the commands to execute. Never blocks, never does I/O.
    fn on(&mut self, ctx: &mut Ctx<'_>, input: Self::Input) -> Vec<Self::Output>;
}
