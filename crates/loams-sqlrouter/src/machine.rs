//! The sans-I/O machine seam (§31 §7.1, D313). Every protocol is a
//! [`Machine`]: it consumes one input, returns the commands to execute and
//! never blocks or does I/O. Drivers supply time and randomness through
//! [`Ctx`]: tokio adapters in production, the seeded scheduler in simulation.
//! Network, disk and spawning do not exist inside a machine; they are
//! commands in its output.

use rand_core::RngCore;

use crate::trace::TraceSink;

/// Milliseconds on the driver's clock: real or simulated.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Millis(pub u64);

impl Millis {
    pub fn saturating_add(self, ms: u64) -> Millis {
        Millis(self.0.saturating_add(ms))
    }
}

/// What a machine may use besides its input: the time, a random source and
/// the spec-event sink. All three come from the driver.
pub struct Ctx<'a> {
    pub now: Millis,
    pub rng: &'a mut dyn RngCore,
    pub trace: &'a mut dyn TraceSink,
}

impl std::fmt::Debug for Ctx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("now", &self.now)
            .finish_non_exhaustive()
    }
}

pub trait Machine {
    type Input;
    type Output;
    /// Consume one input and return the commands to execute. Never blocks, never does I/O.
    fn on(&mut self, ctx: &mut Ctx<'_>, input: Self::Input) -> Vec<Self::Output>;
}
