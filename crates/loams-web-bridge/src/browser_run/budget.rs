//! The local browser-time budget.
//!
//! Cloudflare meters browser time against the account and, on the free plan,
//! stops at 10 minutes a day with a `429` until midnight UTC (read on
//! 2026-10-03). A budget guard here means the operator finds out from a log
//! line instead of from a failed run, and it means a runaway loop cannot spend
//! a paid account's money in an afternoon.
//!
//! It is a guard, not a meter: it counts what this process's sessions cost in
//! wall-clock time and knows nothing about other processes. Cloudflare's
//! dashboard remains the source of truth.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::BridgeError;

/// A per-UTC-day ceiling on browser time.
pub struct BrowserBudget {
    limit_ms: u64,
    used_ms: AtomicU64,
    day: Mutex<u64>,
    now: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl std::fmt::Debug for BrowserBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserBudget")
            .field("limit_ms", &self.limit_ms)
            .field("used_ms", &self.used_ms())
            .finish()
    }
}

impl BrowserBudget {
    /// A budget of `limit_ms` milliseconds per UTC day.
    pub fn new(limit_ms: u64) -> Self {
        Self {
            limit_ms,
            used_ms: AtomicU64::new(0),
            day: Mutex::new(0),
            now: Box::new(wall_clock_ms),
        }
    }

    /// A budget that never refuses anything. Used when the local guard is off.
    pub fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    /// The ceiling in milliseconds.
    pub fn limit_ms(&self) -> u64 {
        self.limit_ms
    }

    /// What this process has spent today.
    pub fn used_ms(&self) -> u64 {
        self.roll_over();
        self.used_ms.load(Ordering::Relaxed)
    }

    /// Whether a new session may start.
    pub fn check(&self) -> Result<(), BridgeError> {
        self.roll_over();
        if self.used_ms() >= self.limit_ms {
            return Err(BridgeError::DailyBudget {
                budget_ms: self.limit_ms,
            });
        }
        Ok(())
    }

    /// Add `spent` to today's total.
    pub fn charge(&self, spent: Duration) {
        self.roll_over();
        self.used_ms.fetch_add(
            u64::try_from(spent.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Reset the counter when the UTC day changed.
    fn roll_over(&self) {
        let today = (self.now)() / 86_400_000;
        let mut day = self
            .day
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *day != today {
            *day = today;
            self.used_ms.store(0, Ordering::Relaxed);
        }
    }

    /// Replace the clock, so a test does not have to wait for midnight.
    pub fn set_clock(&mut self, now: Box<dyn Fn() -> u64 + Send + Sync>) {
        self.now = now;
    }
}

fn wall_clock_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn the_counter_resets_at_the_utc_day_boundary() {
        // A clock the test moves: the first "day" is 100 ms after the epoch,
        // the second is a day later.
        let now = Arc::new(AtomicU64::new(100));
        let clock = Arc::clone(&now);
        let mut budget = BrowserBudget::new(1_000);
        budget.set_clock(Box::new(move || clock.load(Ordering::SeqCst)));
        budget.charge(Duration::from_millis(900));
        assert!(budget.check().is_ok());
        assert_eq!(budget.used_ms(), 900);
        now.store(86_400_100, Ordering::SeqCst);
        assert_eq!(budget.used_ms(), 0, "a new day starts from zero");
    }

    #[test]
    fn spending_the_budget_refuses_the_next_session() {
        let mut budget = BrowserBudget::new(1_000);
        budget.set_clock(Box::new(|| 1));
        budget.charge(Duration::from_millis(1_000));
        let error = budget.check().err().unwrap_or_else(|| panic!("spent"));
        assert!(matches!(
            error,
            BridgeError::DailyBudget { budget_ms: 1_000 }
        ));
    }
}
