//! The unapplied-data budget and write backpressure (plan M1.3 Task 15, D86).
//!
//! A collection's **backlog** is what its implicit stream holds past the
//! live manifest's `applied` offsets: records, and the bytes of the offset
//! index entries that hold them (pro rata for an entry that straddles
//! `applied`). [`BacklogMonitor::admit`] refuses a collection write while the
//! backlog is at or over the budget, with a `Retry-After` from the apply
//! rate.
//!
//! The budget is soft: admission reserves nothing, so writes admitted
//! against one cached measurement can overshoot it until the next refresh.
//! `ServerConfig::validate` keeps the byte budget at most half the live
//! tail, so an overshoot of up to one more budget still fits the tail; past
//! that, strong reads fall back to range tails (D86), which costs speed, not
//! correctness.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use loams_collection::CollectionContext;
use loams_common::meta::{Collection, Consistency, IndexEntry};
use loams_common::{CollectionId, NamespaceId};
use serde::{Deserialize, Serialize};

use crate::error::ServiceError;

/// The budget and how admission measures it (D86).
#[derive(Clone, Debug, PartialEq)]
pub struct BackpressureConfig {
    /// true.
    pub enabled: bool,
    /// 1 000 000.
    pub max_unapplied_records: u64,
    /// 128 MiB; `ServerConfig::validate` keeps it at most `tail.max_bytes / 2`.
    pub max_unapplied_bytes: u64,
    /// 4: a `Bulk` write is admitted below 4 × each budget.
    pub override_factor: u64,
    /// 250 ms: an older measurement is refreshed before admission.
    pub refresh_interval: Duration,
    /// 60 s: the window of the apply-rate estimate.
    pub rate_window: Duration,
    /// 1 s.
    pub min_retry_after: Duration,
    /// 30 s.
    pub max_retry_after: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_unapplied_records: 1_000_000,
            max_unapplied_bytes: 128 << 20,
            override_factor: 4,
            refresh_interval: Duration::from_millis(250),
            rate_window: Duration::from_secs(60),
            min_retry_after: Duration::from_secs(1),
            max_retry_after: Duration::from_secs(30),
        }
    }
}

/// What a collection's implicit stream holds past `applied`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Backlog {
    pub records: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureState {
    /// A plain write is admitted now.
    #[default]
    Open,
    /// A plain write is refused now.
    Throttling,
    /// Backpressure is off: nothing is refused.
    Disabled,
}

/// The backpressure part of `CollectionInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackpressureStatus {
    pub state: BackpressureState,
    pub unapplied_records: u64,
    pub unapplied_bytes: u64,
    pub max_unapplied_records: u64,
    pub max_unapplied_bytes: u64,
}

/// A write's override of the budget: `Bulk` (`Loams-Backpressure: off`)
/// is admitted up to `override_factor` × each budget.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Override {
    #[default]
    None,
    Bulk,
}

/// Per-process counts (M1.7 Task 11 exposes them).
#[derive(Debug, Default)]
pub struct BackpressureCounters {
    /// Writes refused.
    pub throttled_writes: AtomicU64,
    /// `Bulk` writes admitted while the backlog was over a plain budget.
    pub override_writes: AtomicU64,
}

/// Bytes of `entries` (one partition's offset index entries) above
/// `applied`: each entry's byte range, pro rata for its records above
/// `applied` and rounded up (rule 1.3).
pub fn unapplied_bytes<'a>(entries: impl IntoIterator<Item = &'a IndexEntry>, applied: u64) -> u64 {
    entries
        .into_iter()
        .filter(|entry| entry.end_offset() > applied && entry.records > 0)
        .map(|entry| {
            let len = u128::from(entry.byte_range.end.saturating_sub(entry.byte_range.start));
            let above = u128::from(entry.end_offset() - entry.base_offset.max(applied));
            let records = u128::from(entry.records);
            u64::try_from((len * above).div_ceil(records)).unwrap_or(u64::MAX)
        })
        .sum()
}

/// Rule 4: how long a refused writer should wait, from the apply rate
/// `rate` (records per second): the time to bring the backlog to 90 % of
/// each budget, clamped to `[min_retry_after, max_retry_after]`; a stalled
/// link (`rate` 0) gives `max_retry_after`.
pub fn retry_after(config: &BackpressureConfig, backlog: Backlog, rate: f64) -> Duration {
    let floor_90 = |budget: u64| (u128::from(budget) * 9 / 10) as u64;
    let excess_records = backlog
        .records
        .saturating_sub(floor_90(config.max_unapplied_records));
    let excess_bytes = backlog
        .bytes
        .saturating_sub(floor_90(config.max_unapplied_bytes));
    let wait = if rate <= 0.0 || !rate.is_finite() {
        config.max_retry_after
    } else {
        let byte_rate = match backlog.records {
            0 => 0.0,
            records => rate * backlog.bytes as f64 / records as f64,
        };
        let for_records = excess_records as f64 / rate;
        let for_bytes = match (excess_bytes, byte_rate > 0.0) {
            (0, _) => 0.0,
            (_, true) => excess_bytes as f64 / byte_rate,
            (_, false) => f64::INFINITY,
        };
        let seconds = for_records.max(for_bytes);
        if seconds.is_finite() {
            Duration::try_from_secs_f64(seconds).unwrap_or(config.max_retry_after)
        } else {
            config.max_retry_after
        }
    };
    // Not `clamp`, which panics on an inverted config (a monitor built
    // without `ServerConfig::validate`): then `max_retry_after` wins.
    wait.max(config.min_retry_after).min(config.max_retry_after)
}

/// One measurement and the rate samples of a collection.
#[derive(Debug, Default)]
struct Measurements {
    last: Option<(Instant, Backlog)>,
    /// `(time, Σ applied)` of each refresh within `rate_window`, oldest first.
    samples: VecDeque<(Instant, u64)>,
}

impl Measurements {
    /// The apply rate in records per second (rule 1.5).
    fn rate(&self) -> f64 {
        match (self.samples.front(), self.samples.back()) {
            (Some((t0, a0)), Some((t1, a1))) if self.samples.len() >= 2 => {
                let seconds = t1.saturating_duration_since(*t0).as_secs_f64();
                if seconds > 0.0 {
                    a1.saturating_sub(*a0) as f64 / seconds
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }
}

/// Measures collection backlogs and admits writes against the budget.
#[derive(Debug)]
pub struct BacklogMonitor {
    ctx: CollectionContext,
    config: BackpressureConfig,
    /// Per collection; the async lock makes at most one refresh run per
    /// collection, and concurrent callers wait for it (rule 1.4).
    collections: Mutex<HashMap<CollectionId, Arc<tokio::sync::Mutex<Measurements>>>>,
    counters: BackpressureCounters,
    refreshes: AtomicU64,
}

impl BacklogMonitor {
    pub fn new(ctx: CollectionContext, config: BackpressureConfig) -> Self {
        Self {
            ctx,
            config,
            collections: Mutex::default(),
            counters: BackpressureCounters::default(),
            refreshes: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &BackpressureConfig {
        &self.config
    }

    /// The collection's backlog, measured at most `refresh_interval` ago
    /// (rule 1).
    pub async fn backlog(
        &self,
        ns: NamespaceId,
        collection: &Collection,
    ) -> Result<Backlog, ServiceError> {
        Ok(self.measured(ns, collection).await?.0)
    }

    /// The backlog when the write may proceed; `ResourceExhausted` otherwise
    /// (rules 3–4).
    pub async fn admit(
        &self,
        ns: NamespaceId,
        collection: &Collection,
        ov: Override,
    ) -> Result<Backlog, ServiceError> {
        let (backlog, rate) = self.measured(ns, collection).await?;
        let config = &self.config;
        if !config.enabled {
            return Ok(backlog);
        }
        let factor = match ov {
            Override::None => 1,
            Override::Bulk => config.override_factor.max(1),
        };
        let over = |factor: u64| {
            backlog.records >= config.max_unapplied_records.saturating_mul(factor)
                || backlog.bytes >= config.max_unapplied_bytes.saturating_mul(factor)
        };
        if over(factor) {
            self.counters
                .throttled_writes
                .fetch_add(1, Ordering::Relaxed);
            let wait = retry_after(config, backlog, rate);
            let wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
            return Err(ServiceError::ResourceExhausted {
                message: format!(
                    "collection {}: {} records ({} bytes) are not yet applied (budget {} records, {} bytes); retry after {} s",
                    collection.name,
                    backlog.records,
                    backlog.bytes,
                    config.max_unapplied_records,
                    config.max_unapplied_bytes,
                    wait_ms.div_ceil(1000),
                ),
                retry_after_ms: wait_ms,
            });
        }
        if ov == Override::Bulk && over(1) {
            self.counters
                .override_writes
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(backlog)
    }

    /// The status of `backlog` (rule 6): `Throttling` iff a plain write
    /// would be refused.
    pub fn status(&self, backlog: Backlog) -> BackpressureStatus {
        let config = &self.config;
        let state = if !config.enabled {
            BackpressureState::Disabled
        } else if backlog.records >= config.max_unapplied_records
            || backlog.bytes >= config.max_unapplied_bytes
        {
            BackpressureState::Throttling
        } else {
            BackpressureState::Open
        };
        BackpressureStatus {
            state,
            unapplied_records: backlog.records,
            unapplied_bytes: backlog.bytes,
            max_unapplied_records: config.max_unapplied_records,
            max_unapplied_bytes: config.max_unapplied_bytes,
        }
    }

    pub fn counters(&self) -> &BackpressureCounters {
        &self.counters
    }

    /// How many measurements have been taken (each is one `collection_head`
    /// read; row 15.2).
    pub fn refreshes(&self) -> u64 {
        self.refreshes.load(Ordering::Relaxed)
    }

    /// The cached backlog and apply rate, refreshed if older than
    /// `refresh_interval`.
    async fn measured(
        &self,
        ns: NamespaceId,
        collection: &Collection,
    ) -> Result<(Backlog, f64), ServiceError> {
        let slot = self
            .collections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(collection.id)
            .or_default()
            .clone();
        let mut measurements = slot.lock().await;
        if let Some((at, backlog)) = measurements.last
            && at.elapsed() < self.config.refresh_interval
        {
            return Ok((backlog, measurements.rate()));
        }
        let (backlog, applied) = self.measure(ns, collection).await?;
        self.refreshes.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        measurements.last = Some((now, backlog));
        measurements.samples.push_back((now, applied));
        while measurements
            .samples
            .front()
            .is_some_and(|(at, _)| now.saturating_duration_since(*at) > self.config.rate_window)
        {
            measurements.samples.pop_front();
        }
        Ok((backlog, measurements.rate()))
    }

    /// Rule 1.1–1.3: the backlog, and Σ applied.
    async fn measure(
        &self,
        ns: NamespaceId,
        collection: &Collection,
    ) -> Result<(Backlog, u64), ServiceError> {
        let meta = &self.ctx.meta;
        let head = meta
            .collection_head(Consistency::Local, collection.id)
            .await?
            .filter(|head| head.collection.namespace == ns)
            .ok_or_else(|| ServiceError::NotFound {
                kind: "collection",
                name: collection.name.clone(),
            })?;
        let applied = match &head.pointer {
            Some(pointer) => self
                .ctx
                .manifests
                .load(&self.ctx.store, &pointer.value)
                .await?
                .applied
                .clone(),
            None => Default::default(),
        };
        let mut backlog = Backlog::default();
        for partition in 0..head.collection.partitions {
            let hwm = head
                .high_watermarks
                .get(partition as usize)
                .copied()
                .unwrap_or(0);
            let applied_p = applied.get(&partition).copied().unwrap_or(0);
            if hwm <= applied_p {
                continue;
            }
            backlog.records += hwm - applied_p;
            if let Some(index) = meta
                .partition_index(
                    Consistency::Local,
                    head.collection.stream,
                    partition,
                    applied_p,
                    None,
                )
                .await?
            {
                backlog.bytes += unapplied_bytes(index.entries(), applied_p);
            }
        }
        Ok((backlog, applied.values().sum()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_rate_or_no_excess_is_clamped() {
        let config = BackpressureConfig {
            max_unapplied_records: 1000,
            max_unapplied_bytes: 1 << 20,
            ..BackpressureConfig::default()
        };
        let backlog = Backlog {
            records: 1000,
            bytes: 1000,
        };
        assert_eq!(retry_after(&config, backlog, 0.0), config.max_retry_after);
        assert_eq!(
            retry_after(&config, Backlog::default(), 5.0),
            config.min_retry_after
        );
    }
}
