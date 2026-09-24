//! Retention (design §02 §5): trims partitions by age and size,
//! metadata-first. Trimmed objects are retired in the metastore and deleted
//! later by garbage collection.

use std::time::Duration;

use operon_common::StreamId;
use operon_meta::{ApplyError, Consistency, MetaClient, MetaError, PartitionState};

use crate::error::LogError;
use crate::segmenter::BackgroundTask;

/// The lease that keeps one retention loop active at a time. Trims are
/// idempotent, so the lease only avoids duplicate work.
const LEASE: &str = "retention";

/// How often retention runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Default 30 s.
    pub interval: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// What one retention run did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    /// Partitions whose log start moved.
    pub trimmed: u32,
    /// WAL commit records pruned.
    pub pruned: u32,
    /// Whether another holder had the retention lease, so nothing was done.
    pub skipped: bool,
}

/// Applies stream retention policies and prunes old WAL commit records.
#[derive(Clone, Debug)]
pub struct Retention {
    meta: MetaClient,
    owner: String,
    config: RetentionConfig,
}

/// Where retention would move one partition's log start, if anywhere.
fn trim_point(
    state: &PartitionState,
    max_age_ms: Option<u64>,
    max_bytes: Option<u64>,
    now_ms: u64,
) -> Option<u64> {
    let by_age = max_age_ms.map(|age| {
        let cutoff = i64::try_from(now_ms.saturating_sub(age)).unwrap_or(i64::MAX);
        state
            .entries()
            .find(|e| e.max_timestamp_ms >= cutoff)
            .map_or(state.high_watermark(), |e| e.base_offset)
    });
    // Like Kafka, which never deletes the active segment, the newest entry is
    // always kept, even when it alone exceeds the limit: otherwise a
    // partition could be emptied right after an acknowledged append.
    let by_bytes = max_bytes.map(|limit| {
        let mut bytes = state.bytes();
        let mut before = state.log_start_offset();
        let mut entries = state.entries().peekable();
        while let Some(entry) = entries.next() {
            if bytes <= limit || entries.peek().is_none() {
                break;
            }
            bytes -= entry.byte_range.end - entry.byte_range.start;
            before = entry.end_offset();
        }
        before
    });
    let before = by_age.into_iter().chain(by_bytes).max()?;
    (before > state.log_start_offset()).then_some(before)
}

impl Retention {
    /// `owner` names this process incarnation in the retention lease.
    pub fn new(meta: MetaClient, owner: impl Into<String>, config: RetentionConfig) -> Self {
        Self {
            meta,
            owner: owner.into(),
            config,
        }
    }

    fn lease_ttl(&self) -> Duration {
        (self.config.interval * 3)
            .max(Duration::from_secs(1))
            .min(Duration::from_secs(3600))
    }

    /// Trims every partition whose policy says so, then prunes WAL commit
    /// records older than twice the commit window.
    pub async fn run_once(&self) -> Result<RetentionReport, LogError> {
        match self
            .meta
            .acquire_lease(LEASE, &self.owner, self.lease_ttl())
            .await
        {
            Ok(_) => {}
            Err(MetaError::Rejected(ApplyError::LeaseHeld { .. })) => {
                return Ok(RetentionReport {
                    skipped: true,
                    ..RetentionReport::default()
                });
            }
            Err(err) => return Err(err.into()),
        }
        let now = self.meta.now_ms();
        // One short read for the policies, then one per partition, so the
        // scan never holds the state lock for long.
        let policies: Vec<(StreamId, u32, operon_meta::Retention)> = self
            .meta
            .read(Consistency::Local, |s| {
                s.all_streams()
                    .filter(|st| {
                        st.retention.max_age_ms.is_some() || st.retention.max_bytes.is_some()
                    })
                    .map(|st| (st.id, st.partitions, st.retention))
                    .collect()
            })
            .await?;
        let mut report = RetentionReport::default();
        for (stream, partitions, policy) in policies {
            for partition in 0..partitions {
                let before = self
                    .meta
                    .read(Consistency::Local, |s| {
                        s.partition(stream, partition).and_then(|state| {
                            trim_point(state, policy.max_age_ms, policy.max_bytes, now)
                        })
                    })
                    .await?;
                if let Some(before) = before {
                    self.meta.trim_partition(stream, partition, before).await?;
                    report.trimmed += 1;
                }
            }
        }
        report.pruned = self.meta.prune_wal_commits().await?;
        Ok(report)
    }

    /// Runs retention every `interval` until the task is stopped.
    pub fn spawn(self) -> BackgroundTask {
        let interval = self.config.interval;
        BackgroundTask::spawn("retention", interval, move || {
            let retention = self.clone();
            async move {
                match retention.run_once().await {
                    Ok(report) if report.trimmed > 0 || report.pruned > 0 => {
                        tracing::debug!(?report, "retention run");
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(%err, "retention run failed"),
                }
            }
        })
    }
}
