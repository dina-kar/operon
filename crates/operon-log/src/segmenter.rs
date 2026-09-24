//! The segmenter (design §02 §5): rewrites runs of WAL chunks into
//! per-partition segments and swaps them into the offset index.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use operon_cache::RangeCache;
use operon_common::{NamespaceId, StreamId};
use operon_meta::{
    ApplyError, Consistency, EntryKind, Fence, IndexEntry, MetaClient, MetaError, WalClass,
};
use operon_store::Store;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::batch;
use crate::error::{LogError, corrupt};
use crate::paths;
use crate::record::Encoding;
use crate::segment::SegmentBuilder;

/// When and how the segmenter runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmenterConfig {
    /// A run of WAL entries is segmented once it holds this many bytes.
    /// Default 64 MiB.
    pub min_bytes: u64,
    /// A segment holds at most this many bytes of WAL entries (at least one
    /// entry). Default 256 MiB.
    pub target_bytes: u64,
    /// A run is segmented regardless of size once its oldest entry's newest
    /// record timestamp is this old. Default 10 min.
    pub max_wal_age: Duration,
    /// Time between runs. Default 5 s.
    pub interval: Duration,
    /// The per-partition lease the segmenter takes. Default 30 s.
    pub lease_ttl: Duration,
}

impl Default for SegmenterConfig {
    fn default() -> Self {
        Self {
            min_bytes: 64 * 1024 * 1024,
            target_bytes: 256 * 1024 * 1024,
            max_wal_age: Duration::from_secs(600),
            interval: Duration::from_secs(5),
            lease_ttl: Duration::from_secs(30),
        }
    }
}

/// What one segmenter run did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmenterReport {
    /// Segments written and swapped in.
    pub segments: u32,
    /// Partitions skipped because another holder had the lease, or because
    /// the index changed under the run (the new segment was deleted).
    pub skipped: u32,
    /// Partitions whose attempt failed (logged; retried next run).
    pub failed: u32,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A background loop started with `spawn`.
#[derive(Debug)]
pub struct BackgroundTask {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl BackgroundTask {
    /// Runs `run` now and then every `interval`, until stopped.
    pub(crate) fn spawn<F, Fut>(name: &'static str, interval: Duration, run: F) -> Self
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let (stop, mut stopped) = watch::channel(false);
        let handle = tokio::spawn(async move {
            loop {
                run().await;
                tokio::select! {
                    _ = stopped.wait_for(|s| *s) => break,
                    () = tokio::time::sleep(interval) => {}
                }
            }
            tracing::debug!(task = name, "background task stopped");
        });
        Self { stop, handle }
    }

    /// Stops the loop after its current run, and waits for it.
    pub async fn stop(self) {
        self.stop.send_replace(true);
        if let Err(err) = self.handle.await {
            tracing::error!(%err, "background task failed");
        }
    }
}

/// A run of WAL entries of one partition to segment.
struct Candidate {
    namespace: NamespaceId,
    stream: StreamId,
    partition: u32,
    entries: Vec<IndexEntry>,
}

/// Rewrites WAL chunks into segments. Each partition is guarded by the lease
/// `segmenter/<stream>/<partition>`, and the swap is fenced by it, so a
/// segmenter that lost its lease cannot change the index. Replaced WAL
/// objects are not deleted: the metastore retires them, and garbage
/// collection deletes them later.
#[derive(Clone, Debug)]
pub struct Segmenter {
    meta: MetaClient,
    store: Store,
    cache: RangeCache,
    owner: String,
    config: SegmenterConfig,
}

impl Segmenter {
    /// `owner` names this process incarnation in leases; it must be unique
    /// per process (see `Command::AcquireLease`).
    pub fn new(
        meta: MetaClient,
        store: Store,
        cache: RangeCache,
        owner: impl Into<String>,
        config: SegmenterConfig,
    ) -> Self {
        Self {
            meta,
            store,
            cache,
            owner: owner.into(),
            config,
        }
    }

    /// Segments one run of WAL entries in every partition that is due.
    pub async fn run_once(&self) -> Result<SegmenterReport, LogError> {
        let now = self.meta.now_ms();
        let cutoff =
            i64::try_from(now.saturating_sub(millis(self.config.max_wal_age))).unwrap_or(i64::MAX);
        let config = &self.config;
        let candidates = self
            .meta
            .read(Consistency::Local, |s| {
                let mut out = Vec::new();
                for stream in s.all_streams().filter(|st| st.class == WalClass::Standard) {
                    for partition in 0..stream.partitions {
                        let Some(state) = s.partition(stream.id, partition) else {
                            continue;
                        };
                        let mut entries = Vec::new();
                        let mut bytes = 0;
                        for entry in state
                            .entries()
                            .skip_while(|e| e.kind == EntryKind::Segment)
                            .take_while(|e| e.kind == EntryKind::Wal)
                        {
                            let len = entry.byte_range.end - entry.byte_range.start;
                            if !entries.is_empty() && bytes + len > config.target_bytes {
                                break;
                            }
                            bytes += len;
                            entries.push(entry.clone());
                        }
                        let Some(first) = entries.first() else {
                            continue;
                        };
                        if bytes >= config.min_bytes || first.max_timestamp_ms < cutoff {
                            out.push(Candidate {
                                namespace: stream.namespace,
                                stream: stream.id,
                                partition,
                                entries,
                            });
                        }
                    }
                }
                out
            })
            .await?;

        let mut report = SegmenterReport::default();
        for candidate in candidates {
            match self.segment(&candidate).await {
                Ok(true) => report.segments += 1,
                Ok(false) => report.skipped += 1,
                Err(err) => {
                    tracing::warn!(
                        stream = %candidate.stream,
                        partition = candidate.partition,
                        %err,
                        "segmenting failed"
                    );
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }

    /// Writes and swaps in one segment. `Ok(false)` if the partition was
    /// skipped.
    async fn segment(&self, candidate: &Candidate) -> Result<bool, LogError> {
        let Candidate {
            namespace,
            stream,
            partition,
            entries,
        } = candidate;
        let (stream, partition) = (*stream, *partition);
        let lease = format!("segmenter/{stream}/{partition}");
        let grant = match self
            .meta
            .acquire_lease(&lease, &self.owner, self.config.lease_ttl)
            .await
        {
            Ok(grant) => grant,
            Err(MetaError::Rejected(ApplyError::LeaseHeld { .. })) => return Ok(false),
            Err(err) => return Err(err.into()),
        };

        let base_offset = entries[0].base_offset;
        let mut builder = SegmentBuilder::new(stream, partition, base_offset, Encoding::Kafka);
        let mut max_timestamp_ms = i64::MIN;
        for entry in entries {
            let bytes = self
                .cache
                .read(&entry.object, entry.byte_range.clone())
                .await?;
            for batch in batch::batches(&bytes) {
                let batch = batch?;
                builder
                    .push_batch(
                        Bytes::copy_from_slice(batch.bytes),
                        batch.record_count,
                        batch.max_timestamp_ms,
                    )
                    .map_err(|e| corrupt(format!("WAL chunk in {}: {e}", entry.object)))?;
            }
            if builder.next_offset() != entry.end_offset() {
                return Err(corrupt(format!(
                    "WAL chunk in {} holds records up to {}, its index entry up to {}",
                    entry.object,
                    builder.next_offset(),
                    entry.end_offset()
                )));
            }
            max_timestamp_ms = max_timestamp_ms.max(entry.max_timestamp_ms);
        }
        let (bytes, footer) = builder.finish();
        let ulid = Ulid::from_parts(self.meta.now_ms(), Ulid::generate().random());
        let path = paths::segment(*namespace, stream, partition, base_offset, ulid);
        self.store.put_if_absent(&path, bytes).await?;

        let replaces = entries
            .iter()
            .map(|e| (e.base_offset, e.object.clone()))
            .collect();
        let fence = Fence {
            lease,
            epoch: grant.epoch,
        };
        let swapped = self
            .meta
            .swap_segment(
                stream,
                partition,
                replaces,
                &path,
                footer.data,
                max_timestamp_ms,
                Some(fence),
            )
            .await;
        match swapped {
            Ok(()) => Ok(true),
            // Definitely not applied (a retry of an applied swap succeeds), so
            // the new segment is unreferenced.
            Err(MetaError::Rejected(
                ApplyError::IndexMismatch { .. } | ApplyError::Fenced { .. },
            )) => {
                if let Err(err) = self.store.delete(&path).await {
                    tracing::warn!(%path, %err, "could not delete an unused segment");
                }
                Ok(false)
            }
            // The outcome may be unknown: leave the object for garbage
            // collection if the swap did not land.
            Err(err) => Err(err.into()),
        }
    }

    /// Runs the segmenter every `interval` until the task is stopped.
    pub fn spawn(self) -> BackgroundTask {
        let interval = self.config.interval;
        BackgroundTask::spawn("segmenter", interval, move || {
            let segmenter = self.clone();
            async move {
                match segmenter.run_once().await {
                    Ok(report) if report != SegmenterReport::default() => {
                        tracing::debug!(?report, "segmenter run");
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(%err, "segmenter run failed"),
                }
            }
        })
    }
}
