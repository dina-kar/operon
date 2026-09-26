//! The leaderless `standard` write path (design §02 §3).
//!
//! Appends from any number of callers are buffered. A flush takes the whole
//! buffer, writes it as one multi-partition WAL object, commits the object to
//! the metastore's sequencer, and only then acknowledges each append with its
//! offsets. One flush is in flight at a time (M0.3 plan, ruling 8).

use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_common::StreamId;
use operon_common::meta::{
    ApplyError, Consistency, MetaError, MetaStore, WAL_COMMIT_WINDOW_MS, WalChunk, WalClass,
    WalCommit,
};
use operon_store::{Store, StoreError};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::batch;
use crate::error::LogError;
use crate::paths;
use crate::record::Record;
use crate::wal::WalObjectBuilder;

/// How a [`LogWriter`] buffers and flushes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogConfig {
    /// This log node's id; it names the node's WAL objects.
    pub node_id: u64,
    /// A buffered append is flushed at most this long after the buffer
    /// became non-empty. Default 250 ms (design §02 §3).
    pub flush_interval: Duration,
    /// A flush starts as soon as this many bytes are buffered. Default 8 MiB.
    pub flush_bytes: usize,
    /// Buffered plus in-flight bytes beyond which `append` fails with
    /// [`LogError::Backpressure`]. Default 64 MiB.
    pub max_buffered_bytes: usize,
    /// How long the writer keeps starting new attempts to commit a WAL
    /// object before its appends fail with [`LogError::CommitUnknown`].
    /// Default 60 s, at most a third of
    /// [`WAL_COMMIT_WINDOW_MS`] (5 min). Each attempt is a metastore write,
    /// which retries on its own inside the implementation for up to its own
    /// retry budget, so a commit is given up after at most
    /// `commit_retry_deadline` plus that budget (about 75 s with the
    /// openraft implementation's defaults).
    pub commit_retry_deadline: Duration,
    /// Most records one append may carry. Default 10 000.
    pub max_batch_records: usize,
}

impl LogConfig {
    /// The defaults, for log node `node_id`.
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            flush_interval: Duration::from_millis(250),
            flush_bytes: 8 * 1024 * 1024,
            max_buffered_bytes: 64 * 1024 * 1024,
            commit_retry_deadline: Duration::from_secs(60),
            max_batch_records: 10_000,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self::new(1)
    }
}

/// The longest `commit_retry_deadline`: a third of the commit window, leaving
/// the rest for the metastore client's own retries and request timeouts.
const MAX_COMMIT_RETRY_DEADLINE: Duration = Duration::from_millis(WAL_COMMIT_WINDOW_MS / 3);

impl LogConfig {
    /// Checks the settings: at least one record per append and one buffered
    /// byte, and a commit retry deadline of at most 5 minutes.
    pub fn validate(&self) -> Result<(), LogError> {
        let invalid = |what: String| Err(LogError::InvalidArgument(what));
        if self.max_batch_records == 0 || self.flush_bytes == 0 || self.max_buffered_bytes == 0 {
            return invalid(
                "max_batch_records, flush_bytes and max_buffered_bytes must be at least 1"
                    .to_string(),
            );
        }
        if self.commit_retry_deadline > MAX_COMMIT_RETRY_DEADLINE {
            return invalid(format!(
                "commit_retry_deadline must be at most {MAX_COMMIT_RETRY_DEADLINE:?}, got {:?}",
                self.commit_retry_deadline
            ));
        }
        Ok(())
    }
}

/// Where an acknowledged append landed: offsets `base_offset..=last_offset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendAck {
    pub stream: StreamId,
    pub partition: u32,
    pub base_offset: u64,
    pub last_offset: u64,
}

/// First wait between WAL commit retries; it doubles up to 1 s. The metastore
/// client retries on its own too; this outer loop covers longer outages.
const COMMIT_BACKOFF: Duration = Duration::from_millis(50);
const COMMIT_MAX_BACKOFF: Duration = Duration::from_secs(1);

struct Pending {
    stream: StreamId,
    partition: u32,
    batch: Bytes,
    records: u32,
    max_timestamp_ms: i64,
    reply: oneshot::Sender<Result<AppendAck, LogError>>,
}

#[derive(Default)]
struct State {
    pending: Vec<Pending>,
    pending_bytes: usize,
    inflight_bytes: usize,
    /// When the buffer became non-empty.
    opened_at: Option<Instant>,
    flush_waiters: Vec<oneshot::Sender<Result<(), LogError>>>,
    /// Whether a flush is in flight.
    flushing: bool,
    /// Flush callers that arrived while a flush was in flight and nothing
    /// else was buffered: they get that flush's result.
    inflight_waiters: Vec<oneshot::Sender<Result<(), LogError>>>,
    closed: bool,
    /// The flush task has exited; nobody will answer new flush waiters.
    stopped: bool,
}

impl State {
    /// Registers a flush caller: it waits for the next flush if anything is
    /// buffered, else for the flush in flight, if any.
    fn add_waiter(&mut self, waiter: oneshot::Sender<Result<(), LogError>>) {
        if self.pending.is_empty() && self.flushing {
            self.inflight_waiters.push(waiter);
        } else {
            self.flush_waiters.push(waiter);
        }
    }
}

struct Shared {
    meta: Arc<dyn MetaStore>,
    store: Store,
    config: LogConfig,
    state: Mutex<State>,
    /// Wakes the flush task; `notify_one` keeps a permit, so no wake-up is lost.
    wake: Notify,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn close(&self) {
        self.state().closed = true;
        self.wake.notify_one();
    }
}

/// Why a flush failed, shareable by every append in it.
#[derive(Clone, Debug)]
enum FlushFailure {
    Store(Arc<StoreError>),
    Rejected(ApplyError),
    /// The leader refused the first commit attempt: nothing was committed.
    ClockSkew {
        stamped_ms: u64,
        leader_ms: u64,
    },
    Unknown(String),
}

impl FlushFailure {
    fn to_error(&self) -> LogError {
        match self {
            FlushFailure::Store(err) => LogError::Store(err.clone()),
            FlushFailure::Rejected(err) => LogError::Meta(MetaError::Rejected(err.clone())),
            FlushFailure::ClockSkew {
                stamped_ms,
                leader_ms,
            } => LogError::Meta(MetaError::ClockSkew {
                stamped_ms: *stamped_ms,
                leader_ms: *leader_ms,
            }),
            FlushFailure::Unknown(message) => LogError::CommitUnknown(message.clone()),
        }
    }
}

struct Handle {
    shared: Arc<Shared>,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Handle {
    /// The last handle is gone: flush what is buffered and stop the task.
    fn drop(&mut self) {
        self.shared.close();
    }
}

/// Appends records to stream partitions on the `standard` WAL class.
/// Cheap to clone; clones share the buffer and the flush task. Must be
/// created inside a Tokio runtime.
///
/// An acknowledged append is durable: its WAL object was written and its
/// commit applied by the metastore before the acknowledgement. Failed appends
/// were not committed, except those failing with [`LogError::CommitUnknown`],
/// which may have been. A commit rejection is reported as such only when it
/// answered the first attempt; a rejection of a retry after an attempt whose
/// outcome was unknown (for example `StaleCommit` after the first attempt's
/// commit record was pruned) is `CommitUnknown`.
#[derive(Clone)]
pub struct LogWriter {
    handle: Arc<Handle>,
}

impl std::fmt::Debug for LogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogWriter")
            .field("config", &self.handle.shared.config)
            .finish_non_exhaustive()
    }
}

impl LogWriter {
    /// Starts the writer and its flush task. Fails with
    /// [`LogError::InvalidArgument`] if the config is invalid
    /// ([`LogConfig::validate`]).
    pub fn start(
        meta: impl Into<Arc<dyn MetaStore>>,
        store: Store,
        config: LogConfig,
    ) -> Result<Self, LogError> {
        config.validate()?;
        let shared = Arc::new(Shared {
            meta: meta.into(),
            store,
            config,
            state: Mutex::new(State::default()),
            wake: Notify::new(),
        });
        let task = tokio::spawn(run(shared.clone()));
        Ok(Self {
            handle: Arc::new(Handle {
                shared,
                task: tokio::sync::Mutex::new(Some(task)),
            }),
        })
    }

    fn shared(&self) -> &Shared {
        &self.handle.shared
    }

    /// Appends `records` to one partition as one batch, and waits until they
    /// are durable. Records with a negative timestamp get the writer's clock.
    pub async fn append(
        &self,
        stream: StreamId,
        partition: u32,
        records: Vec<Record>,
    ) -> Result<AppendAck, LogError> {
        let mut acks = self.append_many(stream, vec![(partition, records)]).await?;
        acks.pop()
            .ok_or_else(|| LogError::CommitUnknown("no acknowledgement for an append".to_string()))
    }

    /// Appends one batch per partition of `stream`, all in the same WAL object
    /// and the same `CommitWal`, so the request is atomic across partitions
    /// (§01 §5): every batch is acknowledged or none is. Returns one ack per
    /// input batch, in input order.
    ///
    /// The request is validated before anything is buffered: it needs at
    /// least one batch, every batch at least one and at most
    /// `max_batch_records` records, no partition twice, and partitions of a
    /// known `standard` stream. Each ack means exactly what an
    /// [`append`](Self::append) ack means; a failed request fails with its
    /// flush's error.
    pub async fn append_many(
        &self,
        stream: StreamId,
        mut batches: Vec<(u32, Vec<Record>)>,
    ) -> Result<Vec<AppendAck>, LogError> {
        let shared = self.shared();
        let invalid = |what: String| Err(LogError::InvalidArgument(what));
        if batches.is_empty() {
            return invalid("an append needs at least one batch".to_string());
        }
        let mut seen = BTreeSet::new();
        for (partition, records) in &batches {
            if records.is_empty() {
                return invalid(format!(
                    "the batch for partition {partition} needs at least one record"
                ));
            }
            if !seen.insert(*partition) {
                return invalid(format!(
                    "partition {partition} appears more than once in one append"
                ));
            }
            if records.len() > shared.config.max_batch_records {
                return invalid(format!(
                    "a batch holds at most {} records, got {} for partition {partition}",
                    shared.config.max_batch_records,
                    records.len()
                ));
            }
        }
        let known = shared
            .meta
            .stream(Consistency::Local, stream)
            .await?
            .map(|st| (st.class, st.partitions));
        let (class, partitions) = known.ok_or(LogError::UnknownStream(stream))?;
        if let Some(&partition) = seen.iter().find(|&&p| p >= partitions) {
            return Err(LogError::UnknownPartition { stream, partition });
        }
        if class != WalClass::Standard {
            return invalid(format!(
                "stream {stream} uses WAL class {class:?}, which this build does not serve"
            ));
        }

        let now = i64::try_from(shared.meta.now_ms()).unwrap_or(i64::MAX);
        let mut encoded = Vec::with_capacity(batches.len());
        for (partition, records) in &mut batches {
            for record in records.iter_mut() {
                if record.timestamp_ms < 0 {
                    record.timestamp_ms = now;
                }
            }
            let max_timestamp_ms = records.iter().map(|r| r.timestamp_ms).max().unwrap_or(now);
            let batch = batch::encode(records)?;
            let count = u32::try_from(records.len())
                .map_err(|_| LogError::InvalidArgument("too many records".to_string()))?;
            encoded.push((*partition, batch, count, max_timestamp_ms));
        }
        let bytes: usize = encoded.iter().map(|(_, batch, _, _)| batch.len()).sum();

        let mut replies = Vec::with_capacity(encoded.len());
        {
            let mut state = shared.state();
            if state.closed {
                return Err(LogError::Closed);
            }
            let limit = shared.config.max_buffered_bytes;
            if bytes > limit {
                return invalid(format!(
                    "an append of {bytes} bytes exceeds the buffer limit of {limit} bytes"
                ));
            }
            if state.pending_bytes + state.inflight_bytes + bytes > limit {
                return Err(LogError::Backpressure);
            }
            // All in one go under the lock: the flush task takes the whole
            // buffer, so these land in the same WAL object and commit.
            state.pending_bytes += bytes;
            state.opened_at.get_or_insert_with(Instant::now);
            for (partition, batch, records, max_timestamp_ms) in encoded {
                let (reply, ack) = oneshot::channel();
                state.pending.push(Pending {
                    stream,
                    partition,
                    batch,
                    records,
                    max_timestamp_ms,
                    reply,
                });
                replies.push(ack);
            }
        }
        shared.wake.notify_one();
        // Every batch shares the flush's outcome; the first error is the error.
        let mut acks = Vec::with_capacity(replies.len());
        for reply in replies {
            acks.push(reply.await.unwrap_or_else(|_| {
                Err(LogError::CommitUnknown(
                    "the writer stopped before answering".to_string(),
                ))
            })?);
        }
        Ok(acks)
    }

    /// How many appends are buffered, waiting for the next flush (not counting
    /// a flush in flight). For monitoring and tests.
    pub fn buffered_appends(&self) -> usize {
        self.shared().state().pending.len()
    }

    /// Flushes whatever is buffered now and waits for that flush. Returns the
    /// flush's error if it failed (its appends fail with the same error).
    pub async fn flush(&self) -> Result<(), LogError> {
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.shared().state();
            if state.stopped {
                // Closed, with everything flushed.
                return Ok(());
            }
            state.add_waiter(tx);
        }
        self.shared().wake.notify_one();
        rx.await.unwrap_or(Ok(()))
    }

    /// Refuses new appends ([`LogError::Closed`]), flushes what is buffered,
    /// and waits for the flush task to stop. Returns the final flush's error,
    /// if any.
    pub async fn shutdown(&self) -> Result<(), LogError> {
        let (tx, rx) = oneshot::channel();
        let stopped = {
            let mut state = self.shared().state();
            state.closed = true;
            if !state.stopped {
                state.add_waiter(tx);
            }
            state.stopped
        };
        self.shared().wake.notify_one();
        let result = if stopped {
            Ok(())
        } else {
            rx.await.unwrap_or(Ok(()))
        };
        if let Some(task) = self.handle.task.lock().await.take()
            && let Err(err) = task.await
        {
            tracing::error!(%err, "log writer flush task failed");
        }
        result
    }
}

struct Job {
    pending: Vec<Pending>,
    bytes: usize,
    waiters: Vec<oneshot::Sender<Result<(), LogError>>>,
}

/// What the flush task should do next.
enum Next {
    Flush(Job),
    Wait(Option<Instant>),
    Exit,
}

fn next(shared: &Shared) -> Next {
    let mut state = shared.state();
    if state.pending.is_empty() {
        for waiter in state.flush_waiters.drain(..) {
            let _ = waiter.send(Ok(()));
        }
        return if state.closed {
            state.stopped = true;
            Next::Exit
        } else {
            Next::Wait(None)
        };
    }
    let deadline = state
        .opened_at
        .map(|opened| opened + shared.config.flush_interval);
    let due = state.closed
        || !state.flush_waiters.is_empty()
        || state.pending_bytes >= shared.config.flush_bytes
        || deadline.is_none_or(|d| d <= Instant::now());
    if !due {
        return Next::Wait(deadline);
    }
    let bytes = mem::take(&mut state.pending_bytes);
    state.inflight_bytes += bytes;
    state.opened_at = None;
    state.flushing = true;
    Next::Flush(Job {
        pending: mem::take(&mut state.pending),
        bytes,
        waiters: mem::take(&mut state.flush_waiters),
    })
}

async fn run(shared: Arc<Shared>) {
    loop {
        let job = match next(&shared) {
            Next::Flush(job) => job,
            Next::Exit => return,
            Next::Wait(None) => {
                shared.wake.notified().await;
                continue;
            }
            Next::Wait(Some(deadline)) => {
                tokio::select! {
                    () = shared.wake.notified() => {}
                    () = tokio::time::sleep_until(deadline.into()) => {}
                }
                continue;
            }
        };
        let Job {
            pending,
            bytes,
            waiters,
        } = job;
        let result = flush(&shared, pending).await;
        let late_waiters = {
            let mut state = shared.state();
            state.inflight_bytes -= bytes;
            state.flushing = false;
            mem::take(&mut state.inflight_waiters)
        };
        for waiter in waiters.into_iter().chain(late_waiters) {
            let _ = waiter.send(result.clone().map_err(|f| f.to_error()));
        }
    }
}

/// Writes `pending` as one WAL object, commits it, and answers every append.
async fn flush(shared: &Shared, pending: Vec<Pending>) -> Result<(), FlushFailure> {
    let outcome = write_and_commit(shared, &pending).await;
    match &outcome {
        Ok(acks) => {
            for (append, ack) in pending.into_iter().zip(acks) {
                let _ = append.reply.send(Ok(*ack));
            }
        }
        Err(failure) => {
            tracing::warn!(?failure, appends = pending.len(), "WAL flush failed");
            for append in pending {
                let _ = append.reply.send(Err(failure.to_error()));
            }
        }
    }
    outcome.map(|_| ())
}

async fn write_and_commit(
    shared: &Shared,
    pending: &[Pending],
) -> Result<Vec<AppendAck>, FlushFailure> {
    let config = &shared.config;
    let ulid = Ulid::from_parts(shared.meta.now_ms(), Ulid::generate().random());
    let mut builder = WalObjectBuilder::new(config.node_id, WalClass::Standard, ulid);
    for p in pending {
        builder.push(
            p.stream,
            p.partition,
            p.batch.clone(),
            p.records,
            p.max_timestamp_ms,
        );
    }
    let (object, metas) = builder.finish();
    let path = paths::wal_object(WalClass::Standard, config.node_id, ulid);
    shared
        .store
        .put_if_absent(&path, object)
        .await
        .map_err(|e| FlushFailure::Store(Arc::new(e)))?;
    crate::failpoint!("wal.after_put");

    let chunks: Vec<WalChunk> = metas
        .iter()
        .map(|m| WalChunk {
            stream: m.stream,
            partition: m.partition,
            records: m.records,
            byte_range: m.byte_range(),
            max_timestamp_ms: m.max_timestamp_ms,
        })
        .collect();
    let base_offsets = commit(shared, &path, ulid.timestamp_ms(), chunks).await?;
    crate::failpoint!("wal.after_commit");
    if base_offsets.len() != metas.len() {
        return Err(FlushFailure::Unknown(format!(
            "the metastore returned {} offsets for {} chunks",
            base_offsets.len(),
            metas.len()
        )));
    }

    // Each chunk holds its partition's batches in push order.
    let mut next: BTreeMap<(StreamId, u32), u64> = metas
        .iter()
        .zip(base_offsets)
        .map(|(m, base)| ((m.stream, m.partition), base))
        .collect();
    let mut acks = Vec::with_capacity(pending.len());
    for p in pending {
        let slot = next
            .get_mut(&(p.stream, p.partition))
            .ok_or_else(|| FlushFailure::Unknown("chunk missing for an append".to_string()))?;
        let base_offset = *slot;
        *slot += u64::from(p.records);
        acks.push(AppendAck {
            stream: p.stream,
            partition: p.partition,
            base_offset,
            last_offset: base_offset + u64::from(p.records) - 1,
        });
    }
    Ok(acks)
}

/// Commits a written WAL object, retrying until `commit_retry_deadline`.
///
/// A rejection or a clock-skew refusal is definite only if no earlier
/// attempt (here or inside the metastore client) ended with an unknown
/// outcome. Otherwise the first attempt may have committed the object and
/// its commit record may since have been pruned, so a `StaleCommit` for the
/// retry proves nothing: the outcome is unknown.
async fn commit(
    shared: &Shared,
    path: &str,
    created_at_ms: u64,
    chunks: Vec<WalChunk>,
) -> Result<Vec<u64>, FlushFailure> {
    let deadline = Instant::now() + shared.config.commit_retry_deadline;
    let mut backoff = COMMIT_BACKOFF;
    let mut outcome_unknown = false;
    loop {
        let tracked = shared
            .meta
            .commit_wal(WalCommit {
                object: path.to_string(),
                created_at_ms,
                chunks: chunks.clone(),
            })
            .await;
        outcome_unknown |= tracked.earlier_unknown;
        let unknown = |why: String| {
            FlushFailure::Unknown(format!(
                "committing {path}: {why}, after an attempt whose outcome is unknown; \
                 the records may be committed"
            ))
        };
        let err = match tracked.result {
            Ok(base_offsets) => return Ok(base_offsets),
            Err(MetaError::Rejected(err)) if outcome_unknown => {
                return Err(unknown(format!("rejected: {err}")));
            }
            Err(MetaError::Rejected(err)) => return Err(FlushFailure::Rejected(err)),
            Err(err @ MetaError::ClockSkew { .. }) if outcome_unknown => {
                return Err(unknown(err.to_string()));
            }
            Err(MetaError::ClockSkew {
                stamped_ms,
                leader_ms,
            }) => {
                return Err(FlushFailure::ClockSkew {
                    stamped_ms,
                    leader_ms,
                });
            }
            Err(err) => err,
        };
        let retryable = matches!(
            err,
            MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_)
        );
        // This attempt may have been applied.
        outcome_unknown = true;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !retryable || remaining.is_zero() {
            return Err(FlushFailure::Unknown(format!(
                "committing {path} failed: {err}"
            )));
        }
        tracing::warn!(%path, %err, "WAL commit failed; retrying");
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(COMMIT_MAX_BACKOFF);
    }
}
