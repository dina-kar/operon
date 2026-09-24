//! The leaderless `standard` write path (design §02 §3).
//!
//! Appends from any number of callers are buffered. A flush takes the whole
//! buffer, writes it as one multi-partition WAL object, commits the object to
//! the metastore's sequencer, and only then acknowledges each append with its
//! offsets. One flush is in flight at a time (M0.3 plan, ruling 8).

use std::collections::BTreeMap;
use std::mem;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use operon_common::StreamId;
use operon_meta::{ApplyError, Consistency, MetaClient, MetaError, WalChunk, WalClass};
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
    /// How long a WAL commit is retried before its appends fail with
    /// [`LogError::CommitUnknown`]. Must stay well below
    /// [`operon_meta::WAL_COMMIT_WINDOW_MS`]. Default 60 s.
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
    closed: bool,
    /// The flush task has exited; nobody will answer new flush waiters.
    stopped: bool,
}

struct Shared {
    meta: MetaClient,
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
    Unknown(String),
}

impl FlushFailure {
    fn to_error(&self) -> LogError {
        match self {
            FlushFailure::Store(err) => LogError::Store(err.clone()),
            FlushFailure::Rejected(err) => LogError::Meta(MetaError::Rejected(err.clone())),
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
/// which may have been.
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
    /// Starts the writer and its flush task.
    pub fn start(meta: MetaClient, store: Store, config: LogConfig) -> Self {
        let shared = Arc::new(Shared {
            meta,
            store,
            config,
            state: Mutex::new(State::default()),
            wake: Notify::new(),
        });
        let task = tokio::spawn(run(shared.clone()));
        Self {
            handle: Arc::new(Handle {
                shared,
                task: tokio::sync::Mutex::new(Some(task)),
            }),
        }
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
        mut records: Vec<Record>,
    ) -> Result<AppendAck, LogError> {
        let shared = self.shared();
        if records.is_empty() {
            return Err(LogError::InvalidArgument(
                "an append needs at least one record".to_string(),
            ));
        }
        if records.len() > shared.config.max_batch_records {
            return Err(LogError::InvalidArgument(format!(
                "an append holds at most {} records, got {}",
                shared.config.max_batch_records,
                records.len()
            )));
        }
        let class = shared
            .meta
            .read(Consistency::Local, |s| {
                s.stream(stream)
                    .map(|st| (st.class, partition < st.partitions))
            })
            .await?;
        match class {
            None => return Err(LogError::UnknownStream(stream)),
            Some((_, false)) => return Err(LogError::UnknownPartition { stream, partition }),
            Some((WalClass::Standard, true)) => {}
            Some((class, true)) => {
                return Err(LogError::InvalidArgument(format!(
                    "stream {stream} uses WAL class {class:?}, which this build does not serve"
                )));
            }
        }
        let now = i64::try_from(shared.meta.now_ms()).unwrap_or(i64::MAX);
        for record in &mut records {
            if record.timestamp_ms < 0 {
                record.timestamp_ms = now;
            }
        }
        let max_timestamp_ms = records.iter().map(|r| r.timestamp_ms).max().unwrap_or(now);
        let batch = batch::encode(&records)?;
        let count = u32::try_from(records.len())
            .map_err(|_| LogError::InvalidArgument("too many records".to_string()))?;

        let (reply, ack) = oneshot::channel();
        {
            let mut state = shared.state();
            if state.closed {
                return Err(LogError::Closed);
            }
            let limit = shared.config.max_buffered_bytes;
            if batch.len() > limit {
                return Err(LogError::InvalidArgument(format!(
                    "an append of {} bytes exceeds the buffer limit of {limit} bytes",
                    batch.len()
                )));
            }
            if state.pending_bytes + state.inflight_bytes + batch.len() > limit {
                return Err(LogError::Backpressure);
            }
            state.pending_bytes += batch.len();
            state.opened_at.get_or_insert_with(Instant::now);
            state.pending.push(Pending {
                stream,
                partition,
                batch,
                records: count,
                max_timestamp_ms,
                reply,
            });
        }
        shared.wake.notify_one();
        ack.await.unwrap_or_else(|_| {
            Err(LogError::CommitUnknown(
                "the writer stopped before answering".to_string(),
            ))
        })
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
            state.flush_waiters.push(tx);
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
                state.flush_waiters.push(tx);
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
        shared.state().inflight_bytes -= bytes;
        for waiter in waiters {
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
async fn commit(
    shared: &Shared,
    path: &str,
    created_at_ms: u64,
    chunks: Vec<WalChunk>,
) -> Result<Vec<u64>, FlushFailure> {
    let deadline = Instant::now() + shared.config.commit_retry_deadline;
    let mut backoff = COMMIT_BACKOFF;
    loop {
        let err = match shared
            .meta
            .commit_wal(path, created_at_ms, chunks.clone())
            .await
        {
            Ok(offsets) => return Ok(offsets),
            Err(MetaError::Rejected(err)) => return Err(FlushFailure::Rejected(err)),
            Err(err) => err,
        };
        let retryable = matches!(
            err,
            MetaError::NotLeader { .. } | MetaError::Timeout | MetaError::Unavailable(_)
        );
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
