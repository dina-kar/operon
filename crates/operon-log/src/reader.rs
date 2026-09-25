//! The fetch path (design §02 §6): offset index → range reads through the
//! range cache, with long-poll at the high watermark.

use std::sync::Arc;
use std::time::{Duration, Instant};

use operon_cache::{CacheError, RangeCache};
use operon_common::StreamId;
use operon_common::meta::{Consistency, EntryKind, IndexEntry, MetaStore};
use operon_store::StoreError;

use crate::batch;
use crate::error::{LogError, corrupt};
use crate::record::OffsetRecord;
use crate::segment::{self, SegmentFooter, TRAILER_LEN};

/// How many segment footers a reader keeps. Segments are immutable, so a
/// cached footer never goes stale.
const FOOTER_CACHE_ENTRIES: u64 = 10_000;

/// A fetch of one partition from `offset`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub stream: StreamId,
    pub partition: u32,
    pub offset: u64,
    /// Stop adding record batches once this many batch bytes are gathered.
    /// The first batch is always returned whole, however large, so a
    /// consumer can always make progress.
    pub max_bytes: usize,
    /// At the high watermark, wait this long for new records.
    pub max_wait: Duration,
}

/// What a fetch found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchResponse {
    /// Records from the requested offset on, in offset order.
    pub records: Vec<OffsetRecord>,
    /// The offset to fetch next.
    pub next_offset: u64,
    pub high_watermark: u64,
    pub log_start_offset: u64,
}

/// The index entries a fetch reads, as of one local metastore read.
struct Plan {
    log_start_offset: u64,
    high_watermark: u64,
    entries: Vec<IndexEntry>,
}

/// Reads records by offset. Serves from the local node's metastore state
/// ([`Consistency::Local`], M0.3 plan ruling 11): on a follower, a fetch may
/// see fewer records than the leader has committed, and reports the
/// follower's high watermark. Cheap to clone.
#[derive(Clone)]
pub struct LogReader {
    meta: Arc<dyn MetaStore>,
    cache: RangeCache,
    footers: moka::future::Cache<String, Arc<SegmentFooter>>,
}

impl std::fmt::Debug for LogReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogReader")
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

/// Whether a read failed because its object is gone: garbage collection
/// deleted it after a segment swap or trim moved the index past it.
fn is_gone(err: &LogError) -> bool {
    matches!(
        err,
        LogError::Cache(CacheError::Store(StoreError::NotFound { .. }))
    ) || matches!(err, LogError::Store(e) if matches!(**e, StoreError::NotFound { .. }))
}

/// A range the metastore says an object holds but the object does not.
fn cache_error(err: CacheError) -> LogError {
    match err {
        CacheError::OutOfRange { .. } | CacheError::SizeMismatch { .. } => {
            corrupt(format!("object shorter than its index entry: {err}"))
        }
        other => LogError::Cache(other),
    }
}

/// Collects batches under the fetch's byte budget.
struct Gather {
    offset: u64,
    max_bytes: usize,
    bytes: usize,
    records: Vec<OffsetRecord>,
    full: bool,
}

impl Gather {
    /// Whether a batch of `len` bytes still fits; the first batch always does.
    fn fits(&mut self, len: usize) -> bool {
        if self.bytes == 0 || self.bytes.saturating_add(len) <= self.max_bytes {
            true
        } else {
            self.full = true;
            false
        }
    }

    /// Decodes a checked batch whose first record has offset `base` and keeps
    /// the records at or after the fetch offset.
    fn add(&mut self, batch: &batch::BatchRef<'_>, base: u64) -> Result<(), LogError> {
        self.bytes += batch.bytes.len();
        let start = self.records.len();
        batch::decode_batch(batch, base, &mut self.records)?;
        let offset = self.offset;
        let kept: Vec<OffsetRecord> = self
            .records
            .drain(start..)
            .filter(|r| r.offset >= offset)
            .collect();
        self.records.extend(kept);
        Ok(())
    }
}

impl LogReader {
    pub fn new(meta: impl Into<Arc<dyn MetaStore>>, cache: RangeCache) -> Self {
        Self {
            meta: meta.into(),
            cache,
            footers: moka::future::Cache::new(FOOTER_CACHE_ENTRIES),
        }
    }

    /// Fetches records from `request.offset`.
    ///
    /// Fails with [`LogError::OffsetOutOfRange`] below the log start or above
    /// the high watermark. At the high watermark it waits up to `max_wait`
    /// for a commit, then returns an empty response. If an object is gone
    /// because the index moved under the read (a segment swap or trim, then
    /// garbage collection), the fetch re-plans once from fresh metastore
    /// state.
    pub async fn fetch(&self, request: FetchRequest) -> Result<FetchResponse, LogError> {
        // A wait too long to represent is a wait without a deadline.
        let deadline = Instant::now().checked_add(request.max_wait);
        let mut timed_out = request.max_wait.is_zero();
        let mut replanned = false;
        loop {
            // Subscribe before reading, so a commit between the read and the
            // wait still wakes the wait.
            let mut applied = self.meta.watch_changes();
            let plan = self.plan(&request).await?;
            let out_of_range = || LogError::OffsetOutOfRange {
                requested: request.offset,
                log_start_offset: plan.log_start_offset,
                high_watermark: plan.high_watermark,
            };
            if request.offset < plan.log_start_offset || request.offset > plan.high_watermark {
                return Err(out_of_range());
            }
            if request.offset == plan.high_watermark {
                if timed_out {
                    return Ok(FetchResponse {
                        records: Vec::new(),
                        next_offset: request.offset,
                        high_watermark: plan.high_watermark,
                        log_start_offset: plan.log_start_offset,
                    });
                }
                let sleep = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    changed = applied.changed() => {
                        if changed.is_err() {
                            // The node stopped: nothing will change any more.
                            timed_out = true;
                        }
                    }
                    () = sleep => timed_out = true,
                }
                continue;
            }
            match self.read(&request, &plan).await {
                Ok(records) => {
                    let next_offset = records.last().map_or(request.offset, |r| r.offset + 1);
                    return Ok(FetchResponse {
                        records,
                        next_offset,
                        high_watermark: plan.high_watermark,
                        log_start_offset: plan.log_start_offset,
                    });
                }
                Err(err) if is_gone(&err) && !replanned => {
                    tracing::debug!(%err, "fetch planned against a moved index; re-planning");
                    replanned = true;
                }
                Err(err) if is_gone(&err) => {
                    let fresh = self.plan(&request).await?;
                    if request.offset < fresh.log_start_offset {
                        return Err(LogError::OffsetOutOfRange {
                            requested: request.offset,
                            log_start_offset: fresh.log_start_offset,
                            high_watermark: fresh.high_watermark,
                        });
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Reads the partition's bounds and the index entries a fetch needs.
    async fn plan(&self, request: &FetchRequest) -> Result<Plan, LogError> {
        let FetchRequest {
            stream,
            partition,
            offset,
            max_bytes,
            ..
        } = *request;
        let index = self
            .meta
            .partition_index(
                Consistency::Local,
                stream,
                partition,
                offset,
                Some(max_bytes as u64),
            )
            .await?;
        let index = match index {
            Some(index) => index,
            None => {
                let unknown = if self
                    .meta
                    .stream(Consistency::Local, stream)
                    .await?
                    .is_some()
                {
                    LogError::UnknownPartition { stream, partition }
                } else {
                    LogError::UnknownStream(stream)
                };
                return Err(unknown);
            }
        };
        Ok(Plan {
            log_start_offset: index.log_start_offset(),
            high_watermark: index.high_watermark(),
            entries: index.into_entries(),
        })
    }

    async fn read(
        &self,
        request: &FetchRequest,
        plan: &Plan,
    ) -> Result<Vec<OffsetRecord>, LogError> {
        let mut gather = Gather {
            offset: request.offset,
            max_bytes: request.max_bytes,
            bytes: 0,
            records: Vec::new(),
            full: false,
        };
        for entry in &plan.entries {
            match entry.kind {
                EntryKind::Wal => self.read_wal(entry, &mut gather).await?,
                EntryKind::Segment => self.read_segment(request, entry, &mut gather).await?,
            }
            if gather.full {
                break;
            }
        }
        Ok(gather.records)
    }

    /// Reads a WAL chunk: its batches, back to back, starting at the entry's
    /// base offset.
    async fn read_wal(&self, entry: &IndexEntry, gather: &mut Gather) -> Result<(), LogError> {
        let bytes = self
            .cache
            .read(&entry.object, entry.byte_range.clone())
            .await
            .map_err(cache_error)?;
        let mut base = entry.base_offset;
        for batch in batch::batches(&bytes) {
            let batch = batch?;
            let end = base + u64::from(batch.record_count);
            if end > gather.offset {
                if !gather.fits(batch.bytes.len()) {
                    return Ok(());
                }
                gather.add(&batch, base)?;
            }
            base = end;
        }
        if base != entry.end_offset() {
            return Err(corrupt(format!(
                "WAL chunk in {} holds records up to {base}, its index entry up to {}",
                entry.object,
                entry.end_offset()
            )));
        }
        Ok(())
    }

    /// Reads whole batches of a segment, from the one holding the fetch
    /// offset, as far as the byte budget allows, in one range read.
    async fn read_segment(
        &self,
        request: &FetchRequest,
        entry: &IndexEntry,
        gather: &mut Gather,
    ) -> Result<(), LogError> {
        let footer = self.footer(&entry.object).await?;
        if footer.stream != request.stream
            || footer.partition != request.partition
            || footer.base_offset != entry.base_offset
            || footer.end_offset != entry.end_offset()
            || footer.data != entry.byte_range
        {
            return Err(corrupt(format!(
                "segment {} does not match its index entry",
                entry.object
            )));
        }
        let from = gather.offset.max(entry.base_offset);
        let first = footer
            .batch_index_for(from)
            .ok_or_else(|| corrupt(format!("segment {} has no batch for {from}", entry.object)))?;
        let mut last = first;
        let mut planned = gather.bytes;
        for i in first..footer.batches.len() {
            let range = footer.batch_range(i);
            let len = (range.end - range.start) as usize;
            if planned != 0 && planned.saturating_add(len) > gather.max_bytes {
                break;
            }
            planned += len;
            last = i;
        }
        let start = footer.batch_range(first).start;
        let end = footer.batch_range(last).end;
        let bytes = self
            .cache
            .read(&entry.object, start..end)
            .await
            .map_err(cache_error)?;
        let mut parsed = batch::batches(&bytes);
        for i in first..=last {
            let batch = parsed
                .next()
                .ok_or_else(|| corrupt(format!("segment {} ends early", entry.object)))??;
            let range = footer.batch_range(i);
            let base = footer.batches[i].base_offset;
            if batch.bytes.len() as u64 != range.end - range.start
                || u64::from(batch.record_count) != footer.batch_end_offset(i) - base
            {
                return Err(corrupt(format!(
                    "segment {} batch {i} does not match its footer",
                    entry.object
                )));
            }
            if !gather.fits(batch.bytes.len()) {
                return Ok(());
            }
            gather.add(&batch, base)?;
        }
        if last + 1 < footer.batches.len() {
            gather.full = true;
        }
        Ok(())
    }

    /// A segment's footer, from the cache or read once from the object.
    async fn footer(&self, path: &str) -> Result<Arc<SegmentFooter>, LogError> {
        if let Some(footer) = self.footers.get(path).await {
            return Ok(footer);
        }
        let size = self.cache.size(path).await.map_err(cache_error)?;
        if size < segment::HEADER_LEN + TRAILER_LEN {
            return Err(corrupt(format!(
                "segment {path} is too short: {size} bytes"
            )));
        }
        let tail = self
            .cache
            .read(path, size - TRAILER_LEN..size)
            .await
            .map_err(cache_error)?;
        let tail: &[u8; 40] = tail
            .as_ref()
            .try_into()
            .map_err(|_| corrupt("short segment trailer read"))?;
        let trailer = segment::parse_trailer(tail)?;
        let index_range = trailer.index_range(size)?;
        let header = self
            .cache
            .read(path, 0..segment::HEADER_LEN)
            .await
            .map_err(cache_error)?;
        let index = self
            .cache
            .read(path, index_range)
            .await
            .map_err(cache_error)?;
        let footer = Arc::new(segment::parse_footer(&header, &index, &trailer)?);
        self.footers.insert(path.to_string(), footer.clone()).await;
        Ok(footer)
    }
}
