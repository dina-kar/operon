//! The sequencer and the offset index (design §20 §11.3): `commit_wal`,
//! `swap_segment`, `trim_partition`, `partition_index`, `stream_state`.
//!
//! The three writes are pessimistic transactions (R1 Ruling 2). Each locks
//! the partition heads it touches with `get_for_update` first (in one batch,
//! key order), then reads what its decision depends on either through
//! `get_for_update` as well (the rows it rewrites) or through a snapshot at a
//! fresh TSO timestamp taken after the locks (the stream records and index
//! entries, row T5-3). A pessimistic transaction's plain reads use its start
//! timestamp, which can predate a commit that finished before its locks; the
//! fresh snapshot sees that commit, and every writer of a partition's index
//! entries writes (or deletes) its head, so the head lock keeps them fixed.
//!
//! `commit_wal` commits one transaction per partition group of at most
//! [`MAX_GROUP_CHUNKS`] chunks and [`MAX_GROUP_BYTES`] of metastore writes;
//! one stream's chunks never span groups, and each group commits
//! idempotently through its record `w/<object>/<group>` (D59, D124).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, EntryKind, IndexEntry, MetaError, MetaResult, PartitionBounds, PartitionIndex,
    SegmentSwap, Stream, StreamState, Tracked, WAL_COMMIT_WINDOW_MS, WalChunk, WalCommit,
};
use loams_tikv::{Mode, Snap, Tikv, Txn, TxnError};

use crate::catalog::{load_heads, load_stream, validate_key};
use crate::keys::{self, Head, ObjectRef, WalGroup};
use crate::leases::check_fence;
use crate::{Reader, TikvMeta, decode_all, fatal, fresh_snapshot, load, rejected};

/// Most chunks one partition group commits in one transaction.
pub const MAX_GROUP_CHUNKS: usize = 1_024;

/// Most bytes of metastore writes (by [`chunk_cost`]) one partition group
/// commits in one transaction: well under TiKV's 8 MiB Raft entry limit.
pub const MAX_GROUP_BYTES: u64 = 4 * 1024 * 1024;

/// How far a WAL object's `created_at_ms` may be ahead of the metastore
/// clock before `commit_wal` refuses it with `ClockSkew` (the openraft
/// backend's default `max_clock_skew`).
pub const MAX_CLOCK_SKEW_MS: u64 = 300_000;

/// Index entries per scan page.
const ENTRY_PAGE: usize = 256;

/// The metastore bytes one chunk costs a group: its index entry (key and
/// value, the object's path included), its slot in the group's commit
/// record and a share of its partition head, rounded up.
fn chunk_cost(object: &str) -> u64 {
    u64::try_from(object.len())
        .unwrap_or(u64::MAX)
        .saturating_add(128)
}

/// Splits a call's chunks into partition groups: streams in the order of
/// their first chunk, each stream's chunks whole in one group, a new group
/// when the next stream does not fit. Each group lists its chunks' positions
/// in call order. A call without chunks is one empty group. A stream whose
/// chunks alone exceed a group is refused.
pub(crate) fn plan_groups(
    object: &str,
    chunks: &[WalChunk],
    max_chunks: usize,
    max_bytes: u64,
) -> Result<Vec<Vec<usize>>, ApplyError> {
    let mut order: Vec<StreamId> = Vec::new();
    let mut by_stream: HashMap<StreamId, Vec<usize>> = HashMap::new();
    for (i, chunk) in chunks.iter().enumerate() {
        by_stream
            .entry(chunk.stream)
            .or_insert_with(|| {
                order.push(chunk.stream);
                Vec::new()
            })
            .push(i);
    }
    let cost = chunk_cost(object);
    let mut groups = Vec::new();
    let mut current: Vec<usize> = Vec::new();
    let mut current_bytes = 0u64;
    for stream in order {
        let positions = by_stream.remove(&stream).unwrap_or_default();
        let bytes = cost.saturating_mul(u64::try_from(positions.len()).unwrap_or(u64::MAX));
        if positions.len() > max_chunks || bytes > max_bytes {
            return Err(ApplyError::InvalidArgument(format!(
                "stream {stream} has {} chunks in WAL object {object:?}, and one stream's chunks \
                 must fit one partition group ({max_chunks} chunks, {max_bytes} bytes of \
                 metastore writes): split the object",
                positions.len()
            )));
        }
        if !current.is_empty()
            && (current.len() + positions.len() > max_chunks
                || current_bytes.saturating_add(bytes) > max_bytes)
        {
            groups.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.extend(positions);
        current_bytes = current_bytes.saturating_add(bytes);
    }
    groups.push(current);
    for group in &mut groups {
        group.sort_unstable();
    }
    Ok(groups)
}

/// The chunk checks of the openraft state machine, in its order: the stream,
/// the partition, a record, a byte range.
fn validate_chunk(streams: &HashMap<StreamId, Stream>, chunk: &WalChunk) -> Result<(), ApplyError> {
    let Some(stream) = streams.get(&chunk.stream) else {
        return Err(ApplyError::StreamNotFound(chunk.stream));
    };
    if chunk.partition >= stream.partitions {
        return Err(ApplyError::PartitionNotFound {
            stream: chunk.stream,
            partition: chunk.partition,
        });
    }
    if chunk.records == 0 {
        return Err(ApplyError::InvalidArgument(
            "a WAL chunk needs at least one record".to_string(),
        ));
    }
    if chunk.byte_range.start >= chunk.byte_range.end {
        return Err(ApplyError::InvalidArgument(format!(
            "a WAL chunk needs a non-empty byte range, got {:?}",
            chunk.byte_range
        )));
    }
    Ok(())
}

/// The streams the given chunks name, from `r`.
async fn load_streams(
    r: &mut dyn Reader,
    chunks: impl Iterator<Item = &WalChunk>,
) -> Result<HashMap<StreamId, Stream>, TxnError> {
    let ids: BTreeSet<StreamId> = chunks.map(|c| c.stream).collect();
    let pairs = r
        .batch_get(ids.iter().map(|&id| keys::stream(id)).collect())
        .await?;
    let streams: Vec<Stream> = decode_all("stream", &pairs)?;
    Ok(streams.into_iter().map(|s| (s.id, s)).collect())
}

fn decode_head(value: Option<&Vec<u8>>) -> Result<Head, TxnError> {
    value.map_or(Ok(Head::default()), |v| {
        keys::decode("partition head", v).map_err(fatal)
    })
}

fn entry_bytes(entry: &IndexEntry) -> u64 {
    entry.byte_range.end.saturating_sub(entry.byte_range.start)
}

/// Offsets by call position, in call order.
fn in_call_order(mut offsets: Vec<(u32, u64)>) -> Vec<u64> {
    offsets.sort_unstable_by_key(|&(position, _)| position);
    offsets.into_iter().map(|(_, base)| base).collect()
}

/// Accounts for index entries a write removed, as the openraft state
/// machine's `release_entry` does: a WAL chunk decrements its object's live
/// count (`W/`), retiring the object at zero; a segment is retired at once.
/// Retirement is stamped `now_ms`. The live counts are read with
/// `get_for_update`, so concurrent releases of one object's chunks in other
/// partitions serialize.
pub(crate) async fn release_entries(
    txn: &mut Txn,
    entries: &[IndexEntry],
    now_ms: u64,
) -> Result<(), TxnError> {
    let mut wal: BTreeMap<&str, u32> = BTreeMap::new();
    for entry in entries {
        match entry.kind {
            EntryKind::Wal => *wal.entry(entry.object.as_str()).or_default() += 1,
            EntryKind::Segment => {
                txn.put(&keys::retired(&entry.object), keys::encode_u64(now_ms))
                    .await?;
            }
        }
    }
    if wal.is_empty() {
        return Ok(());
    }
    let live: HashMap<Vec<u8>, Vec<u8>> = txn
        .batch_get_for_update(wal.keys().map(|object| keys::wal_live(object)))
        .await?
        .into_iter()
        .collect();
    for (object, released) in wal {
        let key = keys::wal_live(object);
        let remaining = match live.get(&key) {
            Some(v) => keys::decode_u32("WAL live count", v)
                .map_err(fatal)?
                .saturating_sub(released),
            // Every WAL entry is counted at commit, so this is unreachable;
            // retiring keeps the object collectable.
            None => 0,
        };
        if remaining == 0 {
            txn.delete(&key).await?;
            txn.put(&keys::retired(object), keys::encode_u64(now_ms))
                .await?;
        } else {
            txn.put(&key, remaining.to_be_bytes().to_vec()).await?;
        }
    }
    Ok(())
}

/// The first key after `key` (keys under one prefix have no shorter
/// successor).
fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// The index entries of a partition from the one holding `offset` (or the
/// first after it), in offset order, while `keep_going` says so; `keep_going`
/// sees the entries taken so far and the next one.
async fn entries_from(
    r: &mut dyn ScanReader,
    stream: StreamId,
    partition: u32,
    offset: u64,
    mut keep_going: impl FnMut(&[(Vec<u8>, IndexEntry)], &IndexEntry) -> bool,
) -> Result<Vec<(Vec<u8>, IndexEntry)>, TxnError> {
    let (lo, hi) = keys::prefix_range(&keys::partition_entries(stream, partition));
    let at = keys::index_entry(stream, partition, offset);
    // The entry holding `offset` is the last one starting at or before it.
    let holding = r.scan(&lo, Some(&after(&at)), 1, true).await?;
    let mut start = at;
    if let Some((key, value)) = holding.first() {
        let entry: IndexEntry = keys::decode("index entry", value).map_err(fatal)?;
        if offset < entry.end_offset() {
            start.clone_from(key);
        }
    }
    let mut out = Vec::new();
    loop {
        let page = r.scan(&start, hi.as_deref(), ENTRY_PAGE, false).await?;
        let full = page.len() == ENTRY_PAGE;
        for (key, value) in page {
            let entry: IndexEntry = keys::decode("index entry", &value).map_err(fatal)?;
            if !keep_going(&out, &entry) {
                return Ok(out);
            }
            start = after(&key);
            out.push((key, entry));
        }
        if !full {
            return Ok(out);
        }
    }
}

/// Range scans over a snapshot, forward or reverse.
trait ScanReader: Send {
    fn scan<'a>(
        &'a mut self,
        start: &'a [u8],
        end: Option<&'a [u8]>,
        limit: usize,
        reverse: bool,
    ) -> futures::future::BoxFuture<'a, Result<Vec<loams_tikv::Pair>, TxnError>>;
}

impl ScanReader for Snap {
    fn scan<'a>(
        &'a mut self,
        start: &'a [u8],
        end: Option<&'a [u8]>,
        limit: usize,
        reverse: bool,
    ) -> futures::future::BoxFuture<'a, Result<Vec<loams_tikv::Pair>, TxnError>> {
        Box::pin(async move {
            if reverse {
                Snap::scan_reverse(self, start, end, limit).await
            } else {
                Snap::scan(self, start, end, limit).await
            }
        })
    }
}

/// Checks that `stream` exists and has `partition`.
async fn check_partition(
    r: &mut dyn Reader,
    stream: StreamId,
    partition: u32,
) -> Result<Result<Stream, ApplyError>, TxnError> {
    Ok(match load_stream(r, stream).await? {
        None => Err(ApplyError::StreamNotFound(stream)),
        Some(s) if partition >= s.partitions => {
            Err(ApplyError::PartitionNotFound { stream, partition })
        }
        Some(s) => Ok(s),
    })
}

impl TikvMeta {
    // ---- commit_wal ----

    pub(crate) async fn commit_wal_impl(&self, commit: WalCommit) -> Tracked<Vec<u64>> {
        let refuse = |e| Tracked {
            result: rejected(e),
            earlier_unknown: false,
        };
        if let Err(e) = validate_key("WAL object path", &commit.object) {
            return refuse(e);
        }
        // Refused before any transaction, as the openraft leader refuses
        // before proposing (row T5-6).
        let clock = self.now_estimate();
        if commit.created_at_ms > clock.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Tracked {
                result: Err(MetaError::ClockSkew {
                    stamped_ms: commit.created_at_ms,
                    leader_ms: clock,
                }),
                earlier_unknown: false,
            };
        }
        let groups = match plan_groups(
            &commit.object,
            &commit.chunks,
            MAX_GROUP_CHUNKS,
            MAX_GROUP_BYTES,
        ) {
            Ok(groups) => groups,
            Err(e) => return refuse(e),
        };
        let commit = Arc::new(commit);
        if let [members] = groups.as_slice() {
            // One group: every check and every write in one transaction, so
            // the call is atomic across all its partitions.
            let (result, earlier_unknown) = self.commit_group(&commit, 0, 1, members).await;
            return Tracked {
                result: result.map(in_call_order),
                earlier_unknown,
            };
        }
        self.commit_groups(&commit, &groups).await
    }

    /// A call over several groups: validate everything from one snapshot
    /// first (unless a retry finds records), then commit each group missing
    /// its record, in order. Success only once every group has committed.
    async fn commit_groups(
        &self,
        commit: &Arc<WalCommit>,
        groups: &[Vec<usize>],
    ) -> Tracked<Vec<u64>> {
        let count = u32::try_from(groups.len()).unwrap_or(u32::MAX);
        let pre = {
            let commit = commit.clone();
            self.read(move |snap| {
                let commit = commit.clone();
                Box::pin(async move {
                    let prefix = keys::wal_groups(&commit.object);
                    let mut records = BTreeMap::new();
                    for (key, value) in snap.scan_prefix(&prefix).await? {
                        if let Some(group) = keys::wal_group_of(&key, &commit.object) {
                            let record: WalGroup =
                                keys::decode("WAL commit record", &value).map_err(fatal)?;
                            records.insert(group, record);
                        }
                    }
                    if records.is_empty() {
                        let streams = load_streams(snap, commit.chunks.iter()).await?;
                        for chunk in &commit.chunks {
                            if let Err(e) = validate_chunk(&streams, chunk) {
                                return Ok(Err(e));
                            }
                        }
                    }
                    Ok(Ok(records))
                })
            })
            .await
        };
        let records = match pre {
            Ok(records) => records,
            Err(e) => {
                return Tracked {
                    result: Err(e),
                    earlier_unknown: false,
                };
            }
        };
        if let Some(record) = records.values().find(|r| r.groups != count) {
            return Tracked {
                result: rejected(regrouped(&commit.object, record.groups, count)),
                earlier_unknown: false,
            };
        }
        let mut offsets = Vec::with_capacity(commit.chunks.len());
        let mut earlier_unknown = false;
        for (group, members) in (0u32..).zip(groups) {
            if let Some(record) = records.get(&group) {
                offsets.extend(record.offsets.iter().copied());
                continue;
            }
            let (result, unknown) = self.commit_group(commit, group, count, members).await;
            earlier_unknown |= unknown;
            match result {
                Ok(group_offsets) => offsets.extend(group_offsets),
                Err(e) => {
                    return Tracked {
                        result: Err(e),
                        earlier_unknown,
                    };
                }
            }
        }
        Tracked {
            result: Ok(in_call_order(offsets)),
            earlier_unknown,
        }
    }

    /// Commits one partition group in one pessimistic transaction; returns
    /// its chunks' `(call position, base offset)`s. Finding the group's
    /// record returns the record's offsets (a retry).
    async fn commit_group(
        &self,
        commit: &Arc<WalCommit>,
        group: u32,
        groups: u32,
        members: &[usize],
    ) -> (MetaResult<Vec<(u32, u64)>>, bool) {
        let commit = commit.clone();
        let members: Arc<[usize]> = members.into();
        let tikv = self.inner.tikv.clone();
        self.write_in(Mode::Pessimistic, "meta.commit_wal", move |txn| {
            let (commit, members, tikv) = (commit.clone(), members.clone(), tikv.clone());
            Box::pin(async move {
                let object = commit.object.as_str();
                let chunks = || members.iter().map(|&i| &commit.chunks[i]);
                // Lock the heads, the record and the object's rows at once.
                let heads: BTreeSet<Vec<u8>> = chunks()
                    .map(|c| keys::head(c.stream, c.partition))
                    .collect();
                let record_key = keys::wal_group(object, group);
                let live_key = keys::wal_live(object);
                let retired_key = keys::retired(object);
                let object_key = keys::object_ref(object);
                let mut lock: Vec<Vec<u8>> = heads.iter().cloned().collect();
                lock.extend([
                    record_key.clone(),
                    live_key.clone(),
                    retired_key.clone(),
                    object_key.clone(),
                ]);
                let locked: HashMap<Vec<u8>, Vec<u8>> =
                    txn.batch_get_for_update(lock).await?.into_iter().collect();

                // A retry: the group's record holds its first commit's offsets.
                if let Some(value) = locked.get(&record_key) {
                    let record: WalGroup =
                        keys::decode("WAL commit record", value).map_err(fatal)?;
                    if record.groups != groups {
                        return Ok(Err(regrouped(object, record.groups, groups)));
                    }
                    return Ok(Ok(record.offsets));
                }
                let clock_ms = Tikv::physical_ms(&txn.start_ts());
                let stale = || ApplyError::StaleCommit {
                    object: object.to_string(),
                };
                if commit.created_at_ms.saturating_add(WAL_COMMIT_WINDOW_MS) < clock_ms {
                    return Ok(Err(stale()));
                }
                if members.is_empty() {
                    return Ok(Err(ApplyError::InvalidArgument(
                        "a WAL commit needs at least one chunk".to_string(),
                    )));
                }
                // An object already retired (every chunk of an earlier group
                // released) or claimed by garbage collection must not become
                // reachable again (row T5-5).
                let claimed = match locked.get(&object_key) {
                    Some(v) => {
                        keys::decode::<ObjectRef>("object reference", v)
                            .map_err(fatal)?
                            .gc_claim
                    }
                    None => false,
                };
                if claimed || locked.contains_key(&retired_key) {
                    return Ok(Err(stale()));
                }
                let mut snap = fresh_snapshot(&tikv).await?;
                let streams = load_streams(&mut snap, chunks()).await?;
                for chunk in chunks() {
                    if let Err(e) = validate_chunk(&streams, chunk) {
                        return Ok(Err(e));
                    }
                }

                let mut head_values: BTreeMap<Vec<u8>, Head> = BTreeMap::new();
                for key in heads {
                    let head = decode_head(locked.get(&key))?;
                    head_values.insert(key, head);
                }
                let mut offsets = Vec::with_capacity(members.len());
                for &position in members.iter() {
                    let chunk = &commit.chunks[position];
                    let head_key = keys::head(chunk.stream, chunk.partition);
                    let Some(head) = head_values.get_mut(&head_key) else {
                        return Err(TxnError::Fatal("a chunk's head was not locked".to_string()));
                    };
                    let base = head.next;
                    head.next += u64::from(chunk.records);
                    let entry = IndexEntry {
                        kind: EntryKind::Wal,
                        base_offset: base,
                        records: chunk.records,
                        object: object.to_string(),
                        byte_range: chunk.byte_range.clone(),
                        max_timestamp_ms: chunk.max_timestamp_ms,
                    };
                    head.bytes += entry_bytes(&entry);
                    txn.put(
                        &keys::index_entry(chunk.stream, chunk.partition, base),
                        keys::encode(&entry),
                    )
                    .await?;
                    let position = u32::try_from(position).map_err(|_| {
                        TxnError::Fatal("a WAL commit has over u32::MAX chunks".to_string())
                    })?;
                    offsets.push((position, base));
                }
                for (key, head) in head_values {
                    txn.put(&key, keys::encode(&head)).await?;
                }
                let live = match locked.get(&live_key) {
                    Some(v) => keys::decode_u32("WAL live count", v).map_err(fatal)?,
                    None => 0,
                };
                let added = u32::try_from(members.len()).unwrap_or(u32::MAX);
                txn.put(&live_key, live.saturating_add(added).to_be_bytes().to_vec())
                    .await?;
                txn.put(
                    &record_key,
                    keys::encode(&WalGroup {
                        groups,
                        created_at_ms: commit.created_at_ms,
                        offsets: offsets.clone(),
                    }),
                )
                .await?;
                Ok(Ok(offsets))
            })
        })
        .await
    }

    // ---- swap_segment ----

    pub(crate) async fn swap_segment_impl(&self, swap: SegmentSwap) -> Tracked<()> {
        if let Err(e) = validate_key("segment path", &swap.segment) {
            return Tracked {
                result: rejected(e),
                earlier_unknown: false,
            };
        }
        self.reach_now().await;
        let swap = Arc::new(swap);
        let tikv = self.inner.tikv.clone();
        let (result, earlier_unknown) = self
            .write_in(Mode::Pessimistic, "meta.swap_segment", move |txn| {
                let (swap, tikv) = (swap.clone(), tikv.clone());
                Box::pin(async move { swap_body(txn, &tikv, &swap).await })
            })
            .await;
        Tracked {
            result,
            earlier_unknown,
        }
    }

    // ---- trim_partition ----

    pub(crate) async fn trim_partition_impl(
        &self,
        stream: StreamId,
        partition: u32,
        before_offset: u64,
        fence: Option<loams_common::meta::Fence>,
    ) -> MetaResult<u64> {
        self.reach_now().await;
        let tikv = self.inner.tikv.clone();
        self.write_in(Mode::Pessimistic, "meta.trim_partition", move |txn| {
            let (fence, tikv) = (fence.clone(), tikv.clone());
            Box::pin(async move {
                let head_key = keys::head(stream, partition);
                let mut head = decode_head(txn.get_for_update(&head_key).await?.as_ref())?;
                let mut snap = fresh_snapshot(&tikv).await?;
                if let Err(e) = check_partition(&mut snap, stream, partition).await? {
                    return Ok(Err(e));
                }
                if let Some(fence) = &fence
                    && let Err(e) = check_fence(txn, fence).await?
                {
                    return Ok(Err(e));
                }
                let before = before_offset.min(head.next);
                let removed = entries_from(&mut snap, stream, partition, 0, |_, e| {
                    e.end_offset() <= before
                })
                .await?;
                let log_start = head.log_start.max(before);
                if removed.is_empty() && log_start == head.log_start {
                    return Ok(Ok(log_start));
                }
                let now_ms = Tikv::physical_ms(&txn.start_ts());
                let mut entries = Vec::with_capacity(removed.len());
                for (key, entry) in removed {
                    txn.delete(&key).await?;
                    head.bytes = head.bytes.saturating_sub(entry_bytes(&entry));
                    entries.push(entry);
                }
                head.log_start = log_start;
                txn.put(&head_key, keys::encode(&head)).await?;
                release_entries(txn, &entries, now_ms).await?;
                Ok(Ok(log_start))
            })
        })
        .await
        .0
    }

    // ---- Reads ----

    pub(crate) async fn stream_state_impl(&self, id: StreamId) -> MetaResult<Option<StreamState>> {
        self.read(move |snap| {
            Box::pin(async move {
                let Some(stream) = load_stream(snap, id).await? else {
                    return Ok(Ok(None));
                };
                // An absent head is an empty partition (row T4-5).
                let heads = load_heads(snap, id, stream.partitions).await?;
                let partitions = heads
                    .into_iter()
                    .map(|h| {
                        Some(PartitionBounds {
                            log_start_offset: h.log_start,
                            high_watermark: h.next,
                            bytes: h.bytes,
                        })
                    })
                    .collect();
                Ok(Ok(Some(StreamState { stream, partitions })))
            })
        })
        .await
    }

    pub(crate) async fn partition_index_impl(
        &self,
        stream: StreamId,
        partition: u32,
        from_offset: u64,
        max_bytes: Option<u64>,
    ) -> MetaResult<Option<PartitionIndex>> {
        self.read(move |snap| {
            Box::pin(async move {
                if check_partition(snap, stream, partition).await?.is_err() {
                    return Ok(Ok(None));
                }
                let head: Head = load(snap, "partition head", &keys::head(stream, partition))
                    .await?
                    .unwrap_or_default();
                // Offsets below the log start count as the log start.
                let offset = from_offset.max(head.log_start);
                let entries = if offset >= head.next {
                    Vec::new()
                } else {
                    let mut bytes = 0u64;
                    entries_from(snap, stream, partition, offset, |taken, entry| {
                        if let Some(max) = max_bytes
                            && !taken.is_empty()
                            && bytes >= max
                        {
                            return false;
                        }
                        bytes += entry_bytes(entry);
                        true
                    })
                    .await?
                    .into_iter()
                    .map(|(_, e)| e)
                    .collect()
                };
                Ok(Ok(Some(PartitionIndex::new(
                    head.log_start,
                    head.next,
                    head.bytes,
                    entries,
                ))))
            })
        })
        .await
    }
}

/// The body of one `swap_segment` attempt; checks in the openraft state
/// machine's order.
async fn swap_body(
    txn: &mut Txn,
    tikv: &Tikv,
    swap: &SegmentSwap,
) -> Result<Result<(), ApplyError>, TxnError> {
    let (stream, partition) = (swap.stream, swap.partition);
    let head_key = keys::head(stream, partition);
    let mut head = decode_head(txn.get_for_update(&head_key).await?.as_ref())?;
    let mut snap = fresh_snapshot(tikv).await?;
    if let Err(e) = check_partition(&mut snap, stream, partition).await? {
        return Ok(Err(e));
    }
    let Some(&(first_base, _)) = swap.replaces.first() else {
        return Ok(Err(ApplyError::InvalidArgument(
            "a segment swap must replace at least one entry".to_string(),
        )));
    };
    let entry_keys: Vec<Vec<u8>> = swap
        .replaces
        .iter()
        .map(|(base, _)| keys::index_entry(stream, partition, *base))
        .collect();
    let found: HashMap<Vec<u8>, IndexEntry> = snap
        .batch_get(entry_keys.clone())
        .await?
        .into_iter()
        .map(|(k, v)| Ok((k, keys::decode("index entry", &v).map_err(fatal)?)))
        .collect::<Result<_, TxnError>>()?;
    // A retry of a swap that was applied: the segment entry is in place.
    if let Some(entry) = found.get(&entry_keys[0])
        && entry.kind == EntryKind::Segment
        && entry.object == swap.segment
    {
        return Ok(Ok(()));
    }
    if let Some(fence) = &swap.fence
        && let Err(e) = check_fence(txn, fence).await?
    {
        return Ok(Err(e));
    }
    if swap.byte_range.start >= swap.byte_range.end {
        return Ok(Err(ApplyError::InvalidArgument(format!(
            "a segment needs a non-empty byte range, got {:?}",
            swap.byte_range
        ))));
    }
    let clock_ms = Tikv::physical_ms(&txn.start_ts());
    let stale = || ApplyError::StaleObject {
        object: swap.segment.clone(),
        created_at_ms: swap.fresh.created_at_ms,
        max_age_ms: swap.fresh.max_age_ms,
        clock_ms,
    };
    if swap.fresh.expired_at(clock_ms) {
        return Ok(Err(stale()));
    }
    // A segment garbage collection has claimed must not become reachable
    // (§20 §11.3); the lock makes a concurrent claim conflict.
    if let Some(v) = txn.get_for_update(&keys::object_ref(&swap.segment)).await?
        && keys::decode::<ObjectRef>("object reference", &v)
            .map_err(fatal)?
            .gc_claim
    {
        return Ok(Err(stale()));
    }
    let mismatch = || ApplyError::IndexMismatch { stream, partition };
    let mut expected_base = first_base;
    let mut records: u32 = 0;
    let mut removed = Vec::with_capacity(swap.replaces.len());
    for ((base, object), key) in swap.replaces.iter().zip(&entry_keys) {
        let Some(entry) = found.get(key) else {
            return Ok(Err(mismatch()));
        };
        if *base != expected_base || entry.kind != EntryKind::Wal || entry.object != *object {
            return Ok(Err(mismatch()));
        }
        let Some(sum) = records.checked_add(entry.records) else {
            return Ok(Err(ApplyError::InvalidArgument(
                "a segment holds at most u32::MAX records".to_string(),
            )));
        };
        records = sum;
        expected_base = entry.end_offset();
        removed.push(entry.clone());
    }

    // Everything is checked: apply.
    for (entry, key) in removed.iter().zip(&entry_keys) {
        head.bytes = head.bytes.saturating_sub(entry_bytes(entry));
        if entry.base_offset != first_base {
            txn.delete(key).await?;
        }
    }
    let segment = IndexEntry {
        kind: EntryKind::Segment,
        base_offset: first_base,
        records,
        object: swap.segment.clone(),
        byte_range: swap.byte_range.clone(),
        max_timestamp_ms: swap.max_timestamp_ms,
    };
    head.bytes += entry_bytes(&segment);
    txn.put(&entry_keys[0], keys::encode(&segment)).await?;
    txn.put(&head_key, keys::encode(&head)).await?;
    release_entries(txn, &removed, clock_ms).await?;
    Ok(Ok(()))
}

/// A retry whose chunks group differently from the first attempt's.
fn regrouped(object: &str, recorded: u32, now: u32) -> ApplyError {
    ApplyError::InvalidArgument(format!(
        "WAL object {object:?} was committed in {recorded} partition groups and this retry \
         makes {now}: retry it with its original chunks"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(stream: u64, partition: u32) -> WalChunk {
        WalChunk {
            stream: StreamId(stream),
            partition,
            records: 1,
            byte_range: 0..10,
            max_timestamp_ms: 0,
        }
    }

    #[test]
    fn one_group_when_everything_fits() {
        let chunks = vec![chunk(1, 0), chunk(2, 0), chunk(1, 1)];
        assert_eq!(
            plan_groups("w", &chunks, MAX_GROUP_CHUNKS, MAX_GROUP_BYTES).expect("plan"),
            vec![vec![0, 1, 2]]
        );
        assert_eq!(
            plan_groups("w", &[], MAX_GROUP_CHUNKS, MAX_GROUP_BYTES).expect("plan"),
            vec![Vec::<usize>::new()]
        );
    }

    #[test]
    fn a_stream_is_never_split_and_groups_keep_call_order() {
        // Streams 1 (3 chunks), 2 (2), 3 (2), interleaved; groups of 4.
        let chunks = vec![
            chunk(1, 0),
            chunk(2, 0),
            chunk(1, 1),
            chunk(3, 0),
            chunk(2, 1),
            chunk(1, 2),
            chunk(3, 1),
        ];
        let groups = plan_groups("w", &chunks, 4, MAX_GROUP_BYTES).expect("plan");
        assert_eq!(groups, vec![vec![0, 2, 5], vec![1, 3, 4, 6]]);
        for group in &groups {
            let streams: BTreeSet<u64> = group.iter().map(|&i| chunks[i].stream.0).collect();
            for s in streams {
                let all = chunks.iter().filter(|c| c.stream.0 == s).count();
                let here = group.iter().filter(|&&i| chunks[i].stream.0 == s).count();
                assert_eq!(all, here, "stream {s} spans groups");
            }
        }
    }

    #[test]
    fn the_byte_budget_splits_groups_too() {
        let chunks = vec![chunk(1, 0), chunk(2, 0)];
        let cost = chunk_cost("w");
        assert_eq!(
            plan_groups("w", &chunks, 100, cost).expect("plan"),
            vec![vec![0], vec![1]]
        );
    }

    #[test]
    fn a_stream_over_one_group_is_refused() {
        let chunks: Vec<WalChunk> = (0..5).map(|p| chunk(9, p)).collect();
        match plan_groups("w", &chunks, 4, MAX_GROUP_BYTES) {
            Err(ApplyError::InvalidArgument(m)) => assert!(m.contains("stream 9"), "{m}"),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        match plan_groups("w", &chunks[..1], 4, chunk_cost("w") - 1) {
            Err(ApplyError::InvalidArgument(_)) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn offsets_come_back_in_call_order() {
        assert_eq!(in_call_order(vec![(2, 7), (0, 3), (1, 0)]), vec![3, 0, 7]);
    }
}
