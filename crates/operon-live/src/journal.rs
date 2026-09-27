//! The commit journal (design §20 §5.3, D119; R1 plan Task 9).
//!
//! Every mutation that writes appends one [`pb::JournalEntry`] to one of the
//! app's journal shards, inside its own transaction: it reads the shard's
//! head `h` at its start timestamp and writes the entry at `seq = h + 1` and
//! the head `= h + 1`. Two mutations on one shard conflict on the head and
//! one of them reruns (and picks a shard again). So each shard's sequence is
//! dense and in commit order, and an entry is visible at `T` exactly when its
//! transaction committed at or before `T`: the entries visible at `T` and not
//! at `t` are `head(t) < seq ≤ head(T)` (§20 §8.3).
//!
//! An entry larger than [`MAX_CHUNK_BYTES`] is split over consecutive
//! sequences of its shard, each chunk a `JournalEntry` with a share of the
//! writes and the same `function` and `request_id` (a mutation may write up
//! to 8 MiB, and TiKV values stop at 2 MiB).
//!
//! [`Tailer`] follows the journal from positions (one per shard): a tick at a
//! TSO timestamp reads the heads and the entries in `(position, head]` of
//! every moved shard, and the positions advance only when the caller
//! acknowledges the batch. Consumers checkpoint their positions per shard,
//! with a time to live for the in-memory ones; [`Janitor`] deletes entries
//! every live checkpoint has passed once they are older than the retention
//! (10 min).
//!
//! The journal's own transactions (checkpoints, trimming) commit with
//! two-phase commit, as Live's mutations do (R1 plan row T7-1).

use std::future::Future;
use std::time::Duration;

use buffa::Message;
use operon_tikv::{CommitMode, Tikv, Timestamp, Txn, TxnError, TxnOptions};
use rand::Rng;

use crate::docs::Reads;
use crate::keys::{AppKeys, KeyRange, key_after};
use crate::{LiveError, pb};

/// The most shards an app's journal may have (§20 §5.3).
pub const MAX_SHARDS: u16 = 1024;

/// How long the janitor keeps entries every consumer has passed (§20 §5.3).
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(600);

/// The largest encoded chunk of one entry; a larger entry is split over
/// consecutive sequences. Well under the 2 MiB TiKV value bound.
pub const MAX_CHUNK_BYTES: usize = 1024 * 1024;

/// The longest consumer id, in bytes.
pub const MAX_CONSUMER_BYTES: usize = 256;

/// The most checkpoints one shard may hold (consumers of one app).
pub const MAX_CONSUMERS: usize = 4096;

/// Entries the janitor deletes per transaction.
pub const TRIM_BATCH: usize = 256;

/// Entries per batch get of [`Journal::read_bounded`] (the store layer pages
/// further on gRPC size limits).
pub const READ_PAGE_ENTRIES: usize = 256;

/// The encoded bytes of entries one [`Tailer::tick`] reads by default
/// (64 MiB); the rest of a backlog is read by the next ticks.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// The commit mode of the journal's own transactions (row T7-1).
pub const COMMIT_MODE: CommitMode = CommitMode::TwoPc;

/// One entry as read: its shard, its sequence and the entry.
pub type Read = (u16, u64, pb::JournalEntry);

/// The journal of one app: its keys and its shard count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Journal {
    app: AppKeys,
    shards: u16,
}

/// A consumer's checkpoint: its position in every shard, and when it
/// expires (`None`: never, for durable consumers such as the bridge).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub positions: Vec<u64>,
    pub expires_ms: Option<u64>,
}

impl Journal {
    /// The journal of `app` with `shards` shards (1 to [`MAX_SHARDS`]).
    pub fn new(app: AppKeys, shards: u16) -> Result<Self, LiveError> {
        if shards == 0 || shards > MAX_SHARDS {
            return Err(LiveError::invalid(format!(
                "a journal has 1 to {MAX_SHARDS} shards, not {shards}"
            )));
        }
        Ok(Journal { app, shards })
    }

    /// The shard count.
    pub fn shards(&self) -> u16 {
        self.shards
    }

    /// The app's keys.
    pub fn app(&self) -> &AppKeys {
        &self.app
    }

    /// A shard drawn from `rng`, uniformly.
    pub fn pick(&self, rng: &mut (impl Rng + ?Sized)) -> u16 {
        rng.random_range(0..self.shards)
    }

    /// A shard drawn from `rng` uniformly among those other than `exclude`
    /// (every shard when `exclude` is `None` or the journal has one shard).
    /// A mutation's rerun excludes the shard its previous attempt drew, so a
    /// shard-head conflict is not met again on the same head (R1 plan row
    /// T10-2).
    pub fn pick_other(&self, rng: &mut (impl Rng + ?Sized), exclude: Option<u16>) -> u16 {
        match exclude {
            Some(skip) if self.shards > 1 && skip < self.shards => {
                let shard = rng.random_range(0..self.shards - 1);
                if shard >= skip { shard + 1 } else { shard }
            }
            _ => self.pick(rng),
        }
    }

    /// Appends `entry` to a shard drawn from `rng`; returns the shard and the
    /// entry's sequence (the last one, if it was split: the shard's new
    /// head). The shard is drawn before the returned future runs, so `rng`
    /// need not be `Send`; a rerun of the transaction draws again.
    ///
    /// Sets `commit_hint_ms` to the physical time of the transaction's start
    /// timestamp. Refuses an entry without writes (read-only mutations write
    /// none). A conflict on the head is a [`TxnError::Conflict`] of the
    /// commit, which the runner reruns.
    pub fn append<'a>(
        &'a self,
        txn: &'a mut Txn,
        entry: pb::JournalEntry,
        rng: &mut (impl Rng + ?Sized),
    ) -> impl Future<Output = Result<(u16, u64), LiveError>> + Send + 'a {
        let shard = self.pick(rng);
        self.append_to(txn, shard, entry)
    }

    /// Like [`append`](Self::append), into `shard`.
    pub async fn append_to(
        &self,
        txn: &mut Txn,
        shard: u16,
        mut entry: pb::JournalEntry,
    ) -> Result<(u16, u64), LiveError> {
        self.check_shard(shard)?;
        if entry.writes.is_empty() {
            return Err(LiveError::invalid(
                "a journal entry needs at least one write (read-only mutations write none)",
            ));
        }
        entry.commit_hint_ms = Tikv::physical_ms(&txn.start_ts());
        let head_key = self.app.journal_head(shard);
        let mut seq = decode_head(txn.get(&head_key).await?.as_deref())?;
        for chunk in split(entry)? {
            seq += 1;
            txn.put(&self.app.journal_entry(shard, seq), chunk.encode_to_vec())
                .await?;
        }
        txn.put(&head_key, seq.to_be_bytes().to_vec()).await?;
        Ok((shard, seq))
    }

    /// Every shard's head (0 for a shard never written), in one batch get.
    pub async fn heads(&self, reads: &mut impl Reads) -> Result<Vec<u64>, LiveError> {
        // Head keys sort by shard, so a key's index in this list is its shard.
        let keys: Vec<Vec<u8>> = (0..self.shards)
            .map(|shard| self.app.journal_head(shard))
            .collect();
        let found = reads.batch_get(keys.clone()).await?;
        let mut heads = vec![0; usize::from(self.shards)];
        for (key, value) in found {
            let shard = keys
                .binary_search(&key)
                .map_err(|_| LiveError::Corrupt("a journal head outside the shards".into()))?;
            heads[shard] = decode_head(Some(&value))?;
        }
        Ok(heads)
    }

    /// The entries in `(from[s], to[s]]` of every shard `s`, by shard, then
    /// sequence. A range whose first entries are gone is
    /// [`LiveError::JournalTrimmed`]; a gap inside it is corrupt.
    pub async fn read(
        &self,
        reads: &mut impl Reads,
        from: &[u64],
        to: &[u64],
    ) -> Result<Vec<Read>, LiveError> {
        let (entries, _) = self.read_bounded(reads, from, to, usize::MAX).await?;
        Ok(entries)
    }

    /// Like [`read`](Self::read), stopping once the entries read hold
    /// `max_bytes` encoded bytes (at an entry boundary, after at least one
    /// entry): returns the entries and the last sequence read in every shard
    /// (`to[s]` for the shards read to the end, `from[s]` for those not
    /// reached).
    ///
    /// Every key wanted is known (`(from[s], to[s]]` of each shard), so the
    /// entries are read by batch gets of up to [`READ_PAGE_ENTRIES`] keys over
    /// all the moved shards at once, not by one scan per shard: at 64
    /// shards a tick under load reads most of them (R1 plan row T11-3).
    pub async fn read_bounded(
        &self,
        reads: &mut impl Reads,
        from: &[u64],
        to: &[u64],
        max_bytes: usize,
    ) -> Result<(Vec<Read>, Vec<u64>), LiveError> {
        self.check_positions(from)?;
        self.check_positions(to)?;
        for shard in 0..self.shards {
            let (lo, hi) = (from[usize::from(shard)], to[usize::from(shard)]);
            if hi < lo {
                return Err(LiveError::invalid(format!(
                    "journal shard {shard}: head {hi} is below position {lo}"
                )));
            }
        }
        let mut out = Vec::new();
        let mut reached = from.to_vec();
        let mut bytes = 0usize;
        // The next sequence wanted in each shard, and the shard being read.
        let mut next: Vec<u64> = from.iter().map(|p| p + 1).collect();
        let mut shard = 0u16;
        while shard < self.shards && bytes < max_bytes {
            // The next batch: the wanted keys in shard, then sequence, order.
            let mut wanted: Vec<(u16, u64)> = Vec::new();
            let mut s = shard;
            while s < self.shards && wanted.len() < READ_PAGE_ENTRIES {
                let hi = to[usize::from(s)];
                let seq = &mut next[usize::from(s)];
                while *seq <= hi && wanted.len() < READ_PAGE_ENTRIES {
                    wanted.push((s, *seq));
                    *seq += 1;
                }
                if *seq > hi {
                    s += 1;
                }
            }
            if wanted.is_empty() {
                break;
            }
            let keys: Vec<Vec<u8>> = wanted
                .iter()
                .map(|(s, seq)| self.app.journal_entry(*s, *seq))
                .collect();
            let mut found: std::collections::HashMap<Vec<u8>, Vec<u8>> =
                reads.batch_get(keys.clone()).await?.into_iter().collect();
            for ((s, seq), key) in wanted.into_iter().zip(keys) {
                let Some(value) = found.remove(&key) else {
                    let position = from[usize::from(s)];
                    return Err(if seq == position + 1 {
                        LiveError::JournalTrimmed {
                            shard: s,
                            position,
                            first: self.first_entry(reads, s, seq, to[usize::from(s)]).await?,
                        }
                    } else {
                        LiveError::Corrupt(format!("journal shard {s}: entry {seq} is missing"))
                    });
                };
                bytes = bytes.saturating_add(value.len());
                out.push((s, seq, decode_entry(&value)?));
                reached[usize::from(s)] = seq;
                if bytes >= max_bytes {
                    return Ok((out, reached));
                }
            }
            shard = s;
        }
        Ok((out, reached))
    }

    /// The first entry of `shard` in `from..=to`, if any (the error path of
    /// a trimmed read).
    async fn first_entry(
        &self,
        reads: &mut impl Reads,
        shard: u16,
        from: u64,
        to: u64,
    ) -> Result<Option<u64>, LiveError> {
        let range = KeyRange {
            lo: self.app.journal_entry(shard, from),
            hi: key_after(&self.app.journal_entry(shard, to)),
        };
        let pairs = reads.scan(&range, 1, false).await?;
        Ok(pairs
            .first()
            .and_then(|(key, _)| self.app.seq_of_journal_entry(shard, key)))
    }

    /// Writes `consumer`'s checkpoint at `positions`, expiring `ttl` after
    /// the transaction's start (`None`: never).
    pub async fn checkpoint(
        &self,
        txn: &mut Txn,
        consumer: &str,
        positions: &[u64],
        ttl: Option<Duration>,
    ) -> Result<(), LiveError> {
        check_consumer(consumer)?;
        self.check_positions(positions)?;
        let expires_ms = match ttl {
            None => 0,
            Some(ttl) => Tikv::physical_ms(&txn.start_ts())
                .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
                .max(1),
        };
        for shard in 0..self.shards {
            let mut value = Vec::with_capacity(16);
            value.extend_from_slice(&positions[usize::from(shard)].to_be_bytes());
            value.extend_from_slice(&expires_ms.to_be_bytes());
            txn.put(&self.app.journal_checkpoint(shard, consumer), value)
                .await?;
        }
        Ok(())
    }

    /// `consumer`'s checkpoint, or `None` if it has none.
    pub async fn load_checkpoint(
        &self,
        reads: &mut impl Reads,
        consumer: &str,
    ) -> Result<Option<Checkpoint>, LiveError> {
        check_consumer(consumer)?;
        let keys: Vec<Vec<u8>> = (0..self.shards)
            .map(|shard| self.app.journal_checkpoint(shard, consumer))
            .collect();
        let found = reads.batch_get(keys.clone()).await?;
        if found.is_empty() {
            return Ok(None);
        }
        if found.len() != keys.len() {
            return Err(LiveError::Corrupt(format!(
                "consumer {consumer} has checkpoints in {} of {} shards",
                found.len(),
                keys.len()
            )));
        }
        let mut positions = vec![0; usize::from(self.shards)];
        let mut expires = 0;
        for (key, value) in found {
            let shard = keys
                .iter()
                .position(|k| *k == key)
                .ok_or_else(|| LiveError::Corrupt("a checkpoint outside the shards".into()))?;
            let (seq, expires_ms) = decode_checkpoint(&value)?;
            positions[shard] = seq;
            expires = expires_ms;
        }
        Ok(Some(Checkpoint {
            positions,
            expires_ms: (expires != 0).then_some(expires),
        }))
    }

    /// Deletes `consumer`'s checkpoint.
    pub async fn remove_checkpoint(&self, txn: &mut Txn, consumer: &str) -> Result<(), LiveError> {
        check_consumer(consumer)?;
        for shard in 0..self.shards {
            txn.delete(&self.app.journal_checkpoint(shard, consumer))
                .await?;
        }
        Ok(())
    }

    /// One janitor pass over `shard`: deletes expired checkpoints, then up
    /// to [`TRIM_BATCH`] entries at or below every live checkpoint (the
    /// head, when there is none) whose `commit_hint_ms` is at least
    /// `retention_ms` before the transaction's start.
    async fn trim(
        &self,
        txn: &mut Txn,
        shard: u16,
        retention_ms: u64,
    ) -> Result<TrimPass, LiveError> {
        let now_ms = Tikv::physical_ms(&txn.start_ts());
        let mut pass = TrimPass::default();
        let range = self.app.journal_checkpoints(shard);
        let (lo, hi) = range.bounds();
        let checkpoints = txn.scan(lo, hi, MAX_CONSUMERS + 1).await?;
        if checkpoints.len() > MAX_CONSUMERS {
            return Err(LiveError::limit(
                "journal_consumers",
                format!("journal shard {shard} has more than {MAX_CONSUMERS} checkpoints"),
            ));
        }
        let mut floor: Option<u64> = None;
        for (key, value) in checkpoints {
            let (seq, expires_ms) = decode_checkpoint(&value)?;
            if expires_ms != 0 && expires_ms <= now_ms {
                // An expired consumer goes from every shard at once, so
                // `load_checkpoint` never sees it in some shards only.
                let consumer = key
                    .strip_prefix(range.lo.as_slice())
                    .and_then(|c| std::str::from_utf8(c).ok())
                    .ok_or_else(|| LiveError::Corrupt("a journal checkpoint key".into()))?;
                for s in 0..self.shards {
                    txn.delete(&self.app.journal_checkpoint(s, consumer))
                        .await?;
                }
                pass.expired += u64::from(self.shards);
            } else {
                floor = Some(floor.map_or(seq, |f| f.min(seq)));
            }
        }
        let head = decode_head(txn.get(&self.app.journal_head(shard)).await?.as_deref())?;
        let floor = floor.unwrap_or(head).min(head);
        pass.floor = floor;
        pass.done = true;
        if floor == 0 {
            return Ok(pass);
        }
        let lo = self.app.journal_entries(shard).lo;
        let hi = key_after(&self.app.journal_entry(shard, floor));
        let entries = txn.scan(&lo, Some(&hi), TRIM_BATCH).await?;
        pass.done = entries.len() < TRIM_BATCH;
        for (key, value) in entries {
            if decode_entry(&value)?
                .commit_hint_ms
                .saturating_add(retention_ms)
                > now_ms
            {
                pass.done = true;
                break;
            }
            txn.delete(&key).await?;
            pass.deleted += 1;
        }
        Ok(pass)
    }

    fn check_shard(&self, shard: u16) -> Result<(), LiveError> {
        if shard >= self.shards {
            return Err(LiveError::invalid(format!(
                "journal shard {shard} is not below the shard count {}",
                self.shards
            )));
        }
        Ok(())
    }

    fn check_positions(&self, positions: &[u64]) -> Result<(), LiveError> {
        if positions.len() != usize::from(self.shards) {
            return Err(LiveError::invalid(format!(
                "{} journal positions for {} shards",
                positions.len(),
                self.shards
            )));
        }
        Ok(())
    }
}

/// What the tailer read in one tick: the entries in `(from, heads]` of every
/// shard at `at`. `heads` is the last sequence read in each shard; when the
/// tick's byte budget stopped it before the heads visible at `at`,
/// `complete` is false and the next tick continues from `heads`.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    pub at: Timestamp,
    pub from: Vec<u64>,
    pub heads: Vec<u64>,
    pub entries: Vec<Read>,
    /// Every entry committed at or before `at` has been read (by this and
    /// earlier ticks): `heads` are the heads visible at `at`.
    pub complete: bool,
}

impl Batch {
    /// Whether no shard moved.
    pub fn is_empty(&self) -> bool {
        self.from == self.heads
    }
}

/// Follows an app's journal from one position per shard (§20 §8.2 step 1).
#[derive(Debug, Clone)]
pub struct Tailer {
    tikv: Tikv,
    journal: Journal,
    positions: Vec<u64>,
    max_batch_bytes: usize,
}

impl Tailer {
    /// A tailer at `positions`.
    pub fn new(tikv: Tikv, journal: Journal, positions: Vec<u64>) -> Result<Self, LiveError> {
        journal.check_positions(&positions)?;
        Ok(Tailer {
            tikv,
            journal,
            positions,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
        })
    }

    /// With another per-tick byte budget (at least one entry is always
    /// read).
    #[must_use]
    pub fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes.max(1);
        self
    }

    /// A tailer at the heads visible at `at`: it sees exactly the entries
    /// committed after `at`.
    pub async fn start(tikv: Tikv, journal: Journal, at: Timestamp) -> Result<Self, LiveError> {
        let mut snap = snapshot(&tikv, at).await?;
        let positions = journal.heads(&mut snap).await?;
        Tailer::new(tikv, journal, positions)
    }

    /// A tailer at `consumer`'s checkpoint, or `None` if it has none.
    pub async fn resume(
        tikv: Tikv,
        journal: Journal,
        consumer: &str,
    ) -> Result<Option<Self>, LiveError> {
        let at = tikv
            .now()
            .await
            .map_err(|e| LiveError::Internal(e.to_string()))?;
        let mut snap = snapshot(&tikv, at).await?;
        match journal.load_checkpoint(&mut snap, consumer).await? {
            None => Ok(None),
            Some(checkpoint) => Tailer::new(tikv, journal, checkpoint.positions).map(Some),
        }
    }

    /// The positions: the last sequence consumed in every shard.
    pub fn positions(&self) -> &[u64] {
        &self.positions
    }

    /// The journal it follows.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// Reads the heads at `at`, then the entries after the positions of
    /// every moved shard, at `at`, up to the tick's byte budget (a backlog
    /// larger than it takes several ticks, each `complete == false` but the
    /// last). The positions do not move until [`ack`](Self::ack): a tick
    /// that is not acknowledged is read again by the next one.
    pub async fn tick(&self, at: Timestamp) -> Result<Batch, LiveError> {
        let mut snap = snapshot(&self.tikv, at.clone()).await?;
        let visible = self.journal.heads(&mut snap).await?;
        let (entries, heads) = self
            .journal
            .read_bounded(&mut snap, &self.positions, &visible, self.max_batch_bytes)
            .await?;
        Ok(Batch {
            at,
            from: self.positions.clone(),
            complete: heads == visible,
            heads,
            entries,
        })
    }

    /// Advances the positions to `batch`'s heads. Refuses a batch that was
    /// not read from the current positions (an older tick's).
    pub fn ack(&mut self, batch: &Batch) -> Result<(), LiveError> {
        if batch.from != self.positions {
            return Err(LiveError::invalid(
                "the batch was not read from the tailer's current positions",
            ));
        }
        self.positions.clone_from(&batch.heads);
        Ok(())
    }

    /// Writes the positions as `consumer`'s checkpoint, expiring after `ttl`
    /// (`None`: never).
    pub async fn checkpoint(&self, consumer: &str, ttl: Option<Duration>) -> Result<(), LiveError> {
        let journal = self.journal.clone();
        let positions = self.positions.clone();
        let consumer = consumer.to_string();
        self.tikv
            .run(journal_txn("live.journal.checkpoint"), move |txn| {
                let journal = journal.clone();
                let positions = positions.clone();
                let consumer = consumer.clone();
                Box::pin(
                    async move { lift(journal.checkpoint(txn, &consumer, &positions, ttl).await) },
                )
            })
            .await?
            .value
    }
}

/// What one [`Janitor::run_once`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JanitorReport {
    /// Entries deleted.
    pub deleted: u64,
    /// Expired checkpoint records deleted (one per consumer and shard).
    pub expired_checkpoints: u64,
    /// Per shard, the sequence every live consumer has passed (the head
    /// when there is none) at the last pass.
    pub floors: Vec<u64>,
    /// Expired idempotency records deleted (R1 plan Task 10 semantics 3).
    pub expired_idempotency: u64,
}

#[derive(Debug, Default)]
struct TrimPass {
    deleted: u64,
    expired: u64,
    floor: u64,
    done: bool,
}

/// Deletes journal entries every live consumer has passed once they are
/// older than the retention (§20 §5.3). Checkpoints past their expiry are
/// deleted and no longer hold entries. Each pass also deletes the app's
/// expired idempotency records (§20 §5.1).
#[derive(Debug, Clone)]
pub struct Janitor {
    tikv: Tikv,
    journal: Journal,
    retention: Duration,
}

impl Janitor {
    /// A janitor with the default retention (10 min).
    pub fn new(tikv: Tikv, journal: Journal) -> Self {
        Janitor {
            tikv,
            journal,
            retention: DEFAULT_RETENTION,
        }
    }

    /// With another retention.
    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// One pass over every shard, in transactions of up to [`TRIM_BATCH`]
    /// deletions.
    pub async fn run_once(&self) -> Result<JanitorReport, LiveError> {
        let retention_ms = u64::try_from(self.retention.as_millis()).unwrap_or(u64::MAX);
        let mut report = JanitorReport {
            floors: vec![0; usize::from(self.journal.shards)],
            ..JanitorReport::default()
        };
        for shard in 0..self.journal.shards {
            loop {
                let journal = self.journal.clone();
                let pass = self
                    .tikv
                    .run(journal_txn("live.journal.trim"), move |txn| {
                        let journal = journal.clone();
                        Box::pin(async move { lift(journal.trim(txn, shard, retention_ms).await) })
                    })
                    .await?
                    .value?;
                report.deleted += pass.deleted;
                report.expired_checkpoints += pass.expired;
                report.floors[usize::from(shard)] = pass.floor;
                if pass.done {
                    break;
                }
            }
        }
        let mut from = self.journal.app.idempotency_records().lo;
        loop {
            let app = self.journal.app.clone();
            let start = from.clone();
            let pass = self
                .tikv
                .run(journal_txn("live.idempotency.sweep"), move |txn| {
                    let app = app.clone();
                    let start = start.clone();
                    Box::pin(async move { lift(sweep_idempotency(txn, &app, start).await) })
                })
                .await?
                .value?;
            report.expired_idempotency += pass.0;
            match pass.1 {
                Some(next) => from = next,
                None => break,
            }
        }
        Ok(report)
    }
}

/// Deletes the expired idempotency records among the next [`TRIM_BATCH`]
/// from `from`; returns how many, and where the next pass starts (`None`
/// at the end).
async fn sweep_idempotency(
    txn: &mut Txn,
    app: &AppKeys,
    from: Vec<u8>,
) -> Result<(u64, Option<Vec<u8>>), LiveError> {
    let now_ms = Tikv::physical_ms(&txn.start_ts());
    let all = app.idempotency_records();
    let (_, hi) = all.bounds();
    let records = txn.scan(&from, hi, TRIM_BATCH).await?;
    let next = (records.len() == TRIM_BATCH)
        .then(|| records.last().map(|(k, _)| key_after(k)))
        .flatten();
    let mut deleted = 0;
    for (key, value) in records {
        if crate::txn::decode_idempotency(&value)?.expires_ms <= now_ms {
            txn.delete(&key).await?;
            deleted += 1;
        }
    }
    Ok((deleted, next))
}

/// The options of the journal's own transactions.
fn journal_txn(op: &'static str) -> TxnOptions {
    let mut opts = TxnOptions::new(op);
    opts.commit_mode = Some(COMMIT_MODE);
    opts
}

/// A run body's result: a storage error goes back to the runner (which
/// retries conflicts); any other stays in the value.
fn lift<T>(r: Result<T, LiveError>) -> Result<Result<T, LiveError>, TxnError> {
    match r {
        Ok(v) => Ok(Ok(v)),
        Err(e) => e.into_txn().map(Err),
    }
}

async fn snapshot(tikv: &Tikv, at: Timestamp) -> Result<operon_tikv::Snap, LiveError> {
    tikv.snapshot(at)
        .await
        .map_err(|e| LiveError::Internal(format!("a journal snapshot: {e}")))
}

fn check_consumer(consumer: &str) -> Result<(), LiveError> {
    if consumer.is_empty() || consumer.len() > MAX_CONSUMER_BYTES {
        return Err(LiveError::invalid(format!(
            "a journal consumer id has 1 to {MAX_CONSUMER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn decode_head(value: Option<&[u8]>) -> Result<u64, LiveError> {
    match value {
        None => Ok(0),
        Some(bytes) => bytes
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| LiveError::Corrupt("a journal head is not 8 bytes".into())),
    }
}

fn decode_checkpoint(value: &[u8]) -> Result<(u64, u64), LiveError> {
    if value.len() != 16 {
        return Err(LiveError::Corrupt(
            "a journal checkpoint is not 16 bytes".into(),
        ));
    }
    let (seq, expires) = value.split_at(8);
    Ok((
        u64::from_be_bytes(seq.try_into().unwrap_or_default()),
        u64::from_be_bytes(expires.try_into().unwrap_or_default()),
    ))
}

fn decode_entry(value: &[u8]) -> Result<pb::JournalEntry, LiveError> {
    pb::JournalEntry::decode_from_slice(value)
        .map_err(|e| LiveError::Corrupt(format!("a journal entry: {e}")))
}

/// Splits `entry` into chunks of at most [`MAX_CHUNK_BYTES`] encoded, in
/// order, each with the entry's header fields.
pub fn split(entry: pb::JournalEntry) -> Result<Vec<pb::JournalEntry>, LiveError> {
    let pb::JournalEntry {
        commit_hint_ms,
        writes,
        function,
        request_id,
        ..
    } = entry;
    let header = pb::JournalEntry {
        commit_hint_ms,
        function,
        request_id,
        ..Default::default()
    };
    let header_len = header.encoded_len() as usize;
    let mut chunks = Vec::new();
    let mut chunk = header.clone();
    let mut len = header_len;
    for write in writes {
        // The record, its field tag and its length prefix (at most 5 bytes).
        let write_len = write.encoded_len() as usize + 6;
        if header_len + write_len > MAX_CHUNK_BYTES {
            return Err(LiveError::Internal(format!(
                "a write record of {write_len} bytes does not fit a journal chunk"
            )));
        }
        if !chunk.writes.is_empty() && len + write_len > MAX_CHUNK_BYTES {
            chunks.push(std::mem::replace(&mut chunk, header.clone()));
            len = header_len;
        }
        len += write_len;
        chunk.writes.push(write);
    }
    chunks.push(chunk);
    Ok(chunks)
}
