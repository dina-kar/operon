//! The shared journal on local NVMe (§28 §7.2, D265–D266): one append-only
//! log for every timeline a shard owns.
//!
//! - **Records** ([`format`]) are packed by the caller into the open **flush
//!   unit**, a 4 KiB-aligned buffer; [`Journal::append`] returns at once with
//!   the unit's id and where the payload will sit on disk.
//! - **An engine** takes sealed units and makes them durable: the
//!   [`Tier::Buffered`] thread (`pwrite` + back-to-back `fdatasync`), the
//!   [`Tier::Pwritev2`] pool (`O_DIRECT` + `pwritev2(RWF_DSYNC)`, one unit per
//!   thread in flight), or the io_uring engine on a compio shard (feature
//!   `compio`). Whatever an engine finds pending when it frees up goes out as
//!   one unit: that is the group commit.
//! - **Durability** is a unit id: [`Journal::subscribe`] reports the highest
//!   id such that it and every lower id are durable. Units can complete out
//!   of order; acknowledgements only follow the prefix.
//! - **Segments** ([`segments`]) are prepared ahead by a background thread
//!   (pre-zeroed or recycled), so a rollover never allocates on the commit
//!   path.
//! - **Recovery** ([`Journal::open`]) replays every record in order to a
//!   callback, stopping at each segment's torn tail, then `fdatasync`s what it
//!   read (a crashed process may have left unsynced pages behind).

pub mod format;
pub mod segments;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::Error;
use crate::types::TimelineId;
use format::{BLOCK, Kind, Parsed, RECORD_HEADER, RecordHeader, SEGMENT_HEADER, align_up};
use segments::{SegmentDir, SegmentFile};

/// A heap buffer aligned to [`BLOCK`], as `O_DIRECT` needs: a `Vec` one
/// block larger than the capacity, used from its first aligned byte (the
/// heap block never moves, so neither does the alignment).
pub struct AlignedBuf {
    raw: Vec<u8>,
    start: usize,
    cap: usize,
    len: usize,
}

impl AlignedBuf {
    /// An empty buffer of at least `cap` bytes (rounded up to a block).
    pub fn new(cap: usize) -> AlignedBuf {
        let cap = align_up(cap.max(1), BLOCK);
        let raw = vec![0u8; cap + BLOCK];
        let start = raw.as_ptr().align_offset(BLOCK);
        AlignedBuf {
            raw,
            start,
            cap,
            len: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Append bytes; panics past the capacity (callers check first).
    pub fn extend_from_slice(&mut self, b: &[u8]) {
        assert!(self.len + b.len() <= self.cap, "AlignedBuf overflow");
        let at = self.start + self.len;
        self.raw[at..at + b.len()].copy_from_slice(b);
        self.len += b.len();
    }

    /// Grow to `n` bytes with zeroes (never shrinks).
    pub fn zero_to(&mut self, n: usize) {
        assert!(n <= self.cap, "AlignedBuf overflow");
        if n > self.len {
            self.raw[self.start + self.len..self.start + n].fill(0);
            self.len = n;
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.raw[self.start..].as_ptr()
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.raw[self.start..self.start + self.len]
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.raw[self.start..self.start + self.len]
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlignedBuf({}/{})", self.len, self.cap)
    }
}

/// Where a record's bytes go: a flush unit, or a plain `Vec` in tests.
pub trait Sink {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn put(&mut self, b: &[u8]);
    fn zero_to(&mut self, n: usize);
    fn patch(&mut self, at: usize, b: &[u8]);
    fn tail_from(&self, at: usize) -> &[u8];
}

impl Sink for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn put(&mut self, b: &[u8]) {
        self.extend_from_slice(b);
    }
    fn zero_to(&mut self, n: usize) {
        if n > Vec::len(self) {
            self.resize(n, 0);
        }
    }
    fn patch(&mut self, at: usize, b: &[u8]) {
        self[at..at + b.len()].copy_from_slice(b);
    }
    fn tail_from(&self, at: usize) -> &[u8] {
        &self[at..]
    }
}

impl Sink for AlignedBuf {
    fn len(&self) -> usize {
        self.len
    }
    fn put(&mut self, b: &[u8]) {
        self.extend_from_slice(b);
    }
    fn zero_to(&mut self, n: usize) {
        AlignedBuf::zero_to(self, n);
    }
    fn patch(&mut self, at: usize, b: &[u8]) {
        self[at..at + b.len()].copy_from_slice(b);
    }
    fn tail_from(&self, at: usize) -> &[u8] {
        &self[at..]
    }
}

/// How the journal makes units durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// `pwrite` into the page cache; one thread runs `fdatasync` back to back.
    Buffered,
    /// `O_DIRECT` + `pwritev2(RWF_DSYNC)`; `depth` threads, one unit each.
    Pwritev2 { depth: usize },
    /// io_uring on the owning compio shard ([`crate::uring`]); started by the
    /// shard, not by [`Journal::start`].
    Uring { depth: usize },
}

impl Tier {
    /// Whether the tier writes with `O_DIRECT`.
    pub fn direct(&self) -> bool {
        !matches!(self, Tier::Buffered)
    }
}

/// Settings of a [`Journal`].
#[derive(Clone, Debug)]
pub struct JournalConfig {
    pub dir: PathBuf,
    /// Bytes per segment file (a multiple of 4 KiB).
    pub segment_size: u64,
    pub tier: Tier,
    /// Segments kept prepared ahead of the writer.
    pub prepared: usize,
    /// The largest flush unit, and so the largest record.
    pub unit_capacity: usize,
    /// A segment that fills faster than this means the journal is ingesting
    /// fast: new segments are then prepared without pre-zeroing, which would
    /// double the bytes written. The preparer zeroes them once it is idle.
    pub hot_segment: Duration,
}

impl JournalConfig {
    pub fn new(dir: impl Into<PathBuf>, tier: Tier) -> Self {
        Self {
            dir: dir.into(),
            segment_size: 64 << 20,
            tier,
            prepared: 2,
            unit_capacity: 2 << 20,
            hot_segment: Duration::from_secs(2),
        }
    }

    /// The largest payload one record may carry.
    pub fn max_payload(&self) -> usize {
        self.unit_capacity - RECORD_HEADER - 8
    }
}

/// The journal's durable position: every unit up to and including `unit` is
/// on disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Durable {
    /// Every unit up to and including this one is durable (0: none yet).
    pub unit: u64,
    /// The journal stopped after a failed write: nothing more becomes durable.
    pub failed: bool,
}

/// Where [`Journal::append`] put a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Placed {
    /// The flush unit; the record is durable once [`Durable::unit`] reaches it.
    pub unit: u64,
    /// The segment and the payload's offset in it.
    pub seq: u64,
    pub payload_off: u64,
}

/// A sealed flush unit, ready for an engine.
#[derive(Debug)]
pub struct Unit {
    pub id: u64,
    pub seg: Arc<SegmentFile>,
    /// Offset in the segment (a multiple of [`BLOCK`]).
    pub off: u64,
    /// The unit's bytes, padded to [`BLOCK`].
    pub buf: AlignedBuf,
}

#[derive(Debug)]
struct State {
    cur: Arc<SegmentFile>,
    /// When `cur` became the current segment.
    cur_since: std::time::Instant,
    /// Where the open unit starts in `cur`.
    next_off: u64,
    open: Option<AlignedBuf>,
    /// The id the open unit gets when sealed.
    next_id: u64,
    sealed: VecDeque<Unit>,
    /// Prepared, unused segments, in order.
    ready: VecDeque<Arc<SegmentFile>>,
    /// Segments with data (and `cur`), by sequence.
    live: BTreeMap<u64, Arc<SegmentFile>>,
    /// Retired segments, for the preparer to recycle.
    free: Vec<Retired>,
    max_seq: u64,
    /// Units taken by an engine and not yet completed: id → segment.
    inflight: BTreeMap<u64, u64>,
    done: BTreeSet<u64>,
    durable: u64,
    failed: Option<String>,
    /// Per segment: the highest WAL end of each timeline's Append records.
    ends: BTreeMap<u64, HashMap<TimelineId, u64>>,
    pool: Vec<AlignedBuf>,
    shutdown: bool,
}

struct Inner {
    segs: SegmentDir,
    cfg: JournalConfig,
    st: Mutex<State>,
    /// Wakes the thread engines.
    work: Condvar,
    /// Wakes the preparer.
    prep: Condvar,
    durable: watch::Sender<Durable>,
    /// Wakes an async engine (the compio shard's).
    #[allow(clippy::type_complexity)]
    waker: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// The engine and preparer threads, joined by [`Journal::close`].
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("dir", &self.cfg.dir)
            .field("tier", &self.cfg.tier)
            .finish_non_exhaustive()
    }
}

/// The shared journal. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Journal {
    inner: Arc<Inner>,
}

/// A retired segment file: recyclable once no reader holds it.
#[derive(Debug)]
struct Retired {
    path: PathBuf,
    held: Option<Arc<SegmentFile>>,
}

/// Buffers kept for reuse per journal.
const POOL: usize = 8;

fn io_err(e: io::Error) -> Error {
    Error::Store(format!("journal: {e}"))
}

impl Journal {
    /// Open (or create) the journal in `cfg.dir`, replaying every valid
    /// record to `on_record(seq, header, payload_offset)` in log order.
    /// Engines are not started: call [`Journal::start`] (or start the
    /// io_uring engine on a shard).
    pub fn open(
        cfg: JournalConfig,
        mut on_record: impl FnMut(u64, &RecordHeader, u64),
    ) -> Result<Journal, Error> {
        if cfg.unit_capacity < BLOCK
            || cfg.unit_capacity as u64 + SEGMENT_HEADER as u64 > cfg.segment_size
        {
            return Err(Error::Store(format!(
                "journal: a {}-byte unit does not fit a {}-byte segment",
                cfg.unit_capacity, cfg.segment_size
            )));
        }
        let segs =
            SegmentDir::open(&cfg.dir, cfg.segment_size, cfg.tier.direct()).map_err(io_err)?;
        let seqs = segs.list().map_err(io_err)?;
        let mut with_data = Vec::new();
        let mut empty = Vec::new();
        let mut ends: BTreeMap<u64, HashMap<TimelineId, u64>> = BTreeMap::new();
        let mut buf = vec![0u8; cfg.segment_size as usize];
        let mut records = 0u64;
        for &seq in &seqs {
            let f = segs.open_segment(seq).map_err(io_err)?;
            let n = read_fully(&f.read, &mut buf).map_err(io_err)?;
            let data = &buf[..n];
            let mut pos = SEGMENT_HEADER;
            let mut any = false;
            loop {
                match format::parse_record(&data[pos.min(data.len())..], seq, pos) {
                    Parsed::Record { header, size, .. } => {
                        on_record(seq, &header, (pos + RECORD_HEADER) as u64);
                        if header.kind == Kind::Append {
                            let end = header.lsn.0 + u64::from(header.len);
                            let e = ends.entry(seq).or_default().entry(header.tl).or_default();
                            *e = (*e).max(end);
                        }
                        pos += size;
                        any = true;
                        records += 1;
                    }
                    Parsed::Pad => pos = align_up(pos, BLOCK),
                    Parsed::End => break,
                }
            }
            if any {
                f.sync().map_err(io_err)?;
                with_data.push(f);
            } else {
                empty.push(f);
            }
        }
        drop(buf);
        let (mut ready, mut free): (VecDeque<_>, Vec<_>) = (VecDeque::new(), Vec::new());
        for p in segs.list_free().map_err(io_err)? {
            free.push(Retired {
                path: p,
                held: None,
            });
        }
        // An empty segment may still hold stale records past the first unit
        // (a torn or zeroed head): never reuse it under its own number, only
        // recycle its file under a new one.
        for f in empty {
            let seq = f.seq;
            drop(f);
            free.push(Retired {
                path: segs.retire(seq).map_err(io_err)?,
                held: None,
            });
        }
        // A `.free` file keeps the number it was last written under, and its
        // later units may still validate under that number: start above every
        // retired number too, so a crash between retiring empty segments and
        // preparing new ones never brings a stale number back.
        let free_max = free
            .iter()
            .filter_map(|r| {
                r.path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| u64::from_str_radix(s, 16).ok())
            })
            .max()
            .unwrap_or(0);
        let mut max_seq = seqs.last().copied().unwrap_or(0).max(free_max);
        // The first segments are prepared before the journal serves anything,
        // so pre-zeroing never competes with the first commits.
        while ready.len() < cfg.prepared + 1 {
            max_seq += 1;
            let recycle = take_recyclable(&mut free);
            ready.push_back(
                segs.prepare(max_seq, recycle.as_deref(), true)
                    .map_err(io_err)?,
            );
        }
        let cur = ready
            .pop_front()
            .ok_or_else(|| Error::Store("journal: no segment".into()))?;
        let mut live: BTreeMap<u64, Arc<SegmentFile>> =
            with_data.into_iter().map(|f| (f.seq, f)).collect();
        live.insert(cur.seq, cur.clone());
        info!(
            dir = %cfg.dir.display(),
            segments = live.len(),
            records,
            fs = ?segs.fs,
            direct = segs.direct,
            tier = ?cfg.tier,
            "journal opened"
        );
        let (durable, _) = watch::channel(Durable::default());
        let inner = Arc::new(Inner {
            segs,
            st: Mutex::new(State {
                cur,
                cur_since: std::time::Instant::now(),
                next_off: SEGMENT_HEADER as u64,
                open: None,
                next_id: 1,
                sealed: VecDeque::new(),
                ready,
                live,
                free,
                max_seq,
                inflight: BTreeMap::new(),
                done: BTreeSet::new(),
                durable: 0,
                failed: None,
                ends,
                pool: Vec::new(),
                shutdown: false,
            }),
            cfg,
            work: Condvar::new(),
            prep: Condvar::new(),
            durable,
            waker: Mutex::new(None),
            threads: Mutex::new(Vec::new()),
        });
        let j = Journal { inner };
        j.spawn_preparer();
        Ok(j)
    }

    /// Start the engine threads of a thread tier ([`Tier::Buffered`] or
    /// [`Tier::Pwritev2`]). A [`Tier::Uring`] journal is driven by its shard.
    pub fn start(&self) {
        match self.inner.cfg.tier {
            Tier::Buffered => self.spawn("journal-sync", run_buffered),
            Tier::Pwritev2 { depth } => {
                for i in 0..depth.max(1) {
                    self.spawn(&format!("journal-dsync-{i}"), run_pwritev2);
                }
            }
            Tier::Uring { .. } => {}
        }
    }

    fn spawn(&self, name: &str, f: fn(Journal)) {
        let j = self.clone();
        match std::thread::Builder::new()
            .name(name.into())
            .spawn(move || f(j))
        {
            Ok(h) => self
                .inner
                .threads
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(h),
            Err(e) => warn!(name, error = %e, "journal: could not start a thread"),
        }
    }

    pub fn config(&self) -> &JournalConfig {
        &self.inner.cfg
    }

    /// Whether write handles use `O_DIRECT` (it may have been refused).
    pub fn direct(&self) -> bool {
        self.inner.segs.direct
    }

    pub fn fs(&self) -> segments::FsKind {
        self.inner.segs.fs
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner.st.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Set the callback that wakes an async engine when work arrives.
    pub fn set_waker(&self, w: Arc<dyn Fn() + Send + Sync>) {
        *self.inner.waker.lock().unwrap_or_else(|p| p.into_inner()) = Some(w);
    }

    fn wake(&self) {
        self.inner.work.notify_one();
        if let Some(w) = &*self.inner.waker.lock().unwrap_or_else(|p| p.into_inner()) {
            w();
        }
    }

    /// Pack one record into the open unit. Returns where it went; it is
    /// durable once [`Durable::unit`] reaches [`Placed::unit`].
    pub fn append(&self, h: &RecordHeader, payload: &[&[u8]]) -> Result<Placed, Error> {
        let len: usize = payload.iter().map(|p| p.len()).sum();
        if len != h.len as usize {
            return Err(Error::Store(format!(
                "journal: header says {} bytes, payload has {len}",
                h.len
            )));
        }
        let size = format::record_size(len);
        let cap = self.inner.cfg.unit_capacity;
        if size > cap {
            return Err(Error::Store(format!(
                "journal: a {len}-byte record exceeds the {cap}-byte unit"
            )));
        }
        let seg_size = self.inner.cfg.segment_size;
        let placed = {
            let mut st = self.lock();
            if let Some(f) = &st.failed {
                return Err(Error::Store(format!("journal failed: {f}")));
            }
            let open_len = st.open.as_ref().map_or(0, |b| b.len());
            if st.next_off + align_up(open_len + size, BLOCK) as u64 > seg_size {
                self.seal(&mut st);
                self.rollover(&mut st)?;
            } else if open_len + size > cap {
                self.seal(&mut st);
            }
            if st.open.is_none() {
                let b = st.pool.pop().unwrap_or_else(|| AlignedBuf::new(cap));
                st.open = Some(b);
            }
            let seq = st.cur.seq;
            let next_off = st.next_off;
            let unit = st.next_id;
            let open = st
                .open
                .as_mut()
                .ok_or_else(|| Error::Store("no open unit".into()))?;
            let pos = open.len();
            format::put_record(open, seq, h, payload);
            if h.kind == Kind::Append {
                let end = h.lsn.0 + len as u64;
                let e = st.ends.entry(seq).or_default().entry(h.tl).or_default();
                *e = (*e).max(end);
            }
            Placed {
                unit,
                seq,
                payload_off: next_off + (pos + RECORD_HEADER) as u64,
            }
        };
        self.wake();
        Ok(placed)
    }

    fn seal(&self, st: &mut State) {
        let Some(mut buf) = st.open.take() else {
            return;
        };
        let padded = align_up(buf.len(), BLOCK);
        buf.zero_to(padded);
        let unit = Unit {
            id: st.next_id,
            seg: st.cur.clone(),
            off: st.next_off,
            buf,
        };
        st.next_off += padded as u64;
        st.next_id += 1;
        st.sealed.push_back(unit);
    }

    /// Whether the current segment is filling fast enough that new ones
    /// should skip pre-zeroing.
    fn is_hot(&self, st: &State) -> bool {
        st.cur_since.elapsed() < self.inner.cfg.hot_segment
    }

    fn rollover(&self, st: &mut State) -> Result<(), Error> {
        let next = match st.ready.pop_front() {
            Some(f) => f,
            None => {
                warn!("journal: no prepared segment at rollover; preparing inline");
                st.max_seq += 1;
                let recycle = take_recyclable(&mut st.free);
                self.inner
                    .segs
                    .prepare(st.max_seq, recycle.as_deref(), !self.is_hot(st))
                    .map_err(io_err)?
            }
        };
        st.live.insert(next.seq, next.clone());
        st.cur = next;
        st.cur_since = std::time::Instant::now();
        st.next_off = SEGMENT_HEADER as u64;
        self.inner.prep.notify_one();
        Ok(())
    }

    /// Take the next unit for an engine, sealing the open one if nothing is
    /// sealed yet. `None` when there is nothing to write.
    pub fn try_take(&self) -> Option<Unit> {
        let mut st = self.lock();
        Self::take_locked(self, &mut st)
    }

    fn take_locked(&self, st: &mut State) -> Option<Unit> {
        if st.sealed.is_empty() && st.open.as_ref().is_some_and(|b| !b.is_empty()) {
            self.seal(st);
        }
        let u = st.sealed.pop_front()?;
        st.inflight.insert(u.id, u.seg.seq);
        Some(u)
    }

    /// Block until there is work (`None` on shutdown), then take up to `max`
    /// units.
    fn take_blocking(&self, max: usize) -> Option<Vec<Unit>> {
        let mut st = self.lock();
        loop {
            if st.shutdown {
                return None;
            }
            let mut out = Vec::new();
            while out.len() < max {
                match self.take_locked(&mut st) {
                    Some(u) => out.push(u),
                    None => break,
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
            st = self.inner.work.wait(st).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Report units written (or failed). Buffers go back to the pool.
    pub fn complete(&self, units: Vec<Unit>, res: io::Result<()>) {
        let mut guard = self.lock();
        let st = &mut *guard;
        match res {
            Err(e) => {
                warn!(error = %e, "journal write failed; the journal stops");
                st.failed = Some(e.to_string());
                self.inner.durable.send_replace(Durable {
                    unit: st.durable,
                    failed: true,
                });
            }
            Ok(()) => {
                for u in &units {
                    st.inflight.remove(&u.id);
                    st.done.insert(u.id);
                }
                let before = st.durable;
                while st.done.remove(&(st.durable + 1)) {
                    st.durable += 1;
                }
                if st.durable != before && st.failed.is_none() {
                    self.inner.durable.send_replace(Durable {
                        unit: st.durable,
                        failed: false,
                    });
                }
            }
        }
        for mut u in units {
            if st.pool.len() < POOL && u.buf.capacity() == self.inner.cfg.unit_capacity {
                u.buf.clear();
                st.pool.push(u.buf);
            }
        }
    }

    /// Watch the durable position.
    pub fn subscribe(&self) -> watch::Receiver<Durable> {
        self.inner.durable.subscribe()
    }

    /// The durable position now.
    pub fn durable(&self) -> Durable {
        *self.inner.durable.borrow()
    }

    /// Wait until unit `unit` is durable.
    pub async fn wait(&self, unit: u64) -> Result<(), Error> {
        let mut rx = self.subscribe();
        let d = rx
            .wait_for(|d| d.failed || d.unit >= unit)
            .await
            .map_err(|_| Error::Store("journal closed".into()))?;
        if d.failed && d.unit < unit {
            return Err(Error::Store("journal failed".into()));
        }
        Ok(())
    }

    /// Read `len` bytes at `off` of segment `seq` (durable data only: the
    /// caller checks). An error if the segment was freed.
    pub fn read(&self, seq: u64, off: u64, len: usize) -> Result<Vec<u8>, Error> {
        let f = self
            .lock()
            .live
            .get(&seq)
            .cloned()
            .ok_or_else(|| Error::Store(format!("journal segment {seq} is gone")))?;
        let mut out = vec![0u8; len];
        f.read.read_exact_at(&mut out, off).map_err(io_err)?;
        Ok(out)
    }

    /// Free the oldest segments (never the current one) in which every
    /// timeline's Append records end at or below what `trimmed` says may go.
    /// Returns the freed sequence numbers; the preparer recycles their files.
    pub fn free(&self, trimmed: impl Fn(&TimelineId, u64) -> bool) -> Vec<u64> {
        let mut st = self.lock();
        let cur = st.cur.seq;
        let mut freed = Vec::new();
        while let Some((&seq, _)) = st.live.first_key_value() {
            if seq >= cur {
                break;
            }
            // A unit still queued or in flight in this segment keeps it.
            if st.sealed.iter().any(|u| u.seg.seq == seq) || st.inflight.values().any(|&s| s == seq)
            {
                break;
            }
            let ok = st
                .ends
                .get(&seq)
                .is_none_or(|m| m.iter().all(|(tl, end)| trimmed(tl, *end)));
            if !ok {
                break;
            }
            if let Some(f) = st.live.remove(&seq) {
                match self.inner.segs.retire(seq) {
                    Ok(path) => st.free.push(Retired {
                        path,
                        held: Some(f),
                    }),
                    Err(e) => {
                        warn!(seq, error = %e, "could not retire a segment; keeping it");
                        st.live.insert(seq, f);
                        break;
                    }
                }
            }
            st.ends.remove(&seq);
            freed.push(seq);
        }
        // Keep a few files to recycle; delete the rest.
        while st.free.len() > self.inner.cfg.prepared {
            if let Some(r) = st.free.pop()
                && let Err(e) = self.inner.segs.remove(&r.path)
            {
                warn!(path = %r.path.display(), error = %e, "could not delete a freed segment");
            }
        }
        if !freed.is_empty() {
            self.inner.prep.notify_one();
        }
        freed
    }

    /// Whether each prepared, unused segment has been pre-zeroed (tests).
    #[cfg(test)]
    fn ready_zeroed(&self) -> Vec<bool> {
        self.lock()
            .ready
            .iter()
            .map(|f| f.zeroed.load(std::sync::atomic::Ordering::Relaxed))
            .collect()
    }

    /// The live segments' sequence numbers (for tests and metrics).
    pub fn live_segments(&self) -> Vec<u64> {
        self.lock().live.keys().copied().collect()
    }

    /// Stop the engines and the preparer and wait for them. Pending units
    /// are not written.
    pub fn close(&self) {
        self.lock().shutdown = true;
        self.inner.work.notify_all();
        self.inner.prep.notify_all();
        self.wake();
        let threads: Vec<_> = self
            .inner
            .threads
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
            .collect();
        let me = std::thread::current().id();
        for h in threads {
            if h.thread().id() != me {
                let _ = h.join();
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.lock().shutdown
    }

    fn spawn_preparer(&self) {
        self.spawn("journal-prepare", |j| j.run_preparer());
    }

    fn run_preparer(&self) {
        loop {
            let (seq, recycle, zero) = {
                let mut st = self.lock();
                loop {
                    if st.shutdown {
                        return;
                    }
                    if st.ready.len() < self.inner.cfg.prepared {
                        break;
                    }
                    // Idle and full: finish pre-zeroing a segment made without.
                    if !self.is_hot(&st)
                        && let Some(i) = st
                            .ready
                            .iter()
                            .position(|f| !f.zeroed.load(std::sync::atomic::Ordering::Relaxed))
                        && let Some(f) = st.ready.remove(i)
                    {
                        drop(st);
                        // Out of `ready` while it is written, so a rollover
                        // cannot take a segment being zeroed.
                        let res = self.inner.segs.zero_rest(&f);
                        if let Err(e) = &res {
                            warn!(seq = f.seq, error = %e, "could not pre-zero a segment");
                            std::thread::sleep(Duration::from_secs(1));
                        }
                        st = self.lock();
                        if f.seq < st.cur.seq {
                            let seq = f.seq;
                            drop(f);
                            match self.inner.segs.retire(seq) {
                                Ok(path) => st.free.push(Retired { path, held: None }),
                                Err(e) => {
                                    warn!(seq, error = %e, "could not retire a stale segment")
                                }
                            }
                        } else {
                            let at = st.ready.partition_point(|r| r.seq < f.seq);
                            st.ready.insert(at, f);
                        }
                        continue;
                    }
                    // Wake now and then to notice that the journal went idle.
                    st = self
                        .inner
                        .prep
                        .wait_timeout(
                            st,
                            self.inner.cfg.hot_segment.max(Duration::from_millis(100)),
                        )
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
                st.max_seq += 1;
                let zero = !self.is_hot(&st);
                (st.max_seq, take_recyclable(&mut st.free), zero)
            };
            match self.inner.segs.prepare(seq, recycle.as_deref(), zero) {
                Ok(f) => {
                    let mut st = self.lock();
                    if f.seq < st.cur.seq {
                        // A rollover prepared a later segment inline while
                        // this one was being made: writing here now would put
                        // newer records under an older number, and recovery
                        // replays by number. Recycle it instead.
                        let seq = f.seq;
                        drop(f);
                        match self.inner.segs.retire(seq) {
                            Ok(path) => st.free.push(Retired { path, held: None }),
                            Err(e) => warn!(seq, error = %e, "could not retire a stale segment"),
                        }
                    } else {
                        let at = st.ready.partition_point(|r| r.seq < f.seq);
                        st.ready.insert(at, f);
                    }
                }
                Err(e) => {
                    warn!(seq, error = %e, "journal: preparing a segment failed; retrying");
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
}

/// A retired segment no reader holds any more, if there is one.
fn take_recyclable(free: &mut Vec<Retired>) -> Option<PathBuf> {
    let i = free
        .iter()
        .position(|r| r.held.as_ref().is_none_or(|f| Arc::strong_count(f) == 1))?;
    Some(free.swap_remove(i).path)
}

fn read_fully(f: &std::fs::File, buf: &mut [u8]) -> io::Result<usize> {
    let mut at = 0usize;
    while at < buf.len() {
        match f.read_at(&mut buf[at..], at as u64) {
            Ok(0) => break,
            Ok(n) => at += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(at)
}

/// The buffered tier: write everything pending, `fdatasync` each segment
/// touched, report, repeat.
fn run_buffered(j: Journal) {
    while let Some(units) = j.take_blocking(usize::MAX) {
        let res = (|| -> io::Result<()> {
            for u in &units {
                u.seg.write.write_all_at(&u.buf, u.off)?;
            }
            let mut synced = BTreeSet::new();
            for u in &units {
                if synced.insert(u.seg.seq) {
                    u.seg.sync()?;
                }
            }
            Ok(())
        })();
        j.complete(units, res);
    }
}

/// `pwritev2(RWF_DSYNC)` of a whole buffer at `off`: one durable write (FUA
/// on a device that has it, write + flush otherwise).
pub fn pwrite_dsync(f: &std::fs::File, buf: &[u8], mut off: u64) -> io::Result<()> {
    use rustix::io::{ReadWriteFlags, pwritev2};
    let mut done = 0usize;
    while done < buf.len() {
        match pwritev2(
            f,
            &[io::IoSlice::new(&buf[done..])],
            off,
            ReadWriteFlags::DSYNC,
        ) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "pwritev2 wrote nothing",
                ));
            }
            Ok(n) => {
                done += n;
                off += n as u64;
            }
            Err(e) if e == rustix::io::Errno::INTR => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The pwritev2 tier: each thread takes one unit and writes it durably.
fn run_pwritev2(j: Journal) {
    while let Some(units) = j.take_blocking(1) {
        let res = units
            .iter()
            .try_for_each(|u| pwrite_dsync(&u.seg.write, &u.buf, u.off));
        j.complete(units, res);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Id, Lsn};

    fn tl(n: u8) -> TimelineId {
        TimelineId::new(Id([n; 16]), Id([n; 16]))
    }

    fn append_hdr(t: TimelineId, lsn: u64, len: usize) -> RecordHeader {
        RecordHeader {
            kind: Kind::Append,
            tl: t,
            term: 1,
            lsn: Lsn(lsn),
            aux: 0,
            aux2: 0,
            len: len as u32,
        }
    }

    fn cfg(dir: &std::path::Path, tier: Tier) -> JournalConfig {
        JournalConfig {
            segment_size: 64 << 10,
            unit_capacity: 16 << 10,
            ..JournalConfig::new(dir, tier)
        }
    }

    fn replay(c: JournalConfig) -> (Journal, Vec<(u64, RecordHeader, u64)>) {
        let mut out = Vec::new();
        let j = Journal::open(c, |s, h, o| out.push((s, *h, o))).unwrap();
        (j, out)
    }

    #[tokio::test]
    async fn records_are_durable_readable_and_replayed() {
        for tier in [Tier::Buffered, Tier::Pwritev2 { depth: 3 }] {
            let d = tempfile::tempdir().unwrap();
            let (j, none) = replay(cfg(d.path(), tier));
            assert!(none.is_empty());
            j.start();
            let mut placed = Vec::new();
            for i in 0..200u64 {
                let data = vec![i as u8; 300 + i as usize];
                let p = j
                    .append(&append_hdr(tl(1), i * 1000, data.len()), &[&data[..]])
                    .unwrap();
                placed.push((p, data));
            }
            let last = placed.last().unwrap().0.unit;
            j.wait(last).await.unwrap();
            for (p, data) in &placed {
                assert_eq!(&j.read(p.seq, p.payload_off, data.len()).unwrap(), data);
            }
            assert!(j.live_segments().len() > 1, "it rolled over");
            j.close();
            let (_j2, got) = replay(cfg(d.path(), tier));
            assert_eq!(got.len(), 200);
            for (i, (seq, h, off)) in got.iter().enumerate() {
                assert_eq!(h.lsn, Lsn(i as u64 * 1000));
                assert_eq!((*seq, *off), (placed[i].0.seq, placed[i].0.payload_off));
            }
        }
    }

    #[tokio::test]
    async fn eight_byte_padding_does_not_hide_the_next_durable_unit() {
        for tier in [Tier::Buffered, Tier::Pwritev2 { depth: 3 }] {
            let d = tempfile::tempdir().unwrap();
            let config = cfg(d.path(), tier);
            let (j, _) = replay(config.clone());
            j.start();
            // Leave exactly eight padding bytes at the end of the first unit.
            let first = vec![1; BLOCK - RECORD_HEADER - 8];
            let a = j
                .append(&append_hdr(tl(1), 0, first.len()), &[&first])
                .unwrap();
            j.wait(a.unit).await.unwrap();
            let second = vec![2; 3000];
            let b = j
                .append(
                    &append_hdr(tl(1), first.len() as u64, second.len()),
                    &[&second],
                )
                .unwrap();
            assert_eq!(b.unit, a.unit + 1);
            j.wait(b.unit).await.unwrap();
            j.close();

            let (reopened, got) = replay(config);
            assert_eq!(got.len(), 2, "both acknowledged units must survive replay");
            assert_eq!(got[0].1.lsn, Lsn(0));
            assert_eq!(got[1].1.lsn, Lsn(first.len() as u64));
            assert_eq!(
                reopened.read(a.seq, a.payload_off, first.len()).unwrap(),
                first
            );
            assert_eq!(
                reopened.read(b.seq, b.payload_off, second.len()).unwrap(),
                second
            );
            reopened.close();
        }
    }

    #[tokio::test]
    async fn a_torn_unit_ends_the_replay_and_writing_resumes_after_it() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        j.start();
        let a = j
            .append(&append_hdr(tl(1), 0, 10), &[&[1u8; 10][..]])
            .unwrap();
        j.wait(a.unit).await.unwrap();
        let b = j
            .append(&append_hdr(tl(1), 10, 10), &[&[2u8; 10][..]])
            .unwrap();
        j.wait(b.unit).await.unwrap();
        j.close();
        // Tear the second record.
        let path = d.path().join(format!("{:016x}.seg", b.seq));
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0xff; 4], b.payload_off).unwrap();
        let (j2, got) = replay(cfg(d.path(), Tier::Buffered));
        assert_eq!(got.len(), 1);
        j2.start();
        let c = j2
            .append(&append_hdr(tl(1), 10, 5), &[&[3u8; 5][..]])
            .unwrap();
        j2.wait(c.unit).await.unwrap();
        j2.close();
        let (_j3, got) = replay(cfg(d.path(), Tier::Buffered));
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].1.lsn, Lsn(10));
        assert!(got[1].0 > got[0].0, "the new record is in a later segment");
    }

    #[tokio::test]
    async fn an_emptied_head_never_lets_stale_later_units_replay() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        j.start();
        let a = j
            .append(&append_hdr(tl(1), 0, 10), &[&[1u8; 10][..]])
            .unwrap();
        j.wait(a.unit).await.unwrap();
        let b = j
            .append(&append_hdr(tl(1), 10, 10), &[&[2u8; 10][..]])
            .unwrap();
        j.wait(b.unit).await.unwrap();
        j.close();
        assert_eq!(a.seq, b.seq);
        // Zero the first unit only: the second is still on disk behind it.
        let path = d.path().join(format!("{:016x}.seg", a.seq));
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.write_all_at(&[0u8; BLOCK], SEGMENT_HEADER as u64)
            .unwrap();
        let (j2, got) = replay(cfg(d.path(), Tier::Buffered));
        assert!(got.is_empty());
        j2.start();
        // Write past where the stale unit sits, with different LSNs.
        let mut last = 0;
        for i in 0..4u64 {
            last = j2
                .append(&append_hdr(tl(1), 100 + i, 10), &[&[3u8; 10][..]])
                .unwrap()
                .unit;
        }
        j2.wait(last).await.unwrap();
        j2.close();
        let (_j3, got) = replay(cfg(d.path(), Tier::Buffered));
        assert!(got.iter().all(|(_, h, _)| h.lsn.0 >= 100), "{got:?}");
    }

    #[test]
    fn retired_numbers_are_never_reused_after_a_crash() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        let made = j.live_segments();
        j.close();
        drop(j);
        // A crash after `open` retired every empty segment but before it
        // prepared new ones: only `.free` files are left.
        let mut top = 0;
        for e in std::fs::read_dir(d.path()).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "seg") {
                let seq =
                    u64::from_str_radix(p.file_stem().unwrap().to_str().unwrap(), 16).unwrap();
                top = top.max(seq);
                std::fs::rename(&p, p.with_extension("free")).unwrap();
            }
        }
        assert!(top >= made.into_iter().max().unwrap());
        let (j2, got) = replay(cfg(d.path(), Tier::Buffered));
        assert!(got.is_empty());
        assert!(
            j2.live_segments().iter().all(|s| *s > top),
            "{:?} reuses a number up to {top}",
            j2.live_segments()
        );
        j2.close();
    }

    #[tokio::test]
    async fn freed_segments_are_recycled_and_their_records_never_replay() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        j.start();
        let mut last = 0;
        for i in 0..60u64 {
            let data = vec![7u8; 4000];
            last = j
                .append(&append_hdr(tl(2), i * 4000, 4000), &[&data[..]])
                .unwrap()
                .unit;
        }
        j.wait(last).await.unwrap();
        let before = j.live_segments();
        assert!(before.len() >= 3);
        // Everything below 100 000 may go.
        let freed = j.free(|_, end| end <= 100_000);
        assert!(!freed.is_empty());
        assert_eq!(freed[0], before[0]);
        // Keep writing until the recycled files come back into use.
        for i in 60..200u64 {
            let data = vec![8u8; 4000];
            last = j
                .append(&append_hdr(tl(2), i * 4000, 4000), &[&data[..]])
                .unwrap()
                .unit;
            let _ = j.free(|_, end| end <= i * 4000 - 40_000);
        }
        j.wait(last).await.unwrap();
        j.close();
        let (_j2, got) = replay(cfg(d.path(), Tier::Buffered));
        let lsns: Vec<u64> = got.iter().map(|(_, h, _)| h.lsn.0).collect();
        assert!(
            lsns.windows(2).all(|w| w[0] < w[1]),
            "log order, no stale records"
        );
        assert_eq!(*lsns.last().unwrap(), 199 * 4000);
    }

    #[test]
    fn oversized_records_and_bad_lengths_are_refused() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        let big = vec![0u8; 20 << 10];
        assert!(
            j.append(&append_hdr(tl(1), 0, big.len()), &[&big[..]])
                .is_err()
        );
        assert!(j.append(&append_hdr(tl(1), 0, 3), &[&b"ab"[..]]).is_err());
        j.close();
    }

    #[test]
    fn units_are_block_aligned_and_ids_follow_the_order() {
        let d = tempfile::tempdir().unwrap();
        let (j, _) = replay(cfg(d.path(), Tier::Buffered));
        let a = j
            .append(&append_hdr(tl(1), 0, 10), &[&[1u8; 10][..]])
            .unwrap();
        let u1 = j.try_take().unwrap();
        let b = j
            .append(&append_hdr(tl(1), 10, 10), &[&[1u8; 10][..]])
            .unwrap();
        let u2 = j.try_take().unwrap();
        assert_eq!((a.unit, b.unit), (1, 2));
        assert_eq!((u1.id, u2.id), (1, 2));
        assert_eq!(u1.buf.len(), BLOCK);
        assert_eq!(u2.off, u1.off + BLOCK as u64);
        // Out-of-order completion: nothing is durable until 1 is.
        j.complete(vec![u2], Ok(()));
        assert_eq!(j.durable().unit, 0);
        j.complete(vec![u1], Ok(()));
        assert_eq!(j.durable().unit, 2);
        j.close();
    }

    #[tokio::test]
    async fn fast_ingest_skips_pre_zeroing_and_idle_catches_up() {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path(), Tier::Buffered);
        c.hot_segment = Duration::from_secs(5);
        let (j, _) = replay(c);
        j.start();
        // Fill several segments at once: each rollover finds the journal hot.
        let data = vec![5u8; 4000];
        let mut last = 0;
        for i in 0..40u64 {
            last = j
                .append(&append_hdr(tl(1), i * 4000, 4000), &[&data[..]])
                .unwrap()
                .unit;
        }
        j.wait(last).await.unwrap();
        // The preparer makes the replacements after the rollovers that took
        // the pre-zeroed ones, so `ready` can briefly hold none of them: poll
        // (well inside the hot window) rather than look once.
        let mut saw_unzeroed = false;
        for _ in 0..40 {
            if j.ready_zeroed().contains(&false) {
                saw_unzeroed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(saw_unzeroed, "segments made while hot are not pre-zeroed");
        // Idle: the preparer zeroes them.
        for _ in 0..400 {
            if j.ready_zeroed().iter().all(|z| *z) {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(j.ready_zeroed().iter().all(|z| *z), "zeroed once idle");
        j.close();
        // Everything written is still there.
        let (_j2, got) = replay(cfg(d.path(), Tier::Buffered));
        assert_eq!(got.len(), 40);
    }
}
