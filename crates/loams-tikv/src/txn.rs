//! [`Txn`] and [`Snap`]: reads and writes under a handle's root, with paged
//! scans and batch reads, the 2 MiB value bound and the GC safe window (R1
//! plan Task 2 semantics 5a, 5b; rows R7, R10).

use std::time::{Duration, Instant};

use tikv_client::{BoundRange, Key, KvPair, Snapshot, Timestamp, Transaction};

use crate::classify::{Class, classify};
use crate::runner::TxnError;
use crate::{Tikv, codec};

/// A key and its value, the key relative to the handle's root.
pub type Pair = (Vec<u8>, Vec<u8>);

/// The largest value [`Txn::put`] accepts (2 MiB): a one-key page, with its
/// key and framing, then always fits gRPC's 4 MiB default decoding limit.
pub const MAX_VALUE_BYTES: usize = 2 * 1024 * 1024;

/// Keys per scan or batch-get request; halved on gRPC `OutOfRange` down to 1
/// and doubled back after a success (row R10).
pub const PAGE_KEYS: u32 = 256;

/// The GC safe-window refusal text.
pub(crate) const BELOW_SAFE_POINT: &str = "read below the GC safe point";

/// A transaction of [`Tikv::run`]. Every key is relative to the handle's root.
pub struct Txn {
    pub(crate) inner: Transaction,
    tikv: Tikv,
    attempt: u32,
    refuse_reads_after: Instant,
    pub(crate) tso_closed: bool,
}

impl std::fmt::Debug for Txn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("start_ts", &self.inner.start_timestamp())
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// A read-only view at one timestamp, from [`Tikv::snapshot`]. Every key is
/// relative to the handle's root. Its reads are refused once its timestamp
/// leaves the GC safe window, unless a [`GcBarrier`](crate::GcBarrier) of the
/// handle covers it.
pub struct Snap {
    inner: Snapshot,
    tikv: Tikv,
    at: Timestamp,
    refuse_reads_after: Instant,
}

impl std::fmt::Debug for Snap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snap")
            .field("at", &self.at)
            .finish_non_exhaustive()
    }
}

impl Txn {
    pub(crate) fn new(inner: Transaction, tikv: Tikv, attempt: u32) -> Self {
        let refuse_reads_after = Instant::now() + tikv.safe_window();
        Txn {
            inner,
            tikv,
            attempt,
            refuse_reads_after,
            tso_closed: false,
        }
    }

    /// The transaction's start timestamp: every read sees the data committed
    /// before it.
    pub fn start_ts(&self) -> Timestamp {
        self.inner.start_timestamp()
    }

    /// Which attempt of [`Tikv::run`] this is, from 1.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reads `key` at the start timestamp.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        self.check_window()?;
        let full = self.tikv.key(key);
        let res = self.inner.get(full).await;
        self.map(res)
    }

    /// Reads `key` and locks it: in a pessimistic transaction at once (a
    /// second holder waits or restarts), in an optimistic one at commit.
    pub async fn get_for_update(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        self.check_window()?;
        let full = self.tikv.key(key);
        let res = self.inner.get_for_update(full).await;
        self.map(res)
    }

    /// Reads `keys` and locks them all, in one request per region: in a
    /// pessimistic transaction at once, returning the latest committed values
    /// (a second holder waits or restarts); in an optimistic one at commit,
    /// returning the values at the start timestamp. Absent keys are left out;
    /// the result is sorted by key. Not paged: meant for small records, such
    /// as partition heads (design §20 §11.4: batching the locks matters).
    pub async fn batch_get_for_update<K: AsRef<[u8]>>(
        &mut self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let keys: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|k| self.tikv.key(k.as_ref()))
            .collect();
        let res = self.inner.batch_get_for_update(keys).await;
        let pairs = self.map(res)?;
        let mut out: Vec<Pair> = pairs
            .into_iter()
            .map(|pair| {
                let (key, value): (Key, Vec<u8>) = pair.into();
                (self.tikv.relative(key), value)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Reads `keys` at the start timestamp, in pages; absent keys are left
    /// out. The result is sorted by key.
    pub async fn batch_get<K: AsRef<[u8]>>(
        &mut self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let keys = keys
            .into_iter()
            .map(|k| self.tikv.key(k.as_ref()))
            .collect();
        let tikv = self.tikv.clone();
        let res = paged_batch_get(&mut self.inner, &tikv, keys).await;
        self.note(res)
    }

    /// Up to `limit` pairs with keys in `start..end` in key order (`end`
    /// `None`: to the end of the root), in pages.
    pub async fn scan(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let (lo, hi) = self.tikv.full_range(start, end);
        let tikv = self.tikv.clone();
        let res = paged_scan(&mut self.inner, &tikv, lo, hi, limit, false).await;
        self.note(res)
    }

    /// Like [`scan`](Self::scan), in descending key order.
    pub async fn scan_reverse(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let (lo, hi) = self.tikv.full_range(start, end);
        let tikv = self.tikv.clone();
        let res = paged_scan(&mut self.inner, &tikv, lo, hi, limit, true).await;
        self.note(res)
    }

    /// Writes `value` at `key`. Refuses a value over 2 MiB
    /// ([`MAX_VALUE_BYTES`]) with `TxnError::Fatal("value over 2 MiB")`.
    pub async fn put(&mut self, key: &[u8], value: impl Into<Vec<u8>>) -> Result<(), TxnError> {
        let value = value.into();
        if value.len() > MAX_VALUE_BYTES {
            return Err(TxnError::Fatal("value over 2 MiB".to_string()));
        }
        let full = self.tikv.key(key);
        let res = self.inner.put(full, value).await;
        self.map(res)
    }

    /// Writes `value` at `key` if the key is absent; the commit fails with
    /// [`TxnError::AlreadyExists`] if it is not. The 2 MiB bound applies.
    pub async fn insert(&mut self, key: &[u8], value: impl Into<Vec<u8>>) -> Result<(), TxnError> {
        let value = value.into();
        if value.len() > MAX_VALUE_BYTES {
            return Err(TxnError::Fatal("value over 2 MiB".to_string()));
        }
        let full = self.tikv.key(key);
        let res = self.inner.insert(full, value).await;
        self.map(res)
    }

    /// Deletes `key`.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), TxnError> {
        let full = self.tikv.key(key);
        let res = self.inner.delete(full).await;
        self.map(res)
    }

    /// Locks `keys` without writing them: a concurrent write to one of them
    /// is a write-write conflict (design §20 §5.2's read promotion).
    pub async fn lock_keys<K: AsRef<[u8]>>(
        &mut self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<(), TxnError> {
        let keys: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|k| self.tikv.key(k.as_ref()))
            .collect();
        let res = self.inner.lock_keys(keys).await;
        self.map(res)
    }

    fn check_window(&self) -> Result<(), TxnError> {
        if Instant::now() > self.refuse_reads_after {
            return Err(TxnError::Fatal(BELOW_SAFE_POINT.to_string()));
        }
        Ok(())
    }

    fn map<T>(&mut self, res: tikv_client::Result<T>) -> Result<T, TxnError> {
        res.map_err(|e| {
            if classify(&e) == Class::TsoClosed {
                self.tso_closed = true;
            }
            self.tikv.txn_error(&e)
        })
    }

    fn note<T>(&mut self, res: Result<T, (TxnError, bool)>) -> Result<T, TxnError> {
        res.map_err(|(e, tso_closed)| {
            self.tso_closed |= tso_closed;
            e
        })
    }
}

impl Snap {
    pub(crate) fn new(inner: Snapshot, tikv: Tikv, at: Timestamp, window_left: Duration) -> Self {
        Snap {
            inner,
            tikv,
            at,
            refuse_reads_after: Instant::now() + window_left,
        }
    }

    /// The snapshot's timestamp.
    pub fn ts(&self) -> &Timestamp {
        &self.at
    }

    /// Reads `key`.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        self.check_window()?;
        let full = self.tikv.key(key);
        self.inner
            .get(full)
            .await
            .map_err(|e| self.tikv.txn_error(&e))
    }

    /// Reads `keys`, in pages; absent keys are left out. Sorted by key.
    pub async fn batch_get<K: AsRef<[u8]>>(
        &mut self,
        keys: impl IntoIterator<Item = K>,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let keys = keys
            .into_iter()
            .map(|k| self.tikv.key(k.as_ref()))
            .collect();
        paged_batch_get(&mut self.inner, &self.tikv, keys)
            .await
            .map_err(|(e, _)| e)
    }

    /// Up to `limit` pairs with keys in `start..end` in key order (`end`
    /// `None`: to the end of the root), in pages.
    pub async fn scan(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let (lo, hi) = self.tikv.full_range(start, end);
        paged_scan(&mut self.inner, &self.tikv, lo, hi, limit, false)
            .await
            .map_err(|(e, _)| e)
    }

    /// Like [`scan`](Self::scan), in descending key order.
    pub async fn scan_reverse(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Pair>, TxnError> {
        self.check_window()?;
        let (lo, hi) = self.tikv.full_range(start, end);
        paged_scan(&mut self.inner, &self.tikv, lo, hi, limit, true)
            .await
            .map_err(|(e, _)| e)
    }

    /// Past the window, reads go on only while a barrier of the handle
    /// covers the snapshot's timestamp.
    fn check_window(&self) -> Result<(), TxnError> {
        if Instant::now() > self.refuse_reads_after
            && !self
                .tikv
                .barriers
                .covers(tikv_client::TimestampExt::version(&self.at))
        {
            return Err(TxnError::Fatal(BELOW_SAFE_POINT.to_string()));
        }
        Ok(())
    }
}

impl Tikv {
    /// The full-key range of `start..end` under the root; `end` `None` is the
    /// end of the root (unbounded when the root has no successor).
    fn full_range(&self, start: &[u8], end: Option<&[u8]>) -> (Vec<u8>, Option<Vec<u8>>) {
        let lo = self.key(start);
        let hi = match end {
            Some(end) => Some(self.key(end)),
            None => {
                let succ = codec::tuple::successor(self.root());
                (!succ.is_empty()).then_some(succ)
            }
        };
        (lo, hi)
    }

    /// Strips the root from a key the client returned.
    fn relative(&self, key: Key) -> Vec<u8> {
        let mut key: Vec<u8> = key.into();
        key.drain(..self.root().len().min(key.len()));
        key
    }
}

/// The reads the pager needs, over a transaction or a snapshot.
trait RawRead {
    async fn raw_scan(
        &mut self,
        range: BoundRange,
        limit: u32,
        reverse: bool,
    ) -> tikv_client::Result<Vec<KvPair>>;
    async fn raw_batch_get(&mut self, keys: Vec<Vec<u8>>) -> tikv_client::Result<Vec<KvPair>>;
}

impl RawRead for Transaction {
    async fn raw_scan(
        &mut self,
        range: BoundRange,
        limit: u32,
        reverse: bool,
    ) -> tikv_client::Result<Vec<KvPair>> {
        if reverse {
            Ok(self.scan_reverse(range, limit).await?.collect())
        } else {
            Ok(self.scan(range, limit).await?.collect())
        }
    }

    async fn raw_batch_get(&mut self, keys: Vec<Vec<u8>>) -> tikv_client::Result<Vec<KvPair>> {
        Ok(self.batch_get(keys).await?.collect())
    }
}

impl RawRead for Snapshot {
    async fn raw_scan(
        &mut self,
        range: BoundRange,
        limit: u32,
        reverse: bool,
    ) -> tikv_client::Result<Vec<KvPair>> {
        if reverse {
            Ok(self.scan_reverse(range, limit).await?.collect())
        } else {
            Ok(self.scan(range, limit).await?.collect())
        }
    }

    async fn raw_batch_get(&mut self, keys: Vec<Vec<u8>>) -> tikv_client::Result<Vec<KvPair>> {
        Ok(self.batch_get(keys).await?.collect())
    }
}

/// Maps a read error; the flag says whether the TSO stream closed.
fn read_error(tikv: &Tikv, e: &tikv_client::Error) -> (TxnError, bool) {
    (tikv.txn_error(e), classify(e) == Class::TsoClosed)
}

fn one_key_too_large() -> (TxnError, bool) {
    (
        TxnError::Fatal("a single key's response exceeds the gRPC decoding limit".to_string()),
        false,
    )
}

/// A scan in pages of at most [`PAGE_KEYS`] keys (row R10).
async fn paged_scan(
    reader: &mut impl RawRead,
    tikv: &Tikv,
    mut lo: Vec<u8>,
    mut hi: Option<Vec<u8>>,
    limit: usize,
    reverse: bool,
) -> Result<Vec<Pair>, (TxnError, bool)> {
    let mut out = Vec::new();
    let mut page = PAGE_KEYS;
    while out.len() < limit {
        if hi.as_ref().is_some_and(|hi| lo >= *hi) {
            break;
        }
        let left = u32::try_from(limit - out.len()).unwrap_or(u32::MAX);
        let want = page.min(left);
        let range = BoundRange::from((lo.clone(), hi.clone()));
        match reader.raw_scan(range, want, reverse).await {
            Ok(pairs) => {
                let got = pairs.len();
                let mut last = None;
                for pair in pairs {
                    let (key, value): (Key, Vec<u8>) = pair.into();
                    let full: Vec<u8> = key.into();
                    last = Some(full.clone());
                    out.push((tikv.relative(full.into()), value));
                }
                let Some(last) = last else { break };
                if got < want as usize {
                    break;
                }
                if reverse {
                    hi = Some(last);
                } else {
                    lo = last;
                    lo.push(0);
                }
                page = page.saturating_mul(2).min(PAGE_KEYS);
            }
            Err(e) if classify(&e) == Class::TooLarge => {
                if want <= 1 {
                    return Err(one_key_too_large());
                }
                page = (want / 2).max(1);
                tikv.counters().page_halved();
            }
            Err(e) => return Err(read_error(tikv, &e)),
        }
    }
    out.truncate(limit);
    Ok(out)
}

/// A batch get in pages of at most [`PAGE_KEYS`] keys (row R10).
async fn paged_batch_get(
    reader: &mut impl RawRead,
    tikv: &Tikv,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Pair>, (TxnError, bool)> {
    let mut out = Vec::new();
    let mut page = PAGE_KEYS as usize;
    let mut at = 0;
    while at < keys.len() {
        let end = (at + page).min(keys.len());
        match reader.raw_batch_get(keys[at..end].to_vec()).await {
            Ok(pairs) => {
                for pair in pairs {
                    let (key, value): (Key, Vec<u8>) = pair.into();
                    out.push((tikv.relative(key), value));
                }
                at = end;
                page = (page * 2).min(PAGE_KEYS as usize);
            }
            Err(e) if classify(&e) == Class::TooLarge => {
                let asked = end - at;
                if asked <= 1 {
                    return Err(one_key_too_large());
                }
                page = (asked / 2).max(1);
                tikv.counters().page_halved();
            }
            Err(e) => return Err(read_error(tikv, &e)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}
