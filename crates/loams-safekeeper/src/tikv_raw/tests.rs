//! The raw store's rules, and the fence's safety: a deposed writer's WAL is
//! never acknowledged beyond what the new proposer adopts, and never read.

use std::sync::atomic::{AtomicUsize, Ordering};

use proptest::prelude::*;
use tokio::sync::oneshot;

use super::*;
use crate::types::{Id, TermHistory, TermLsn};

fn tl() -> TimelineId {
    TimelineId::new(Id([1; 16]), Id([2; 16]))
}

fn elected(term: Term, start: u64, history: &[(Term, u64)]) -> ProposerElected {
    ProposerElected {
        generation: 0,
        term,
        start_streaming_at: Lsn(start),
        term_history: TermHistory(
            history
                .iter()
                .map(|&(t, l)| TermLsn {
                    term: t,
                    lsn: Lsn(l),
                })
                .collect(),
        ),
    }
}

fn batch(term: Term, begin: u64, data: &[u8], commit: u64) -> AppendBatch {
    AppendBatch {
        term,
        begin_lsn: Lsn(begin),
        wal: vec![Bytes::copy_from_slice(data)],
        commit_lsn: Lsn(commit),
        truncate_lsn: Lsn::INVALID,
    }
}

async fn read_all<K: RawKv>(s: &RawWalStore<K>, from: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = Lsn(from);
    loop {
        let got = s.read(&tl(), at, 1 << 20).await.unwrap();
        if got.is_empty() {
            return out;
        }
        for (lsn, b) in got {
            assert_eq!(lsn, at);
            at = Lsn(at.0 + b.len() as u64);
            out.extend_from_slice(&b);
        }
    }
}

fn store(kv: &Arc<MemRawKv>) -> RawWalStore<MemRawKv> {
    RawWalStore::new(kv.clone(), b"root/".to_vec(), 8)
}

async fn elect(s: &RawWalStore<impl RawKv>, term: Term, start: u64, history: &[(Term, u64)]) {
    assert!(s.vote(&tl(), term).await.unwrap().0);
    s.elected(&tl(), &elected(term, start, history))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn append_read_and_restart() {
    let kv = Arc::new(MemRawKv::new());
    let s = store(&kv);
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&s, 1, 100, &[(1, 100)]).await;
    let st = s
        .append(&tl(), &batch(1, 100, b"abcde", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(105));
    s.append(&tl(), &batch(1, 105, b"fgh", 103))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, 100).await, b"abcdefgh");
    assert_eq!(read_all(&s, 102).await, b"cdefgh");

    // Another instance (or a restart) finds the end by scanning.
    let other = store(&kv);
    let st = other.load(&tl()).await.unwrap().unwrap();
    assert_eq!(st.flush_lsn, Lsn(108));
    assert_eq!(st.term, 1);
    // The heartbeat commit LSN is persisted by record_commit_lsn only.
    s.record_commit_lsn(&tl(), 1, Lsn(107))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        other.load(&tl()).await.unwrap().unwrap().commit_lsn,
        Lsn(107)
    );
    // So is the proposer's truncate LSN (the peer horizon).
    let mut b = batch(1, 108, b"i", 0);
    b.truncate_lsn = Lsn(104);
    s.append(&tl(), &b).await.unwrap().unwrap();
    s.record_commit_lsn(&tl(), 1, Lsn(107))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        other.load(&tl()).await.unwrap().unwrap().peer_horizon_lsn,
        Lsn(104)
    );
}

#[tokio::test]
async fn retried_and_overlapping_appends_store_the_stream() {
    let kv = Arc::new(MemRawKv::new());
    let s = store(&kv);
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&s, 1, 0, &[(1, 0)]).await;
    s.append(&tl(), &batch(1, 0, b"abc", 0))
        .await
        .unwrap()
        .unwrap();
    s.append(&tl(), &batch(1, 0, b"abc", 0))
        .await
        .unwrap()
        .unwrap();
    let st = s
        .append(&tl(), &batch(1, 1, b"bcdef", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(6));
    assert_eq!(read_all(&s, 0).await, b"abcdef");
    // A gap is never acknowledged.
    let st = s
        .append(&tl(), &batch(1, 9, b"z", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(st.flush_lsn, Lsn(6));
    assert_eq!(read_all(&s, 0).await, b"abcdef");
}

#[tokio::test]
async fn large_chunks_are_split_and_read_back() {
    let kv = Arc::new(MemRawKv::new());
    let s = store(&kv);
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&s, 1, 0, &[(1, 0)]).await;
    let data: Vec<u8> = (0..(MAX_CHUNK * 3 + 17)).map(|i| (i % 251) as u8).collect();
    s.append(&tl(), &batch(1, 0, &data, 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, 0).await, data);
    assert_eq!(
        read_all(&s, MAX_CHUNK as u64 + 5).await,
        &data[MAX_CHUNK + 5..]
    );
    // Many small chunks: more than a scan page inside one MAX_CHUNK window.
    let mut at = data.len() as u64;
    let mut all = data.clone();
    for i in 0..200u32 {
        let d = i.to_be_bytes();
        s.append(&tl(), &batch(1, at, &d, 0))
            .await
            .unwrap()
            .unwrap();
        at += 4;
        all.extend_from_slice(&d);
    }
    assert_eq!(read_all(&s, 0).await, all);
    assert_eq!(read_all(&s, at - 7).await, &all[all.len() - 7..]);
    assert_eq!(
        store(&kv).load(&tl()).await.unwrap().unwrap().flush_lsn,
        Lsn(at)
    );
}

#[tokio::test]
async fn a_new_term_truncates_by_segments_and_ignores_stale_keys() {
    let kv = Arc::new(MemRawKv::new());
    let s = store(&kv);
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&s, 1, 0, &[(1, 0)]).await;
    s.append(&tl(), &batch(1, 0, b"abcdef", 2))
        .await
        .unwrap()
        .unwrap();
    // Term 2 keeps term 1's WAL up to 4 and writes its own from there.
    let (given, st) = s.vote(&tl(), 2).await.unwrap();
    assert!(given);
    assert_eq!(st.flush_lsn, Lsn(6));
    s.elected(&tl(), &elected(2, 4, &[(1, 0), (2, 4)]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&s, 0).await, b"abcd");
    s.append(&tl(), &batch(2, 4, b"XYZW", 0))
        .await
        .unwrap()
        .unwrap();
    // Term 1's key [0, 6) still holds "ef" at 4..6; readers take term 2's.
    assert_eq!(read_all(&s, 0).await, b"abcdXYZW");
    assert_eq!(read_all(&s, 5).await, b"YZW");
    let st = store(&kv).load(&tl()).await.unwrap().unwrap();
    assert_eq!(st.flush_lsn, Lsn(8));
    assert_eq!(st.last_log_term(), 2);
}

#[tokio::test]
async fn a_deposed_writer_learns_on_its_next_append() {
    let kv = Arc::new(MemRawKv::new());
    let old = store(&kv);
    old.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&old, 1, 0, &[(1, 0)]).await;
    old.append(&tl(), &batch(1, 0, b"abc", 0))
        .await
        .unwrap()
        .unwrap();
    // A proposer on another instance takes over.
    let new = store(&kv);
    let (given, st) = new.vote(&tl(), 2).await.unwrap();
    assert!(given);
    assert_eq!(st.flush_lsn, Lsn(3));
    // The old writer's put lands, but its fence check sees term 2.
    assert_eq!(
        old.append(&tl(), &batch(1, 3, b"def", 3)).await.unwrap(),
        Err(Deposed { current: 2 })
    );
    new.elected(&tl(), &elected(2, 3, &[(1, 0), (2, 3)]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&new, 0).await, b"abc");
    // And on this instance it is refused without a write.
    assert_eq!(
        new.append(&tl(), &batch(1, 3, b"def", 3)).await.unwrap(),
        Err(Deposed { current: 2 })
    );
    assert_eq!(
        new.record_commit_lsn(&tl(), 1, Lsn(3)).await.unwrap(),
        Err(Deposed { current: 2 })
    );
}

/// A [`RawKv`] that holds WAL puts until the test lets them land.
#[derive(Debug, Default)]
struct Gated {
    inner: MemRawKv,
    hold: std::sync::atomic::AtomicBool,
    held: Mutex<Vec<oneshot::Sender<()>>>,
}

impl Gated {
    fn release_all(&self) {
        for tx in self.held.lock().unwrap().drain(..) {
            let _ = tx.send(());
        }
    }
}

#[async_trait]
impl RawKv for Gated {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        self.inner.get(key).await
    }
    async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        if self.hold.load(Ordering::SeqCst) {
            let (tx, rx) = oneshot::channel();
            self.held.lock().unwrap().push(tx);
            let _ = rx.await;
        }
        self.inner.batch_put(pairs).await
    }
    async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, bool), Error> {
        self.inner.compare_and_swap(key, expected, new).await
    }
    async fn scan(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        self.inner.scan(from, to, limit).await
    }
    async fn delete_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
        self.inner.delete_range(from, to).await
    }
}

#[tokio::test]
async fn a_put_landing_after_the_vote_is_never_acknowledged_nor_read() {
    let kv = Arc::new(Gated::default());
    let old = Arc::new(RawWalStore::new(kv.clone(), b"r/".to_vec(), 8));
    old.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&*old, 1, 0, &[(1, 0)]).await;
    old.append(&tl(), &batch(1, 0, b"abc", 0))
        .await
        .unwrap()
        .unwrap();

    // The old writer's next put is in flight when the new proposer votes.
    kv.hold.store(true, Ordering::SeqCst);
    let o = old.clone();
    let late = tokio::spawn(async move { o.append(&tl(), &batch(1, 3, b"def", 3)).await });
    while kv.held.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    let new = RawWalStore::new(kv.clone(), b"r/".to_vec(), 8);
    let (given, st) = new.vote(&tl(), 2).await.unwrap();
    assert!(given);
    assert_eq!(st.flush_lsn, Lsn(3), "the vote sees only what landed");
    new.elected(&tl(), &elected(2, 3, &[(1, 0), (2, 3)]))
        .await
        .unwrap()
        .unwrap();

    // Now the stale put lands: it is refused, and invisible.
    kv.hold.store(false, Ordering::SeqCst);
    kv.release_all();
    assert_eq!(late.await.unwrap().unwrap(), Err(Deposed { current: 2 }));
    assert_eq!(read_all(&new, 0).await, b"abc");
    new.append(&tl(), &batch(2, 3, b"XY", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_all(&new, 0).await, b"abcXY");
    assert_eq!(read_all(&*old, 0).await, b"abcXY");
    // Trim deletes nothing it must keep; the stale key stays harmless.
    assert_eq!(new.load(&tl()).await.unwrap().unwrap().flush_lsn, Lsn(5));
}

#[tokio::test]
async fn out_of_order_completions_acknowledge_only_the_contiguous_end() {
    let kv = Arc::new(Gated::default());
    let s = Arc::new(RawWalStore::new(kv.clone(), b"r/".to_vec(), 8));
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&*s, 1, 0, &[(1, 0)]).await;
    // The first append is held; the second lands first.
    kv.hold.store(true, Ordering::SeqCst);
    let s1 = s.clone();
    let first = tokio::spawn(async move { s1.append(&tl(), &batch(1, 0, b"abc", 0)).await });
    while kv.held.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    kv.hold.store(false, Ordering::SeqCst);
    let second = s
        .append(&tl(), &batch(1, 3, b"def", 0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second.flush_lsn,
        Lsn(0),
        "a hole below: nothing acknowledged"
    );
    kv.release_all();
    let first = first.await.unwrap().unwrap().unwrap();
    assert_eq!(first.flush_lsn, Lsn(6));
    assert_eq!(read_all(&*s, 0).await, b"abcdef");
}

#[tokio::test]
async fn trim_drops_old_segments_and_stale_terms() {
    let kv = Arc::new(MemRawKv::new());
    let s = store(&kv);
    s.create(&tl(), ServerInfo::default(), Lsn::INVALID)
        .await
        .unwrap();
    elect(&s, 1, 0, &[(1, 0)]).await;
    s.append(&tl(), &batch(1, 0, b"abcdef", 0))
        .await
        .unwrap()
        .unwrap();
    elect(&s, 3, 6, &[(1, 0), (3, 6)]).await;
    let big = vec![7u8; MAX_CHUNK * 2];
    s.append(&tl(), &batch(3, 6, &big, 0))
        .await
        .unwrap()
        .unwrap();
    // A stray key of term 2 (a deposed writer that was never in the history).
    kv.batch_put(vec![(s.wal_key(&tl(), 2, 6), b"zz".to_vec())])
        .await
        .unwrap();
    let end = 6 + big.len() as u64;
    s.record_commit_lsn(&tl(), 3, Lsn(end))
        .await
        .unwrap()
        .unwrap();
    s.record_backup_lsn(&tl(), Lsn(end)).await.unwrap();
    s.record_remote_consistent_lsn(&tl(), Lsn(end))
        .await
        .unwrap();
    let bound = s.trim(&tl(), Lsn(end - 10)).await.unwrap();
    assert_eq!(bound, Lsn(end - 10));
    assert!(s.read(&tl(), Lsn(3), 10).await.is_err());
    assert_eq!(read_all(&s, end - 10).await, vec![7u8; 10]);
    let wal_keys = kv
        .keys()
        .into_iter()
        .filter(|k| k.get(37) == Some(&b'W'))
        .count();
    // Term 1 and the stray term 2 are gone; term 3 keeps the chunk(s) near
    // the bound.
    assert!(wal_keys <= 2, "{wal_keys} WAL keys left");
    assert_eq!(
        store(&kv).load(&tl()).await.unwrap().unwrap().flush_lsn,
        Lsn(end)
    );
}

/// Yields a scripted number of times around every operation, so a
/// single-threaded runtime interleaves concurrent calls differently per case.
#[derive(Debug)]
struct Jitter {
    inner: MemRawKv,
    script: Vec<u8>,
    at: AtomicUsize,
}

impl Jitter {
    async fn pause(&self) {
        let i = self.at.fetch_add(1, Ordering::Relaxed);
        let n = self
            .script
            .get(i % self.script.len().max(1))
            .copied()
            .unwrap_or(0);
        for _ in 0..n % 8 {
            tokio::task::yield_now().await;
        }
    }
}

#[async_trait]
impl RawKv for Jitter {
    async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, Error> {
        self.pause().await;
        let v = self.inner.get(key).await;
        self.pause().await;
        v
    }
    async fn batch_put(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), Error> {
        self.pause().await;
        self.inner.batch_put(pairs).await?;
        self.pause().await;
        Ok(())
    }
    async fn compare_and_swap(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    ) -> Result<(Option<Vec<u8>>, bool), Error> {
        self.pause().await;
        let v = self.inner.compare_and_swap(key, expected, new).await;
        self.pause().await;
        v
    }
    async fn scan(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        self.pause().await;
        let v = self.inner.scan(from, to, limit).await;
        self.pause().await;
        v
    }
    async fn delete_range(&self, from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
        self.inner.delete_range(from, to).await
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// An old writer streams pipelined appends while a proposer on another
    /// instance votes, is elected at the WAL end its vote saw, and writes its
    /// own WAL. Under every interleaving:
    /// - every flush LSN acknowledged to the old writer is at or below the
    ///   end the vote reported (so the new history contains it), and
    /// - readers see exactly the old WAL up to that end, then the new WAL.
    #[test]
    fn a_deposed_writer_is_never_acknowledged_past_the_new_history(
        script in proptest::collection::vec(any::<u8>(), 1..64),
        cuts in proptest::collection::vec(1usize..9, 1..12),
        vote_after in 0usize..12,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let kv = Arc::new(Jitter { inner: MemRawKv::new(), script, at: AtomicUsize::new(0) });
            let old = Arc::new(RawWalStore::new(kv.clone(), b"r/".to_vec(), 8));
            old.create(&tl(), ServerInfo::default(), Lsn::INVALID).await.unwrap();
            elect(&*old, 1, 0, &[(1, 0)]).await;

            let old_data: Vec<u8> = (0..cuts.iter().sum::<usize>()).map(|i| i as u8).collect();
            let mut tasks = Vec::new();
            let mut at = 0usize;
            for (i, &n) in cuts.iter().enumerate() {
                let o = old.clone();
                let piece = old_data[at..at + n].to_vec();
                let begin = at as u64;
                tasks.push(tokio::spawn(async move {
                    o.append(&tl(), &batch(1, begin, &piece, 0)).await
                }));
                at += n;
                if i == vote_after {
                    tokio::task::yield_now().await;
                }
            }
            let new = RawWalStore::new(kv.clone(), b"r/".to_vec(), 8);
            let (given, st) = new.vote(&tl(), 2).await.unwrap();
            assert!(given);
            let x = st.flush_lsn.0;
            new.elected(&tl(), &elected(2, x, &[(1, 0), (2, x)])).await.unwrap().unwrap();

            let mut acked = 0u64;
            for t in tasks {
                match t.await.unwrap() {
                    Ok(Ok(st)) => acked = acked.max(st.flush_lsn.0),
                    Ok(Err(Deposed { current })) => assert_eq!(current, 2),
                    Err(e) => panic!("{e}"),
                }
            }
            assert!(acked <= x, "acknowledged {acked} past the adopted end {x}");

            let new_data = b"NEWTERM";
            new.append(&tl(), &batch(2, x, new_data, 0)).await.unwrap().unwrap();
            let mut want = old_data[..x as usize].to_vec();
            want.extend_from_slice(new_data);
            assert_eq!(read_all(&new, 0).await, want);
            let st = RawWalStore::new(kv.clone(), b"r/".to_vec(), 8).load(&tl()).await.unwrap().unwrap();
            assert_eq!(st.flush_lsn.0, x + new_data.len() as u64);
        });
    }
}
