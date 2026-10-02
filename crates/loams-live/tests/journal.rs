//! R1 plan Task 9: the sharded, sequenced commit journal, its tailer and its
//! janitor (design §20 §5.3, D119). The key-layout and chunking tests run
//! without a cluster; the rest need TiKV and skip without `LOAMS_TEST_PD`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use buffa::Message;
use loams_live::journal::{self, MAX_CHUNK_BYTES, MAX_SHARDS};
use loams_live::{AppKeys, Janitor, Journal, LiveError, Tailer, pb};
use loams_tikv::testing::{self, TEST_LIVE};
use loams_tikv::{CommitMode, Tikv, Timestamp, TimestampExt, TxnError, TxnOptions};
use rand::SeedableRng;
use rand::rngs::StdRng;

// ---- helpers ----

fn lift<T>(r: Result<T, LiveError>) -> Result<Result<T, LiveError>, TxnError> {
    match r {
        Ok(v) => Ok(Ok(v)),
        Err(e) => e.into_txn().map(Err),
    }
}

fn opts() -> TxnOptions {
    let mut opts = TxnOptions::new("test.journal");
    opts.commit_mode = Some(CommitMode::TwoPc);
    // 32 writers on 16 heads conflict often; give them room.
    opts.max_attempts = 64;
    opts.deadline = Duration::from_secs(60);
    opts
}

fn write(i: usize, key_bytes: usize) -> pb::WriteRecord {
    pb::WriteRecord {
        table_id: 1,
        doc_id: (i as u128).to_be_bytes().to_vec(),
        kind: pb::WriteKind::WRITE_KIND_INSERT.into(),
        index_keys_added: vec![vec![0x03; key_bytes]],
        ..Default::default()
    }
}

fn entry(request_id: &str, writes: usize) -> pb::JournalEntry {
    pb::JournalEntry {
        writes: (0..writes).map(|i| write(i, 24)).collect(),
        function: "test:append".to_string(),
        request_id: request_id.to_string(),
        ..Default::default()
    }
}

async fn live() -> Option<(Tikv, Journal)> {
    let cluster = testing::cluster().await?;
    let tikv = cluster.connect(TEST_LIVE).await;
    let journal = Journal::new(AppKeys::dedicated(), 16).expect("16 shards");
    Some((tikv, journal))
}

/// One committed append: the shard, the sequence, the commit timestamp and
/// the attempts it took. `shard` `None` draws one per attempt.
async fn append(
    tikv: &Tikv,
    journal: &Journal,
    e: pb::JournalEntry,
    shard: Option<u16>,
) -> (u16, u64, Timestamp, u32) {
    let journal = journal.clone();
    let c = tikv
        .run(opts(), move |txn| {
            let journal = journal.clone();
            let e = e.clone();
            Box::pin(async move {
                let r = match shard {
                    Some(shard) => journal.append_to(txn, shard, e).await,
                    None => {
                        let mut rng = StdRng::from_rng(&mut rand::rng());
                        journal.append(txn, e, &mut rng).await
                    }
                };
                lift(r)
            })
        })
        .await
        .expect("the append commits");
    let (shard, seq) = c.value.expect("appended");
    (shard, seq, c.commit_ts, c.attempts)
}

async fn snap_at(tikv: &Tikv, at: Timestamp) -> loams_tikv::Snap {
    tikv.snapshot(at).await.expect("a snapshot")
}

async fn heads(tikv: &Tikv, journal: &Journal) -> Vec<u64> {
    let mut snap = snap_at(tikv, tikv.now().await.expect("now")).await;
    journal.heads(&mut snap).await.expect("heads")
}

async fn read(
    tikv: &Tikv,
    journal: &Journal,
    from: &[u64],
    to: &[u64],
) -> Result<Vec<journal::Read>, LiveError> {
    let mut snap = snap_at(tikv, tikv.now().await.expect("now")).await;
    journal.read(&mut snap, from, to).await
}

async fn checkpoint(
    tikv: &Tikv,
    journal: &Journal,
    consumer: &str,
    positions: Vec<u64>,
    ttl: Option<Duration>,
) {
    let journal = journal.clone();
    let consumer = consumer.to_string();
    tikv.run(opts(), move |txn| {
        let journal = journal.clone();
        let consumer = consumer.clone();
        let positions = positions.clone();
        Box::pin(async move { lift(journal.checkpoint(txn, &consumer, &positions, ttl).await) })
    })
    .await
    .expect("the checkpoint commits")
    .value
    .expect("checkpointed");
}

fn at(positions: &[(u16, u64)]) -> Vec<u64> {
    let mut v = vec![0; 16];
    for &(shard, seq) in positions {
        v[usize::from(shard)] = seq;
    }
    v
}

// ---- without a cluster ----

#[test]
fn journal_keys_sort_by_shard_then_sequence() {
    let app = AppKeys::dedicated();
    let keys = [
        app.journal_entry(0, 1),
        app.journal_entry(0, 255),
        app.journal_entry(0, 256),
        app.journal_entry(0, u64::MAX),
        app.journal_entry(1, 0),
        app.journal_entry(256, 7),
    ];
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "entries sort by shard, then sequence"
    );
    for (shard, seq) in [(0, 1), (3, 256), (1023, u64::MAX)] {
        let key = app.journal_entry(shard, seq);
        assert_eq!(app.seq_of_journal_entry(shard, &key), Some(seq));
        assert!(app.journal_entries(shard).contains(&key));
        assert!(!app.journal_entries(shard + 1).contains(&key));
        assert_eq!(app.seq_of_journal_entry(shard + 1, &key), None);
    }
    // §20 §4.3: head = 0x04 ‖ 0x00 ‖ shard, entry = 0x04 ‖ 0x01 ‖ shard ‖ seq.
    assert_eq!(app.journal_head(0x0102), vec![0x04, 0x00, 0x01, 0x02]);
    assert_eq!(
        app.journal_entry(2, 3),
        vec![0x04, 0x01, 0x00, 0x02, 0, 0, 0, 0, 0, 0, 0, 3]
    );
    let head = app.journal_head(0);
    let cp = app.journal_checkpoint(0, "node-a");
    assert!(!app.journal_entries(0).contains(&head));
    assert!(!app.journal_entries(0).contains(&cp));
    assert!(app.journal_checkpoints(0).contains(&cp));
    assert!(!app.journal_checkpoints(1).contains(&cp));
}

#[test]
fn journal_shard_count_is_bounded() {
    assert!(matches!(
        Journal::new(AppKeys::dedicated(), 0),
        Err(LiveError::InvalidArgument(_))
    ));
    assert!(matches!(
        Journal::new(AppKeys::dedicated(), MAX_SHARDS + 1),
        Err(LiveError::InvalidArgument(_))
    ));
    let j = Journal::new(AppKeys::dedicated(), MAX_SHARDS).expect("1 024 shards");
    let mut rng = StdRng::seed_from_u64(7);
    let picked: BTreeSet<u16> = (0..20_000).map(|_| j.pick(&mut rng)).collect();
    assert_eq!(
        picked.len(),
        usize::from(MAX_SHARDS),
        "every shard is drawn"
    );
}

#[test]
fn a_large_entry_splits_into_bounded_chunks_in_order() {
    let mut big = entry("big", 0);
    big.commit_hint_ms = 42;
    big.writes = (0..3000).map(|i| write(i, 1024)).collect();
    let chunks = journal::split(big.clone()).expect("split");
    assert!(chunks.len() >= 3, "3 MB of writes need at least 3 chunks");
    for chunk in &chunks {
        assert!(chunk.encoded_len() as usize <= MAX_CHUNK_BYTES);
        assert!(!chunk.writes.is_empty());
        assert_eq!(chunk.request_id, "big");
        assert_eq!(chunk.function, "test:append");
        assert_eq!(chunk.commit_hint_ms, 42);
    }
    let rejoined: Vec<_> = chunks.into_iter().flat_map(|c| c.writes).collect();
    assert_eq!(rejoined, big.writes);
    // A small entry is one chunk, unchanged.
    let small = entry("small", 3);
    assert_eq!(journal::split(small.clone()).expect("split"), vec![small]);
}

// ---- on TiKV ----

/// Review Focus 1: 32 writers, 2 000 mutations; every shard's sequence is
/// 1..=head with no gap, each mutation is in exactly one entry, and within a
/// shard the sequence follows the commit order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_is_dense_under_concurrent_mutations() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    const WRITERS: usize = 32;
    const MUTATIONS: usize = 2000;
    let next = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..WRITERS {
        let (tikv, journal, next) = (tikv.clone(), journal.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            let mut done = Vec::new();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= MUTATIONS {
                    return done;
                }
                let id = format!("m{i}");
                let (shard, seq, commit_ts, attempts) =
                    append(&tikv, &journal, entry(&id, 1), None).await;
                done.push((shard, seq, commit_ts.version(), attempts, id));
            }
        }));
    }
    let mut appended = Vec::new();
    for task in tasks {
        appended.extend(task.await.expect("a writer"));
    }
    assert_eq!(appended.len(), MUTATIONS);
    let reruns: u32 = appended.iter().map(|a| a.3 - 1).sum();
    eprintln!("{MUTATIONS} appends, {reruns} reruns on head conflicts");

    let heads = heads(&tikv, &journal).await;
    assert_eq!(heads.iter().sum::<u64>(), MUTATIONS as u64);
    // `read` itself refuses a gap; check the result once more.
    let entries = read(&tikv, &journal, &[0; 16], &heads)
        .await
        .expect("the whole journal");
    let mut by_shard: BTreeMap<u16, Vec<u64>> = BTreeMap::new();
    for (shard, seq, _) in &entries {
        by_shard.entry(*shard).or_default().push(*seq);
    }
    for (shard, seqs) in &by_shard {
        let expected: Vec<u64> = (1..=heads[usize::from(*shard)]).collect();
        assert_eq!(seqs, &expected, "shard {shard} is dense");
    }
    let ids: BTreeMap<(u16, u64), String> = entries
        .into_iter()
        .map(|(s, q, e)| ((s, q), e.request_id))
        .collect();
    let mut seen = BTreeSet::new();
    for (shard, seq, _, _, id) in &appended {
        assert_eq!(
            ids.get(&(*shard, *seq)),
            Some(id),
            "the entry at {shard}/{seq}"
        );
        assert!(seen.insert(id.clone()), "{id} is in one entry");
    }
    // Sequence order is commit order within a shard.
    let mut ordered = appended.clone();
    ordered.sort_by_key(|a| (a.0, a.1));
    for w in ordered.windows(2) {
        if w[0].0 == w[1].0 {
            assert!(
                w[0].2 < w[1].2,
                "shard {}: seq {} committed before {}",
                w[0].0,
                w[0].1,
                w[1].1
            );
        }
    }
}

/// An aborted mutation leaves no entry and no gap: the next one on its
/// shard takes the sequence the aborted one would have had.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_mutation_leaves_no_entry_and_no_gap() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    let (_, first, _, _) = append(&tikv, &journal, entry("a", 1), Some(3)).await;
    assert_eq!(first, 1);
    let j = journal.clone();
    let aborted = tikv
        .run(opts(), move |txn| {
            let j = j.clone();
            Box::pin(async move {
                let (_, seq) = j
                    .append_to(txn, 3, entry("aborted", 1))
                    .await
                    .expect("appended");
                assert_eq!(seq, 2);
                Err::<(), _>(TxnError::Fatal("the function threw".into()))
            })
        })
        .await;
    assert!(matches!(aborted, Err(TxnError::Fatal(_))));
    assert_eq!(heads(&tikv, &journal).await[3], 1, "the head did not move");
    let (_, next, _, _) = append(&tikv, &journal, entry("b", 1), Some(3)).await;
    assert_eq!(next, 2, "no gap");
    let entries = read(&tikv, &journal, &[0; 16], &at(&[(3, 2)]))
        .await
        .expect("dense");
    let ids: Vec<_> = entries.iter().map(|e| e.2.request_id.as_str()).collect();
    assert_eq!(ids, ["a", "b"]);
}

/// Two mutations that read the same head both write `h + 1`: the second to
/// commit conflicts, reruns and takes `h + 2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conflict_on_the_head_reruns_and_stays_dense() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for name in ["x", "y"] {
        let (tikv, journal, barrier) = (tikv.clone(), journal.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            tikv.run(opts(), move |txn| {
                let (journal, barrier) = (journal.clone(), barrier.clone());
                Box::pin(async move {
                    let r = journal.append_to(txn, 5, entry(name, 1)).await;
                    if txn.attempt() == 1 {
                        // Both have read head 0 before either commits.
                        barrier.wait().await;
                    }
                    lift(r)
                })
            })
            .await
            .expect("commits")
        }));
    }
    let mut seqs = Vec::new();
    let mut attempts = 0;
    for task in tasks {
        let c = task.await.expect("a writer");
        seqs.push(c.value.expect("appended").1);
        attempts += c.attempts;
    }
    seqs.sort_unstable();
    assert_eq!(seqs, [1, 2]);
    assert_eq!(attempts, 3, "one of the two reran once");
    read(&tikv, &journal, &[0; 16], &at(&[(5, 2)]))
        .await
        .expect("dense");
}

/// An entry is visible at `T` exactly when its transaction committed at or
/// before `T`; an aborted one is never visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entry_visible_iff_committed() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    append(&tikv, &journal, entry("before", 1), Some(0)).await;
    let (_, seq, commit_ts, _) = append(&tikv, &journal, entry("it", 1), Some(0)).await;
    assert_eq!(seq, 2);
    let key = AppKeys::dedicated().journal_entry(0, seq);

    let mut at_commit = snap_at(&tikv, commit_ts.clone()).await;
    assert_eq!(journal.heads(&mut at_commit).await.expect("heads")[0], 2);
    let got = journal
        .read(&mut at_commit, &at(&[(0, 1)]), &at(&[(0, 2)]))
        .await
        .expect("visible at its commit timestamp");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].2.request_id, "it");
    assert!(got[0].2.commit_hint_ms > 0, "append sets the hint");

    let before = Timestamp::from_version(commit_ts.version() - 1);
    let mut just_before = snap_at(&tikv, before).await;
    assert_eq!(journal.heads(&mut just_before).await.expect("heads")[0], 1);
    assert_eq!(just_before.get(&key).await.expect("a read"), None);

    // An aborted append is visible at no timestamp.
    let j = journal.clone();
    let aborted = tikv
        .run(opts(), move |txn| {
            let j = j.clone();
            Box::pin(async move {
                j.append_to(txn, 0, entry("aborted", 1))
                    .await
                    .expect("appended");
                Err::<(), _>(TxnError::Fatal("abort".into()))
            })
        })
        .await;
    assert!(aborted.is_err());
    let mut later = snap_at(&tikv, tikv.now().await.expect("now")).await;
    assert_eq!(journal.heads(&mut later).await.expect("heads")[0], 2);
    assert_eq!(
        later
            .get(&AppKeys::dedicated().journal_entry(0, 3))
            .await
            .expect("a read"),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_without_writes_is_refused() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    let j = journal.clone();
    let r = tikv
        .run(opts(), move |txn| {
            let j = j.clone();
            Box::pin(async move { lift(j.append_to(txn, 0, entry("empty", 0)).await) })
        })
        .await
        .expect("commits")
        .value;
    assert!(matches!(r, Err(LiveError::InvalidArgument(_))), "{r:?}");
    assert_eq!(heads(&tikv, &journal).await, vec![0; 16]);
}

/// An entry over the chunk size takes consecutive sequences of one shard;
/// `append` returns the last.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_entry_takes_consecutive_sequences() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    let mut big = entry("big", 0);
    big.writes = (0..3000).map(|i| write(i, 1024)).collect();
    let chunks = journal::split(big.clone()).expect("split").len() as u64;
    let (_, seq, _, _) = append(&tikv, &journal, big.clone(), Some(2)).await;
    assert_eq!(seq, chunks);
    let entries = read(&tikv, &journal, &[0; 16], &at(&[(2, seq)]))
        .await
        .expect("dense");
    assert_eq!(entries.len() as u64, chunks);
    assert!(entries.iter().all(|e| e.2.request_id == "big"));
    let rejoined: Vec<_> = entries.into_iter().flat_map(|e| e.2.writes).collect();
    assert_eq!(rejoined, big.writes);
}

/// Every entry reaches the tailer exactly once across ticks; an
/// unacknowledged tick is read again, and a stale batch cannot be acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tailer_sees_each_entry_once_across_ticks() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    append(&tikv, &journal, entry("before-start", 1), None).await;
    let mut tailer = Tailer::start(
        tikv.clone(),
        journal.clone(),
        tikv.now().await.expect("now"),
    )
    .await
    .expect("a tailer");
    let mut appended = BTreeSet::new();
    let mut seen = Vec::new();
    for round in 0..12usize {
        for i in 0..(round % 5) {
            let (shard, seq, _, _) =
                append(&tikv, &journal, entry(&format!("r{round}-{i}"), 1), None).await;
            appended.insert((shard, seq));
        }
        let batch = tailer
            .tick(tikv.now().await.expect("now"))
            .await
            .expect("a tick");
        assert_eq!(batch.is_empty(), round % 5 == 0);
        if round % 3 == 1 {
            // Not acknowledged: the next tick reads the same entries again.
            let again = tailer
                .tick(tikv.now().await.expect("now"))
                .await
                .expect("a tick");
            assert_eq!(again.entries, batch.entries);
            tailer.ack(&again).expect("ack");
            assert!(
                tailer.ack(&batch).is_err() || batch.is_empty(),
                "a stale batch"
            );
        } else {
            tailer.ack(&batch).expect("ack");
        }
        seen.extend(batch.entries.iter().map(|e| (e.0, e.1)));
        assert!(
            batch
                .entries
                .iter()
                .all(|e| e.2.request_id != "before-start")
        );
    }
    let unique: BTreeSet<_> = seen.iter().copied().collect();
    assert_eq!(unique.len(), seen.len(), "no entry twice");
    assert_eq!(unique, appended, "every entry once");
    assert_eq!(tailer.positions(), heads(&tikv, &journal).await.as_slice());
}

/// Review of #73: a tick reads at most its byte budget; a backlog takes
/// several ticks at one timestamp, each entry once, in shard and sequence
/// order, and only the last is `complete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tailer_reads_a_backlog_in_bounded_ticks() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    let mut tailer = Tailer::start(
        tikv.clone(),
        journal.clone(),
        tikv.now().await.expect("now"),
    )
    .await
    .expect("a tailer")
    .with_max_batch_bytes(1);
    let mut appended = Vec::new();
    for (i, shard) in [2u16, 0, 2, 5, 0, 2].into_iter().enumerate() {
        let (shard, seq, _, _) =
            append(&tikv, &journal, entry(&format!("b{i}"), 1), Some(shard)).await;
        appended.push((shard, seq));
    }
    appended.sort();
    let at = tikv.now().await.expect("now");
    let mut seen = Vec::new();
    loop {
        let batch = tailer.tick(at.clone()).await.expect("a tick");
        assert_eq!(batch.entries.len(), 1, "one entry per 1-byte budget");
        seen.extend(batch.entries.iter().map(|e| (e.0, e.1)));
        tailer.ack(&batch).expect("ack");
        if batch.complete {
            break;
        }
        assert!(seen.len() < appended.len(), "complete at the last entry");
    }
    assert_eq!(seen, appended, "every entry once, in order");
    assert_eq!(tailer.positions(), heads(&tikv, &journal).await.as_slice());
    let idle = tailer.tick(at).await.expect("a tick");
    assert!(idle.is_empty() && idle.complete);
}

/// A tailer resumes from its checkpoint and sees only what came after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tailer_resumes_from_its_checkpoint() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    assert!(
        Tailer::resume(tikv.clone(), journal.clone(), "node-a")
            .await
            .expect("a lookup")
            .is_none()
    );
    let mut tailer = Tailer::start(
        tikv.clone(),
        journal.clone(),
        tikv.now().await.expect("now"),
    )
    .await
    .expect("a tailer");
    for i in 0..3 {
        append(&tikv, &journal, entry(&format!("old{i}"), 1), None).await;
    }
    let batch = tailer
        .tick(tikv.now().await.expect("now"))
        .await
        .expect("a tick");
    assert_eq!(batch.entries.len(), 3);
    tailer.ack(&batch).expect("ack");
    tailer
        .checkpoint("node-a", Some(Duration::from_secs(60)))
        .await
        .expect("checkpointed");
    for i in 0..2 {
        append(&tikv, &journal, entry(&format!("new{i}"), 1), None).await;
    }
    let resumed = Tailer::resume(tikv.clone(), journal.clone(), "node-a")
        .await
        .expect("a lookup")
        .expect("a checkpoint");
    assert_eq!(resumed.positions(), tailer.positions());
    let batch = resumed
        .tick(tikv.now().await.expect("now"))
        .await
        .expect("a tick");
    let ids: BTreeSet<_> = batch
        .entries
        .iter()
        .map(|e| e.2.request_id.clone())
        .collect();
    assert_eq!(
        ids,
        BTreeSet::from(["new0".to_string(), "new1".to_string()])
    );
    let mut snap = snap_at(&tikv, tikv.now().await.expect("now")).await;
    let cp = journal
        .load_checkpoint(&mut snap, "node-a")
        .await
        .expect("a lookup")
        .expect("a checkpoint");
    assert!(cp.expires_ms.is_some());
}

/// The janitor deletes only entries every live consumer has passed and that
/// are older than the retention; an expired checkpoint holds nothing; a
/// consumer below the trimmed floor gets `JournalTrimmed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn janitor_respects_slowest_consumer() {
    let Some((tikv, journal)) = live().await else {
        return;
    };
    for i in 0..10 {
        append(&tikv, &journal, entry(&format!("s0-{i}"), 1), Some(0)).await;
    }
    for i in 0..6 {
        append(&tikv, &journal, entry(&format!("s1-{i}"), 1), Some(1)).await;
    }
    let head = at(&[(0, 10), (1, 6)]);
    checkpoint(&tikv, &journal, "fast", head.clone(), None).await;
    checkpoint(&tikv, &journal, "slow", at(&[(0, 4), (1, 2)]), None).await;
    checkpoint(
        &tikv,
        &journal,
        "gone",
        vec![0; 16],
        Some(Duration::from_millis(1)),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Within the retention nothing goes, but the expired checkpoint does.
    let report = Janitor::new(tikv.clone(), journal.clone())
        .run_once()
        .await
        .expect("a pass");
    assert_eq!(report.deleted, 0);
    assert_eq!(report.expired_checkpoints, 16, "one record per shard");
    assert_eq!(report.floors, at(&[(0, 4), (1, 2)]));

    let janitor = Janitor::new(tikv.clone(), journal.clone()).with_retention(Duration::ZERO);
    let report = janitor.run_once().await.expect("a pass");
    assert_eq!(report.deleted, 4 + 2, "up to the slowest consumer");
    let slow = at(&[(0, 4), (1, 2)]);
    read(&tikv, &journal, &slow, &head)
        .await
        .expect("the slow consumer reads on");
    match read(&tikv, &journal, &[0; 16], &head).await {
        Err(LiveError::JournalTrimmed {
            shard: 0,
            position: 0,
            first: Some(5),
        }) => {}
        other => panic!("expected JournalTrimmed, got {other:?}"),
    }

    // The slow consumer catches up: everything goes.
    checkpoint(&tikv, &journal, "slow", head.clone(), None).await;
    let report = janitor.run_once().await.expect("a pass");
    assert_eq!(report.deleted, 6 + 4);
    assert_eq!(report.floors, head);
    match read(&tikv, &journal, &slow, &head).await {
        Err(LiveError::JournalTrimmed { first: None, .. }) => {}
        other => panic!("expected JournalTrimmed, got {other:?}"),
    }
    // Appends go on at the head; a consumer at the head reads them.
    let (shard, seq, _, _) = append(&tikv, &journal, entry("after", 1), Some(0)).await;
    assert_eq!((shard, seq), (0, 11));
    let entries = read(&tikv, &journal, &head, &at(&[(0, 11), (1, 6)]))
        .await
        .expect("reads on");
    assert_eq!(entries.len(), 1);

    // Without consumers the head is the floor; more than one batch per shard.
    for i in 0..300 {
        append(&tikv, &journal, entry(&format!("b{i}"), 1), Some(7)).await;
    }
    let j = journal.clone();
    tikv.run(opts(), move |txn| {
        let j = j.clone();
        Box::pin(async move {
            lift(
                async {
                    j.remove_checkpoint(txn, "fast").await?;
                    j.remove_checkpoint(txn, "slow").await
                }
                .await,
            )
        })
    })
    .await
    .expect("commits")
    .value
    .expect("removed");
    let report = janitor.run_once().await.expect("a pass");
    assert_eq!(report.deleted, 300 + 1, "no consumer holds anything");
    assert_eq!(report.floors, at(&[(0, 11), (1, 6), (7, 300)]));
}
