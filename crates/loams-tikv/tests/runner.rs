//! The transaction runner, commit tokens, fault hooks, paging and the GC safe
//! window (R1 plan Task 2). Every test needs a cluster and skips without
//! `LOAMS_TEST_PD`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use loams_tikv::testing::{self, TEST_META};
use loams_tikv::{
    CommitMode, Fault, FaultPlan, FaultPoint, MAX_VALUE_BYTES, Tikv, TikvError, Timestamp,
    TimestampExt, TxnError, TxnOptions,
};
use tokio::sync::{Barrier, Notify};

// ---- helpers ----

type Rule = dyn Fn(&str, FaultPoint, u32) -> Option<Fault> + Send + Sync;

/// A fault plan from a closure that records every consultation.
struct Plan {
    rule: Box<Rule>,
    calls: Mutex<Vec<(String, FaultPoint, u32)>>,
}

impl Plan {
    fn new(
        rule: impl Fn(&str, FaultPoint, u32) -> Option<Fault> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Plan {
            rule: Box::new(rule),
            calls: Mutex::new(Vec::new()),
        })
    }

    fn calls(&self, op: &str) -> Vec<(FaultPoint, u32)> {
        self.calls
            .lock()
            .expect("plan")
            .iter()
            .filter(|(o, _, _)| o == op)
            .map(|(_, p, a)| (*p, *a))
            .collect()
    }
}

impl FaultPlan for Plan {
    fn at(&self, op: &str, point: FaultPoint, attempt: u32) -> Option<Fault> {
        self.calls
            .lock()
            .expect("plan")
            .push((op.to_string(), point, attempt));
        (self.rule)(op, point, attempt)
    }
}

fn as_u64(v: Option<Vec<u8>>) -> u64 {
    v.map(|b| u64::from_be_bytes(b.try_into().expect("an 8-byte counter")))
        .unwrap_or(0)
}

/// The value at `key` at a fresh timestamp.
async fn read(tikv: &Tikv, key: &[u8]) -> Option<Vec<u8>> {
    let mut snap = tikv
        .snapshot(tikv.now().await.expect("a timestamp"))
        .await
        .expect("a snapshot");
    snap.get(key).await.expect("a read")
}

/// Increments the counter at `c` in one optimistic run.
async fn increment(tikv: &Tikv, opts: TxnOptions) -> Result<loams_tikv::Committed<u64>, TxnError> {
    tikv.run(opts, |txn| {
        Box::pin(async move {
            let n = as_u64(txn.get(b"c").await?) + 1;
            txn.put(b"c", n.to_be_bytes().to_vec()).await?;
            Ok(n)
        })
    })
    .await
}

// ---- conflicts and pessimistic locks ----

#[tokio::test]
async fn conflicting_writers_both_finish_one_retries() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let barrier = Arc::new(Barrier::new(2));
    let writer = |tikv: Tikv, barrier: Arc<Barrier>| async move {
        tikv.run(TxnOptions::new("incr"), move |txn| {
            let barrier = barrier.clone();
            Box::pin(async move {
                let n = as_u64(txn.get(b"c").await?) + 1;
                if txn.attempt() == 1 {
                    // Both read the same value before either writes.
                    barrier.wait().await;
                }
                txn.put(b"c", n.to_be_bytes().to_vec()).await?;
                Ok(n)
            })
        })
        .await
        .expect("both writers finish")
    };
    let (a, b) = tokio::join!(
        writer(tikv.clone(), barrier.clone()),
        writer(tikv.clone(), barrier.clone())
    );
    assert_eq!(a.attempts.min(b.attempts), 1, "one commits first time");
    assert!(a.attempts.max(b.attempts) >= 2, "the other retries");
    assert_eq!(as_u64(read(&tikv, b"c").await), 2);
    let mut values = [a.value, b.value];
    values.sort_unstable();
    assert_eq!(values, [1, 2]);
    assert!(tikv.stats().restarts >= 1);
}

/// Runs a pessimistic increment that, on its first attempt, signals `locked`
/// once it holds the lock and keeps it for `hold`.
async fn pessimistic_holder(
    tikv: Tikv,
    locked: Arc<Notify>,
    hold: Duration,
) -> loams_tikv::Committed<u64> {
    tikv.run(TxnOptions::pessimistic("hold"), move |txn| {
        let locked = locked.clone();
        Box::pin(async move {
            let n = as_u64(txn.get_for_update(b"c").await?) + 1;
            if txn.attempt() == 1 {
                locked.notify_one();
                tokio::time::sleep(hold).await;
            }
            txn.put(b"c", n.to_be_bytes().to_vec()).await?;
            Ok(n)
        })
    })
    .await
    .expect("the holder commits")
}

/// A pessimistic increment that starts once `locked` fires.
async fn pessimistic_waiter(
    tikv: Tikv,
    locked: Arc<Notify>,
) -> (loams_tikv::Committed<u64>, Instant) {
    locked.notified().await;
    let committed = tikv
        .run(TxnOptions::pessimistic("wait"), |txn| {
            Box::pin(async move {
                let n = as_u64(txn.get_for_update(b"c").await?) + 1;
                txn.put(b"c", n.to_be_bytes().to_vec()).await?;
                Ok(n)
            })
        })
        .await
        .expect("the waiter commits");
    (committed, Instant::now())
}

#[tokio::test]
async fn pessimistic_lock_queues_second_writer() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let locked = Arc::new(Notify::new());
    let hold = Duration::from_millis(500);
    let started = Instant::now();
    let (first, (second, second_done)) = tokio::join!(
        async {
            let c = pessimistic_holder(tikv.clone(), locked.clone(), hold).await;
            (c, Instant::now())
        },
        pessimistic_waiter(tikv.clone(), locked.clone())
    );
    let (first, first_done) = first;
    assert_eq!(first.value, 1, "the holder went first");
    assert_eq!(second.value, 2, "the waiter saw the holder's write");
    assert!(
        second_done >= first_done,
        "the waiter finished after the holder"
    );
    assert!(
        second_done - started >= hold,
        "the waiter queued behind the lock"
    );
    assert_eq!(as_u64(read(&tikv, b"c").await), 2);
}

#[tokio::test]
async fn pessimistic_retry_restarts_the_transaction() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let locked = Arc::new(Notify::new());
    let (first, (second, _)) = tokio::join!(
        pessimistic_holder(tikv.clone(), locked.clone(), Duration::from_millis(300)),
        pessimistic_waiter(tikv.clone(), locked.clone())
    );
    assert_eq!(first.attempts, 1);
    assert!(
        second.attempts >= 2,
        "the waiter's lock request met the holder's newer write (PessimisticRetry) and \
         restarted; attempts = {}",
        second.attempts
    );
    assert_eq!(second.value, 2);
    assert!(tikv.stats().restarts >= 1);
    assert_eq!(as_u64(read(&tikv, b"c").await), 2);
}

/// `batch_get_for_update` in a pessimistic transaction reads the latest
/// committed values, newer than its start timestamp, leaves absent keys out,
/// sorts by key, and locks: a writer of a locked key waits for the commit.
#[tokio::test]
async fn batch_get_for_update_reads_latest_values_and_locks() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    tikv.run(TxnOptions::new("seed"), |txn| {
        Box::pin(async move {
            txn.put(b"a", 1u64.to_be_bytes().to_vec()).await?;
            txn.put(b"b", 2u64.to_be_bytes().to_vec()).await
        })
    })
    .await
    .expect("seed");
    let started = Arc::new(Notify::new());
    let written = Arc::new(Notify::new());
    let locked = Arc::new(Notify::new());
    let hold = Duration::from_millis(300);
    let holder = {
        let (tikv, started, written, locked) = (
            tikv.clone(),
            started.clone(),
            written.clone(),
            locked.clone(),
        );
        async move {
            tikv.run(TxnOptions::pessimistic("batch_lock"), move |txn| {
                let (started, written, locked) = (started.clone(), written.clone(), locked.clone());
                Box::pin(async move {
                    if txn.attempt() == 1 {
                        // `a` changes after this transaction began.
                        started.notify_one();
                        written.notified().await;
                    }
                    let got = txn
                        .batch_get_for_update([b"zz".as_slice(), b"b", b"a"])
                        .await?;
                    if txn.attempt() == 1 {
                        locked.notify_one();
                        tokio::time::sleep(hold).await;
                    }
                    txn.put(b"b", 3u64.to_be_bytes().to_vec()).await?;
                    Ok(got)
                })
            })
            .await
            .expect("the holder commits")
        }
    };
    let other = {
        let tikv = tikv.clone();
        async move {
            started.notified().await;
            tikv.run(TxnOptions::new("write_a"), |txn| {
                Box::pin(async move { txn.put(b"a", 10u64.to_be_bytes().to_vec()).await })
            })
            .await
            .expect("write a");
            written.notify_one();
            locked.notified().await;
            let asked = Instant::now();
            // `b` is locked: this writer queues behind the holder's commit.
            let n = tikv
                .run(TxnOptions::pessimistic("write_b"), |txn| {
                    Box::pin(async move {
                        let n = as_u64(txn.get_for_update(b"b").await?) + 1;
                        txn.put(b"b", n.to_be_bytes().to_vec()).await?;
                        Ok(n)
                    })
                })
                .await
                .expect("write b")
                .value;
            (n, asked.elapsed())
        }
    };
    let (held, (n, waited)) = tokio::join!(holder, other);
    assert_eq!(held.attempts, 1);
    assert_eq!(
        held.value,
        vec![
            (b"a".to_vec(), 10u64.to_be_bytes().to_vec()),
            (b"b".to_vec(), 2u64.to_be_bytes().to_vec()),
        ]
    );
    assert_eq!(n, 4, "the queued writer saw the holder's write");
    assert!(waited >= hold / 2, "the writer did not wait: {waited:?}");
}

// ---- unknown outcomes, refusals, deadlines, fault points ----

#[tokio::test]
async fn undetermined_commit_resolves_by_token() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let plan = Plan::new(|op, point, attempt| match (op, point, attempt) {
        ("lost-ack", FaultPoint::AfterCommit, 1) => Some(Fault::LoseAck),
        ("lost-request", FaultPoint::BeforeCommit, 1) => Some(Fault::LoseAck),
        ("lost-untracked", FaultPoint::AfterCommit, 1) => Some(Fault::LoseAck),
        _ => None,
    });
    let tikv = cluster.connect(TEST_META).await.with_faults(plan.clone());

    // The commit applied and its acknowledgement was lost: the token is
    // there, so the run succeeds without replaying the body.
    let c = increment(&tikv, TxnOptions::new("lost-ack").with_token())
        .await
        .expect("resolved through the token");
    assert!(c.earlier_unknown);
    assert_eq!(c.attempts, 1);
    assert_eq!(c.value, 1);
    assert_eq!(as_u64(read(&tikv, b"c").await), 1, "applied exactly once");

    // The commit request was lost before it reached TiKV: the resolver finds
    // no token, fences it, and the runner retries.
    let c = increment(&tikv, TxnOptions::new("lost-request").with_token())
        .await
        .expect("retried after the fence");
    assert!(!c.earlier_unknown);
    assert_eq!(c.attempts, 2);
    assert_eq!(as_u64(read(&tikv, b"c").await), 2, "applied exactly once");

    // Without a token the outcome stays unknown, and nothing is replayed.
    let err = increment(&tikv, TxnOptions::new("lost-untracked"))
        .await
        .expect_err("no token to resolve");
    assert_eq!(err, TxnError::Undetermined { token: None });
    assert_eq!(
        as_u64(read(&tikv, b"c").await),
        3,
        "applied once, not replayed"
    );
    assert_eq!(tikv.stats().unknown_outcomes, 3);
}

#[tokio::test]
async fn refused_before_prewrite_is_not_applied() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let plan =
        Plan::new(|_, point, _| (point == FaultPoint::BeforePrewrite).then_some(Fault::Refuse));
    let tikv = cluster.connect(TEST_META).await.with_faults(plan.clone());
    let opts = TxnOptions {
        max_attempts: 3,
        ..TxnOptions::new("refused")
    };
    match increment(&tikv, opts).await {
        Err(TxnError::NotApplied(msg)) => assert!(msg.contains("refused before prewrite"), "{msg}"),
        other => panic!("expected NotApplied, got {other:?}"),
    }
    assert_eq!(read(&tikv, b"c").await, None, "nothing was written");
    let prewrites = plan
        .calls("refused")
        .into_iter()
        .filter(|(p, _)| *p == FaultPoint::BeforePrewrite)
        .count();
    assert_eq!(prewrites, 3, "every attempt was refused");
}

#[tokio::test]
async fn deadline_stops_retries() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let plan =
        Plan::new(|_, point, _| (point == FaultPoint::BeforeCommit).then_some(Fault::Conflict));
    let tikv = cluster.connect(TEST_META).await.with_faults(plan);
    let opts = TxnOptions {
        max_attempts: 100_000,
        deadline: Duration::from_millis(300),
        ..TxnOptions::new("forever")
    };
    let started = Instant::now();
    assert_eq!(
        increment(&tikv, opts).await.unwrap_err(),
        TxnError::Deadline
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(read(&tikv, b"c").await, None);
}

#[tokio::test]
async fn faults_fire_per_op_and_attempt() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let plan = Plan::new(|op, point, attempt| {
        (op == "a" && point == FaultPoint::BeforeCommit && attempt == 1).then_some(Fault::Conflict)
    });
    let tikv = cluster.connect(TEST_META).await.with_faults(plan.clone());
    assert_eq!(
        increment(&tikv, TxnOptions::new("a"))
            .await
            .unwrap()
            .attempts,
        2
    );
    assert_eq!(
        increment(&tikv, TxnOptions::new("b"))
            .await
            .unwrap()
            .attempts,
        1
    );
    use FaultPoint::*;
    assert_eq!(
        plan.calls("a"),
        vec![
            (BeforeBegin, 1),
            (BeforePrewrite, 1),
            (BeforeCommit, 1),
            (BeforeBegin, 2),
            (BeforePrewrite, 2),
            (BeforeCommit, 2),
            (AfterCommit, 2),
        ]
    );
    assert_eq!(
        plan.calls("b"),
        vec![
            (BeforeBegin, 1),
            (BeforePrewrite, 1),
            (BeforeCommit, 1),
            (AfterCommit, 1)
        ]
    );
    // A delay fires and the run still commits.
    let plan = Plan::new(|_, point, _| {
        (point == FaultPoint::AfterCommit).then_some(Fault::Delay(Duration::from_millis(50)))
    });
    let tikv = tikv.with_faults(plan);
    let c = increment(&tikv, TxnOptions::new("slow")).await.unwrap();
    assert_eq!((c.attempts, c.earlier_unknown), (1, false));
    assert_eq!(as_u64(read(&tikv, b"c").await), 3);
}

// ---- the TSO supervisor ----

#[tokio::test]
async fn tso_stream_loss_rebuilds_the_client() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    assert_eq!(tikv.stats().client_rebuilds, 0);
    tikv.inject_tso_stream_loss();
    let c = increment(&tikv, TxnOptions::new("after-loss"))
        .await
        .expect("the run succeeds on the rebuilt client");
    assert_eq!(c.attempts, 2, "the first begin met the closed stream");
    assert_eq!(tikv.stats().client_rebuilds, 1);
    assert_eq!(as_u64(read(&tikv, b"c").await), 1);

    // The clock goes through the supervisor too.
    tikv.inject_tso_stream_loss();
    assert!(tikv.now().await.is_err());
    assert!(tikv.now().await.is_ok());
    assert_eq!(tikv.stats().client_rebuilds, 2);
}

// ---- commit modes ----

#[tokio::test]
async fn commit_mode_two_pc_and_async_1pc_both_commit() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    assert_eq!(
        tikv.commit_mode(),
        CommitMode::Async1pc,
        "the default (Ruling 3)"
    );
    for mode in [CommitMode::Async1pc, CommitMode::TwoPc] {
        for pessimistic in [false, true] {
            let base = if pessimistic {
                TxnOptions::pessimistic("mode")
            } else {
                TxnOptions::new("mode")
            };
            let opts = TxnOptions {
                commit_mode: Some(mode),
                ..base
            };
            let tag = format!("{mode:?}/{pessimistic}");
            let t = tag.clone();
            let c = tikv
                .run(opts, move |txn| {
                    let tag = t.clone();
                    Box::pin(async move {
                        // Keys far apart, so the transaction may span regions.
                        for k in [b"a".as_slice(), b"m", b"z"] {
                            let mut key = tag.as_bytes().to_vec();
                            key.extend_from_slice(k);
                            if pessimistic {
                                txn.get_for_update(&key).await?;
                            }
                            txn.put(&key, tag.as_bytes().to_vec()).await?;
                        }
                        Ok(txn.start_ts())
                    })
                })
                .await
                .unwrap_or_else(|e| panic!("{tag}: {e}"));
            assert!(c.commit_ts.version() > c.value.version(), "{tag}");
            for k in [b"a".as_slice(), b"m", b"z"] {
                let mut key = tag.as_bytes().to_vec();
                key.extend_from_slice(k);
                assert_eq!(
                    read(&tikv, &key).await,
                    Some(tag.as_bytes().to_vec()),
                    "{tag}"
                );
            }
        }
    }
    let mut config = cluster.config(TEST_META);
    config.commit_mode = CommitMode::TwoPc;
    let tikv = Tikv::connect(config).await.expect("connect");
    assert_eq!(tikv.commit_mode(), CommitMode::TwoPc);
    assert_eq!(
        increment(&tikv, TxnOptions::new("cfg"))
            .await
            .unwrap()
            .value,
        1
    );
}

// ---- limits: paging, the value bound, the GC safe window ----

#[tokio::test]
async fn scan_pages_stay_under_the_grpc_limit() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    const N: u32 = 2_000;
    const VALUE: usize = 64 * 1024;
    const BATCH: u32 = 32;
    let tikv = cluster.connect(TEST_META).await;
    let key = |i: u32| format!("v/{i:05}").into_bytes();
    let value = |i: u32| {
        let mut v = vec![0xA5; VALUE];
        v[..4].copy_from_slice(&i.to_be_bytes());
        v
    };
    // 125 MiB in 2 MiB transactions, four at a time.
    let mut writers = tokio::task::JoinSet::new();
    for lane in 0..4 {
        let tikv = tikv.clone();
        writers.spawn(async move {
            let mut first = lane * BATCH;
            while first < N {
                tikv.run(TxnOptions::new("load"), move |txn| {
                    Box::pin(async move {
                        for i in first..(first + BATCH).min(N) {
                            txn.put(&key(i), value(i)).await?;
                        }
                        Ok(())
                    })
                })
                .await
                .expect("load");
                first += 4 * BATCH;
            }
        });
    }
    while let Some(done) = writers.join_next().await {
        done.expect("a writer");
    }

    let check = |pairs: &[(Vec<u8>, Vec<u8>)], reverse: bool| {
        assert_eq!(pairs.len(), N as usize);
        for (n, (k, v)) in pairs.iter().enumerate() {
            let n = u32::try_from(n).expect("small");
            let i = if reverse { N - 1 - n } else { n };
            assert_eq!(k, &key(i));
            assert_eq!(v.len(), VALUE);
            assert_eq!(v[..4], i.to_be_bytes());
        }
    };
    let mut snap = tikv.snapshot(tikv.now().await.unwrap()).await.unwrap();
    // 256 values of 64 KiB exceed even the raised 16 MiB limit: the first page
    // fails with OutOfRange and is halved.
    check(&snap.scan(b"v/", Some(b"v0"), 10_000).await.unwrap(), false);
    assert!(tikv.stats().page_halvings >= 1, "{:?}", tikv.stats());
    check(
        &snap.scan_reverse(b"v/", Some(b"v0"), 10_000).await.unwrap(),
        true,
    );
    let keys: Vec<Vec<u8>> = (0..N).map(key).collect();
    check(&snap.batch_get(&keys).await.unwrap(), false);
    let limited = snap.scan(b"v/", None, 300).await.unwrap();
    assert_eq!(limited.len(), 300, "a caller's limit is the total");

    let counts = tikv
        .run(TxnOptions::new("read"), |txn| {
            let keys = keys.clone();
            Box::pin(async move {
                let forward = txn.scan(b"v/", Some(b"v0"), 10_000).await?.len();
                let backward = txn.scan_reverse(b"v/", None, 10_000).await?.len();
                let got = txn.batch_get(&keys).await?.len();
                Ok((forward, backward, got))
            })
        })
        .await
        .unwrap()
        .value;
    assert_eq!(counts, (N as usize, N as usize, N as usize));
}

#[tokio::test]
async fn put_over_2_mib_is_refused() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    tikv.run(TxnOptions::new("max"), |txn| {
        Box::pin(async move { txn.put(b"max", vec![7; MAX_VALUE_BYTES]).await })
    })
    .await
    .expect("exactly 2 MiB is accepted");
    assert_eq!(
        read(&tikv, b"max").await.map(|v| v.len()),
        Some(MAX_VALUE_BYTES)
    );

    for insert in [false, true] {
        let err = tikv
            .run(TxnOptions::new("over"), move |txn| {
                Box::pin(async move {
                    txn.put(b"small", b"x".to_vec()).await?;
                    let big = vec![7; MAX_VALUE_BYTES + 1];
                    if insert {
                        txn.insert(b"over", big).await
                    } else {
                        txn.put(b"over", big).await
                    }
                })
            })
            .await
            .expect_err("over 2 MiB");
        assert_eq!(err, TxnError::Fatal("value over 2 MiB".to_string()));
        assert_eq!(read(&tikv, b"over").await, None);
        assert_eq!(
            read(&tikv, b"small").await,
            None,
            "the transaction rolled back"
        );
    }
}

#[tokio::test]
async fn insert_of_an_existing_key_is_already_exists() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let insert = || {
        tikv.run(TxnOptions::new("insert"), |txn| {
            Box::pin(async move { txn.insert(b"name/x", b"1".to_vec()).await })
        })
    };
    insert().await.expect("the first insert");
    match insert().await {
        Err(TxnError::AlreadyExists(key)) => {
            assert!(key.contains("name/x"), "{key}");
            let root = format!("{}", tikv.root().escape_ascii());
            assert!(!key.contains(&root), "the root is scrubbed: {key}");
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
}

#[tokio::test]
async fn snapshot_below_the_safe_window_is_refused() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let tikv = cluster.connect(TEST_META).await;
    let now = tikv.now().await.unwrap();
    let ago = |d: Duration| Timestamp {
        physical: now.physical - i64::try_from(d.as_millis()).expect("small"),
        logical: 0,
        suffix_bits: 0,
    };
    // The default life time is 10 min, so the window is 9 min.
    match tikv.snapshot(ago(Duration::from_secs(9 * 60 + 30))).await {
        Err(TikvError::GcSafePoint { at, safe_point }) => assert!(at < safe_point),
        other => panic!("expected GcSafePoint, got {other:?}"),
    }
    let mut snap = tikv
        .snapshot(ago(Duration::from_secs(8 * 60)))
        .await
        .expect("inside the window");
    assert_eq!(snap.get(b"k").await.unwrap(), None);

    // A shorter life time narrows the window.
    let mut config = cluster.config(TEST_META);
    config.gc_life_time = Duration::from_secs(120);
    let short = Tikv::connect(config).await.unwrap();
    let now = short.now().await.unwrap();
    let at = |ms: i64| Timestamp {
        physical: now.physical - ms,
        logical: 0,
        suffix_bits: 0,
    };
    assert!(matches!(
        short.snapshot(at(90_000)).await,
        Err(TikvError::GcSafePoint { .. })
    ));
    assert!(short.snapshot(at(30_000)).await.is_ok());
}
