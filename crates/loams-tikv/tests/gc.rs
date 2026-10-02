//! The cluster MVCC GC loop and GC barriers (R1 plan Task 3). Every test
//! needs a cluster and skips without `LOAMS_TEST_PD`.
//!
//! The GC safe point is one per cluster, so the tests of this file run one at
//! a time (the `SERIAL` lock) and compare against what PD holds afterwards.
//! Several use a GC life time of 2 s (the feature `faults`, which the tests
//! build with, allows it) so that locks and versions fall below the safe
//! point within seconds. Physical removal of old versions waits for RocksDB
//! compaction with TiKV's default compaction filter, so the one test that
//! checks it (`old_versions_dropped_after_gc`) is nightly-only and turns the
//! filter off through TiKV's online config.

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt;
use loams_tikv::testing::{self, TEST_LIVE, TEST_META, TEST_SQL};
use loams_tikv::token::{TOKEN_TTL, fence_value, token_key, token_value};
use loams_tikv::{
    CommitMode, GcBarrier, GcConfig, GcLoop, Tikv, TikvError, Timestamp, TimestampExt, TxnError,
    TxnOptions,
};
use tokio_util::sync::CancellationToken;

/// One GC test at a time: they share the cluster's one safe point, and the
/// lock test turns on process-wide failpoints.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SHORT: Duration = Duration::from_secs(2);

fn short_config() -> GcConfig {
    GcConfig {
        life_time: SHORT,
        ..GcConfig::default()
    }
}

/// The cluster safe point as PD reports it now, through a fresh handle (no
/// cached answer).
async fn pd_safe_point(cluster: &testing::TestCluster) -> u64 {
    cluster
        .connect(TEST_META)
        .await
        .gc_safe_point()
        .await
        .expect("GetGCSafePoint")
        .version()
}

async fn put(tikv: &Tikv, key: &'static [u8], value: &'static [u8]) {
    tikv.run(TxnOptions::new("test.put"), move |txn| {
        Box::pin(async move { txn.put(key, value).await })
    })
    .await
    .expect("put");
}

async fn read_at(tikv: &Tikv, at: Timestamp, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
    let mut snap = tikv.snapshot(at).await.expect("a snapshot");
    snap.get(key).await
}

/// TiKV's `tikv_gcworker_autogc_safe_point` gauge (a float of the TSO
/// version).
async fn tikv_safe_point_metric(http: &reqwest::Client) -> Option<f64> {
    let url = format!("{}/metrics", testing::tikv_status());
    let body = http.get(url).send().await.ok()?.text().await.ok()?;
    body.lines()
        .find(|l| l.starts_with("tikv_gcworker_autogc_safe_point"))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

async fn set_compaction_filter(http: &reqwest::Client, on: bool) {
    let url = format!("{}/config", testing::tikv_status());
    let response = http
        .post(url)
        .json(&serde_json::json!({ "gc.enable-compaction-filter": on }))
        .send()
        .await
        .expect("TiKV's status server answers");
    assert!(
        response.status().is_success(),
        "POST /config answered {}",
        response.status()
    );
}

#[tokio::test]
async fn safe_point_advances_cluster_wide() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let tikv = cluster.connect(TEST_META).await;
    let before = tikv.now().await.expect("now");
    let gc = GcLoop::new(tikv.clone(), GcConfig::default()).expect("a loop");
    let report = gc.run_once().await.expect("a run");

    let after = tikv.now().await.expect("now");
    // target = now − life time (the default, 10 min), with now taken inside
    // the run.
    let back = |t: &Timestamp| Tikv::physical_ms(t) - Tikv::physical_ms(&report.target);
    assert!(
        back(&before) <= 600_000 && back(&after) >= 600_000,
        "{report:?}"
    );
    assert!(
        report.keyspaces >= 6,
        "every playground keyspace: {report:?}"
    );
    assert_eq!(pd_safe_point(&cluster).await, report.safe_point.version());

    // TiKV polls PD for the safe point about every 10 s.
    let http = reqwest::Client::new();
    let want = report.safe_point.version() as f64;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seen = tikv_safe_point_metric(&http).await;
        // The gauge is a float: allow its rounding (well under 1 ms of TSO).
        if seen.is_some_and(|v| v >= want - 1024.0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "TiKV's autogc safe point is {seen:?}, want {want}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn barrier_holds_safe_point() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let tikv = cluster.connect(TEST_META).await;
    let barrier = GcBarrier::new(&tikv);
    let id = GcBarrier::service_id("test", &format!("{:x}", rand::random::<u64>()));
    let at = tikv.now().await.expect("now");
    barrier
        .set(&id, &at, Duration::from_secs(120))
        .await
        .expect("a barrier");

    tokio::time::sleep(SHORT + Duration::from_millis(500)).await;
    let gc = GcLoop::new(tikv.clone(), short_config()).expect("a loop");
    let held = gc.run_once().await;
    let released = async {
        barrier.delete(&id).await.expect("delete the barrier");
        gc.run_once().await
    }
    .await;

    let held = held.expect("a run");
    assert!(held.target.version() > at.version());
    assert_eq!(held.held_by.as_deref(), Some(id.as_str()));
    assert_eq!(held.safe_point.version(), at.version());

    let released = released.expect("a run");
    assert_eq!(released.held_by, None);
    assert!(released.safe_point.version() > at.version());
    assert_eq!(pd_safe_point(&cluster).await, released.safe_point.version());
}

#[tokio::test]
async fn barrier_below_the_safe_point_is_refused() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let tikv = cluster.connect(TEST_META).await;
    // A fresh cluster's safe point is 0, so a barrier at version 1 is not
    // below it until some round has advanced it: run one first, whatever
    // order the tests run in.
    GcLoop::new(tikv.clone(), short_config())
        .expect("a loop")
        .run_once()
        .await
        .expect("a run");
    let id = GcBarrier::service_id("test", "below");
    let barrier = GcBarrier::new(&tikv);
    let set = barrier
        .set(&id, &Timestamp::from_version(1), Duration::from_secs(60))
        .await;
    if set.is_ok() {
        // Never leave the barrier to hold the safe point for the other tests.
        barrier.delete(&id).await.expect("delete the barrier");
    }
    match set {
        Err(TikvError::BarrierBelowSafePoint { min_safe_point, .. }) => {
            assert!(min_safe_point > 1);
        }
        other => panic!("expected BarrierBelowSafePoint, got {other:?}"),
    }
    assert!(matches!(
        GcBarrier::new(&tikv)
            .set(
                "gc_worker",
                &tikv.now().await.expect("now"),
                Duration::from_secs(60)
            )
            .await,
        Err(TikvError::Config(_))
    ));
}

#[tokio::test]
async fn barrier_lets_old_snapshots_through() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    // A 62 s life time leaves a 2 s read window.
    let mut config = cluster.config(TEST_META);
    config.gc_life_time = Duration::from_secs(62);
    let tikv = Tikv::connect(config.clone()).await.expect("connect");
    let other = Tikv::connect(config).await.expect("connect");
    put(&tikv, b"k", b"v1").await;
    let at = tikv.now().await.expect("now");
    put(&tikv, b"k", b"v2").await;

    let barrier = GcBarrier::new(&tikv);
    let id = GcBarrier::service_id("test", &format!("{:x}", rand::random::<u64>()));
    barrier
        .set(&id, &at, Duration::from_secs(120))
        .await
        .expect("a barrier");
    let mut held = tikv.snapshot(at.clone()).await.expect("inside the window");
    tokio::time::sleep(Duration::from_millis(2_500)).await;

    // Past the window: the barrier's handle reads on, another handle is
    // refused.
    let outcome = async {
        let late = read_at(&tikv, at.clone(), b"k").await;
        let held_read = held.get(b"k").await;
        let refused = other.snapshot(at.clone()).await.map(|_| ());
        (late, held_read, refused)
    }
    .await;
    barrier.delete(&id).await.expect("delete the barrier");
    let (late, held_read, refused) = outcome;
    assert_eq!(late.expect("covered"), Some(b"v1".to_vec()));
    assert_eq!(held_read.expect("covered"), Some(b"v1".to_vec()));
    assert!(matches!(refused, Err(TikvError::GcSafePoint { .. })));

    // Without the barrier both refuse.
    assert!(matches!(held.get(b"k").await, Err(TxnError::Fatal(_))));
    assert!(matches!(
        tikv.snapshot(at).await,
        Err(TikvError::GcSafePoint { .. })
    ));
}

#[tokio::test]
async fn only_one_loop_runs_per_cluster() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let tikv = cluster.connect(TEST_META).await;
    let a = GcLoop::new(tikv.clone(), GcConfig::default()).expect("a loop");
    let b = GcLoop::new(tikv.clone(), GcConfig::default()).expect("a loop");
    a.run_once().await.expect("the first loop takes the lease");
    assert!(matches!(b.run_once().await, Err(TikvError::GcLease { .. })));
    a.run_once().await.expect("the holder runs again");

    // Racing for a free lease (a new root): exactly one wins.
    let tikv = cluster.connect(TEST_META).await;
    let a = GcLoop::new(tikv.clone(), GcConfig::default()).expect("a loop");
    let b = GcLoop::new(tikv.clone(), GcConfig::default()).expect("a loop");
    let (ra, rb) = tokio::join!(a.run_once(), b.run_once());
    let wins = [ra.is_ok(), rb.is_ok()];
    assert_eq!(wins.iter().filter(|w| **w).count(), 1, "{ra:?} / {rb:?}");
    for r in [ra, rb] {
        if let Err(e) = r {
            assert!(matches!(e, TikvError::GcLease { .. }), "{e}");
        }
    }

    // A spawned loop runs until its shutdown token is cancelled.
    let tikv = cluster.connect(TEST_META).await;
    let shutdown = CancellationToken::new();
    let config = GcConfig {
        interval: Duration::from_millis(200),
        ..GcConfig::default()
    };
    let handle = GcLoop::spawn(tikv, config, shutdown.clone()).expect("spawn");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !handle.last().is_some_and(|r| r.is_ok()) {
        assert!(Instant::now() < deadline, "{:?}", handle.last());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), handle.stopped())
        .await
        .expect("the loop stops");
}

#[tokio::test]
async fn locks_below_the_safe_point_are_resolved_in_every_keyspace() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let handles = [
        cluster.connect(TEST_META).await,
        cluster.connect(TEST_LIVE).await,
        cluster.connect(TEST_SQL).await,
    ];

    // Leave a prewrite lock in each keyspace: a two-phase commit fails right
    // after its prewrite, and its rollback is suppressed.
    fail::cfg("after-prewrite", "return").expect("failpoint");
    fail::cfg("before-rollback", "return").expect("failpoint");
    let left = AssertUnwindSafe(async {
        for tikv in &handles {
            let opts = TxnOptions {
                max_attempts: 1,
                commit_mode: Some(CommitMode::TwoPc),
                ..TxnOptions::new("test.leave_lock")
            };
            let res = tikv
                .run(opts, |txn| {
                    Box::pin(async move {
                        txn.put(b"locked-a", b"x").await?;
                        txn.put(b"locked-b", b"y").await
                    })
                })
                .await;
            assert!(matches!(res, Err(TxnError::NotApplied(_))), "{res:?}");
        }
    })
    .catch_unwind()
    .await;
    fail::remove("after-prewrite");
    fail::remove("before-rollback");
    left.expect("locks left");

    let now = handles[0].now().await.expect("now");
    for tikv in &handles {
        assert_eq!(tikv.locks_below(&now).await.expect("scan locks"), 2);
    }

    // The locks' TTL (3 s) expires and their start falls below now − 2 s.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let gc = GcLoop::new(handles[0].clone(), short_config()).expect("a loop");
    let report = gc.run_once().await.expect("a run");
    assert!(report.locks_resolved >= 6, "{report:?}");
    assert!(report.safe_point.version() > now.version());
    for tikv in &handles {
        let later = tikv.now().await.expect("now");
        assert_eq!(tikv.locks_below(&later).await.expect("scan locks"), 0);
        assert_eq!(read_at(tikv, later, b"locked-a").await.expect("read"), None);
    }
}

#[tokio::test]
async fn expired_tokens_are_swept() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let meta = cluster.connect(TEST_META).await;
    let live = cluster.connect(TEST_LIVE).await;
    let now_ms = Tikv::physical_ms(&meta.now().await.expect("now"));
    let ttl_ms = u64::try_from(TOKEN_TTL.as_millis()).expect("small");
    let long_ago = now_ms - ttl_ms - 60_000;
    let token = |b: u8| token_key(&[b; 16]);

    let write = |tikv: Tikv, rows: Vec<(Vec<u8>, Vec<u8>)>| async move {
        tikv.run(TxnOptions::new("test.tokens"), move |txn| {
            let rows = rows.clone();
            Box::pin(async move {
                for (k, v) in &rows {
                    txn.put(k, v.clone()).await?;
                }
                Ok(())
            })
        })
        .await
        .expect("write tokens");
    };
    write(
        meta.clone(),
        vec![
            (token(1), token_value(long_ago)),
            (token(2), token_value(long_ago)),
            (token(3), fence_value(long_ago)),
            (token(4), token_value(now_ms)),
            (token(5), fence_value(now_ms)),
            (token(6), b"not a token".to_vec()),
        ],
    )
    .await;
    write(
        live.clone(),
        vec![
            (token(7), token_value(long_ago)),
            (token(8), token_value(now_ms)),
        ],
    )
    .await;

    let config = GcConfig {
        sweep: vec![live.clone()],
        ..GcConfig::default()
    };
    let gc = GcLoop::new(meta.clone(), config).expect("a loop");
    let report = gc.run_once().await.expect("a run");
    assert_eq!(report.tokens_swept, 4, "{report:?}");
    assert_eq!(report.sweep_error, None);

    let at = meta.now().await.expect("now");
    for (tikv, b, kept) in [
        (&meta, 1, false),
        (&meta, 2, false),
        (&meta, 3, false),
        (&meta, 4, true),
        (&meta, 5, true),
        (&meta, 6, true),
        (&live, 7, false),
        (&live, 8, true),
    ] {
        let present = read_at(tikv, at.clone(), &token(b))
            .await
            .expect("read")
            .is_some();
        assert_eq!(present, kept, "token {b}");
    }
}

#[tokio::test]
async fn old_versions_dropped_after_gc() {
    let Some(cluster) = testing::cluster().await else {
        return;
    };
    if !testing::nightly() {
        return;
    }
    let _serial = SERIAL.lock().await;
    let http = reqwest::Client::new();
    // With the compaction filter on (the default), TiKV drops old versions
    // only when RocksDB compacts; off, its GC worker scans and deletes them.
    set_compaction_filter(&http, false).await;
    let outcome = AssertUnwindSafe(async {
        let tikv = cluster.connect(TEST_META).await;
        put(&tikv, b"k", b"v1").await;
        let mid = tikv.now().await.expect("now");
        put(&tikv, b"k", b"v2").await;
        assert_eq!(
            read_at(&tikv, mid.clone(), b"k").await.expect("read"),
            Some(b"v1".to_vec())
        );
        tokio::time::sleep(SHORT + Duration::from_millis(500)).await;
        let gc = GcLoop::new(tikv.clone(), short_config()).expect("a loop");
        let report = gc.run_once().await.expect("a run");
        assert!(report.safe_point.version() > mid.version());
        // `mid` is seconds old, inside the handle's read window, so the read
        // goes through and shows what TiKV still holds.
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let seen = read_at(&tikv, mid.clone(), b"k").await.expect("read");
            if seen.is_none() {
                break;
            }
            assert!(Instant::now() < deadline, "v1 still readable at mid");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let now = tikv.now().await.expect("now");
        assert_eq!(
            read_at(&tikv, now, b"k").await.expect("read"),
            Some(b"v2".to_vec())
        );
    })
    .catch_unwind()
    .await;
    set_compaction_filter(&http, true).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
