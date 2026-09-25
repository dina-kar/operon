//! Garbage collection of collections and implicit-stream trimming (plan
//! M1.1 Task 12; Rulings 2, 12 and 13; overview A6, A15, A21).
//!
//! The metastore runs on a `ManualClock` started at the wall clock, so GC's
//! grace can be passed at once. Objects named with a ULID are dated by that
//! clock, Lance files by their modification time (the wall clock), so
//! [`Env::past_grace`] moves the clock past the grace of both. After such a
//! jump the wall clock lags the metastore clock, so a Lance file written
//! later looks older than it is: the tests create every unreferenced Lance
//! file they age before jumping.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{TargetFixture, WAIT, field, patch, schema, upsert};
use futures::FutureExt;
use operon_collection::{
    CollectionCommitHook, CollectionCommitStep, CollectionConfig, CollectionContext,
    CollectionError, CollectionGcRoots, CollectionSchema, CollectionSnapshot, CollectionTrimSource,
    DocOp, DynamicMapping, FieldKind, IndexBuildSource, PkGcRoots, PrimaryKey, StoredDoc,
    TRIM_TASK_PREFIX, lance_prefix, retained_chain,
};
use operon_common::StreamId;
use operon_link::LinkApplySource;
use operon_log::gc::{GcConfig, GcReport, GcRoots, GcSource};
use operon_meta::{
    Clock, Consistency, Fence, ManualClock, SystemClock, collection_pk_prefix, collection_prefix,
};
use operon_store::StoreError;
use operon_worker::{
    CancellationToken, RunResult, TaskContext, TaskError, TaskOutcome, TaskSource, run_once,
};
use serde_json::json;
use ulid::Ulid;

const GRACE: Duration = Duration::from_secs(60);

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("millis fit a u64")
}

fn tagged() -> CollectionSchema {
    schema(
        vec![field("tag", FieldKind::Keyword), field("n", FieldKind::I64)],
        DynamicMapping::Ignore,
    )
}

fn key(n: u64) -> PrimaryKey {
    PrimaryKey::U64(n)
}

fn upserts(keys: std::ops::Range<u64>, tag: &str) -> Vec<DocOp> {
    keys.map(|n| upsert(n, json!({ "n": n, "tag": tag })))
        .collect()
}

fn ulid_at(ms: u64) -> Ulid {
    Ulid::from_parts(ms, Ulid::generate().random())
}

/// A collection over a `FaultyStore`, its link, GC with both collection
/// roots, and the manual clock.
struct Env {
    f: TargetFixture,
    clock: Arc<ManualClock>,
    gc: GcSource,
    link: Option<LinkApplySource>,
}

impl Env {
    async fn start(config: CollectionConfig) -> Self {
        let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
        let f = TargetFixture::start_with_clock(tagged(), 2, config, clock.clone()).await;
        let gc = Self::gc_source(&f);
        let link = Some(f.source(f.factory()));
        Self { f, clock, gc, link }
    }

    fn gc_source(f: &TargetFixture) -> GcSource {
        let config = GcConfig {
            grace: GRACE,
            ..GcConfig::default()
        };
        GcSource::with_roots(
            f.store.clone(),
            config,
            vec![
                Arc::new(CollectionGcRoots::new(f.ctx.clone())),
                Arc::new(PkGcRoots),
            ],
        )
    }

    /// Writes `ops` and applies them in one link commit; returns the new
    /// live version and its documents.
    async fn commit(&self, ops: Vec<DocOp>) -> (u64, Vec<StoredDoc>) {
        self.f.write(ops).await;
        let link = self.link.as_ref().expect("the link runs");
        self.f.apply_all(link, "linker").await;
        self.live().await
    }

    /// The live version and its documents.
    async fn live(&self) -> (u64, Vec<StoredDoc>) {
        let snapshot = self.f.snapshot().await;
        let docs = snapshot.scan_all().await.expect("scan");
        (snapshot.manifest().version, docs)
    }

    async fn gc(&self) -> GcReport {
        self.gc
            .run_once(&self.f.meta.client, "gc")
            .await
            .expect("gc")
            .expect("the gc lease")
    }

    /// Moves the clock past the grace of every object written so far,
    /// whether dated by ULID (this clock) or by modification time (the
    /// wall clock).
    fn past_grace(&self) {
        let now = self.clock.now_ms().max(SystemClock.now_ms());
        self.clock.set(now + millis(GRACE) + 1_000);
    }

    async fn objects(&self, prefix: &str) -> BTreeSet<String> {
        self.f
            .store
            .list(prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|info| info.path)
            .collect()
    }

    async fn exists(&self, path: &str) -> bool {
        match self.f.store.head(path).await {
            Ok(_) => true,
            Err(StoreError::NotFound { .. }) => false,
            Err(err) => panic!("head {path}: {err}"),
        }
    }

    fn prefix(&self) -> String {
        collection_prefix(self.f.ns, self.f.cid)
    }

    fn pk_prefix(&self) -> String {
        collection_pk_prefix(self.f.ns, self.f.cid)
    }

    fn lance(&self) -> String {
        lance_prefix(self.f.ns, self.f.cid)
    }

    async fn retired(&self) -> Vec<String> {
        self.f
            .meta
            .client
            .read(Consistency::Linearizable, |s| {
                s.retired().map(|(p, _)| p.to_string()).collect()
            })
            .await
            .expect("read")
    }

    /// The live manifest's retained chain at the metastore clock, newest
    /// first, as (version, path).
    async fn retained(&self) -> Vec<(u64, String)> {
        let ctx = &self.f.ctx;
        let clock_ms = ctx
            .meta
            .read(Consistency::Linearizable, |s| s.clock_ms())
            .await
            .expect("read");
        let live = operon_collection::live_manifest(
            &ctx.meta,
            &ctx.store,
            &ctx.manifests,
            self.f.ns,
            self.f.cid,
            Consistency::Linearizable,
        )
        .await
        .expect("live")
        .expect("a pointer");
        retained_chain(
            &ctx.store,
            &ctx.manifests,
            live,
            ctx.config.keep_manifests,
            ctx.config.time_travel_retention,
            clock_ms,
        )
        .await
        .expect("chain")
        .into_iter()
        .map(|(path, m)| (m.version, path))
        .collect()
    }

    /// A context with no warm caches (manifests, ranges, Lance's session),
    /// so every read after GC goes to the store.
    async fn cold_ctx(&self) -> CollectionContext {
        common::context(
            &self.f.meta.client,
            &self.f.store,
            self.f.ctx.config.clone(),
        )
        .await
    }

    /// Opens `version` through a [cold context](Self::cold_ctx).
    async fn open_version(&self, version: u64) -> Result<CollectionSnapshot, CollectionError> {
        let ctx = self.cold_ctx().await;
        CollectionSnapshot::open_version(&ctx, self.f.ns, self.f.cid, version).await
    }

    /// The live collection matches the stream's fold, read through a
    /// [cold context](Self::cold_ctx).
    async fn verify(&self) {
        let ctx = self.cold_ctx().await;
        let expected = self.f.expected().await;
        let problems = operon_collection::verify_collection(&ctx, self.f.ns, self.f.cid, &expected)
            .await
            .expect("verify");
        assert!(problems.is_empty(), "{problems:#?}");
    }

    async fn shutdown(self) {
        drop(self.link);
        self.f.shutdown().await;
    }
}

/// Every retained version opens and reads what it read when it was live.
async fn assert_versions_read(env: &Env, versions: &BTreeMap<u64, Vec<StoredDoc>>) {
    let retained = env.retained().await;
    assert!(!retained.is_empty());
    for (version, _) in &retained {
        let expected = versions.get(version).expect("a recorded version");
        let snapshot = env
            .open_version(*version)
            .await
            .unwrap_or_else(|err| panic!("open version {version}: {err}"));
        assert_eq!(
            &snapshot.scan_all().await.expect("scan"),
            expected,
            "version {version}"
        );
        for split in snapshot.splits() {
            snapshot.open_split(split).await.expect("open split");
            snapshot.deleted_docs(split).await.expect("delete bitmap");
        }
    }
}

#[tokio::test]
async fn live_collection_objects_survive_gc() {
    let env = Env::start(CollectionConfig {
        keep_manifests: 2,
        time_travel_retention: Duration::ZERO,
        pk_index_min_unindexed_rows: 5,
        index_poll_interval: Duration::ZERO,
        ..CollectionConfig::default()
    })
    .await;
    let mut versions = BTreeMap::new();
    let mut record = |(version, docs): (u64, Vec<StoredDoc>)| versions.insert(version, docs);
    record(env.commit(upserts(0..10, "a")).await);
    record(env.commit(upserts(5..15, "b")).await);
    record(
        env.commit((0..3).map(|n| DocOp::Delete(key(n))).collect())
            .await,
    );
    // The `_pk` BTREE, committed as its own manifest.
    let index = IndexBuildSource::new(env.f.ctx.clone());
    let results = run_once(&env.f.meta.client, "indexer", WAIT, &index)
        .await
        .expect("index run");
    assert!(
        matches!(results.as_slice(), [(_, RunResult::Ran(Ok(_)))]),
        "{results:?}"
    );
    let (version, docs) = env.live().await;
    assert_eq!(
        env.f.manifest().await.kind,
        operon_collection::CommitKind::IndexBuild
    );
    record((version, docs));
    let mut ops = upserts(20..25, "c");
    ops.push(DocOp::Delete(key(10)));
    record(env.commit(ops).await);
    record(
        env.commit(
            (11..14)
                .map(|n| patch(key(n), json!({ "tag": "patched" })))
                .collect(),
        )
        .await,
    );
    assert_eq!(versions.len(), 6);

    let mut deleted = 0;
    for _ in 0..3 {
        env.past_grace();
        deleted += env.gc().await.orphan_other;
    }
    assert!(deleted > 0, "the manifests beyond retention were collected");
    env.verify().await;
    assert_versions_read(&env, &versions).await;
    // The `_pk` index still serves lookups.
    let live = CollectionSnapshot::open(
        &env.cold_ctx().await,
        env.f.ns,
        env.f.cid,
        Consistency::Linearizable,
    )
    .await
    .expect("open");
    let found = live.get_by_pk(&[key(20), key(0)]).await.expect("get");
    assert!(found[0].is_some() && found[1].is_none());
    env.shutdown().await;
}

async fn wait_for(what: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Runs the link with a factory whose first commit holds forever at
/// `step`, and drops the task there.
async fn crash_at(env: &Env, step: CollectionCommitStep) {
    let reached = Arc::new(AtomicBool::new(false));
    let seen = Arc::new(AtomicU32::new(0));
    let flag = reached.clone();
    let hook: CollectionCommitHook = Arc::new(move |at, _fence| {
        if at == step && seen.fetch_add(1, Ordering::SeqCst) == 0 {
            flag.store(true, Ordering::SeqCst);
            futures::future::pending::<()>().boxed()
        } else {
            futures::future::ready(()).boxed()
        }
    });
    let source = env.f.source(env.f.hooked_factory(hook));
    let meta = env.f.meta.client.clone();
    let crashed = tokio::spawn(async move {
        loop {
            run_once(&meta, "crashed", Duration::from_millis(300), &source)
                .await
                .expect("run");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    wait_for("the crash point", || reached.load(Ordering::SeqCst)).await;
    crashed.abort();
    let _ = crashed.await;
    // Let the crashed run's lease expire.
    env.clock.advance(Duration::from_secs(1));
}

#[tokio::test]
async fn unreachable_collection_objects_are_deleted_after_grace() {
    let env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..10, "a")).await;
    // Replacements and deletes, so the commit writes deletion files and
    // delete bitmaps too.
    let mut ops = upserts(5..15, "b");
    ops.extend((0..2).map(|n| DocOp::Delete(key(n))));
    env.f.write(ops).await;
    let before = env.objects(&env.prefix()).await;
    crash_at(&env, CollectionCommitStep::AfterManifestPut).await;
    let crashed: BTreeSet<String> = env
        .objects(&env.prefix())
        .await
        .difference(&before)
        .cloned()
        .collect();
    let has = |part: &str| crashed.iter().any(|path| path.contains(part));
    for part in [
        "/text/splits/",
        "/manifests/",
        "/pkdelta/",
        "/lance/data/",
        "/lance/_versions/d",
    ] {
        assert!(
            has(part),
            "the crashed commit wrote no {part}: {crashed:#?}"
        );
    }
    // A fresh run commits the batch with objects of its own.
    env.f
        .apply_all(env.link.as_ref().expect("link"), "linker")
        .await;
    env.verify().await;
    let referenced: BTreeSet<String> = env
        .objects(&env.prefix())
        .await
        .difference(&crashed)
        .cloned()
        .collect();

    // Young: kept.
    assert_eq!(env.gc().await.orphan_other, 0);
    for path in &crashed {
        assert!(
            env.exists(path).await,
            "{path} was deleted before the grace"
        );
    }
    env.past_grace();
    // The crashed manifest goes first, what it references one run later.
    let first = env.gc().await.orphan_other;
    for path in &crashed {
        let is_manifest = path.contains("/manifests/");
        assert_eq!(
            env.exists(path).await,
            !is_manifest,
            "{path} after the first run"
        );
    }
    let second = env.gc().await.orphan_other;
    assert_eq!((first + second) as usize, crashed.len());
    for path in &crashed {
        assert!(!env.exists(path).await, "{path} was not deleted");
    }
    for path in &referenced {
        assert!(env.exists(path).await, "{path} was deleted");
    }
    env.verify().await;
    env.shutdown().await;
}

#[tokio::test]
async fn manifests_beyond_keep_and_retention_are_collected() {
    let env = Env::start(CollectionConfig {
        keep_manifests: 2,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    })
    .await;
    let mut last = 0;
    for round in 0..5u64 {
        // Replacements, so every commit writes a PK delta.
        (last, _) = env.commit(upserts(0..8, &format!("r{round}"))).await;
    }
    env.past_grace();
    env.gc().await;
    let manifests = env.objects(&format!("{}manifests/", env.prefix())).await;
    let kept: BTreeSet<u64> = manifests
        .iter()
        .map(|path| operon_collection::manifest_version(path).expect("a manifest"))
        .collect();
    assert_eq!(kept, (last - 2..=last).collect(), "{manifests:#?}");
    // The released manifests' PK deltas go one run after them.
    let deltas = env.objects(&format!("{}pkdelta/", env.prefix())).await;
    assert_eq!(deltas.len(), 5, "{deltas:#?}");
    env.gc().await;
    let deltas = env.objects(&format!("{}pkdelta/", env.prefix())).await;
    assert_eq!(deltas.len(), 3, "{deltas:#?}");
    for version in 1..last - 2 {
        assert!(matches!(
            env.open_version(version).await,
            Err(CollectionError::ManifestGone(v)) if v == version
        ));
    }
    env.verify().await;
    env.shutdown().await;
}

#[tokio::test]
async fn a_pinned_manifest_within_retention_stays_readable() {
    let env = Env::start(CollectionConfig {
        keep_manifests: 2,
        time_travel_retention: Duration::from_secs(3_600),
        ..CollectionConfig::default()
    })
    .await;
    let mut versions = BTreeMap::new();
    for round in 0..10u64 {
        let (version, docs) = env
            .commit(upserts(round..round + 5, &format!("r{round}")))
            .await;
        versions.insert(version, docs);
    }
    env.past_grace();
    env.gc().await;
    env.gc().await;
    let pinned = env.open_version(3).await.expect("version 3 is retained");
    assert_eq!(&pinned.scan_all().await.expect("scan"), &versions[&3]);
    assert_versions_read(&env, &versions).await;
    env.shutdown().await;
}

/// Every version up to `live` either opens and reads exactly what it read
/// when it was live, or is `ManifestGone`; never readable but broken.
async fn assert_no_broken_version(
    env: &Env,
    config: &CollectionConfig,
    versions: &BTreeMap<u64, Vec<StoredDoc>>,
) -> Vec<u64> {
    let mut readable = Vec::new();
    for (&version, docs) in versions {
        let ctx = CollectionContext {
            config: config.clone(),
            ..env.cold_ctx().await
        };
        match CollectionSnapshot::open_version(&ctx, env.f.ns, env.f.cid, version).await {
            Ok(snapshot) => {
                let read = snapshot
                    .scan_all()
                    .await
                    .unwrap_or_else(|err| panic!("version {version} opens but reads {err}"));
                assert_eq!(&read, docs, "version {version}");
                for split in snapshot.splits() {
                    snapshot.open_split(split).await.expect("open split");
                    snapshot.deleted_docs(split).await.expect("delete bitmap");
                }
                readable.push(version);
            }
            Err(CollectionError::ManifestGone(v)) if v == version => {}
            Err(err) => panic!("version {version}: {err}"),
        }
    }
    readable
}

/// Review P30: a manifest that leaves retention goes first and its objects
/// one run later, so a manifest that lingers (here: its delete "failed",
/// simulated by putting it back) is never readable but broken, even after
/// a retention increase walks back into it.
#[tokio::test]
async fn a_released_manifests_objects_go_one_run_after_it() {
    let env = Env::start(CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    })
    .await;
    let mut versions = BTreeMap::new();
    let mut manifests = BTreeMap::new();
    let batches = [
        upserts(0..10, "a"),
        upserts(0..1, "b"),
        upserts(1..2, "b"),
        upserts(20..22, "c"),
        upserts(22..24, "c"),
    ];
    for ops in batches {
        let (version, docs) = env.commit(ops).await;
        versions.insert(version, docs);
        let snapshot = env.f.snapshot().await;
        let path = snapshot.manifest_path().expect("a manifest").to_string();
        let (bytes, _) = env.f.store.get(&path).await.expect("get");
        manifests.insert(version, (path, snapshot.manifest().clone(), bytes));
    }
    // v5 and v4 are retained; v1..v3 are not. v2's PK delta and its Lance
    // deletion file are referenced by v2 alone.
    let (_, v2, _) = &manifests[&2];
    let v2_delta = v2.pk_delta.clone().expect("v2 replaced a key");
    let v2_deletion = deletion_file(&env, v2.lance_version).await;
    let (v3_path, v3, v3_bytes) = manifests[&3].clone();
    let v3_delta = v3.pk_delta.clone().expect("v3 replaced a key");
    let wider = CollectionConfig {
        keep_manifests: 10,
        time_travel_retention: Duration::from_secs(3_600),
        ..env.f.ctx.config.clone()
    };

    env.past_grace();
    env.gc().await;
    for version in 1..=3 {
        assert!(
            !env.exists(&manifests[&version].0).await,
            "manifest v{version}"
        );
    }
    for path in [&v2_delta, &v2_deletion, &v3_delta] {
        assert!(env.exists(path).await, "{path} went with its manifest");
    }
    // v3's delete "failed": it lingers. A wider retention walks back into
    // it and reads it whole; v1 and v2 are gone.
    env.f.store.put(&v3_path, v3_bytes).await.expect("put back");
    assert_eq!(
        assert_no_broken_version(&env, &wider, &versions).await,
        vec![3, 4, 5]
    );

    // v2's objects go now; v3 is listed again, so its objects stay while it
    // goes.
    env.gc().await;
    assert!(!env.exists(&v2_delta).await && !env.exists(&v2_deletion).await);
    assert!(!env.exists(&v3_path).await);
    assert!(env.exists(&v3_delta).await);
    assert_eq!(
        assert_no_broken_version(&env, &wider, &versions).await,
        vec![4, 5]
    );
    env.gc().await;
    assert!(!env.exists(&v3_delta).await);
    assert_eq!(
        assert_no_broken_version(&env, &wider, &versions).await,
        vec![4, 5]
    );
    env.verify().await;
    env.shutdown().await;
}

/// A collection whose objects GC cannot read (here: its pointer names a
/// missing manifest) is kept whole, and GC goes on with the others.
#[tokio::test]
async fn an_unreadable_collection_is_kept_whole() {
    let env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..4, "a")).await;
    let client = &env.f.meta.client;
    let (bad, _, _) = client
        .create_collection(env.f.ns, "broken", tagged(), 1)
        .await
        .expect("create");
    let bad_prefix = collection_prefix(env.f.ns, bad);
    let missing = operon_collection::manifest_path(env.f.ns, bad, 1, ulid_at(1));
    client
        .cas_pointer(
            env.f.ns,
            &operon_meta::collection_pointer_key(bad),
            None,
            &missing,
            None,
        )
        .await
        .expect("point at a missing manifest");
    let bad_orphan = format!("{bad_prefix}text/splits/{}.split", ulid_at(1));
    let good_orphan = format!("{}text/splits/{}.split", env.prefix(), ulid_at(1));
    for path in [&bad_orphan, &good_orphan] {
        env.f
            .store
            .put(path, Bytes::from_static(b"x"))
            .await
            .expect("put");
    }
    env.past_grace();
    assert_eq!(env.gc().await.orphan_other, 1);
    assert!(
        env.exists(&bad_orphan).await,
        "the unreadable collection lost an object"
    );
    assert!(
        !env.exists(&good_orphan).await,
        "the readable collection was not collected"
    );
    // Review P38: the keep is the answer of one call, so overlapping runs
    // on one roots instance each see the unreadable collection.
    let roots = CollectionGcRoots::new(env.f.ctx.clone());
    let (store, ns) = (&env.f.store, env.f.ns);
    let (a, b) = tokio::join!(
        roots.reachable(client, store, ns, 0),
        roots.reachable(client, store, ns, 0)
    );
    for keep in [a.expect("first pass"), b.expect("second pass")] {
        assert!(keep.prefixes.contains(&bad_prefix), "{:?}", keep.prefixes);
        assert!(
            !keep.prefixes.contains(&env.prefix()),
            "{:?}",
            keep.prefixes
        );
    }
    env.verify().await;
    env.shutdown().await;
}

/// The deletion file of fragment 0 in Lance version `version`.
async fn deletion_file(env: &Env, version: u64) -> String {
    let dataset = env
        .f
        .ctx
        .lance
        .open(env.f.ns, env.f.cid, version)
        .await
        .expect("open");
    let fragment = dataset
        .manifest
        .fragments
        .iter()
        .find(|f| f.id == 0)
        .expect("fragment 0");
    let deletion = fragment.deletion_file.as_ref().expect("a deletion file");
    format!(
        "{}{}",
        env.lance(),
        lance_table::io::deletion::relative_deletion_file_path(fragment.id, deletion)
    )
}

#[tokio::test]
async fn lance_files_of_retained_versions_are_kept_and_others_deleted() {
    let env = Env::start(CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    })
    .await;
    env.commit(upserts(0..10, "a")).await;
    // Replacing key 0 writes fragment 0's first deletion file.
    env.commit(upserts(0..1, "b")).await;
    let old = deletion_file(&env, env.f.manifest().await.lance_version).await;
    // Replacing key 1 rewrites it.
    env.commit(upserts(1..2, "b")).await;
    let new = deletion_file(&env, env.f.manifest().await.lance_version).await;
    assert_ne!(old, new);
    // Two more commits: only the last two manifests stay retained, and
    // both name the new deletion file.
    env.commit(upserts(20..22, "c")).await;
    env.commit(upserts(22..24, "c")).await;
    assert!(env.exists(&old).await);
    env.past_grace();
    // The manifests that name it go first, then it.
    env.gc().await;
    assert!(env.exists(&old).await, "{old} went with its manifests");
    env.gc().await;
    assert!(!env.exists(&old).await, "{old} was not deleted");
    assert!(env.exists(&new).await, "{new} was deleted");
    env.verify().await;
    env.shutdown().await;
}

#[tokio::test]
async fn unknown_lance_files_are_never_deleted() {
    let env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..4, "a")).await;
    let unknown: Vec<String> = [
        "_refs/x",
        "_versions/.tmp-orphan.manifest",
        "data/0101/blob.blob",
        "_indices/not-a-uuid/index.idx",
    ]
    .iter()
    .map(|name| format!("{}{name}", env.lance()))
    .collect();
    for path in &unknown {
        env.f
            .store
            .put(path, Bytes::from_static(b"x"))
            .await
            .expect("put");
    }
    env.past_grace();
    env.gc().await;
    env.clock.advance(Duration::from_secs(7_200));
    env.gc().await;
    for path in &unknown {
        assert!(env.exists(path).await, "{path} was deleted");
    }
    env.verify().await;
    env.shutdown().await;
}

/// Lance names carry no ULID (one may parse as one, as here): Lance files
/// are aged by their modification time.
#[tokio::test]
async fn lance_files_are_aged_by_last_modified() {
    let env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..4, "a")).await;
    let orphans: Vec<String> = [
        format!("data/{}.lance", ulid_at(1)),
        format!("_indices/{}/index.idx", uuid::Uuid::new_v4()),
        "_versions/d9223372036854775809.manifest".to_string(),
        format!("_deletions/0-1-{}.arrow", ulid_at(1)),
        format!("_transactions/1-{}.txn", ulid_at(1)),
    ]
    .iter()
    .map(|name| format!("{}{name}", env.lance()))
    .collect();
    for path in &orphans {
        env.f
            .store
            .put(path, Bytes::from_static(b"x"))
            .await
            .expect("put");
    }
    // Their names say 1970; their modification time says now.
    assert_eq!(env.gc().await.orphan_other, 0);
    for path in &orphans {
        assert!(env.exists(path).await, "{path} was deleted while young");
    }
    env.past_grace();
    assert_eq!(env.gc().await.orphan_other as usize, orphans.len());
    for path in &orphans {
        assert!(!env.exists(path).await, "{path} was not deleted");
    }
    env.verify().await;
    env.shutdown().await;
}

#[tokio::test]
async fn a_dropped_collection_is_deleted_only_after_grace() {
    let mut env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..10, "a")).await;
    env.commit(upserts(5..15, "b")).await;
    // No task of the dropped collection runs any more.
    env.link = None;
    // Everything is already older than the grace when the collection is
    // dropped: only the drop's own grace protects it.
    env.past_grace();
    let snapshot = env.f.snapshot().await;
    let docs = snapshot.scan_all().await.expect("scan");
    let (prefix, pk_prefix) = (env.prefix(), env.pk_prefix());
    let (objects, pk_objects) = (env.objects(&prefix).await, env.objects(&pk_prefix).await);
    assert!(!objects.is_empty() && !pk_objects.is_empty());
    env.f
        .meta
        .client
        .drop_collection(env.f.ns, "docs")
        .await
        .expect("drop")
        .expect("dropped");
    let retired = env.retired().await;
    assert!(retired.contains(&prefix) && retired.contains(&pk_prefix));

    // Before the grace: nothing under either prefix is deleted, and the
    // snapshot still reads every document.
    env.clock.advance(GRACE / 2);
    env.gc().await;
    assert_eq!(env.objects(&prefix).await, objects);
    assert_eq!(env.objects(&pk_prefix).await, pk_objects);
    assert_eq!(snapshot.scan_all().await.expect("scan"), docs);
    // And so does the same version read with no warm cache.
    let cold = CollectionSnapshot::at(
        &env.cold_ctx().await,
        env.f.ns,
        snapshot.collection().clone(),
        snapshot.manifest_path().map(str::to_string),
        Arc::new(snapshot.manifest().clone()),
    )
    .await
    .expect("open the dropped version");
    assert_eq!(cold.scan_all().await.expect("scan"), docs);
    for split in cold.splits() {
        cold.open_split(split).await.expect("open split");
        cold.deleted_docs(split).await.expect("delete bitmap");
    }

    env.past_grace();
    env.gc().await;
    assert!(env.objects(&prefix).await.is_empty());
    assert!(env.objects(&pk_prefix).await.is_empty());
    env.gc().await;
    let retired = env.retired().await;
    assert!(
        !retired.contains(&prefix) && !retired.contains(&pk_prefix),
        "{retired:?}"
    );
    env.shutdown().await;
}

#[tokio::test]
async fn a_live_pk_index_is_never_touched() {
    let mut env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..10, "a")).await;
    env.commit(upserts(5..15, "b")).await;
    // Close the PK index writer, so SlateDB changes nothing on its own.
    env.link = None;
    let pk_objects = env.objects(&env.pk_prefix()).await;
    assert!(!pk_objects.is_empty());
    for _ in 0..2 {
        env.past_grace();
        env.gc().await;
    }
    env.clock.advance(Duration::from_secs(7_200));
    env.gc().await;
    assert_eq!(env.objects(&env.pk_prefix()).await, pk_objects);
    env.link = Some(env.f.source(env.f.factory()));
    env.commit(upserts(15..18, "c")).await;
    env.verify().await;
    env.shutdown().await;
}

#[tokio::test]
async fn orphan_pk_prefixes_are_deleted_after_grace() {
    let mut env = Env::start(CollectionConfig::default()).await;
    env.commit(upserts(0..10, "a")).await;
    env.link = None;
    let pk_objects = env.objects(&env.pk_prefix()).await;
    let orphans: Vec<String> = [
        "pk/collection-999/manifest/00000000000000000001.manifest",
        "pk/collection-999/wal/00000000000000000001.sst",
        "pk/stray",
    ]
    .iter()
    .map(|name| format!("ns/{}/{name}", env.f.ns))
    .collect();
    for path in &orphans {
        env.f
            .store
            .put(path, Bytes::from_static(b"x"))
            .await
            .expect("put");
    }
    env.gc().await;
    for path in &orphans {
        assert!(env.exists(path).await, "{path} was deleted while young");
    }
    env.past_grace();
    env.gc().await;
    for path in &orphans {
        assert!(!env.exists(path).await, "{path} was not deleted");
    }
    assert_eq!(env.objects(&env.pk_prefix()).await, pk_objects);
    env.shutdown().await;
}

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One committed version, as the test saw it.
struct Seen {
    created_at_ms: u64,
    docs: Vec<StoredDoc>,
}

/// Seeded interleavings of commits, GC runs, clock steps, pinned reads and
/// one drop and re-create. A pinned read either reads exactly what its
/// version read when it was live, or fails with `ManifestGone`, and only
/// for a version the retention rule may have released: one more than
/// `keep_manifests` behind the live version whose child is older than the
/// retention.
#[tokio::test]
async fn gc_racing_commits_and_reads_never_breaks_a_read() {
    const KEEP: usize = 2;
    const RETENTION: Duration = Duration::from_secs(30);
    let (mut total_reads, mut total_gone) = (0, 0);
    for seed in 0..16u64 {
        let mut rng = Rng(seed);
        let mut env = Env::start(CollectionConfig {
            keep_manifests: KEEP,
            time_travel_retention: RETENTION,
            ..CollectionConfig::default()
        })
        .await;
        let mut seen: BTreeMap<u64, Seen> = BTreeMap::new();
        let drop_at = 6 + rng.below(12);
        let (mut reads, mut gone) = (0, 0);
        for step in 0..24u64 {
            if step == drop_at {
                env.f
                    .meta
                    .client
                    .drop_collection(env.f.ns, "docs")
                    .await
                    .expect("drop");
                let (cid, stream, link) = env
                    .f
                    .meta
                    .client
                    .create_collection(env.f.ns, "docs", tagged(), 2)
                    .await
                    .expect("re-create");
                (env.f.cid, env.f.stream, env.f.link) = (cid, stream, link);
                seen.clear();
                continue;
            }
            match rng.below(10) {
                0..=3 => {
                    let ops = (0..1 + rng.below(6))
                        .map(|_| {
                            let n = rng.below(20);
                            if rng.below(4) == 0 {
                                DocOp::Delete(key(n))
                            } else {
                                upsert(n, json!({ "n": n, "tag": format!("s{step}") }))
                            }
                        })
                        .collect();
                    let (version, docs) = env.commit(ops).await;
                    let created_at_ms = env.f.manifest().await.created_at_ms;
                    seen.insert(
                        version,
                        Seen {
                            created_at_ms,
                            docs,
                        },
                    );
                }
                4..=5 => {
                    env.gc().await;
                }
                6..=7 => env.clock.advance(Duration::from_secs(5 + rng.below(40))),
                _ => {
                    let Some(&live) = seen.keys().next_back() else {
                        continue;
                    };
                    for _ in 0..3 {
                        let version = 1 + rng.below(live);
                        reads += 1;
                        let may_be_gone = live - version > KEEP as u64
                            && seen[&(version + 1)].created_at_ms + millis(RETENTION)
                                <= env.clock.now_ms();
                        match env.open_version(version).await {
                            Ok(snapshot) => {
                                let docs = snapshot.scan_all().await.unwrap_or_else(|err| {
                                    panic!("seed {seed}: version {version} opened but reads {err}")
                                });
                                assert_eq!(docs, seen[&version].docs, "seed {seed} v{version}");
                            }
                            Err(CollectionError::ManifestGone(v))
                                if v == version && may_be_gone =>
                            {
                                gone += 1;
                            }
                            Err(err) => panic!("seed {seed}: version {version} of {live}: {err}"),
                        }
                    }
                }
            }
        }
        if !seen.is_empty() {
            env.verify().await;
        }
        (total_reads, total_gone) = (total_reads + reads, total_gone + gone);
        env.shutdown().await;
    }
    // The interleavings both read retained versions and see released ones.
    assert!(total_reads > 50, "{total_reads} pinned reads");
    assert!(total_gone > 0, "no pinned read found its version released");
}

// ---- Trimming (Ruling 12) ----

/// Log start offsets of the implicit stream's partitions.
async fn log_starts(env: &Env) -> Vec<u64> {
    let (stream, partitions) = (env.f.stream, env.f.partitions);
    env.f
        .meta
        .client
        .read(Consistency::Linearizable, |s| {
            (0..partitions)
                .map(|p| s.partition(stream, p).map_or(0, |ps| ps.log_start_offset()))
                .collect()
        })
        .await
        .expect("read")
}

async fn fetch(env: &Env, stream: StreamId, partition: u32, offset: u64) -> Result<usize, String> {
    env.f
        .reader
        .fetch(operon_log::FetchRequest {
            stream,
            partition,
            offset,
            max_bytes: 16 << 20,
            max_wait: Duration::ZERO,
        })
        .await
        .map(|response| response.records.len())
        .map_err(|err| err.to_string())
}

fn trim_config(trim: bool) -> CollectionConfig {
    CollectionConfig {
        keep_manifests: 2,
        time_travel_retention: Duration::ZERO,
        trim,
        trim_interval: Duration::ZERO,
        ..CollectionConfig::default()
    }
}

/// The outcome of the one trim task a pass of `source` runs.
async fn trim_once(env: &Env, source: &CollectionTrimSource) -> Result<TaskOutcome, TaskError> {
    let results = run_once(&env.f.meta.client, "trimmer", WAIT, source)
        .await
        .expect("run");
    let [(task, RunResult::Ran(result))] = <[_; 1]>::try_from(results).expect("one task") else {
        panic!("the trim task did not run");
    };
    assert_eq!(task.key, format!("{TRIM_TASK_PREFIX}{}", env.f.cid));
    result
}

#[tokio::test]
async fn trim_keeps_the_tail_of_the_oldest_retained_manifest() {
    let env = Env::start(trim_config(true)).await;
    let mut applied = BTreeMap::new();
    for round in 0..5u64 {
        let (version, _) = env
            .commit(upserts(round * 4..round * 4 + 6, &format!("r{round}")))
            .await;
        applied.insert(version, env.f.applied().await);
    }
    let live = *applied.keys().next_back().expect("a version");
    let oldest = &applied[&(live - 2)];
    assert_eq!(oldest.len(), 2, "both partitions have records: {oldest:?}");
    env.clock.advance(Duration::from_secs(1));
    let source = CollectionTrimSource::new(env.f.ctx.clone());
    assert_eq!(
        trim_once(&env, &source).await.expect("trim"),
        TaskOutcome::Done
    );
    let starts = log_starts(&env).await;
    let high = env.f.high_watermarks().await;
    for partition in 0..env.f.partitions {
        let bound = oldest[&partition];
        assert_eq!(starts[partition as usize], bound, "partition {partition}");
        // The whole tail of manifest v−2 reads; below it, nothing does.
        let tail = fetch(&env, env.f.stream, partition, bound).await;
        let expected = usize::try_from(high[&partition] - bound).expect("fits");
        assert_eq!(tail.expect("the tail reads"), expected);
        assert!(
            fetch(&env, env.f.stream, partition, bound - 1)
                .await
                .is_err()
        );
    }
    // Nothing more to trim until the retained set moves.
    assert_eq!(
        trim_once(&env, &source).await.expect("trim"),
        TaskOutcome::Idle
    );
    let pinned = env.open_version(live - 2).await.expect("v-2 is retained");
    assert_eq!(pinned.manifest().applied, *oldest);
    env.shutdown().await;
}

#[tokio::test]
async fn trim_is_off_when_configured() {
    let env = Env::start(trim_config(false)).await;
    for round in 0..5u64 {
        env.commit(upserts(round * 4..round * 4 + 6, "x")).await;
    }
    env.clock.advance(Duration::from_secs(1));
    let source = CollectionTrimSource::new(env.f.ctx.clone());
    assert_eq!(
        trim_once(&env, &source).await.expect("trim"),
        TaskOutcome::Idle
    );
    assert_eq!(log_starts(&env).await, vec![0, 0]);
    env.shutdown().await;
}

#[tokio::test]
async fn a_fenced_trim_changes_nothing() {
    let env = Env::start(trim_config(true)).await;
    for round in 0..5u64 {
        env.commit(upserts(round * 4..round * 4 + 6, "x")).await;
    }
    let source = CollectionTrimSource::new(env.f.ctx.clone());
    let [(key, task)] = <[_; 1]>::try_from(
        source
            .candidates(&env.f.meta.client)
            .await
            .expect("candidates"),
    )
    .map_err(|c| c.len())
    .expect("one candidate");
    let lease = key.lease();
    let client = &env.f.meta.client;
    let stale = client
        .acquire_lease(&lease, "a", Duration::from_secs(1))
        .await
        .expect("lease");
    env.clock.advance(Duration::from_secs(2));
    let current = client
        .acquire_lease(&lease, "b", Duration::from_secs(30))
        .await
        .expect("take over");
    assert!(current.epoch > stale.epoch);
    let ctx = TaskContext {
        key,
        fence: Fence {
            lease,
            epoch: stale.epoch,
        },
        cancel: CancellationToken::new(),
        meta: client.clone(),
    };
    assert!(matches!(task.run(ctx).await, Err(TaskError::Fenced)));
    assert_eq!(log_starts(&env).await, vec![0, 0]);
    env.shutdown().await;
}
