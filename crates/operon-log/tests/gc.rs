//! Garbage collection of retired and orphaned log objects.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{Meta, fast_config, faulty_store, read_direct, records, segment_now};
use operon_log::LogWriter;
use operon_log::gc::{GcConfig, GcReport, GcSource};
use operon_meta::{Clock, Consistency, ManualClock, MetaClientConfig, SystemClock};
use operon_store::{Fault, Op, Store, StoreError};
use ulid::Ulid;

const GRACE: Duration = Duration::from_secs(60);

fn config() -> GcConfig {
    GcConfig {
        grace: GRACE,
        ..GcConfig::default()
    }
}

struct Fixture {
    clock: Arc<ManualClock>,
    meta: Meta,
    store: Store,
    writer: LogWriter,
    ns: operon_common::NamespaceId,
    stream: operon_common::StreamId,
}

impl Fixture {
    async fn start(store: Store) -> Self {
        // Start at real time, so ULIDs made from either clock agree.
        let clock = Arc::new(ManualClock::new(SystemClock.now_ms()));
        let meta = Meta::start_with(clock.clone(), MetaClientConfig::default()).await;
        let (ns, stream) = meta.stream("acme", "events", 1).await;
        let writer = LogWriter::start(meta.client.clone(), store.clone(), fast_config())
            .expect("start writer");
        Self {
            clock,
            meta,
            store,
            writer,
            ns,
            stream,
        }
    }

    async fn retired(&self) -> Vec<String> {
        self.meta
            .client
            .read(Consistency::Local, |s| {
                s.retired().map(|(p, _)| p.to_string()).collect()
            })
            .await
            .expect("read")
    }

    async fn gc(&self, source: &GcSource) -> GcReport {
        source
            .run_once(&self.meta.client, "gc-a")
            .await
            .expect("gc")
            .expect("lease")
    }

    async fn exists(&self, path: &str) -> bool {
        match self.store.head(path).await {
            Ok(_) => true,
            Err(StoreError::NotFound { .. }) => false,
            Err(err) => panic!("head {path}: {err}"),
        }
    }

    async fn check(&self) {
        let violations = self
            .meta
            .client
            .read(Consistency::Local, |s| s.check_invariants())
            .await
            .expect("read");
        assert!(violations.is_empty(), "{violations:?}");
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.meta.shutdown().await;
    }
}

#[tokio::test]
async fn retired_objects_are_deleted_after_the_grace_period_and_forgotten() {
    let f = Fixture::start(Store::in_memory()).await;
    f.writer.append(f.stream, 0, records("a", 3)).await.unwrap();
    f.writer.append(f.stream, 0, records("b", 3)).await.unwrap();
    segment_now(&f.meta.client, &f.store, f.ns, f.stream, 0, 10)
        .await
        .expect("segment");
    let retired = f.retired().await;
    assert_eq!(retired.len(), 2, "both WAL objects are retired");
    let source = GcSource::new(f.store.clone(), config());

    // Younger than the grace period: kept.
    assert_eq!(f.gc(&source).await, GcReport::default());
    for path in &retired {
        assert!(f.exists(path).await);
    }
    f.clock.advance(GRACE + Duration::from_secs(1));
    let report = f.gc(&source).await;
    assert_eq!(report.retired, 2);
    for path in &retired {
        assert!(!f.exists(path).await, "{path} was not deleted");
    }
    assert!(f.retired().await.is_empty());
    f.check().await;
    f.shutdown().await;
}

/// A crash (or a lost acknowledgement) between the deletes and
/// `ForgetObjects` leaves deleted objects in the retired set; the next run
/// deletes them again (missing counts as deleted) and forgets them.
#[tokio::test]
async fn a_crash_between_delete_and_forget_reruns_cleanly() {
    let (faults, store) = faulty_store();
    let f = Fixture::start(store).await;
    for tag in ["a", "b", "c"] {
        f.writer.append(f.stream, 0, records(tag, 2)).await.unwrap();
    }
    segment_now(&f.meta.client, &f.store, f.ns, f.stream, 0, 10)
        .await
        .expect("segment");
    let retired = f.retired().await;
    assert_eq!(retired.len(), 3);
    f.clock.advance(GRACE + Duration::from_secs(1));
    // As if a GC run deleted the first object and then crashed.
    f.store.delete(&retired[0]).await.unwrap();
    // The next delete lands but reports failure.
    faults.inject(Op::Delete, Fault::ErrorAfterApply);
    let source = GcSource::new(f.store.clone(), config());
    let report = f.gc(&source).await;
    assert_eq!(report.retired, 2, "the failed delete is not forgotten yet");
    assert_eq!(f.retired().await.len(), 1);
    let report = f.gc(&source).await;
    assert_eq!(report.retired, 1);
    assert!(f.retired().await.is_empty());
    for path in &retired {
        assert!(!f.exists(path).await);
    }
    f.check().await;
    f.shutdown().await;
}

fn ulid_at(ms: u64) -> Ulid {
    Ulid::from_parts(ms, Ulid::generate().random())
}

/// Orphans (a WAL object whose commit never landed, a segment whose swap
/// never landed) are deleted once old enough; young ones and referenced
/// ones never are, however old.
#[tokio::test]
async fn only_old_unreferenced_objects_are_deleted() {
    let f = Fixture::start(Store::in_memory()).await;
    f.writer.append(f.stream, 0, records("a", 3)).await.unwrap();
    f.writer.append(f.stream, 0, records("b", 2)).await.unwrap();
    let live_segment = segment_now(&f.meta.client, &f.store, f.ns, f.stream, 0, 1)
        .await
        .expect("segment");
    let live_wal: Vec<String> = f
        .meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(f.stream, 0)
                .unwrap()
                .entries()
                .filter(|e| e.kind == operon_meta::EntryKind::Wal)
                .map(|e| e.object.clone())
                .collect()
        })
        .await
        .unwrap();
    assert_eq!(live_wal.len(), 1);

    let now = f.clock.now_ms();
    let orphan_wal =
        operon_log::paths::wal_object(operon_meta::WalClass::Standard, 9, ulid_at(now));
    let orphan_segment = operon_log::paths::segment(f.ns, f.stream, 0, 0, ulid_at(now));
    for path in [&orphan_wal, &orphan_segment] {
        f.store
            .put(path, Bytes::from_static(b"orphan"))
            .await
            .unwrap();
    }
    let source = GcSource::new(f.store.clone(), config());
    let untouched = |report: GcReport| report.orphan_wal == 0 && report.orphan_segments == 0;

    // Young: kept.
    assert!(untouched(f.gc(&source).await));
    // Old enough for a segment, not for a WAL object (which could still be
    // committed within the commit window).
    f.clock.advance(GRACE + Duration::from_secs(1));
    let report = f.gc(&source).await;
    assert_eq!((report.orphan_segments, report.orphan_wal), (1, 0));
    assert!(!f.exists(&orphan_segment).await);
    assert!(f.exists(&orphan_wal).await);
    // Past twice the commit window plus the grace period.
    f.clock
        .advance(Duration::from_millis(2 * operon_meta::WAL_COMMIT_WINDOW_MS));
    let report = f.gc(&source).await;
    assert_eq!(report.orphan_wal, 1);
    assert!(!f.exists(&orphan_wal).await);
    // Referenced objects stay, however old.
    assert!(f.exists(&live_segment).await);
    assert!(f.exists(&live_wal[0]).await);
    // Objects GC does not know stay too.
    f.store
        .put("ns/1/other/thing", Bytes::from_static(b"x"))
        .await
        .unwrap();
    f.clock.advance(Duration::from_secs(7_200));
    f.gc(&source).await;
    assert!(f.exists("ns/1/other/thing").await);
    assert!(f.exists(&live_segment).await);
    f.check().await;
    f.shutdown().await;
}

#[tokio::test]
async fn a_gc_run_skips_when_another_owner_holds_the_lease() {
    let f = Fixture::start(Store::in_memory()).await;
    f.meta
        .client
        .acquire_lease("task/gc", "gc-b", Duration::from_secs(30))
        .await
        .unwrap();
    let source = GcSource::new(f.store.clone(), config());
    assert!(
        source
            .run_once(&f.meta.client, "gc-a")
            .await
            .unwrap()
            .is_none()
    );
    f.shutdown().await;
}

/// M0.4 review I1: a segment PUT whose swap is delayed past the swap deadline
/// (a frozen segmenter, slow retries) while GC deletes the unreferenced
/// segment. The metastore must refuse the late swap, so the index never
/// references the deleted segment and no acknowledged offset is lost.
#[tokio::test]
async fn a_swap_delayed_past_its_deadline_is_refused_after_gc_deleted_the_segment() {
    let f = Fixture::start(Store::in_memory()).await;
    let acked = f.writer.append(f.stream, 0, records("a", 3)).await.unwrap();
    assert_eq!(acked.base_offset, 0);
    let entries: Vec<operon_meta::IndexEntry> = f
        .meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(f.stream, 0)
                .expect("partition")
                .entries()
                .cloned()
                .collect()
        })
        .await
        .expect("read");

    // The segmenter's steps by hand: build and PUT the segment (not late).
    let mut builder =
        operon_log::segment::SegmentBuilder::new(f.stream, 0, 0, operon_log::Encoding::Kafka);
    for entry in &entries {
        let bytes = f
            .store
            .get_range(&entry.object, entry.byte_range.clone())
            .await
            .expect("get");
        for b in operon_log::batch::batches(&bytes) {
            let b = b.expect("batch");
            builder
                .push_batch(
                    Bytes::copy_from_slice(b.bytes),
                    b.record_count,
                    b.max_timestamp_ms,
                )
                .expect("push");
        }
    }
    let (bytes, footer) = builder.finish();
    let written_at = f.clock.now_ms();
    let path = operon_log::paths::segment(f.ns, f.stream, 0, 0, ulid_at(written_at));
    f.store.put_if_absent(&path, bytes).await.expect("put");
    let swap = operon_meta::Command::SwapSegment {
        stream: f.stream,
        partition: 0,
        replaces: entries
            .iter()
            .map(|e| (e.base_offset, e.object.clone()))
            .collect(),
        segment: path.clone(),
        byte_range: footer.data,
        max_timestamp_ms: entries.iter().map(|e| e.max_timestamp_ms).max().unwrap(),
        fence: None,
        now_ms: written_at,
        fresh: operon_meta::Freshness {
            created_at_ms: written_at,
            max_age_ms: 30_000,
        },
    };

    // The stall, then GC deletes the unreferenced segment.
    f.clock.advance(GRACE + Duration::from_secs(1));
    let source = GcSource::new(f.store.clone(), config());
    assert_eq!(f.gc(&source).await.orphan_segments, 1);
    assert!(!f.exists(&path).await);

    // The late swap is refused and changes nothing.
    let err = f.meta.client.write(swap).await.unwrap_err();
    assert!(
        matches!(
            err,
            operon_meta::MetaError::Rejected(operon_meta::ApplyError::StaleObject { .. })
        ),
        "{err:?}"
    );
    assert!(f.retired().await.is_empty());
    f.check().await;

    // Another GC after the grace period deletes nothing live, and every
    // acknowledged record is still readable.
    f.clock.advance(GRACE + Duration::from_secs(1));
    assert_eq!(f.gc(&source).await.retired, 0);
    let read = read_direct(&f.meta.client, &f.store, f.stream, 0).await;
    assert_eq!(
        read.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    f.shutdown().await;
}

/// M0.4 re-review m1: a freshness deadline equal to the grace period is
/// refused, and the error names that deadline (the other one is below grace).
#[test]
fn a_deadline_equal_to_grace_is_rejected() {
    let gc = GcConfig {
        grace: Duration::from_secs(10),
        ..GcConfig::default()
    };
    let err = gc
        .check_deadlines(&[
            ("segmenter.swap_deadline", Duration::from_millis(9_999)),
            ("link.max_commit_delay", Duration::from_secs(10)),
        ])
        .unwrap_err();
    assert_eq!(err.name, "link.max_commit_delay");
    assert_eq!(err.deadline, Duration::from_secs(10));
    assert_eq!(err.grace, Duration::from_secs(10));
    assert_eq!(
        err.to_string(),
        "link.max_commit_delay (10s) must be strictly below gc.grace (10s)"
    );
}

#[test]
fn deadlines_below_grace_pass() {
    let gc = GcConfig {
        grace: Duration::from_secs(10),
        ..GcConfig::default()
    };
    gc.check_deadlines(&[
        ("segmenter.swap_deadline", Duration::from_millis(9_999)),
        ("link.max_commit_delay", Duration::from_millis(9_999)),
    ])
    .unwrap();
}

/// Plan M1.1 Ruling 13: a dropped collection's prefixes are retired; once
/// they are `grace` old, pass 1 deletes everything under them, and forgets
/// each prefix only after a later listing finds it empty.
#[tokio::test]
async fn a_retired_prefix_is_deleted_after_grace_then_forgotten() {
    let f = Fixture::start(Store::in_memory()).await;
    let schema = operon_common::schema::CollectionSchema::new(
        vec![],
        vec![],
        operon_common::schema::DynamicMapping::Ignore,
    );
    let (cid, _, _) = f
        .meta
        .client
        .create_collection(f.ns, "docs", schema, 1)
        .await
        .expect("create collection");
    let prefix = operon_meta::collection_prefix(f.ns, cid);
    let pk_prefix = operon_meta::collection_pk_prefix(f.ns, cid);
    let objects: Vec<String> = [
        "lance/data/a.lance",
        "manifests/x.pb",
        "text/splits/y.split",
    ]
    .iter()
    .map(|name| format!("{prefix}{name}"))
    .collect();
    for path in &objects {
        f.store.put(path, Bytes::from_static(b"x")).await.unwrap();
    }
    f.meta
        .client
        .drop_collection(f.ns, "docs")
        .await
        .expect("drop");
    let retired = f.retired().await;
    assert!(retired.contains(&prefix), "{retired:?}");
    assert!(retired.contains(&pk_prefix), "{retired:?}");
    let source = GcSource::new(f.store.clone(), config());

    // Younger than the grace period: nothing under the prefix is deleted.
    f.gc(&source).await;
    for path in &objects {
        assert!(f.exists(path).await, "{path} was deleted before the grace");
    }
    assert!(f.retired().await.contains(&prefix));

    f.clock.advance(GRACE + Duration::from_secs(1));
    let report = f.gc(&source).await;
    for path in &objects {
        assert!(!f.exists(path).await, "{path} was not deleted");
    }
    assert!(report.retired >= 3, "{report:?}");
    // The listing was not empty in this pass, so the prefix is still
    // retired; the empty pk prefix is forgotten at once.
    let retired = f.retired().await;
    assert!(retired.contains(&prefix), "{retired:?}");
    assert!(!retired.contains(&pk_prefix), "{retired:?}");

    // A later pass lists it empty and forgets it.
    f.gc(&source).await;
    let retired = f.retired().await;
    assert!(!retired.contains(&prefix), "{retired:?}");
    f.check().await;
    f.shutdown().await;
}

/// A root that owns `things/`, reports nothing reachable, keeps
/// `things/keep/`, and optionally dates objects by their modification time.
struct TestRoots {
    kept: Vec<String>,
    by_last_modified: bool,
}

#[async_trait::async_trait]
impl operon_log::gc::GcRoots for TestRoots {
    fn prefix(&self) -> &str {
        "things/"
    }

    async fn reachable(
        &self,
        _meta: &operon_meta::MetaClient,
        _store: &Store,
        _namespace: operon_common::NamespaceId,
        _keep_manifests: usize,
    ) -> Result<operon_log::gc::GcKeep, operon_log::LogError> {
        Ok(operon_log::gc::GcKeep {
            objects: std::collections::BTreeSet::new(),
            prefixes: self.kept.clone(),
        })
    }

    fn object_time_ms(&self, info: &operon_store::ObjectInfo) -> u64 {
        if self.by_last_modified {
            info.last_modified_ms
        } else {
            operon_log::gc::object_time_ms(info)
        }
    }
}

#[tokio::test]
async fn kept_prefixes_are_never_deleted() {
    let f = Fixture::start(Store::in_memory()).await;
    let now = f.clock.now_ms();
    let kept = format!("ns/{}/things/keep/{}.x", f.ns, ulid_at(now));
    let other = format!("ns/{}/things/other/{}.x", f.ns, ulid_at(now));
    for path in [&kept, &other] {
        f.store.put(path, Bytes::from_static(b"x")).await.unwrap();
    }
    let roots = TestRoots {
        kept: vec![format!("ns/{}/things/keep/", f.ns)],
        by_last_modified: false,
    };
    let source = GcSource::with_roots(f.store.clone(), config(), vec![Arc::new(roots)]);
    f.gc(&source).await;
    assert!(f.exists(&kept).await && f.exists(&other).await);
    f.clock.advance(GRACE + Duration::from_secs(1));
    let report = f.gc(&source).await;
    assert_eq!(report.orphan_other, 1);
    assert!(!f.exists(&other).await);
    f.clock.advance(Duration::from_secs(7_200));
    f.gc(&source).await;
    assert!(
        f.exists(&kept).await,
        "an object under a kept prefix was deleted"
    );
    f.shutdown().await;
}

/// A name that looks like an old ULID would be old by the default; a root
/// that dates its objects by modification time keeps it for the grace.
#[tokio::test]
async fn a_roots_object_time_overrides_the_default() {
    let f = Fixture::start(Store::in_memory()).await;
    // Lance names carry no ULID, but one may parse as one.
    let path = format!("ns/{}/things/{}.lance", f.ns, ulid_at(1));
    f.store.put(&path, Bytes::from_static(b"x")).await.unwrap();
    let by_default = GcSource::with_roots(
        f.store.clone(),
        config(),
        vec![Arc::new(TestRoots {
            kept: vec![],
            by_last_modified: false,
        })],
    );
    let by_last_modified = GcSource::with_roots(
        f.store.clone(),
        config(),
        vec![Arc::new(TestRoots {
            kept: vec![],
            by_last_modified: true,
        })],
    );
    assert_eq!(f.gc(&by_last_modified).await.orphan_other, 0);
    assert!(f.exists(&path).await, "a young object was deleted");
    // The default reads the name's ULID time, 1 ms after the epoch.
    assert_eq!(f.gc(&by_default).await.orphan_other, 1);
    assert!(!f.exists(&path).await);

    f.store.put(&path, Bytes::from_static(b"x")).await.unwrap();
    let modified = f.store.head(&path).await.unwrap().last_modified_ms;
    f.clock
        .set(modified.max(f.clock.now_ms()) + millis(GRACE) + 1_000);
    assert_eq!(f.gc(&by_last_modified).await.orphan_other, 1);
    assert!(!f.exists(&path).await);
    f.shutdown().await;
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).expect("millis fit a u64")
}
