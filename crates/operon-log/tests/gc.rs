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
