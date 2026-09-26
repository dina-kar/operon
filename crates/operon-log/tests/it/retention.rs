//! Retention by age and by size, and WAL-commit pruning.

use std::sync::Arc;
use std::time::Duration;

use crate::common::{Meta, fast_config, records, segment_now};
use bytes::Bytes;
use operon_common::StreamId;
use operon_log::{LogWriter, Record, Retention, RetentionConfig, RetentionReport, RetentionSource};
use operon_meta::{Consistency, ManualClock, MetaClientConfig, WAL_COMMIT_WINDOW_MS};
use operon_store::Store;
use operon_worker::{Worker, WorkerConfig};

const NOW: u64 = 1_700_000_000_000;

fn at(timestamp_ms: u64, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: None,
            value: Some(Bytes::from(vec![b'v'; 10 + i])),
            headers: vec![],
            timestamp_ms: timestamp_ms as i64,
        })
        .collect()
}

struct Fixture {
    clock: Arc<ManualClock>,
    meta: Meta,
    store: Store,
    writer: LogWriter,
    ns: operon_common::NamespaceId,
    stream: StreamId,
}

impl Fixture {
    async fn start() -> Self {
        let clock = Arc::new(ManualClock::new(NOW));
        let meta = Meta::start_with(clock.clone(), MetaClientConfig::default()).await;
        let (ns, stream) = meta.stream("acme", "events", 1).await;
        let store = Store::in_memory();
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

    fn retention(&self) -> Retention {
        Retention::new(
            self.meta.client.clone(),
            "retention-a",
            RetentionConfig::default(),
        )
    }

    async fn log_start(&self) -> u64 {
        self.meta
            .client
            .read(Consistency::Local, |s| {
                s.partition(self.stream, 0)
                    .expect("partition")
                    .log_start_offset()
            })
            .await
            .expect("read")
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

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.meta.shutdown().await;
    }
}

#[tokio::test]
async fn retention_by_age_trims_entries_older_than_the_limit() {
    let f = Fixture::start().await;
    f.writer
        .append(f.stream, 0, at(NOW - 60_000, 2))
        .await
        .unwrap();
    f.writer
        .append(f.stream, 0, at(NOW - 50_000, 3))
        .await
        .unwrap();
    f.writer
        .append(f.stream, 0, at(NOW - 1_000, 4))
        .await
        .unwrap();
    let old_objects: Vec<String> = f
        .meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(f.stream, 0)
                .unwrap()
                .entries()
                .take(2)
                .map(|e| e.object.clone())
                .collect()
        })
        .await
        .unwrap();

    // No policy yet: nothing happens.
    let report = f.retention().run_once().await.unwrap();
    assert_eq!(report.trimmed, 0);
    assert_eq!(f.log_start().await, 0);

    f.meta
        .client
        .set_retention(
            f.stream,
            operon_meta::Retention {
                max_age_ms: Some(30_000),
                max_bytes: None,
            },
        )
        .await
        .unwrap();
    let report = f.retention().run_once().await.unwrap();
    assert_eq!(report.trimmed, 1);
    assert_eq!(f.log_start().await, 5);
    let mut retired = f.retired().await;
    retired.sort();
    let mut expected = old_objects;
    expected.sort();
    assert_eq!(retired, expected);

    // A second run changes nothing; once everything is old, all is trimmed.
    assert_eq!(f.retention().run_once().await.unwrap().trimmed, 0);
    f.clock.advance(Duration::from_secs(60));
    assert_eq!(f.retention().run_once().await.unwrap().trimmed, 1);
    assert_eq!(f.log_start().await, 9);
    f.shutdown().await;
}

#[tokio::test]
async fn retention_by_bytes_drops_whole_oldest_entries() {
    let f = Fixture::start().await;
    for _ in 0..4 {
        f.writer.append(f.stream, 0, at(NOW, 2)).await.unwrap();
    }
    let sizes: Vec<u64> = f
        .meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(f.stream, 0)
                .unwrap()
                .entries()
                .map(|e| e.byte_range.end - e.byte_range.start)
                .collect()
        })
        .await
        .unwrap();
    // Keep the last two entries, and one byte less than three.
    let limit = sizes[1] + sizes[2] + sizes[3] - 1;
    f.meta
        .client
        .set_retention(
            f.stream,
            operon_meta::Retention {
                max_age_ms: None,
                max_bytes: Some(limit),
            },
        )
        .await
        .unwrap();
    assert_eq!(f.retention().run_once().await.unwrap().trimmed, 1);
    assert_eq!(f.log_start().await, 4);
    let bytes = f
        .meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(f.stream, 0).unwrap().bytes()
        })
        .await
        .unwrap();
    assert!(bytes <= limit);
    assert_eq!(bytes, sizes[2] + sizes[3]);
    f.shutdown().await;
}

#[tokio::test]
async fn trimmed_segments_are_retired() {
    let f = Fixture::start().await;
    f.writer
        .append(f.stream, 0, at(NOW - 60_000, 2))
        .await
        .unwrap();
    f.writer
        .append(f.stream, 0, at(NOW - 60_000, 2))
        .await
        .unwrap();
    f.writer.append(f.stream, 0, at(NOW, 2)).await.unwrap();
    let segment = segment_now(&f.meta.client, &f.store, f.ns, f.stream, 0, 2)
        .await
        .unwrap();
    f.meta
        .client
        .set_retention(
            f.stream,
            operon_meta::Retention {
                max_age_ms: Some(30_000),
                max_bytes: None,
            },
        )
        .await
        .unwrap();
    f.retention().run_once().await.unwrap();
    assert_eq!(f.log_start().await, 4);
    assert!(f.retired().await.contains(&segment));
    f.shutdown().await;
}

#[tokio::test]
async fn each_run_prunes_old_wal_commit_records() {
    let f = Fixture::start().await;
    f.writer.append(f.stream, 0, records("x", 1)).await.unwrap();
    let object = f.store.list("wal/").await.unwrap().remove(0).path;
    let recorded = |object: String| {
        let client = f.meta.client.clone();
        async move {
            client
                .read(Consistency::Local, move |s| s.wal_commit(&object).is_some())
                .await
                .unwrap()
        }
    };
    assert_eq!(f.retention().run_once().await.unwrap().pruned, 0);
    assert!(recorded(object.clone()).await);
    f.clock
        .advance(Duration::from_millis(2 * WAL_COMMIT_WINDOW_MS + 1));
    assert_eq!(
        f.retention().run_once().await.unwrap(),
        RetentionReport {
            trimmed: 0,
            pruned: 1,
            skipped: false
        }
    );
    assert!(!recorded(object).await);
    f.shutdown().await;
}

#[tokio::test]
async fn only_one_retention_task_works_at_a_time() {
    let f = Fixture::start().await;
    f.retention().run_once().await.unwrap();
    // Another worker holds the task lease: this one skips the run.
    f.meta
        .client
        .acquire_lease("task/retention", "retention-b", Duration::from_secs(30))
        .await
        .unwrap();
    assert!(f.retention().run_once().await.unwrap().skipped);
    f.shutdown().await;
}

#[tokio::test]
async fn a_worker_runs_retention_until_stopped() {
    let f = Fixture::start().await;
    f.writer
        .append(f.stream, 0, at(NOW - 60_000, 2))
        .await
        .unwrap();
    f.meta
        .client
        .set_retention(
            f.stream,
            operon_meta::Retention {
                max_age_ms: Some(1_000),
                max_bytes: None,
            },
        )
        .await
        .unwrap();
    let mut worker = Worker::new(
        f.meta.client.clone(),
        WorkerConfig {
            poll_interval: Duration::from_millis(20),
            ..WorkerConfig::new("worker-a")
        },
    );
    worker.add_source(Arc::new(RetentionSource::new(RetentionConfig {
        interval: Duration::from_millis(20),
    })));
    let worker = worker.start();
    crate::common::eventually("the task trims", || async { f.log_start().await == 2 }).await;
    worker.stop().await;
    f.shutdown().await;
}

/// Review M4: like Kafka's active segment, the newest entry is kept even when
/// it alone exceeds `max_bytes`, so an acknowledged append stays readable.
#[tokio::test]
async fn retention_by_bytes_keeps_the_newest_entry() {
    let f = Fixture::start().await;
    f.meta
        .client
        .set_retention(
            f.stream,
            operon_meta::Retention {
                max_age_ms: None,
                max_bytes: Some(100),
            },
        )
        .await
        .unwrap();
    f.writer.append(f.stream, 0, at(NOW, 20)).await.unwrap();
    assert_eq!(f.retention().run_once().await.unwrap().trimmed, 0);
    assert_eq!(f.log_start().await, 0);
    f.writer.append(f.stream, 0, at(NOW, 20)).await.unwrap();
    assert_eq!(f.retention().run_once().await.unwrap().trimmed, 1);
    assert_eq!(f.log_start().await, 20);
    f.shutdown().await;
}
