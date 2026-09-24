//! The fetch path with long-poll.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Meta, fast_config, records, segment_now, small_cache, value};
use operon_common::{NamespaceId, StreamId};
use operon_log::{FetchRequest, FetchResponse, LogConfig, LogError, LogReader, LogWriter};
use operon_meta::{Consistency, EntryKind};
use operon_store::Store;

struct Fixture {
    meta: Meta,
    store: Store,
    writer: LogWriter,
    reader: LogReader,
    ns: NamespaceId,
    stream: StreamId,
}

impl Fixture {
    async fn start(config: LogConfig) -> Self {
        let meta = Meta::start().await;
        let (ns, stream) = meta.stream("acme", "events", 1).await;
        let store = Store::in_memory();
        let writer =
            LogWriter::start(meta.client.clone(), store.clone(), config).expect("start writer");
        let reader = LogReader::new(meta.client.clone(), small_cache(&store).await);
        Self {
            meta,
            store,
            writer,
            reader,
            ns,
            stream,
        }
    }

    /// Appends batches of the given sizes, one flush each; returns the model:
    /// offset → value.
    async fn append_batches(&self, sizes: &[usize]) -> BTreeMap<u64, String> {
        let mut model = BTreeMap::new();
        for (i, &n) in sizes.iter().enumerate() {
            let batch = records(&format!("b{i}"), n);
            let ack = self
                .writer
                .append(self.stream, 0, batch.clone())
                .await
                .expect("append");
            for (offset, record) in (ack.base_offset..).zip(&batch) {
                model.insert(offset, value(record));
            }
        }
        model
    }

    async fn fetch(&self, offset: u64, max_bytes: usize) -> Result<FetchResponse, LogError> {
        self.reader
            .fetch(FetchRequest {
                stream: self.stream,
                partition: 0,
                offset,
                max_bytes,
                max_wait: Duration::ZERO,
            })
            .await
    }

    async fn kinds(&self) -> Vec<EntryKind> {
        self.meta
            .client
            .read(Consistency::Local, |s| {
                s.partition(self.stream, 0)
                    .expect("partition")
                    .entries()
                    .map(|e| e.kind)
                    .collect()
            })
            .await
            .expect("read")
    }

    async fn segment(&self, max_entries: usize) -> String {
        segment_now(
            &self.meta.client,
            &self.store,
            self.ns,
            self.stream,
            0,
            max_entries,
        )
        .await
        .expect("segmented")
    }

    async fn shutdown(self) {
        self.writer.shutdown().await.expect("shutdown writer");
        self.meta.shutdown().await;
    }
}

fn values(response: &FetchResponse) -> Vec<(u64, String)> {
    response
        .records
        .iter()
        .map(|r| (r.offset, value(&r.record)))
        .collect()
}

fn model_from(model: &BTreeMap<u64, String>, offset: u64) -> Vec<(u64, String)> {
    model
        .range(offset..)
        .map(|(o, v)| (*o, v.clone()))
        .collect()
}

/// Every offset of the log, fetched from each start offset, returns exactly
/// the model's records.
async fn check_all_offsets(f: &Fixture, model: &BTreeMap<u64, String>) {
    let end = model.len() as u64;
    for offset in 0..end {
        let response = f.fetch(offset, usize::MAX).await.expect("fetch");
        assert_eq!(
            values(&response),
            model_from(model, offset),
            "from {offset}"
        );
        assert_eq!(response.next_offset, end);
        assert_eq!(response.high_watermark, end);
        assert_eq!(response.log_start_offset, 0);
    }
}

#[tokio::test]
async fn fetches_from_wal_entries() {
    let f = Fixture::start(fast_config()).await;
    let model = f.append_batches(&[2, 3, 4]).await;
    assert_eq!(f.kinds().await, [EntryKind::Wal; 3]);
    check_all_offsets(&f, &model).await;
    f.shutdown().await;
}

#[tokio::test]
async fn fetches_from_segment_entries() {
    let f = Fixture::start(fast_config()).await;
    let model = f.append_batches(&[2, 3, 4, 1]).await;
    f.segment(2).await;
    f.segment(2).await;
    assert_eq!(f.kinds().await, [EntryKind::Segment; 2]);
    check_all_offsets(&f, &model).await;
    f.shutdown().await;
}

#[tokio::test]
async fn fetches_across_mixed_segment_and_wal_entries() {
    let f = Fixture::start(fast_config()).await;
    let model = f.append_batches(&[2, 3, 4, 1, 5]).await;
    f.segment(2).await;
    assert_eq!(
        f.kinds().await,
        [
            EntryKind::Segment,
            EntryKind::Wal,
            EntryKind::Wal,
            EntryKind::Wal
        ]
    );
    check_all_offsets(&f, &model).await;
    f.shutdown().await;
}

/// A batch larger than `max_bytes` is still returned whole, so a consumer
/// never livelocks; stepping through with a tiny budget returns one batch per
/// fetch.
#[tokio::test]
async fn a_batch_larger_than_max_bytes_is_returned_whole() {
    let f = Fixture::start(fast_config()).await;
    let model = f.append_batches(&[3, 2, 4, 1, 2]).await;
    f.segment(3).await;
    let batch_ends = [3, 5, 9, 10, 12];
    let mut offset = 0;
    for end in batch_ends {
        let response = f.fetch(offset, 1).await.unwrap();
        assert_eq!(response.next_offset, end, "from {offset}");
        let expected: Vec<(u64, String)> = model
            .range(offset..end)
            .map(|(o, v)| (*o, v.clone()))
            .collect();
        assert_eq!(values(&response), expected);
        offset = end;
    }
    // Mid-batch, the rest of that batch only.
    let response = f.fetch(6, 1).await.unwrap();
    assert_eq!(response.next_offset, 9);
    assert_eq!(response.records.len(), 3);
    f.shutdown().await;
}

#[tokio::test]
async fn a_long_poll_at_the_high_watermark_wakes_on_commit() {
    let flush_interval = Duration::from_millis(250);
    let f = Fixture::start(LogConfig {
        flush_interval,
        ..LogConfig::new(1)
    })
    .await;
    f.append_batches(&[2]).await;
    let reader = f.reader.clone();
    let stream = f.stream;
    let poll = tokio::spawn(async move {
        let response = reader
            .fetch(FetchRequest {
                stream,
                partition: 0,
                offset: 2,
                max_bytes: usize::MAX,
                max_wait: Duration::from_secs(30),
            })
            .await;
        (response, Instant::now())
    });
    // Let the poll reach its wait (it can only return early on a commit).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!poll.is_finished());
    let started = Instant::now();
    f.writer
        .append(f.stream, 0, records("late", 3))
        .await
        .unwrap();
    let (response, woke) = poll.await.unwrap();
    let response = response.unwrap();
    assert_eq!(response.records.len(), 3);
    assert_eq!(response.records[0].offset, 2);
    assert_eq!(response.next_offset, 5);
    let latency = woke.duration_since(started);
    assert!(
        latency < flush_interval + Duration::from_millis(100),
        "{latency:?}"
    );
    f.shutdown().await;
}

#[tokio::test]
async fn a_long_poll_times_out_empty() {
    let f = Fixture::start(fast_config()).await;
    f.append_batches(&[2]).await;
    let started = Instant::now();
    let response = f
        .reader
        .fetch(FetchRequest {
            stream: f.stream,
            partition: 0,
            offset: 2,
            max_bytes: 100,
            max_wait: Duration::from_millis(200),
        })
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(200));
    assert!(response.records.is_empty());
    assert_eq!(
        (
            response.next_offset,
            response.high_watermark,
            response.log_start_offset
        ),
        (2, 2, 0)
    );
    f.shutdown().await;
}

#[tokio::test]
async fn offsets_out_of_range_are_rejected_on_both_sides() {
    let f = Fixture::start(fast_config()).await;
    f.append_batches(&[2, 3]).await;
    let err = f.fetch(6, 100).await.unwrap_err();
    assert!(
        matches!(
            err,
            LogError::OffsetOutOfRange {
                requested: 6,
                log_start_offset: 0,
                high_watermark: 5
            }
        ),
        "{err:?}"
    );
    f.meta.client.trim_partition(f.stream, 0, 3).await.unwrap();
    let err = f.fetch(2, 100).await.unwrap_err();
    assert!(
        matches!(
            err,
            LogError::OffsetOutOfRange {
                requested: 2,
                log_start_offset: 3,
                high_watermark: 5
            }
        ),
        "{err:?}"
    );
    let response = f.fetch(3, 100).await.unwrap();
    assert_eq!(response.records[0].offset, 3);
    assert_eq!(response.log_start_offset, 3);

    let err = f
        .reader
        .fetch(FetchRequest {
            stream: StreamId(99),
            partition: 0,
            offset: 0,
            max_bytes: 1,
            max_wait: Duration::ZERO,
        })
        .await
        .unwrap_err();
    assert!(matches!(err, LogError::UnknownStream(_)), "{err:?}");
    f.shutdown().await;
}

/// Fetches race segment swaps, trims, and deletion of the objects they
/// retire: every fetch returns exactly the model's records from its offset,
/// or `OffsetOutOfRange` below the log start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fetches_racing_swaps_and_trims_are_correct_or_out_of_range() {
    let f = Arc::new(Fixture::start(fast_config()).await);
    let sizes: Vec<usize> = (0..40).map(|i| 1 + i % 4).collect();
    let model = f.append_batches(&sizes).await;
    let end = model.len() as u64;

    let done = Arc::new(AtomicBool::new(false));
    let background = {
        let f = f.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut trim_to = 0;
            while segment_now(&f.meta.client, &f.store, f.ns, f.stream, 0, 3)
                .await
                .is_some()
            {
                // Garbage-collect whatever is retired, so racing reads find
                // their objects gone.
                let retired: Vec<String> = f
                    .meta
                    .client
                    .read(Consistency::Local, |s| {
                        s.retired().map(|(p, _)| p.to_string()).collect()
                    })
                    .await
                    .unwrap();
                for path in &retired {
                    f.store.delete(path).await.unwrap();
                }
                f.meta.client.forget_objects(retired).await.unwrap();
                trim_to += 2;
                f.meta
                    .client
                    .trim_partition(f.stream, 0, trim_to)
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    let mut checked = 0;
    let mut out_of_range = 0;
    let mut offset = 0;
    while !done.load(Ordering::SeqCst) || checked < 50 {
        offset = (offset + 7) % end;
        match f.fetch(offset, 64).await {
            Ok(response) => {
                let got = values(&response);
                assert!(!got.is_empty());
                let expected: Vec<(u64, String)> = model_from(&model, offset)
                    .into_iter()
                    .take(got.len())
                    .collect();
                assert_eq!(got, expected, "from {offset}");
                checked += 1;
            }
            Err(LogError::OffsetOutOfRange {
                requested,
                log_start_offset,
                ..
            }) => {
                assert!(requested < log_start_offset);
                out_of_range += 1;
                checked += 1;
            }
            Err(err) => panic!("fetch from {offset}: {err:?}"),
        }
        tokio::task::yield_now().await;
    }
    background.await.unwrap();
    assert!(
        checked >= 50 && out_of_range > 0,
        "{checked} {out_of_range}"
    );
    match Arc::try_unwrap(f) {
        Ok(f) => f.shutdown().await,
        Err(_) => panic!("fixture still shared"),
    }
}

#[tokio::test]
async fn a_corrupt_object_fails_the_fetch_as_corrupt() {
    // A WAL chunk.
    let f = Fixture::start(fast_config()).await;
    f.append_batches(&[3]).await;
    let object = f.store.list("wal/").await.unwrap().remove(0).path;
    let (bytes, _) = f.store.get(&object).await.unwrap();
    let mut corrupted = bytes.to_vec();
    corrupted[operon_log::wal::HEADER_LEN + 30] ^= 0xff;
    f.store.put(&object, corrupted.into()).await.unwrap();
    let err = f.fetch(0, 1000).await.unwrap_err();
    assert!(matches!(err, LogError::Corrupt(_)), "{err:?}");
    f.shutdown().await;

    // A segment's data.
    let f = Fixture::start(fast_config()).await;
    f.append_batches(&[3, 2]).await;
    let segment = f.segment(2).await;
    let (bytes, _) = f.store.get(&segment).await.unwrap();
    let mut corrupted = bytes.to_vec();
    corrupted[operon_log::segment::HEADER_LEN as usize + 30] ^= 0xff;
    f.store.put(&segment, corrupted.into()).await.unwrap();
    let err = f.fetch(0, 1000).await.unwrap_err();
    assert!(matches!(err, LogError::Corrupt(_)), "{err:?}");
    f.shutdown().await;
}

/// Review M1: a wait too long to add to `Instant::now()` does not panic.
#[tokio::test]
async fn an_unrepresentable_max_wait_does_not_panic() {
    let f = Fixture::start(fast_config()).await;
    f.append_batches(&[2]).await;
    let response = f
        .reader
        .fetch(FetchRequest {
            stream: f.stream,
            partition: 0,
            offset: 0,
            max_bytes: 100,
            max_wait: Duration::MAX,
        })
        .await
        .unwrap();
    assert_eq!(response.records.len(), 2);
    // At the high watermark, the wait ends with the next commit.
    let reader = f.reader.clone();
    let stream = f.stream;
    let poll = tokio::spawn(async move {
        reader
            .fetch(FetchRequest {
                stream,
                partition: 0,
                offset: 2,
                max_bytes: 100,
                max_wait: Duration::MAX,
            })
            .await
    });
    f.append_batches(&[1]).await;
    assert_eq!(poll.await.unwrap().unwrap().records.len(), 1);
    f.shutdown().await;
}
