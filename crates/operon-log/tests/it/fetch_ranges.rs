//! A fetch inside a WAL chunk whose batch boundaries the reader has learned
//! reads only from the batch holding the fetch offset (plan M1.2 Task 3
//! rule 11; the M0 known limitation).

use std::time::Duration;

use bytes::Bytes;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_log::{FetchRequest, LogConfig, LogReader, LogWriter, Record};
use operon_meta::Consistency;
use operon_store::Store;

use crate::common::Meta;

const BATCHES: usize = 16;
const VALUE_BYTES: usize = 4096;

#[tokio::test]
async fn a_fetch_inside_a_known_chunk_reads_only_from_its_batch() {
    let meta = Meta::start().await;
    let (_, stream) = meta.stream("acme", "events", 1).await;
    let store = Store::in_memory();
    // One flush for all 16 appends.
    let config = LogConfig {
        flush_interval: Duration::from_millis(500),
        ..LogConfig::new(1)
    };
    let writer = LogWriter::start(meta.client.clone(), store.clone(), config).expect("writer");
    let appends = (0..BATCHES).map(|i| {
        let writer = writer.clone();
        async move {
            let record = Record {
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(vec![b'a' + i as u8; VALUE_BYTES])),
                headers: vec![],
                timestamp_ms: 1_000,
            };
            writer
                .append(stream, 0, vec![record])
                .await
                .expect("append")
        }
    });
    futures::future::join_all(appends).await;
    let entries = meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(stream, 0)
                .map(|p| p.entries().count())
                .expect("partition")
        })
        .await
        .expect("read");
    assert_eq!(entries, 1, "the 16 appends share one WAL chunk");

    // 4 KiB blocks, and memory for only a few of them, so nothing read by
    // the first fetch is still cached for the second.
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: 4096,
            memory_bytes: 8 * 1024,
            disk: None,
        },
    )
    .await
    .expect("cache");
    let reader = LogReader::new(meta.client.clone(), cache.clone());
    let fetch = |offset| {
        reader.fetch(FetchRequest {
            stream,
            partition: 0,
            offset,
            max_bytes: 16 << 20,
            max_wait: Duration::ZERO,
        })
    };
    let first = fetch(0).await.expect("fetch from 0");
    assert_eq!(first.records.len(), BATCHES);
    // Each append is one batch, so the 15th batch starts at offset 14.
    let before = cache.stats().misses;
    let second = fetch(14).await.expect("fetch from the 15th batch");
    let misses = cache.stats().misses - before;
    assert!(misses <= 2, "{misses} block misses");
    assert_eq!(second.records, first.records[14..]);

    writer.shutdown().await.expect("shutdown writer");
    meta.shutdown().await;
}
