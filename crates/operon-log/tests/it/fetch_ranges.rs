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
    let entries: Vec<_> = meta
        .client
        .read(Consistency::Local, |s| {
            s.partition(stream, 0)
                .map(|p| p.entries().cloned().collect())
                .expect("partition")
        })
        .await
        .expect("read");
    assert_eq!(entries.len(), 1, "the 16 appends share one WAL chunk");
    let chunk = &entries[0];

    const BLOCK: u64 = 4096;
    let cache = RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: BLOCK,
            memory_bytes: 1 << 20,
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
    // Block misses of one fetch from a cold cache: every block is evicted
    // first, so the count does not depend on what an earlier read left.
    let cold_misses = |offset| {
        let cache = cache.clone();
        let fetch = &fetch;
        let object = chunk.object.clone();
        async move {
            cache.forget(&object).await;
            let before = cache.stats().misses;
            let response = fetch(offset).await.expect("fetch");
            (response, cache.stats().misses - before)
        }
    };
    // The first fetch reads the whole chunk and learns its batches.
    let (first, whole) = cold_misses(0).await;
    assert_eq!(first.records.len(), BATCHES);
    let span = |range: std::ops::Range<u64>| (range.end - 1) / BLOCK - range.start / BLOCK + 1;
    let chunk_bytes = chunk.byte_range.end - chunk.byte_range.start;
    assert_eq!(
        whole,
        span(chunk.byte_range.clone()),
        "a cold whole-chunk read"
    );

    // Each append is one batch, so the 15th batch starts at offset 14. The
    // 16 batches differ by at most one key byte, so the last two hold at most
    // `2 · ⌈chunk / 16⌉ + 2` bytes and end at the chunk's end.
    let (second, misses) = cold_misses(14).await;
    let last_two = 2 * chunk_bytes.div_ceil(BATCHES as u64) + 2;
    let bound = span(chunk.byte_range.end - last_two..chunk.byte_range.end);
    assert!(
        misses <= bound,
        "{misses} block misses; the last two batches span at most {bound} of the chunk's {whole}"
    );
    assert!(
        misses < whole,
        "{misses} of {whole} blocks: the whole chunk was read"
    );
    assert_eq!(second.records, first.records[14..]);

    writer.shutdown().await.expect("shutdown writer");
    meta.shutdown().await;
}
