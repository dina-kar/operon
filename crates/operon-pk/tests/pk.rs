//! PkIndex on SlateDB over an in-memory (optionally faulty) object store.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use object_store::memory::InMemory;
use operon_pk::{PkError, PkIndex, PkIndexConfig, PkReader};
use operon_store::{Fault, FaultyStore, Op, Store};

const PATH: &str = "ns/1/pk/7/";

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn put(key: &str, value: &str) -> (Bytes, Option<Bytes>) {
    (b(key), Some(b(value)))
}

fn config() -> PkIndexConfig {
    PkIndexConfig {
        flush_interval: Duration::from_millis(10),
        ..PkIndexConfig::default()
    }
}

#[tokio::test]
async fn writes_round_trip_and_survive_a_reopen() {
    let store = Store::in_memory();
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    index
        .write(vec![put("a", "1"), put("b", "2"), put("c", "3")])
        .await
        .unwrap();
    assert_eq!(index.get(b"a").await.unwrap(), Some(b("1")));
    assert_eq!(index.get(b"missing").await.unwrap(), None);
    index.close().await.unwrap();

    let reopened = PkIndex::open(&store, PATH, config()).await.unwrap();
    assert_eq!(reopened.get(b"b").await.unwrap(), Some(b("2")));
    let all = reopened.scan_prefix(b"", 10).await.unwrap();
    assert_eq!(
        all,
        vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))]
    );
    // Everything lives under the index's prefix.
    let objects = store.list("").await.unwrap();
    assert!(!objects.is_empty());
    assert!(objects.iter().all(|o| o.path.starts_with("ns/1/pk/7/")));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn deletes_remove_keys() {
    let store = Store::in_memory();
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    index
        .write(vec![put("a", "1"), put("b", "2")])
        .await
        .unwrap();
    index.write(vec![(b("a"), None)]).await.unwrap();
    assert_eq!(index.get(b"a").await.unwrap(), None);
    // A put and a delete of the same key in one batch: the last one wins.
    index
        .write(vec![put("c", "3"), (b("c"), None)])
        .await
        .unwrap();
    assert_eq!(index.get(b"c").await.unwrap(), None);
    index.close().await.unwrap();
    let reopened = PkIndex::open(&store, PATH, config()).await.unwrap();
    assert_eq!(
        reopened.scan_prefix(b"", 10).await.unwrap(),
        vec![(b("b"), b("2"))]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn scan_prefix_honours_the_prefix_and_the_limit() {
    let store = Store::in_memory();
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    let batch = (0..20)
        .map(|i| put(&format!("user/{i:02}"), &i.to_string()))
        .chain([put("other", "x")])
        .collect();
    index.write(batch).await.unwrap();
    let page = index.scan_prefix(b"user/", 5).await.unwrap();
    assert_eq!(page.len(), 5);
    assert_eq!(page[0].0, b("user/00"));
    assert_eq!(page[4].0, b("user/04"));
    assert_eq!(index.scan_prefix(b"user/", 100).await.unwrap().len(), 20);
    index.close().await.unwrap();
}

/// Review focus 4: a writer fenced by a newer one fails its writes, and
/// nothing it attempted becomes visible.
#[tokio::test]
async fn a_second_open_fences_the_first_writer() {
    let store = Store::in_memory();
    let first = PkIndex::open(&store, PATH, config()).await.unwrap();
    first.write(vec![put("a", "1")]).await.unwrap();

    let second = PkIndex::open(&store, PATH, config()).await.unwrap();
    assert_eq!(second.get(b"a").await.unwrap(), Some(b("1")));

    let err = first.write(vec![put("zombie", "z")]).await.unwrap_err();
    assert!(matches!(err, PkError::Fenced), "{err:?}");
    // Still fenced on later writes.
    let err = first.write(vec![put("zombie2", "z")]).await.unwrap_err();
    assert!(matches!(err, PkError::Fenced | PkError::Closed), "{err:?}");

    second.write(vec![put("b", "2")]).await.unwrap();
    assert_eq!(second.get(b"zombie").await.unwrap(), None);
    second.close().await.unwrap();

    let third = PkIndex::open(&store, PATH, config()).await.unwrap();
    assert_eq!(third.get(b"zombie").await.unwrap(), None);
    assert_eq!(third.get(b"zombie2").await.unwrap(), None);
    assert_eq!(
        third.scan_prefix(b"", 10).await.unwrap(),
        vec![(b("a"), b("1")), (b("b"), b("2"))]
    );
    third.close().await.unwrap();
}

#[tokio::test]
async fn a_reader_sees_new_writes_after_refresh() {
    let store = Store::in_memory();
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    index.write(vec![put("a", "1")]).await.unwrap();
    let mut reader = PkReader::open(&store, PATH).await.unwrap();
    assert_eq!(reader.get(b"a").await.unwrap(), Some(b("1")));

    index
        .write(vec![put("b", "2"), (b("a"), None)])
        .await
        .unwrap();
    reader.refresh().await.unwrap();
    assert_eq!(reader.get(b"b").await.unwrap(), Some(b("2")));
    assert_eq!(reader.get(b"a").await.unwrap(), None);
    assert_eq!(
        reader.scan_prefix(b"", 10).await.unwrap(),
        vec![(b("b"), b("2"))]
    );
    reader.close().await.unwrap();
    index.close().await.unwrap();
}

/// Lost PUT acknowledgements (the object landed, the caller saw an error)
/// never lose an acknowledged write: either the write succeeds or it
/// reports an error, and everything acknowledged is there after a reopen.
#[tokio::test]
async fn lost_put_acknowledgements_lose_no_acknowledged_write() {
    let faults = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faults.clone());
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    let mut acknowledged = Vec::new();
    for i in 0..20 {
        if i % 3 == 0 {
            faults.inject(Op::Put, Fault::ErrorAfterApply);
        }
        if i % 5 == 0 {
            faults.inject(Op::Put, Fault::Error);
        }
        let key = format!("k{i:02}");
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            index.write(vec![put(&key, &i.to_string())]),
        )
        .await
        .expect("a write finishes");
        // SlateDB retries a failed WAL PUT, also one that was applied, so
        // every write succeeds and the only writer never fences itself
        // (review M7: `Fenced` means a newer writer exists).
        match result {
            Ok(()) => acknowledged.push(key),
            Err(err) => panic!("write {i} failed: {err:?}"),
        }
    }
    assert_eq!(acknowledged.len(), 20);
    assert_eq!(faults.pending(Op::Put), 0, "every fault was reached");
    // Whatever state the writer ended in, a new writer sees every
    // acknowledged write.
    let _ = index.close().await;
    let reopened = PkIndex::open(&store, PATH, config()).await.unwrap();
    for key in &acknowledged {
        assert!(
            reopened.get(key.as_bytes()).await.unwrap().is_some(),
            "{key} was acknowledged but is missing"
        );
    }
    reopened.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hundred_thousand_keys_fit_the_time_budget() {
    let store = Store::in_memory();
    let index = PkIndex::open(&store, PATH, config()).await.unwrap();
    let started = Instant::now();
    for batch in 0..100u32 {
        let entries = (0..1_000u32)
            .map(|i| {
                let n = batch * 1_000 + i;
                (
                    Bytes::from(format!("key-{n:08}")),
                    Some(Bytes::from(n.to_le_bytes().to_vec())),
                )
            })
            .collect();
        index.write(entries).await.unwrap();
    }
    for n in (0..100_000u32).step_by(997) {
        let value = index
            .get(format!("key-{n:08}").as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.as_ref(), n.to_le_bytes());
    }
    assert_eq!(
        index.scan_prefix(b"key-0009", 100_000).await.unwrap().len(),
        10_000
    );
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(60), "took {elapsed:?}");
    index.close().await.unwrap();
}
