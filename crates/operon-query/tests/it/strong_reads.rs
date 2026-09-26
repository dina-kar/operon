//! Task 15 item 3: strong reads right after each acknowledged write, while
//! a worker applies the link continuously, never miss the write and never
//! see a key twice (the ES write→read cycle, R11, Review Focus 1).

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::common::{Fixture, GATE_NS, battery_schema, upsert, write_ops};
use operon_collection::PrimaryKey;
use operon_query::{
    BoolOperator, Projection, Query, ReadConsistency, Retriever, SearchRequest, SourceFilter,
};

const DOCS: &str = "docs";

/// The token only the document written in iteration `i` holds.
fn marker(i: usize) -> String {
    format!("mark{i:04}")
}

fn find(text: &str) -> SearchRequest {
    let mut request = SearchRequest::new(DOCS);
    request.retrievers = vec![Retriever::Text {
        query: Query::Match {
            field: "title".to_string(),
            text: text.to_string(),
            operator: BoolOperator::Or,
            minimum_should_match: None,
            fuzziness: None,
            analyzer: None,
        },
        k: 10,
    }];
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strong_reads_during_link_apply_never_miss_or_duplicate() {
    let f = Fixture::start().await;
    let service = f.service();
    service
        .create_collection(GATE_NS, DOCS, battery_schema(), Some(3))
        .await
        .expect("create");
    // Continuously, but not so often that hundreds of one-document splits
    // make every search slow.
    f.start_worker_every(std::time::Duration::from_millis(250));
    let source = Projection {
        source: SourceFilter::All,
        ..Projection::default()
    };
    // Key → the iteration whose marker it holds.
    let mut model: BTreeMap<u64, usize> = BTreeMap::new();
    for i in 0..300 {
        // Every 5th iteration updates an earlier key instead.
        let key = if i % 5 == 4 {
            (i as u64 * 7) % (model.len() as u64)
        } else {
            i as u64
        };
        let previous = model.insert(key, i);
        let source_json = json!({"title": format!("common {}", marker(i)), "n": i});
        write_ops(&service, DOCS, vec![upsert(key, source_json.clone())]).await;

        let hits = service
            .search(GATE_NS, find(&marker(i)))
            .await
            .expect("search")
            .hits;
        let keys: Vec<PrimaryKey> = hits.iter().map(|hit| hit.pk.clone()).collect();
        assert_eq!(
            keys,
            [PrimaryKey::U64(key)],
            "iteration {i}: {marker} finds its key once",
            marker = marker(i)
        );
        if let Some(previous) = previous {
            let stale = service
                .search(GATE_NS, find(&marker(previous)))
                .await
                .expect("search")
                .hits;
            assert!(
                stale.is_empty(),
                "iteration {i}: the replaced {} is gone",
                marker(previous)
            );
        }
        let got = service
            .get(
                GATE_NS,
                DOCS,
                &[PrimaryKey::U64(key)],
                &source,
                ReadConsistency::Strong,
            )
            .await
            .expect("get");
        let stored = got[0].as_ref().expect("the key exists");
        assert_eq!(
            stored.source.as_ref().map(|s| Value::Object(s.clone())),
            Some(source_json),
            "iteration {i}"
        );
        let count = service
            .count(GATE_NS, DOCS, None, ReadConsistency::Strong)
            .await
            .expect("count");
        assert_eq!(count, model.len() as u64, "iteration {i}");
        if i % 25 != 24 {
            continue;
        }
        let common = service
            .search(GATE_NS, {
                let mut request = find("common");
                request.limit = 1_000;
                if let Some(Retriever::Text { k, .. }) = request.retrievers.first_mut() {
                    *k = 1_000;
                }
                request
            })
            .await
            .expect("search")
            .hits;
        assert_eq!(common.len(), model.len(), "iteration {i}: every key once");
    }
    let info = service.get_collection(GATE_NS, DOCS).await.expect("info");
    assert!(
        info.manifest_version >= 10,
        "manifests landed during the run: {}",
        info.manifest_version
    );
    f.shutdown().await;
}
