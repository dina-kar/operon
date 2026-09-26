//! Task 15 item 6: the hot tier's hooks never change exact results, and
//! approximate results carry exact scores (R12, A13, A23).

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::common::{
    FakeHot, Fixture, GATE_NS, battery, battery_projection, battery_schema, random_document,
    random_history, run_probe, title_word, with_hot, write_ops,
};
use operon_collection::{CollectionConfig, Distance, DocOp, PrimaryKey};
use operon_query::hot::HotKind;
use operon_query::{
    AnnParams, BoolOperator, Fusion, Query, ReadConsistency, Retriever, SearchRequest,
    ServiceConfig,
};

const HOT: &str = "hot";

/// An approximate vector search of `query` (k 10), or its fusion with a
/// text search.
fn approximate(query: &[f32], fused: bool) -> SearchRequest {
    let mut request = SearchRequest::new(HOT);
    request.retrievers = vec![Retriever::Vector {
        field: "v".to_string(),
        query: query.to_vec(),
        k: 10,
        params: AnnParams::default(),
        filter: None,
    }];
    if fused {
        request.retrievers.push(Retriever::Text {
            query: Query::Match {
                field: "title".to_string(),
                text: format!("{} {}", title_word(0), title_word(1)),
                operator: BoolOperator::Or,
                minimum_should_match: None,
                fuzziness: None,
                analyzer: None,
            },
            k: 10,
        });
        request.fusion = Some(Fusion::Rrf { k: 60 });
    }
    request.select = battery_projection();
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hot_hooks_never_change_exact_results() {
    let collection = CollectionConfig {
        index_min_rows: 256,
        index_delta_min_rows: 1_000_000,
        index_poll_interval: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let f = Fixture::start_configured(ServiceConfig::default(), collection).await;
    let service = f.service();
    service
        .create_collection(GATE_NS, HOT, battery_schema(), Some(3))
        .await
        .expect("create");
    let mut rng = ChaCha8Rng::seed_from_u64(66);
    // 400 documents outside the battery's key space, indexed, then a
    // history over the key space applied, then more of it in the tail.
    let bulk: Vec<DocOp> = (0..400u64)
        .map(|i| DocOp::Upsert(random_document(&mut rng, PrimaryKey::U64(1 << 40 | i))))
        .collect();
    write_ops(&service, HOT, bulk).await;
    f.settle().await;
    f.build_indexes().await;
    write_ops(&service, HOT, random_history(&mut rng, 80)).await;
    f.settle().await;
    write_ops(&service, HOT, random_history(&mut rng, 20)).await;
    let info = service.get_collection(GATE_NS, HOT).await.expect("info");
    assert!(info.manifest_version >= 3, "{info:?}");

    let hot = FakeHot::new();
    hot.pin_splits(&f, HOT).await;
    let live = hot.serve_ann(&f, HOT).await;
    assert_eq!(live, info.manifest_version);
    service.set_hot_tier(hot.clone());

    // Every exact probe equals its result without a hot tier.
    for (name, probe) in battery(HOT) {
        let strong = ReadConsistency::Strong;
        let cold = with_hot(false, run_probe(&service, HOT, &probe, &strong)).await;
        let warm = with_hot(true, run_probe(&service, HOT, &probe, &strong)).await;
        assert!(cold.get("error").is_none(), "{name}: {cold}");
        assert_eq!(cold, warm, "{name}");
    }

    // Approximate probes return exact scores, hot or not.
    for _ in 0..8 {
        let query: Vec<f32> = (0..4).map(|_| rng.random_range(-1.0f32..1.0)).collect();
        for enabled in [true, false] {
            let response = with_hot(enabled, service.search(GATE_NS, approximate(&query, false)))
                .await
                .expect("search");
            assert_eq!(response.hits.len(), 10);
            for hit in &response.hits {
                let v = &hit.vectors["v"];
                let exact = operon_query::vector::score(Distance::Cosine, &query, v);
                assert_eq!(hit.score.to_bits(), exact.to_bits(), "hot {enabled}");
            }
        }
    }

    // What the hot tier reports, and that the switch keeps it out.
    let query = [0.3, -0.5, 0.8, 0.1];
    let warm = with_hot(true, service.search(GATE_NS, approximate(&query, true)))
        .await
        .expect("search");
    assert_eq!(
        warm.hot_used,
        BTreeSet::from([HotKind::Hnsw, HotKind::Splits])
    );
    assert!(hot.ann_calls.load(Ordering::SeqCst) > 0);
    let calls = hot.calls();
    let cold = with_hot(false, service.search(GATE_NS, approximate(&query, true)))
        .await
        .expect("search");
    assert!(cold.hot_used.is_empty(), "{:?}", cold.hot_used);
    assert_eq!(hot.calls(), calls, "the switch keeps the hot tier out");
    f.shutdown().await;
}
