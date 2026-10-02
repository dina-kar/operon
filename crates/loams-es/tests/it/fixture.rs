//! The index of the Task 7 and Task 8 suites (plan M1.5 Task 7 Tests).

use loams_common::{CollectionId, StreamId};
use loams_es::EsError;
use loams_es::mapping::{IndexView, plan_create};
use loams_query::CollectionInfo;
use loams_query::backlog::BackpressureStatus;
use loams_query::hot::HotStatus;
use serde_json::{Value, json};

/// 2026-09-24T10:11:12.345Z.
pub const NOW_MS: i64 = 1_790_244_672_345;

/// The fixture's mappings: the plan's fields, BEIR's `title` and `txt`,
/// numeric, boolean and flattened fields, the cache's unindexed `text` and
/// `binary`, and l2_norm and dot_product vectors.
pub fn mappings() -> Value {
    json!({"properties": {
        "text": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "metadata": {"properties": {
            "page": {"type": "long"},
            "author": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
        }},
        "session_id": {"type": "keyword"},
        "created_at": {"type": "date"},
        "vector": {"type": "dense_vector", "dims": 3, "index": true, "similarity": "cosine"},
        "v2": {"type": "dense_vector", "dims": 3, "index": false},
        "l2": {"type": "dense_vector", "dims": 3, "index": true, "similarity": "l2_norm"},
        "dot": {"type": "dense_vector", "dims": 3, "index": true, "similarity": "dot_product"},
        "title": {"type": "text", "analyzer": "english"},
        "txt": {"type": "text", "analyzer": "english"},
        "score": {"type": "double"},
        "flag": {"type": "boolean"},
        "labels": {"type": "flattened"},
        "llm_output": {"type": "text", "index": false},
        "vector_dump": {"type": "binary", "doc_values": false}
    }})
}

pub fn view() -> IndexView {
    let schema = plan_create(Some(&mappings()), None)
        .expect("plan_create")
        .schema();
    IndexView::new(CollectionInfo {
        id: CollectionId(7),
        name: "i".to_string(),
        namespace: "default".to_string(),
        schema,
        partitions: 1,
        aliases: Vec::new(),
        stream: StreamId(1),
        manifest_version: 0,
        live_doc_count: 0,
        size_bytes: 0,
        created_at_ms: 1_700_000_000_000,
        link_lag_records: 0,
        hot: HotStatus::default(),
        unapplied_bytes: 0,
        backpressure: BackpressureStatus::default(),
    })
}

/// Asserts `error`'s status, type and reason prefix.
pub fn assert_error(error: &EsError, status: u16, kind: &str, reason: &str) {
    assert_eq!(error.status, status, "{error:?}");
    assert_eq!(error.kind, kind, "{error:?}");
    assert!(
        error.reason.starts_with(reason),
        "reason {:?} does not start with {reason:?}",
        error.reason
    );
}
