//! The Lance integration (plan M1.1 Task 7): the object-store provider, the
//! Arrow schema, and detached commits (R7, Ruling 1).
//!
//! Every test runs twice (plan M1.3 Task 2): in `plain` over
//! [`LanceEnv::new`], and in `cached` over [`LanceEnv::with_cache`], whose
//! Lance reads go through the range cache.

use std::sync::Arc;

use crate::common::{doc, range_cache};
use lance::dataset::transaction::Operation;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use operon_collection::{
    CachedObjectStore, CollectionSchema, DynamicMapping, LanceCommitter, LanceConfig, LanceEnv,
    NewRow, PrimaryKey, to_record_batch,
};
use operon_common::{CollectionId, NamespaceId};
use operon_store::{FaultyStore, Op, Store};
use serde_json::json;

mod plain {
    async fn make_env(store: Store, config: LanceConfig) -> LanceEnv {
        LanceEnv::new(store, config)
    }

    include!("lance_cases.rs");
}

/// `writes_and_lists_pass_through_the_cache`: every M1.1 test with the
/// range cache in front of Lance's reads.
mod cached {
    async fn make_env(store: Store, config: LanceConfig) -> LanceEnv {
        let cache = crate::common::range_cache(&store).await;
        LanceEnv::with_cache(store, cache, config)
    }

    include!("lance_cases.rs");
}

/// A `FaultyStore` over memory, a range cache over it, and a Lance
/// environment that reads through the cache.
async fn cached_env() -> (Arc<FaultyStore>, operon_cache::RangeCache, LanceEnv) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    let store = Store::new(faulty.clone());
    let cache = range_cache(&store).await;
    let env = LanceEnv::with_cache(store, cache.clone(), LanceConfig::default());
    (faulty, cache, env)
}

/// A second identical scan is served from the range cache: no store GET,
/// only cache hits.
#[tokio::test]
async fn lance_reads_go_through_the_range_cache() {
    let (faulty, cache, env) = cached_env().await;
    let (ns, cid) = (NamespaceId(1), CollectionId(1));
    let schema = CollectionSchema::new(vec![], vec![], DynamicMapping::Strict);
    let docs: Vec<_> = (0..50)
        .map(|i| {
            doc(
                PrimaryKey::U64(i),
                json!({ "n": i, "text": "some words to read" }),
            )
        })
        .collect();
    let rows: Vec<NewRow<'_>> = docs
        .iter()
        .zip(0u64..)
        .map(|(doc, offset)| NewRow {
            doc,
            partition: 0,
            offset,
        })
        .collect();
    let parent = env.ensure_created(ns, cid).await.expect("create");
    let batch = to_record_batch(&schema, &rows).expect("batch");
    let fragments = LanceCommitter::write_fragments(&env, &parent, batch)
        .await
        .expect("write");
    let committed = LanceCommitter::commit(&env, &parent, Operation::Append { fragments })
        .await
        .expect("commit");
    let dataset = env
        .open(ns, cid, committed.manifest.version)
        .await
        .expect("open");
    let first = dataset.scan().try_into_batch().await.expect("scan");
    assert_eq!(first.num_rows(), 50);
    let (gets, hits) = (faulty.calls(Op::Get), cache.stats().hits);
    let second = dataset.scan().try_into_batch().await.expect("scan");
    assert_eq!(second, first);
    assert_eq!(
        faulty.calls(Op::Get),
        gets,
        "the second scan reached the store"
    );
    assert!(
        cache.stats().hits > hits,
        "the second scan hit no cached block"
    );
}

/// A missing object is `NotFound` through the cache: `ensure_created`
/// probes the missing version-1 manifest and creates it.
#[tokio::test]
async fn a_missing_object_is_not_found_through_the_cache() {
    let (faulty, cache, env) = cached_env().await;
    let store = CachedObjectStore::new(faulty.clone(), cache);
    let missing = store
        .get(&Path::from("ns/1/collections/1/lance/nothing"))
        .await;
    assert!(
        matches!(missing, Err(object_store::Error::NotFound { .. })),
        "{missing:?}"
    );
    let created = env
        .ensure_created(NamespaceId(1), CollectionId(1))
        .await
        .expect("create");
    assert_eq!(created.manifest.version, 1);
    let opened = env
        .open(NamespaceId(1), CollectionId(1), 1)
        .await
        .expect("open");
    assert_eq!(opened.manifest.version, 1);
}
