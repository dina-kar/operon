//! The Lance integration (plan M1.1 Task 7): the object-store provider and
//! the Arrow schema.

mod common;

use std::sync::Arc;

use arrow_array::RecordBatch;
use common::{doc, sparse, vector};
use object_store::memory::InMemory;
use operon_collection::{
    CollectionError, CollectionSchema, Document, DynamicMapping, LanceConfig, LanceEnv, NewRow,
    PrimaryKey, SparseModifier, SparseVectorSpec, StoredRow, base_arrow_schema, row_from_batch,
    to_record_batch,
};
use operon_common::{CollectionId, NamespaceId};
use operon_store::{Fault, FaultyStore, Op, Store};
use serde_json::json;

const NS: NamespaceId = NamespaceId(1);
const CID: CollectionId = CollectionId(1);
/// Where collection 1's Lance manifests live in the store.
const VERSIONS: &str = "ns/1/collections/1/lance/_versions/";

fn faulty() -> (Arc<FaultyStore>, Store) {
    let faulty = Arc::new(FaultyStore::new(Arc::new(InMemory::new())));
    (faulty.clone(), Store::new(faulty))
}

fn env() -> (Arc<FaultyStore>, Store, LanceEnv) {
    let (faulty, store) = faulty();
    let env = LanceEnv::new(store.clone(), LanceConfig::default());
    (faulty, store, env)
}

fn batch(schema: &CollectionSchema, docs: &[Document]) -> RecordBatch {
    let rows: Vec<NewRow<'_>> = docs
        .iter()
        .zip(0u64..)
        .map(|(doc, offset)| NewRow {
            doc,
            partition: 0,
            offset,
        })
        .collect();
    to_record_batch(schema, &rows).expect("record batch")
}

/// The manifest file names under `_versions/`.
async fn manifests(store: &Store) -> Vec<String> {
    store
        .list(VERSIONS)
        .await
        .expect("list")
        .into_iter()
        .map(|info| info.path.rsplit('/').next().expect("name").to_string())
        .filter(|name| name.ends_with(".manifest"))
        .collect()
}

/// Version 1's manifest name (V2 naming: `u64::MAX - version`, 20 digits).
fn version_one_manifest() -> String {
    format!("{:020}.manifest", u64::MAX - 1)
}

#[test]
fn a_record_batch_round_trips_every_vector_kind() {
    let schema = CollectionSchema::new(
        vec![],
        vec![vector("", 2), vector("title", 3)],
        DynamicMapping::Strict,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "words".to_string(),
        modifier: SparseModifier::Idf,
    }]);
    let arrow = operon_collection::arrow_schema(&schema);
    let names: Vec<&str> = arrow.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        [
            "_pk",
            "_source",
            "_ingest_partition",
            "_ingest_offset",
            "_vector_0",
            "_vector_1",
            "_sparse_0"
        ]
    );
    assert_eq!(
        &arrow.fields()[..4],
        &base_arrow_schema().fields()[..],
        "the system columns come first"
    );
    let mut input = vec![
        doc(PrimaryKey::U64(7), json!({ "a": { "b": [1, 2.5, "x"] } })),
        doc(PrimaryKey::Uuid([9; 16]), json!({})),
        doc(PrimaryKey::Str("s".to_string()), json!({ "z": null })),
    ];
    input[0].vectors.insert(String::new(), vec![0.25, -1.0]);
    input[0]
        .vectors
        .insert("title".to_string(), vec![1.0, 2.0, f32::MIN_POSITIVE]);
    input[0]
        .sparse_vectors
        .insert("words".to_string(), sparse(&[3, 1], &[0.0, 2.0]));
    input[1].vectors.insert("title".to_string(), vec![0.0; 3]);
    input[1]
        .sparse_vectors
        .insert("words".to_string(), sparse(&[], &[]));
    let batch = batch(&schema, &input);
    assert_eq!(batch.schema().as_ref(), &arrow);
    for (row, doc) in input.iter().enumerate() {
        let stored = row_from_batch(&schema, &batch, row).expect("row");
        assert_eq!(
            stored,
            StoredRow {
                pk: doc.pk.clone(),
                source: doc.source.clone(),
                vectors: doc.vectors.clone(),
                sparse_vectors: doc.sparse_vectors.clone(),
                partition: 0,
                offset: row as u64,
            }
        );
    }
    // A vector the schema lacks, or of the wrong dimension, is refused.
    let mut unknown = input[2].clone();
    unknown.vectors.insert("other".to_string(), vec![1.0]);
    let mut wrong_dim = input[2].clone();
    wrong_dim.vectors.insert(String::new(), vec![1.0]);
    for doc in [&unknown, &wrong_dim] {
        let row = NewRow {
            doc,
            partition: 0,
            offset: 0,
        };
        assert!(matches!(
            to_record_batch(&schema, &[row]),
            Err(CollectionError::Internal(_))
        ));
    }
}

#[tokio::test]
async fn version_one_creation_race_is_harmless() {
    let (_, store, env) = env();
    let (a, b) = tokio::join!(env.ensure_created(NS, CID), env.ensure_created(NS, CID));
    for dataset in [a.expect("first creator"), b.expect("second creator")] {
        assert_eq!(dataset.manifest.version, 1);
        assert_eq!(
            arrow_schema::Schema::from(dataset.schema()),
            base_arrow_schema()
        );
        assert!(dataset.manifest.fragments.is_empty());
    }
    assert_eq!(manifests(&store).await, vec![version_one_manifest()]);
    // A second environment over the same store finds the same version 1.
    let other = LanceEnv::new(store.clone(), LanceConfig::default());
    let again = other.ensure_created(NS, CID).await.expect("reopen");
    assert_eq!(again.manifest.version, 1);
    assert_eq!(manifests(&store).await, vec![version_one_manifest()]);
}

/// How many GETs a creator's probe for a missing version 1 makes.
async fn probe_gets() -> u64 {
    let (faulty, _, env) = env();
    assert!(matches!(
        env.open(NS, CID, 1).await,
        Err(CollectionError::NotFound(_))
    ));
    faulty.calls(Op::Get)
}

#[tokio::test]
async fn a_creator_that_finds_version_one_late_commits_nothing() {
    // Two environments over one bucket. The slow creator's last probe GET is
    // delayed, so its probe misses version 1 and the fast creator commits it
    // meanwhile; Lance's own load inside the slow creator's commit then finds
    // it. Without the strict overwrite, Lance would commit mainline version 2
    // on top of it.
    let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let slow_faults = Arc::new(FaultyStore::new(bucket.clone()));
    let slow = LanceEnv::new(Store::new(slow_faults.clone()), LanceConfig::default());
    let fast = LanceEnv::new(Store::new(bucket.clone()), LanceConfig::default());
    let delay = std::time::Duration::from_millis(500);
    slow_faults.inject_nth(Op::Get, probe_gets().await, Fault::Delay(delay));
    let (slow_v1, fast_v1) = tokio::join!(slow.ensure_created(NS, CID), async {
        tokio::time::sleep(delay / 5).await;
        fast.ensure_created(NS, CID).await
    });
    assert_eq!(
        slow_faults.pending(Op::Get),
        0,
        "the slow creator never probed"
    );
    assert_eq!(slow_v1.expect("slow creator").manifest.version, 1);
    assert_eq!(fast_v1.expect("fast creator").manifest.version, 1);
    assert_eq!(
        manifests(&Store::new(bucket)).await,
        vec![version_one_manifest()]
    );
}

#[tokio::test]
async fn a_lost_creation_that_left_nothing_is_retryable() {
    let (faulty, store, env) = env();
    // The create-only write of version 1 reports that it already exists, but
    // nothing is there (plan ruling P18).
    faulty.inject(Op::PutCreate, Fault::Precondition);
    let err = env
        .ensure_created(NS, CID)
        .await
        .expect_err("lost creation");
    assert!(err.is_retryable(), "{err}");
    assert!(manifests(&store).await.is_empty());
    let v1 = env.ensure_created(NS, CID).await.expect("retry");
    assert_eq!(v1.manifest.version, 1);
}

#[tokio::test]
async fn opening_a_missing_version_is_not_found() {
    let (_, _, env) = env();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let reopened = env.open(NS, CID, 1).await.expect("open version 1");
    assert_eq!(reopened.manifest.version, v1.manifest.version);
    let missing = lance_table::format::DETACHED_VERSION_MASK | 42;
    assert!(matches!(
        env.open(NS, CID, missing).await,
        Err(CollectionError::NotFound(_))
    ));
    assert!(matches!(
        env.open(NS, CollectionId(2), 1).await,
        Err(CollectionError::NotFound(_))
    ));
}
