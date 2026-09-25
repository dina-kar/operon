//! The Lance integration (plan M1.1 Task 7): the object-store provider, the
//! Arrow schema, and detached commits (R7, Ruling 1).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, FixedSizeListArray, RecordBatch};
use common::{doc, sparse, vector};
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance_file::version::LanceFileVersion;
use lance_table::format::{Fragment, is_detached_version};
use object_store::memory::InMemory;
use operon_collection::{
    CollectionError, CollectionSchema, Document, DynamicMapping, INGEST_OFFSET_COLUMN,
    LanceCommitter, LanceConfig, LanceEnv, NewRow, PK_COLUMN, PrimaryKey, SparseModifier,
    SparseVectorSpec, StoredRow, base_arrow_schema, row_from_batch, sparse_column, to_record_batch,
    vector_column,
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

fn plain_schema() -> CollectionSchema {
    CollectionSchema::new(vec![], vec![], DynamicMapping::Strict)
}

fn docs(prefix: &str, n: usize) -> Vec<Document> {
    (0..n)
        .map(|i| doc(PrimaryKey::Str(format!("{prefix}{i}")), json!({ "n": i })))
        .collect()
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

/// Appends `docs` on top of `parent`, as one detached commit.
async fn append(
    env: &LanceEnv,
    parent: &Arc<Dataset>,
    schema: &CollectionSchema,
    docs: &[Document],
) -> Arc<Dataset> {
    let fragments = LanceCommitter::write_fragments(env, parent, batch(schema, docs))
        .await
        .expect("write fragments");
    LanceCommitter::commit(env, parent, Operation::Append { fragments })
        .await
        .expect("commit")
}

fn update(new: Vec<Fragment>, updated: Vec<Fragment>, removed: Vec<u64>) -> Operation {
    Operation::Update {
        removed_fragment_ids: removed,
        updated_fragments: updated,
        new_fragments: new,
        fields_modified: vec![],
        compacted_sstables: vec![],
        fields_for_preserving_frag_bitmap: vec![],
        update_mode: None,
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    }
}

async fn scan_all(dataset: &Dataset) -> RecordBatch {
    dataset.scan().try_into_batch().await.expect("scan")
}

/// The keys of every live row.
async fn pks(dataset: &Dataset) -> BTreeSet<PrimaryKey> {
    let batch = scan_all(dataset).await;
    let column = batch
        .column_by_name(PK_COLUMN)
        .expect("_pk")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary")
        .clone();
    column
        .iter()
        .map(|pk| PrimaryKey::from_canonical(pk.expect("non-null pk")).expect("pk"))
        .collect()
}

fn keys(docs: &[&[Document]]) -> BTreeSet<PrimaryKey> {
    docs.iter()
        .flat_map(|docs| docs.iter().map(|doc| doc.pk.clone()))
        .collect()
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

async fn detached_manifests(store: &Store) -> BTreeSet<String> {
    manifests(store)
        .await
        .into_iter()
        .filter(|name| name.starts_with('d'))
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
async fn datasets_use_file_format_2_1_and_stable_row_ids() {
    let (_, _, env) = env();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let schema = plain_schema();
    let committed = append(&env, &v1, &schema, &docs("a", 3)).await;
    for dataset in [&v1, &committed] {
        assert!(dataset.manifest.uses_stable_row_ids());
        assert_eq!(
            dataset.manifest.data_storage_format.lance_file_format(),
            LanceFileVersion::V2_1.resolve()
        );
    }
    for fragment in committed.manifest.fragments.iter() {
        for file in &fragment.files {
            assert_eq!(
                (file.file_major_version, file.file_minor_version),
                (2, 1),
                "data file {}",
                file.path
            );
        }
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
    // The slow creator wrote nothing, so it took the late-probe path.
    assert_eq!(slow_faults.calls(Op::PutCreate), 0);
    assert_eq!(slow_faults.calls(Op::Put), 0);
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

#[tokio::test]
async fn a_detached_commit_is_based_exactly_on_its_parent() {
    let (_, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let a_docs = docs("a", 3);
    let b_docs = docs("b", 2);
    let c_docs = docs("c", 1);
    let a = append(&env, &v1, &schema, &a_docs).await;
    let b = append(&env, &a, &schema, &b_docs).await;
    let c = append(&env, &a, &schema, &c_docs).await;
    for dataset in [&a, &b, &c] {
        assert!(is_detached_version(dataset.manifest.version));
    }
    assert_eq!(pks(&c).await, keys(&[&a_docs, &c_docs]));
    assert_eq!(pks(&b).await, keys(&[&a_docs, &b_docs]));
    assert_eq!(pks(&a).await, keys(&[&a_docs]));
}

#[tokio::test]
async fn a_detached_commit_ignores_a_sibling_version() {
    let (_, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p_docs = docs("p", 2);
    let p = append(&env, &v1, &schema, &p_docs).await;
    let z_docs = docs("z", 3);
    let l_docs = docs("l", 2);
    let (z, l) = tokio::join!(
        append(&env, &p, &schema, &z_docs),
        append(&env, &p, &schema, &l_docs)
    );
    assert_ne!(z.manifest.version, l.manifest.version);
    assert!(is_detached_version(z.manifest.version));
    assert!(is_detached_version(l.manifest.version));
    let reopened = env
        .open(NS, CID, l.manifest.version)
        .await
        .expect("reopen L");
    assert_eq!(pks(&reopened).await, keys(&[&p_docs, &l_docs]));
    let reopened = env
        .open(NS, CID, z.manifest.version)
        .await
        .expect("reopen Z");
    assert_eq!(pks(&reopened).await, keys(&[&p_docs, &z_docs]));
}

#[tokio::test]
async fn mainline_never_moves_past_version_one() {
    let (_, store, env) = env();
    let schema = plain_schema();
    let mut parent = env.ensure_created(NS, CID).await.expect("create");
    for i in 0..6 {
        parent = append(&env, &parent, &schema, &docs(&format!("r{i}-"), 2)).await;
    }
    // A re-creation attempt finds version 1 and commits nothing.
    env.ensure_created(NS, CID).await.expect("ensure again");
    let names = manifests(&store).await;
    let mainline: Vec<&String> = names.iter().filter(|n| !n.starts_with('d')).collect();
    assert_eq!(mainline, vec![&version_one_manifest()], "{names:?}");
    assert_eq!(names.len(), 7, "{names:?}");
}

#[tokio::test]
async fn an_orphan_detached_version_never_blocks_the_next_commit() {
    let (_, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p_docs = docs("p", 2);
    let p = append(&env, &v1, &schema, &p_docs).await;
    // A crashed writer's version: committed, never referenced.
    drop(append(&env, &p, &schema, &docs("o", 4)).await);
    let l_docs = docs("l", 1);
    let l = append(&env, &p, &schema, &l_docs).await;
    assert_eq!(pks(&l).await, keys(&[&p_docs, &l_docs]));
}

#[tokio::test]
async fn stable_row_ids_are_fresh_and_survive_deletes() {
    let (_, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let first = docs("a", 4);
    let a = append(&env, &v1, &schema, &first).await;
    let mut ids = LanceCommitter::new_row_ids(&a, &v1).await.expect("row ids");
    ids.sort_by_key(|(_, row_id)| *row_id);
    let n = ids[0].1;
    assert_eq!(
        ids.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        (n..n + 4).collect::<Vec<_>>()
    );
    let id_of = |pk: &PrimaryKey| {
        ids.iter()
            .find(|(bytes, _)| *bytes == pk.canonical())
            .map(|(_, id)| *id)
            .expect("row id of pk")
    };
    let survivors = [id_of(&first[0].pk), id_of(&first[3].pk)];
    let doomed = [id_of(&first[1].pk), id_of(&first[2].pk)];

    let (updated, removed) = LanceCommitter::delete_rows(&a, &doomed)
        .await
        .expect("delete rows");
    assert!(removed.is_empty());
    let extra = docs("b", 1);
    let new = LanceCommitter::write_fragments(&env, &a, batch(&schema, &extra))
        .await
        .expect("write");
    let b = LanceCommitter::commit(&env, &a, update(new, updated, removed))
        .await
        .expect("commit update");
    let added = LanceCommitter::new_row_ids(&b, &a).await.expect("new ids");
    assert_eq!(added, vec![(extra[0].pk.canonical(), n + 4)]);
    assert_eq!(
        pks(&b).await,
        keys(&[&[first[0].clone(), first[3].clone()], &extra])
    );
    let taken = b
        .take_rows(&survivors, b.schema().clone())
        .await
        .expect("take rows");
    let taken: Vec<PrimaryKey> = (0..taken.num_rows())
        .map(|row| row_from_batch(&schema, &taken, row).expect("row").pk)
        .collect();
    assert_eq!(taken, vec![first[0].pk.clone(), first[3].pk.clone()]);

    // Deleting every row of a fragment removes it.
    let (updated, removed) = LanceCommitter::delete_rows(&b, &[n + 4])
        .await
        .expect("delete the only row of a fragment");
    assert!(updated.is_empty());
    let added_fragment = b
        .manifest
        .fragments
        .iter()
        .map(|f| f.id)
        .max()
        .expect("fragments");
    assert_eq!(removed, vec![added_fragment]);
    let c = LanceCommitter::commit(&env, &b, update(vec![], updated, removed))
        .await
        .expect("commit removal");
    assert_eq!(
        pks(&c).await,
        keys(&[&[first[0].clone(), first[3].clone()]])
    );

    // A row id that is not in the version is corrupt input.
    assert!(matches!(
        LanceCommitter::delete_rows(&c, &[n + 1_000]).await,
        Err(CollectionError::Corrupt(_))
    ));
}

#[tokio::test]
async fn new_row_ids_join_on_the_key() {
    let (_, store) = faulty();
    let config = LanceConfig {
        max_rows_per_file: 400,
        ..LanceConfig::default()
    };
    let env = LanceEnv::new(store, config);
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let input = docs("k", 1_000);
    let fragments = LanceCommitter::write_fragments(&env, &v1, batch(&schema, &input))
        .await
        .expect("write");
    assert_eq!(fragments.len(), 3);
    let committed = LanceCommitter::commit(&env, &v1, Operation::Append { fragments })
        .await
        .expect("commit");
    let pairs = LanceCommitter::new_row_ids(&committed, &v1)
        .await
        .expect("row ids");
    assert_eq!(pairs.len(), 1_000);
    let got: BTreeSet<Vec<u8>> = pairs.iter().map(|(pk, _)| pk.clone()).collect();
    let want: BTreeSet<Vec<u8>> = input.iter().map(|doc| doc.pk.canonical()).collect();
    assert_eq!(got, want);
    let row_ids: BTreeSet<u64> = pairs.iter().map(|(_, id)| *id).collect();
    assert_eq!(row_ids.len(), 1_000);
    // Each pair is the key and row id of the same row.
    let (pk, row_id) = &pairs[537];
    let taken = committed
        .take_rows(&[*row_id], committed.schema().clone())
        .await
        .expect("take");
    assert_eq!(
        row_from_batch(&schema, &taken, 0)
            .expect("row")
            .pk
            .canonical(),
        *pk
    );
}

#[tokio::test]
async fn adding_a_vector_adds_a_null_column_lazily() {
    let (_, _, env) = env();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let one = CollectionSchema::new(vec![], vec![vector("a", 3)], DynamicMapping::Strict);
    let p = LanceCommitter::ensure_vectors(&env, &v1, &one)
        .await
        .expect("add _vector_0");
    assert!(p.schema().field(&vector_column(0)).is_some());
    let mut with_vector = docs("v", 2);
    for (i, doc) in with_vector.iter_mut().enumerate() {
        let x = i as f32;
        doc.vectors.insert("a".to_string(), vec![x, x + 0.5, -x]);
    }
    let a = append(&env, &p, &one, &with_vector).await;

    let two = CollectionSchema::new(
        vec![],
        vec![vector("a", 3), vector("b", 2)],
        DynamicMapping::Strict,
    );
    let m = LanceCommitter::ensure_vectors(&env, &a, &two)
        .await
        .expect("add _vector_1");
    assert!(is_detached_version(m.manifest.version));
    assert_ne!(m.manifest.version, a.manifest.version);
    let rows = scan_all(&m).await;
    let added = rows
        .column_by_name(&vector_column(1))
        .expect("_vector_1")
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("fixed size list");
    assert_eq!(added.len(), 2);
    assert_eq!(added.null_count(), 2);
    for row in 0..rows.num_rows() {
        let stored = row_from_batch(&two, &rows, row).expect("row");
        assert_eq!(stored.vectors.keys().collect::<Vec<_>>(), vec!["a"]);
        let original = with_vector
            .iter()
            .find(|doc| doc.pk == stored.pk)
            .expect("original");
        assert_eq!(stored.vectors["a"], original.vectors["a"]);
    }
    let again = LanceCommitter::ensure_vectors(&env, &m, &two)
        .await
        .expect("nothing to add");
    assert!(Arc::ptr_eq(&again, &m));
}

#[tokio::test]
async fn sparse_vectors_round_trip_through_lance() {
    let (_, _, env) = env();
    let schema =
        CollectionSchema::new(vec![], vec![], DynamicMapping::Strict).with_sparse_vectors(vec![
            SparseVectorSpec {
                name: "a".to_string(),
                modifier: SparseModifier::None,
            },
            SparseVectorSpec {
                name: "b".to_string(),
                modifier: SparseModifier::Idf,
            },
        ]);
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p = LanceCommitter::ensure_vectors(&env, &v1, &schema)
        .await
        .expect("add sparse columns");
    assert!(p.schema().field(&sparse_column(0)).is_some());
    assert!(p.schema().field(&sparse_column(1)).is_some());

    let mut input = docs("s", 3);
    input[0]
        .sparse_vectors
        .insert("a".to_string(), sparse(&[1, 7], &[0.5, 0.0]));
    input[1]
        .sparse_vectors
        .insert("a".to_string(), sparse(&[], &[]));
    let want: BTreeMap<PrimaryKey, StoredRow> = input
        .iter()
        .zip(0u64..)
        .map(|(doc, offset)| {
            let row = StoredRow {
                pk: doc.pk.clone(),
                source: doc.source.clone(),
                vectors: BTreeMap::new(),
                sparse_vectors: doc.sparse_vectors.clone(),
                partition: 0,
                offset,
            };
            (doc.pk.clone(), row)
        })
        .collect();

    // Directly…
    let direct = batch(&schema, &input);
    for row in 0..direct.num_rows() {
        let stored = row_from_batch(&schema, &direct, row).expect("row");
        assert_eq!(stored, want[&stored.pk]);
    }
    // …and through Lance.
    let committed = append(&env, &p, &schema, &input).await;
    let rows = scan_all(&committed).await;
    assert_eq!(rows.num_rows(), 3);
    for row in 0..rows.num_rows() {
        let stored = row_from_batch(&schema, &rows, row).expect("row");
        assert_eq!(stored, want[&stored.pk]);
    }
    let empty = &want[&input[1].pk].sparse_vectors["a"];
    assert!(empty.is_empty());
}

#[tokio::test]
async fn lance_io_goes_through_the_faulty_store() {
    let (faulty, store, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p = append(&env, &v1, &schema, &docs("p", 2)).await;

    let gets = faulty.calls(Op::Get);
    env.open(NS, CID, p.manifest.version).await.expect("open");
    assert!(faulty.calls(Op::Get) > gets, "open read nothing");

    // A precondition failure on the manifest's create-only write: Lance draws
    // a new random version and writes exactly one manifest.
    let before = detached_manifests(&store).await;
    faulty.inject(Op::PutCreate, Fault::Precondition);
    let landed = append(&env, &p, &schema, &docs("q", 1)).await;
    assert_eq!(
        faulty.pending(Op::PutCreate),
        0,
        "the fault was not consumed"
    );
    let after = detached_manifests(&store).await;
    let new: Vec<&String> = after.difference(&before).collect();
    assert_eq!(new, vec![&format!("d{}.manifest", landed.manifest.version)]);

    // A write that lands but reports failure: Lance reads it back and
    // returns the version that landed.
    let before = after;
    faulty.inject(Op::PutCreate, Fault::ErrorAfterApply);
    let landed = append(&env, &p, &schema, &docs("r", 1)).await;
    assert_eq!(
        faulty.pending(Op::PutCreate),
        0,
        "the fault was not consumed"
    );
    let after = detached_manifests(&store).await;
    let new: Vec<&String> = after.difference(&before).collect();
    assert_eq!(new, vec![&format!("d{}.manifest", landed.manifest.version)]);
    let reopened = env
        .open(NS, CID, landed.manifest.version)
        .await
        .expect("reopen");
    assert_eq!(reopened.manifest.version, landed.manifest.version);
}

#[tokio::test]
async fn an_unverifiable_commit_is_retryable() {
    let (faulty, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p_docs = docs("p", 2);
    let p = append(&env, &v1, &schema, &p_docs).await;
    let q_docs = docs("q", 1);
    let fragments = LanceCommitter::write_fragments(&env, &p, batch(&schema, &q_docs))
        .await
        .expect("write");
    // The manifest write fails, and so does every read that could tell
    // whether it landed: Lance cannot know the outcome.
    faulty.inject(Op::PutCreate, Fault::Error);
    for _ in 0..64 {
        faulty.inject(Op::Get, Fault::Error);
    }
    let err = LanceCommitter::commit(
        &env,
        &p,
        Operation::Append {
            fragments: fragments.clone(),
        },
    )
    .await
    .expect_err("unverifiable commit");
    faulty.clear();
    assert!(
        matches!(&err, CollectionError::Lance(inner) if inner.is_commit_status_unknown()),
        "{err}"
    );
    assert!(err.is_retryable(), "{err}");
    // Retrying the whole commit on the same parent is safe.
    let retried = LanceCommitter::commit(&env, &p, Operation::Append { fragments })
        .await
        .expect("retry");
    assert_eq!(pks(&retried).await, keys(&[&p_docs, &q_docs]));
}

#[tokio::test]
async fn a_lance_io_error_is_retryable() {
    let (faulty, store, env) = env();
    env.ensure_created(NS, CID).await.expect("create");
    // A fresh environment has no cached manifest, so opening must read.
    let cold = LanceEnv::new(store, LanceConfig::default());
    for _ in 0..16 {
        faulty.inject(Op::Get, Fault::Error);
    }
    let err = cold
        .open(NS, CID, 1)
        .await
        .expect_err("injected read error");
    faulty.clear();
    assert!(
        matches!(&err, CollectionError::Lance(lance::Error::IO { .. })),
        "{err:?}"
    );
    assert!(err.is_retryable(), "{err}");
    cold.open(NS, CID, 1).await.expect("open after the faults");
}

#[tokio::test]
async fn no_auto_cleanup_is_ever_configured() {
    let (_, _, env) = env();
    let schema = CollectionSchema::new(vec![], vec![vector("a", 2)], DynamicMapping::Strict);
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let p = LanceCommitter::ensure_vectors(&env, &v1, &schema)
        .await
        .expect("vectors");
    let a = append(&env, &p, &schema, &docs("a", 2)).await;
    for dataset in [&v1, &p, &a] {
        let keys: Vec<&String> = dataset
            .manifest
            .config
            .keys()
            .filter(|key| key.starts_with("lance.auto_cleanup."))
            .collect();
        assert!(keys.is_empty(), "{keys:?}");
    }
    let params = env.write_params();
    assert!(params.auto_cleanup.is_none());
    assert!(params.skip_auto_cleanup);
    assert!(params.enable_stable_row_ids);
    assert_eq!(params.data_storage_version, Some(LanceFileVersion::V2_1));
}

#[tokio::test]
async fn the_ingest_columns_hold_the_record_that_wrote_the_row() {
    let (_, _, env) = env();
    let schema = plain_schema();
    let v1 = env.ensure_created(NS, CID).await.expect("create");
    let input = docs("i", 2);
    let rows = [
        NewRow {
            doc: &input[0],
            partition: 3,
            offset: 17,
        },
        NewRow {
            doc: &input[1],
            partition: 5,
            offset: u64::MAX - 1,
        },
    ];
    let fragments =
        LanceCommitter::write_fragments(&env, &v1, to_record_batch(&schema, &rows).expect("batch"))
            .await
            .expect("write");
    let committed = LanceCommitter::commit(&env, &v1, Operation::Append { fragments })
        .await
        .expect("commit");
    let batch = scan_all(&committed).await;
    assert!(batch.column_by_name(INGEST_OFFSET_COLUMN).is_some());
    let stored: BTreeSet<(u32, u64)> = (0..batch.num_rows())
        .map(|row| {
            let row = row_from_batch(&schema, &batch, row).expect("row");
            (row.partition, row.offset)
        })
        .collect();
    assert_eq!(stored, BTreeSet::from([(3, 17), (5, u64::MAX - 1)]));
}
