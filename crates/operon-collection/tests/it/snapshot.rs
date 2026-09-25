//! `CollectionSnapshot`, the collection read API (plan M1.1 Task 9): manifests
//! assembled by hand over Task 7 Lance versions and Task 8 splits.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{Meta, collection, doc, faulty_store, namespace, sparse, text, vector};
use lance::Dataset;
use lance::dataset::transaction::Operation;
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionConfig, CollectionContext, CollectionError, CollectionManifest, CollectionSchema,
    CollectionSnapshot, CommitKind, Document, DynamicMapping, LanceCommitter, LanceConfig,
    LanceEnv, ManifestCache, NewRow, PrimaryKey, SparseModifier, SparseVectorSpec, SplitRef,
    StoredDoc, check_document, delete_bitmap_path, encode_manifest, manifest_path, split_path,
    tantivy_layout, to_record_batch, to_tantivy_doc,
};
use operon_common::{CollectionId, NamespaceId};
use operon_meta::{Consistency, collection_pointer_key};
use operon_store::{FaultyStore, Op, Store};
use operon_text::{build_split, encode_delete_bitmap, warm_up_all};
use roaring::RoaringBitmap;
use serde_json::json;
use ulid::Ulid;

fn schema() -> CollectionSchema {
    let schema = CollectionSchema::new(
        vec![text("title")],
        vec![vector("v", 3)],
        DynamicMapping::Strict,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    schema.validate().expect("valid schema");
    schema
}

/// Document `n`: `v` on even keys, `s` on keys divisible by 3.
fn document(n: u64) -> Document {
    let mut d = doc(PrimaryKey::U64(n), json!({ "title": format!("doc {n}") }));
    if n.is_multiple_of(2) {
        d.vectors.insert("v".to_string(), vec![n as f32, 1.0, -1.0]);
    }
    if n.is_multiple_of(3) {
        d.sparse_vectors
            .insert("s".to_string(), sparse(&[1, n as u32 + 10], &[0.5, 2.0]));
    }
    d
}

/// The offset document `n` is written at.
fn offset_of(n: u64) -> u64 {
    100 + n
}

struct Fixture {
    meta: Meta,
    faulty: Arc<FaultyStore>,
    ctx: CollectionContext,
    ns: NamespaceId,
    cid: CollectionId,
    schema: CollectionSchema,
    /// The live manifest (path, manifest) and its dataset.
    live: Option<(String, Arc<CollectionManifest>, Arc<Dataset>)>,
    /// pk → row id, of every live row.
    rows: BTreeMap<u64, u64>,
}

async fn range_cache(store: &Store) -> RangeCache {
    RangeCache::new(
        store.clone(),
        RangeCacheConfig {
            block_size: 1 << 20,
            memory_bytes: 64 << 20,
            disk: None,
        },
    )
    .await
    .expect("range cache")
}

async fn context(meta: &Meta, store: &Store, config: CollectionConfig) -> CollectionContext {
    CollectionContext {
        meta: meta.client.clone().into(),
        store: store.clone(),
        cache: range_cache(store).await,
        lance: LanceEnv::new(store.clone(), LanceConfig::default()),
        manifests: ManifestCache::new(config.manifest_cache_entries),
        config,
    }
}

/// A new object's ULID: its time is the metastore client's clock.
fn object_ulid(now_ms: u64) -> Ulid {
    Ulid::from_parts(now_ms, Ulid::generate().random())
}

/// Maximal runs of consecutive ids, in order.
fn runs(ids: &[u64]) -> Vec<Range<u64>> {
    let mut out: Vec<Range<u64>> = Vec::new();
    for &id in ids {
        match out.last_mut() {
            Some(last) if last.end == id => last.end += 1,
            _ => out.push(id..id + 1),
        }
    }
    out
}

impl Fixture {
    async fn start() -> Self {
        let meta = Meta::start().await;
        let (faulty, store) = faulty_store();
        let ns = namespace(&meta.client, "n").await;
        let schema = schema();
        let (cid, _) = collection(&meta.client, ns, "docs", schema.clone(), 1).await;
        let ctx = context(&meta, &store, CollectionConfig::default()).await;
        Self {
            meta,
            faulty,
            ctx,
            ns,
            cid,
            schema,
            live: None,
            rows: BTreeMap::new(),
        }
    }

    /// One commit, as Task 10 will make it: upserts of new keys `add` and
    /// deletes of `delete` in one Lance version, one new split, the changed
    /// delete bitmaps, the manifest, then the CAS.
    async fn commit(&mut self, add: &[u64], delete: &[u64]) {
        let ctx = &self.ctx;
        let parent = match &self.live {
            Some((_, _, dataset)) => dataset.clone(),
            None => {
                let one = ctx
                    .lance
                    .ensure_created(self.ns, self.cid)
                    .await
                    .expect("version 1");
                LanceCommitter::ensure_vectors(&ctx.lance, &one, &self.schema)
                    .await
                    .expect("vector columns")
            }
        };
        let docs: Vec<Document> = add.iter().map(|&n| document(n)).collect();
        let rows: Vec<NewRow<'_>> = docs
            .iter()
            .zip(add)
            .map(|(doc, &n)| NewRow {
                doc,
                partition: 0,
                offset: offset_of(n),
            })
            .collect();
        let new_fragments = LanceCommitter::write_fragments(
            &ctx.lance,
            &parent,
            to_record_batch(&self.schema, &rows).expect("batch"),
        )
        .await
        .expect("fragments");
        let deleted_rows: Vec<u64> = delete.iter().map(|n| self.rows[n]).collect();
        let (updated, removed) = LanceCommitter::delete_rows(&parent, &deleted_rows)
            .await
            .expect("deletions");
        let operation = Operation::Update {
            removed_fragment_ids: removed,
            updated_fragments: updated,
            new_fragments,
            fields_modified: vec![],
            compacted_sstables: vec![],
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: None,
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        };
        let dataset = LanceCommitter::commit(&ctx.lance, &parent, operation)
            .await
            .expect("lance commit");
        let mut new_rows: Vec<(u64, u64)> = LanceCommitter::new_row_ids(&dataset, &parent)
            .await
            .expect("new row ids")
            .into_iter()
            .map(
                |(pk, row_id)| match PrimaryKey::from_canonical(&pk).expect("pk") {
                    PrimaryKey::U64(n) => (row_id, n),
                    other => panic!("unexpected key {other:?}"),
                },
            )
            .collect();
        new_rows.sort_unstable();
        for n in delete {
            self.rows.remove(n);
        }

        let now = ctx.meta.now_ms();
        let (parent_path, mut manifest) = match &self.live {
            Some((path, manifest, _)) => (Some(path.clone()), (**manifest).clone()),
            None => (None, CollectionManifest::empty(self.cid)),
        };
        // The delete bitmaps of the old splits.
        let locator = operon_collection::RowLocator::new(&manifest.splits).expect("locator");
        let mut deleted: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
        for row_id in &deleted_rows {
            let (split, doc_id) = locator.locate(*row_id).expect("row in a split");
            deleted.entry(split).or_default().insert(doc_id);
        }
        for (index, docs) in deleted {
            let split = &mut manifest.splits[index];
            let mut bitmap = RoaringBitmap::new();
            if let Some(path) = &split.delete_bitmap {
                let (bytes, _) = ctx.store.get(path).await.expect("old bitmap");
                bitmap = operon_text::decode_delete_bitmap(&bytes).expect("bitmap").2;
            }
            bitmap |= docs;
            let path = delete_bitmap_path(self.ns, self.cid, split.ulid, object_ulid(now));
            let bytes = encode_delete_bitmap(split.ulid, split.doc_count as u32, &bitmap)
                .expect("encode bitmap");
            ctx.store.put_if_absent(&path, bytes).await.expect("bitmap");
            split.deleted_count = bitmap.len();
            split.delete_bitmap = Some(path);
        }
        // The new split, in ascending row-id order.
        if !new_rows.is_empty() {
            let layout = tantivy_layout(&self.schema);
            let tantivy_docs = new_rows
                .iter()
                .map(|&(row_id, n)| {
                    let doc = document(n);
                    let extracted = check_document(&self.schema, &doc).expect("valid doc");
                    to_tantivy_doc(&layout, &self.schema, &doc, &extracted, row_id)
                })
                .collect();
            let built = build_split(layout.schema.clone(), tantivy_docs).expect("split");
            let ulid = object_ulid(now);
            ctx.store
                .put_if_absent(&split_path(self.ns, self.cid, ulid), built.bytes.clone())
                .await
                .expect("put split");
            let ids: Vec<u64> = new_rows.iter().map(|(row_id, _)| *row_id).collect();
            manifest.splits.push(SplitRef {
                ulid,
                doc_count: built.doc_count,
                deleted_count: 0,
                size_bytes: built.bytes.len() as u64,
                footer_range: built.footer_range.clone(),
                row_id_ranges: runs(&ids),
                delete_bitmap: None,
                schema_version: self.schema.version,
                created_at_ms: now,
                merge_ops: 0,
            });
        }
        for &(row_id, n) in &new_rows {
            self.rows.insert(n, row_id);
        }
        let version = manifest.version + 1;
        let applied = add.iter().chain(delete).map(|&n| offset_of(n) + 1).max();
        manifest = CollectionManifest {
            version,
            parent_version: manifest.version,
            parent_manifest: parent_path,
            schema_version: self.schema.version,
            created_at_ms: now,
            lance_version: dataset.manifest.version,
            applied: BTreeMap::from([(0, applied.unwrap_or(0))]),
            live_doc_count: self.rows.len() as u64,
            kind: CommitKind::LinkApply,
            pk_delta: None,
            dead_letters: None,
            ..manifest
        };
        let path = manifest_path(self.ns, self.cid, version, object_ulid(now));
        ctx.store
            .put_if_absent(&path, encode_manifest(&manifest))
            .await
            .expect("put manifest");
        let expected = (version > 1).then_some(version - 1);
        let pointer = self
            .meta
            .client
            .cas_pointer(
                self.ns,
                &collection_pointer_key(self.cid),
                expected,
                &path,
                None,
            )
            .await
            .expect("cas");
        assert_eq!(pointer, version, "pointer version = manifest version");
        self.live = Some((path, Arc::new(manifest), dataset));
    }

    async fn open(&self) -> CollectionSnapshot {
        CollectionSnapshot::open(&self.ctx, self.ns, self.cid, Consistency::Linearizable)
            .await
            .expect("open")
    }
}

fn keys(docs: &[StoredDoc]) -> Vec<u64> {
    docs.iter()
        .map(|d| match d.pk {
            PrimaryKey::U64(n) => n,
            ref other => panic!("unexpected key {other:?}"),
        })
        .collect()
}

/// `doc` is document `n` as written.
fn assert_is(doc: &StoredDoc, n: u64, row_id: u64) {
    let want = document(n);
    assert_eq!(doc.pk, want.pk);
    assert_eq!(doc.row_id, row_id);
    assert_eq!(doc.source, want.source);
    assert_eq!(doc.vectors, want.vectors, "dense vectors of {n}");
    assert_eq!(
        doc.sparse_vectors, want.sparse_vectors,
        "sparse vectors of {n}"
    );
    assert_eq!(doc.partition, 0);
    assert_eq!(doc.seq_no, offset_of(n));
}

#[tokio::test]
async fn a_collection_without_commits_is_empty() {
    let fx = Fixture::start().await;
    let snap = fx.open().await;
    assert_eq!(snap.manifest(), &CollectionManifest::empty(fx.cid));
    assert_eq!(snap.manifest_path(), None);
    assert!(snap.dataset().is_none());
    assert!(snap.splits().is_empty());
    assert_eq!(snap.collection().id, fx.cid);
    assert_eq!(snap.locate_row(0), None);
    assert_eq!(snap.take_rows(&[0, 1]).await.unwrap(), [None, None]);
    assert_eq!(snap.get_by_pk(&[PrimaryKey::U64(1)]).await.unwrap(), [None]);
    assert!(snap.scan_all().await.unwrap().is_empty());

    let missing = CollectionSnapshot::open(
        &fx.ctx,
        fx.ns,
        CollectionId(fx.cid.0 + 100),
        Consistency::Local,
    )
    .await;
    assert!(
        matches!(missing, Err(CollectionError::NotFound(_))),
        "{missing:?}"
    );
    let wrong_ns = CollectionSnapshot::open(
        &fx.ctx,
        NamespaceId(fx.ns.0 + 1),
        fx.cid,
        Consistency::Local,
    )
    .await;
    assert!(
        matches!(wrong_ns, Err(CollectionError::NotFound(_))),
        "{wrong_ns:?}"
    );
    fx.meta.shutdown().await;
}

#[tokio::test]
async fn a_snapshot_reads_exactly_its_lance_version_and_splits() {
    let mut fx = Fixture::start().await;
    fx.commit(&[1, 2, 3, 4, 5], &[]).await;
    let first = fx.open().await;
    let first_rows = fx.rows.clone();
    fx.commit(&[6, 7], &[2]).await;
    let second = fx.open().await;

    // The first snapshot still reads version 1, the second version 2.
    assert_eq!(first.manifest().version, 1);
    assert_eq!(second.manifest().version, 2);
    for snap in [&first, &second] {
        assert_eq!(
            snap.dataset().expect("dataset").manifest.version,
            snap.manifest().lance_version
        );
        assert_eq!(
            snap.manifest_path()
                .and_then(operon_collection::manifest_version),
            Some(snap.manifest().version)
        );
    }
    assert_eq!(keys(&first.scan_all().await.unwrap()), [1, 2, 3, 4, 5]);
    assert_eq!(keys(&second.scan_all().await.unwrap()), [1, 3, 4, 5, 6, 7]);
    for doc in second.scan_all().await.unwrap() {
        let PrimaryKey::U64(n) = doc.pk else {
            panic!("u64 keys")
        };
        assert_is(&doc, n, fx.rows[&n]);
    }
    assert_eq!(first.splits().len(), 1);
    assert_eq!(second.splits().len(), 2);

    // Doc 2 is deleted in the second snapshot's bitmap of the first split,
    // and nowhere in the first snapshot.
    let (split, doc_id) = second.locate_row(first_rows[&2]).expect("located");
    assert_eq!(split, 0);
    assert!(
        first
            .deleted_docs(&first.splits()[0])
            .await
            .unwrap()
            .is_empty()
    );
    let deleted = second.deleted_docs(&second.splits()[0]).await.unwrap();
    assert_eq!(deleted.iter().collect::<Vec<_>>(), [doc_id]);
    assert!(
        second
            .deleted_docs(&second.splits()[1])
            .await
            .unwrap()
            .is_empty()
    );

    // Every row id locates to the split doc holding that row id.
    for (snap, rows) in [(&first, &first_rows), (&second, &fx.rows)] {
        let mut searchers = Vec::new();
        for split in snap.splits() {
            let index = snap.open_split(split).await.expect("open split");
            searchers.push(warm_up_all(&index).await.expect("warm"));
        }
        for (&n, &row_id) in rows {
            let (split, doc_id) = snap.locate_row(row_id).expect("located");
            let rowids = searchers[split]
                .segment_reader(0)
                .fast_fields()
                .u64("_rowid")
                .expect("_rowid");
            assert_eq!(rowids.first(doc_id), Some(row_id), "doc {n}");
        }
        assert_eq!(
            searchers.iter().map(|s| s.num_docs()).sum::<u64>(),
            snap.splits().iter().map(|s| s.doc_count).sum::<u64>()
        );
    }
    fx.meta.shutdown().await;
}

#[tokio::test]
async fn get_by_pk_returns_documents_in_input_order_with_misses() {
    let mut fx = Fixture::start().await;
    fx.commit(&[1, 2, 3, 4, 5], &[]).await;
    fx.commit(&[6], &[2]).await;
    let snap = fx.open().await;
    let pks: Vec<PrimaryKey> = [5, 99, 1, 2, 6, 5]
        .into_iter()
        .map(PrimaryKey::U64)
        .collect();
    let got = snap.get_by_pk(&pks).await.unwrap();
    assert_eq!(got.len(), pks.len());
    assert_is(got[0].as_ref().expect("5"), 5, fx.rows[&5]);
    assert_eq!(got[1], None, "never written");
    assert_is(got[2].as_ref().expect("1"), 1, fx.rows[&1]);
    assert_eq!(got[3], None, "deleted");
    assert_is(got[4].as_ref().expect("6"), 6, fx.rows[&6]);
    assert_eq!(got[5], got[0], "a repeated key");
    assert!(snap.get_by_pk(&[]).await.unwrap().is_empty());

    // Lookups are split into rounds of max_lookup_batch keys.
    let mut ctx = fx.ctx.clone();
    ctx.config.max_lookup_batch = 2;
    let small = CollectionSnapshot::open(&ctx, fx.ns, fx.cid, Consistency::Local)
        .await
        .unwrap();
    assert_eq!(small.get_by_pk(&pks).await.unwrap(), got);
    fx.meta.shutdown().await;
}

#[tokio::test]
async fn take_rows_returns_seq_no_and_vectors() {
    let mut fx = Fixture::start().await;
    fx.commit(&[1, 2, 3, 4, 5, 6], &[]).await;
    let deleted_row = fx.rows[&3];
    fx.commit(&[7], &[3]).await;
    let snap = fx.open().await;
    // 6 has both vectors, 2 and 4 only the dense one, 3 (deleted) the sparse
    // one, 1 neither.
    let ids = [
        fx.rows[&6],
        u64::MAX - 1,
        fx.rows[&1],
        deleted_row,
        fx.rows[&2],
        fx.rows[&6],
    ];
    let got = snap.take_rows(&ids).await.unwrap();
    assert_is(got[0].as_ref().expect("6"), 6, fx.rows[&6]);
    assert!(got[0].as_ref().unwrap().vectors.contains_key("v"));
    assert!(got[0].as_ref().unwrap().sparse_vectors.contains_key("s"));
    assert_eq!(got[1], None, "a row id this version does not have");
    let one = got[2].as_ref().expect("1");
    assert_is(one, 1, fx.rows[&1]);
    assert!(one.vectors.is_empty() && one.sparse_vectors.is_empty());
    assert_eq!(got[3], None, "a deleted row");
    assert_is(got[4].as_ref().expect("2"), 2, fx.rows[&2]);
    assert_eq!(got[5], got[0]);
    assert!(snap.take_rows(&[]).await.unwrap().is_empty());

    let mut ctx = fx.ctx.clone();
    ctx.config.max_lookup_batch = 1;
    let small = CollectionSnapshot::open(&ctx, fx.ns, fx.cid, Consistency::Local)
        .await
        .unwrap();
    assert_eq!(small.take_rows(&ids).await.unwrap(), got);
    fx.meta.shutdown().await;
}

#[tokio::test]
async fn open_version_of_a_retained_manifest_works_and_a_gone_one_fails() {
    let mut fx = Fixture::start().await;
    // Before the first commit only the empty version 0 exists.
    let empty = CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 0)
        .await
        .expect("version 0");
    assert_eq!(empty.manifest(), &CollectionManifest::empty(fx.cid));
    assert!(matches!(
        CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 1).await,
        Err(CollectionError::ManifestGone(1))
    ));

    fx.commit(&[1, 2], &[]).await;
    fx.commit(&[3], &[1]).await;
    fx.commit(&[4], &[]).await;

    // Default config: keep 10, so every version is retained.
    let pinned = CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 1)
        .await
        .expect("version 1");
    assert_eq!(pinned.manifest().version, 1);
    assert_eq!(keys(&pinned.scan_all().await.unwrap()), [1, 2]);
    let live = CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 3)
        .await
        .expect("version 3");
    assert_eq!(live.manifest(), fx.open().await.manifest());
    assert!(matches!(
        CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 4).await,
        Err(CollectionError::ManifestGone(4))
    ));
    assert!(matches!(
        CollectionSnapshot::open_version(&fx.ctx, fx.ns, fx.cid, 0).await,
        Err(CollectionError::ManifestGone(0))
    ));

    // Keep one ancestor and no time travel: version 2 is retained, 1 is gone.
    // The metastore clock moves only with time-stamped commands, so advance
    // it past the last manifest's creation.
    tokio::time::sleep(Duration::from_millis(5)).await;
    fx.meta
        .client
        .acquire_lease("clock", "test", Duration::from_secs(1))
        .await
        .expect("advance the metastore clock");
    let config = CollectionConfig {
        keep_manifests: 1,
        time_travel_retention: Duration::ZERO,
        ..CollectionConfig::default()
    };
    let strict = context(&fx.meta, &fx.ctx.store, config).await;
    let two = CollectionSnapshot::open_version(&strict, fx.ns, fx.cid, 2)
        .await
        .expect("version 2");
    assert_eq!(keys(&two.scan_all().await.unwrap()), [2, 3]);
    assert!(matches!(
        CollectionSnapshot::open_version(&strict, fx.ns, fx.cid, 1).await,
        Err(CollectionError::ManifestGone(1))
    ));
    fx.meta.shutdown().await;
}

#[tokio::test]
async fn at_equals_open_for_the_same_manifest() {
    let mut fx = Fixture::start().await;
    fx.commit(&[1, 2, 3, 4], &[]).await;
    fx.commit(&[5, 6], &[3]).await;
    let opened = fx.open().await;
    let collection = opened.collection().clone();
    let path = opened.manifest_path().map(str::to_string);
    let manifest = Arc::new(opened.manifest().clone());

    // With the metastore shut down, a metastore read fails, but `at` works.
    fx.meta.shutdown().await;
    assert!(
        fx.meta
            .client
            .read(Consistency::Linearizable, |_| ())
            .await
            .is_err()
    );
    let at = CollectionSnapshot::at(&fx.ctx, fx.ns, collection, path, manifest)
        .await
        .expect("at");
    assert_eq!(at.manifest(), opened.manifest());
    assert_eq!(at.manifest_path(), opened.manifest_path());

    let mut ids: Vec<u64> = fx.rows.values().copied().collect();
    ids.extend([u64::MAX, 0, 1, 2, 3]);
    assert_eq!(
        at.take_rows(&ids).await.unwrap(),
        opened.take_rows(&ids).await.unwrap()
    );
    let pks: Vec<PrimaryKey> = (0..8).map(PrimaryKey::U64).collect();
    assert_eq!(
        at.get_by_pk(&pks).await.unwrap(),
        opened.get_by_pk(&pks).await.unwrap()
    );
    let max = fx.rows.values().max().copied().unwrap_or(0) + 3;
    for row_id in 0..=max {
        assert_eq!(at.locate_row(row_id), opened.locate_row(row_id), "{row_id}");
    }
    assert_eq!(
        at.scan_all().await.unwrap(),
        opened.scan_all().await.unwrap()
    );
}

#[tokio::test]
async fn opening_a_split_through_the_snapshot_is_one_get() {
    let mut fx = Fixture::start().await;
    fx.commit(&[1, 2, 3, 4, 5, 6, 7, 8], &[]).await;
    // A cold range cache.
    let ctx = context(&fx.meta, &fx.ctx.store, CollectionConfig::default()).await;
    let snap = CollectionSnapshot::open(&ctx, fx.ns, fx.cid, Consistency::Local)
        .await
        .expect("open");
    let split = snap.splits()[0].clone();
    assert!(split.footer_range.end - split.footer_range.start < 1 << 20);

    let gets = fx.faulty.calls(Op::Get);
    let index = snap.open_split(&split).await.expect("open split");
    assert_eq!(
        fx.faulty.calls(Op::Get),
        gets + 1,
        "one ranged GET, no HEAD"
    );
    let searcher = warm_up_all(&index).await.expect("warm");
    assert_eq!(searcher.num_docs(), 8);
    fx.meta.shutdown().await;
}
