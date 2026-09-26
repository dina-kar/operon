//! `CollectionSnapshot`, the collection read API (plan M1.1 Task 9; overview
//! §6.4, §6.5): one manifest version, its Lance version and its splits.
//! M1.2 builds its query engine on it.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, UInt64Array};
use lance::Dataset;
use lance::dataset::{ProjectionRequest, ROW_ID};
use lance::deps::datafusion::prelude::{col, lit};
use lance::deps::datafusion::scalar::ScalarValue;
use operon_cache::RangeCache;
use operon_common::meta::{Collection, CollectionHead, Consistency, MetaStore};
use operon_common::{CollectionId, NamespaceId};
use operon_store::Store;
use operon_text::{OperonStorage, decode_delete_bitmap};
use roaring::RoaringBitmap;
use serde_json::{Map, Value};

use crate::arrow_schema::{
    INGEST_OFFSET_COLUMN, INGEST_PARTITION_COLUMN, PK_COLUMN, SOURCE_COLUMN, row_from_batch,
    sparse_column, vector_column,
};
use crate::chain::{ManifestCache, load_pointed, retained_chain};
use crate::config::CollectionConfig;
use crate::doc::SparseVector;
use crate::error::CollectionError;
use crate::lance::LanceEnv;
use crate::manifest::{CollectionManifest, RowLocator, SplitRef};
use crate::paths::split_path;
use crate::pk::PrimaryKey;

/// Everything collection storage needs. Cheap to clone.
#[derive(Clone)]
pub struct CollectionContext {
    pub meta: Arc<dyn MetaStore>,
    pub store: Store,
    pub cache: RangeCache,
    pub lance: LanceEnv,
    pub manifests: ManifestCache,
    pub config: CollectionConfig,
}

impl fmt::Debug for CollectionContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionContext")
            .field("meta", &self.meta)
            .field("store", &self.store)
            .field("lance", &self.lance)
            .field("manifests", &self.manifests)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A document as a snapshot reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredDoc {
    pub pk: PrimaryKey,
    /// The Lance stable row id.
    pub row_id: u64,
    pub source: Map<String, Value>,
    /// Dense vectors by name; absent when null.
    pub vectors: BTreeMap<String, Vec<f32>>,
    /// Sparse vectors by name; absent when null.
    pub sparse_vectors: BTreeMap<String, SparseVector>,
    /// The partition of the record that last wrote the document.
    pub partition: u32,
    /// The offset of that record (`_ingest_offset`, overview A11).
    pub seq_no: u64,
}

/// One collection version: its manifest, the Lance version and the splits it
/// names. Splits open lazily. Cheap to clone.
#[derive(Clone)]
pub struct CollectionSnapshot {
    ctx: CollectionContext,
    namespace: NamespaceId,
    collection: Collection,
    manifest_path: Option<String>,
    manifest: Arc<CollectionManifest>,
    dataset: Option<Arc<Dataset>>,
    locator: RowLocator,
}

impl fmt::Debug for CollectionSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionSnapshot")
            .field("namespace", &self.namespace)
            .field("collection", &self.collection.id)
            .field("manifest_path", &self.manifest_path)
            .field("version", &self.manifest.version)
            .field("lance_version", &self.manifest.lance_version)
            .field("splits", &self.manifest.splits.len())
            .finish_non_exhaustive()
    }
}

/// The collection `cid` of namespace `ns` with its pointer and the metastore
/// clock, in one metastore read with `consistency`; else `NotFound`.
async fn head(
    ctx: &CollectionContext,
    ns: NamespaceId,
    cid: CollectionId,
    consistency: Consistency,
) -> Result<CollectionHead, CollectionError> {
    ctx.meta
        .collection_head(consistency, cid)
        .await?
        .filter(|head| head.collection.namespace == ns)
        .ok_or_else(|| CollectionError::NotFound(format!("collection {cid} in namespace {ns}")))
}

impl CollectionSnapshot {
    /// The live version of collection `cid`: the collection and its pointer in
    /// one metastore read with `consistency`, then the manifest the pointer
    /// names (`CollectionManifest::empty` before the first commit).
    pub async fn open(
        ctx: &CollectionContext,
        ns: NamespaceId,
        cid: CollectionId,
        consistency: Consistency,
    ) -> Result<Self, CollectionError> {
        let CollectionHead {
            collection,
            pointer,
            ..
        } = head(ctx, ns, cid, consistency).await?;
        match pointer {
            None => {
                let empty = Arc::new(CollectionManifest::empty(cid));
                Self::at(ctx, ns, collection, None, empty).await
            }
            Some(pointer) => {
                let manifest = load_pointed(
                    &ctx.store,
                    &ctx.manifests,
                    cid,
                    pointer.version,
                    &pointer.value,
                )
                .await?;
                Self::at(ctx, ns, collection, Some(pointer.value), manifest).await
            }
        }
    }

    /// A retained older manifest version (pinned reads); `ManifestGone(version)` when it is not retained.
    ///
    /// The retained set is the live manifest's [`retained_chain`] under
    /// `ctx.config`, at the metastore clock of a `Linearizable` read. Before
    /// the first commit only version 0 (the empty manifest) exists.
    pub async fn open_version(
        ctx: &CollectionContext,
        ns: NamespaceId,
        cid: CollectionId,
        version: u64,
    ) -> Result<Self, CollectionError> {
        let CollectionHead {
            collection,
            pointer,
            clock_ms,
            ..
        } = head(ctx, ns, cid, Consistency::Linearizable).await?;
        let Some(pointer) = pointer else {
            if version == 0 {
                let empty = Arc::new(CollectionManifest::empty(cid));
                return Self::at(ctx, ns, collection, None, empty).await;
            }
            return Err(CollectionError::ManifestGone(version));
        };
        let live = load_pointed(
            &ctx.store,
            &ctx.manifests,
            cid,
            pointer.version,
            &pointer.value,
        )
        .await?;
        if version > live.version {
            return Err(CollectionError::ManifestGone(version));
        }
        let chain = retained_chain(
            &ctx.store,
            &ctx.manifests,
            (pointer.value, live),
            ctx.config.keep_manifests,
            ctx.config.time_travel_retention,
            clock_ms,
        )
        .await?;
        let (path, manifest) = chain
            .into_iter()
            .find(|(_, manifest)| manifest.version == version)
            .ok_or(CollectionError::ManifestGone(version))?;
        Self::at(ctx, ns, collection, Some(path), manifest).await
    }

    /// A snapshot of an already loaded manifest (M1.2's tail, which holds its manifest); no metastore read.
    ///
    /// Opens the Lance dataset at `lance_version` (none when it is 0) and
    /// builds the [`RowLocator`].
    pub async fn at(
        ctx: &CollectionContext,
        ns: NamespaceId,
        collection: Collection,
        manifest_path: Option<String>,
        manifest: Arc<CollectionManifest>,
    ) -> Result<Self, CollectionError> {
        if manifest.collection_id != collection.id {
            return Err(CollectionError::Corrupt(format!(
                "manifest {manifest_path:?} is of collection {}, not {}",
                manifest.collection_id, collection.id
            )));
        }
        let locator = RowLocator::new(&manifest.splits)?;
        let dataset = match manifest.lance_version {
            0 => None,
            version => Some(ctx.lance.open(ns, collection.id, version).await?),
        };
        Ok(Self {
            ctx: ctx.clone(),
            namespace: ns,
            collection,
            manifest_path,
            manifest,
            dataset,
            locator,
        })
    }

    /// The catalog record read at open (the current schema).
    pub fn collection(&self) -> &Collection {
        &self.collection
    }

    pub fn manifest(&self) -> &CollectionManifest {
        &self.manifest
    }

    /// `None` before the first commit.
    pub fn manifest_path(&self) -> Option<&str> {
        self.manifest_path.as_deref()
    }

    /// `None` while `lance_version == 0`.
    pub fn dataset(&self) -> Option<&Arc<Dataset>> {
        self.dataset.as_ref()
    }

    pub fn splits(&self) -> &[SplitRef] {
        &self.manifest.splits
    }

    /// The split index and doc id of `row_id`.
    pub fn locate_row(&self, row_id: u64) -> Option<(usize, u32)> {
        self.locator.locate(row_id)
    }

    /// Opens `split` with one ranged GET of its footer (Ruling 9); warm it
    /// before a synchronous search.
    pub async fn open_split(&self, split: &SplitRef) -> Result<tantivy::Index, CollectionError> {
        let storage = OperonStorage::new(self.ctx.store.clone(), self.ctx.cache.clone(), "");
        let path = split_path(self.namespace, self.collection.id, split.ulid);
        Ok(operon_text::open_split(
            Arc::new(storage),
            &path,
            split.size_bytes,
            split.footer_range.clone(),
        )
        .await?)
    }

    /// The deleted doc ids of `split` (empty without a delete bitmap).
    pub async fn deleted_docs(&self, split: &SplitRef) -> Result<RoaringBitmap, CollectionError> {
        let Some(path) = &split.delete_bitmap else {
            return Ok(RoaringBitmap::new());
        };
        let (bytes, _) = self.ctx.store.get(path).await?;
        let (owner, doc_count, deleted) = decode_delete_bitmap(&bytes)?;
        if owner != split.ulid || u64::from(doc_count) != split.doc_count {
            return Err(CollectionError::Corrupt(format!(
                "{path} is the bitmap of split {owner} with {doc_count} docs, not of split {} with {}",
                split.ulid, split.doc_count
            )));
        }
        Ok(deleted)
    }

    /// The documents at `row_ids`, in input order; `None` for a row id this
    /// version does not have.
    pub async fn take_rows(
        &self,
        row_ids: &[u64],
    ) -> Result<Vec<Option<StoredDoc>>, CollectionError> {
        let Some(dataset) = &self.dataset else {
            return Ok(vec![None; row_ids.len()]);
        };
        let mut columns = self.columns(dataset);
        columns.push(ROW_ID.to_string());
        let projection = dataset.schema().project_preserve_system_columns(&columns)?;
        let mut found = HashMap::new();
        for chunk in row_ids.chunks(self.lookup_batch()) {
            let batch = dataset
                .take_rows(chunk, ProjectionRequest::from_schema(projection.clone()))
                .await?;
            for doc in self.docs(&batch)? {
                found.insert(doc.row_id, doc);
            }
        }
        Ok(row_ids.iter().map(|id| found.get(id).cloned()).collect())
    }

    /// The documents with keys `pks`, in input order; `None` for a key this
    /// version does not have. A scan filtered on `_pk`, which the `_pk`
    /// BTREE index serves when present (Task 11).
    pub async fn get_by_pk(
        &self,
        pks: &[PrimaryKey],
    ) -> Result<Vec<Option<StoredDoc>>, CollectionError> {
        let Some(dataset) = &self.dataset else {
            return Ok(vec![None; pks.len()]);
        };
        let columns = self.columns(dataset);
        let mut found = HashMap::new();
        for chunk in pks.chunks(self.lookup_batch()) {
            let keys = chunk
                .iter()
                .map(|pk| lit(ScalarValue::Binary(Some(pk.canonical()))))
                .collect();
            let mut scanner = dataset.scan();
            scanner
                .project(&columns)?
                .filter_expr(col(PK_COLUMN).in_list(keys, false))
                .with_row_id();
            let batch = scanner.try_into_batch().await?;
            for doc in self.docs(&batch)? {
                found.insert(doc.pk.clone(), doc);
            }
        }
        Ok(pks.iter().map(|pk| found.get(pk).cloned()).collect())
    }

    /// Every document, sorted by canonical PK. For tests and small
    /// collections only; M1.2 streams.
    pub async fn scan_all(&self) -> Result<Vec<StoredDoc>, CollectionError> {
        let Some(dataset) = &self.dataset else {
            return Ok(Vec::new());
        };
        let mut scanner = dataset.scan();
        scanner.project(&self.columns(dataset))?.with_row_id();
        let batch = scanner.try_into_batch().await?;
        let mut docs = self.docs(&batch)?;
        // `PrimaryKey`'s order is the order of its canonical bytes.
        docs.sort_by(|a, b| a.pk.cmp(&b.pk));
        Ok(docs)
    }

    fn lookup_batch(&self) -> usize {
        self.ctx.config.max_lookup_batch.max(1)
    }

    /// The system columns, then every dense and sparse vector column of the
    /// current schema that this Lance version has.
    fn columns(&self, dataset: &Dataset) -> Vec<String> {
        let schema = &self.collection.schema;
        let mut columns: Vec<String> = [
            PK_COLUMN,
            SOURCE_COLUMN,
            INGEST_PARTITION_COLUMN,
            INGEST_OFFSET_COLUMN,
        ]
        .iter()
        .map(|c| c.to_string())
        .collect();
        let vectors = (0..schema.vectors.len()).map(vector_column);
        let sparse = (0..schema.sparse_vectors.len()).map(sparse_column);
        columns.extend(
            vectors
                .chain(sparse)
                .filter(|column| dataset.schema().field(column).is_some()),
        );
        columns
    }

    /// Every row of `batch` (which has a `_rowid` column) as a [`StoredDoc`].
    fn docs(&self, batch: &RecordBatch) -> Result<Vec<StoredDoc>, CollectionError> {
        let row_ids = batch.column_by_name(ROW_ID).ok_or_else(|| {
            CollectionError::Internal(format!("the batch has no {ROW_ID} column"))
        })?;
        let row_ids = row_ids
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                CollectionError::Corrupt(format!("{ROW_ID} is {}", row_ids.data_type()))
            })?;
        (0..batch.num_rows())
            .map(|row| {
                let stored = row_from_batch(&self.collection.schema, batch, row)?;
                Ok(StoredDoc {
                    pk: stored.pk,
                    row_id: row_ids.value(row),
                    source: stored.source,
                    vectors: stored.vectors,
                    sparse_vectors: stored.sparse_vectors,
                    partition: stored.partition,
                    seq_no: stored.offset,
                })
            })
            .collect()
    }
}
