//! Lance integration (plan M1.1 Task 7, Rulings 1, 2 and 5; overview R7,
//! R18, R19).
//!
//! Every dataset is opened through a [`LanceEnv`]: Lance's I/O goes through
//! the Operon [`Store`] (so a `FaultyStore` sees all of it), with an explicit
//! commit handler, file format 2.1, stable row ids and no auto-cleanup.
//!
//! A dataset has exactly one mainline version, the empty version 1. Every
//! later commit is detached and built from exactly its parent's manifest
//! ([`LanceCommitter::commit`]); the collection manifest records which
//! detached version is live.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, RecordBatch, UInt64Array};
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::rowids::get_row_id_index;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, InsertBuilder, ROW_ID, WriteMode, WriteParams};
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use lance_io::object_store::providers::ObjectStoreProvider;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_table::format::{Fragment, is_detached_version};
use lance_table::io::commit::{CommitHandler, ConditionalPutCommitHandler};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};
use operon_cache::{CacheError, RangeCache};
use operon_common::{CollectionId, NamespaceId};
use operon_store::{Store, StoreError};
use url::Url;

use crate::arrow_schema::{
    PK_COLUMN, base_arrow_schema, sparse_column, sparse_field, vector_column, vector_field,
};
use crate::config::LanceConfig;
use crate::error::CollectionError;
use crate::schema::CollectionSchema;

/// The URL scheme of Operon's Lance object-store provider.
pub const LANCE_SCHEME: &str = "operon";

/// Retries of a detached commit, which retries only on a collision of its
/// random version id.
const DETACHED_COMMIT_RETRIES: u32 = 20;

/// Retries Lance makes of one failed download.
const DOWNLOAD_RETRIES: usize = 3;

/// Opens collection datasets over one [`Store`]. Cheap to clone.
#[derive(Clone)]
pub struct LanceEnv {
    store: Store,
    /// Unique per environment, so two stores in one process never share
    /// Lance's caches.
    authority: String,
    session: Arc<Session>,
    handler: Arc<dyn CommitHandler>,
    config: LanceConfig,
}

impl fmt::Debug for LanceEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceEnv")
            .field("store", &self.store)
            .field("authority", &self.authority)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Hands Lance the Operon store (or a [`CachedObjectStore`] over it) for
/// `operon://<authority>/…` URLs; the URL path is the path within the store.
#[derive(Debug)]
struct OperonStoreProvider {
    inner: Arc<dyn object_store::ObjectStore>,
    authority: String,
    io_parallelism: usize,
}

#[async_trait::async_trait]
impl ObjectStoreProvider for OperonStoreProvider {
    async fn new_store(
        &self,
        base: Url,
        _params: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        if base.host_str() != Some(self.authority.as_str()) {
            return Err(lance::Error::invalid_input(format!(
                "{base} is not in the Operon store {LANCE_SCHEME}://{}/",
                self.authority
            )));
        }
        let location = Url::parse(&format!("{LANCE_SCHEME}://{}/", self.authority))
            .map_err(|err| lance::Error::invalid_input(err.to_string()))?;
        Ok(ObjectStore::new(
            self.inner.clone(),
            location,
            None,
            None,
            false,
            // Claimed even for stores whose listings are not ordered: the
            // mainline only ever has version 1 (Ruling 1), so Lance's
            // latest-version listing never has to pick among several.
            true,
            self.io_parallelism,
            DOWNLOAD_RETRIES,
            None,
        ))
    }

    fn calculate_object_store_prefix(
        &self,
        _url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> lance::Result<String> {
        Ok(format!("{LANCE_SCHEME}${}", self.authority))
    }
}

impl LanceEnv {
    /// An environment with a fresh authority (a lowercase ULID) and its own
    /// Lance session.
    pub fn new(store: Store, config: LanceConfig) -> Self {
        let inner = store.inner().clone();
        Self::over(store, inner, config)
    }

    /// Like [`LanceEnv::new`], but every Lance read of a byte range goes
    /// through `cache` (Ruling 9): Lance files are create-only and never
    /// rewritten, so caching them by path is safe. Writes, lists, deletes
    /// and conditional reads go to `store` unchanged.
    pub fn with_cache(store: Store, cache: RangeCache, config: LanceConfig) -> Self {
        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(CachedObjectStore::new(store.inner().clone(), cache));
        Self::over(store, inner, config)
    }

    /// An environment whose Lance I/O goes through `inner` (the store's own
    /// object store, or a cache over it).
    fn over(store: Store, inner: Arc<dyn object_store::ObjectStore>, config: LanceConfig) -> Self {
        let authority = ulid::Ulid::generate().to_string().to_lowercase();
        let registry = Arc::new(ObjectStoreRegistry::empty());
        registry.insert(
            LANCE_SCHEME,
            Arc::new(OperonStoreProvider {
                inner,
                authority: authority.clone(),
                io_parallelism: config.io_parallelism,
            }),
        );
        let session = Arc::new(Session::new(
            config.index_cache_bytes,
            config.metadata_cache_bytes,
            registry,
        ));
        Self {
            store,
            authority,
            session,
            handler: Arc::new(ConditionalPutCommitHandler),
            config,
        }
    }

    /// `operon://<authority>/ns/<ns>/collections/<cid>/lance`.
    pub fn uri(&self, namespace: NamespaceId, collection: CollectionId) -> String {
        format!(
            "{LANCE_SCHEME}://{}/ns/{namespace}/collections/{collection}/lance",
            self.authority
        )
    }

    /// The parameters of every Lance data write (R8, R18, R19).
    pub fn write_params(&self) -> WriteParams {
        WriteParams {
            max_rows_per_file: self.config.max_rows_per_file,
            max_rows_per_group: self.config.max_rows_per_group,
            // Data is only ever staged onto an existing dataset.
            mode: WriteMode::Append,
            commit_handler: Some(self.handler.clone()),
            data_storage_version: Some(LanceFileVersion::V2_1),
            enable_stable_row_ids: true,
            enable_v2_manifest_paths: true,
            session: Some(self.session.clone()),
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..WriteParams::default()
        }
    }

    /// Mainline version 1 (empty, base schema), creating it if absent (Ruling 5).
    pub async fn ensure_created(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let uri = self.uri(namespace, collection);
        match self.load(&uri, 1).await {
            Ok(dataset) => return check_version_one(dataset),
            Err(err) if is_not_found(&err) => {}
            Err(err) => return Err(err.into()),
        }
        let operation = Operation::Overwrite {
            fragments: vec![],
            schema: lance::datatypes::Schema::try_from(&base_arrow_schema())?,
            config_upsert_values: None,
            initial_bases: None,
        };
        let created = CommitBuilder::new(uri.as_str())
            .with_session(self.session.clone())
            .with_commit_handler(self.handler.clone())
            .use_stable_row_ids(true)
            .with_storage_format(LanceFileVersion::V2_1)
            .enable_v2_manifest_paths(true)
            .with_skip_auto_cleanup(true)
            // A strict overwrite: if another creator's version 1 appears
            // after the load above, Lance fails (it cannot check out our read
            // version 0) instead of committing version 2 on top of it.
            .with_max_retries(0)
            .execute(Transaction::new(0, operation, None))
            .await;
        match created {
            Ok(dataset) => check_version_one(dataset),
            // Another creator won, or our write landed ambiguously: either way
            // version 1 is the same empty dataset. If it is not there, the
            // original (retryable) error stands.
            Err(err) => match self.load(&uri, 1).await {
                Ok(dataset) => check_version_one(dataset),
                Err(load) if is_not_found(&load) => Err(err.into()),
                Err(load) => Err(load.into()),
            },
        }
    }

    /// The dataset at `version` (1 or a detached id). A missing version is
    /// `NotFound`.
    pub async fn open(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
        version: u64,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let uri = self.uri(namespace, collection);
        match self.load(&uri, version).await {
            Ok(dataset) => Ok(Arc::new(dataset)),
            Err(err) if is_not_found(&err) => Err(CollectionError::NotFound(format!(
                "lance version {version} of {uri}: {err}"
            ))),
            Err(err) => Err(err.into()),
        }
    }

    async fn load(&self, uri: &str, version: u64) -> lance::Result<Dataset> {
        DatasetBuilder::from_uri(uri)
            .with_version(version)
            .with_session(self.session.clone())
            .with_commit_handler(self.handler.clone())
            .load()
            .await
    }
}

fn is_not_found(err: &lance::Error) -> bool {
    matches!(
        err,
        lance::Error::NotFound { .. }
            | lance::Error::DatasetNotFound { .. }
            | lance::Error::VersionNotFound { .. }
    )
}

/// `dataset` if it is a mainline version 1 whose schema starts with the
/// system columns.
fn check_version_one(dataset: Dataset) -> Result<Arc<Dataset>, CollectionError> {
    if dataset.manifest.version != 1 {
        return Err(CollectionError::Internal(format!(
            "expected lance version 1, got {}",
            dataset.manifest.version
        )));
    }
    let schema = ArrowSchema::from(dataset.schema());
    let base = base_arrow_schema();
    let starts_with_base = schema.fields().len() >= base.fields().len()
        && base
            .fields()
            .iter()
            .zip(schema.fields())
            .all(|(want, got)| want == got);
    if !starts_with_base {
        return Err(CollectionError::Corrupt(format!(
            "lance version 1 at {} does not start with the system columns: {schema:?}",
            dataset.uri()
        )));
    }
    Ok(Arc::new(dataset))
}

/// The only way Operon commits to Lance after a dataset's creation (R7).
#[derive(Debug)]
pub struct LanceCommitter;

impl LanceCommitter {
    /// The only Lance commit path after creation: a detached commit of
    /// `operation` on exactly `parent`.
    pub async fn commit(
        env: &LanceEnv,
        parent: &Arc<Dataset>,
        operation: Operation,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let transaction = Transaction::new(parent.manifest.version, operation, None);
        let committed = CommitBuilder::new(parent.clone())
            .with_session(env.session.clone())
            .with_commit_handler(env.handler.clone())
            .with_detached(true)
            .with_skip_auto_cleanup(true)
            .with_max_retries(DETACHED_COMMIT_RETRIES)
            .execute(transaction)
            .await?;
        if !is_detached_version(committed.manifest.version) {
            return Err(CollectionError::Internal(format!(
                "lance committed mainline version {} for a detached commit",
                committed.manifest.version
            )));
        }
        Ok(Arc::new(committed))
    }

    /// Adds an all-null column for each dense or sparse vector of `schema`
    /// that `parent` lacks (one detached Merge), else returns `parent`.
    pub async fn ensure_vectors(
        env: &LanceEnv,
        parent: &Arc<Dataset>,
        schema: &CollectionSchema,
    ) -> Result<Arc<Dataset>, CollectionError> {
        let current = parent.schema();
        let mut missing: Vec<_> = schema
            .vectors
            .iter()
            .enumerate()
            .filter(|(index, _)| current.field(&vector_column(*index)).is_none())
            .map(vector_field)
            .collect();
        missing.extend(
            (0..schema.sparse_vectors.len())
                .filter(|index| current.field(&sparse_column(*index)).is_none())
                .map(sparse_field),
        );
        if missing.is_empty() {
            return Ok(parent.clone());
        }
        let mut merged = current.merge(&ArrowSchema::new(missing))?;
        merged.set_field_id(Some(parent.manifest.max_field_id()));
        let operation = Operation::Merge {
            fragments: parent.manifest.fragments.to_vec(),
            schema: merged,
            preserves_nullability: true,
        };
        Self::commit(env, parent, operation).await
    }

    /// Writes `batch` as new data files; returns the fragments (ids assigned
    /// at commit).
    pub async fn write_fragments(
        env: &LanceEnv,
        parent: &Arc<Dataset>,
        batch: RecordBatch,
    ) -> Result<Vec<Fragment>, CollectionError> {
        let params = env.write_params();
        let transaction = InsertBuilder::new(parent.clone())
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await?;
        match transaction.operation {
            Operation::Append { fragments } => Ok(fragments),
            other => Err(CollectionError::Internal(format!(
                "lance staged {} instead of an append",
                other.name()
            ))),
        }
    }

    /// Deletion files for `row_ids` of `parent`: (updated fragments, removed
    /// fragment ids). A fragment whose every row is deleted is removed.
    pub async fn delete_rows(
        parent: &Arc<Dataset>,
        row_ids: &[u64],
    ) -> Result<(Vec<Fragment>, Vec<u64>), CollectionError> {
        let version = parent.manifest.version;
        let index = get_row_id_index(parent).await?.ok_or_else(|| {
            CollectionError::Internal(format!("lance version {version} has no stable row ids"))
        })?;
        let mut offsets: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for &row_id in row_ids {
            let address = index.get(row_id)?.ok_or_else(|| {
                CollectionError::Corrupt(format!(
                    "row id {row_id} is not in lance version {version}"
                ))
            })?;
            offsets
                .entry(address.fragment_id())
                .or_default()
                .push(address.row_offset());
        }
        let mut updated = Vec::new();
        let mut removed = Vec::new();
        for (fragment_id, offsets) in offsets {
            let fragment = parent.get_fragment(fragment_id as usize).ok_or_else(|| {
                CollectionError::Corrupt(format!(
                    "fragment {fragment_id} is not in lance version {version}"
                ))
            })?;
            match fragment.extend_deletions(offsets).await? {
                Some(fragment) => updated.push(fragment.metadata().clone()),
                None => removed.push(u64::from(fragment_id)),
            }
        }
        Ok((updated, removed))
    }

    /// (canonical pk, row id) of every row in `committed`'s fragments absent
    /// from `parent`. It joins on the key rather than trusting Lance's write
    /// order.
    pub async fn new_row_ids(
        committed: &Arc<Dataset>,
        parent: &Arc<Dataset>,
    ) -> Result<Vec<(Vec<u8>, u64)>, CollectionError> {
        let old: HashSet<u64> = parent.manifest.fragments.iter().map(|f| f.id).collect();
        let fragments: Vec<Fragment> = committed
            .manifest
            .fragments
            .iter()
            .filter(|fragment| !old.contains(&fragment.id))
            .cloned()
            .collect();
        if fragments.is_empty() {
            return Ok(Vec::new());
        }
        pk_row_ids(committed, Some(fragments)).await
    }
}

/// (canonical pk, row id) of every row of `dataset`, or only of its
/// `fragments`.
pub(crate) async fn pk_row_ids(
    dataset: &Dataset,
    fragments: Option<Vec<Fragment>>,
) -> Result<Vec<(Vec<u8>, u64)>, CollectionError> {
    let mut scanner = dataset.scan();
    if let Some(fragments) = fragments {
        scanner.with_fragments(fragments);
    }
    scanner.project(&[PK_COLUMN])?.with_row_id();
    let batch = scanner.try_into_batch().await?;
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| CollectionError::Corrupt(format!("the scan has no {name} column")))
    };
    let pks = column(PK_COLUMN)?;
    let pks = pks
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| CollectionError::Corrupt(format!("{PK_COLUMN} is {}", pks.data_type())))?;
    let row_ids = column(ROW_ID)?;
    let row_ids = row_ids
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CollectionError::Corrupt(format!("{ROW_ID} is {}", row_ids.data_type())))?;
    (0..batch.num_rows())
        .map(|row| {
            if pks.is_null(row) {
                return Err(CollectionError::Corrupt(format!(
                    "row id {} has a null {PK_COLUMN}",
                    row_ids.value(row)
                )));
            }
            Ok((pks.value(row).to_vec(), row_ids.value(row)))
        })
        .collect()
}

/// An `object_store` over `inner` that serves ranged and whole-object GETs
/// from `cache` (Task 2 rule 5): a GET with no precondition, no version and
/// `head == false` is read from the cache; every other call (heads, puts,
/// multipart uploads, lists, deletes, copies, renames, conditional GETs) goes
/// to `inner` unchanged. Only for stores whose objects are never rewritten
/// in place, like Lance's.
#[derive(Clone, Debug)]
pub struct CachedObjectStore {
    inner: Arc<dyn object_store::ObjectStore>,
    cache: RangeCache,
}

impl CachedObjectStore {
    pub fn new(inner: Arc<dyn object_store::ObjectStore>, cache: RangeCache) -> Self {
        Self { inner, cache }
    }
}

impl fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CachedObjectStore({})", self.inner)
    }
}

/// Whether `options` is a plain read: no precondition, no version, not a
/// HEAD.
fn plain_read(options: &GetOptions) -> bool {
    options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.version.is_none()
        && !options.head
}

/// A missing object is `NotFound`, as `object_store` reports it; any other
/// cache failure is `Generic`.
fn cache_error(err: CacheError) -> object_store::Error {
    match err {
        CacheError::Store(StoreError::NotFound { path }) => object_store::Error::NotFound {
            path: path.clone(),
            source: Box::new(StoreError::NotFound { path }),
        },
        other => object_store::Error::Generic {
            store: "CachedObjectStore",
            source: Box::new(other),
        },
    }
}

/// The bytes `range` asks for of an object of `size` bytes, as
/// `object_store` defines them (a bounded range past the end is cut at the
/// end); `None` for a range `object_store` refuses, which `inner` then
/// answers with its own error.
fn resolve_range(range: Option<&GetRange>, size: u64) -> Option<Range<u64>> {
    let resolved = match range {
        None => return Some(0..size),
        Some(GetRange::Bounded(r)) => r.start..r.end.min(size),
        Some(GetRange::Offset(offset)) => *offset..size,
        Some(GetRange::Suffix(n)) => size.saturating_sub(*n)..size,
    };
    (resolved.start < resolved.end).then_some(resolved)
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CachedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !plain_read(&options) {
            return self.inner.get_opts(location, options).await;
        }
        let path = location.as_ref();
        let size = self.cache.size(path).await.map_err(cache_error)?;
        let Some(range) = resolve_range(options.range.as_ref(), size) else {
            return self.inner.get_opts(location, options).await;
        };
        let bytes = self
            .cache
            .read(path, range.clone())
            .await
            .map_err(cache_error)?;
        let payload: BoxStream<'static, object_store::Result<Bytes>> =
            futures::stream::once(async move { Ok(bytes) }).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta: ObjectMeta {
                location: location.clone(),
                // Lance files never change, so their age is never asked.
                last_modified: Default::default(),
                size,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let path = location.as_ref();
        let mut out = Vec::with_capacity(ranges.len());
        for range in ranges {
            out.push(
                self.cache
                    .read(path, range.clone())
                    .await
                    .map_err(cache_error)?,
            );
        }
        Ok(out)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
