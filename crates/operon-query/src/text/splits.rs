//! Opening a view's splits for one request (plan M1.2 Task 5 rule 2):
//! from a hot local file when the hot tier pins the split, else through the
//! range cache; warmed for the query; with the split's mask.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use operon_collection::{ROWID_FIELD, SplitRef};
use operon_quickwit::doc_mapper::WarmupInfo;
use operon_quickwit::shim::Uri;
use operon_quickwit::storage::{
    BulkDeleteError, OwnedBytes, PutPayload, SendableAsync, Storage, StorageError,
    StorageErrorKind, StorageResult,
};
use roaring::{RoaringBitmap, RoaringTreemap};
use tantivy::schema::Schema;
use tantivy::{Index, ReloadPolicy, Searcher};

use crate::error::ServiceError;
use crate::exec::mask::SplitMask;
use crate::hot::HotKind;
use crate::read::{ReadView, collection_error};

/// How many splits open (and later search) at a time.
pub const DEFAULT_PARALLELISM: usize = 8;

/// One split of the view's manifest, open and warmed for one request.
pub struct OpenSplit {
    pub index_in_manifest: usize,
    pub split: SplitRef,
    pub searcher: Searcher,
    pub mask: SplitMask,
    /// Opened from the hot tier's local file.
    pub from_hot_file: bool,
}

impl fmt::Debug for OpenSplit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenSplit")
            .field("index_in_manifest", &self.index_in_manifest)
            .field("split", &self.split.ulid)
            .field("deleted", &self.mask.deleted.len())
            .field("shadowed", &self.mask.shadowed.len())
            .field("from_hot_file", &self.from_hot_file)
            .finish_non_exhaustive()
    }
}

impl OpenSplit {
    /// Per segment of the searcher, its masked local doc ids.
    pub fn segment_masks(&self) -> Vec<RoaringBitmap> {
        let mut base = 0u32;
        self.searcher
            .segment_readers()
            .iter()
            .map(|reader| {
                let end = base + reader.max_doc();
                let local = |bitmap: &RoaringBitmap| {
                    bitmap
                        .range(base..end)
                        .map(|doc| doc - base)
                        .collect::<RoaringBitmap>()
                };
                let mut masked = local(&self.mask.deleted);
                masked |= local(&self.mask.shadowed);
                if let Some(alive) = reader.alive_bitset() {
                    masked.extend((0..reader.max_doc()).filter(|doc| alive.is_deleted(*doc)));
                }
                base = end;
                masked
            })
            .collect()
    }
}

/// Per segment of a tail searcher, the doc ids that are not live: deleted
/// in the RAM index, or not among `live` row ids.
pub fn tail_segment_masks(
    searcher: &Searcher,
    live: &RoaringTreemap,
) -> Result<Vec<RoaringBitmap>, ServiceError> {
    searcher
        .segment_readers()
        .iter()
        .map(|reader| {
            let rowids = reader
                .fast_fields()
                .u64(ROWID_FIELD)
                .map_err(|err| ServiceError::Internal(format!("tail _rowid: {err}")))?;
            let alive = reader.alive_bitset();
            Ok((0..reader.max_doc())
                .filter(|doc| {
                    alive.is_some_and(|alive| alive.is_deleted(*doc))
                        || rowids.first(*doc).is_none_or(|row| !live.contains(row))
                })
                .collect())
        })
        .collect()
}

fn tantivy_error(err: tantivy::TantivyError) -> ServiceError {
    ServiceError::Internal(format!("tantivy: {err}"))
}

/// A searcher over `index`, whose sync reads hit only its hotcache.
fn searcher_of(index: &Index) -> Result<Searcher, ServiceError> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(tantivy_error)?;
    Ok(reader.searcher())
}

/// Opens the local file `path` holding the whole split.
async fn open_local(path: &Path, split: &SplitRef) -> Result<Index, ServiceError> {
    let storage = LocalSplitStorage::new(path.to_path_buf())?;
    let len = storage
        .file
        .metadata()
        .map_err(|err| ServiceError::Unavailable(format!("{}: {err}", path.display())))?
        .len();
    operon_text::open_split(
        Arc::new(storage),
        &path.to_string_lossy(),
        len,
        split.footer_range.clone(),
    )
    .await
    .map_err(|err| ServiceError::Unavailable(format!("{}: {err}", path.display())))
}

/// The doc ids of the view's shadowed rows, per split index.
fn shadowed_by_split(view: &ReadView) -> BTreeMap<usize, RoaringBitmap> {
    let mut out: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
    for row in view.tail.shadow() {
        if let Some((split, doc)) = view.snapshot.locate_row(row) {
            out.entry(split).or_default().insert(doc);
        }
    }
    out
}

/// A function of a split's schema giving what a request reads from it.
pub type Warmups<'a> = dyn Fn(&Schema) -> Result<WarmupInfo, ServiceError> + Send + Sync + 'a;

/// Opens every split of the view's manifest, in manifest order,
/// `DEFAULT_PARALLELISM` at a time (rule 2).
pub async fn open_splits(
    view: &ReadView,
    warmups: &Warmups<'_>,
) -> Result<Vec<OpenSplit>, ServiceError> {
    open_splits_with(view, warmups, DEFAULT_PARALLELISM).await
}

/// [`open_splits`] with `parallelism` splits at a time.
pub async fn open_splits_with(
    view: &ReadView,
    warmups: &Warmups<'_>,
    parallelism: usize,
) -> Result<Vec<OpenSplit>, ServiceError> {
    let mut shadowed = shadowed_by_split(view);
    let jobs: Vec<(usize, SplitRef, RoaringBitmap)> = view
        .snapshot
        .splits()
        .iter()
        .enumerate()
        .map(|(i, split)| (i, split.clone(), shadowed.remove(&i).unwrap_or_default()))
        .collect();
    futures::stream::iter(jobs)
        .map(|(i, split, shadowed)| open_one(view, warmups, i, split, shadowed))
        .buffered(parallelism.max(1))
        .try_collect()
        .await
}

async fn open_one(
    view: &ReadView,
    warmups: &Warmups<'_>,
    index_in_manifest: usize,
    split: SplitRef,
    shadowed: RoaringBitmap,
) -> Result<OpenSplit, ServiceError> {
    let mut from_hot_file = false;
    let mut index = None;
    if let Some(path) = view.hot.split_file(view.ns, view.collection.id, split.ulid) {
        match open_local(&path, &split).await {
            Ok(opened) => {
                from_hot_file = true;
                index = Some(opened);
            }
            Err(err) => {
                // Demoted and deleted after the lookup: read it remotely.
                tracing::info!(split = %split.ulid, %err, "the hot split file did not open; reading the split remotely");
            }
        }
    }
    let index = match index {
        Some(index) => index,
        None => view
            .snapshot
            .open_split(&split)
            .await
            .map_err(collection_error)?,
    };
    let searcher = searcher_of(&index)?;
    let info = warmups(searcher.schema())?;
    operon_quickwit::search::warmup(&searcher, &info)
        .await
        .map_err(|err| {
            ServiceError::Unavailable(format!("warming split {}: {err:#}", split.ulid))
        })?;
    let deleted = view.bitmaps.deleted_docs(&view.snapshot, &split).await?;
    if from_hot_file {
        view.hot_used.record(HotKind::Splits);
    }
    Ok(OpenSplit {
        index_in_manifest,
        split,
        searcher,
        mask: SplitMask {
            deleted: (*deleted).clone(),
            shadowed,
        },
        from_hot_file,
    })
}

/// A Quickwit [`Storage`] over one local file (a split the hot tier pins):
/// reads only.
///
/// The file is opened once, in [`LocalSplitStorage::new`], and read with
/// positional reads: an open handle stays readable after the hot tier
/// demotes the split and unlinks its path, so a split that opened keeps
/// serving its request (warmup included) until its `Index` is dropped.
#[derive(Clone, Debug)]
pub struct LocalSplitStorage {
    pub path: PathBuf,
    file: Arc<std::fs::File>,
    uri: Uri,
}

impl LocalSplitStorage {
    /// Opens `path`; a file that is gone is `Unavailable`.
    pub fn new(path: PathBuf) -> Result<Self, ServiceError> {
        let uri = Uri::from_str(&format!("file://{}", path.display()))
            .map_err(|err| ServiceError::Internal(format!("{}: {err}", path.display())))?;
        let file = std::fs::File::open(&path)
            .map_err(|err| ServiceError::Unavailable(format!("{}: {err}", path.display())))?;
        Ok(Self {
            path,
            file: Arc::new(file),
            uri,
        })
    }

    async fn read(&self, range: Option<Range<usize>>) -> StorageResult<OwnedBytes> {
        let file = self.file.clone();
        let bytes = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            match range {
                Some(range) => {
                    let mut out = vec![0; range.len()];
                    file.read_exact_at(&mut out, range.start as u64)?;
                    Ok(out)
                }
                None => {
                    let mut out = Vec::new();
                    let mut offset = 0u64;
                    let mut chunk = vec![0; 1 << 16];
                    loop {
                        let n = file.read_at(&mut chunk, offset)?;
                        if n == 0 {
                            break;
                        }
                        out.extend_from_slice(&chunk[..n]);
                        offset += n as u64;
                    }
                    Ok(out)
                }
            }
        })
        .await
        .map_err(|err| StorageErrorKind::Internal.with_error(err))?
        .map_err(io_error)?;
        Ok(OwnedBytes::new(bytes))
    }
}

fn io_error(err: std::io::Error) -> StorageError {
    let kind = match err.kind() {
        std::io::ErrorKind::NotFound => StorageErrorKind::NotFound,
        _ => StorageErrorKind::Io,
    };
    kind.with_error(err)
}

fn read_only() -> StorageError {
    StorageErrorKind::Internal.with_error(anyhow::anyhow!("read-only local split"))
}

#[async_trait]
impl Storage for LocalSplitStorage {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put(&self, _path: &Path, _payload: Box<dyn PutPayload>) -> StorageResult<()> {
        Err(read_only())
    }

    fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _path: &'life1 Path,
        _output: &'life2 mut dyn SendableAsync,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { Err(read_only()) })
    }

    async fn copy_to_file(&self, _path: &Path, _output_path: &Path) -> StorageResult<u64> {
        Err(read_only())
    }

    async fn get_slice(&self, _path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes> {
        self.read(Some(range)).await
    }

    async fn get_slice_stream(
        &self,
        _path: &Path,
        _range: Range<usize>,
    ) -> StorageResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        Err(read_only())
    }

    async fn get_all(&self, _path: &Path) -> StorageResult<OwnedBytes> {
        self.read(None).await
    }

    async fn delete(&self, _path: &Path) -> StorageResult<()> {
        Err(read_only())
    }

    async fn bulk_delete<'a>(&self, _paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        Err(BulkDeleteError {
            error: Some(read_only()),
            ..BulkDeleteError::default()
        })
    }

    async fn file_num_bytes(&self, _path: &Path) -> StorageResult<u64> {
        let file = self.file.clone();
        let metadata = tokio::task::spawn_blocking(move || file.metadata())
            .await
            .map_err(|err| StorageErrorKind::Internal.with_error(err))?
            .map_err(io_error)?;
        Ok(metadata.len())
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }
}
