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
use loams_collection::{ROWID_FIELD, SplitRef};
use loams_quickwit::doc_mapper::WarmupInfo;
use loams_quickwit::shim::Uri;
use loams_quickwit::storage::{
    BulkDeleteError, OwnedBytes, PutPayload, SendableAsync, Storage, StorageError,
    StorageErrorKind, StorageResult,
};
use roaring::{RoaringBitmap, RoaringTreemap};
use tantivy::schema::Schema;
use tantivy::{Index, ReloadPolicy, Searcher};

use super::checksums::SplitChecksums;
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
    loams_text::open_split(
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
    let mut hot = None;
    if let Some(path) = view.hot.split_file(view.ns, view.collection.id, split.ulid) {
        match open_hot(&path, &split, warmups).await {
            Ok(searcher) => hot = Some(searcher),
            Err(HotFileError::Gone(err)) => {
                // Demoted and deleted after the lookup: read it remotely.
                tracing::info!(split = %split.ulid, %err, "the hot split file is gone; reading the split remotely");
            }
            Err(HotFileError::Request(err)) => {
                // The request's own warm-up failed (it names an unknown
                // field, say): the file is not at fault, so keep it and let
                // the remote path answer.
                tracing::debug!(split = %split.ulid, %err, "the request's warm-up failed on the hot split file; reading the split remotely");
            }
            Err(HotFileError::Bad(err)) => {
                // Corrupt, or not this split (row F3): never trust it again.
                tracing::warn!(split = %split.ulid, path = %path.display(), %err, "the hot split file failed; quarantining it and reading the split remotely");
                view.hot
                    .quarantine_split(view.ns, view.collection.id, split.ulid, &path);
            }
        }
    }
    let from_hot_file = hot.is_some();
    let searcher = match hot {
        Some(searcher) => searcher,
        None => {
            let index = view
                .snapshot
                .open_split(&split)
                .await
                .map_err(collection_error)?;
            let searcher = searcher_of(&index)?;
            warm(&searcher, &split, warmups).await?;
            searcher
        }
    };
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

/// Why a hot split file was not used.
enum HotFileError {
    /// The file is no longer there (demoted after the lookup).
    Gone(ServiceError),
    /// It opened but failed: checksums, footer or warm-up.
    Bad(ServiceError),
    /// The request's warm-up could not be compiled against the file's
    /// schema: a fault of the request, not of the file.
    Request(ServiceError),
}

/// Opens and warms the hot file `path` of `split`.
async fn open_hot(
    path: &Path,
    split: &SplitRef,
    warmups: &Warmups<'_>,
) -> Result<Searcher, HotFileError> {
    if !path.exists() {
        return Err(HotFileError::Gone(ServiceError::Unavailable(format!(
            "{} does not exist",
            path.display()
        ))));
    }
    let index = open_local(path, split).await.map_err(HotFileError::Bad)?;
    let searcher = searcher_of(&index).map_err(HotFileError::Bad)?;
    let info = warmups(searcher.schema()).map_err(HotFileError::Request)?;
    warm_with(&searcher, split, &info)
        .await
        .map_err(HotFileError::Bad)?;
    Ok(searcher)
}

/// Warms `searcher` for what the request reads of it.
async fn warm(
    searcher: &Searcher,
    split: &SplitRef,
    warmups: &Warmups<'_>,
) -> Result<(), ServiceError> {
    let info = warmups(searcher.schema())?;
    warm_with(searcher, split, &info).await
}

/// Warms `searcher` with `info`.
async fn warm_with(
    searcher: &Searcher,
    split: &SplitRef,
    info: &WarmupInfo,
) -> Result<(), ServiceError> {
    loams_quickwit::search::warmup(searcher, info)
        .await
        .map(|_| ())
        .map_err(|err| ServiceError::Unavailable(format!("warming split {}: {err:#}", split.ulid)))
}

/// A Quickwit [`Storage`] over one local file (a split the hot tier pins):
/// reads only.
///
/// Every read covers whole checksum blocks and checks them against the
/// file's `<path>.crc` (row F3), so a corrupt byte fails the read instead of
/// reaching Tantivy.
///
/// The file is opened once, in [`LocalSplitStorage::new`], and read with
/// positional reads: an open handle stays readable after the hot tier
/// demotes the split and unlinks its path, so a split that opened keeps
/// serving its request (warmup included) until its `Index` is dropped.
#[derive(Clone, Debug)]
pub struct LocalSplitStorage {
    pub path: PathBuf,
    file: Arc<std::fs::File>,
    checksums: Arc<SplitChecksums>,
    uri: Uri,
}

impl LocalSplitStorage {
    /// Opens `path` and its block checksums (`<path>.crc`, row F3); a file
    /// that is gone, or whose checksums are missing, unreadable or for
    /// another size, is `Unavailable`.
    pub fn new(path: PathBuf) -> Result<Self, ServiceError> {
        let uri = Uri::from_str(&format!("file://{}", path.display()))
            .map_err(|err| ServiceError::Internal(format!("{}: {err}", path.display())))?;
        let file = std::fs::File::open(&path)
            .map_err(|err| ServiceError::Unavailable(format!("{}: {err}", path.display())))?;
        let checksums = SplitChecksums::read_for(&path).map_err(|err| {
            ServiceError::Unavailable(format!("{}: its checksums: {err}", path.display()))
        })?;
        let len = file
            .metadata()
            .map_err(|err| ServiceError::Unavailable(format!("{}: {err}", path.display())))?
            .len();
        if len != checksums.size() {
            return Err(ServiceError::Unavailable(format!(
                "{} is {len} bytes, its checksums cover {}",
                path.display(),
                checksums.size()
            )));
        }
        Ok(Self {
            path,
            file: Arc::new(file),
            checksums: Arc::new(checksums),
            uri,
        })
    }

    /// Bytes `range` of the file (all of it for `None`), read in whole
    /// checked blocks: a block that fails its checksum fails the read.
    async fn read(&self, range: Option<Range<usize>>) -> StorageResult<OwnedBytes> {
        let size = self.checksums.size();
        let (start, end) = match range {
            Some(range) => (range.start as u64, range.end as u64),
            None => (0, size),
        };
        if start > end || end > size {
            return Err(StorageErrorKind::Io.with_error(anyhow::anyhow!(
                "{}: bytes {start}..{end} of a {size}-byte file",
                self.path.display()
            )));
        }
        let (from, to) = self.checksums.aligned(start, end);
        let (file, checksums, path) =
            (self.file.clone(), self.checksums.clone(), self.path.clone());
        let bytes = tokio::task::spawn_blocking(move || -> StorageResult<Vec<u8>> {
            let mut out = vec![0; (to - from) as usize];
            file.read_exact_at(&mut out, from).map_err(io_error)?;
            checksums.verify(from, &out).map_err(|err| {
                StorageErrorKind::Io.with_error(anyhow::anyhow!("{}: {err}", path.display()))
            })?;
            out.truncate((end - from) as usize);
            out.drain(..(start - from) as usize);
            Ok(out)
        })
        .await
        .map_err(|err| StorageErrorKind::Internal.with_error(err))??;
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
