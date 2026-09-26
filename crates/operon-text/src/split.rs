//! Splits: a single-segment Tantivy index bundled with its hotcache into one
//! immutable object (plan M1.1 Task 8 rules 6–7, Rulings 8–10).
//!
//! A split is Quickwit's bundle: every index file, then the bundle metadata
//! and the hotcache (the footer), then the 16-byte footer trailer (Ruling 9).
//! `footer_range` is recorded in the manifest, so [`open_split`] reads the
//! footer in one ranged GET and needs nothing else to open the index.

use std::collections::BTreeSet;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use operon_quickwit::directories::{
    CachingDirectory, HotDirectory, StorageDirectory, write_hotcache,
};
use operon_quickwit::storage::{BundleStorage, PutPayload, SplitPayloadBuilder, Storage};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::{Directory, RamDirectory};
use tantivy::indexer::SingleSegmentIndexWriter;
use tantivy::schema::Schema;
use tantivy::{Index, IndexSettings, ReloadPolicy, Searcher, TantivyDocument};

use crate::analyzers::tokenizer_manager;
use crate::error::TextError;

/// The memory budget of the split writer.
pub const SPLIT_WRITER_MEMORY: usize = 256 * 1024 * 1024;

/// A split's bytes, the range of its footer (bundle metadata, hotcache and
/// trailer, which end the split) and its number of documents.
#[derive(Clone, Debug)]
pub struct BuiltSplit {
    pub bytes: Bytes,
    pub footer_range: Range<u64>,
    pub doc_count: u64,
}

/// The files of an index that a split bundles: `meta.json`, `.managed.json`
/// (read by `Index::open`) and every file of every segment that exists.
const INDEX_FILES: [&str; 2] = ["meta.json", ".managed.json"];

/// Builds a split of `docs` with `schema`. Doc id *i* is `docs[i]` (Ruling
/// 8): the documents go into one segment in order.
pub fn build_split(schema: Schema, docs: Vec<TantivyDocument>) -> Result<BuiltSplit, TextError> {
    build_split_from(schema, docs)
}

/// [`build_split`] over documents that arrive one by one: doc id *i* is the
/// *i*-th document `docs` yields, and none is kept after it is indexed, so
/// a caller can stream them (a split merge, M1.3).
pub fn build_split_from(
    schema: Schema,
    docs: impl IntoIterator<Item = TantivyDocument>,
) -> Result<BuiltSplit, TextError> {
    let directory = RamDirectory::create();
    let mut index = Index::create(directory.clone(), schema, IndexSettings::default())?;
    index.set_tokenizers(tokenizer_manager());
    index.set_fast_field_tokenizers(tokenizer_manager());
    let mut doc_count = 0u64;
    let mut writer = SingleSegmentIndexWriter::new(index, SPLIT_WRITER_MEMORY)?;
    for doc in docs {
        writer.add_document(doc)?;
        doc_count += 1;
    }
    let index = writer.finalize()?;

    let mut files: BTreeSet<PathBuf> = INDEX_FILES.iter().map(PathBuf::from).collect();
    for segment in index.load_metas()?.segments {
        files.extend(segment.list_files());
    }
    let mut builder = SplitPayloadBuilder::default();
    for file in files {
        let bytes = match directory.atomic_read(&file) {
            Ok(bytes) => bytes,
            Err(OpenReadError::FileDoesNotExist(_)) => continue,
            Err(err) => return Err(TextError::Tantivy(err.into())),
        };
        let payload: Box<dyn PutPayload> = Box::new(bytes);
        builder.add_payload(file.to_string_lossy().into_owned(), payload);
    }
    let mut hotcache = Vec::new();
    write_hotcache(directory, &mut hotcache)?;
    let payload = builder
        .finalize(&hotcache)
        .map_err(|err| TextError::Other(format!("bundling a split: {err:#}")))?;
    let bytes = payload
        .range_bytes(0..payload.len())
        .map_err(|err| TextError::Other(format!("reading a split bundle: {err}")))?;
    Ok(BuiltSplit {
        bytes: Bytes::from_owner(bytes),
        footer_range: payload.footer_range,
        doc_count,
    })
}

/// Quickwit's footer trailer: the footer's start (u64 LE), the trailer
/// version (u32 LE) and the magic (`bundle_storage.rs`).
const TRAILER_LEN: usize = 16;
const TRAILER_VERSION: u32 = 1;
const TRAILER_MAGIC: &[u8; 4] = b"QWFT";
/// The two u32 LE lengths of the footer: bundle metadata and hotcache.
const LEN_FIELD: usize = 4;

fn corrupt(split_path: &str, message: impl std::fmt::Display) -> TextError {
    TextError::Corrupt(format!("split {split_path}: {message}"))
}

fn u32_at(bytes: &[u8], at: usize) -> usize {
    let mut le = [0; 4];
    le.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(le) as usize
}

/// Checks that `footer` (at least 24 bytes) is exactly the footer at
/// `footer_range`: its trailer names the same start, and its two lengths
/// account for every byte, so the bundle parser never slices past it
/// (which panics) whatever the split or its reference is.
fn check_footer(
    split_path: &str,
    footer_range: &Range<u64>,
    footer: &[u8],
) -> Result<(), TextError> {
    let len = footer.len();
    if len as u64 != footer_range.end - footer_range.start {
        return Err(corrupt(
            split_path,
            format!("read {len} footer bytes, expected {footer_range:?}"),
        ));
    }
    let trailer = &footer[len - TRAILER_LEN..];
    let mut start = [0; 8];
    start.copy_from_slice(&trailer[..8]);
    if &trailer[12..] != TRAILER_MAGIC || u32_at(trailer, 8) != TRAILER_VERSION as usize {
        return Err(corrupt(
            split_path,
            "no footer trailer at the end of footer_range",
        ));
    }
    if u64::from_le_bytes(start) != footer_range.start {
        return Err(corrupt(
            split_path,
            format!(
                "the trailer puts the footer at {}, not {}",
                u64::from_le_bytes(start),
                footer_range.start
            ),
        ));
    }
    let hotcache_len = u32_at(footer, len - TRAILER_LEN - LEN_FIELD);
    let Some(metadata_len_at) = (len - TRAILER_LEN - LEN_FIELD)
        .checked_sub(hotcache_len)
        .and_then(|at| at.checked_sub(LEN_FIELD))
    else {
        return Err(corrupt(
            split_path,
            format!("a {hotcache_len}-byte hotcache overflows the footer"),
        ));
    };
    let metadata_len = u32_at(footer, metadata_len_at);
    if metadata_len != metadata_len_at {
        return Err(corrupt(
            split_path,
            format!(
                "{metadata_len} bytes of bundle metadata do not fill the footer's {metadata_len_at}"
            ),
        ));
    }
    Ok(())
}

/// Opens the split at `split_path` (a path of `storage`), which is
/// `split_size` bytes and has its footer at `footer_range`, with exactly one
/// ranged GET (Ruling 9): the footer, read without a HEAD because the caller
/// knows the size (`SplitRef.size_bytes`). A `footer_range` that does not
/// end the split, a truncated split or a footer whose lengths do not add up
/// is [`TextError::Corrupt`] or a storage error.
///
/// The index reads through a `HotDirectory` (the hotcache) over a
/// `CachingDirectory` over the bundle, so its reads are async: warm it (see
/// [`warm_up_all`]) before a synchronous search.
pub async fn open_split(
    storage: Arc<dyn Storage>,
    split_path: &str,
    split_size: u64,
    footer_range: Range<u64>,
) -> Result<Index, TextError> {
    let min_footer = (TRAILER_LEN + 2 * LEN_FIELD) as u64;
    if footer_range.end != split_size
        || footer_range.start > footer_range.end
        || footer_range.end - footer_range.start < min_footer
    {
        return Err(corrupt(
            split_path,
            format!("footer_range {footer_range:?} does not end a split of {split_size} bytes"),
        ));
    }
    let path = PathBuf::from(split_path);
    let footer = storage
        .get_slice_with_file_len(
            &path,
            split_size,
            footer_range.start as usize..footer_range.end as usize,
        )
        .await?;
    check_footer(split_path, &footer_range, footer.as_slice())?;
    let (bundle, hotcache) = BundleStorage::open_from_split_bytes(storage, path, footer)
        .map_err(|err| corrupt(split_path, format!("{err:#}")))?;
    let directory =
        CachingDirectory::new_unbounded(Arc::new(StorageDirectory::new(Arc::new(bundle))));
    let directory = HotDirectory::open(directory, hotcache)
        .map_err(|err| corrupt(split_path, format!("hotcache: {err:#}")))?;
    let mut index = Index::open(directory)?;
    index.set_tokenizers(tokenizer_manager());
    index.set_fast_field_tokenizers(tokenizer_manager());
    Ok(index)
}

/// Warms every term dictionary, posting list, fast field and field norm of `index`, so that
/// synchronous search works over the async directory (tests and small splits; M1.2 warms per query).
///
/// It reads every file of every segment whole, once, into the split's
/// `CachingDirectory`, then opens a searcher (which only reads warm bytes).
pub async fn warm_up_all(index: &Index) -> Result<Searcher, TextError> {
    let directory = index.directory();
    let mut files = BTreeSet::new();
    for segment in index.searchable_segment_metas()? {
        files.extend(segment.list_files());
    }
    let mut reads = Vec::new();
    for file in files {
        if !directory
            .exists(&file)
            .map_err(tantivy::TantivyError::from)?
        {
            continue;
        }
        let handle = directory
            .get_file_handle(&file)
            .map_err(tantivy::TantivyError::from)?;
        reads.push(async move {
            let len = handle.len();
            handle.read_bytes_async(0..len).await
        });
    }
    futures::future::try_join_all(reads)
        .await
        .map_err(|err| TextError::Tantivy(err.into()))?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    Ok(reader.searcher())
}
