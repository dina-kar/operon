//! Fragment prefetch (plan M1.3 Task 7 rule 2; Ruling 9): reads every data,
//! deletion and index file of a Lance dataset version through the range
//! cache, so the Lance reads of a collection with effective `fragments` hit
//! H1 instead of the object store. Lance files are create-only, so a file
//! read once never needs reading again: [`FragmentProgress`] remembers the
//! files done, and a pass that stops at its byte budget resumes on the next
//! one where it stopped.

use std::collections::HashSet;

use lance::Dataset;
use lance::index::DatasetIndexExt;
use lance_table::io::deletion::relative_deletion_file_path;
use operon_cache::RangeCache;
use operon_store::Store;

use crate::TierError;

/// Each read of a prefetch is this many cache blocks (rule 2).
pub const PREFETCH_PIECE_BLOCKS: u64 = 16;

/// What has been prefetched of a collection's Lance files, across passes
/// and versions.
#[derive(Clone, Debug, Default)]
pub struct FragmentProgress {
    /// Files read completely.
    done: HashSet<String>,
    /// The file being read and the bytes of it read so far.
    partial: Option<(String, u64)>,
}

/// What one prefetch pass did, and where the version stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrefetchPass {
    /// Bytes read through the cache by this pass.
    pub read: u64,
    /// Bytes of this version's files read so far (this pass and earlier).
    pub prefetched: u64,
    /// Bytes of all of this version's files.
    pub total: u64,
}

impl PrefetchPass {
    /// Whether every file of the version has been read.
    pub fn complete(&self) -> bool {
        self.prefetched >= self.total
    }
}

/// Reads every data, deletion and index file of `dataset` through `cache` (Ruling 9), up to `max_bytes`; returns bytes read.
pub async fn prefetch_fragments(
    cache: &RangeCache,
    store: &Store,
    lance_prefix: &str,
    dataset: &Dataset,
    max_bytes: u64,
) -> Result<u64, TierError> {
    let mut progress = FragmentProgress::default();
    let pass = prefetch_fragments_resuming(
        cache,
        store,
        lance_prefix,
        dataset,
        &mut progress,
        max_bytes,
    )
    .await?;
    Ok(pass.read)
}

/// [`prefetch_fragments`] that skips the files `progress` has done and
/// resumes a partly read file where it stopped, and records what it read.
pub async fn prefetch_fragments_resuming(
    cache: &RangeCache,
    store: &Store,
    lance_prefix: &str,
    dataset: &Dataset,
    progress: &mut FragmentProgress,
    max_bytes: u64,
) -> Result<PrefetchPass, TierError> {
    let files = dataset_files(cache, store, lance_prefix, dataset).await?;
    let total = files.iter().map(|(_, size)| size).sum();
    let piece = PREFETCH_PIECE_BLOCKS
        .saturating_mul(cache.block_size())
        .max(1);
    let mut read = 0u64;
    for (path, size) in &files {
        if progress.done.contains(path) {
            continue;
        }
        let mut offset = match &progress.partial {
            Some((partial, offset)) if partial == path => *offset,
            _ => 0,
        };
        while offset < *size && read < max_bytes {
            let end = offset
                .saturating_add(piece)
                .min(*size)
                .min(offset.saturating_add(max_bytes - read));
            cache.read_with_size(path, *size, offset..end).await?;
            read += end - offset;
            offset = end;
        }
        if offset >= *size {
            progress.done.insert(path.clone());
            progress.partial = None;
        } else {
            progress.partial = Some((path.clone(), offset));
            break;
        }
    }
    let prefetched = files
        .iter()
        .map(|(path, size)| match &progress.partial {
            _ if progress.done.contains(path) => *size,
            Some((partial, offset)) if partial == path => *offset,
            _ => 0,
        })
        .sum();
    Ok(PrefetchPass {
        read,
        prefetched,
        total,
    })
}

/// Every file of `dataset` that a read may touch, with its size, in a fixed
/// order: each fragment's data files and deletion file (fragments by id),
/// then every object under `_indices/<uuid>/` of each index (by path).
async fn dataset_files(
    cache: &RangeCache,
    store: &Store,
    lance_prefix: &str,
    dataset: &Dataset,
) -> Result<Vec<(String, u64)>, TierError> {
    let mut files = Vec::new();
    let mut fragments: Vec<_> = dataset.fragments().iter().collect();
    fragments.sort_by_key(|fragment| fragment.id);
    for fragment in fragments {
        for file in &fragment.files {
            if file.base_id.is_some() {
                // Operon datasets have one base; another base is not ours.
                continue;
            }
            let path = format!("{lance_prefix}data/{}", file.path);
            let size = match file.file_size_bytes.get() {
                Some(size) => size.get(),
                None => cache.size(&path).await?,
            };
            files.push((path, size));
        }
        if let Some(deletion) = &fragment.deletion_file {
            let path = format!(
                "{lance_prefix}{}",
                relative_deletion_file_path(fragment.id, deletion)
            );
            let size = cache.size(&path).await?;
            files.push((path, size));
        }
    }
    let indices = dataset
        .load_indices()
        .await
        .map_err(operon_collection::CollectionError::from)?;
    let mut uuids: Vec<String> = indices.iter().map(|index| index.uuid.to_string()).collect();
    uuids.sort_unstable();
    uuids.dedup();
    for uuid in uuids {
        for object in store
            .list(&format!("{lance_prefix}_indices/{uuid}/"))
            .await?
        {
            files.push((object.path, object.size));
        }
    }
    Ok(files)
}
