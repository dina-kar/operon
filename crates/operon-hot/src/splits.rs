//! Pinned splits (plan M1.3 Task 7 rule 1): whole Tantivy split files on
//! local NVMe for the collections this node owns with effective `text`, so
//! M1.2's text path opens them locally instead of ranged reads through the
//! cache (`HotTier::split_file`).
//!
//! A split is downloaded to `<ulid>.split.tmp`, fsynced, checked against its
//! manifest size and Quickwit trailer, then renamed to `<ulid>.split`: a path
//! the tier hands out always names a whole, checked file. A split that leaves
//! every live manifest stays served for `split_linger`, then is removed from
//! the map and deleted; an evicted split leaves the map at once and its file
//! is deleted after `split_linger`, so a query that already holds its path
//! keeps reading it (M1.2 falls back to the object store if opening fails,
//! B10).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use operon_common::{CollectionId, NamespaceId};
use operon_store::Store;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

use crate::TierError;

/// Each `get_range` of a split download (rule 1).
pub const SPLIT_PIECE_BYTES: u64 = 64 << 20;

/// Quickwit's footer trailer: `footer_start u64 LE | 1u32 LE | b"QWFT"`.
const TRAILER_LEN: usize = 16;
const TRAILER_VERSION: u32 = 1;
const TRAILER_MAGIC: &[u8; 4] = b"QWFT";

/// The temporary name of a download into `to`: `<to>.tmp`.
fn tmp_path(to: &Path) -> PathBuf {
    let mut name = to.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// Downloads the split object `path` (`size` bytes, footer at
/// `footer_start`) to `to` (rule 1): `get_range` pieces of
/// [`SPLIT_PIECE_BYTES`] into `<to>.tmp`, an fsync, the size and trailer
/// checks, then a rename to `to`. Any failure removes the temporary file; a
/// size or trailer mismatch is [`TierError::Corrupt`].
pub async fn download_split(
    store: &Store,
    path: &str,
    size: u64,
    footer_start: u64,
    to: &Path,
) -> Result<(), TierError> {
    download_split_in(store, path, size, footer_start, to, SPLIT_PIECE_BYTES).await
}

/// [`download_split`] with `piece`-byte GETs.
pub(crate) async fn download_split_in(
    store: &Store,
    path: &str,
    size: u64,
    footer_start: u64,
    to: &Path,
    piece: u64,
) -> Result<(), TierError> {
    if let Some(parent) = to.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = tmp_path(to);
    let written = write_split(store, path, size, footer_start, &tmp, piece.max(1)).await;
    let renamed = match written {
        Ok(()) => tokio::fs::rename(&tmp, to).await.map_err(TierError::from),
        Err(err) => Err(err),
    };
    if renamed.is_err() {
        match tokio::fs::remove_file(&tmp).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(tmp = %tmp.display(), %err, "removing a failed split download");
            }
        }
    }
    renamed
}

async fn write_split(
    store: &Store,
    path: &str,
    size: u64,
    footer_start: u64,
    tmp: &Path,
    piece: u64,
) -> Result<(), TierError> {
    let corrupt = |message: String| TierError::Corrupt(format!("split {path}: {message}"));
    if size < TRAILER_LEN as u64 {
        return Err(corrupt(format!("{size} bytes cannot hold a footer")));
    }
    let mut file = tokio::fs::File::create(tmp).await?;
    let mut tail: Vec<u8> = Vec::with_capacity(TRAILER_LEN);
    let mut offset = 0;
    while offset < size {
        let end = offset.saturating_add(piece).min(size);
        let bytes = if offset == 0 {
            // The first piece also reports the object's size: no HEAD.
            let (bytes, info) = store.get_range_with_info(path, 0..end).await?;
            if info.size != size {
                return Err(corrupt(format!(
                    "the object is {} bytes, the manifest says {size}",
                    info.size
                )));
            }
            bytes
        } else {
            store.get_range(path, offset..end).await?
        };
        if bytes.len() as u64 != end - offset {
            return Err(corrupt(format!(
                "read {} bytes at {offset}, expected {}",
                bytes.len(),
                end - offset
            )));
        }
        file.write_all(&bytes).await?;
        tail.extend_from_slice(&bytes[bytes.len().saturating_sub(TRAILER_LEN)..]);
        let extra = tail.len().saturating_sub(TRAILER_LEN);
        tail.drain(..extra);
        offset = end;
    }
    file.flush().await?;
    file.sync_all().await?;
    let written = file.metadata().await?.len();
    if written != size {
        return Err(corrupt(format!("wrote {written} bytes, expected {size}")));
    }
    let mut start = [0; 8];
    start.copy_from_slice(&tail[..8]);
    let mut version = [0; 4];
    version.copy_from_slice(&tail[8..12]);
    if &tail[12..] != TRAILER_MAGIC || u32::from_le_bytes(version) != TRAILER_VERSION {
        return Err(corrupt("no Quickwit footer trailer at its end".to_string()));
    }
    if u64::from_le_bytes(start) != footer_start {
        return Err(corrupt(format!(
            "the trailer puts the footer at {}, the manifest at {footer_start}",
            u64::from_le_bytes(start)
        )));
    }
    Ok(())
}

/// A split key: namespace, collection and split ULID.
pub(crate) type SplitKey = (NamespaceId, CollectionId, Ulid);

#[derive(Clone, Debug)]
struct PinnedSplit {
    path: PathBuf,
    size: u64,
    /// The last pass that saw it in its collection's live manifest.
    last_referenced: Instant,
}

/// The splits pinned on this node, by `(namespace, collection, ulid)`.
#[derive(Debug)]
pub struct PinnedSplits {
    /// `<hot dir>/splits`.
    dir: PathBuf,
    map: RwLock<HashMap<SplitKey, PinnedSplit>>,
    /// Collections whose splits are served: owned, with effective `text`.
    serving: RwLock<HashSet<(NamespaceId, CollectionId)>>,
    /// Files no longer in the map, deleted once `split_linger` has passed
    /// since they left it.
    lingering: Mutex<Vec<(PathBuf, Instant)>>,
}

impl PinnedSplits {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            map: RwLock::new(HashMap::new()),
            serving: RwLock::new(HashSet::new()),
            lingering: Mutex::new(Vec::new()),
        }
    }

    /// The local file of `split` while it is pinned and its collection's
    /// splits are served (a map lookup, no I/O).
    pub fn path(&self, ns: NamespaceId, cid: CollectionId, split: Ulid) -> Option<PathBuf> {
        if !self
            .serving
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&(ns, cid))
        {
            return None;
        }
        self.map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(ns, cid, split))
            .map(|pinned| pinned.path.clone())
    }

    /// Where `split` is downloaded to: `<dir>/<ns>/<cid>/<ulid>.split`.
    pub(crate) fn local_path(&self, ns: NamespaceId, cid: CollectionId, split: Ulid) -> PathBuf {
        self.dir
            .join(ns.to_string())
            .join(cid.to_string())
            .join(format!("{split}.split"))
    }

    pub(crate) fn contains(&self, key: &SplitKey) -> bool {
        self.map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(key)
    }

    pub(crate) fn insert(&self, key: SplitKey, path: PathBuf, size: u64, now: Instant) {
        self.map
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                key,
                PinnedSplit {
                    path,
                    size,
                    last_referenced: now,
                },
            );
    }

    /// Serves (or stops serving) the splits of `(ns, cid)`.
    pub(crate) fn set_serving(&self, ns: NamespaceId, cid: CollectionId, serving: bool) {
        let mut set = self.serving.write().unwrap_or_else(PoisonError::into_inner);
        match serving {
            true => set.insert((ns, cid)),
            false => set.remove(&(ns, cid)),
        };
    }

    /// Keeps serving only `keep`.
    pub(crate) fn retain_serving(&self, keep: &HashSet<(NamespaceId, CollectionId)>) {
        self.serving
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|key| keep.contains(key));
    }

    /// Marks the pinned splits of `(ns, cid)` named in `live` as referenced
    /// at `now`.
    pub(crate) fn touch(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        live: impl IntoIterator<Item = Ulid>,
        now: Instant,
    ) {
        let mut map = self.map.write().unwrap_or_else(PoisonError::into_inner);
        for ulid in live {
            if let Some(pinned) = map.get_mut(&(ns, cid, ulid)) {
                pinned.last_referenced = now;
            }
        }
    }

    /// Drops `key` from the map now (an eviction); its file is deleted once
    /// `split_linger` has passed.
    pub(crate) fn evict(&self, key: &SplitKey, now: Instant) {
        let removed = self
            .map
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
        if let Some(pinned) = removed {
            self.lingering
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((pinned.path, now));
        }
    }

    /// Removes the splits unreferenced for `linger` from the map and
    /// returns their files, with the evicted files whose linger has passed:
    /// the caller deletes them.
    pub(crate) fn expire(&self, linger: Duration, now: Instant) -> Vec<PathBuf> {
        let mut out = Vec::new();
        self.map
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, pinned| {
                let keep = now.saturating_duration_since(pinned.last_referenced) < linger;
                if !keep {
                    out.push(pinned.path.clone());
                }
                keep
            });
        self.lingering
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(path, since)| {
                let keep = now.saturating_duration_since(*since) < linger;
                if !keep {
                    out.push(path.clone());
                }
                keep
            });
        out
    }

    /// Every pinned split with its size, by collection.
    pub(crate) fn by_collection(&self) -> BTreeMap<(NamespaceId, CollectionId), Vec<(Ulid, u64)>> {
        let mut out: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for ((ns, cid, ulid), pinned) in self
            .map
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            out.entry((*ns, *cid))
                .or_default()
                .push((*ulid, pinned.size));
        }
        out
    }

    /// Forgets everything (shutdown); files are left for the next start.
    pub(crate) fn clear(&self) {
        self.map
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.serving
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        self.lingering
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// Deletes `files` on a blocking thread; a missing file is fine.
pub(crate) async fn delete_files(files: Vec<PathBuf>) {
    if files.is_empty() {
        return;
    }
    let deleted = tokio::task::spawn_blocking(move || {
        for file in files {
            match std::fs::remove_file(&file) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    tracing::warn!(file = %file.display(), %err, "deleting a pinned split");
                }
            }
        }
    })
    .await;
    if let Err(err) = deleted {
        tracing::warn!(%err, "deleting pinned splits");
    }
}
