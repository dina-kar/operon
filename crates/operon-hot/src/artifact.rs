//! The hot HNSW artifact format (plan M1.3 Task 4, Rulings 1 and 6).
//!
//! An artifact lives under
//! `ns/<ns>/collections/<cid>/hot/hnsw/<column>/<source_version:020>-<ulid>/`:
//! - `files/<relative path>.<chunk:06>`: every file the engine wrote, cut into
//!   `chunk_bytes` pieces, each stored as one zstd frame (a single PUT is
//!   capped at 5 GiB, and qdrant-edge's sparse page files compress to almost
//!   nothing);
//! - `covered.bin` (`OPHC`): the row ids the build scanned;
//! - `descriptor.bin` (`OPHD`), written last: the engine, the spec, the source
//!   manifest version, and every file's size and crc32c. A reader trusts a
//!   prefix only through its descriptor, so a half-uploaded artifact is
//!   inert.
//!
//! An artifact built from manifest *s* stays **current** at every later
//! manifest until a link commit inserts or deletes rows (Ruling 1):
//! [`currency`].

use std::io::Read;
use std::path::{Component, Path};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use operon_collection::{CollectionError, CollectionManifest, CommitKind, ManifestCache};
use operon_common::meta::collection_prefix;
use operon_common::{CollectionId, NamespaceId};
use operon_hnsw::{BuildSpec, BuiltFiles, engine_by_name};
use operon_store::{Store, StoreError};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ulid::Ulid;

use crate::TierError;

/// `HotArtifactRef::kind` of an HNSW artifact.
pub const HNSW_KIND: &str = "hnsw";
pub const DESCRIPTOR_FILE: &str = "descriptor.bin";
pub const COVERED_FILE: &str = "covered.bin";
pub const FILES_DIR: &str = "files/";
pub const DESCRIPTOR_MAGIC: &[u8; 4] = b"OPHD";
pub const COVERED_MAGIC: &[u8; 4] = b"OPHC";
pub const ARTIFACT_FORMAT_VERSION: u16 = 1;

/// The zstd level of every chunk.
const ZSTD_LEVEL: i32 = 3;
/// Magic and format version.
const HEADER_LEN: usize = 6;
/// The crc32c trailer.
const CRC_LEN: usize = 4;
/// The results [`CurrencyCache`] keeps.
const CURRENCY_CACHE_ENTRIES: u64 = 10_000;

/// What an artifact holds and how to verify it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArtifactDescriptor {
    /// `HnswEngine::name`.
    pub engine: String,
    pub collection: u64,
    /// `_vector_<i>`.
    pub column: String,
    /// `VectorSpec.name`.
    pub vector: String,
    pub spec: BuildSpec,
    /// The manifest version the build read.
    pub source_version: u64,
    /// That manifest's Lance version.
    pub lance_version: u64,
    /// The dataset's `next_row_id` at that version: rows at or above it
    /// were inserted after the build.
    pub next_row_id: u64,
    /// Points in the index (rows with the vector).
    pub points: u64,
    /// Rows the build scanned (the covered set's cardinality).
    pub scanned: u64,
    pub files: Vec<ArtifactFile>,
    /// The length and crc32c of the whole `covered.bin` object.
    pub covered_len: u64,
    pub covered_crc32c: u32,
    /// The uncompressed size of every chunk but a file's last.
    pub chunk_bytes: u64,
    pub created_at_ms: u64,
}

/// One engine file of an artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFile {
    /// Relative, '/'-separated.
    pub path: String,
    /// Uncompressed bytes.
    pub size: u64,
    /// crc32c of the whole uncompressed file.
    pub crc32c: u32,
    /// Chunks stored (at least 1: an empty file is one empty chunk).
    pub chunks: u32,
}

/// `ns/<ns>/collections/<cid>/hot/hnsw/<column>/<source_version:020>-<ulid>/`.
pub fn artifact_prefix(
    ns: NamespaceId,
    cid: CollectionId,
    column: &str,
    source_version: u64,
    ulid: Ulid,
) -> String {
    format!(
        "{}hot/{HNSW_KIND}/{column}/{source_version:020}-{ulid}/",
        collection_prefix(ns, cid)
    )
}

/// `prefix` + `files/` + `file` + `.` + `chunk:06`.
pub fn chunk_path(prefix: &str, file: &str, chunk: u32) -> String {
    format!("{prefix}{FILES_DIR}{file}.{chunk:06}")
}

fn corrupt(message: impl Into<String>) -> TierError {
    TierError::Corrupt(message.into())
}

/// `magic | u16 LE version | body | u32 LE crc32c` of everything before it.
fn envelope(magic: &[u8; 4], body: impl FnOnce(&mut Vec<u8>)) -> Bytes {
    let mut out = Vec::new();
    out.extend_from_slice(magic);
    out.extend_from_slice(&ARTIFACT_FORMAT_VERSION.to_le_bytes());
    body(&mut out);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Bytes::from(out)
}

/// The body of an envelope written by [`envelope`], after checking its
/// magic, version and checksum.
fn open_envelope<'a>(what: &str, magic: &[u8; 4], bytes: &'a [u8]) -> Result<&'a [u8], TierError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(corrupt(format!("{what}: {} bytes", bytes.len())));
    }
    if &bytes[..4] != magic {
        return Err(corrupt(format!("{what}: bad magic")));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != ARTIFACT_FORMAT_VERSION {
        return Err(corrupt(format!("{what}: unknown format version {version}")));
    }
    let (covered, trailer) = bytes.split_at(bytes.len() - CRC_LEN);
    let stored = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    if crc32c::crc32c(covered) != stored {
        return Err(corrupt(format!("{what}: checksum mismatch")));
    }
    Ok(&covered[HEADER_LEN..])
}

/// `OPHD | u16 LE 1 | postcard(descriptor) | u32 LE crc32c`.
pub fn encode_descriptor(d: &ArtifactDescriptor) -> Bytes {
    envelope(DESCRIPTOR_MAGIC, |out| {
        // Plain structs, sequences and enums: postcard cannot fail on them.
        let body = postcard::to_stdvec(d).expect("an artifact descriptor encodes");
        out.extend_from_slice(&body);
    })
}

/// The descriptor in `bytes`; `Corrupt` unless it is exactly what
/// [`encode_descriptor`] writes.
pub fn decode_descriptor(bytes: &[u8]) -> Result<ArtifactDescriptor, TierError> {
    let body = open_envelope("descriptor", DESCRIPTOR_MAGIC, bytes)?;
    let (descriptor, rest) = postcard::take_from_bytes::<ArtifactDescriptor>(body)
        .map_err(|err| corrupt(format!("descriptor: {err}")))?;
    if !rest.is_empty() {
        return Err(corrupt(format!(
            "descriptor: {} trailing bytes",
            rest.len()
        )));
    }
    Ok(descriptor)
}

/// `OPHC | u16 LE 1 | u64 LE cardinality | RoaringTreemap | u32 LE crc32c`.
pub fn encode_covered(rows: &RoaringTreemap) -> Bytes {
    envelope(COVERED_MAGIC, |out| {
        out.extend_from_slice(&rows.len().to_le_bytes());
        rows.serialize_into(&mut *out)
            .expect("serializing into a Vec cannot fail");
    })
}

/// The covered set in `bytes`; `Corrupt` unless it is exactly what
/// [`encode_covered`] writes.
pub fn decode_covered(bytes: &[u8]) -> Result<RoaringTreemap, TierError> {
    let body = open_envelope("covered set", COVERED_MAGIC, bytes)?;
    if body.len() < 8 {
        return Err(corrupt("covered set: no cardinality"));
    }
    let (cardinality, mut rest) = body.split_at(8);
    let mut le = [0; 8];
    le.copy_from_slice(cardinality);
    let rows = RoaringTreemap::deserialize_from(&mut rest)
        .map_err(|err| corrupt(format!("covered set: {err}")))?;
    if !rest.is_empty() {
        return Err(corrupt(format!(
            "covered set: {} trailing bytes",
            rest.len()
        )));
    }
    if rows.len() != u64::from_le_bytes(le) {
        return Err(corrupt(format!(
            "covered set: {} rows, the header says {}",
            rows.len(),
            u64::from_le_bytes(le)
        )));
    }
    Ok(rows)
}

/// A relative, '/'-separated path with only normal components.
fn check_relative(path: &str) -> Result<(), TierError> {
    let normal = !path.is_empty()
        && Path::new(path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)));
    match normal {
        true => Ok(()),
        false => Err(corrupt(format!("file path {path:?} is not relative"))),
    }
}

/// PUTs `bytes` create-only at `path`. An `AlreadyExists` is success only
/// if the stored bytes are identical (a retried build with the same prefix,
/// or a lost acknowledgement).
async fn put_same(store: &Store, path: &str, bytes: Bytes) -> Result<(), TierError> {
    match store.put_if_absent(path, bytes.clone()).await {
        Ok(_) => Ok(()),
        Err(err @ StoreError::AlreadyExists { .. }) => match store.get(path).await {
            Ok((stored, _)) if stored == bytes => Ok(()),
            Ok(_) => Err(TierError::Other(format!(
                "{path} exists with other content"
            ))),
            Err(StoreError::NotFound { .. }) => Err(err.into()),
            Err(other) => Err(other.into()),
        },
        Err(err) => Err(err.into()),
    }
}

/// Reads up to `limit` bytes from `file`.
async fn read_piece(file: &mut tokio::fs::File, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut piece = Vec::new();
    file.take(limit).read_to_end(&mut piece).await?;
    Ok(piece)
}

/// Uploads every file of `dir` (the `BuiltFiles`) in zstd chunks, then
/// `covered.bin`, then `descriptor.bin` (rule 3), and returns the
/// descriptor with `files`, `covered_len` and `covered_crc32c` filled in.
/// Retrying with the same prefix and the same files succeeds.
pub async fn publish(
    store: &Store,
    prefix: &str,
    dir: &Path,
    built: &BuiltFiles,
    covered: &RoaringTreemap,
    mut descriptor: ArtifactDescriptor,
) -> Result<ArtifactDescriptor, TierError> {
    if descriptor.chunk_bytes == 0 {
        return Err(TierError::Other("chunk_bytes must be positive".into()));
    }
    if descriptor.engine != built.engine {
        return Err(TierError::Other(format!(
            "the descriptor names engine {}, the files are {}'s",
            descriptor.engine, built.engine
        )));
    }
    let mut files = Vec::with_capacity(built.files.len());
    for name in &built.files {
        check_relative(name)?;
        let mut file = tokio::fs::File::open(dir.join(name)).await?;
        let (mut size, mut crc, mut chunks) = (0u64, 0u32, 0u32);
        loop {
            let piece = read_piece(&mut file, descriptor.chunk_bytes).await?;
            if piece.is_empty() && chunks > 0 {
                break;
            }
            let len = piece.len() as u64;
            crc = crc32c::crc32c_append(crc, &piece);
            let frame =
                tokio::task::spawn_blocking(move || zstd::encode_all(&piece[..], ZSTD_LEVEL))
                    .await
                    .map_err(|err| TierError::Other(format!("compressing a chunk: {err}")))??;
            put_same(store, &chunk_path(prefix, name, chunks), Bytes::from(frame)).await?;
            chunks = chunks
                .checked_add(1)
                .ok_or_else(|| TierError::Other(format!("{name} has too many chunks")))?;
            size += len;
            if len < descriptor.chunk_bytes {
                break;
            }
        }
        files.push(ArtifactFile {
            path: name.clone(),
            size,
            crc32c: crc,
            chunks,
        });
    }
    let covered = encode_covered(covered);
    descriptor.covered_len = covered.len() as u64;
    descriptor.covered_crc32c = crc32c::crc32c(&covered);
    put_same(store, &format!("{prefix}{COVERED_FILE}"), covered).await?;
    descriptor.files = files;
    put_same(
        store,
        &format!("{prefix}{DESCRIPTOR_FILE}"),
        encode_descriptor(&descriptor),
    )
    .await?;
    crate::failpoint!("hot.after_artifact_put");
    Ok(descriptor)
}

/// GETs `path`; a missing object is `Corrupt(missing)`.
async fn get_required(store: &Store, path: &str, missing: &str) -> Result<Bytes, TierError> {
    match store.get(path).await {
        Ok((bytes, _)) => Ok(bytes),
        Err(StoreError::NotFound { .. }) => Err(corrupt(missing)),
        Err(err) => Err(err.into()),
    }
}

/// Decompresses one chunk frame, refusing more than `limit` bytes.
fn decompress(frame: &[u8], limit: u64) -> Result<Vec<u8>, TierError> {
    let mut out = Vec::new();
    zstd::stream::read::Decoder::new(frame)
        .map_err(|err| corrupt(format!("a chunk: {err}")))?
        .take(limit + 1)
        .read_to_end(&mut out)
        .map_err(|err| corrupt(format!("a chunk: {err}")))?;
    if out.len() as u64 > limit {
        return Err(corrupt("a chunk is larger than chunk_bytes"));
    }
    Ok(out)
}

/// A file being written by [`download`].
struct Writing {
    index: usize,
    file: tokio::fs::File,
    size: u64,
    crc: u32,
}

impl Writing {
    async fn finish(mut self, descriptor: &ArtifactDescriptor) -> Result<(), TierError> {
        self.file.flush().await?;
        let want = &descriptor.files[self.index];
        if self.size != want.size || self.crc != want.crc32c {
            return Err(corrupt(format!(
                "{}: {} bytes with crc32c {:08x}, the descriptor says {} bytes with crc32c {:08x}",
                want.path, self.size, self.crc, want.size, want.crc32c
            )));
        }
        Ok(())
    }
}

/// Downloads and verifies an artifact into `dir` (created empty by the
/// caller), with at most `parallelism` chunk GETs in flight; returns its
/// descriptor and covered set. Any mismatch is `Corrupt`, and the caller
/// removes `dir`.
pub async fn download(
    store: &Store,
    prefix: &str,
    dir: &Path,
    parallelism: usize,
) -> Result<(ArtifactDescriptor, RoaringTreemap), TierError> {
    let bytes = get_required(
        store,
        &format!("{prefix}{DESCRIPTOR_FILE}"),
        "no descriptor",
    )
    .await?;
    let descriptor = decode_descriptor(&bytes)?;
    if engine_by_name(&descriptor.engine).is_none() {
        return Err(corrupt(format!("unknown engine {:?}", descriptor.engine)));
    }
    if descriptor.chunk_bytes == 0 {
        return Err(corrupt("chunk_bytes is 0"));
    }
    let bytes = get_required(store, &format!("{prefix}{COVERED_FILE}"), "no covered set").await?;
    if bytes.len() as u64 != descriptor.covered_len
        || crc32c::crc32c(&bytes) != descriptor.covered_crc32c
    {
        return Err(corrupt("the covered set does not match the descriptor"));
    }
    let covered = decode_covered(&bytes)?;
    if covered.len() != descriptor.scanned {
        return Err(corrupt(format!(
            "the covered set has {} rows, the descriptor says {} were scanned",
            covered.len(),
            descriptor.scanned
        )));
    }
    for file in &descriptor.files {
        check_relative(&file.path)?;
        let most = u64::from(file.chunks).saturating_mul(descriptor.chunk_bytes);
        if file.chunks == 0 || file.size > most {
            return Err(corrupt(format!(
                "{}: {} bytes in {} chunks",
                file.path, file.size, file.chunks
            )));
        }
    }

    let limit = descriptor.chunk_bytes;
    // Owned paths: a stream over borrowing iterator adaptors makes the
    // download future `Send` only for one lifetime, which a caller's spawned
    // future cannot use.
    let mut chunks = Vec::new();
    for (index, file) in descriptor.files.iter().enumerate() {
        for chunk in 0..file.chunks {
            chunks.push((index, chunk, chunk_path(prefix, &file.path, chunk)));
        }
    }
    let mut frames = futures::stream::iter(chunks)
        .map(|(index, chunk, path)| async move {
            let frame = get_required(store, &path, &format!("missing chunk {path}")).await?;
            let plain = tokio::task::spawn_blocking(move || decompress(&frame, limit))
                .await
                .map_err(|err| TierError::Other(format!("decompressing a chunk: {err}")))??;
            Ok::<_, TierError>((index, chunk, plain))
        })
        .buffered(parallelism.max(1));
    let mut writing: Option<Writing> = None;
    while let Some((index, chunk, plain)) = frames.try_next().await? {
        if chunk == 0 {
            if let Some(done) = writing.take() {
                done.finish(&descriptor).await?;
            }
            let path = dir.join(&descriptor.files[index].path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            writing = Some(Writing {
                index,
                file: tokio::fs::File::create(&path).await?,
                size: 0,
                crc: 0,
            });
        }
        let current = writing
            .as_mut()
            .ok_or_else(|| TierError::Other("a chunk before its file".into()))?;
        current.file.write_all(&plain).await?;
        current.size += plain.len() as u64;
        current.crc = crc32c::crc32c_append(current.crc, &plain);
    }
    if let Some(done) = writing.take() {
        done.finish(&descriptor).await?;
    }
    drop(frames);
    Ok((descriptor, covered))
}

/// Whether an artifact is current at a manifest (Ruling 1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Currency {
    pub current: bool,
    /// When not current: the `created_at_ms` of the oldest manifest since
    /// the artifact's source that made it stale (0 when the chain is
    /// broken). `None` when current.
    pub stale_since_ms: Option<u64>,
}

impl Currency {
    const CURRENT: Currency = Currency {
        current: true,
        stale_since_ms: None,
    };
}

/// Ruling 1: whether an artifact built from `source_version` is current at
/// `live` (rule 5). Walks from `live` through `parent_manifest` while the
/// version is above `source_version`; the artifact is stale iff one of those
/// manifests is a link commit with a PK delta (a commit that inserted or
/// deleted rows). A missing manifest on the walk makes it stale since 0. An
/// artifact newer than `live` is `Corrupt`: a manifest only references
/// artifacts built from itself or an ancestor.
pub async fn currency(
    store: &Store,
    manifests: &ManifestCache,
    live: (&str, &CollectionManifest),
    source_version: u64,
) -> Result<Currency, TierError> {
    let (live_path, live) = live;
    if live.version < source_version {
        return Err(corrupt(format!(
            "an artifact of manifest version {source_version} is referenced at version {}",
            live.version
        )));
    }
    let broken = Currency {
        current: false,
        stale_since_ms: Some(0),
    };
    let mut stale_since = None;
    let mut path = live_path.to_string();
    let mut manifest = std::sync::Arc::new(live.clone());
    while manifest.version > source_version {
        if manifest.kind == CommitKind::LinkApply && manifest.pk_delta.is_some() {
            stale_since = Some(manifest.created_at_ms);
        }
        if manifest.version == source_version + 1 {
            break;
        }
        let Some(parent_path) = manifest.parent_manifest.clone() else {
            return Ok(broken);
        };
        let parent = match manifests.load(store, &parent_path).await {
            Ok(parent) => parent,
            Err(CollectionError::Store(StoreError::NotFound { .. })) => return Ok(broken),
            Err(err) => return Err(err.into()),
        };
        if parent.version >= manifest.version {
            return Err(corrupt(format!(
                "{path} (version {}) names parent {parent_path} at version {}",
                manifest.version, parent.version
            )));
        }
        path = parent_path;
        manifest = parent;
    }
    Ok(match stale_since {
        None => Currency::CURRENT,
        Some(ms) => Currency {
            current: false,
            stale_since_ms: Some(ms),
        },
    })
}

/// The effective source version the status reports: `live_version` if the
/// artifact is current there, else `source_version`.
pub fn effective_source_version(currency: Currency, live_version: u64, source_version: u64) -> u64 {
    match currency.current {
        true => live_version,
        false => source_version,
    }
}

/// [`currency`] results by (live manifest path, source version), at most
/// 10 000 (rule 5): manifests are immutable, so a result never changes.
#[derive(Clone)]
pub struct CurrencyCache {
    cache: moka::sync::Cache<(String, u64), Currency>,
}

impl std::fmt::Debug for CurrencyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurrencyCache")
            .field("entries", &self.cache.entry_count())
            .finish()
    }
}

impl Default for CurrencyCache {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrencyCache {
    pub fn new() -> Self {
        Self {
            cache: moka::sync::Cache::new(CURRENCY_CACHE_ENTRIES),
        }
    }

    /// [`currency`], cached.
    pub async fn currency(
        &self,
        store: &Store,
        manifests: &ManifestCache,
        live: (&str, &CollectionManifest),
        source_version: u64,
    ) -> Result<Currency, TierError> {
        let key = (live.0.to_string(), source_version);
        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit);
        }
        let result = currency(store, manifests, live, source_version).await?;
        self.cache.insert(key, result);
        Ok(result)
    }
}
