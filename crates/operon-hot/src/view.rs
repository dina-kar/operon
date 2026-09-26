//! Loaded artifacts and per-manifest views (plan M1.3 Task 6 rule 3;
//! Ruling 2): the artifact's points minus the rows deleted since its source
//! manifest, plus its delta index, as the [`HotAnn`] M1.2's query engine
//! searches.

use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use operon_hnsw::{HnswEngine, HnswError, HnswIndex, IdFilter, SearchParams, engine_by_name};
use operon_query::hot::{HotAnn, HotError};
use operon_store::Store;
use roaring::RoaringTreemap;

use crate::TierError;
use crate::artifact::{ArtifactDescriptor, download};
use crate::build::LocalCopy;
use crate::delta::DeltaIndex;

/// An artifact downloaded to local disk and opened read-only.
#[derive(Debug)]
pub struct LoadedArtifact {
    pub descriptor: ArtifactDescriptor,
    /// The rows the build scanned (the covered set).
    pub scanned: RoaringTreemap,
    pub index: Arc<dyn HnswIndex>,
    /// The uncompressed bytes of its files.
    pub bytes: u64,
    /// The engine that opened it; its delta indexes use it too.
    engine: Arc<dyn HnswEngine>,
    dir: PathBuf,
    /// Removes the local directory on drop (after `index`, declared above).
    _dir: LocalCopy,
}

fn join_error(err: tokio::task::JoinError) -> TierError {
    TierError::Other(format!("a hot artifact task: {err}"))
}

impl LoadedArtifact {
    /// Downloads the artifact at `prefix` into `dir` (created here; removed
    /// on failure and when the artifact is dropped) and opens it: with
    /// `engine` when the descriptor names it, else with the engine of that
    /// name (row 6.4).
    pub async fn load(
        store: &Store,
        prefix: &str,
        dir: PathBuf,
        parallelism: usize,
        engine: &Arc<dyn HnswEngine>,
    ) -> Result<Self, TierError> {
        let guard = LocalCopy(dir.clone());
        let fresh = dir.clone();
        tokio::task::spawn_blocking(move || {
            match std::fs::remove_dir_all(&fresh) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            std::fs::create_dir_all(&fresh)
        })
        .await
        .map_err(join_error)??;
        let (descriptor, scanned) = download(store, prefix, &dir, parallelism).await?;
        let engine = match descriptor.engine == engine.name() {
            true => engine.clone(),
            false => engine_by_name(&descriptor.engine).ok_or_else(|| {
                TierError::Corrupt(format!("unknown engine {:?}", descriptor.engine))
            })?,
        };
        let (open_engine, spec, open_dir) = (engine.clone(), descriptor.spec.clone(), dir.clone());
        let index = tokio::task::spawn_blocking(move || open_engine.open(&spec, &open_dir))
            .await
            .map_err(join_error)??;
        if index.len() != descriptor.points {
            return Err(TierError::Corrupt(format!(
                "{prefix}: the index holds {} points, the descriptor says {}",
                index.len(),
                descriptor.points
            )));
        }
        let bytes = descriptor.files.iter().map(|f| f.size).sum();
        Ok(Self {
            descriptor,
            scanned,
            index,
            bytes,
            engine,
            dir,
            _dir: guard,
        })
    }

    /// The engine that opened the artifact.
    pub fn engine(&self) -> &Arc<dyn HnswEngine> {
        &self.engine
    }

    /// The local directory (removed when the artifact is dropped).
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The view of one column at one manifest version: an artifact, its delta
/// index, and the row sets that make them answer for that version only.
/// Cheap to clone.
#[derive(Clone, Debug)]
pub struct ColumnView {
    inner: Arc<ViewInner>,
}

#[derive(Debug)]
struct ViewInner {
    version: u64,
    /// The effective source version (Ruling 1).
    source_version: u64,
    artifact: Arc<LoadedArtifact>,
    delta: Arc<DeltaIndex>,
    /// `artifact.scanned − live(v)`: rows deleted since the artifact.
    excluded: RoaringTreemap,
    /// `(live(v) ∩ artifact.scanned) ∪ (live(v) ∩ delta.scanned)`.
    covered: RoaringTreemap,
    /// `delta.scanned − live(v)`: delta rows deleted by `version`.
    delta_excluded: RoaringTreemap,
    /// `delta.appended()` when the view was made; points appended later are
    /// for later manifests.
    delta_len_at_creation: u64,
}

impl ColumnView {
    /// Rule 3; pure construction from its parts (used by tests).
    pub fn new(
        version: u64,
        source_version: u64,
        artifact: Arc<LoadedArtifact>,
        delta: Arc<DeltaIndex>,
        live: &RoaringTreemap,
    ) -> Self {
        let (delta_scanned, delta_len_at_creation) = delta.state();
        let excluded = &artifact.scanned - live;
        let covered = (live & &artifact.scanned) | (live & &delta_scanned);
        let delta_excluded = delta_scanned - live;
        Self {
            inner: Arc::new(ViewInner {
                version,
                source_version,
                artifact,
                delta,
                excluded,
                covered,
                delta_excluded,
                delta_len_at_creation,
            }),
        }
    }

    pub fn version(&self) -> u64 {
        self.inner.version
    }

    pub fn excluded(&self) -> &RoaringTreemap {
        &self.inner.excluded
    }

    /// The artifact the view searches.
    pub fn artifact(&self) -> &Arc<LoadedArtifact> {
        &self.inner.artifact
    }

    /// The delta index the view searches.
    pub fn delta(&self) -> &Arc<DeltaIndex> {
        &self.inner.delta
    }

    /// The serialized size of the view's own row sets (Task 7's RAM
    /// accounting).
    pub fn ram_bytes(&self) -> u64 {
        let inner = &self.inner;
        (inner.excluded.serialized_size()
            + inner.covered.serialized_size()
            + inner.delta_excluded.serialized_size()) as u64
    }
}

/// Score descending, then id ascending (`operon-hnsw` rule 3).
fn hit_order(a: &(u64, f32), b: &(u64, f32)) -> Ordering {
    b.1.total_cmp(&a.1).then(a.0.cmp(&b.0))
}

impl ViewInner {
    /// Rule 3's search, on a blocking thread.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        allow: Option<&RoaringTreemap>,
        ef: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let params = SearchParams { ef, exact: false };
        let artifact = &self.artifact.index;
        let delta = self.delta.index();
        let mut hits = match allow {
            Some(allow) => {
                let only = allow & &self.covered;
                // `only ⊆ covered`, and the artifact's and the delta's
                // scanned rows are disjoint, so `only − artifact.scanned` is
                // `only ∩ delta.scanned` without reading the delta's lock.
                let in_artifact = &only & &self.artifact.scanned;
                let in_delta = &only - &self.artifact.scanned;
                let mut hits = match in_artifact.is_empty() {
                    true => Vec::new(),
                    false => artifact.search(query, k, IdFilter::Only(&in_artifact), params)?,
                };
                if !in_delta.is_empty() {
                    hits.extend(delta.search(query, k, IdFilter::Only(&in_delta), params)?);
                }
                hits
            }
            None => {
                let mut hits =
                    artifact.search(query, k, IdFilter::Except(&self.excluded), params)?;
                let appended = self.delta.appended();
                if appended > 0 {
                    // Points appended for later manifests are not covered
                    // and are dropped below; ask for enough extra.
                    let later = appended.saturating_sub(self.delta_len_at_creation);
                    let wanted = k.saturating_add(usize::try_from(later).unwrap_or(usize::MAX));
                    hits.extend(delta.search(
                        query,
                        wanted,
                        IdFilter::Except(&self.delta_excluded),
                        params,
                    )?);
                }
                hits
            }
        };
        hits.retain(|(id, _)| self.covered.contains(*id));
        hits.sort_unstable_by(hit_order);
        hits.truncate(k);
        Ok(hits)
    }
}

#[async_trait]
impl HotAnn for ColumnView {
    fn source_version(&self) -> u64 {
        self.inner.source_version
    }

    fn covered(&self) -> &RoaringTreemap {
        &self.inner.covered
    }

    async fn search(
        &self,
        query: &[f32],
        k: usize,
        allow: Option<&RoaringTreemap>,
        ef: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HotError> {
        let inner = self.inner.clone();
        let query = query.to_vec();
        let allow = allow.cloned();
        tokio::task::spawn_blocking(move || inner.search(&query, k, allow.as_ref(), ef))
            .await
            .map_err(|err| HotError::Failed(format!("a hot search task: {err}")))?
            .map_err(|err| HotError::Failed(err.to_string()))
    }
}
