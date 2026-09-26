//! The appendable delta index of a loaded artifact (plan M1.3 Task 6 rule 2;
//! Ruling 2): the live rows inserted since the artifact's source manifest,
//! appended as each new manifest is viewed.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use operon_collection::CollectionSnapshot;
use operon_hnsw::{AppendableHnsw, Point};
use roaring::RoaringTreemap;

use crate::TierError;
use crate::build::{LocalCopy, payload, spec_payload_fields};
use crate::config::HotTierConfig;
use crate::view::LoadedArtifact;

/// Rows per `take_rows` call of an extension (rule 2).
const TAKE_BATCH: usize = 1_000;

/// One delta index: every row it examined, and the points it holds.
#[derive(Debug)]
pub struct DeltaIndex {
    // Declared before `_dir`, so the index is closed before its directory
    // is removed.
    index: Arc<dyn AppendableHnsw>,
    /// Every row examined, with or without a vector. `appended` changes only
    /// while this lock is held for writing.
    scanned: RwLock<RoaringTreemap>,
    appended: AtomicU64,
    since_optimize: AtomicU64,
    optimizing: AtomicBool,
    /// Serializes extensions.
    extending: tokio::sync::Mutex<()>,
    dir: PathBuf,
    _dir: LocalCopy,
}

/// What one [`DeltaIndex::extend`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Extension {
    /// Rows examined.
    pub scanned: u64,
    /// Points appended (examined rows with the vector).
    pub appended: u64,
    /// Whether `delta_max_rows` left new rows out.
    pub capped: bool,
}

fn join_error(err: tokio::task::JoinError) -> TierError {
    TierError::Other(format!("a delta index task: {err}"))
}

impl DeltaIndex {
    /// A new, empty delta index of `artifact` in `dir` (created here, and
    /// removed when the index is dropped), with the artifact's engine and spec.
    pub async fn create(artifact: &LoadedArtifact, dir: PathBuf) -> Result<Arc<Self>, TierError> {
        let guard = LocalCopy(dir.clone());
        let engine = artifact.engine().clone();
        let spec = artifact.descriptor.spec.clone();
        let work = dir.clone();
        let index = tokio::task::spawn_blocking(move || {
            match std::fs::remove_dir_all(&work) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(TierError::Io(err)),
            }
            std::fs::create_dir_all(&work)?;
            Ok(engine.appendable(&spec, &work)?)
        })
        .await
        .map_err(join_error)??;
        Ok(Arc::new(Self {
            index,
            scanned: RwLock::new(RoaringTreemap::new()),
            appended: AtomicU64::new(0),
            since_optimize: AtomicU64::new(0),
            optimizing: AtomicBool::new(false),
            extending: tokio::sync::Mutex::new(()),
            dir,
            _dir: guard,
        }))
    }

    /// Every row examined (with or without a vector).
    pub fn scanned(&self) -> RoaringTreemap {
        self.scanned
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Points appended so far.
    pub fn appended(&self) -> u64 {
        self.appended.load(Ordering::Acquire)
    }

    /// The scanned rows and the appended count, read together.
    pub(crate) fn state(&self) -> (RoaringTreemap, u64) {
        let scanned = self.scanned.read().unwrap_or_else(PoisonError::into_inner);
        (scanned.clone(), self.appended.load(Ordering::Acquire))
    }

    /// The local directory (removed when the index is dropped).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn index(&self) -> &Arc<dyn AppendableHnsw> {
        &self.index
    }

    /// Rule 2: appends the rows of `live` (the live rows at `snapshot`) that
    /// neither `artifact` nor this index has scanned and that were inserted
    /// after the artifact (`>= descriptor.next_row_id`), in ascending order,
    /// up to `delta_max_rows` scanned rows in all (row 6.3). Rows with the
    /// artifact's vector become points with the artifact's payload fields;
    /// every examined row joins `scanned`. Starts an `optimize` on a
    /// blocking thread once `delta_optimize_rows` points were appended since
    /// the last one.
    pub async fn extend(
        self: &Arc<Self>,
        snapshot: &CollectionSnapshot,
        live: &RoaringTreemap,
        artifact: &LoadedArtifact,
        config: &HotTierConfig,
    ) -> Result<Extension, TierError> {
        let _extending = self.extending.lock().await;
        let mut new = live - &artifact.scanned;
        let capped = {
            let scanned = self.scanned.read().unwrap_or_else(PoisonError::into_inner);
            new -= &*scanned;
            new.remove_range(..artifact.descriptor.next_row_id);
            let room = config.delta_max_rows.saturating_sub(scanned.len());
            if new.len() > room {
                new = new.iter().take(room as usize).collect();
                true
            } else {
                false
            }
        };
        let extension = self.append_rows(snapshot, new, artifact, config).await?;
        Ok(Extension {
            capped,
            ..extension
        })
    }

    async fn append_rows(
        self: &Arc<Self>,
        snapshot: &CollectionSnapshot,
        rows: RoaringTreemap,
        artifact: &LoadedArtifact,
        config: &HotTierConfig,
    ) -> Result<Extension, TierError> {
        let mut extension = Extension::default();
        if rows.is_empty() {
            return Ok(extension);
        }
        let schema = &snapshot.collection().schema;
        let vector = &artifact.descriptor.vector;
        let fields = spec_payload_fields(&artifact.descriptor.spec);
        let rows: Vec<u64> = rows.iter().collect();
        for chunk in rows.chunks(TAKE_BATCH) {
            let docs = snapshot.take_rows(chunk).await?;
            let mut examined = RoaringTreemap::new();
            let mut points = Vec::new();
            for (row_id, doc) in chunk.iter().zip(docs) {
                // A live row the Lance version lacks cannot happen (every
                // live split doc is a live Lance row); leaving it unscanned
                // keeps it outside `covered`, so the durable path answers it.
                let Some(doc) = doc else {
                    tracing::warn!(
                        row_id,
                        version = snapshot.manifest().version,
                        "a live row is missing from the Lance version"
                    );
                    continue;
                };
                examined.insert(*row_id);
                if let Some(values) = doc.vectors.get(vector) {
                    points.push(Point {
                        id: *row_id,
                        vector: values.clone(),
                        payload: payload(schema, &fields, &doc.source),
                    });
                }
            }
            let count = points.len() as u64;
            if !points.is_empty() {
                let index = self.index.clone();
                tokio::task::spawn_blocking(move || index.append(points))
                    .await
                    .map_err(join_error)??;
            }
            {
                let mut scanned = self.scanned.write().unwrap_or_else(PoisonError::into_inner);
                *scanned |= &examined;
                self.appended.fetch_add(count, Ordering::AcqRel);
            }
            self.since_optimize.fetch_add(count, Ordering::AcqRel);
            extension.scanned += examined.len();
            extension.appended += count;
        }
        self.maybe_optimize(config.delta_optimize_rows);
        Ok(extension)
    }

    /// Starts `optimize` on a blocking thread once `threshold` points were
    /// appended since the last one, unless one is running. Searches stay
    /// valid meanwhile (`AppendableHnsw::optimize`); the task holds the
    /// index, so its directory outlives it.
    fn maybe_optimize(self: &Arc<Self>, threshold: u64) {
        if self.since_optimize.load(Ordering::Acquire) < threshold.max(1)
            || self.optimizing.swap(true, Ordering::AcqRel)
        {
            return;
        }
        self.since_optimize.store(0, Ordering::Release);
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(err) = this.index.optimize() {
                tracing::warn!(dir = %this.dir.display(), %err, "optimizing a delta index failed");
            }
            this.optimizing.store(false, Ordering::Release);
        });
    }
}
