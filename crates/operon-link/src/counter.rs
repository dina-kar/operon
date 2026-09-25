//! `CounterTable`, the M0 link target (plan ruling 4).
//!
//! Storage (exact, M0.4 plan Task 3):
//! - data: `ns/<ns>/links/<link_id>/data/<ulid>.cnt`, the batch's deltas as a
//!   `BTreeMap<String, i64>`, written create-only;
//! - manifest: `ns/<ns>/links/<link_id>/manifests/<version:020>-<ulid>.man`,
//!   `{ version, parent, data, applied, skipped }`, written create-only;
//!   `data` lists every data file so far, so one manifest describes the table.
//!   The ULID makes every commit attempt's manifest path unique, so a
//!   manifest orphaned by a crash or a fenced task never blocks the next
//!   commit (M0.4 review M1); garbage collection removes it;
//! - commit: a CAS of the pointer `link/<link_id>` from `version` to the new
//!   manifest's path, fenced by the task lease and carrying the commit's
//!   [`Freshness`](operon_meta::Freshness): the metastore refuses it once the
//!   commit is older than `max_commit_delay`, so it can never reference an
//!   object garbage collection may have deleted (M0.4 review I1).
//!
//! Both objects are postcard bodies in Operon's envelope (magic, format
//! version, crc32c trailer; M0.3 global constraints).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use operon_common::NamespaceId;
use operon_meta::{
    ApplyError, Consistency, Fence, Freshness, Link, LinkId, MetaClient, MetaError, Pointer,
};
use operon_store::{Store, StoreError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::LinkError;
use crate::target::{ApplyBatch, CommitError, CommitStep, LinkTarget, TargetState};

/// The link target kind `CounterTable` serves.
pub const COUNTER_KIND: &str = "counter";

/// The longest a commit may take from its data PUT to its pointer CAS,
/// enforced by the metastore when the CAS is applied. Older objects are left
/// to garbage collection, so a commit never references an object that GC
/// may already have deleted: GC's grace must be longer than this.
pub const MAX_COMMIT_DELAY: Duration = Duration::from_secs(600);

const DATA_MAGIC: &[u8; 8] = b"OPNCNTD\0";
const MANIFEST_MAGIC: &[u8; 8] = b"OPNLMAN\0";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 10;
const TRAILER_LEN: usize = 4;

/// One version of the table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub version: u64,
    pub parent: u64,
    /// Every data file of the table, oldest first.
    pub data: Vec<String>,
    pub applied: BTreeMap<u32, u64>,
    /// Records that were not applied: values that are not decimal `i64`s,
    /// records without a UTF-8 key (dead letters), and records trimmed from
    /// the stream before the link reached them.
    pub skipped: u64,
}

fn encode<T: Serialize>(magic: &[u8; 8], value: &T) -> Result<Bytes, LinkError> {
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(magic);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    postcard::to_io(value, &mut out).map_err(|e| LinkError::Corrupt(e.to_string()))?;
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(Bytes::from(out))
}

fn decode<T: DeserializeOwned>(magic: &[u8; 8], path: &str, bytes: &[u8]) -> Result<T, LinkError> {
    let corrupt = |why: &str| LinkError::Corrupt(format!("{path}: {why}"));
    if bytes.len() < HEADER_LEN + TRAILER_LEN || &bytes[..8] != magic {
        return Err(corrupt("bad magic or too short"));
    }
    let (covered, trailer) = bytes.split_at(bytes.len() - TRAILER_LEN);
    let stored = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
    if crc32c::crc32c(covered) != stored {
        return Err(corrupt("checksum mismatch"));
    }
    let version = u16::from_le_bytes([covered[8], covered[9]]);
    if version != FORMAT_VERSION {
        return Err(corrupt(&format!("unsupported format version {version}")));
    }
    let (value, rest) =
        postcard::take_from_bytes(&covered[HEADER_LEN..]).map_err(|e| corrupt(&e.to_string()))?;
    if !rest.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(value)
}

/// Decodes a manifest object.
pub(crate) fn decode_manifest(path: &str, bytes: &[u8]) -> Result<Manifest, LinkError> {
    decode(MANIFEST_MAGIC, path, bytes)
}

/// `ns/<ns>/links/<link_id>/`.
pub(crate) fn link_prefix(namespace: NamespaceId, link: LinkId) -> String {
    format!("ns/{namespace}/links/{link}/")
}

fn data_path(namespace: NamespaceId, link: LinkId, ulid: Ulid) -> String {
    format!("{}data/{ulid}.cnt", link_prefix(namespace, link))
}

fn manifest_path(namespace: NamespaceId, link: LinkId, version: u64, ulid: Ulid) -> String {
    format!(
        "{}manifests/{version:020}-{ulid}.man",
        link_prefix(namespace, link)
    )
}

/// The version in a manifest path's name (`<version:020>-<ulid>.man`).
pub(crate) fn manifest_version(path: &str) -> Option<u64> {
    let name = path.rsplit('/').next()?.strip_suffix(".man")?;
    name.split('-').next()?.parse().ok()
}

/// The pointer holding the current manifest path: `link/<link_id>`.
pub(crate) fn pointer_key(link: LinkId) -> String {
    format!("link/{link}")
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A consistent view of the table at one version.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub version: u64,
    pub counters: BTreeMap<String, i64>,
    pub applied: BTreeMap<u32, u64>,
    pub skipped: u64,
}

/// A table of counters maintained by a link. Sums use wrapping `i64`
/// arithmetic, so a table's sums do not depend on how records were batched.
pub struct CounterTable {
    meta: MetaClient,
    store: Store,
    namespace: NamespaceId,
    link: LinkId,
    /// The manifest the last `load` or commit saw, to build the next commit on.
    last: Mutex<Option<Arc<Manifest>>>,
    max_commit_delay: Duration,
    #[cfg(feature = "test-util")]
    hook: Option<crate::target::CommitHook>,
}

impl std::fmt::Debug for CounterTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CounterTable")
            .field("namespace", &self.namespace)
            .field("link", &self.link)
            .finish_non_exhaustive()
    }
}

impl CounterTable {
    /// The table of link `name` in `namespace`.
    pub async fn open(
        meta: MetaClient,
        store: Store,
        namespace: NamespaceId,
        name: &str,
    ) -> Result<Self, LinkError> {
        let link = meta
            .read(Consistency::Local, |s| {
                s.link_by_name(namespace, name).cloned()
            })
            .await?
            .ok_or_else(|| LinkError::NotFound(format!("link {namespace}/{name}")))?;
        Ok(Self::for_link(meta, store, &link))
    }

    /// The table of `link`.
    pub fn for_link(meta: MetaClient, store: Store, link: &Link) -> Self {
        Self {
            meta,
            store,
            namespace: link.namespace,
            link: link.id,
            last: Mutex::default(),
            max_commit_delay: MAX_COMMIT_DELAY,
            #[cfg(feature = "test-util")]
            hook: None,
        }
    }

    /// Test hook: awaited at every commit step.
    #[cfg(feature = "test-util")]
    pub fn with_hook(mut self, hook: Option<crate::target::CommitHook>) -> Self {
        self.hook = hook;
        self
    }

    /// The longest a commit may take from its data PUT to its CAS (default
    /// [`MAX_COMMIT_DELAY`]). Garbage collection's grace must be longer.
    pub fn with_max_commit_delay(mut self, delay: Duration) -> Self {
        self.max_commit_delay = delay;
        self
    }

    pub fn link(&self) -> LinkId {
        self.link
    }

    async fn step(&self, step: CommitStep, fence: &Fence) {
        match step {
            CommitStep::AfterDataPut => {
                crate::failpoint!("link.after_data_put");
            }
            CommitStep::AfterManifestPut => {
                crate::failpoint!("link.after_manifest_put");
            }
            CommitStep::AfterCas => {
                crate::failpoint!("link.after_cas");
            }
        }
        #[cfg(feature = "test-util")]
        if let Some(hook) = &self.hook {
            hook(step, fence.clone()).await;
        }
        let _ = (step, fence);
    }

    fn remember(&self, manifest: Arc<Manifest>) {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(manifest);
    }

    fn remembered(&self, version: u64) -> Option<Arc<Manifest>> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .filter(|m| m.version == version)
    }

    async fn read_manifest(&self, path: &str) -> Result<Manifest, LinkError> {
        let (bytes, _) = self.store.get(path).await?;
        decode(MANIFEST_MAGIC, path, &bytes)
    }

    async fn pointer(&self) -> Result<Option<Pointer>, LinkError> {
        let (namespace, key) = (self.namespace, pointer_key(self.link));
        Ok(self
            .meta
            .read(Consistency::Linearizable, |s| {
                s.pointer(namespace, &key).cloned()
            })
            .await?)
    }

    /// The current manifest (an empty one at version 0 before any commit).
    async fn current(&self) -> Result<Arc<Manifest>, LinkError> {
        let Some(pointer) = self.pointer().await? else {
            return Ok(Arc::new(Manifest::default()));
        };
        if let Some(manifest) = self.remembered(pointer.version) {
            return Ok(manifest);
        }
        let manifest = self.read_manifest(&pointer.value).await?;
        if manifest.version != pointer.version {
            return Err(LinkError::Corrupt(format!(
                "{} holds version {}, its pointer says {}",
                pointer.value, manifest.version, pointer.version
            )));
        }
        let manifest = Arc::new(manifest);
        self.remember(manifest.clone());
        Ok(manifest)
    }

    /// The table as of its current version.
    pub async fn snapshot(&self) -> Result<CounterSnapshot, LinkError> {
        let manifest = self.current().await?;
        let mut counters: BTreeMap<String, i64> = BTreeMap::new();
        for path in &manifest.data {
            let (bytes, _) = self.store.get(path).await?;
            let deltas: BTreeMap<String, i64> = decode(DATA_MAGIC, path, &bytes)?;
            for (name, delta) in deltas {
                let sum = counters.entry(name).or_default();
                *sum = sum.wrapping_add(delta);
            }
        }
        Ok(CounterSnapshot {
            version: manifest.version,
            counters,
            applied: manifest.applied.clone(),
            skipped: manifest.skipped,
        })
    }

    /// The sum of one counter (0 if it never appeared).
    pub async fn get(&self, counter: &str) -> Result<i64, LinkError> {
        Ok(self
            .snapshot()
            .await?
            .counters
            .get(counter)
            .copied()
            .unwrap_or(0))
    }

    /// Partition → next offset to apply, as committed.
    pub async fn applied(&self) -> Result<BTreeMap<u32, u64>, LinkError> {
        Ok(self.current().await?.applied.clone())
    }
}

fn cas_error(err: MetaError) -> CommitError {
    match err {
        MetaError::Rejected(ApplyError::VersionMismatch { .. }) => CommitError::Conflict,
        MetaError::Rejected(ApplyError::Fenced { .. }) => CommitError::Fenced,
        // Too late: the new objects are left to garbage collection and the
        // next run commits the batch again with new ones.
        MetaError::Rejected(err @ ApplyError::StaleObject { .. }) => {
            CommitError::Other(LinkError::Blocked(err.to_string()))
        }
        other => CommitError::Other(LinkError::Meta(other)),
    }
}

/// The deltas of a batch and how many of its offsets were not applied.
fn deltas(
    parent: &Manifest,
    batch: &ApplyBatch,
) -> Result<(BTreeMap<String, i64>, u64), LinkError> {
    let mut deltas: BTreeMap<String, i64> = BTreeMap::new();
    let mut skipped: u64 = 0;
    let mut per_partition: BTreeMap<u32, u64> = BTreeMap::new();
    for (partition, record) in &batch.records {
        let from = parent.applied.get(partition).copied().unwrap_or(0);
        let to = batch.applied_after.get(partition).copied().unwrap_or(from);
        if !(from..to).contains(&record.offset) {
            return Err(LinkError::Corrupt(format!(
                "record {partition}/{} is outside the batch's range {from}..{to}",
                record.offset
            )));
        }
        *per_partition.entry(*partition).or_default() += 1;
        let key = record
            .record
            .key
            .as_ref()
            .and_then(|k| std::str::from_utf8(k).ok());
        let delta = record
            .record
            .value
            .as_ref()
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.trim().parse::<i64>().ok());
        match (key, delta) {
            (Some(key), Some(delta)) => {
                let sum = deltas.entry(key.to_string()).or_default();
                *sum = sum.wrapping_add(delta);
            }
            // Dead letter: counted, not applied.
            _ => skipped += 1,
        }
    }
    // Offsets the batch covers without a record were trimmed before the
    // link reached them.
    for (partition, to) in &batch.applied_after {
        let from = parent.applied.get(partition).copied().unwrap_or(0);
        let covered = to.checked_sub(from).ok_or_else(|| {
            LinkError::Corrupt(format!(
                "partition {partition} would go back from {from} to {to}"
            ))
        })?;
        let records = per_partition.get(partition).copied().unwrap_or(0);
        skipped += covered.saturating_sub(records);
    }
    Ok((deltas, skipped))
}

#[async_trait]
impl LinkTarget for CounterTable {
    async fn load(&self) -> Result<TargetState, LinkError> {
        let manifest = self.current().await?;
        Ok(TargetState {
            version: manifest.version,
            applied: manifest.applied.clone(),
        })
    }

    async fn commit(
        &self,
        expected_version: u64,
        batch: ApplyBatch,
        fence: &Fence,
    ) -> Result<u64, CommitError> {
        let parent = match self.remembered(expected_version) {
            Some(parent) => parent,
            None => {
                let current = self.current().await?;
                if current.version != expected_version {
                    return Err(CommitError::Conflict);
                }
                current
            }
        };
        let (deltas, skipped) = deltas(&parent, &batch)?;
        let started = self.meta.now_ms();
        let mut data = parent.data.clone();
        if !deltas.is_empty() {
            let ulid = Ulid::from_parts(started, Ulid::generate().random());
            let path = data_path(self.namespace, self.link, ulid);
            self.store
                .put_if_absent(&path, encode(DATA_MAGIC, &deltas)?)
                .await
                .map_err(LinkError::from)?;
            data.push(path);
        }
        self.step(CommitStep::AfterDataPut, fence).await;

        let mut applied = parent.applied.clone();
        applied.extend(batch.applied_after.iter().map(|(p, o)| (*p, *o)));
        let manifest = Manifest {
            version: expected_version + 1,
            parent: expected_version,
            data,
            applied,
            skipped: parent.skipped.saturating_add(skipped),
        };
        let ulid = Ulid::from_parts(started, Ulid::generate().random());
        let path = manifest_path(self.namespace, self.link, manifest.version, ulid);
        match self
            .store
            .put_if_absent(&path, encode(MANIFEST_MAGIC, &manifest)?)
            .await
        {
            Ok(_) => {}
            // Only a retry of our own PUT can have created this unique path.
            Err(err @ StoreError::AlreadyExists { .. }) => match self.read_manifest(&path).await {
                Ok(existing) if existing == manifest => {}
                Ok(_) => {
                    return Err(CommitError::Other(LinkError::Corrupt(format!(
                        "{path} exists with other content"
                    ))));
                }
                // The store reported a conflict but holds nothing there: the
                // PUT was not applied. Report the conflict, which is
                // retryable: the next attempt writes a new path.
                Err(LinkError::Store(StoreError::NotFound { .. })) => {
                    return Err(LinkError::from(err).into());
                }
                Err(other) => return Err(other.into()),
            },
            Err(err) => return Err(LinkError::from(err).into()),
        }
        self.step(CommitStep::AfterManifestPut, fence).await;

        // Too slow: garbage collection could delete the new data file around
        // the time the pointer starts referencing it. Leave it unreferenced.
        if self.meta.now_ms().saturating_sub(started) > millis(self.max_commit_delay) {
            return Err(CommitError::Other(LinkError::Blocked(format!(
                "the commit of {path} took longer than {:?}",
                self.max_commit_delay
            ))));
        }
        let expected = (expected_version > 0).then_some(expected_version);
        let key = pointer_key(self.link);
        let fresh = Freshness {
            created_at_ms: started,
            max_age_ms: millis(self.max_commit_delay),
        };
        let version = match self
            .meta
            .cas_pointer_fresh(
                self.namespace,
                &key,
                expected,
                &path,
                Some(fence.clone()),
                Some(fresh),
            )
            .await
        {
            Ok(version) => version,
            // A lost acknowledgement: the pointer names our manifest, which
            // only we could have created (create-only, identical content).
            Err(MetaError::Rejected(ApplyError::VersionMismatch {
                current: Some(current),
            })) if current.version == manifest.version && current.value == path => current.version,
            Err(err) => return Err(cas_error(err)),
        };
        self.step(CommitStep::AfterCas, fence).await;
        self.remember(Arc::new(manifest));
        Ok(version)
    }
}
