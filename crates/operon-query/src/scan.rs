//! Scan pinning (plan M1.2 Task 14, D53): a collection resolved into a
//! [`ScanPlan`], so an external reader can read the Lance version a manifest
//! names directly, or fall back to a pinned read of the same state.
//!
//! A plan names the manifest version, the Lance dataset URI with its
//! (detached) version id, the fragments with their data and deletion files,
//! the readable columns, the applied offsets against the requested ones
//! (`tail`), a `pin` that reads exactly the requested state through the
//! native API or Flight SQL, and the retention deadline. Lance's own deletion
//! files already exclude every deleted and superseded row (Ruling 22), so no
//! split delete bitmap is part of the plan. Planning reads the metastore, the
//! manifest chain and one Lance manifest, never the tail, so any node serves
//! it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use lance::Dataset;
use lance::dataset::fragment::FileFragment;
use lance::table::format::{DeletionFileType, Fragment};
use lance_table::io::deletion::relative_deletion_file_path;
use operon_collection::{
    CollectionError, CollectionManifest, ConsistencyToken, INGEST_OFFSET_COLUMN,
    INGEST_PARTITION_COLUMN, PK_COLUMN, SOURCE_COLUMN, SparseModifier, lance_prefix,
    retained_chain,
};
use operon_common::meta::{Collection, CollectionHead, Consistency};
use operon_common::{CollectionId, NamespaceId};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::error::ServiceError;
use crate::ir::ReadConsistency;
use crate::json::schema::distance_name;
use crate::read::collection_error;
use crate::service::CollectionService;

/// How a plan's `_pk` column encodes keys: [`PrimaryKey::canonical`]
/// (`0x01` + u64 big-endian, `0x02` + 16 UUID bytes, `0x03` + UTF-8).
///
/// [`PrimaryKey::canonical`]: operon_collection::PrimaryKey::canonical
pub const PK_ENCODING: &str = "operon_canonical_v1";

/// The Flight SQL request metadata key that pins a statement to a manifest
/// version, together with `operon-consistency-token` (rule 6).
pub const PIN_MANIFEST_METADATA: &str = "operon-pin-manifest";

/// The state a scan plan describes. JSON: `"current"`,
/// `{"manifest_version": v}` or `{"token": "<v1 token>"}`.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanAt {
    /// The live manifest; the plan reports the writes it does not hold.
    #[default]
    Current,
    /// A retained manifest version, as it is.
    ManifestVersion(u64),
    /// The first live manifest whose applied offsets cover the token,
    /// waited for up to `consistency_wait` (Ruling 24).
    Token(#[serde(with = "crate::json::token")] ConsistencyToken),
}

impl ScanAt {
    /// Parses a scan point (rule 1). `{"tag": …}` is refused until M2 (D52);
    /// any other form is an unknown scan point.
    pub fn from_json(value: &Value) -> Result<Self, ServiceError> {
        let unknown = || ServiceError::InvalidArgument(format!("unknown scan point {value}"));
        match value {
            Value::String(point) if point == "current" => Ok(Self::Current),
            Value::Object(point) if point.len() == 1 => {
                let Some((key, inner)) = point.iter().next() else {
                    return Err(unknown());
                };
                match key.as_str() {
                    "manifest_version" => inner
                        .as_u64()
                        .map(Self::ManifestVersion)
                        .ok_or_else(unknown),
                    "token" => {
                        let text = inner.as_str().ok_or_else(unknown)?;
                        text.parse().map(Self::Token).map_err(|err| {
                            ServiceError::InvalidArgument(format!(
                                "invalid consistency token in the scan point: {err}"
                            ))
                        })
                    }
                    "tag" => Err(ServiceError::InvalidArgument(
                        "tags arrive in M2 (D52)".to_string(),
                    )),
                    _ => Err(unknown()),
                }
            }
            _ => Err(unknown()),
        }
    }
}

impl<'de> Deserialize<'de> for ScanAt {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(d)?;
        Self::from_json(&value).map_err(|err| match err {
            ServiceError::InvalidArgument(message) => D::Error::custom(message),
            other => D::Error::custom(other),
        })
    }
}

/// The body of `POST …/collections/{c}/scan`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRequest {
    /// `current` when absent.
    #[serde(default)]
    pub at: ScanAt,
}

/// A collection resolved into what an external reader needs to read one
/// state of it (rule 2 is its JSON form).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanPlan {
    pub namespace: String,
    /// The collection's name (never the alias it was asked by).
    pub collection: String,
    pub collection_id: CollectionId,
    pub manifest_version: u64,
    pub schema_version: u64,
    /// `None` while the manifest has no Lance version (`lance_version` 0).
    pub lance: Option<LanceVersionRef>,
    /// In the Lance manifest's order.
    pub fragments: Vec<ScanFragment>,
    /// Σ fragment `live_rows`, which equals the manifest's `live_doc_count`.
    pub live_rows: u64,
    /// The Lance version's schema, in its order.
    pub columns: Vec<ScanColumn>,
    /// [`PK_ENCODING`].
    pub pk_encoding: String,
    /// Whether the requested state holds records the Lance version does not.
    pub tail: bool,
    /// Σ over partitions of `target − applied`.
    pub tail_records: u64,
    /// One per partition, ascending.
    pub offsets: Vec<ScanOffsets>,
    /// Exactly what the Lance version holds: the applied offsets.
    #[serde(with = "crate::json::token")]
    pub durable_token: ConsistencyToken,
    /// Reads the requested state, tail included (rule 6).
    pub pin: ScanPin,
    /// The metastore clock of the planning read: the clock `expires_at_ms`
    /// is on. It advances only with time-stamped metastore writes (log
    /// appends and lease operations among them), so it is 0 on a cluster
    /// that has had none yet (plan row 15.5); it is not a wall clock.
    pub planned_at_ms: u64,
    /// The earliest moment GC may stop retaining the manifest, on the
    /// metastore clock (rule 5); `None` for manifest version 0.
    pub expires_at_ms: Option<u64>,
}

impl ScanPlan {
    /// The pinned read of exactly the planned state (Task 4 rule 3).
    pub fn pinned(&self) -> ReadConsistency {
        ReadConsistency::Pinned {
            manifest_version: self.pin.manifest_version,
            token: self.pin.token.clone(),
        }
    }
}

/// The Lance version a manifest names.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LanceVersionRef {
    /// `<lance_base_url>/<lance_prefix>` without a trailing `/`; `None`
    /// when the service has no base URL.
    pub uri: Option<String>,
    /// The (detached) Lance version id: bit 63 is set for detached versions.
    pub version: u64,
    /// The Lance manifest, relative to the dataset root
    /// (`_versions/d<id>.manifest`).
    pub manifest_path: String,
    /// `"2.1"`.
    pub storage_format: String,
    pub stable_row_ids: bool,
}

/// One Lance fragment of the planned version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanFragment {
    pub id: u64,
    pub physical_rows: u64,
    pub deleted_rows: u64,
    pub live_rows: u64,
    pub files: Vec<ScanFile>,
    pub deletion_file: Option<ScanDeletionFile>,
    /// Lance's own `Fragment` JSON (what pylance's
    /// `FragmentMetadata.from_json` reads).
    pub lance: Value,
}

/// A data file of a fragment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanFile {
    /// Relative to the dataset root: `data/…`.
    pub path: String,
    pub size_bytes: Option<u64>,
}

/// A fragment's deletion file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanDeletionFile {
    /// Relative to the dataset root: `_deletions/…`.
    pub path: String,
    pub kind: DeletionKind,
    pub deleted_rows: u64,
}

/// How a deletion file stores its row offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionKind {
    Array,
    Bitmap,
}

/// A readable column of the Lance version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanColumn {
    pub name: String,
    /// Arrow's `DataType` display form.
    pub data_type: String,
    pub role: ColumnRole,
    /// The vector's name, for vector columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dim: Option<u32>,
    /// `"cosine"`, `"dot"`, `"euclid"` or `"manhattan"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<String>,
    /// `"none"` or `"idf"`, for sparse vector columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<String>,
}

/// What a column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnRole {
    /// The canonical key bytes ([`PK_ENCODING`]).
    Pk,
    /// `serde_json::to_vec` of the document: UTF-8 JSON. Typed fields are
    /// read from it (D36).
    Source,
    IngestPartition,
    IngestOffset,
    Vector,
    SparseVector,
}

/// One partition's applied offset against the requested one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanOffsets {
    pub partition: u32,
    pub applied: u64,
    pub target: u64,
}

/// The fields of [`ReadConsistency::Pinned`] that read the planned state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanPin {
    pub manifest_version: u64,
    #[serde(with = "crate::json::token")]
    pub token: ConsistencyToken,
}

/// A resolved scan point: the manifest to plan, the requested offsets and
/// the clock and deadline of the planning read.
struct Point {
    collection: Collection,
    manifest_path: Option<String>,
    manifest: Arc<CollectionManifest>,
    targets: BTreeMap<u32, u64>,
    planned_at_ms: u64,
    expires_at_ms: Option<u64>,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn offset_in(map: &BTreeMap<u32, u64>, partition: u32) -> u64 {
    map.get(&partition).copied().unwrap_or(0)
}

fn token_of(collection: &Collection, offsets: &BTreeMap<u32, u64>) -> ConsistencyToken {
    ConsistencyToken(
        (0..collection.partitions)
            .map(|p| (collection.stream, p, offset_in(offsets, p)))
            .collect(),
    )
}

fn lance_error(err: lance::Error) -> ServiceError {
    collection_error(CollectionError::Lance(err))
}

/// The path of a Lance manifest relative to the dataset root.
fn relative_manifest_path(path: &str) -> String {
    match path.rfind("_versions/") {
        Some(start) => path[start..].to_string(),
        None => path.to_string(),
    }
}

impl CollectionService {
    /// The collection record, its pointer and its stream bounds, from one
    /// linearizable read.
    async fn scan_head(
        &self,
        ns_id: NamespaceId,
        cid: CollectionId,
        name: &str,
    ) -> Result<CollectionHead, ServiceError> {
        self.ctx
            .meta
            .collection_head(Consistency::Linearizable, cid)
            .await?
            .filter(|head| head.collection.namespace == ns_id)
            .ok_or_else(|| ServiceError::NotFound {
                kind: "collection",
                name: name.to_string(),
            })
    }

    /// The manifest `head`'s pointer names, or the empty one before the
    /// first commit.
    async fn pointed(
        &self,
        head: &CollectionHead,
    ) -> Result<(Option<String>, Arc<CollectionManifest>), ServiceError> {
        let cid = head.collection.id;
        let Some(pointer) = &head.pointer else {
            return Ok((None, Arc::new(CollectionManifest::empty(cid))));
        };
        let manifest = self
            .ctx
            .manifests
            .load(&self.ctx.store, &pointer.value)
            .await?;
        if manifest.version != pointer.version || manifest.collection_id != cid {
            return Err(ServiceError::Internal(format!(
                "{} is version {} of collection {}, but the pointer of collection {cid} is at version {}",
                pointer.value, manifest.version, manifest.collection_id, pointer.version
            )));
        }
        Ok((Some(pointer.value.clone()), manifest))
    }

    fn retention_ms(&self) -> u64 {
        millis(self.ctx.config.time_travel_retention)
    }

    /// Rule 3, `Current`: the live manifest against the high watermarks.
    async fn scan_current(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
    ) -> Result<Point, ServiceError> {
        let head = self
            .scan_head(ns_id, collection.id, &collection.name)
            .await?;
        let (manifest_path, manifest) = self.pointed(&head).await?;
        let targets = (0..head.collection.partitions)
            .map(|p| {
                let hwm = head.high_watermarks.get(p as usize).copied().unwrap_or(0);
                (p, hwm)
            })
            .collect();
        let expires_at_ms =
            (manifest.version != 0).then(|| head.clock_ms.saturating_add(self.retention_ms()));
        Ok(Point {
            planned_at_ms: head.clock_ms,
            collection: head.collection,
            manifest_path,
            manifest,
            targets,
            expires_at_ms,
        })
    }

    /// Rule 3, `ManifestVersion(v)`: a retained manifest as it is, under the
    /// retention rule of `CollectionSnapshot::open_version`, at the clock of
    /// the same linearizable read.
    async fn scan_version(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        version: u64,
    ) -> Result<Point, ServiceError> {
        let gone = || ServiceError::NotFound {
            kind: "pin",
            name: format!("{}@{version}", collection.name),
        };
        let head = self
            .scan_head(ns_id, collection.id, &collection.name)
            .await?;
        let (live_path, live) = self.pointed(&head).await?;
        let (manifest_path, manifest, expires_at_ms) = match live_path {
            None if version == 0 => (None, live, None),
            None => return Err(gone()),
            Some(_) if version > live.version => return Err(gone()),
            Some(path) => {
                let chain = retained_chain(
                    &self.ctx.store,
                    &self.ctx.manifests,
                    (path, live),
                    self.ctx.config.keep_manifests,
                    self.ctx.config.time_travel_retention,
                    head.clock_ms,
                )
                .await?;
                let index = chain
                    .iter()
                    .position(|(_, manifest)| manifest.version == version)
                    .ok_or_else(gone)?;
                // Rule 5: retention runs from supersession. The live manifest
                // is superseded no earlier than now; an older one by its
                // child, the entry before it in the newest-first chain.
                let superseded_at = match index {
                    0 => head.clock_ms,
                    _ => chain[index - 1].1.created_at_ms,
                };
                let (path, manifest) = chain[index].clone();
                let expires = superseded_at.saturating_add(self.retention_ms());
                (Some(path), manifest, Some(expires))
            }
        };
        let targets = (0..head.collection.partitions)
            .map(|p| (p, offset_in(&manifest.applied, p)))
            .collect();
        Ok(Point {
            planned_at_ms: head.clock_ms,
            collection: head.collection,
            manifest_path,
            manifest,
            targets,
            expires_at_ms,
        })
    }

    /// Rule 3, `Token(t)` (Ruling 24): waits up to `consistency_wait` for a
    /// live manifest whose applied offsets cover the token's offsets on the
    /// collection's stream.
    async fn scan_token(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        token: &ConsistencyToken,
    ) -> Result<Point, ServiceError> {
        let deadline = tokio::time::Instant::now() + self.reads.config().consistency_wait;
        loop {
            // Armed before the read, so a commit after it wakes the wait.
            let mut changes = self.ctx.meta.watch_changes();
            let head = self
                .scan_head(ns_id, collection.id, &collection.name)
                .await?;
            let (manifest_path, manifest) = self.pointed(&head).await?;
            let stream = head.collection.stream;
            let wanted = |p: u32| token.offset(stream, p).unwrap_or(0);
            let covered = (0..head.collection.partitions)
                .all(|p| offset_in(&manifest.applied, p) >= wanted(p));
            if covered {
                // The manifest may hold more than the token asks for; the
                // requested point is then its own applied offsets, so a pin
                // never names offsets below them (Task 4 rule 3 refuses it).
                let targets = (0..head.collection.partitions)
                    .map(|p| (p, wanted(p).max(offset_in(&manifest.applied, p))))
                    .collect();
                let expires_at_ms = (manifest.version != 0)
                    .then(|| head.clock_ms.saturating_add(self.retention_ms()));
                return Ok(Point {
                    planned_at_ms: head.clock_ms,
                    collection: head.collection,
                    manifest_path,
                    manifest,
                    targets,
                    expires_at_ms,
                });
            }
            tokio::select! {
                changed = changes.changed() => {
                    if changed.is_err() {
                        return Err(ServiceError::Unavailable(
                            "the metastore stopped".to_string(),
                        ));
                    }
                }
                () = tokio::time::sleep_until(deadline) => return Err(ServiceError::Timeout),
            }
        }
    }

    /// The Lance part of a plan (rule 4): the version, its fragments and
    /// its columns, through the snapshot cache (one Lance open per manifest
    /// per node).
    async fn scan_lance(
        &self,
        ns_id: NamespaceId,
        point: &Point,
    ) -> Result<(Option<LanceVersionRef>, Vec<ScanFragment>, Vec<ScanColumn>), ServiceError> {
        if point.manifest.lance_version == 0 {
            return Ok((None, Vec::new(), Vec::new()));
        }
        let snapshot = self
            .reads
            .snapshot(
                ns_id,
                &point.collection,
                point.manifest_path.clone(),
                point.manifest.clone(),
            )
            .await?;
        let dataset = snapshot.dataset().cloned().ok_or_else(|| {
            ServiceError::Internal(format!(
                "manifest {} of {} names lance version {} but opened none",
                point.manifest.version, point.collection.name, point.manifest.lance_version
            ))
        })?;
        let uri = self.config.lance_base_url.as_ref().map(|base| {
            format!(
                "{}/{}",
                base.trim_end_matches('/'),
                lance_prefix(ns_id, point.collection.id).trim_end_matches('/')
            )
        });
        let lance = LanceVersionRef {
            uri,
            version: point.manifest.lance_version,
            manifest_path: relative_manifest_path(dataset.manifest_location().path.as_ref()),
            storage_format: dataset.manifest.data_storage_format.version.to_string(),
            stable_row_ids: dataset.manifest.uses_stable_row_ids(),
        };
        let mut fragments = Vec::with_capacity(dataset.fragments().len());
        for fragment in dataset.fragments().iter() {
            fragments.push(scan_fragment(&dataset, fragment).await?);
        }
        let columns = scan_columns(&point.collection, &ArrowSchema::from(dataset.schema()))?;
        Ok((Some(lance), fragments, columns))
    }

    /// Resolves `name_or_alias` in `ns` into the scan plan of `at` (rules
    /// 3–6).
    pub(crate) async fn plan_scan(
        &self,
        ns: &str,
        name_or_alias: &str,
        at: ScanAt,
    ) -> Result<ScanPlan, ServiceError> {
        let (ns_id, collection) = self.resolve(ns, name_or_alias).await?;
        let point = match &at {
            ScanAt::Current => self.scan_current(ns_id, &collection).await?,
            ScanAt::ManifestVersion(version) => {
                self.scan_version(ns_id, &collection, *version).await?
            }
            ScanAt::Token(token) => self.scan_token(ns_id, &collection, token).await?,
        };
        let (lance, fragments, columns) = self.scan_lance(ns_id, &point).await?;
        let live_rows: u64 = fragments.iter().map(|fragment| fragment.live_rows).sum();
        if live_rows != point.manifest.live_doc_count {
            return Err(ServiceError::Internal(format!(
                "scan plan of {}@{}: lance holds {live_rows} live rows, the manifest {}",
                point.collection.name, point.manifest.version, point.manifest.live_doc_count
            )));
        }
        let applied: BTreeMap<u32, u64> = (0..point.collection.partitions)
            .map(|p| (p, offset_in(&point.manifest.applied, p)))
            .collect();
        let offsets: Vec<ScanOffsets> = (0..point.collection.partitions)
            .map(|p| ScanOffsets {
                partition: p,
                applied: offset_in(&applied, p),
                target: offset_in(&point.targets, p),
            })
            .collect();
        let tail_records: u64 = offsets
            .iter()
            .map(|o| o.target.saturating_sub(o.applied))
            .sum();
        Ok(ScanPlan {
            namespace: ns.to_string(),
            collection: point.collection.name.clone(),
            collection_id: point.collection.id,
            manifest_version: point.manifest.version,
            // Before the first commit the empty manifest carries schema
            // version 0; the plan names the schema current now.
            schema_version: match point.manifest.version {
                0 => point.collection.schema.version,
                _ => point.manifest.schema_version,
            },
            lance,
            fragments,
            live_rows,
            columns,
            pk_encoding: PK_ENCODING.to_string(),
            tail: tail_records > 0,
            tail_records,
            offsets,
            durable_token: token_of(&point.collection, &applied),
            pin: ScanPin {
                manifest_version: point.manifest.version,
                token: token_of(&point.collection, &point.targets),
            },
            planned_at_ms: point.planned_at_ms,
            expires_at_ms: point.expires_at_ms,
        })
    }
}

/// One fragment (rule 4): Lance's row counts (read from the data or
/// deletion file when its metadata lacks them), files and deletion file.
async fn scan_fragment(
    dataset: &Arc<Dataset>,
    fragment: &Fragment,
) -> Result<ScanFragment, ServiceError> {
    let file_fragment = FileFragment::new(dataset.clone(), fragment.clone());
    let physical_rows = file_fragment.physical_rows().await.map_err(lance_error)? as u64;
    let deleted_rows = file_fragment.count_deletions().await.map_err(lance_error)? as u64;
    let live_rows = physical_rows.checked_sub(deleted_rows).ok_or_else(|| {
        ServiceError::Internal(format!(
            "lance fragment {} has {deleted_rows} deleted of {physical_rows} rows",
            fragment.id
        ))
    })?;
    let files = fragment
        .files
        .iter()
        .map(|file| ScanFile {
            path: format!("data/{}", file.path),
            size_bytes: file.file_size_bytes.get().map(u64::from),
        })
        .collect();
    let deletion_file = fragment
        .deletion_file
        .as_ref()
        .map(|deletion| ScanDeletionFile {
            path: relative_deletion_file_path(fragment.id, deletion),
            kind: match deletion.file_type {
                DeletionFileType::Array => DeletionKind::Array,
                DeletionFileType::Bitmap => DeletionKind::Bitmap,
            },
            deleted_rows,
        });
    let lance = serde_json::to_value(fragment).map_err(|err| {
        ServiceError::Internal(format!("lance fragment {} as JSON: {err}", fragment.id))
    })?;
    Ok(ScanFragment {
        id: fragment.id,
        physical_rows,
        deleted_rows,
        live_rows,
        files,
        deletion_file,
        lance,
    })
}

/// The index `i` of a column named `<prefix><i>`.
fn column_index(name: &str, prefix: &str) -> Option<usize> {
    name.strip_prefix(prefix)?.parse().ok()
}

/// The Lance version's columns in its order, with their roles (rule 4):
/// vector columns carry the name, dim and distance of the schema's vector
/// at their index, sparse columns the name and modifier.
fn scan_columns(
    collection: &Collection,
    schema: &ArrowSchema,
) -> Result<Vec<ScanColumn>, ServiceError> {
    let unknown = |name: &str| {
        ServiceError::Internal(format!(
            "the lance version of {} has column {name}, which schema version {} does not describe",
            collection.name, collection.schema.version
        ))
    };
    schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name().as_str();
            let mut column = ScanColumn {
                name: name.to_string(),
                data_type: field.data_type().to_string(),
                role: ColumnRole::Pk,
                vector: None,
                dim: None,
                distance: None,
                modifier: None,
            };
            column.role = match name {
                PK_COLUMN => ColumnRole::Pk,
                SOURCE_COLUMN => ColumnRole::Source,
                INGEST_PARTITION_COLUMN => ColumnRole::IngestPartition,
                INGEST_OFFSET_COLUMN => ColumnRole::IngestOffset,
                _ => {
                    if let Some(index) = column_index(name, "_vector_") {
                        let spec = collection
                            .schema
                            .vectors
                            .get(index)
                            .ok_or_else(|| unknown(name))?;
                        column.vector = Some(spec.name.clone());
                        column.dim = Some(spec.dim);
                        column.distance = Some(distance_name(spec.distance).to_string());
                        ColumnRole::Vector
                    } else if let Some(index) = column_index(name, "_sparse_") {
                        let spec = collection
                            .schema
                            .sparse_vectors
                            .get(index)
                            .ok_or_else(|| unknown(name))?;
                        column.vector = Some(spec.name.clone());
                        column.modifier = Some(
                            match spec.modifier {
                                SparseModifier::None => "none",
                                SparseModifier::Idf => "idf",
                            }
                            .to_string(),
                        );
                        ColumnRole::SparseVector
                    } else {
                        return Err(unknown(name));
                    }
                }
            };
            Ok(column)
        })
        .collect()
}
