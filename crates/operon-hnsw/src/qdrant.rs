//! The qdrant-edge engine (rules 5–7): the only code in the workspace that
//! names `qdrant-edge`.
//!
//! A build writes an `EdgeShard` in `work_dir/shard/`, optimizes it until
//! every segment above 1 KB has an HNSW graph, and moves its segments to the
//! output directory next to a `segments_manifest.json` that lists them, the
//! layout `ReadOnlyEdgeShard::open_mmap` reads. An appendable index is a
//! live `EdgeShard`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use qdrant_edge::external::serde_json::{Map, Value};
use qdrant_edge::external::uuid::Uuid;
use qdrant_edge::{
    BinaryQuantizationConfig, CompressionRatio, Condition, EdgeConfig, EdgeConfigBuilder,
    EdgeOptimizersConfig, EdgeShard, EdgeShardRead, EdgeVectorParamsBuilder, FieldIndexOperations,
    Filter, HasIdCondition, HnswIndexConfig, NamedQuery, OperationError, Payload,
    PayloadFieldSchema, PayloadSchemaType, PointId, PointInsertOperations, PointOperations,
    PointStructPersisted, ProductQuantizationConfig, QueryEnum, ReadOnlyEdgeShard,
    ScalarQuantizationConfig, ScalarType, SearchRequestBuilder, SegmentManifestState,
    SegmentsManifest, UpdateOperation, VectorPersisted, VectorStructPersisted, WalOptions,
};
use roaring::RoaringTreemap;

use crate::HnswError;
use crate::types::{
    AppendableHnsw, BuildSpec, BuiltFiles, Distance, HnswBuilder, HnswEngine, HnswIndex, IdFilter,
    PayloadKind, PayloadValue, Point, Quantization, SearchParams, check_points, check_query,
    check_spec, sort_hits,
};

pub const QDRANT_ENGINE: &str = "qdrant-edge-0.8";
pub const VECTOR_NAME: &str = "v";
/// The build shard's directory under `work_dir`.
const SHARD_DIR: &str = "shard";
/// qdrant-edge's segment directory, under a shard and under a built index.
const SEGMENTS_DIR: &str = "segments";
/// The file `ReadOnlyEdgeShard::open_mmap` discovers segments from.
const SEGMENTS_MANIFEST: &str = "segments_manifest.json";

/// The qdrant-edge engine.
#[derive(Clone, Copy, Debug, Default)]
pub struct QdrantEngine;

fn engine_err(err: OperationError) -> HnswError {
    HnswError::Engine(err.to_string())
}

fn qdrant_distance(distance: Distance) -> qdrant_edge::Distance {
    match distance {
        Distance::Cosine => qdrant_edge::Distance::Cosine,
        Distance::Dot => qdrant_edge::Distance::Dot,
        Distance::Euclid => qdrant_edge::Distance::Euclid,
        Distance::Manhattan => qdrant_edge::Distance::Manhattan,
    }
}

/// The shard configuration of rule 5 (pure; used by tests).
#[allow(deprecated)] // `always_ram` is deprecated in qdrant 1.19; `memory` replaces it.
pub fn edge_config(spec: &BuildSpec) -> Result<EdgeConfig, HnswError> {
    check_spec(spec)?;
    let hnsw = &spec.hnsw;
    let h = HnswIndexConfig {
        m: hnsw.m as usize,
        ef_construct: hnsw.ef_construct as usize,
        full_scan_threshold: hnsw.full_scan_threshold_kb as usize,
        max_indexing_threads: spec.indexing_threads,
        payload_m: hnsw.payload_m.map(|p| p as usize),
        ..HnswIndexConfig::default()
    };
    let mut vector =
        EdgeVectorParamsBuilder::new(spec.dim as usize, qdrant_distance(spec.distance))
            .hnsw_config(h);
    if let Some(q) = spec.quantization {
        let q: qdrant_edge::QuantizationConfig = match q {
            Quantization::Scalar {
                quantile_ppm,
                always_ram,
            } => ScalarQuantizationConfig {
                r#type: ScalarType::Int8,
                quantile: quantile_ppm.map(|p| p as f32 / 1_000_000.0),
                always_ram: Some(always_ram),
                memory: None,
            }
            .into(),
            Quantization::Product {
                compression_ratio,
                always_ram,
            } => ProductQuantizationConfig {
                compression: match compression_ratio {
                    4 => CompressionRatio::X4,
                    8 => CompressionRatio::X8,
                    16 => CompressionRatio::X16,
                    32 => CompressionRatio::X32,
                    64 => CompressionRatio::X64,
                    n => {
                        return Err(HnswError::Invalid(format!(
                            "product quantization ratio {n} is not 4, 8, 16, 32 or 64"
                        )));
                    }
                },
                always_ram: Some(always_ram),
                memory: None,
            }
            .into(),
            Quantization::Binary { always_ram } => BinaryQuantizationConfig {
                always_ram: Some(always_ram),
                memory: None,
                encoding: None,
                query_encoding: None,
            }
            .into(),
        };
        vector = vector.quantization_config(q);
    }
    Ok(EdgeConfigBuilder::new()
        .vector(VECTOR_NAME, vector.build())
        .optimizers(EdgeOptimizersConfig {
            indexing_threshold: Some(1),
            ..EdgeOptimizersConfig::default()
        })
        .wal_options(WalOptions {
            segment_capacity: 1 << 20,
            ..WalOptions::default()
        })
        .build())
}

fn payload_schema(kind: PayloadKind) -> PayloadSchemaType {
    match kind {
        PayloadKind::Keyword => PayloadSchemaType::Keyword,
        PayloadKind::Integer => PayloadSchemaType::Integer,
        PayloadKind::Bool => PayloadSchemaType::Bool,
        PayloadKind::Uuid => PayloadSchemaType::Uuid,
    }
}

fn payload_json(value: PayloadValue) -> Value {
    match value {
        PayloadValue::Keyword(s) => Value::String(s),
        PayloadValue::Integer(i) => Value::from(i),
        PayloadValue::Bool(b) => Value::Bool(b),
        PayloadValue::Uuid(bytes) => {
            Value::String(Uuid::from_bytes(bytes).hyphenated().to_string())
        }
    }
}

/// Rule 6.2: one `PointStructPersisted` per point, the vector under `"v"`.
fn upsert(points: Vec<Point>) -> UpdateOperation {
    let points = points
        .into_iter()
        .map(|point| {
            let mut map = Map::new();
            for (key, mut values) in point.payload {
                let value = match values.len() {
                    0 => continue,
                    1 => payload_json(values.remove(0)),
                    _ => Value::Array(values.into_iter().map(payload_json).collect()),
                };
                map.insert(key, value);
            }
            PointStructPersisted {
                id: PointId::NumId(point.id),
                vector: VectorStructPersisted::Named(
                    [(VECTOR_NAME.to_owned(), VectorPersisted::Dense(point.vector))].into(),
                ),
                payload: (!map.is_empty()).then(|| Payload(map)),
            }
        })
        .collect();
    UpdateOperation::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperations::PointsList(points),
    ))
}

fn create_shard(spec: &BuildSpec, dir: &Path) -> Result<EdgeShard, HnswError> {
    let config = edge_config(spec)?;
    std::fs::create_dir_all(dir)?;
    let shard = EdgeShard::new(dir, config).map_err(engine_err)?;
    for field in &spec.payload_fields {
        let field_name = field
            .key
            .parse()
            .map_err(|_| HnswError::Invalid(format!("payload key {:?}", field.key)))?;
        shard
            .update(UpdateOperation::FieldIndexOperation(
                FieldIndexOperations::CreateIndex(qdrant_edge::CreateIndex {
                    field_name,
                    field_schema: Some(PayloadFieldSchema::FieldType(payload_schema(field.kind))),
                }),
            ))
            .map_err(engine_err)?;
    }
    Ok(shard)
}

/// Optimizes until qdrant-edge finds no more work (rule 6.3).
fn optimize_fully(shard: &EdgeShard) -> Result<(), HnswError> {
    while shard.optimize().map_err(engine_err)? {}
    Ok(())
}

impl HnswEngine for QdrantEngine {
    fn name(&self) -> &'static str {
        QDRANT_ENGINE
    }

    fn builder(
        &self,
        spec: &BuildSpec,
        work_dir: &Path,
    ) -> Result<Box<dyn HnswBuilder>, HnswError> {
        let shard_dir = work_dir.join(SHARD_DIR);
        let shard = create_shard(spec, &shard_dir)?;
        Ok(Box::new(QdrantBuilder {
            dim: spec.dim,
            shard_dir,
            shard,
            ids: RoaringTreemap::new(),
        }))
    }

    fn open(&self, spec: &BuildSpec, dir: &Path) -> Result<Arc<dyn HnswIndex>, HnswError> {
        check_spec(spec)?;
        let shard = ReadOnlyEdgeShard::open_mmap(dir)
            .map_err(|e| HnswError::Corrupt(format!("{}: {e}", dir.display())))?;
        let points = shard.info().map_err(engine_err)?.points_count as u64;
        Ok(Arc::new(QdrantIndex {
            dim: spec.dim,
            distance: spec.distance,
            dir: dir.to_owned(),
            points,
            shard: Box::new(shard),
        }))
    }

    fn appendable(
        &self,
        spec: &BuildSpec,
        work_dir: &Path,
    ) -> Result<Arc<dyn AppendableHnsw>, HnswError> {
        let shard = create_shard(spec, work_dir)?;
        Ok(Arc::new(AppendableQdrant {
            dim: spec.dim,
            distance: spec.distance,
            dir: work_dir.to_owned(),
            shard,
        }))
    }
}

struct QdrantBuilder {
    dim: u32,
    shard_dir: PathBuf,
    shard: EdgeShard,
    /// Distinct ids added, for the point count check.
    ids: RoaringTreemap,
}

impl std::fmt::Debug for QdrantBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantBuilder")
            .field("shard_dir", &self.shard_dir)
            .field("points", &self.ids.len())
            .finish_non_exhaustive()
    }
}

impl HnswBuilder for QdrantBuilder {
    fn add(&mut self, points: Vec<Point>) -> Result<(), HnswError> {
        check_points(self.dim, &points)?;
        if points.is_empty() {
            return Ok(());
        }
        let ids: Vec<u64> = points.iter().map(|p| p.id).collect();
        self.shard.update(upsert(points)).map_err(engine_err)?;
        self.ids.extend(ids);
        Ok(())
    }

    fn finish(self: Box<Self>, out_dir: &Path) -> Result<BuiltFiles, HnswError> {
        let Self {
            shard_dir,
            shard,
            ids,
            ..
        } = *self;
        optimize_fully(&shard)?;
        shard.flush().map_err(engine_err)?;
        drop(shard);

        let out_segments = out_dir.join(SEGMENTS_DIR);
        std::fs::create_dir_all(&out_segments)?;
        let mut manifest = SegmentsManifest::default();
        for entry in std::fs::read_dir(shard_dir.join(SEGMENTS_DIR))? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(HnswError::Engine(format!("segment {name:?} is not UTF-8")));
            };
            if !entry.file_type()?.is_dir() || name.starts_with('.') {
                continue;
            }
            let uuid = Uuid::parse_str(name)
                .map_err(|e| HnswError::Engine(format!("segment directory {name:?}: {e}")))?;
            std::fs::rename(entry.path(), out_segments.join(name))?;
            manifest.set(uuid, SegmentManifestState::Active);
        }
        let manifest = qdrant_edge::external::serde_json::to_vec(&manifest)
            .map_err(|e| HnswError::Engine(format!("{SEGMENTS_MANIFEST}: {e}")))?;
        std::fs::write(out_dir.join(SEGMENTS_MANIFEST), manifest)?;

        let opened = ReadOnlyEdgeShard::open_mmap(out_dir).map_err(engine_err)?;
        let points = opened.info().map_err(engine_err)?.points_count as u64;
        drop(opened);
        if points != ids.len() {
            return Err(HnswError::Engine(format!(
                "the built index holds {points} points, {} were added",
                ids.len()
            )));
        }
        std::fs::remove_dir_all(&shard_dir)?;

        let mut files = Vec::new();
        list_files(out_dir, "", &mut files)?;
        files.sort();
        Ok(BuiltFiles {
            engine: QDRANT_ENGINE.to_owned(),
            files,
            points,
        })
    }
}

/// Every file under `dir`, as '/'-separated paths relative to the top.
fn list_files(dir: &Path, prefix: &str, out: &mut Vec<String>) -> Result<(), HnswError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(HnswError::Engine(format!(
                "file name {name:?} is not UTF-8"
            )));
        };
        let path = format!("{prefix}{name}");
        if entry.file_type()?.is_dir() {
            list_files(&entry.path(), &format!("{path}/"), out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

fn has_id(ids: &RoaringTreemap) -> Condition {
    Condition::HasId(ids.iter().map(PointId::NumId).collect::<HasIdCondition>())
}

/// Rule 7's search over either kind of shard.
fn search_shard(
    shard: &dyn EdgeShardRead,
    dim: u32,
    distance: Distance,
    query: &[f32],
    k: usize,
    filter: IdFilter<'_>,
    params: SearchParams,
) -> Result<Vec<(u64, f32)>, HnswError> {
    check_query(dim, query)?;
    let filter = match filter {
        IdFilter::All => None,
        IdFilter::Only(ids) if ids.is_empty() => return Ok(Vec::new()),
        IdFilter::Only(ids) => Some(Filter::new_must(has_id(ids))),
        IdFilter::Except(ids) if ids.is_empty() => None,
        IdFilter::Except(ids) => Some(Filter {
            must_not: Some(vec![has_id(ids)]),
            ..Filter::default()
        }),
    };
    if k == 0 {
        return Ok(Vec::new());
    }
    let mut request = SearchRequestBuilder::new(
        QueryEnum::Nearest(NamedQuery {
            query: query.to_vec().into(),
            using: Some(VECTOR_NAME.into()),
        }),
        k,
    )
    .params(qdrant_edge::SearchParams {
        hnsw_ef: params.ef.map(|e| e as usize),
        exact: params.exact,
        ..qdrant_edge::SearchParams::default()
    });
    if let Some(filter) = filter {
        request = request.filter(filter);
    }
    // `search` is documented as deprecated in favour of `query`; it is the
    // call the dependency spike verified.
    let found = shard.search(request.build()).map_err(engine_err)?;
    let negate = matches!(distance, Distance::Euclid | Distance::Manhattan);
    let mut hits = found
        .into_iter()
        .map(|point| match point.id {
            PointId::NumId(id) => {
                // `+ 0.0` turns -0.0 into 0.0, as `exact_score` does.
                let score = if negate { -point.score } else { point.score };
                Ok((id, score + 0.0))
            }
            PointId::Uuid(uuid) => Err(HnswError::Corrupt(format!(
                "point id {uuid} is not a row id"
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    sort_hits(&mut hits);
    Ok(hits)
}

/// A built index opened read-only.
struct QdrantIndex {
    dim: u32,
    distance: Distance,
    dir: PathBuf,
    points: u64,
    shard: Box<dyn EdgeShardRead + Send + Sync>,
}

impl std::fmt::Debug for QdrantIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QdrantIndex")
            .field("dir", &self.dir)
            .field("points", &self.points)
            .finish_non_exhaustive()
    }
}

impl HnswIndex for QdrantIndex {
    fn len(&self) -> u64 {
        self.points
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: IdFilter<'_>,
        params: SearchParams,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        search_shard(
            self.shard.as_ref(),
            self.dim,
            self.distance,
            query,
            k,
            filter,
            params,
        )
    }
}

/// A live shard that accepts points.
struct AppendableQdrant {
    dim: u32,
    distance: Distance,
    dir: PathBuf,
    shard: EdgeShard,
}

impl std::fmt::Debug for AppendableQdrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppendableQdrant")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl HnswIndex for AppendableQdrant {
    fn len(&self) -> u64 {
        match self.shard.info() {
            Ok(info) => info.points_count as u64,
            Err(err) => {
                tracing::warn!(dir = %self.dir.display(), %err, "qdrant-edge shard info failed");
                0
            }
        }
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: IdFilter<'_>,
        params: SearchParams,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        search_shard(
            &self.shard,
            self.dim,
            self.distance,
            query,
            k,
            filter,
            params,
        )
    }
}

impl AppendableHnsw for AppendableQdrant {
    fn append(&self, points: Vec<Point>) -> Result<(), HnswError> {
        check_points(self.dim, &points)?;
        if points.is_empty() {
            return Ok(());
        }
        self.shard.update(upsert(points)).map_err(engine_err)
    }

    fn optimize(&self) -> Result<(), HnswError> {
        optimize_fully(&self.shard)
    }
}
