//! The exact engine (rule 4): a scan over every point.
//!
//! `flat.bin`: `b"OPFV" | u16 LE 1 | u32 LE dim | u8 distance | u64 LE count |
//! count × u64 LE id (ascending) | count × dim × f32 LE | u32 LE crc32c of
//! every preceding byte`.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};

use crate::HnswError;
use crate::types::{
    AppendableHnsw, BuildSpec, BuiltFiles, Distance, HnswBuilder, HnswEngine, HnswIndex, IdFilter,
    Point, SearchParams, check_points, check_query, check_spec, hit_order, sort_hits,
};

pub const FLAT_ENGINE: &str = "flat";
pub const FLAT_FILE: &str = "flat.bin";
pub const FLAT_MAGIC: &[u8; 4] = b"OPFV";
const FLAT_VERSION: u16 = 1;
/// Magic, version, dim, distance and count.
const HEADER_LEN: usize = 4 + 2 + 4 + 1 + 8;

/// The exact engine.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlatEngine;

/// Operon's score convention for one pair (rule 3); zero-length vectors score 0 under Cosine.
pub fn exact_score(distance: Distance, a: &[f32], b: &[f32]) -> f32 {
    let pairs = a.iter().zip(b).map(|(&x, &y)| (f64::from(x), f64::from(y)));
    let score = match distance {
        Distance::Dot => pairs.map(|(x, y)| x * y).sum(),
        Distance::Cosine => {
            let (mut dot, mut aa, mut bb) = (0.0, 0.0, 0.0);
            for (x, y) in pairs {
                dot += x * y;
                aa += x * x;
                bb += y * y;
            }
            if aa == 0.0 || bb == 0.0 {
                0.0
            } else {
                dot / (aa.sqrt() * bb.sqrt())
            }
        }
        Distance::Euclid => -pairs.map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt(),
        Distance::Manhattan => -pairs.map(|(x, y)| (x - y).abs()).sum::<f64>(),
    };
    // `+ 0.0` turns -0.0 into 0.0, so equal scores tie by id.
    score as f32 + 0.0
}

fn distance_code(distance: Distance) -> u8 {
    match distance {
        Distance::Cosine => 0,
        Distance::Dot => 1,
        Distance::Euclid => 2,
        Distance::Manhattan => 3,
    }
}

impl HnswEngine for FlatEngine {
    fn name(&self) -> &'static str {
        FLAT_ENGINE
    }

    fn builder(
        &self,
        spec: &BuildSpec,
        _work_dir: &Path,
    ) -> Result<Box<dyn HnswBuilder>, HnswError> {
        check_spec(spec)?;
        Ok(Box::new(FlatBuilder {
            spec: spec.clone(),
            points: BTreeMap::new(),
        }))
    }

    fn open(&self, spec: &BuildSpec, dir: &Path) -> Result<Arc<dyn HnswIndex>, HnswError> {
        check_spec(spec)?;
        let bytes = std::fs::read(dir.join(FLAT_FILE))?;
        Ok(Arc::new(decode(spec, &bytes)?))
    }

    fn appendable(
        &self,
        spec: &BuildSpec,
        _work_dir: &Path,
    ) -> Result<Arc<dyn AppendableHnsw>, HnswError> {
        check_spec(spec)?;
        Ok(Arc::new(AppendableFlat {
            spec: spec.clone(),
            points: RwLock::new(BTreeMap::new()),
        }))
    }
}

#[derive(Debug)]
struct FlatBuilder {
    spec: BuildSpec,
    /// A later point with the same id replaces the earlier one (an upsert).
    points: BTreeMap<u64, Vec<f32>>,
}

impl HnswBuilder for FlatBuilder {
    fn add(&mut self, points: Vec<Point>) -> Result<(), HnswError> {
        check_points(self.spec.dim, &points)?;
        for point in points {
            self.points.insert(point.id, point.vector);
        }
        Ok(())
    }

    fn finish(self: Box<Self>, out_dir: &Path) -> Result<BuiltFiles, HnswError> {
        let bytes = encode(&self.spec, &self.points);
        let mut file = std::fs::File::create(out_dir.join(FLAT_FILE))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(BuiltFiles {
            engine: FLAT_ENGINE.to_owned(),
            files: vec![FLAT_FILE.to_owned()],
            points: self.points.len() as u64,
        })
    }
}

fn encode(spec: &BuildSpec, points: &BTreeMap<u64, Vec<f32>>) -> Vec<u8> {
    let count = points.len();
    let mut out = Vec::with_capacity(HEADER_LEN + count * (8 + 4 * spec.dim as usize) + 4);
    out.extend_from_slice(FLAT_MAGIC);
    out.extend_from_slice(&FLAT_VERSION.to_le_bytes());
    out.extend_from_slice(&spec.dim.to_le_bytes());
    out.push(distance_code(spec.distance));
    out.extend_from_slice(&(count as u64).to_le_bytes());
    for id in points.keys() {
        out.extend_from_slice(&id.to_le_bytes());
    }
    for vector in points.values() {
        for value in vector {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn decode(spec: &BuildSpec, bytes: &[u8]) -> Result<FlatIndex, HnswError> {
    let corrupt = |what: String| HnswError::Corrupt(format!("{FLAT_FILE}: {what}"));
    if bytes.len() < HEADER_LEN + 4 {
        return Err(corrupt(format!("{} bytes is too short", bytes.len())));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    let crc = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32c::crc32c(body) != crc {
        return Err(corrupt("checksum mismatch".into()));
    }
    if &body[0..4] != FLAT_MAGIC {
        return Err(corrupt("wrong magic".into()));
    }
    let version = u16::from_le_bytes([body[4], body[5]]);
    if version != FLAT_VERSION {
        return Err(corrupt(format!(
            "version {version}, expected {FLAT_VERSION}"
        )));
    }
    let dim = u32::from_le_bytes([body[6], body[7], body[8], body[9]]);
    if dim != spec.dim {
        return Err(corrupt(format!("dim {dim}, expected {}", spec.dim)));
    }
    let distance = body[10];
    if distance != distance_code(spec.distance) {
        return Err(corrupt(format!(
            "distance code {distance}, expected {}",
            distance_code(spec.distance)
        )));
    }
    let mut count = [0; 8];
    count.copy_from_slice(&body[11..19]);
    let count = u64::from_le_bytes(count);
    let expected = usize::try_from(count)
        .ok()
        .and_then(|c| c.checked_mul(8 + 4 * dim as usize))
        .and_then(|n| n.checked_add(HEADER_LEN));
    if expected != Some(body.len()) {
        return Err(corrupt(format!(
            "{} bytes do not hold {count} points of dim {dim}",
            body.len()
        )));
    }
    let count = count as usize;
    let (ids, vectors) = body[HEADER_LEN..].split_at(count * 8);
    let ids: Vec<u64> = ids
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
        .collect();
    if ids.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupt("ids are not strictly ascending".into()));
    }
    let vectors = vectors
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(FlatIndex {
        spec: spec.clone(),
        ids,
        vectors,
    })
}

/// Exact top-k over `(id, vector)` pairs.
fn scan<'v>(
    spec: &BuildSpec,
    points: impl Iterator<Item = (u64, &'v [f32])>,
    query: &[f32],
    k: usize,
    filter: IdFilter<'_>,
) -> Result<Vec<(u64, f32)>, HnswError> {
    check_query(spec.dim, query)?;
    if k == 0 {
        return Ok(Vec::new());
    }
    let mut hits: Vec<(u64, f32)> = points
        .filter(|(id, _)| filter.allows(*id))
        .map(|(id, vector)| (id, exact_score(spec.distance, query, vector)))
        .collect();
    if hits.len() > k {
        hits.select_nth_unstable_by(k - 1, hit_order);
        hits.truncate(k);
    }
    sort_hits(&mut hits);
    Ok(hits)
}

/// A read-only exact index loaded from `flat.bin`.
#[derive(Debug)]
struct FlatIndex {
    spec: BuildSpec,
    ids: Vec<u64>,
    /// `ids.len() × dim` values.
    vectors: Vec<f32>,
}

impl HnswIndex for FlatIndex {
    fn len(&self) -> u64 {
        self.ids.len() as u64
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: IdFilter<'_>,
        _params: SearchParams,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        let dim = self.spec.dim as usize;
        let points = self.ids.iter().copied().zip(self.vectors.chunks_exact(dim));
        scan(&self.spec, points, query, k, filter)
    }
}

/// An in-memory exact index that accepts points (upserts by id).
#[derive(Debug)]
struct AppendableFlat {
    spec: BuildSpec,
    points: RwLock<BTreeMap<u64, Vec<f32>>>,
}

impl HnswIndex for AppendableFlat {
    fn len(&self) -> u64 {
        self.points
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len() as u64
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter: IdFilter<'_>,
        _params: SearchParams,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        let points = self.points.read().unwrap_or_else(PoisonError::into_inner);
        let points = points.iter().map(|(id, v)| (*id, v.as_slice()));
        scan(&self.spec, points, query, k, filter)
    }
}

impl AppendableHnsw for AppendableFlat {
    fn append(&self, points: Vec<Point>) -> Result<(), HnswError> {
        check_points(self.spec.dim, &points)?;
        let mut map = self.points.write().unwrap_or_else(PoisonError::into_inner);
        for point in points {
            map.insert(point.id, point.vector);
        }
        Ok(())
    }

    fn optimize(&self) -> Result<(), HnswError> {
        Ok(())
    }
}
