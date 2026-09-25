//! Collection schemas (M1 overview §6.3).
//!
//! A schema lists the typed fields extracted from each document's `_source`,
//! the named dense vectors and the named sparse vectors. It evolves only
//! additively ([`CollectionSchema::check_additive`]): fields and dense vectors
//! are appended, and sparse vectors are fixed when the collection is created.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The analyzers a `Text` field may name.
pub const KNOWN_ANALYZERS: [&str; 5] = ["standard", "english", "simple", "whitespace", "keyword"];
/// Largest dense vector dimension.
pub const MAX_VECTOR_DIM: u32 = 65_536;
/// `max_fields` of a new schema (ES `index.mapping.total_fields.limit`).
pub const DEFAULT_MAX_FIELDS: u32 = 1_000;

/// Largest `max_fields`.
const MAX_MAX_FIELDS: u32 = 100_000;
/// Longest field, vector or annotation key name, in bytes.
const MAX_SCHEMA_NAME_LEN: usize = 255;
/// Annotations allowed beyond one per field.
const EXTRA_ANNOTATIONS: usize = 256;
/// Longest annotation value, in bytes.
const MAX_ANNOTATION_VALUE_LEN: usize = 65_536;
/// Annotation keys must start with one of these.
const ANNOTATION_PREFIXES: [&str; 3] = ["es.", "qdrant.", "operon."];
/// Most IVF partitions a vector index may ask for.
const MAX_NUM_PARTITIONS: u32 = 65_536;
/// Qdrant's scalar quantile 0.5..=1.0, in parts per million.
const QUANTILE_PPM: std::ops::RangeInclusive<u32> = 500_000..=1_000_000;
/// Qdrant's product-quantization compression ratios.
const COMPRESSION_RATIOS: [u32; 5] = [4, 8, 16, 32, 64];

/// A collection's schema.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionSchema {
    /// 1 at creation; +1 per `UpdateCollectionSchema`.
    pub version: u64,
    /// Unique names; order is stable (append only).
    pub fields: Vec<FieldSpec>,
    /// Unique names; `""` is Qdrant's unnamed default vector.
    pub vectors: Vec<VectorSpec>,
    /// Unique non-empty names, disjoint from `vectors`; fixed at creation
    /// (overview A26).
    pub sparse_vectors: Vec<SparseVectorSpec>,
    pub dynamic: DynamicMapping,
    /// At most this many fields, vectors and sparse vectors together.
    pub max_fields: u32,
    /// Opaque gateway data, keys `es.*`, `qdrant.*` or `operon.*`.
    pub annotations: BTreeMap<String, String>,
}

/// A typed field, extracted from `_source` at `source_path`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldSpec {
    /// A dot path, such as `meta.author` or `title.keyword`.
    pub name: String,
    /// Where the value comes from in `_source`; `""` (the whole `_source`)
    /// only for `Json` fields.
    pub source_path: String,
    pub kind: FieldKind,
    pub indexed: bool,
    pub fast: bool,
    /// A value of the wrong type is skipped, not a violation (overview A2).
    pub ignore_malformed: bool,
}

/// The type of a field.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldKind {
    Text { analyzer: String, positions: bool },
    Keyword,
    I64,
    F64,
    Bool,
    Date,
    Uuid,
    Json,
}

/// A named dense vector.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VectorSpec {
    pub name: String,
    pub dim: u32,
    pub distance: Distance,
    pub element: VectorElement,
    pub index: VectorIndexSpec,
    pub hnsw: HnswParams,
    pub quantization: Option<Quantization>,
}

/// A dense vector's distance function.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Distance {
    Cosine,
    Dot,
    Euclid,
    Manhattan,
}

/// A dense vector's element type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorElement {
    F32,
}

/// The Lance index built for a dense vector.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorIndexSpec {
    /// IVF_PQ for Cosine, Dot and Euclid; no index for Manhattan.
    Auto,
    None,
    IvfPq {
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        num_bits: u8,
    },
    IvfRq {
        num_partitions: Option<u32>,
        num_bits: u8,
    },
    /// HNSW `m` and `ef_construction` come from [`VectorSpec::hnsw`].
    IvfHnswSq {
        num_partitions: Option<u32>,
    },
}

impl VectorIndexSpec {
    fn num_partitions(&self) -> Option<u32> {
        match *self {
            VectorIndexSpec::Auto | VectorIndexSpec::None => None,
            VectorIndexSpec::IvfPq { num_partitions, .. }
            | VectorIndexSpec::IvfRq { num_partitions, .. }
            | VectorIndexSpec::IvfHnswSq { num_partitions } => num_partitions,
        }
    }
}

/// Qdrant's HNSW parameters, kept for the hot tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HnswParams {
    pub m: u32,
    pub ef_construct: u32,
    pub full_scan_threshold_kb: u32,
    pub payload_m: Option<u32>,
    pub on_disk: bool,
}

impl Default for HnswParams {
    /// Qdrant's defaults.
    fn default() -> Self {
        Self {
            m: 16,
            ef_construct: 100,
            full_scan_threshold_kb: 10_000,
            payload_m: None,
            on_disk: false,
        }
    }
}

/// Qdrant's quantization settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Quantization {
    Scalar {
        /// 500_000..=1_000_000: Qdrant's quantile 0.5..=1.0.
        quantile_ppm: Option<u32>,
        always_ram: bool,
    },
    Product {
        /// 4, 8, 16, 32 or 64.
        compression_ratio: u32,
        always_ram: bool,
    },
    Binary {
        always_ram: bool,
    },
}

/// What a write does with `_source` paths no field maps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DynamicMapping {
    /// Such a write is refused.
    Strict,
    /// They live only in `_source`.
    Ignore,
    /// The gateway adds fields for them (ES dynamic mapping).
    Map,
}

/// A named sparse vector (overview A26).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparseVectorSpec {
    pub name: String,
    pub modifier: SparseModifier,
}

/// Qdrant's sparse vector `modifier`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SparseModifier {
    None,
    /// Reweights the query by IDF at search time.
    Idf,
}

/// Why a schema, or a change to one, was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("invalid schema: {0}")]
    Invalid(String),
    #[error("incompatible schema change: {0}")]
    Incompatible(String),
}

fn invalid(message: impl Into<String>) -> Result<(), SchemaError> {
    Err(SchemaError::Invalid(message.into()))
}

fn incompatible(message: impl Into<String>) -> Result<(), SchemaError> {
    Err(SchemaError::Incompatible(message.into()))
}

/// Checks the rules field, vector and sparse vector names share: at most 255
/// bytes, no control characters, and unique within `seen`.
fn check_name<'a>(
    kind: &str,
    name: &'a str,
    seen: &mut BTreeSet<&'a str>,
) -> Result<(), SchemaError> {
    if name.len() > MAX_SCHEMA_NAME_LEN {
        return invalid(format!(
            "{kind} name {name:?} is longer than {MAX_SCHEMA_NAME_LEN} bytes"
        ));
    }
    if name.chars().any(char::is_control) {
        return invalid(format!("{kind} name {name:?} contains a control character"));
    }
    if !seen.insert(name) {
        return invalid(format!("duplicate {kind} name {name:?}"));
    }
    Ok(())
}

impl FieldSpec {
    fn validate<'a>(&'a self, seen: &mut BTreeSet<&'a str>) -> Result<(), SchemaError> {
        let name = self.name.as_str();
        if name.is_empty() {
            return invalid("a field name is empty");
        }
        check_name("field", name, seen)?;
        if name.starts_with('_') || name.starts_with('-') {
            return invalid(format!("field name {name:?} starts with '_' or '-'"));
        }
        if name.starts_with('.') || name.ends_with('.') || name.contains("..") {
            return invalid(format!(
                "field name {name:?} starts or ends with '.' or contains \"..\""
            ));
        }
        if self.source_path.is_empty() {
            if self.kind != FieldKind::Json {
                return invalid(format!(
                    "field {name:?} has an empty source path, which only Json fields may have"
                ));
            }
        } else if self.source_path.split('.').any(str::is_empty) {
            return invalid(format!(
                "field {name:?} has source path {:?} with an empty segment",
                self.source_path
            ));
        }
        if let FieldKind::Text { analyzer, .. } = &self.kind {
            if !KNOWN_ANALYZERS.contains(&analyzer.as_str()) {
                return invalid(format!(
                    "field {name:?} names unknown analyzer {analyzer:?}"
                ));
            }
            if self.fast {
                return invalid(format!("text field {name:?} cannot be fast"));
            }
        }
        if !self.indexed && !self.fast {
            return invalid(format!("field {name:?} is neither indexed nor fast"));
        }
        Ok(())
    }
}

impl VectorSpec {
    fn validate<'a>(&'a self, seen: &mut BTreeSet<&'a str>) -> Result<(), SchemaError> {
        let name = self.name.as_str();
        check_name("vector", name, seen)?;
        if !(1..=MAX_VECTOR_DIM).contains(&self.dim) {
            return invalid(format!(
                "vector {name:?} has dim {}, expected 1..={MAX_VECTOR_DIM}",
                self.dim
            ));
        }
        if self.distance == Distance::Manhattan
            && !matches!(self.index, VectorIndexSpec::Auto | VectorIndexSpec::None)
        {
            return invalid(format!(
                "vector {name:?} uses Manhattan distance, which has no IVF index"
            ));
        }
        match self.index {
            VectorIndexSpec::IvfPq {
                num_sub_vectors,
                num_bits,
                ..
            } => {
                if !matches!(num_bits, 4 | 8) {
                    return invalid(format!(
                        "vector {name:?}: IVF_PQ num_bits must be 4 or 8, got {num_bits}"
                    ));
                }
                if let Some(sub) = num_sub_vectors
                    && (sub == 0 || !self.dim.is_multiple_of(sub))
                {
                    return invalid(format!(
                        "vector {name:?}: {sub} PQ sub-vectors do not divide dim {}",
                        self.dim
                    ));
                }
            }
            VectorIndexSpec::IvfRq { num_bits, .. } if !(1..=8).contains(&num_bits) => {
                return invalid(format!(
                    "vector {name:?}: IVF_RQ num_bits must be 1..=8, got {num_bits}"
                ));
            }
            _ => {}
        }
        if let Some(partitions) = self.index.num_partitions()
            && !(1..=MAX_NUM_PARTITIONS).contains(&partitions)
        {
            return invalid(format!(
                "vector {name:?}: num_partitions must be 1..={MAX_NUM_PARTITIONS}, got {partitions}"
            ));
        }
        match self.quantization {
            Some(Quantization::Product {
                compression_ratio, ..
            }) if !COMPRESSION_RATIOS.contains(&compression_ratio) => invalid(format!(
                "vector {name:?}: product quantization compression ratio must be one of \
                 {COMPRESSION_RATIOS:?}, got {compression_ratio}"
            )),
            Some(Quantization::Scalar {
                quantile_ppm: Some(ppm),
                ..
            }) if !QUANTILE_PPM.contains(&ppm) => invalid(format!(
                "vector {name:?}: scalar quantile must be {QUANTILE_PPM:?} ppm, got {ppm}"
            )),
            _ => Ok(()),
        }
    }
}

impl CollectionSchema {
    /// A version 1 schema with `max_fields` 1000, no sparse vectors and no
    /// annotations.
    pub fn new(fields: Vec<FieldSpec>, vectors: Vec<VectorSpec>, dynamic: DynamicMapping) -> Self {
        Self {
            version: 1,
            fields,
            vectors,
            sparse_vectors: Vec::new(),
            dynamic,
            max_fields: DEFAULT_MAX_FIELDS,
            annotations: BTreeMap::new(),
        }
    }

    /// This schema with `sparse_vectors`.
    pub fn with_sparse_vectors(mut self, sparse_vectors: Vec<SparseVectorSpec>) -> Self {
        self.sparse_vectors = sparse_vectors;
        self
    }

    /// Fields, vectors and sparse vectors together.
    fn member_count(&self) -> usize {
        self.fields.len() + self.vectors.len() + self.sparse_vectors.len()
    }

    /// Checks the schema on its own; the error names the offender.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.version == 0 {
            return invalid("version 0");
        }
        if !(1..=MAX_MAX_FIELDS).contains(&self.max_fields) {
            return invalid(format!(
                "max_fields must be 1..={MAX_MAX_FIELDS}, got {}",
                self.max_fields
            ));
        }
        let max_fields = self.max_fields as usize;
        if self.member_count() > max_fields {
            return invalid(format!(
                "{} fields, vectors and sparse vectors exceed max_fields {max_fields}",
                self.member_count()
            ));
        }
        let mut seen = BTreeSet::new();
        for field in &self.fields {
            field.validate(&mut seen)?;
        }
        let mut dense = BTreeSet::new();
        for vector in &self.vectors {
            vector.validate(&mut dense)?;
        }
        let mut sparse = BTreeSet::new();
        for spec in &self.sparse_vectors {
            let name = spec.name.as_str();
            if name.is_empty() {
                return invalid("a sparse vector name is empty");
            }
            check_name("sparse vector", name, &mut sparse)?;
            if dense.contains(name) {
                return invalid(format!(
                    "sparse vector {name:?} has the name of a dense vector"
                ));
            }
        }
        if self.annotations.len() > max_fields + EXTRA_ANNOTATIONS {
            return invalid(format!(
                "{} annotations exceed max_fields + {EXTRA_ANNOTATIONS}",
                self.annotations.len()
            ));
        }
        for (key, value) in &self.annotations {
            if !ANNOTATION_PREFIXES.iter().any(|p| key.starts_with(p)) {
                return invalid(format!(
                    "annotation key {key:?} does not start with one of {ANNOTATION_PREFIXES:?}"
                ));
            }
            if key.len() > MAX_SCHEMA_NAME_LEN {
                return invalid(format!(
                    "annotation key {key:?} is longer than {MAX_SCHEMA_NAME_LEN} bytes"
                ));
            }
            if value.len() > MAX_ANNOTATION_VALUE_LEN {
                return invalid(format!(
                    "annotation {key:?} has a value longer than {MAX_ANNOTATION_VALUE_LEN} bytes"
                ));
            }
        }
        Ok(())
    }

    /// Checks that `next` only extends this schema: it keeps every field and
    /// dense vector as they are, in order, and may append more; its sparse
    /// vectors are unchanged; and its `max_fields` still holds everything.
    /// `dynamic` and `annotations` may change freely.
    pub fn check_additive(&self, next: &CollectionSchema) -> Result<(), SchemaError> {
        if !next.fields.starts_with(&self.fields) {
            return incompatible("fields may only be appended, never changed or removed");
        }
        if !next.vectors.starts_with(&self.vectors) {
            return incompatible("vectors may only be appended, never changed or removed");
        }
        if next.sparse_vectors != self.sparse_vectors {
            return incompatible("sparse vectors are fixed when the collection is created");
        }
        if next.member_count() > next.max_fields as usize {
            return incompatible(format!(
                "max_fields {} is below the {} fields, vectors and sparse vectors",
                next.max_fields,
                next.member_count()
            ));
        }
        Ok(())
    }

    /// Whether the two schemas are equal apart from their versions.
    pub fn same_ignoring_version(&self, other: &CollectionSchema) -> bool {
        let Self {
            version: _,
            fields,
            vectors,
            sparse_vectors,
            dynamic,
            max_fields,
            annotations,
        } = self;
        *fields == other.fields
            && *vectors == other.vectors
            && *sparse_vectors == other.sparse_vectors
            && *dynamic == other.dynamic
            && *max_fields == other.max_fields
            && *annotations == other.annotations
    }

    /// The field named `name`.
    pub fn field(&self, name: &str) -> Option<&FieldSpec> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// The dense vector named `name`, with its position in `vectors`.
    pub fn vector(&self, name: &str) -> Option<(usize, &VectorSpec)> {
        self.vectors
            .iter()
            .enumerate()
            .find(|(_, v)| v.name == name)
    }

    /// The sparse vector named `name`, with its position in `sparse_vectors`.
    pub fn sparse_vector(&self, name: &str) -> Option<(usize, &SparseVectorSpec)> {
        self.sparse_vectors
            .iter()
            .enumerate()
            .find(|(_, v)| v.name == name)
    }
}
