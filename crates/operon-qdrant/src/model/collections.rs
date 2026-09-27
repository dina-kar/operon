//! Collection request and response types (Task 3). Every request field is
//! optional unless marked; unknown fields are ignored (Ruling 4).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `GET /collections`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>,
}

/// One collection of `GET /collections`, by name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CollectionDescription {
    pub name: String,
}

/// `PUT /collections/{c}`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct CreateCollection {
    #[serde(default)]
    pub vectors: Option<VectorsConfig>,
    #[serde(default)]
    pub shard_number: Option<u32>,
    #[serde(default)]
    pub sharding_method: Option<String>,
    #[serde(default)]
    pub replication_factor: Option<u32>,
    #[serde(default)]
    pub write_consistency_factor: Option<u32>,
    #[serde(default)]
    pub on_disk_payload: Option<bool>,
    #[serde(default)]
    pub hnsw_config: Option<HnswConfigDiff>,
    #[serde(default)]
    pub wal_config: Option<Value>,
    #[serde(default)]
    pub optimizers_config: Option<Value>,
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default)]
    pub sparse_vectors: Option<BTreeMap<String, SparseVectorParams>>,
    #[serde(default)]
    pub strict_mode_config: Option<Value>,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
    /// Sent by 1.15 clients; a non-null value is unsupported.
    #[serde(default)]
    pub init_from: Option<Value>,
}

/// One unnamed vector, or vectors by name.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum VectorsConfig {
    Single(VectorParams),
    Map(BTreeMap<String, VectorParams>),
}

/// One dense vector's parameters.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct VectorParams {
    pub size: u64,
    pub distance: DistanceName,
    #[serde(default)]
    pub hnsw_config: Option<HnswConfigDiff>,
    #[serde(default)]
    pub quantization_config: Option<QuantizationConfig>,
    #[serde(default)]
    pub on_disk: Option<bool>,
    #[serde(default)]
    pub datatype: Option<String>,
    #[serde(default)]
    pub multivector_config: Option<Value>,
}

/// Qdrant's distance names; any other is a format error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceName {
    Cosine,
    Euclid,
    Dot,
    Manhattan,
}

/// One sparse vector's parameters.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SparseVectorParams {
    #[serde(default)]
    pub index: Option<SparseIndexParams>,
    #[serde(default)]
    pub modifier: Option<ModifierName>,
}

/// Accepted and echoed; none of these changes a search (Ruling 21).
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SparseIndexParams {
    #[serde(default)]
    pub full_scan_threshold: Option<u64>,
    #[serde(default)]
    pub on_disk: Option<bool>,
    #[serde(default)]
    pub memory: Option<Value>,
    #[serde(default)]
    pub datatype: Option<String>,
}

/// A sparse vector's `modifier`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModifierName {
    None,
    Idf,
}

/// `hnsw_config`: each given key overlays Qdrant's defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct HnswConfigDiff {
    #[serde(default)]
    pub m: Option<u32>,
    #[serde(default)]
    pub ef_construct: Option<u32>,
    #[serde(default)]
    pub full_scan_threshold: Option<u32>,
    #[serde(default)]
    pub max_indexing_threads: Option<u32>,
    #[serde(default)]
    pub on_disk: Option<bool>,
    #[serde(default)]
    pub payload_m: Option<u32>,
}

/// Scalar, product or binary quantization; anything else (turbo, …) is
/// unsupported.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum QuantizationConfig {
    Scalar { scalar: ScalarConfig },
    Product { product: ProductConfig },
    Binary { binary: BinaryConfig },
    Other(Value),
}

/// Scalar quantization (`int8` only).
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ScalarConfig {
    /// `"int8"`.
    pub r#type: String,
    #[serde(default)]
    pub quantile: Option<f32>,
    #[serde(default)]
    pub always_ram: Option<bool>,
}

/// Product quantization.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct ProductConfig {
    /// `"x4"`, `"x8"`, `"x16"`, `"x32"` or `"x64"`.
    pub compression: String,
    #[serde(default)]
    pub always_ram: Option<bool>,
}

/// Binary quantization.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BinaryConfig {
    #[serde(default)]
    pub always_ram: Option<bool>,
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub query_encoding: Option<String>,
}

/// `POST /collections/aliases`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ChangeAliases {
    pub actions: Vec<AliasOperation>,
}

/// One action of `POST /collections/aliases`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum AliasOperation {
    Create { create_alias: CreateAlias },
    Delete { delete_alias: DeleteAlias },
    Rename { rename_alias: RenameAlias },
}

/// `create_alias`: `alias_name` names `collection_name`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct CreateAlias {
    pub collection_name: String,
    pub alias_name: String,
}

/// `delete_alias`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct DeleteAlias {
    pub alias_name: String,
}

/// `rename_alias`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct RenameAlias {
    pub old_alias_name: String,
    pub new_alias_name: String,
}

/// `GET /aliases` and `GET /collections/{c}/aliases`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AliasesResponse {
    pub aliases: Vec<AliasDescription>,
}

/// One `(alias, collection)` pair.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AliasDescription {
    pub alias_name: String,
    pub collection_name: String,
}

/// `GET /collections/{c}/exists`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct CollectionExistence {
    pub exists: bool,
}

/// `PUT /collections/{c}/index`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct CreateFieldIndex {
    pub field_name: String,
    /// Required; `None` is `field_schema is required`.
    #[serde(default)]
    pub field_schema: Option<PayloadFieldSchema>,
}

/// A payload index type: its name (`"keyword"`, …), or its params
/// (`{"type": "text", …}`).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum PayloadFieldSchema {
    Name(String),
    Params(Map<String, Value>),
}
