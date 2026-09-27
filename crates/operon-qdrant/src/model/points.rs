//! Point request and response types: writes (Task 5) and reads (Task 6).
//!
//! The unions whose variants are told apart by one key (`PointInsert`,
//! `PointsSelector`, `UpdateOperation`) are read through a raw struct or
//! by key, not `#[serde(untagged)]`, so a malformed filter inside them
//! keeps its own error text (Ruling 4; row T5-1).

use std::collections::BTreeMap;

use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::model::common::{Record, VectorInput, WithPayload, WithVector};
use crate::model::filter::Filter;

/// A point's payload as a request gives it.
pub type Payload = Map<String, Value>;

// ----- writes -----

/// `PUT /collections/{c}/points`: a list of points or a batch.
#[derive(Clone, Debug, PartialEq)]
pub enum PointInsert {
    List {
        points: Vec<PointStruct>,
        shard_key: Option<Value>,
        update_filter: Option<Value>,
        update_mode: Option<String>,
    },
    Batch {
        batch: Batch,
        shard_key: Option<Value>,
        update_filter: Option<Value>,
        update_mode: Option<String>,
    },
}

#[derive(Deserialize)]
struct RawInsert {
    #[serde(default)]
    points: Option<Vec<PointStruct>>,
    #[serde(default)]
    batch: Option<Batch>,
    #[serde(default)]
    shard_key: Option<Value>,
    #[serde(default)]
    update_filter: Option<Value>,
    #[serde(default)]
    update_mode: Option<String>,
}

impl<'de> Deserialize<'de> for PointInsert {
    /// By the one of `points` and `batch` that is present.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = RawInsert::deserialize(d)?;
        match (raw.points, raw.batch) {
            (Some(points), None) => Ok(PointInsert::List {
                points,
                shard_key: raw.shard_key,
                update_filter: raw.update_filter,
                update_mode: raw.update_mode,
            }),
            (None, Some(batch)) => Ok(PointInsert::Batch {
                batch,
                shard_key: raw.shard_key,
                update_filter: raw.update_filter,
                update_mode: raw.update_mode,
            }),
            _ => Err(D::Error::custom(
                "data did not match any variant of untagged enum PointInsertOperations",
            )),
        }
    }
}

/// One point of a list upsert.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PointStruct {
    pub id: Value,
    pub vector: VectorStruct,
    #[serde(default)]
    pub payload: Option<Payload>,
}

/// A point's vectors: one unnamed vector, a multivector, named vectors, or
/// anything else (an inference object).
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum VectorStruct {
    Single(Vec<f32>),
    Multi(Vec<Vec<f32>>),
    Named(BTreeMap<String, VectorInput>),
    Object(Value),
}

/// The columnar upsert form.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Batch {
    pub ids: Vec<Value>,
    pub vectors: BatchVectors,
    #[serde(default)]
    pub payloads: Option<Vec<Option<Payload>>>,
}

/// A batch's vectors: one list per point, or per name one value per point.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum BatchVectors {
    Single(Vec<Vec<f32>>),
    Named(BTreeMap<String, Vec<VectorInput>>),
    Other(Value),
}

/// The points an operation addresses: by id or by filter.
#[derive(Clone, Debug, PartialEq)]
pub enum PointsSelector {
    Ids { points: Vec<Value> },
    Filter { filter: Box<Filter> },
}

#[derive(Deserialize)]
struct RawSelector {
    #[serde(default)]
    points: Option<Vec<Value>>,
    #[serde(default)]
    filter: Option<Filter>,
}

impl<'de> Deserialize<'de> for PointsSelector {
    /// By the one of `points` and `filter` that is present.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = RawSelector::deserialize(d)?;
        match (raw.points, raw.filter) {
            (Some(points), None) => Ok(PointsSelector::Ids { points }),
            (None, Some(filter)) => Ok(PointsSelector::Filter {
                filter: Box::new(filter),
            }),
            _ => Err(D::Error::custom(
                "data did not match any variant of untagged enum PointsSelector",
            )),
        }
    }
}

/// `set_payload` and `overwrite_payload`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct SetPayload {
    pub payload: Payload,
    #[serde(default)]
    pub points: Option<Vec<Value>>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub key: Option<String>,
}

/// `delete_payload`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DeletePayload {
    pub keys: Vec<String>,
    #[serde(default)]
    pub points: Option<Vec<Value>>,
    #[serde(default)]
    pub filter: Option<Filter>,
}

/// `update_vectors`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct UpdateVectors {
    pub points: Vec<PointVectors>,
    #[serde(default)]
    pub update_filter: Option<Value>,
}

/// One point of `update_vectors`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PointVectors {
    pub id: Value,
    pub vector: VectorStruct,
}

/// `delete_vectors`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DeleteVectors {
    #[serde(default)]
    pub points: Option<Vec<Value>>,
    #[serde(default)]
    pub filter: Option<Filter>,
    pub vector: Vec<String>,
}

/// One operation of `POST /points/batch`; every write route is converted
/// to one of these.
#[derive(Clone, Debug, PartialEq)]
pub enum UpdateOperation {
    Upsert { upsert: PointInsert },
    Delete { delete: PointsSelector },
    SetPayload { set_payload: SetPayload },
    OverwritePayload { overwrite_payload: SetPayload },
    DeletePayload { delete_payload: DeletePayload },
    ClearPayload { clear_payload: PointsSelector },
    UpdateVectors { update_vectors: UpdateVectors },
    DeleteVectors { delete_vectors: DeleteVectors },
}

impl<'de> Deserialize<'de> for UpdateOperation {
    /// By its one key, so the inner error text survives.
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        /// `T` from `v`, keeping `v`'s own error text.
        fn inner<T: DeserializeOwned, E: serde::de::Error>(v: &Value) -> Result<T, E> {
            T::deserialize(v).map_err(E::custom)
        }
        let map = Map::<String, Value>::deserialize(d)?;
        let (key, v) = [
            "upsert",
            "delete",
            "set_payload",
            "overwrite_payload",
            "delete_payload",
            "clear_payload",
            "update_vectors",
            "delete_vectors",
        ]
        .into_iter()
        .find_map(|key| map.get(key).map(|v| (key, v)))
        .ok_or_else(|| {
            D::Error::custom("data did not match any variant of untagged enum UpdateOperation")
        })?;
        Ok(match key {
            "upsert" => UpdateOperation::Upsert { upsert: inner(v)? },
            "delete" => UpdateOperation::Delete { delete: inner(v)? },
            "set_payload" => UpdateOperation::SetPayload {
                set_payload: inner(v)?,
            },
            "overwrite_payload" => UpdateOperation::OverwritePayload {
                overwrite_payload: inner(v)?,
            },
            "delete_payload" => UpdateOperation::DeletePayload {
                delete_payload: inner(v)?,
            },
            "clear_payload" => UpdateOperation::ClearPayload {
                clear_payload: inner(v)?,
            },
            "update_vectors" => UpdateOperation::UpdateVectors {
                update_vectors: inner(v)?,
            },
            _ => UpdateOperation::DeleteVectors {
                delete_vectors: inner(v)?,
            },
        })
    }
}

/// `POST /collections/{c}/points/batch`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct UpdateOperations {
    pub operations: Vec<UpdateOperation>,
}

// ----- reads -----

/// `POST /collections/{c}/points` (retrieve).
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PointRequest {
    pub ids: Vec<Value>,
    #[serde(default)]
    pub with_payload: Option<WithPayload>,
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,
}

/// `POST /collections/{c}/points/scroll`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScrollRequest {
    #[serde(default)]
    pub offset: Option<Value>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub with_payload: Option<WithPayload>,
    #[serde(default, alias = "with_vectors")]
    pub with_vector: Option<WithVector>,
    #[serde(default)]
    pub order_by: Option<Value>,
}

/// `{"points": [...], "next_page_offset": id | null}`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ScrollResult {
    pub points: Vec<Record>,
    /// `null` on the last page (never omitted).
    pub next_page_offset: Option<Value>,
}

/// `POST /collections/{c}/points/count`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CountRequest {
    #[serde(default)]
    pub filter: Option<Filter>,
    /// Counts are always exact.
    #[serde(default)]
    pub exact: Option<bool>,
}

/// `{"count": n}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct CountResult {
    pub count: u64,
}
