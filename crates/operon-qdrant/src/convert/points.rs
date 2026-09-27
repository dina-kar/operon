//! gRPC point messages to the REST model (Tasks 5 and 6) and results back.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use crate::convert::common::vector_output_to_grpc;
use crate::convert::filter::filter_from_grpc;
use crate::convert::value::{map_to_payload, payload_to_map};
use crate::error::GatewayError;
use crate::ids::{pk_to_json, point_id_from_grpc};
use crate::model::common::{Record, UpdateResult, UpdateStatus, VectorInput};
use crate::model::filter::Filter;
use crate::model::points::{
    DeletePayload, DeleteVectors, PointInsert, PointStruct, PointVectors, PointsSelector,
    SetPayload, UpdateOperation, UpdateVectors, VectorStruct,
};
use crate::proto::qdrant as pb;

/// A gRPC id as the model's JSON id (canonical), with gRPC's own UUID error.
pub fn id_from_grpc(id: Option<&pb::PointId>) -> Result<Value, GatewayError> {
    let id = id.ok_or_else(|| GatewayError::BadRequest("Empty ID is not allowed".to_string()))?;
    Ok(pk_to_json(&point_id_from_grpc(id)?.to_pk()))
}

/// Several gRPC ids as JSON ids.
fn ids_from_grpc(ids: &[pb::PointId]) -> Result<Vec<Value>, GatewayError> {
    ids.iter().map(|id| id_from_grpc(Some(id))).collect()
}

/// A JSON id (a number or a string) as a gRPC id; a string travels in
/// `uuid` (row T1-5).
pub fn id_to_grpc(id: &Value) -> pb::PointId {
    use pb::point_id::PointIdOptions;
    let options = match id {
        Value::Number(n) => PointIdOptions::Num(n.as_u64().unwrap_or_default()),
        Value::String(s) => PointIdOptions::Uuid(s.clone()),
        other => PointIdOptions::Uuid(other.to_string()),
    };
    pb::PointId {
        point_id_options: Some(options),
    }
}

/// One gRPC vector value: the `vector` oneof first, else the deprecated
/// `data` (+ `indices` for sparse, `vectors_count` for multi).
#[allow(deprecated)]
pub(crate) fn vector_input(v: &pb::Vector) -> VectorInput {
    use pb::vector::Vector;
    match &v.vector {
        Some(Vector::Dense(d)) => VectorInput::Dense(d.data.clone()),
        Some(Vector::Sparse(s)) => VectorInput::Sparse {
            indices: s.indices.clone(),
            values: s.values.clone(),
        },
        Some(Vector::MultiDense(m)) => {
            VectorInput::Multi(m.vectors.iter().map(|d| d.data.clone()).collect())
        }
        Some(Vector::Document(_) | Vector::Image(_) | Vector::Object(_)) => {
            VectorInput::Object(Map::new())
        }
        None => match (&v.indices, v.vectors_count) {
            (Some(indices), _) => VectorInput::Sparse {
                indices: indices.data.clone(),
                values: v.data.clone(),
            },
            (None, Some(count)) if count > 1 => VectorInput::Multi(vec![v.data.clone()]),
            _ => VectorInput::Dense(v.data.clone()),
        },
    }
}

/// A point's gRPC vectors; absent ones are an empty map (no vectors).
pub fn vectors_from_grpc(v: Option<&pb::Vectors>) -> Result<VectorStruct, GatewayError> {
    use pb::vectors::VectorsOptions;
    match v.and_then(|v| v.vectors_options.as_ref()) {
        None => Ok(VectorStruct::Named(BTreeMap::new())),
        Some(VectorsOptions::Vector(v)) => match vector_input(v) {
            VectorInput::Dense(d) => Ok(VectorStruct::Single(d)),
            VectorInput::Multi(m) => Ok(VectorStruct::Multi(m)),
            VectorInput::Sparse { .. } => Err(GatewayError::BadRequest(
                "a sparse vector must be named".to_string(),
            )),
            _ => Ok(VectorStruct::Object(Value::Object(Map::new()))),
        },
        Some(VectorsOptions::Vectors(named)) => Ok(VectorStruct::Named(
            named
                .vectors
                .iter()
                .map(|(name, v)| (name.clone(), vector_input(v)))
                .collect(),
        )),
    }
}

/// A gRPC point as the model's `PointStruct`.
fn point_from_grpc(p: &pb::PointStruct) -> Result<PointStruct, GatewayError> {
    Ok(PointStruct {
        id: id_from_grpc(p.id.as_ref())?,
        vector: vectors_from_grpc(p.vectors.as_ref())?,
        payload: Some(payload_to_map(&p.payload)),
    })
}

/// `update_mode` as its REST name; `upsert` (the default) is none.
fn update_mode(mode: Option<i32>) -> Option<String> {
    let mode = pb::UpdateMode::try_from(mode?).unwrap_or(pb::UpdateMode::Upsert);
    match mode {
        pb::UpdateMode::Upsert => None,
        pb::UpdateMode::InsertOnly => Some("insert_only".to_string()),
        pb::UpdateMode::UpdateOnly => Some("update_only".to_string()),
    }
}

/// A present filter or shard-key selector stands for "given", so the
/// executor answers 501 as over REST.
fn given<T>(v: Option<&T>) -> Option<Value> {
    v.map(|_| Value::Bool(true))
}

/// An insert of points as `PointInsert`, with the unsupported options kept for the executor.
fn insert_from_grpc(
    points: &[pb::PointStruct],
    shard_key: Option<&pb::ShardKeySelector>,
    update_filter: Option<&pb::Filter>,
    mode: Option<i32>,
) -> Result<PointInsert, GatewayError> {
    Ok(PointInsert::List {
        points: points
            .iter()
            .map(point_from_grpc)
            .collect::<Result<_, _>>()?,
        shard_key: given(shard_key),
        update_filter: given(update_filter),
        update_mode: update_mode(mode),
    })
}

/// A selector; an absent one is Qdrant's "either points or filter" error.
pub fn selector_from_grpc(s: Option<&pb::PointsSelector>) -> Result<PointsSelector, GatewayError> {
    use pb::points_selector::PointsSelectorOneOf;
    match s.and_then(|s| s.points_selector_one_of.as_ref()) {
        Some(PointsSelectorOneOf::Points(list)) => Ok(PointsSelector::Ids {
            points: ids_from_grpc(&list.ids)?,
        }),
        Some(PointsSelectorOneOf::Filter(f)) => Ok(PointsSelector::Filter {
            filter: Box::new(filter_from_grpc(f)?),
        }),
        None => Err(GatewayError::BadRequest(
            "Either points or filter must be provided".to_string(),
        )),
    }
}

/// The optional selector of the payload and vector operations as the
/// model's `points` / `filter` pair.
fn points_or_filter(
    s: Option<&pb::PointsSelector>,
) -> Result<(Option<Vec<Value>>, Option<Filter>), GatewayError> {
    if s.and_then(|s| s.points_selector_one_of.as_ref()).is_none() {
        return Ok((None, None));
    }
    Ok(match selector_from_grpc(s)? {
        PointsSelector::Ids { points } => (Some(points), None),
        PointsSelector::Filter { filter } => (None, Some(*filter)),
    })
}

/// `set_payload` or `overwrite_payload` as the model's `SetPayload`.
fn set_payload(
    payload: &std::collections::HashMap<String, pb::Value>,
    selector: Option<&pb::PointsSelector>,
    key: Option<&String>,
) -> Result<SetPayload, GatewayError> {
    let (points, filter) = points_or_filter(selector)?;
    Ok(SetPayload {
        payload: payload_to_map(payload),
        points,
        filter,
        key: key.cloned(),
    })
}

/// `update_vectors` as the model's `UpdateVectors`.
fn update_vectors(
    points: &[pb::PointVectors],
    update_filter: Option<&pb::Filter>,
) -> Result<UpdateVectors, GatewayError> {
    Ok(UpdateVectors {
        points: points
            .iter()
            .map(|p| {
                Ok(PointVectors {
                    id: id_from_grpc(p.id.as_ref())?,
                    vector: vectors_from_grpc(p.vectors.as_ref())?,
                })
            })
            .collect::<Result<_, GatewayError>>()?,
        update_filter: given(update_filter),
    })
}

/// `delete_vectors` as the model's `DeleteVectors`.
fn delete_vectors(
    selector: Option<&pb::PointsSelector>,
    names: Option<&pb::VectorsSelector>,
) -> Result<DeleteVectors, GatewayError> {
    let (points, filter) = points_or_filter(selector)?;
    Ok(DeleteVectors {
        points,
        filter,
        vector: names.map(|n| n.names.clone()).unwrap_or_default(),
    })
}

/// `UpsertPoints`.
pub fn upsert_from_grpc(r: &pb::UpsertPoints) -> Result<UpdateOperation, GatewayError> {
    Ok(UpdateOperation::Upsert {
        upsert: insert_from_grpc(
            &r.points,
            r.shard_key_selector.as_ref(),
            r.update_filter.as_ref(),
            r.update_mode,
        )?,
    })
}

/// `DeletePoints`.
pub fn delete_from_grpc(r: &pb::DeletePoints) -> Result<UpdateOperation, GatewayError> {
    Ok(UpdateOperation::Delete {
        delete: selector_from_grpc(r.points.as_ref())?,
    })
}

/// `SetPayload` (`overwrite: false`) and `OverwritePayload`.
pub fn set_payload_from_grpc(
    r: &pb::SetPayloadPoints,
    overwrite: bool,
) -> Result<UpdateOperation, GatewayError> {
    let set = set_payload(&r.payload, r.points_selector.as_ref(), r.key.as_ref())?;
    Ok(if overwrite {
        UpdateOperation::OverwritePayload {
            overwrite_payload: set,
        }
    } else {
        UpdateOperation::SetPayload { set_payload: set }
    })
}

/// `DeletePayload`.
pub fn delete_payload_from_grpc(
    r: &pb::DeletePayloadPoints,
) -> Result<UpdateOperation, GatewayError> {
    let (points, filter) = points_or_filter(r.points_selector.as_ref())?;
    Ok(UpdateOperation::DeletePayload {
        delete_payload: DeletePayload {
            keys: r.keys.clone(),
            points,
            filter,
        },
    })
}

/// `ClearPayload`.
pub fn clear_payload_from_grpc(
    r: &pb::ClearPayloadPoints,
) -> Result<UpdateOperation, GatewayError> {
    Ok(UpdateOperation::ClearPayload {
        clear_payload: selector_from_grpc(r.points.as_ref())?,
    })
}

/// `UpdateVectors`.
pub fn update_vectors_from_grpc(
    r: &pb::UpdatePointVectors,
) -> Result<UpdateOperation, GatewayError> {
    Ok(UpdateOperation::UpdateVectors {
        update_vectors: update_vectors(&r.points, r.update_filter.as_ref())?,
    })
}

/// `DeleteVectors`.
pub fn delete_vectors_from_grpc(
    r: &pb::DeletePointVectors,
) -> Result<UpdateOperation, GatewayError> {
    Ok(UpdateOperation::DeleteVectors {
        delete_vectors: delete_vectors(r.points_selector.as_ref(), r.vectors.as_ref())?,
    })
}

/// One `UpdateBatch` operation.
pub fn batch_operation_from_grpc(
    op: &pb::PointsUpdateOperation,
) -> Result<UpdateOperation, GatewayError> {
    use pb::points_update_operation::Operation;
    let Some(op) = &op.operation else {
        return Err(GatewayError::BadRequest(
            "Operation is not specified".to_string(),
        ));
    };
    Ok(match op {
        Operation::Upsert(list) => UpdateOperation::Upsert {
            upsert: insert_from_grpc(
                &list.points,
                list.shard_key_selector.as_ref(),
                list.update_filter.as_ref(),
                list.update_mode,
            )?,
        },
        #[allow(deprecated)]
        Operation::DeleteDeprecated(selector) => UpdateOperation::Delete {
            delete: selector_from_grpc(Some(selector))?,
        },
        Operation::DeletePoints(d) => UpdateOperation::Delete {
            delete: selector_from_grpc(d.points.as_ref())?,
        },
        Operation::SetPayload(s) => UpdateOperation::SetPayload {
            set_payload: set_payload(&s.payload, s.points_selector.as_ref(), s.key.as_ref())?,
        },
        Operation::OverwritePayload(s) => UpdateOperation::OverwritePayload {
            overwrite_payload: set_payload(&s.payload, s.points_selector.as_ref(), s.key.as_ref())?,
        },
        Operation::DeletePayload(d) => {
            let (points, filter) = points_or_filter(d.points_selector.as_ref())?;
            UpdateOperation::DeletePayload {
                delete_payload: DeletePayload {
                    keys: d.keys.clone(),
                    points,
                    filter,
                },
            }
        }
        #[allow(deprecated)]
        Operation::ClearPayloadDeprecated(selector) => UpdateOperation::ClearPayload {
            clear_payload: selector_from_grpc(Some(selector))?,
        },
        Operation::ClearPayload(c) => UpdateOperation::ClearPayload {
            clear_payload: selector_from_grpc(c.points.as_ref())?,
        },
        Operation::UpdateVectors(u) => UpdateOperation::UpdateVectors {
            update_vectors: update_vectors(&u.points, u.update_filter.as_ref())?,
        },
        Operation::DeleteVectors(d) => UpdateOperation::DeleteVectors {
            delete_vectors: delete_vectors(d.points_selector.as_ref(), d.vectors.as_ref())?,
        },
    })
}

/// An `UpdateResult` as gRPC sends it.
pub fn update_result_to_grpc(r: &UpdateResult) -> pb::UpdateResult {
    let status = match r.status {
        UpdateStatus::Completed => pb::UpdateStatus::Completed,
        UpdateStatus::Acknowledged => pb::UpdateStatus::Acknowledged,
    };
    pb::UpdateResult {
        operation_id: r.operation_id,
        status: status as i32,
    }
}

/// A `Record` as gRPC's `RetrievedPoint`: a single `vector` for `[""]`,
/// else named vectors (step 5).
pub fn record_to_grpc(r: &Record) -> pb::RetrievedPoint {
    pb::RetrievedPoint {
        id: Some(id_to_grpc(&r.id)),
        payload: r.payload.as_ref().map(map_to_payload).unwrap_or_default(),
        vectors: r.vector.as_ref().map(vector_output_to_grpc),
        shard_key: None,
        order_value: None,
    }
}
