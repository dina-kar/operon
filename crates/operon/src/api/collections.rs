//! Collections, aliases and documents (plan M1.2 Task 11 rule 1): every
//! route goes through `CollectionService`.

use std::collections::BTreeMap;

use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use operon_collection::{DocOp, Document, PatchMode, PrimaryKey, SparseVector};
use operon_query::json::{pk as json_pk, schema as json_schema};
use operon_query::{
    OpResult, Projection, Query, ReadConsistency, ScanAt, ServiceError, StoredDoc, WriteOptions,
    alias_actions_from_json, rejected_op_index,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{ApiError, ApiResult, AppState, parse_json, read_consistency, with_token};

/// The default page of a scroll.
const DEFAULT_SCROLL_LIMIT: usize = 100;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateCollection {
    name: String,
    schema: Value,
    partitions: Option<u32>,
}

/// `POST /v1/namespaces/{ns}/collections`: 201 with the collection, also for
/// a retry-safe repeat.
pub(super) async fn create(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: CreateCollection = parse_json(&body)?;
    let schema = json_schema::from_json(&request.schema)?;
    let info = state
        .collections
        .create_collection(&ns, &request.name, schema, request.partitions)
        .await?;
    Ok((StatusCode::CREATED, axum::Json(info)).into_response())
}

/// `GET /v1/namespaces/{ns}/collections`, sorted by name.
pub(super) async fn list(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
) -> ApiResult {
    let Path(ns) = ns?;
    let collections = state.collections.list_collections(&ns).await?;
    Ok(axum::Json(json!({ "collections": collections })).into_response())
}

/// `GET …/collections/{c}` (a name or an alias).
pub(super) async fn describe(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> ApiResult {
    let Path((ns, name)) = path?;
    let info = state.collections.get_collection(&ns, &name).await?;
    Ok(axum::Json(info).into_response())
}

/// `DELETE …/collections/{c}` (a name, not an alias).
pub(super) async fn drop(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> ApiResult {
    let Path((ns, name)) = path?;
    let dropped = state.collections.drop_collection(&ns, &name).await?;
    Ok(axum::Json(json!({ "dropped": dropped })).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddFields {
    #[serde(default)]
    fields: Vec<Value>,
    #[serde(default)]
    vectors: Vec<Value>,
    #[serde(default)]
    annotations: BTreeMap<String, String>,
}

/// `POST …/collections/{c}/fields`: 200 with the new schema.
pub(super) async fn add_fields(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: AddFields = parse_json(&body)?;
    let fields = request
        .fields
        .iter()
        .map(json_schema::field_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    let vectors = request
        .vectors
        .iter()
        .map(json_schema::vector_from_json)
        .collect::<Result<Vec<_>, _>>()?;
    let schema = state
        .collections
        .add_fields(&ns, &name, fields, vectors, request.annotations)
        .await?;
    Ok(axum::Json(json!({ "schema": json_schema::to_json(&schema) })).into_response())
}

/// `GET …/collections/{c}/versions`, oldest first.
pub(super) async fn versions(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> ApiResult {
    let Path((ns, name)) = path?;
    let versions = state.collections.versions(&ns, &name).await?;
    Ok(axum::Json(json!({ "versions": versions })).into_response())
}

/// `POST …/collections/{c}/scan` (Task 14, D53): body `{"at"?: At}` (an
/// empty body is `{}`), answered with the scan plan and the pin's token in
/// `Operon-Consistency-Token`. Any node serves it: planning reads no tail.
pub(super) async fn scan(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: Value = if body.iter().all(u8::is_ascii_whitespace) {
        json!({})
    } else {
        parse_json(&body)?
    };
    let Value::Object(request) = request else {
        return Err(ApiError::invalid("bad request body: expected an object"));
    };
    if let Some(key) = request.keys().find(|key| *key != "at") {
        return Err(ApiError::invalid(format!(
            "bad request body: unknown field `{key}`, expected `at`"
        )));
    }
    let at = match request.get("at") {
        None => ScanAt::Current,
        Some(at) => ScanAt::from_json(at)?,
    };
    let plan = state.collections.scan_plan(&ns, &name, at).await?;
    let token = plan.pin.token.clone();
    Ok(with_token(axum::Json(plan).into_response(), &token))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Aliases {
    actions: Value,
}

/// `POST /v1/namespaces/{ns}/aliases`: the actions, atomically.
pub(super) async fn aliases(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: Aliases = parse_json(&body)?;
    let actions = alias_actions_from_json(&request.actions)?;
    state.collections.update_aliases(&ns, actions).await?;
    Ok(axum::Json(json!({})).into_response())
}

// ----- Documents -----

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    ops: Vec<Value>,
    #[serde(default)]
    report_existence: bool,
}

/// The error of op `i`, with `"index": i` (rule 1).
fn op_error(i: usize, err: ServiceError) -> ApiError {
    ApiError::from(err).with("index", i)
}

/// `POST …/collections/{c}/documents`: an atomic write (Ruling 16).
pub(super) async fn write(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: Write = parse_json(&body)?;
    let ops = request
        .ops
        .iter()
        .enumerate()
        .map(|(i, op)| op_from_json(i, op).map_err(|err| op_error(i, err)))
        .collect::<Result<Vec<_>, _>>()?;
    let opts = WriteOptions {
        report_existence: request.report_existence,
        atomic: true,
    };
    let result = state
        .collections
        .write(&ns, &name, ops, opts)
        .await
        .map_err(|err| match rejected_op_index(&err) {
            Some(i) => op_error(i, err),
            None => ApiError::from(err),
        })?;
    let mut results = Vec::with_capacity(result.results.len());
    for (i, op) in result.results.iter().enumerate() {
        results.push(match op {
            OpResult::Created => "created",
            OpResult::Updated => "updated",
            OpResult::Deleted => "deleted",
            OpResult::NotFound => "not_found",
            OpResult::Noop => "noop",
            OpResult::Accepted => "accepted",
            // The writer refused an op the validation passed: the schema
            // changed in between. The request fails with that op's error.
            OpResult::Rejected(err) => return Err(op_error(i, err.clone())),
        });
    }
    let response = axum::Json(json!({
        "token": result.token.to_string(),
        "results": results,
        "positions": result.positions,
    }))
    .into_response();
    Ok(with_token(response, &result.token))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GetDocuments {
    #[serde(with = "operon_query::json::pk::vec")]
    ids: Vec<PrimaryKey>,
    #[serde(default)]
    select: Projection,
    consistency: Option<ReadConsistency>,
}

/// `POST …/collections/{c}/documents/get`: the documents in request order,
/// `null` for a missing id.
pub(super) async fn get_documents(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: GetDocuments = parse_json(&body)?;
    let consistency = read_consistency(&headers, request.consistency)?;
    let (docs, token) = state
        .collections
        .get_with_token(&ns, &name, &request.ids, &request.select, consistency)
        .await?;
    let documents: Vec<Value> = docs
        .iter()
        .map(|doc| doc.as_ref().map_or(Value::Null, stored_doc_json))
        .collect();
    let response = axum::Json(json!({
        "documents": documents,
        "read_token": token.to_string(),
    }))
    .into_response();
    Ok(with_token(response, &token))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scroll {
    filter: Option<Query>,
    #[serde(default, with = "operon_query::json::pk::opt")]
    after: Option<PrimaryKey>,
    limit: Option<usize>,
    #[serde(default)]
    select: Projection,
    consistency: Option<ReadConsistency>,
}

/// `POST …/collections/{c}/documents/scroll`: the next page in primary-key
/// order, and the id to continue after.
pub(super) async fn scroll(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: Scroll = parse_json(&body)?;
    let consistency = read_consistency(&headers, request.consistency)?;
    let ((docs, next), token) = state
        .collections
        .scroll_with_token(
            &ns,
            &name,
            request.filter,
            request.after,
            request.limit.unwrap_or(DEFAULT_SCROLL_LIMIT),
            &request.select,
            consistency,
        )
        .await?;
    let documents: Vec<Value> = docs.iter().map(stored_doc_json).collect();
    let response = axum::Json(json!({
        "documents": documents,
        "next": next.as_ref().map_or(Value::Null, json_pk::to_json),
        "read_token": token.to_string(),
    }))
    .into_response();
    Ok(with_token(response, &token))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Count {
    filter: Option<Query>,
    consistency: Option<ReadConsistency>,
}

/// `POST …/collections/{c}/documents/count`.
pub(super) async fn count(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, name)), body) = (path?, body?);
    let request: Count = parse_json(&body)?;
    let consistency = read_consistency(&headers, request.consistency)?;
    let (count, token) = state
        .collections
        .count_with_token(&ns, &name, request.filter, consistency)
        .await?;
    let response = axum::Json(json!({
        "count": count,
        "read_token": token.to_string(),
    }))
    .into_response();
    Ok(with_token(response, &token))
}

/// A stored document's native JSON: its serde form with the key under
/// `"id"` (`{"id", "source", "vectors", "sparse_vectors"?, "fields",
/// "seq_no", "partition"}`).
fn stored_doc_json(doc: &StoredDoc) -> Value {
    match serde_json::to_value(doc) {
        // Renamed in place: maps keep insertion order (M1.3 row E58).
        Ok(Value::Object(object)) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| match key.as_str() {
                    "pk" => ("id".to_string(), value),
                    _ => (key, value),
                })
                .collect(),
        ),
        Ok(other) => other,
        Err(_) => Value::Null,
    }
}

// ----- Op JSON (rule 1) -----

fn op_invalid(i: usize, message: impl std::fmt::Display) -> ServiceError {
    ServiceError::InvalidArgument(format!("op {i}: {message}"))
}

/// The one-key object `value`, and its keys checked against `allowed`.
fn object_of<'a>(
    i: usize,
    what: &str,
    value: &'a Value,
    allowed: &[&str],
) -> Result<&'a Map<String, Value>, ServiceError> {
    let object = value
        .as_object()
        .ok_or_else(|| op_invalid(i, format!("{what} must be an object")))?;
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(op_invalid(i, format!("unknown key {key} in {what}")));
    }
    Ok(object)
}

/// `{"upsert": Doc}`, `{"delete": {"id"}}` or `{"patch": {…}}`.
fn op_from_json(i: usize, value: &Value) -> Result<DocOp, ServiceError> {
    let (kind, body) = value
        .as_object()
        .filter(|object| object.len() == 1)
        .and_then(|object| object.iter().next())
        .ok_or_else(|| {
            op_invalid(
                i,
                "an op must be {\"upsert\": …}, {\"delete\": …} or {\"patch\": …}",
            )
        })?;
    match kind.as_str() {
        "upsert" => Ok(DocOp::Upsert(doc_from_json(i, "upsert", body, None)?)),
        "delete" => {
            let body = object_of(i, "delete", body, &["id"])?;
            Ok(DocOp::Delete(id_of(i, "delete", body)?))
        }
        "patch" => patch_from_json(i, body),
        other => Err(op_invalid(i, format!("unknown op {other}"))),
    }
}

fn id_of(i: usize, what: &str, body: &Map<String, Value>) -> Result<PrimaryKey, ServiceError> {
    let id = body
        .get("id")
        .ok_or_else(|| op_invalid(i, format!("{what}.id is required")))?;
    json_pk::from_json(id).map_err(|err| match err {
        ServiceError::InvalidArgument(message) => op_invalid(i, format!("{what}.id: {message}")),
        other => other,
    })
}

fn source_of(
    i: usize,
    what: &str,
    body: &Map<String, Value>,
) -> Result<Map<String, Value>, ServiceError> {
    match body.get("source") {
        None => Ok(Map::new()),
        Some(Value::Object(source)) => Ok(source.clone()),
        Some(_) => Err(op_invalid(i, format!("{what}.source must be an object"))),
    }
}

fn dense(i: usize, name: &str, value: &Value) -> Result<Vec<f32>, ServiceError> {
    serde_json::from_value(value.clone())
        .map_err(|_| op_invalid(i, format!("vector {name} must be a list of numbers")))
}

fn sparse(i: usize, name: &str, value: &Value) -> Result<SparseVector, ServiceError> {
    serde_json::from_value(value.clone())
        .map_err(|err| op_invalid(i, format!("sparse vector {name}: {err}")))
}

/// The entries of the object under `key`, each mapped by `entry`.
fn named<T>(
    i: usize,
    what: &str,
    body: &Map<String, Value>,
    key: &str,
    mut entry: impl FnMut(&str, &Value) -> Result<T, ServiceError>,
) -> Result<BTreeMap<String, T>, ServiceError> {
    match body.get(key) {
        None => Ok(BTreeMap::new()),
        Some(Value::Object(entries)) => entries
            .iter()
            .map(|(name, value)| Ok((name.clone(), entry(name, value)?)))
            .collect(),
        Some(_) => Err(op_invalid(i, format!("{what}.{key} must be an object"))),
    }
}

/// `{"id", "source"?, "vectors"?, "sparse_vectors"?}`; `default_id` stands
/// in for a missing id (a patch's `upsert` document).
fn doc_from_json(
    i: usize,
    what: &str,
    value: &Value,
    default_id: Option<&PrimaryKey>,
) -> Result<Document, ServiceError> {
    let body = object_of(
        i,
        what,
        value,
        &["id", "source", "vectors", "sparse_vectors"],
    )?;
    let pk = match (body.contains_key("id"), default_id) {
        (false, Some(id)) => id.clone(),
        _ => id_of(i, what, body)?,
    };
    Ok(Document {
        pk,
        source: source_of(i, what, body)?,
        vectors: named(i, what, body, "vectors", |name, v| dense(i, name, v))?,
        sparse_vectors: named(i, what, body, "sparse_vectors", |name, v| {
            sparse(i, name, v)
        })?,
    })
}

fn patch_from_json(i: usize, value: &Value) -> Result<DocOp, ServiceError> {
    let body = object_of(
        i,
        "patch",
        value,
        &[
            "id",
            "mode",
            "source",
            "delete_keys",
            "vectors",
            "sparse_vectors",
            "upsert",
        ],
    )?;
    let pk = id_of(i, "patch", body)?;
    let mode = match body.get("mode") {
        None => PatchMode::MergeDeep,
        Some(mode) => match mode.as_str() {
            Some("merge_deep") => PatchMode::MergeDeep,
            Some("merge_top") => PatchMode::MergeTop,
            Some("replace") => PatchMode::Replace,
            _ => {
                return Err(op_invalid(
                    i,
                    "patch.mode must be \"merge_deep\", \"merge_top\" or \"replace\"",
                ));
            }
        },
    };
    let delete_keys = match body.get("delete_keys") {
        None => Vec::new(),
        Some(keys) => serde_json::from_value(keys.clone())
            .map_err(|_| op_invalid(i, "patch.delete_keys must be a list of strings"))?,
    };
    let vectors = named(i, "patch", body, "vectors", |name, v| {
        Ok(if v.is_null() {
            None
        } else {
            Some(dense(i, name, v)?)
        })
    })?;
    let sparse_vectors = named(i, "patch", body, "sparse_vectors", |name, v| {
        Ok(if v.is_null() {
            None
        } else {
            Some(sparse(i, name, v)?)
        })
    })?;
    let upsert = match body.get("upsert") {
        None | Some(Value::Null) => None,
        Some(doc) => Some(doc_from_json(i, "patch.upsert", doc, Some(&pk))?),
    };
    Ok(DocOp::Patch {
        source: source_of(i, "patch", body)?,
        pk,
        mode,
        delete_keys,
        vectors,
        sparse_vectors,
        upsert,
    })
}
