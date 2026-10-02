//! Point writes (Task 5): every write route becomes one or more
//! [`UpdateOperation`]s, each planned into `DocOp`s, and a request is one
//! atomic `write` call (M1.2 Ruling 16), unless it holds writes by filter.
//!
//! A write by filter runs as the collection service's native
//! `delete_by_filter` or `patch_by_filter` (D87, M1.5 Task 9a): one pin,
//! atomic batches, followed by its cursor. Two kinds keep the
//! read-modify-write of Ruling 12 over scroll-resolved keys, written in
//! chunks (M1.4 row E4's exceptions): `set_payload` with a `key` that is not
//! a plain dotted path or with an object or `null` value, and
//! `overwrite_payload` with a `key` or `delete_payload` of paths with `[]`
//! or `[n]`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use loams_collection::{ConsistencyToken, DocOp, Document, MAX_WRITE_OPS, PatchMode, PrimaryKey};
use loams_query::{
    CollectionInfo, FilterWriteOptions, OpResult, PatchSpec, Projection, Query, ReadConsistency,
    ServiceError, SourceFilter, StoredDoc, WriteOptions, WriteResult,
};
use serde_json::{Map, Value};

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::compile_filter;
use crate::ids::{PointId, pk_to_json};
use crate::jsonpath::{JsonPath, PathItem, value_overwrite, value_remove, value_set};
use crate::model::common::{UpdateResult, UpdateStatus, VectorInput};
use crate::model::filter::Filter;
use crate::model::points::{
    BatchVectors, DeletePayload, DeleteVectors, Payload, PointInsert, PointsSelector, SetPayload,
    UpdateOperation, UpdateVectors, VectorStruct,
};
use crate::scoring::{CheckedVectors, check_named, not_a_vector};

/// Ids per `scroll` page of a read-modify-write by filter (Ruling 13).
const FILTER_PAGE: usize = 1_000;
/// How long a write by filter waits out backpressure without a request
/// timeout (E4; the native filter writes' default deadline too).
const FILTER_RETRY_LIMIT: Duration = Duration::from_secs(60);
/// The shortest pause before retrying a refused chunk.
const MIN_RETRY_PAUSE: Duration = Duration::from_millis(50);

/// One operation's ops, the ops that must address an existing point (by op
/// index), and whether its ops were resolved from a filter; or its native
/// write by filter.
#[derive(Clone, Debug, Default)]
pub(crate) struct Planned {
    pub ops: Vec<DocOp>,
    pub must_exist: Vec<(usize, PrimaryKey)>,
    pub from_filter: bool,
    pub native: Option<NativeFilter>,
}

/// A write by filter the collection service runs natively (D87).
#[derive(Clone, Debug)]
pub(crate) struct NativeFilter {
    pub query: Query,
    pub op: NativeOp,
}

/// What an operation writes to each point it addresses.
#[derive(Clone, Debug)]
pub(crate) enum NativeOp {
    Delete,
    Patch(PatchSpec),
}

impl NativeOp {
    /// A patch of `source` in `mode`, removing `delete_keys`.
    fn patch(mode: PatchMode, source: Map<String, Value>, delete_keys: Vec<String>) -> Self {
        NativeOp::Patch(PatchSpec {
            mode,
            source,
            delete_keys,
            vectors: BTreeMap::new(),
            sparse_vectors: BTreeMap::new(),
        })
    }

    /// The op of point `pk`.
    fn doc_op(&self, pk: PrimaryKey) -> DocOp {
        match self {
            NativeOp::Delete => DocOp::Delete(pk),
            NativeOp::Patch(spec) => spec.op(pk),
        }
    }
}

/// The points an operation addresses.
enum Target {
    Ids(Vec<PrimaryKey>),
    Filter(Box<Filter>),
}

/// JSON ids as keys.
fn parse_ids(ids: &[Value]) -> Result<Vec<PrimaryKey>, GatewayError> {
    ids.iter()
        .map(|v| PointId::from_json(v).map(PointId::to_pk))
        .collect()
}

/// The target of an operation: its ids, or its filter.
fn target(points: Option<Vec<Value>>, filter: Option<Filter>) -> Result<Target, GatewayError> {
    match (points, filter) {
        (Some(points), None) => Ok(Target::Ids(parse_ids(&points)?)),
        (None, Some(filter)) => Ok(Target::Filter(Box::new(filter))),
        _ => Err(GatewayError::BadRequest(
            "Either points or filter must be provided".to_string(),
        )),
    }
}

/// A selector's target.
fn selector_target(selector: PointsSelector) -> Result<Target, GatewayError> {
    match selector {
        PointsSelector::Ids { points } => Ok(Target::Ids(parse_ids(&points)?)),
        PointsSelector::Filter { filter } => Ok(Target::Filter(filter)),
    }
}

/// A patch with nothing but `mode` and `source`.
fn patch(pk: PrimaryKey, mode: PatchMode, source: Map<String, Value>) -> DocOp {
    DocOp::Patch {
        pk,
        mode,
        source,
        delete_keys: Vec::new(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert: None,
    }
}

/// A point id as Qdrant's messages show it: a number, or a UUID unquoted.
fn shown(pk: &PrimaryKey) -> String {
    match pk_to_json(pk) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

// ----- planning -----

/// The ops of one operation (steps 2–6). Reads happen here, before any
/// write of the request, so a request that fails planning writes nothing.
pub(crate) async fn plan_operation(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    op: UpdateOperation,
) -> Result<Planned, GatewayError> {
    let schema = &info.schema;
    match op {
        UpdateOperation::Upsert { upsert } => Ok(Planned {
            ops: plan_upsert(info, upsert)?,
            ..Planned::default()
        }),
        UpdateOperation::Delete { delete } => {
            let target = selector_target(delete)?;
            for_each_key(info, target, false, NativeOp::Delete)
        }
        UpdateOperation::SetPayload { set_payload } => plan_set(gw, ctx, info, set_payload).await,
        UpdateOperation::OverwritePayload { overwrite_payload } => {
            let SetPayload {
                payload,
                points,
                filter,
                key,
            } = overwrite_payload;
            let target = target(points, filter)?;
            match key {
                None => for_each_key(
                    info,
                    target,
                    true,
                    NativeOp::patch(PatchMode::Replace, payload, Vec::new()),
                ),
                Some(key) => {
                    let path: JsonPath = key.parse()?;
                    rewrite(gw, ctx, info, target, |source| {
                        value_overwrite(&path, source, &payload);
                    })
                    .await
                }
            }
        }
        UpdateOperation::DeletePayload { delete_payload } => {
            plan_delete_payload(gw, ctx, info, delete_payload).await
        }
        UpdateOperation::ClearPayload { clear_payload } => {
            let target = selector_target(clear_payload)?;
            for_each_key(
                info,
                target,
                true,
                NativeOp::patch(PatchMode::Replace, Map::new(), Vec::new()),
            )
        }
        UpdateOperation::UpdateVectors { update_vectors } => {
            let UpdateVectors {
                points,
                update_filter,
            } = update_vectors;
            if update_filter.is_some() {
                return Err(GatewayError::Unsupported("update_filter".to_string()));
            }
            let mut planned = Planned::default();
            for point in points {
                let pk = PointId::from_json(&point.id)?.to_pk();
                let checked = checked_vectors(schema, point.vector)?;
                planned.must_exist.push((planned.ops.len(), pk.clone()));
                planned.ops.push(DocOp::Patch {
                    pk,
                    mode: PatchMode::MergeTop,
                    source: Map::new(),
                    delete_keys: Vec::new(),
                    vectors: checked
                        .dense
                        .into_iter()
                        .map(|(n, v)| (n, Some(v)))
                        .collect(),
                    sparse_vectors: checked
                        .sparse
                        .into_iter()
                        .map(|(n, v)| (n, Some(v)))
                        .collect(),
                    upsert: None,
                });
            }
            Ok(planned)
        }
        UpdateOperation::DeleteVectors { delete_vectors } => {
            let DeleteVectors {
                points,
                filter,
                vector,
            } = delete_vectors;
            let mut vectors = BTreeMap::new();
            let mut sparse_vectors = BTreeMap::new();
            for name in vector {
                if schema.vectors.iter().any(|s| s.name == name) {
                    vectors.insert(name, None);
                } else if schema.sparse_vectors.iter().any(|s| s.name == name) {
                    sparse_vectors.insert(name, None);
                } else {
                    return Err(GatewayError::BadRequest(format!(
                        "Not existing vector name error: {name}"
                    )));
                }
            }
            let target = target(points, filter)?;
            let op = NativeOp::Patch(PatchSpec {
                mode: PatchMode::MergeTop,
                source: Map::new(),
                delete_keys: Vec::new(),
                vectors,
                sparse_vectors,
            });
            for_each_key(info, target, true, op)
        }
    }
}

/// Step 2: one `Upsert` per point.
fn plan_upsert(info: &CollectionInfo, insert: PointInsert) -> Result<Vec<DocOp>, GatewayError> {
    let schema = &info.schema;
    let (shard_key, update_filter, update_mode) = match &insert {
        PointInsert::List {
            shard_key,
            update_filter,
            update_mode,
            ..
        }
        | PointInsert::Batch {
            shard_key,
            update_filter,
            update_mode,
            ..
        } => (shard_key, update_filter, update_mode),
    };
    if let Some(mode) = update_mode.as_deref().filter(|m| *m != "upsert") {
        return Err(GatewayError::Unsupported(format!("update_mode {mode}")));
    }
    if update_filter.is_some() {
        return Err(GatewayError::Unsupported("update_filter".to_string()));
    }
    if shard_key.is_some() {
        return Err(GatewayError::Unsupported("shard keys".to_string()));
    }
    let doc = |id: &Value, checked: CheckedVectors, payload: Option<Payload>| {
        Ok::<_, GatewayError>(DocOp::Upsert(Document {
            pk: PointId::from_json(id)?.to_pk(),
            source: payload.unwrap_or_default(),
            vectors: checked.dense,
            sparse_vectors: checked.sparse,
        }))
    };
    match insert {
        PointInsert::List { points, .. } => points
            .into_iter()
            .map(|p| {
                let checked = checked_vectors(schema, p.vector)?;
                doc(&p.id, checked, p.payload)
            })
            .collect(),
        PointInsert::Batch { batch, .. } => {
            let n = batch.ids.len();
            let mismatch = || {
                GatewayError::BadRequest(
                    "Number of ids, vectors and payloads must match".to_string(),
                )
            };
            if batch.payloads.as_ref().is_some_and(|p| p.len() != n) {
                return Err(mismatch());
            }
            let per_point: Vec<VectorStruct> = match batch.vectors {
                BatchVectors::Single(vectors) => {
                    if vectors.len() != n {
                        return Err(mismatch());
                    }
                    vectors.into_iter().map(VectorStruct::Single).collect()
                }
                BatchVectors::Named(named) => {
                    let mut points: Vec<BTreeMap<String, VectorInput>> =
                        (0..n).map(|_| BTreeMap::new()).collect();
                    for (name, values) in named {
                        if values.len() != n {
                            return Err(mismatch());
                        }
                        for (point, value) in points.iter_mut().zip(values) {
                            point.insert(name.clone(), value);
                        }
                    }
                    points.into_iter().map(VectorStruct::Named).collect()
                }
                BatchVectors::Other(Value::Array(_)) => {
                    return Err(GatewayError::Unsupported("multivectors".to_string()));
                }
                BatchVectors::Other(Value::Object(_)) => {
                    return Err(GatewayError::Unsupported("inference objects".to_string()));
                }
                BatchVectors::Other(_) => return Err(not_a_vector()),
            };
            let mut payloads = batch.payloads.unwrap_or_default().into_iter();
            batch
                .ids
                .iter()
                .zip(per_point)
                .map(|(id, vector)| {
                    let checked = checked_vectors(schema, vector)?;
                    doc(id, checked, payloads.next().flatten())
                })
                .collect()
        }
    }
}

/// A point's vectors checked against the schema; `Single(v)` is `{"": v}`
/// and an empty map means no vectors.
fn checked_vectors(
    schema: &loams_collection::CollectionSchema,
    vector: VectorStruct,
) -> Result<CheckedVectors, GatewayError> {
    match vector {
        VectorStruct::Single(v) => check_named(
            schema,
            BTreeMap::from([(String::new(), VectorInput::Dense(v))]),
        ),
        VectorStruct::Named(named) => check_named(schema, named),
        VectorStruct::Multi(_) => Err(GatewayError::Unsupported("multivectors".to_string())),
        VectorStruct::Object(Value::Object(_)) => {
            Err(GatewayError::Unsupported("inference objects".to_string()))
        }
        VectorStruct::Object(_) => Err(not_a_vector()),
    }
}

/// `set_payload`: a `MergeTop` patch without `key` (a `null` value removes
/// its key, as Qdrant's `merge_map` does), a read-modify-write with it
/// (Ruling 12).
async fn plan_set(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    set: SetPayload,
) -> Result<Planned, GatewayError> {
    let SetPayload {
        payload,
        points,
        filter,
        key,
    } = set;
    let target = target(points, filter)?;
    let path = key.map(|k| k.parse::<JsonPath>()).transpose()?;
    // M1.4 row E4 (a): by filter, a plain dotted `key` with no object or
    // `null` value is a `MergeDeep` patch of the payload nested under the
    // path, which `value_set` agrees with (a `null` removes its key there,
    // an object replaces rather than merges).
    if let (Some(path), Target::Filter(_)) = (&path, &target)
        && plain_path(path)
        && payload.values().all(|v| !v.is_object() && !v.is_null())
    {
        let mut source = payload;
        for item in path.rest.iter().rev() {
            if let PathItem::Key(k) = item {
                source = Map::from_iter([(k.clone(), Value::Object(source))]);
            }
        }
        let source = Map::from_iter([(path.first.clone(), Value::Object(source))]);
        return for_each_key(
            info,
            target,
            true,
            NativeOp::patch(PatchMode::MergeDeep, source, Vec::new()),
        );
    }
    // A `null` under a key `delete_keys` cannot address (empty, or holding
    // a '.') needs the read-modify-write.
    let unaddressable = payload
        .iter()
        .any(|(k, v)| v.is_null() && (k.is_empty() || k.contains('.')));
    if path.is_some() || unaddressable {
        return rewrite(gw, ctx, info, target, |source| {
            value_set(path.as_ref(), source, &payload);
        })
        .await;
    }
    let (nulls, source): (Map<String, Value>, Map<String, Value>) =
        payload.into_iter().partition(|(_, v)| v.is_null());
    let delete_keys: Vec<String> = nulls.into_iter().map(|(k, _)| k).collect();
    for_each_key(
        info,
        target,
        true,
        NativeOp::patch(PatchMode::MergeTop, source, delete_keys),
    )
}

/// A path of keys only, none empty or holding a '.'.
fn plain_path(p: &JsonPath) -> bool {
    let key_ok = |k: &str| !k.is_empty() && !k.contains('.');
    key_ok(&p.first)
        && p.rest.iter().all(|item| match item {
            PathItem::Key(k) => key_ok(k),
            PathItem::Index(_) | PathItem::Wildcard => false,
        })
}

/// `delete_payload`: `delete_keys` when every key is a plain dotted path,
/// else a read-modify-write with `value_remove` (Ruling 12).
async fn plan_delete_payload(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    delete: DeletePayload,
) -> Result<Planned, GatewayError> {
    let DeletePayload {
        keys,
        points,
        filter,
    } = delete;
    let target = target(points, filter)?;
    let paths = keys
        .iter()
        .map(|k| k.parse::<JsonPath>())
        .collect::<Result<Vec<_>, _>>()?;
    if paths.iter().all(plain_path) {
        let delete_keys = paths
            .iter()
            .map(JsonPath::normalized)
            .collect::<Result<Vec<_>, _>>()?;
        return for_each_key(
            info,
            target,
            true,
            NativeOp::patch(PatchMode::MergeTop, Map::new(), delete_keys),
        );
    }
    rewrite(gw, ctx, info, target, |source| {
        for path in &paths {
            value_remove(path, source);
        }
    })
    .await
}

/// One op per addressed key: by ids, `must_exist` records each op when
/// `must_exist` is set; by filter, the collection service's native write
/// by filter (D87).
fn for_each_key(
    info: &CollectionInfo,
    target: Target,
    must_exist: bool,
    op: NativeOp,
) -> Result<Planned, GatewayError> {
    match target {
        Target::Ids(pks) => Ok(Planned {
            must_exist: if must_exist {
                pks.iter().cloned().enumerate().collect()
            } else {
                Vec::new()
            },
            ops: pks.into_iter().map(|pk| op.doc_op(pk)).collect(),
            ..Planned::default()
        }),
        Target::Filter(filter) => Ok(Planned {
            native: Some(NativeFilter {
                query: compile_filter(&filter, &info.schema)?,
                op,
            }),
            ..Planned::default()
        }),
    }
}

/// Read-modify-write (Ruling 12): reads each addressed point's payload,
/// applies `edit` and writes `Patch { mode: Replace }`. A missing id gets
/// a no-op `Replace` patch, so the write reports it missing.
async fn rewrite(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    target: Target,
    edit: impl Fn(&mut Map<String, Value>),
) -> Result<Planned, GatewayError> {
    let replace = |pk: PrimaryKey, source: Option<Map<String, Value>>| {
        let mut source = source.unwrap_or_default();
        edit(&mut source);
        patch(pk, PatchMode::Replace, source)
    };
    match target {
        Target::Ids(pks) => {
            let select = Projection {
                source: SourceFilter::All,
                vectors: Vec::new(),
                fields: Vec::new(),
            };
            let docs = gw
                .service()
                .get(&ctx.ns, &info.name, &pks, &select, ctx.consistency.clone())
                .await?;
            let mut planned = Planned::default();
            for (i, (pk, doc)) in pks.into_iter().zip(docs).enumerate() {
                planned.must_exist.push((i, pk.clone()));
                planned.ops.push(match doc {
                    Some(doc) => replace(pk, doc.source),
                    None => patch(pk, PatchMode::Replace, Map::new()),
                });
            }
            Ok(planned)
        }
        Target::Filter(filter) => {
            let docs = scroll_filter(gw, ctx, info, &filter, true).await?;
            Ok(Planned {
                ops: docs.into_iter().map(|d| replace(d.pk, d.source)).collect(),
                from_filter: true,
                ..Planned::default()
            })
        }
    }
}

/// Every point matching `filter`, for a read-modify-write by filter (Ruling
/// 13, E4): pages of 1,000, the first at the request's consistency, the
/// later ones at least at the first page's read token.
async fn scroll_filter(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    filter: &Filter,
    with_source: bool,
) -> Result<Vec<StoredDoc>, GatewayError> {
    let query = compile_filter(filter, &info.schema)?;
    let select = Projection {
        source: if with_source {
            SourceFilter::All
        } else {
            SourceFilter::None
        },
        vectors: Vec::new(),
        fields: Vec::new(),
    };
    let page = FILTER_PAGE.min(gw.service().config().max_scroll_limit.max(1));
    let mut consistency = ctx.consistency.clone();
    let mut after = None;
    let mut first = true;
    let mut out = Vec::new();
    loop {
        let ((docs, next), token) = gw
            .service()
            .scroll_with_token(
                &ctx.ns,
                &info.name,
                Some(query.clone()),
                after.take(),
                page,
                &select,
                consistency.clone(),
            )
            .await?;
        if first {
            consistency = ReadConsistency::AtLeast(token);
            first = false;
        }
        out.extend(docs);
        match next {
            Some(key) => after = Some(key),
            None => return Ok(out),
        }
    }
}

// ----- execution -----

/// Step 7: the operations in order. The ops of consecutive operations
/// without a native write by filter are written as below
/// ([`write_planned`]); each native write by filter runs on its own
/// ([`run_native`]). The answer's token covers them all.
pub(crate) async fn execute(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    planned: Vec<Planned>,
    wait: bool,
) -> Result<(UpdateResult, ConsistencyToken), GatewayError> {
    // Owner ruling O1: the service's limit holds for the ops a request
    // lists itself (PR #51 review); name the way out.
    let listed: usize = planned
        .iter()
        .filter(|p| !p.from_filter)
        .map(|p| p.ops.len())
        .sum();
    if listed > MAX_WRITE_OPS {
        return Err(GatewayError::BadRequest(format!(
            "a write request holds at most {MAX_WRITE_OPS} operations, got {listed}; split the batch into smaller requests"
        )));
    }
    let mut token = ConsistencyToken::default();
    let mut group = Vec::new();
    for p in planned {
        match p.native {
            None => group.push(p),
            Some(native) => {
                if !group.is_empty() {
                    let written =
                        Box::pin(write_planned(gw, ctx, info, std::mem::take(&mut group))).await?;
                    token.merge(&written);
                }
                let written = Box::pin(run_native(gw, ctx, info, native)).await?;
                token.merge(&written);
            }
        }
    }
    if !group.is_empty() {
        let written = Box::pin(write_planned(gw, ctx, info, group)).await?;
        token.merge(&written);
    }
    let result = UpdateResult {
        operation_id: token.0.iter().map(|&(_, _, offset)| offset).max(),
        status: if wait {
            UpdateStatus::Completed
        } else {
            UpdateStatus::Acknowledged
        },
    };
    Ok((result, token))
}

/// A native write by filter (D87): `allow_partial`, following the cursor at
/// the same pin until no rows remain. Each call gets what is left of the
/// request's `timeout` (60 s without one); a batch refused for backpressure
/// past it is 429, with the batches already written kept (the listed
/// divergence "filter writes are not atomic").
async fn run_native(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    native: NativeFilter,
) -> Result<ConsistencyToken, GatewayError> {
    let deadline = ctx.started + ctx.timeout.unwrap_or(FILTER_RETRY_LIMIT);
    let mut token = ConsistencyToken::default();
    let mut cursor = None;
    loop {
        let opts = FilterWriteOptions {
            consistency: ctx.consistency.clone(),
            allow_partial: true,
            cursor: cursor.take(),
            deadline: Some(deadline.saturating_duration_since(Instant::now())),
            ..FilterWriteOptions::default()
        };
        let service = gw.service();
        let query = native.query.clone();
        let result = match &native.op {
            NativeOp::Delete => {
                Box::pin(service.delete_by_filter(&ctx.ns, &info.name, query, opts)).await?
            }
            NativeOp::Patch(spec) => {
                Box::pin(service.patch_by_filter(&ctx.ns, &info.name, query, spec.clone(), opts))
                    .await?
            }
        };
        token.merge(&result.token);
        if let Some(retry_after_ms) = result.retry_after_ms {
            return Err(GatewayError::Service(ServiceError::ResourceExhausted {
                message: format!(
                    "a write by filter was refused after some of its batches were written; retry after {} s",
                    retry_after_ms.div_ceil(1000).max(1)
                ),
                retry_after_ms,
            }));
        }
        match result.cursor {
            Some(next) if result.rows_remaining => {
                if Instant::now() >= deadline {
                    return Err(GatewayError::Service(ServiceError::Timeout));
                }
                cursor = Some(next);
            }
            _ => return Ok(token),
        }
    }
}

/// One `write` call for the ops of `planned`, or chunks of
/// `filter_write_chunk` when read-modify-writes by filter make it longer
/// than one chunk. Then step 6's existence and rejection checks.
async fn write_planned(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    planned: Vec<Planned>,
) -> Result<ConsistencyToken, GatewayError> {
    let mut ops = Vec::new();
    let mut must_exist = Vec::new();
    let mut from_filter = false;
    for p in planned {
        let base = ops.len();
        must_exist.extend(p.must_exist.into_iter().map(|(i, pk)| (base + i, pk)));
        from_filter |= p.from_filter;
        ops.extend(p.ops);
    }
    let opts = WriteOptions {
        report_existence: !must_exist.is_empty(),
        atomic: true,
        ..WriteOptions::default()
    };
    let chunk = gw.config().filter_write_chunk.max(1);
    let mut token = ConsistencyToken::default();
    let mut results = Vec::with_capacity(ops.len());
    if ops.is_empty() {
        // Nothing to write (an empty list, or a filter matching nothing).
    } else if ops.len() <= chunk || !from_filter {
        let written = gw.service().write(&ctx.ns, &info.name, ops, opts).await?;
        token = written.token;
        results = written.results;
    } else {
        let deadline = ctx.started + ctx.timeout.unwrap_or(FILTER_RETRY_LIMIT);
        let mut rest = ops;
        let mut first = true;
        while !rest.is_empty() {
            let tail = rest.split_off(chunk.min(rest.len()));
            let part = std::mem::replace(&mut rest, tail);
            let written = write_chunk(gw, ctx, info, part, opts, first, deadline).await?;
            token.merge(&written.token);
            results.extend(written.results);
            first = false;
        }
    }
    if let Some(err) = results.iter().find_map(|r| match r {
        OpResult::Rejected(err) => Some(err.clone()),
        _ => None,
    }) {
        return Err(GatewayError::Service(err));
    }
    if let Some((_, pk)) = must_exist
        .iter()
        .find(|(i, _)| results.get(*i).and_then(OpResult::existed) == Some(false))
    {
        return Err(GatewayError::PointsNotFound(shown(pk)));
    }
    Ok(token)
}

/// One chunk of a read-modify-write by filter (E4). A chunk after the first that is
/// refused for backpressure is retried after `retry_after_ms` (a refused
/// write appends nothing) while the retry still ends before `deadline`;
/// then the refusal is the answer, and the chunks already written stay.
async fn write_chunk(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    info: &CollectionInfo,
    ops: Vec<DocOp>,
    opts: WriteOptions,
    first: bool,
    deadline: Instant,
) -> Result<WriteResult, GatewayError> {
    loop {
        match gw
            .service()
            .write(&ctx.ns, &info.name, ops.clone(), opts)
            .await
        {
            Err(ServiceError::ResourceExhausted { retry_after_ms, .. }) if !first => {
                let pause = Duration::from_millis(retry_after_ms).max(MIN_RETRY_PAUSE);
                if Instant::now() + pause >= deadline {
                    return Err(GatewayError::Service(ServiceError::ResourceExhausted {
                        message: format!(
                            "a write by filter was refused after some of its chunks were written; retry after {} s",
                            retry_after_ms.div_ceil(1000)
                        ),
                        retry_after_ms,
                    }));
                }
                tokio::time::sleep(pause).await;
            }
            other => return other.map_err(GatewayError::from),
        }
    }
}

/// Plans every operation, then executes them as one request: the
/// collection is resolved once (aliases included), and each operation gets
/// the same [`UpdateResult`].
pub(crate) async fn update(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    operations: Vec<UpdateOperation>,
    wait: bool,
) -> Result<(Vec<UpdateResult>, ConsistencyToken), GatewayError> {
    let info = gw.service().get_collection(&ctx.ns, &collection).await?;
    let n = operations.len();
    let mut planned = Vec::with_capacity(n);
    for op in operations {
        planned.push(Box::pin(plan_operation(&gw, &ctx, &info, op)).await?);
    }
    let (result, token) = Box::pin(execute(&gw, &ctx, &info, planned, wait)).await?;
    Ok((vec![result; n], token))
}

/// [`update`] for a route that carries one operation.
pub(crate) async fn update_one(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    operation: UpdateOperation,
    wait: bool,
) -> Result<(UpdateResult, ConsistencyToken), GatewayError> {
    let (mut results, token) = update(gw, ctx, collection, vec![operation], wait).await?;
    let result = results.pop().ok_or_else(|| {
        GatewayError::Service(ServiceError::Internal("no update result".to_string()))
    })?;
    Ok((result, token))
}
