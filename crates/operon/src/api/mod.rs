//! The native HTTP/JSON API. Namespaces and streams are addressed by name;
//! record keys and values are base64 (M0.3 plan, Task 7). Collections,
//! documents, queries and SQL go through `CollectionService` (plan M1.2
//! Task 11): [`collections`], [`query`] and [`sql`]; so does the scan plan
//! route (Task 14). The hot routes (plan M1.3 Task 8) are [`hot`], and the
//! internal routes a node calls on a collection's owner are [`internal`].

mod collections;
mod errors;
pub mod hot;
pub mod internal;
mod query;
mod sql;
pub mod streams;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use operon_collection::ConsistencyToken;
use operon_common::meta::{Consistency, MetaStore, Retention, StreamState, TargetRef, WalClass};
use operon_common::{NamespaceId, StreamId};
use operon_hot::{ForwardStats, HotTierImpl, Roles};
use operon_link::{COUNTER_KIND, CounterTable, TargetRegistry};
use operon_log::{FetchRequest, LogReader, LogWriter, Record};
use operon_query::hot::HotLayer;
use operon_query::placement::Placement;
use operon_query::{CollectionService, ReadConsistency};
use operon_store::Store;
use serde::Deserialize;
use serde_json::{Value, json};

pub use errors::ApiError;
pub use streams::{NativeStreamProducer, produce_records};

/// The default `max_bytes` of a fetch: 1 MiB.
const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
/// The largest `max_bytes` a fetch may ask for: 16 MiB. Larger values are
/// lowered to it, so one request cannot read a whole partition into memory.
pub const MAX_FETCH_BYTES: usize = 16 * 1024 * 1024;
/// The largest request body: 16 MiB. Larger bodies get `413` with the usual
/// JSON error body.
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
/// The longest a fetch may long-poll.
const MAX_WAIT: Duration = Duration::from_secs(60);
/// The prefix of implicit stream names (`_collection.<name>.<id>`).
const IMPLICIT_STREAM_PREFIX: &str = "_collection.";

/// `Operon-Consistency-Token` (row 0.21: `CONSISTENCY_TOKEN_HEADER` is mixed
/// case, which `HeaderName` refuses).
pub const CONSISTENCY_TOKEN: HeaderName = HeaderName::from_static("operon-consistency-token");

/// What the handlers share.
#[derive(Clone, Debug)]
pub struct AppState {
    pub meta: Arc<dyn MetaStore>,
    pub writer: LogWriter,
    pub reader: LogReader,
    /// For reading counter tables.
    pub store: Store,
    /// The link targets, by kind: describes every registered link kind.
    pub registry: TargetRegistry,
    /// Collections, documents, queries and SQL (plan M1.2).
    pub collections: Arc<CollectionService>,
    /// This node's hot tier; `None` with `--hot off` (plan M1.3 Task 8).
    pub hot: Option<HotTierImpl>,
    /// Which node owns each collection (single node: `AlwaysLocal`).
    pub placement: Arc<dyn Placement>,
    pub node_id: u64,
    /// For the internal routes of other nodes (500 ms connect, 5 s total).
    pub internal: reqwest::Client,
    /// `--hot-pin-all`, reported by a node whose hot tier is off.
    pub hot_pin_all: bool,
    /// This node's roles (every role on a single node; plan M1.3 Task 11).
    pub roles: Roles,
    /// The receiving side of forwarded reads; `Some` on query nodes.
    pub forwarded: Option<ForwardedReads>,
    /// This node's forwarding counters (reads sent and received).
    pub forward_stats: Arc<ForwardStats>,
    /// The metastore view `GET /internal/v1/node/stats` reports in cluster
    /// mode.
    pub node_info: Option<Arc<dyn internal::NodeInfo>>,
}

/// What a query node needs to run forwarded reads (plan M1.3 Task 11).
#[derive(Clone, Debug)]
pub struct ForwardedReads {
    /// The node's routed service; forwarded reads use its `*_local` methods.
    pub service: Arc<CollectionService>,
    pub stats: Arc<ForwardStats>,
}

/// The API's routes, inside `HotLayer` (the `Operon-Hot` switch, with the
/// service's `hot_default` for requests without it), and the internal hot
/// routes outside it.
pub fn router(state: AppState) -> Router {
    let internal = internal::routes().with_state(state.clone());
    let hot_layer = HotLayer::new(state.collections.config().hot_default);
    let collection = "/v1/namespaces/{ns}/collections/{c}";
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/namespaces", post(create_namespace))
        .route("/v1/namespaces/{ns}/streams", post(create_stream))
        .route("/v1/namespaces/{ns}/streams/{stream}", get(describe_stream))
        .route(
            "/v1/namespaces/{ns}/streams/{stream}/partitions/{partition}/records",
            post(produce).get(fetch),
        )
        .route("/v1/namespaces/{ns}/links", post(create_link))
        .route("/v1/namespaces/{ns}/links/{link}", get(describe_link))
        .route(
            "/v1/namespaces/{ns}/collections",
            post(collections::create).get(collections::list),
        )
        .route(
            collection,
            get(collections::describe).delete(collections::drop),
        )
        .route(
            &format!("{collection}/fields"),
            post(collections::add_fields),
        )
        .route(
            &format!("{collection}/versions"),
            get(collections::versions),
        )
        .route(&format!("{collection}/scan"), post(collections::scan))
        .route("/v1/namespaces/{ns}/aliases", post(collections::aliases))
        .route(&format!("{collection}/documents"), post(collections::write))
        .route(
            &format!("{collection}/documents/get"),
            post(collections::get_documents),
        )
        .route(
            &format!("{collection}/documents/scroll"),
            post(collections::scroll),
        )
        .route(
            &format!("{collection}/documents/count"),
            post(collections::count),
        )
        .route("/v1/namespaces/{ns}/query", post(query::search))
        .route("/v1/namespaces/{ns}/sql", post(sql::sql))
        .merge(hot::routes())
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
        .layer(hot_layer)
        .merge(internal)
}

/// The routes of a node without the `gateway` role (plan M1.3 Task 11):
/// `/health`, `/ready` and the internal routes, no native API.
pub fn internal_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .fallback(no_route)
        .with_state(state.clone())
        .merge(internal::routes().with_state(state))
}

async fn no_route() -> ApiError {
    ApiError::not_found("no such route")
}

/// A known route with a method it does not serve: `405` with the usual JSON
/// error body (M0.3 re-review M3), not axum's empty one.
async fn method_not_allowed(method: axum::http::Method) -> ApiError {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "invalid_argument",
        format!("{method} is not allowed on this route"),
    )
}

type ApiResult = Result<Response, ApiError>;

fn parse_json<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::invalid(format!("bad request body: {e}")))
}

/// `response` with `Operon-Consistency-Token: token`.
fn with_token(mut response: Response, token: &ConsistencyToken) -> Response {
    if let Ok(value) = HeaderValue::from_str(&token.to_string()) {
        response.headers_mut().insert(CONSISTENCY_TOKEN, value);
    }
    response
}

/// The consistency of a read (rule 1): the `Operon-Consistency-Token`
/// request header turns a `strong`, `eventual` or absent body consistency
/// into `AtLeast(header)`, merges into an `at_least` one, and loses to a
/// `pinned` one.
fn read_consistency(
    headers: &HeaderMap,
    body: Option<ReadConsistency>,
) -> Result<ReadConsistency, ApiError> {
    let Some(value) = headers.get(&CONSISTENCY_TOKEN) else {
        return Ok(body.unwrap_or_default());
    };
    let text = value.to_str().map_err(|_| {
        ApiError::invalid("invalid Operon-Consistency-Token header: not visible ASCII")
    })?;
    let header = ConsistencyToken::from_str(text).map_err(|err| {
        ApiError::invalid(format!("invalid Operon-Consistency-Token header: {err}"))
    })?;
    Ok(match body {
        None | Some(ReadConsistency::Strong | ReadConsistency::Eventual) => {
            ReadConsistency::AtLeast(header)
        }
        Some(ReadConsistency::AtLeast(mut token)) => {
            token.merge(&header);
            ReadConsistency::AtLeast(token)
        }
        Some(pinned @ ReadConsistency::Pinned { .. }) => pinned,
    })
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    if state.meta.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[derive(Deserialize)]
struct CreateNamespace {
    name: String,
}

async fn create_namespace(
    State(state): State<AppState>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let body = body?;
    let request: CreateNamespace = parse_json(&body)?;
    let id = state.meta.create_namespace(&request.name).await?;
    Ok((StatusCode::CREATED, axum::Json(json!({ "id": id.0 }))).into_response())
}

#[derive(Deserialize)]
struct RetentionBody {
    max_age_ms: Option<u64>,
    max_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct CreateStream {
    name: String,
    partitions: u32,
    retention: Option<RetentionBody>,
}

async fn namespace_id(meta: &dyn MetaStore, name: &str) -> Result<NamespaceId, ApiError> {
    meta.namespace_by_name(Consistency::Local, name)
        .await?
        .map(|n| n.id)
        .ok_or_else(|| ApiError::not_found(format!("namespace {name:?} not found")))
}

async fn stream_id(meta: &dyn MetaStore, ns: &str, stream: &str) -> Result<StreamId, ApiError> {
    let namespace = namespace_id(meta, ns).await?;
    meta.stream_by_name(Consistency::Local, namespace, stream)
        .await?
        .map(|st| st.id)
        .ok_or_else(|| ApiError::not_found(format!("stream {ns}/{stream} not found")))
}

async fn create_stream(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: CreateStream = parse_json(&body)?;
    let namespace = namespace_id(&*state.meta, &ns).await?;
    let retention = request
        .retention
        .map_or(Retention::default(), |r| Retention {
            max_age_ms: r.max_age_ms,
            max_bytes: r.max_bytes,
        });
    // One command, so the stream never exists without its retention.
    let id = state
        .meta
        .create_stream(
            namespace,
            &request.name,
            request.partitions,
            WalClass::Standard,
            retention,
        )
        .await?;
    Ok((StatusCode::CREATED, axum::Json(json!({ "id": id.0 }))).into_response())
}

async fn describe_stream(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> ApiResult {
    let Path((ns, stream)) = path?;
    let id = stream_id(&*state.meta, &ns, &stream).await?;
    let body = state
        .meta
        .stream_state(Consistency::Local, id)
        .await?
        .map(|StreamState { stream, partitions }| {
            let partitions: Vec<Value> = (0u32..)
                .zip(partitions)
                .filter_map(|(p, bounds)| {
                    let bounds = bounds?;
                    Some(json!({
                        "partition": p,
                        "log_start_offset": bounds.log_start_offset,
                        "high_watermark": bounds.high_watermark,
                    }))
                })
                .collect();
            json!({
                "id": id.0,
                "partitions": partitions,
                "retention": {
                    "max_age_ms": stream.retention.max_age_ms,
                    "max_bytes": stream.retention.max_bytes,
                },
            })
        })
        .ok_or_else(|| ApiError::not_found(format!("stream {ns}/{stream} not found")))?;
    Ok(axum::Json(body).into_response())
}

fn parse_partition(partition: &str) -> Result<u32, ApiError> {
    partition
        .parse()
        .map_err(|_| ApiError::invalid(format!("bad partition {partition:?}")))
}

fn decode_b64(what: &str, value: Option<String>) -> Result<Option<Bytes>, ApiError> {
    value
        .map(|v| {
            BASE64
                .decode(v.as_bytes())
                .map(Bytes::from)
                .map_err(|e| ApiError::invalid(format!("{what} is not base64: {e}")))
        })
        .transpose()
}

fn encode_b64(value: &Option<Bytes>) -> Value {
    value
        .as_ref()
        .map_or(Value::Null, |v| Value::from(BASE64.encode(v)))
}

#[derive(Deserialize)]
struct HeaderBody {
    key: String,
    value: Option<String>,
}

#[derive(Deserialize)]
struct RecordBody {
    key: Option<String>,
    value: Option<String>,
    #[serde(default)]
    headers: Vec<HeaderBody>,
    timestamp_ms: Option<i64>,
}

#[derive(Deserialize)]
struct Produce {
    records: Vec<RecordBody>,
}

async fn produce(
    State(state): State<AppState>,
    path: Result<Path<(String, String, String)>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, stream, partition)), body) = (path?, body?);
    let partition = parse_partition(&partition)?;
    let request: Produce = parse_json(&body)?;
    let mut records = Vec::with_capacity(request.records.len());
    for record in request.records {
        let mut headers = Vec::with_capacity(record.headers.len());
        for header in record.headers {
            headers.push((header.key, decode_b64("header value", header.value)?));
        }
        records.push(Record {
            key: decode_b64("key", record.key)?,
            value: decode_b64("value", record.value)?,
            headers,
            // A negative timestamp gets the writer's clock.
            timestamp_ms: record.timestamp_ms.unwrap_or(-1),
        });
    }
    let acks = produce_records(
        &state.meta,
        &state.writer,
        &ns,
        &stream,
        vec![(partition, records)],
    )
    .await?;
    let ack = acks.into_iter().next().ok_or_else(|| {
        ApiError::from(operon_log::LogError::CommitUnknown(
            "no acknowledgement for an append".to_string(),
        ))
    })?;
    let id = ack.stream;
    let response = axum::Json(json!({
        "base_offset": ack.base_offset,
        "last_offset": ack.last_offset,
        "token": [{ "stream": id.0, "partition": partition, "offset": ack.last_offset }],
    }))
    .into_response();
    // As a collection write's token: the next offset of the partition.
    let token = ConsistencyToken(vec![(id, partition, ack.last_offset + 1)]);
    Ok(with_token(response, &token))
}

fn query_number<T: std::str::FromStr>(
    query: &HashMap<String, String>,
    name: &str,
) -> Result<Option<T>, ApiError> {
    query
        .get(name)
        .map(|v| {
            v.parse()
                .map_err(|_| ApiError::invalid(format!("bad {name} {v:?}")))
        })
        .transpose()
}

async fn fetch(
    State(state): State<AppState>,
    path: Result<Path<(String, String, String)>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> ApiResult {
    let (Path((ns, stream, partition)), Query(query)) = (path?, query?);
    let partition = parse_partition(&partition)?;
    let offset = query_number::<u64>(&query, "offset")?
        .ok_or_else(|| ApiError::invalid("the offset query parameter is required"))?;
    let max_bytes = query_number::<usize>(&query, "max_bytes")?
        .unwrap_or(DEFAULT_MAX_BYTES)
        .min(MAX_FETCH_BYTES);
    let max_wait = query_number::<u64>(&query, "max_wait_ms")?
        .map_or(Duration::ZERO, Duration::from_millis)
        .min(MAX_WAIT);
    let id = stream_id(&*state.meta, &ns, &stream).await?;
    let response = state
        .reader
        .fetch(FetchRequest {
            stream: id,
            partition,
            offset,
            max_bytes,
            max_wait,
        })
        .await?;
    let records: Vec<Value> = response
        .records
        .iter()
        .map(|r| {
            let headers: Vec<Value> = r
                .record
                .headers
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": encode_b64(value) }))
                .collect();
            json!({
                "offset": r.offset,
                "key": encode_b64(&r.record.key),
                "value": encode_b64(&r.record.value),
                "headers": headers,
                "timestamp_ms": r.record.timestamp_ms,
            })
        })
        .collect();
    Ok(axum::Json(json!({
        "records": records,
        "next_offset": response.next_offset,
        "high_watermark": response.high_watermark,
        "log_start_offset": response.log_start_offset,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct TargetBody {
    kind: String,
    name: String,
}

#[derive(Deserialize)]
struct CreateLink {
    name: String,
    /// The source stream's name, in the same namespace.
    source: String,
    /// Default: a `counter` target named like the link.
    target: Option<TargetBody>,
    #[serde(default)]
    options: std::collections::BTreeMap<String, String>,
}

async fn create_link(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: CreateLink = parse_json(&body)?;
    let namespace = namespace_id(&*state.meta, &ns).await?;
    let source = stream_id(&*state.meta, &ns, &request.source).await?;
    let target = request.target.map_or_else(
        || TargetRef {
            kind: COUNTER_KIND.to_string(),
            name: request.name.clone(),
        },
        |t| TargetRef {
            kind: t.kind,
            name: t.name,
        },
    );
    let id = state
        .meta
        .create_link(namespace, &request.name, source, target, request.options)
        .await?;
    Ok((StatusCode::CREATED, axum::Json(json!({ "id": id.0 }))).into_response())
}

async fn describe_link(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> ApiResult {
    let Path((ns, name)) = path?;
    let namespace = namespace_id(&*state.meta, &ns).await?;
    // A description: the link and its source's name need not come from one
    // state.
    let not_found = || ApiError::not_found(format!("link {ns}/{name} not found"));
    let link = state
        .meta
        .link_by_name(Consistency::Local, namespace, &name)
        .await?
        .ok_or_else(not_found)?;
    let source = state
        .meta
        .stream(Consistency::Local, link.source)
        .await?
        .map(|st| st.name)
        .ok_or_else(not_found)?;
    let mut body = json!({
        "id": link.id.0,
        "name": link.name,
        "source": source,
        "target": { "kind": link.target.kind, "name": link.target.name },
        "options": link.options,
    });
    let applied_json = |applied: &std::collections::BTreeMap<u32, u64>| -> Value {
        applied
            .iter()
            .map(|(partition, offset)| json!({ "partition": partition, "offset": offset }))
            .collect()
    };
    if link.target.kind == COUNTER_KIND {
        // The counters and the version they belong to, in one consistent view.
        let table = CounterTable::for_link(state.meta.clone(), state.store.clone(), &link);
        let snapshot = table.snapshot().await?;
        body["version"] = json!(snapshot.version);
        body["applied"] = applied_json(&snapshot.applied);
        body["counters"] = json!(snapshot.counters);
        body["skipped"] = json!(snapshot.skipped);
    } else if let Some(factory) = state.registry.get(&link.target.kind) {
        let loaded = factory.open(&state.meta, &link)?.load().await?;
        body["version"] = json!(loaded.version);
        body["applied"] = applied_json(&loaded.applied);
    }
    Ok(axum::Json(body).into_response())
}
