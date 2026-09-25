//! The native HTTP/JSON API (M0.3 plan, Task 7). Namespaces and streams are
//! addressed by name; record keys and values are base64.

use std::collections::HashMap;
use std::time::Duration;

use axum::Router;
use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use operon_common::{NamespaceId, StreamId};
use operon_link::{COUNTER_KIND, CounterTable, LinkError};
use operon_log::{FetchRequest, LogError, LogReader, LogWriter, Record};
use operon_meta::{ApplyError, Consistency, MetaClient, MetaError, Retention, TargetRef, WalClass};
use operon_store::Store;
use serde::Deserialize;
use serde_json::{Value, json};

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

/// What the handlers share.
#[derive(Clone, Debug)]
pub struct AppState {
    pub meta: MetaClient,
    pub writer: LogWriter,
    pub reader: LogReader,
    /// For reading link targets.
    pub store: Store,
}

/// The API's routes.
pub fn router(state: AppState) -> Router {
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
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
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

/// Turns an axum extractor rejection (a bad path segment, query, or body,
/// including a body over the limit) into the API's JSON error body, keeping
/// its status.
fn rejected(status: StatusCode, message: String) -> ApiError {
    let code = if status.is_server_error() {
        "internal"
    } else {
        "invalid_argument"
    };
    ApiError::new(status, code, message)
}

impl From<PathRejection> for ApiError {
    fn from(err: PathRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(err: QueryRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

impl From<BytesRejection> for ApiError {
    fn from(err: BytesRejection) -> Self {
        rejected(err.status(), err.body_text())
    }
}

/// An error response: `{"error": code, "message": ...}` plus any extra fields.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    extra: serde_json::Map<String, Value>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            extra: serde_json::Map::new(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_argument", message)
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    fn with(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra.insert(key.to_string(), value.into());
        self
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = serde_json::Map::new();
        body.insert("error".to_string(), Value::from(self.code));
        body.insert("message".to_string(), Value::from(self.message));
        body.extend(self.extra);
        (self.status, axum::Json(Value::Object(body))).into_response()
    }
}

fn unavailable(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
}

fn internal(message: impl Into<String>) -> ApiError {
    ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
}

impl From<MetaError> for ApiError {
    fn from(err: MetaError) -> Self {
        let message = err.to_string();
        match err {
            MetaError::Rejected(apply) => match apply {
                ApplyError::InvalidArgument(_) => ApiError::invalid(message),
                ApplyError::NamespaceExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::StreamExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::LinkExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::CollectionExists(id) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message).with("id", id.0)
                }
                ApplyError::NameTaken(_) => {
                    ApiError::new(StatusCode::CONFLICT, "already_exists", message)
                }
                ApplyError::NamespaceNotFound(_)
                | ApplyError::StreamNotFound(_)
                | ApplyError::PartitionNotFound { .. }
                | ApplyError::CollectionNotFound(_)
                | ApplyError::UnknownCollection(_) => ApiError::not_found(message),
                ApplyError::IncompatibleSchema(_) => ApiError::invalid(message),
                // The message names the current version.
                ApplyError::SchemaVersionMismatch { .. } => {
                    ApiError::new(StatusCode::CONFLICT, "conflict", message)
                }
                _ => internal(message),
            },
            MetaError::NotLeader { .. }
            | MetaError::Timeout
            | MetaError::Unavailable(_)
            | MetaError::ClockSkew { .. } => unavailable(message),
            _ => internal(message),
        }
    }
}

impl From<LogError> for ApiError {
    fn from(err: LogError) -> Self {
        let message = err.to_string();
        match err {
            LogError::InvalidArgument(_) => ApiError::invalid(message),
            LogError::UnknownStream(_) | LogError::UnknownPartition { .. } => {
                ApiError::not_found(message)
            }
            LogError::OffsetOutOfRange {
                requested,
                log_start_offset,
                high_watermark,
            } => ApiError::new(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "offset_out_of_range",
                message,
            )
            .with("offset", requested)
            .with("log_start_offset", log_start_offset)
            .with("high_watermark", high_watermark),
            // Nothing (or possibly something) was committed; a retry is the
            // caller's call, and the condition is transient.
            LogError::Backpressure
            | LogError::CommitUnknown(_)
            | LogError::Closed
            | LogError::Store(_)
            | LogError::Cache(_) => unavailable(message),
            LogError::Meta(meta) => meta.into(),
            LogError::Task(_) => internal(message),
            LogError::Corrupt(_) | LogError::UnsupportedEncoding(_) => internal(message),
        }
    }
}

type ApiResult = Result<Response, ApiError>;

fn parse_json<T: serde::de::DeserializeOwned>(body: &Bytes) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| ApiError::invalid(format!("bad request body: {e}")))
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    if state.meta.local().status().leader.is_some() {
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

async fn namespace_id(meta: &MetaClient, name: &str) -> Result<NamespaceId, ApiError> {
    meta.read(Consistency::Local, |s| {
        s.namespace_by_name(name).map(|n| n.id)
    })
    .await?
    .ok_or_else(|| ApiError::not_found(format!("namespace {name:?} not found")))
}

async fn stream_id(meta: &MetaClient, ns: &str, stream: &str) -> Result<StreamId, ApiError> {
    let namespace = namespace_id(meta, ns).await?;
    meta.read(Consistency::Local, |s| {
        s.stream_by_name(namespace, stream).map(|st| st.id)
    })
    .await?
    .ok_or_else(|| ApiError::not_found(format!("stream {ns}/{stream} not found")))
}

async fn create_stream(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let request: CreateStream = parse_json(&body)?;
    let namespace = namespace_id(&state.meta, &ns).await?;
    let retention = request
        .retention
        .map_or(Retention::default(), |r| Retention {
            max_age_ms: r.max_age_ms,
            max_bytes: r.max_bytes,
        });
    // One command, so the stream never exists without its retention.
    let id = state
        .meta
        .create_stream_with_retention(
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
    let id = stream_id(&state.meta, &ns, &stream).await?;
    let body = state
        .meta
        .read(Consistency::Local, |s| {
            let stream = s.stream(id)?;
            let partitions: Vec<Value> = (0..stream.partitions)
                .filter_map(|p| {
                    let state = s.partition(id, p)?;
                    Some(json!({
                        "partition": p,
                        "log_start_offset": state.log_start_offset(),
                        "high_watermark": state.high_watermark(),
                    }))
                })
                .collect();
            Some(json!({
                "id": id.0,
                "partitions": partitions,
                "retention": {
                    "max_age_ms": stream.retention.max_age_ms,
                    "max_bytes": stream.retention.max_bytes,
                },
            }))
        })
        .await?
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
    let id = stream_id(&state.meta, &ns, &stream).await?;
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
    let ack = state.writer.append(id, partition, records).await?;
    Ok(axum::Json(json!({
        "base_offset": ack.base_offset,
        "last_offset": ack.last_offset,
        "token": [{ "stream": id.0, "partition": partition, "offset": ack.last_offset }],
    }))
    .into_response())
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
    let id = stream_id(&state.meta, &ns, &stream).await?;
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

impl From<LinkError> for ApiError {
    fn from(err: LinkError) -> Self {
        let message = err.to_string();
        match err {
            LinkError::Meta(meta) => meta.into(),
            LinkError::Log(log) => log.into(),
            LinkError::NotFound(_) => ApiError::not_found(message),
            LinkError::Store(_) | LinkError::Blocked(_) => unavailable(message),
            LinkError::Corrupt(_) => internal(message),
        }
    }
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
    let namespace = namespace_id(&state.meta, &ns).await?;
    let source = stream_id(&state.meta, &ns, &request.source).await?;
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
    let namespace = namespace_id(&state.meta, &ns).await?;
    let link = state
        .meta
        .read(Consistency::Local, |s| {
            let link = s.link_by_name(namespace, &name)?.clone();
            let source = s.stream(link.source).map(|st| st.name.clone())?;
            Some((link, source))
        })
        .await?
        .ok_or_else(|| ApiError::not_found(format!("link {ns}/{name} not found")))?;
    let (link, source) = link;
    let mut body = json!({
        "id": link.id.0,
        "name": link.name,
        "source": source,
        "target": { "kind": link.target.kind, "name": link.target.name },
        "options": link.options,
    });
    if link.target.kind == COUNTER_KIND {
        let table = CounterTable::for_link(state.meta.clone(), state.store.clone(), &link);
        let snapshot = table.snapshot().await?;
        let applied: Vec<Value> = snapshot
            .applied
            .iter()
            .map(|(partition, offset)| json!({ "partition": partition, "offset": offset }))
            .collect();
        body["version"] = json!(snapshot.version);
        body["applied"] = Value::from(applied);
        body["counters"] = json!(snapshot.counters);
        body["skipped"] = json!(snapshot.skipped);
    }
    Ok(axum::Json(body).into_response())
}
