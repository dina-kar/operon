//! The operations API (D1 Task 7, D146, design §21 §6.4): long operations
//! are durable promises of the embedded durable server, submitted by their
//! own routes (Task 8's import) and then polled here.
//!
//! - `GET /v1/operations/{id}` → 200 with the operation, or 404;
//! - `GET /v1/namespaces/{ns}/operations?state=&cursor=` → 200
//!   `{operations, next}`;
//! - `POST /v1/operations/{id}/cancel` → 202, or 409 `operation_finished`.
//!
//! A submit route answers with [`submitted`]: 202 and
//! `Location: /v1/operations/{id}`, or 200 with the same `Location` for an
//! idempotent repeat (an `Idempotency-Key` header, [`idempotency_key`]).
//!
//! The routes exist when the node serves durable execution. Until Loam's
//! durable runtime has started (the listener serves only after it, T6-7),
//! and after the durable server stops, they answer 503
//! `durable_unavailable`.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use axum::Router;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use operon_common::meta::MetaStore;
use operon_durable::ops::RETENTION_TASK;
use operon_durable::{OperationId, OperationState, Operations, OpsError};
use operon_worker::{
    Candidate, Priority, Task, TaskContext, TaskError, TaskKey, TaskOutcome, TaskSource,
};
use serde::Deserialize;
use serde_json::json;

use super::ApiError;

/// The request header that makes a submit idempotent (D146).
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";

/// How often the retention sweep runs (Ruling 8's 7 days is the horizon;
/// this is only the cadence).
pub const RETENTION_INTERVAL: Duration = Duration::from_secs(3600);

/// The node's [`Operations`], set once Loam's durable runtime has started.
/// The routes and the retention task hold it from assembly on.
#[derive(Clone, Default, Debug)]
pub struct OperationsSlot(Arc<OnceLock<Arc<Operations>>>);

impl OperationsSlot {
    /// Serve `ops` from now on. A second call keeps the first.
    pub fn set(&self, ops: Arc<Operations>) {
        let _ = self.0.set(ops);
    }

    /// The operations, when the runtime has started.
    pub fn get(&self) -> Option<Arc<Operations>> {
        self.0.get().cloned()
    }

    fn serving(&self) -> Result<Arc<Operations>, ApiError> {
        self.get().ok_or_else(|| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "durable_unavailable",
                "durable execution is starting",
            )
        })
    }
}

/// The operations routes, over `slot`.
pub fn routes(slot: OperationsSlot) -> Router {
    Router::new()
        .route("/v1/operations/{id}", get(get_operation))
        .route("/v1/operations/{id}/cancel", post(cancel))
        .route("/v1/namespaces/{ns}/operations", get(list))
        .with_state(slot)
}

/// A submit's answer: 202 (created) or 200 (an idempotent repeat), with
/// `Location: /v1/operations/{id}` and `{"id", "location"}`.
pub fn submitted(id: &OperationId, created: bool) -> Response {
    let location = format!("/v1/operations/{id}");
    let status = if created {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    let mut response = (
        status,
        axum::Json(json!({ "id": id.as_str(), "location": location })),
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

/// The request's `Idempotency-Key`, if any. It must be visible ASCII; its
/// length is checked by the submit.
pub fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    headers
        .get(IDEMPOTENCY_KEY)
        .map(|value| {
            value
                .to_str()
                .map(str::to_string)
                .map_err(|_| ApiError::invalid("invalid Idempotency-Key header: not visible ASCII"))
        })
        .transpose()
}

/// The API error for an [`OpsError`].
pub fn ops_error(err: OpsError) -> ApiError {
    let status = match &err {
        OpsError::NotFound(_) => StatusCode::NOT_FOUND,
        OpsError::IdempotencyKeyReused(_) | OpsError::Finished(_) => StatusCode::CONFLICT,
        OpsError::UnknownKind(_) | OpsError::Invalid(_) => StatusCode::BAD_REQUEST,
        OpsError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        OpsError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let message = err.to_string();
    let error = ApiError::new(status, err.code(), message);
    match err {
        OpsError::IdempotencyKeyReused(id) | OpsError::Finished(id) => {
            error.with("operation", id.to_string())
        }
        _ => error,
    }
}

/// `{id}` as an operation id; any other text names no operation (404).
fn operation_id(path: Result<Path<String>, PathRejection>) -> Result<OperationId, ApiError> {
    let Path(id) = path?;
    OperationId::parse(&id).ok_or_else(|| ApiError::not_found(format!("no operation {id}")))
}

async fn get_operation(
    State(slot): State<OperationsSlot>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let id = operation_id(path)?;
    let ops = slot.serving()?;
    let operation = ops.get(&id).await.map_err(ops_error)?;
    Ok(axum::Json(operation).into_response())
}

async fn cancel(
    State(slot): State<OperationsSlot>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let id = operation_id(path)?;
    let ops = slot.serving()?;
    ops.cancel(&id).await.map_err(ops_error)?;
    let body = match ops.get(&id).await {
        Ok(operation) => json!(operation),
        Err(_) => json!({ "id": id.as_str(), "state": OperationState::Canceled }),
    };
    Ok((StatusCode::ACCEPTED, axum::Json(body)).into_response())
}

#[derive(Deserialize)]
struct ListQuery {
    state: Option<String>,
    cursor: Option<String>,
}

async fn list(
    State(slot): State<OperationsSlot>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let Path(ns) = path?;
    let Query(query) = query?;
    let state = match query.state.as_deref() {
        None | Some("") => None,
        Some(state) => Some(state.parse::<OperationState>().map_err(ops_error)?),
    };
    let ops = slot.serving()?;
    let cursor = query.cursor.filter(|cursor| !cursor.is_empty());
    let (operations, next) = ops.list(&ns, state, cursor).await.map_err(ops_error)?;
    Ok(axum::Json(json!({ "operations": operations, "next": next })).into_response())
}

/// Proposes `durable-op-retention` (priority `Maintenance`, Ruling 8) once
/// per [`RETENTION_INTERVAL`], once the operations are served.
#[derive(Debug)]
pub struct RetentionSource {
    slot: OperationsSlot,
    interval: Duration,
    last: Mutex<Option<Instant>>,
}

impl RetentionSource {
    /// A source over `slot`.
    pub fn new(slot: OperationsSlot) -> Self {
        Self {
            slot,
            interval: RETENTION_INTERVAL,
            last: Mutex::new(None),
        }
    }
}

#[async_trait]
impl TaskSource for RetentionSource {
    fn priority(&self) -> Priority {
        Priority::Maintenance
    }

    async fn candidates(&self, _meta: &dyn MetaStore) -> Result<Vec<Candidate>, TaskError> {
        let Some(ops) = self.slot.get() else {
            return Ok(Vec::new());
        };
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last.is_some_and(|at| at.elapsed() < self.interval) {
            return Ok(Vec::new());
        }
        *last = Some(Instant::now());
        let task: Arc<dyn Task> = Arc::new(Retention { ops });
        Ok(vec![(TaskKey::cluster(RETENTION_TASK), task)])
    }
}

/// One retention sweep.
struct Retention {
    ops: Arc<Operations>,
}

#[async_trait]
impl Task for Retention {
    async fn run(&self, _ctx: TaskContext) -> Result<TaskOutcome, TaskError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        match self.ops.prune_finished(now).await {
            Ok(0) => Ok(TaskOutcome::Idle),
            Ok(_) => Ok(TaskOutcome::Done),
            Err(e) => Err(TaskError::failed(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use axum::body::Body;
    use axum::http::Request;
    use operon_durable::{DurableConfig, DurableServer, OperationKinds, OpsConfig};
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    /// A test submit route: `POST /v1/namespaces/{ns}/test-ops` with the
    /// params as the body, as Task 8's import route will submit.
    async fn submit(
        State(slot): State<OperationsSlot>,
        Path(ns): Path<String>,
        headers: HeaderMap,
        axum::Json(params): axum::Json<Value>,
    ) -> Result<Response, ApiError> {
        let key = idempotency_key(&headers)?;
        let ops = slot.serving()?;
        let (id, created) = ops
            .submit(&ns, "test.queued", params, key.as_deref())
            .await
            .map_err(ops_error)?;
        Ok(submitted(&id, created))
    }

    fn app(slot: OperationsSlot) -> Router {
        routes(slot.clone()).merge(
            Router::new()
                .route("/v1/namespaces/{ns}/test-ops", post(submit))
                .with_state(slot),
        )
    }

    async fn call(
        app: &Router,
        method: &str,
        uri: &str,
        key: Option<&str>,
        body: Value,
    ) -> (StatusCode, HeaderMap, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(key) = key {
            request = request.header(IDEMPOTENCY_KEY, key);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, headers, body)
    }

    /// D146 through the routes: a submit answers 202 with `Location`, the
    /// location serves the operation, a repeat with the key answers 200 with
    /// the same location, other parameters 409, and cancel 202 then 409.
    #[tokio::test(flavor = "multi_thread")]
    async fn submit_returns_202_location() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = DurableConfig::sqlite(dir.path().join("durable").join("default.db"));
        config.listen = TcpListener::bind("127.0.0.1:0")
            .expect("probe")
            .local_addr()
            .expect("addr");
        let server = DurableServer::start(config, "1").await.expect("server");
        let slot = OperationsSlot::default();
        let app = app(slot.clone());

        // Before the runtime is up: 503.
        let (status, headers, body) = call(
            &app,
            "GET",
            "/v1/operations/op-00000000000000000000000000",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "durable_unavailable");
        assert!(headers.contains_key(header::RETRY_AFTER));

        // No runtime runs `test.queued`: its operations stay queued.
        slot.set(Arc::new(Operations::new(
            server.client(),
            server.store().clone(),
            &OperationKinds::new().declare("test.queued"),
            OpsConfig::default(),
        )));
        let params = json!({ "target": { "collection": "docs" }, "n": 1 });
        let (status, headers, body) = call(
            &app,
            "POST",
            "/v1/namespaces/default/test-ops",
            Some("load-1"),
            params.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let id = OperationId::for_key("default", "load-1");
        let location = format!("/v1/operations/{id}");
        assert_eq!(headers[header::LOCATION], location.as_str());
        assert_eq!(body, json!({ "id": id.as_str(), "location": location }));

        let (status, _, op) = call(&app, "GET", &location, None, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{op}");
        assert_eq!(op["id"], id.as_str());
        assert_eq!(op["kind"], "test.queued");
        assert_eq!(op["namespace"], "default");
        assert_eq!(op["target"], json!({ "collection": "docs" }));
        assert_eq!(op["state"], "queued");
        assert_eq!(op["result"], Value::Null);
        assert_eq!(op["error"], Value::Null);
        assert!(op["created_at"].as_i64().is_some_and(|t| t > 0), "{op}");

        // The same key and parameters: 200, the same location.
        let (status, headers, _) = call(
            &app,
            "POST",
            "/v1/namespaces/default/test-ops",
            Some("load-1"),
            params,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::LOCATION], location.as_str());
        // Other parameters: 409.
        let (status, _, body) = call(
            &app,
            "POST",
            "/v1/namespaces/default/test-ops",
            Some("load-1"),
            json!({ "n": 2 }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "idempotency_key_reused");
        assert_eq!(body["operation"], id.as_str());

        // Listed, filtered by state.
        let (status, _, page) = call(
            &app,
            "GET",
            "/v1/namespaces/default/operations?state=queued",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert_eq!(page["operations"][0]["id"], id.as_str());
        assert!(page.get("next").is_some());
        let (status, _, page) = call(
            &app,
            "GET",
            "/v1/namespaces/default/operations?state=succeeded",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["operations"], json!([]));
        let (status, _, body) = call(
            &app,
            "GET",
            "/v1/namespaces/default/operations?state=done",
            None,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_argument");

        // Cancel: 202, then 409 operation_finished.
        let cancel = format!("{location}/cancel");
        let (status, _, body) = call(&app, "POST", &cancel, None, Value::Null).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        assert_eq!(body["state"], "canceled");
        let (status, _, body) = call(&app, "POST", &cancel, None, Value::Null).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "operation_finished");

        // Unknown and malformed ids: 404.
        for uri in [
            "/v1/operations/op-00000000000000000000000000",
            "/v1/operations/not-an-operation",
            "/v1/operations/op-00000000000000000000000000/cancel",
        ] {
            let method = if uri.ends_with("/cancel") {
                "POST"
            } else {
                "GET"
            };
            let (status, _, body) = call(&app, method, uri, None, Value::Null).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body}");
            assert_eq!(body["error"], "not_found");
        }

        // The server gone: 503.
        server.stop().await;
        let (status, _, body) = call(&app, "GET", &location, None, Value::Null).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "durable_unavailable");
    }
}
