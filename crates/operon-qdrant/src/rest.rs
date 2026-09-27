//! The REST listener: the router, Qdrant's envelope and the extractors
//! every handler uses ("Qdrant protocol facts": envelope, routes).

use std::time::Instant;

use axum::Router;
use axum::extract::{FromRequest, FromRequestParts, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, get, on, post};
use operon_collection::ConsistencyToken;
use operon_query::hot::{HOT_HEADER, HotLayer, parse_hot_header};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::model::points::CountRequest;
use crate::{QDRANT_TITLE, QdrantGateway, TOKEN_HEADER, reads, schema};

/// The 1.19 OpenAPI routes (and the legacy search routes of Ruling 1) that
/// no task serves yet: each answers `Unsupported("<method> <path>")` (501).
/// A task that serves a route removes it here.
const UNSUPPORTED: &[(&str, &str)] = &[
    ("GET", "/telemetry"),
    ("GET", "/metrics"),
    ("GET", "/issues"),
    ("DELETE", "/issues"),
    ("GET", "/cluster"),
    ("GET", "/cluster/telemetry"),
    ("POST", "/cluster/recover"),
    ("DELETE", "/cluster/peer/{peer_id}"),
    ("GET", "/quotas"),
    ("PUT", "/quotas"),
    ("GET", "/collections/{collection_name}"),
    ("PUT", "/collections/{collection_name}"),
    ("PATCH", "/collections/{collection_name}"),
    ("DELETE", "/collections/{collection_name}"),
    ("POST", "/collections/aliases"),
    ("PUT", "/collections/{collection_name}/index"),
    ("GET", "/collections/{collection_name}/exists"),
    (
        "DELETE",
        "/collections/{collection_name}/index/{field_name}",
    ),
    (
        "PUT",
        "/collections/{collection_name}/vectors/{vector_name}",
    ),
    (
        "DELETE",
        "/collections/{collection_name}/vectors/{vector_name}",
    ),
    ("GET", "/collections/{collection_name}/cluster"),
    ("POST", "/collections/{collection_name}/cluster"),
    ("GET", "/collections/{collection_name}/optimizations"),
    ("GET", "/collections/{collection_name}/aliases"),
    ("GET", "/aliases"),
    ("GET", "/collections/{collection_name}/shards"),
    ("PUT", "/collections/{collection_name}/shards"),
    ("POST", "/collections/{collection_name}/shards/delete"),
    ("POST", "/collections/{collection_name}/snapshots/upload"),
    ("PUT", "/collections/{collection_name}/snapshots/recover"),
    ("GET", "/collections/{collection_name}/snapshots"),
    ("POST", "/collections/{collection_name}/snapshots"),
    (
        "GET",
        "/collections/{collection_name}/snapshots/{snapshot_name}",
    ),
    (
        "DELETE",
        "/collections/{collection_name}/snapshots/{snapshot_name}",
    ),
    ("GET", "/snapshots"),
    ("POST", "/snapshots"),
    ("GET", "/snapshots/{snapshot_name}"),
    ("DELETE", "/snapshots/{snapshot_name}"),
    (
        "GET",
        "/collections/{collection_name}/shards/{shard_id}/snapshot",
    ),
    (
        "POST",
        "/collections/{collection_name}/shards/{shard_id}/snapshots/upload",
    ),
    (
        "PUT",
        "/collections/{collection_name}/shards/{shard_id}/snapshots/recover",
    ),
    (
        "GET",
        "/collections/{collection_name}/shards/{shard_id}/snapshots",
    ),
    (
        "POST",
        "/collections/{collection_name}/shards/{shard_id}/snapshots",
    ),
    (
        "GET",
        "/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
    ),
    (
        "DELETE",
        "/collections/{collection_name}/shards/{shard_id}/snapshots/{snapshot_name}",
    ),
    ("GET", "/collections/{collection_name}/points/{id}"),
    ("POST", "/collections/{collection_name}/points"),
    ("PUT", "/collections/{collection_name}/points"),
    ("POST", "/collections/{collection_name}/points/delete"),
    ("PUT", "/collections/{collection_name}/points/vectors"),
    (
        "POST",
        "/collections/{collection_name}/points/vectors/delete",
    ),
    ("POST", "/collections/{collection_name}/points/payload"),
    ("PUT", "/collections/{collection_name}/points/payload"),
    (
        "POST",
        "/collections/{collection_name}/points/payload/delete",
    ),
    (
        "POST",
        "/collections/{collection_name}/points/payload/clear",
    ),
    ("POST", "/collections/{collection_name}/points/batch"),
    ("POST", "/collections/{collection_name}/points/scroll"),
    ("POST", "/collections/{collection_name}/facet"),
    ("POST", "/collections/{collection_name}/points/query"),
    ("POST", "/collections/{collection_name}/points/query/batch"),
    ("POST", "/collections/{collection_name}/points/query/groups"),
    (
        "POST",
        "/collections/{collection_name}/points/search/matrix/pairs",
    ),
    (
        "POST",
        "/collections/{collection_name}/points/search/matrix/offsets",
    ),
    // Legacy routes, gone from the 1.19 OpenAPI but served by the 1.19.1
    // server (Ruling 1; Tasks 8 and 9).
    ("POST", "/collections/{collection_name}/points/search"),
    ("POST", "/collections/{collection_name}/points/search/batch"),
    (
        "POST",
        "/collections/{collection_name}/points/search/groups",
    ),
    ("POST", "/collections/{collection_name}/points/recommend"),
    (
        "POST",
        "/collections/{collection_name}/points/recommend/batch",
    ),
    (
        "POST",
        "/collections/{collection_name}/points/recommend/groups",
    ),
    ("POST", "/collections/{collection_name}/points/discover"),
    (
        "POST",
        "/collections/{collection_name}/points/discover/batch",
    ),
];

/// Every REST route, inside `HotLayer` and the gateway's own `Operon-Hot`
/// check (step 5a).
pub(crate) fn router(gw: QdrantGateway) -> Router {
    let hot = HotLayer::new(gw.service().config().hot_default);
    let mut router = Router::new()
        .route("/", get(root))
        .route("/healthz", get(|| async { text("healthz check passed") }))
        .route("/livez", get(|| async { text("livez check passed") }))
        .route("/readyz", get(readyz))
        .route("/collections", get(list_collections))
        .route("/collections/{collection_name}/points/count", post(count));
    for &(method, path) in UNSUPPORTED {
        let filter = match method {
            "GET" => MethodFilter::GET,
            "PUT" => MethodFilter::PUT,
            "POST" => MethodFilter::POST,
            "PATCH" => MethodFilter::PATCH,
            _ => MethodFilter::DELETE,
        };
        let feature = format!("{method} {path}");
        router = router.route(
            path,
            on(filter, move || {
                let feature = feature.clone();
                async move { reject(GatewayError::Unsupported(feature)) }
            }),
        );
    }
    router
        .fallback(no_route)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(gw)
        .layer(hot)
        .layer(middleware::from_fn(check_hot_header))
}

/// Answers an invalid `Operon-Hot` with the Qdrant envelope, so no request
/// sees `HotLayer`'s native error body (step 5a, E11).
async fn check_hot_header(request: Request, next: Next) -> Response {
    if let Some(value) = request.headers().get(HOT_HEADER)
        && let Err(err) = parse_hot_header(&String::from_utf8_lossy(value.as_bytes()))
    {
        return reject(err.into());
    }
    next.run(request).await
}

// ----- envelopes -----

fn json_response(status: StatusCode, body: &Value) -> Response {
    let mut response = (status, body.to_string()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// `{"result": …, "status": "ok", "time": …}`.
pub(crate) fn ok<T: Serialize>(ctx: &RequestCtx, result: T) -> Response {
    match serde_json::to_value(result) {
        Ok(result) => json_response(
            StatusCode::OK,
            &json!({"result": result, "status": "ok", "time": ctx.elapsed_secs()}),
        ),
        Err(err) => fail(
            ctx,
            GatewayError::Service(operon_query::ServiceError::Internal(err.to_string())),
        ),
    }
}

/// [`ok`], plus the write's `Operon-Consistency-Token`.
#[allow(dead_code)] // The write routes arrive with Task 5.
pub(crate) fn ok_write<T: Serialize>(
    ctx: &RequestCtx,
    result: T,
    token: &ConsistencyToken,
) -> Response {
    let mut response = ok(ctx, result);
    if response.status() == StatusCode::OK
        && let Ok(value) = HeaderValue::from_str(&token.to_string())
    {
        response.headers_mut().insert(TOKEN_HEADER, value);
    }
    response
}

/// `{"status": {"error": …}, "time": …}` with the error's status, plus
/// `Retry-After` for write backpressure (E2).
pub(crate) fn fail(ctx: &RequestCtx, e: GatewayError) -> Response {
    error_response(ctx.elapsed_secs(), &e)
}

/// [`fail`] before a context exists.
fn reject(e: GatewayError) -> Response {
    error_response(0.0, &e)
}

fn error_response(time: f64, e: &GatewayError) -> Response {
    let mut response = json_response(
        e.http_status(),
        &json!({"status": {"error": e.to_string()}, "time": time}),
    );
    if let Some(secs) = e.retry_after_secs() {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(secs));
    }
    response
}

fn text(body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

async fn no_route(method: Method, uri: Uri) -> Response {
    let message = format!("Not found: route {method} {}", uri.path());
    json_response(
        StatusCode::NOT_FOUND,
        &json!({"status": {"error": message}, "time": 0.0}),
    )
}

async fn method_not_allowed(method: Method, uri: Uri) -> Response {
    let message = format!("Method not allowed: {method} {}", uri.path());
    json_response(
        StatusCode::METHOD_NOT_ALLOWED,
        &json!({"status": {"error": message}, "time": 0.0}),
    )
}

// ----- extractors -----

/// A JSON body of at most `max_request_bytes`: over it is 413, and a body
/// that does not parse is `Format error in JSON body: <serde error>`.
pub(crate) struct QdrantJson<T>(pub T);

impl<T: DeserializeOwned> FromRequest<QdrantGateway> for QdrantJson<T> {
    type Rejection = Response;

    async fn from_request(request: Request, gw: &QdrantGateway) -> Result<Self, Response> {
        let started = Instant::now();
        let fail = |e: GatewayError| error_response(started.elapsed().as_secs_f64(), &e);
        let limit = gw.config().max_request_bytes;
        let declared = request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if declared.is_some_and(|len| len > limit as u64) {
            return Err(fail(GatewayError::TooLarge));
        }
        let bytes = axum::body::to_bytes(request.into_body(), limit)
            .await
            .map_err(|_| fail(GatewayError::TooLarge))?;
        serde_json::from_slice(&bytes)
            .map(QdrantJson)
            .map_err(|err| fail(GatewayError::json(err.to_string())))
    }
}

/// Query parameters; one that does not parse is `Format error in query
/// parameters: …`.
pub(crate) struct QdrantQuery<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequestParts<S> for QdrantQuery<T> {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Response> {
        axum::extract::Query::<T>::try_from_uri(&parts.uri)
            .map(|q| QdrantQuery(q.0))
            .map_err(|err| {
                reject(GatewayError::Format {
                    what: "query parameters",
                    message: err.body_text(),
                })
            })
    }
}

/// The query parameters of a write route.
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)] // The write routes arrive with Task 5.
pub(crate) struct WriteParams {
    #[serde(default)]
    pub wait: bool,
    pub ordering: Option<String>,
    pub timeout: Option<u64>,
}

/// The query parameters of a read route; `consistency` is accepted and
/// ignored (Ruling 14).
#[derive(Debug, serde::Deserialize)]
pub(crate) struct ReadParams {
    #[allow(dead_code)]
    pub consistency: Option<Value>,
    pub timeout: Option<u64>,
}

/// The query parameters of a collection admin route.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct AdminParams {
    pub timeout: Option<u64>,
}

/// Builds the context, runs `op` under its timeout and answers with the
/// envelope.
async fn serve<T, F>(
    gw: &QdrantGateway,
    headers: &HeaderMap,
    timeout: Option<u64>,
    op: impl FnOnce(RequestCtx) -> F,
) -> Response
where
    T: Serialize,
    F: Future<Output = Result<T, GatewayError>>,
{
    let ctx = match RequestCtx::from_http(headers, timeout, gw.config()) {
        Ok(ctx) => ctx,
        Err(err) => return reject(err),
    };
    match ctx.run(op(ctx.clone())).await {
        Ok(result) => ok(&ctx, result),
        Err(err) => fail(&ctx, err),
    }
}

// ----- service endpoints (Task 2) -----

/// Unenveloped, as Qdrant's `GET /`.
async fn root(State(gw): State<QdrantGateway>) -> Response {
    json_response(
        StatusCode::OK,
        &json!({"title": QDRANT_TITLE, "version": gw.config().reported_version}),
    )
}

/// 503 `not ready` while the collection service answers `Unavailable`.
async fn readyz(State(gw): State<QdrantGateway>) -> Response {
    match gw.service().list_collections(&gw.config().namespace).await {
        Err(operon_query::ServiceError::Unavailable(_)) => {
            let mut response = text("not ready");
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            response
        }
        _ => text("all shards are ready"),
    }
}

async fn list_collections(
    State(gw): State<QdrantGateway>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::list_collections(gw.clone(), ctx)
    })
    .await
}

async fn count(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<CountRequest>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        reads::count(gw.clone(), ctx, collection, request)
    })
    .await
}
