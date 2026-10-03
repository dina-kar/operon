//! The REST listener: the router, Qdrant's envelope and the extractors
//! every handler uses ("Qdrant protocol facts": envelope, routes).

use std::time::Instant;

use axum::Router;
use axum::extract::{FromRequest, FromRequestParts, Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, get, on, post, put};
use loams_collection::ConsistencyToken;
use loams_query::hot::{HOT_HEADER, HotLayer, parse_hot_header};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::ids::PointId;
use crate::model::collections::{ChangeAliases, CreateFieldIndex};
use crate::model::common::UpdateResult;
use crate::model::points::{
    CountRequest, DeletePayload, DeleteVectors, PointInsert, PointRequest, PointsSelector,
    ScrollRequest, SetPayload, UpdateOperation, UpdateOperations, UpdateVectors,
};
use crate::model::query::{
    Batch, DiscoverRequest, QueryGroupsRequest, QueryRequest, QueryRequestBatch, QueryResponse,
    RecommendGroupsRequest, RecommendRequest, SearchGroupsRequest, SearchRequest,
};
use crate::schema::NewVector;
use crate::{
    QDRANT_TITLE, QdrantGateway, TOKEN_HEADER, groups, query, reads, schema, snapshots, writes,
};

/// The 1.19 OpenAPI routes (and the legacy search routes of Ruling 1) that
/// no task serves yet: each answers `Unsupported("<method> <path>")` (501).
/// A task that serves a route removes it here.
const UNSUPPORTED: &[(&str, &str)] = &[
    ("GET", "/telemetry"),
    ("GET", "/metrics"),
    ("GET", "/issues"),
    ("DELETE", "/issues"),
    ("GET", "/cluster/telemetry"),
    ("POST", "/cluster/recover"),
    ("DELETE", "/cluster/peer/{peer_id}"),
    ("GET", "/quotas"),
    ("PUT", "/quotas"),
    (
        "DELETE",
        "/collections/{collection_name}/index/{field_name}",
    ),
    (
        "DELETE",
        "/collections/{collection_name}/vectors/{vector_name}",
    ),
    ("POST", "/collections/{collection_name}/cluster"),
    ("GET", "/collections/{collection_name}/optimizations"),
    ("GET", "/collections/{collection_name}/shards"),
    ("PUT", "/collections/{collection_name}/shards"),
    ("POST", "/collections/{collection_name}/shards/delete"),
    ("POST", "/collections/{collection_name}/snapshots/upload"),
    ("PUT", "/collections/{collection_name}/snapshots/recover"),
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
    ("POST", "/collections/{collection_name}/facet"),
    (
        "POST",
        "/collections/{collection_name}/points/search/matrix/pairs",
    ),
    (
        "POST",
        "/collections/{collection_name}/points/search/matrix/offsets",
    ),
];

/// Every REST route, inside `HotLayer` and the gateway's own `Loams-Hot`
/// check (step 5a).
pub(crate) fn router(gw: QdrantGateway) -> Router {
    let hot = HotLayer::new(gw.service().config().hot_default);
    let mut router = Router::new()
        .route("/", get(root))
        .route("/healthz", get(|| async { text("healthz check passed") }))
        .route("/livez", get(|| async { text("livez check passed") }))
        .route("/readyz", get(readyz))
        .route("/collections", get(list_collections))
        .route("/collections/{collection_name}/points/count", post(count))
        .route(
            "/collections/{collection_name}/points",
            put(upsert_points).post(retrieve_points),
        )
        .route("/collections/{collection_name}/points/{id}", get(get_point))
        .route("/collections/{collection_name}/points/scroll", post(scroll))
        .route(
            "/collections/{collection_name}/points/delete",
            post(delete_points),
        )
        .route(
            "/collections/{collection_name}/points/payload",
            post(set_payload).put(overwrite_payload),
        )
        .route(
            "/collections/{collection_name}/points/payload/delete",
            post(delete_payload),
        )
        .route(
            "/collections/{collection_name}/points/payload/clear",
            post(clear_payload),
        )
        .route(
            "/collections/{collection_name}/points/vectors",
            put(update_vectors),
        )
        .route(
            "/collections/{collection_name}/points/vectors/delete",
            post(delete_vectors),
        )
        .route(
            "/collections/{collection_name}/points/batch",
            post(batch_update),
        )
        .route(
            "/collections/{collection_name}/points/query",
            post(query_points),
        )
        .route(
            "/collections/{collection_name}/points/query/batch",
            post(query_batch),
        )
        .route(
            "/collections/{collection_name}/points/search",
            post(search_points),
        )
        .route(
            "/collections/{collection_name}/points/search/batch",
            post(search_batch),
        )
        .route(
            "/collections/{collection_name}/points/recommend",
            post(recommend_points),
        )
        .route(
            "/collections/{collection_name}/points/recommend/batch",
            post(recommend_batch),
        )
        .route(
            "/collections/{collection_name}/points/discover",
            post(discover_points),
        )
        .route(
            "/collections/{collection_name}/points/discover/batch",
            post(discover_batch),
        )
        .route(
            "/collections/{collection_name}/points/query/groups",
            post(query_groups),
        )
        .route(
            "/collections/{collection_name}/points/search/groups",
            post(search_groups),
        )
        .route(
            "/collections/{collection_name}/points/recommend/groups",
            post(recommend_groups),
        )
        .route("/cluster", get(cluster_status))
        .route(
            "/collections/{collection_name}",
            get(collection_info)
                .put(create_collection)
                .patch(update_collection)
                .delete(delete_collection),
        )
        .route("/collections/aliases", post(update_aliases))
        .route(
            "/collections/{collection_name}/exists",
            get(collection_exists),
        )
        .route(
            "/collections/{collection_name}/vectors/{vector_name}",
            put(create_vector_name),
        )
        .route("/collections/{collection_name}/cluster", get(cluster_info))
        .route(
            "/collections/{collection_name}/index",
            put(create_field_index),
        )
        .route(
            "/collections/{collection_name}/aliases",
            get(collection_aliases),
        )
        .route("/aliases", get(list_aliases))
        .route(
            "/collections/{collection_name}/snapshots",
            get(list_snapshots).post(create_snapshot),
        );
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

/// Answers an invalid `Loams-Hot` with the Qdrant envelope, so no request
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

/// A JSON body with `status`.
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
            GatewayError::Service(loams_query::ServiceError::Internal(err.to_string())),
        ),
    }
}

/// [`ok`], plus the write's `Loams-Consistency-Token`.
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

/// The error envelope at `time`, with `Retry-After` for backpressure (E2).
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

/// A `text/plain` body.
fn text(body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// 404 for a path no route knows.
async fn no_route(method: Method, uri: Uri) -> Response {
    let message = format!("Not found: route {method} {}", uri.path());
    json_response(
        StatusCode::NOT_FOUND,
        &json!({"status": {"error": message}, "time": 0.0}),
    )
}

/// 405 for a known path with another method.
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

    /// Reads the body up to `max_request_bytes` and parses it.
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

    /// Parses the query string.
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
pub(crate) struct WriteParams {
    #[serde(default)]
    pub wait: bool,
    #[allow(dead_code)] // Accepted and ignored (Ruling 14).
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

/// [`serve`] for a write: the answer carries the write's
/// `Loams-Consistency-Token`.
async fn serve_write<T, F>(
    gw: &QdrantGateway,
    headers: &HeaderMap,
    timeout: Option<u64>,
    op: impl FnOnce(RequestCtx) -> F,
) -> Response
where
    T: Serialize,
    F: Future<Output = Result<(T, ConsistencyToken), GatewayError>>,
{
    let ctx = match RequestCtx::from_http(headers, timeout, gw.config()) {
        Ok(ctx) => ctx,
        Err(err) => return reject(err),
    };
    match ctx.run(op(ctx.clone())).await {
        Ok((result, token)) => ok_write(&ctx, result, &token),
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
        Err(loams_query::ServiceError::Unavailable(_)) => {
            let mut response = text("not ready");
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            response
        }
        _ => text("all shards are ready"),
    }
}

/// `GET /collections`.
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

/// `POST /collections/{c}/points/count`.
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

// ----- collections, aliases, snapshots, cluster (Task 3) -----

/// `GET /cluster`.
async fn cluster_status(
    State(gw): State<QdrantGateway>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |_| async {
        Ok(schema::cluster_status())
    })
    .await
}

/// `GET /collections/{c}`.
async fn collection_info(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::collection_info(gw.clone(), ctx, collection)
    })
    .await
}

/// `PUT /collections/{c}`.
async fn create_collection(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
    QdrantJson(body): QdrantJson<Value>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::create_collection(gw.clone(), ctx, collection, body)
    })
    .await
}

/// `PATCH /collections/{c}` (Ruling 16).
async fn update_collection(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
    QdrantJson(body): QdrantJson<Value>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::update_collection(gw.clone(), ctx, collection, body)
    })
    .await
}

/// `DELETE /collections/{c}`.
async fn delete_collection(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::delete_collection(gw.clone(), ctx, collection)
    })
    .await
}

/// `GET /collections/{c}/exists`.
async fn collection_exists(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::collection_exists(gw.clone(), ctx, collection)
    })
    .await
}

/// `POST /collections/aliases`.
async fn update_aliases(
    State(gw): State<QdrantGateway>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
    QdrantJson(request): QdrantJson<ChangeAliases>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::update_aliases(gw.clone(), ctx, request.actions)
    })
    .await
}

/// `GET /aliases`.
async fn list_aliases(
    State(gw): State<QdrantGateway>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::list_aliases(gw.clone(), ctx)
    })
    .await
}

/// `GET /collections/{c}/aliases`.
async fn collection_aliases(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::collection_aliases(gw.clone(), ctx, collection)
    })
    .await
}

/// `PUT /collections/{c}/vectors/{v}`.
async fn create_vector_name(
    State(gw): State<QdrantGateway>,
    Path((collection, vector)): Path<(String, String)>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(body): QdrantJson<Value>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::create_vector_name(
            gw.clone(),
            ctx,
            collection,
            vector,
            NewVector::from_json(body),
        )
    })
    .await
}

/// `GET /collections/{c}/cluster`.
async fn cluster_info(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    let points = |ctx| schema::cluster_points(gw.clone(), ctx, collection);
    serve(&gw, &headers, params.timeout, |ctx| async move {
        Ok(schema::cluster_info_json(points(ctx).await?))
    })
    .await
}

/// `POST /collections/{c}/snapshots`.
async fn create_snapshot(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
) -> Response {
    let name = collection.clone();
    let create = |ctx| snapshots::create(gw.clone(), ctx, collection);
    serve(&gw, &headers, params.timeout, |ctx| async move {
        let m = create(ctx).await?;
        Ok(snapshots::snapshot_description(&name, &m))
    })
    .await
}

/// `GET /collections/{c}/snapshots`.
async fn list_snapshots(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<AdminParams>,
) -> Response {
    let name = collection.clone();
    let list = |ctx| snapshots::list(gw.clone(), ctx, collection);
    serve(&gw, &headers, params.timeout, |ctx| async move {
        let versions = list(ctx).await?;
        Ok(versions
            .iter()
            .map(|m| snapshots::snapshot_description(&name, m))
            .collect::<Vec<_>>())
    })
    .await
}

// ----- payload indexes (Task 4) -----

/// `PUT /collections/{c}/index`.
async fn create_field_index(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(request): QdrantJson<CreateFieldIndex>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        schema::create_field_index(gw.clone(), ctx, collection, request, params.wait)
    })
    .await
}

// ----- point writes (Task 5) -----

/// A write route of one operation.
async fn write_one(
    gw: QdrantGateway,
    collection: String,
    headers: HeaderMap,
    params: WriteParams,
    op: UpdateOperation,
) -> Response {
    serve_write(&gw, &headers, params.timeout, |ctx| {
        writes::update_one(gw.clone(), ctx, collection, op, params.wait)
    })
    .await
}

/// `PUT /collections/{c}/points`.
async fn upsert_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(upsert): QdrantJson<PointInsert>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::Upsert { upsert },
    )
    .await
}

/// `POST /collections/{c}/points/delete`.
async fn delete_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(delete): QdrantJson<PointsSelector>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::Delete { delete },
    )
    .await
}

/// `POST /collections/{c}/points/payload`.
async fn set_payload(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(set_payload): QdrantJson<SetPayload>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::SetPayload { set_payload },
    )
    .await
}

/// `PUT /collections/{c}/points/payload`.
async fn overwrite_payload(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(overwrite_payload): QdrantJson<SetPayload>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::OverwritePayload { overwrite_payload },
    )
    .await
}

/// `POST /collections/{c}/points/payload/delete`.
async fn delete_payload(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(delete_payload): QdrantJson<DeletePayload>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::DeletePayload { delete_payload },
    )
    .await
}

/// `POST /collections/{c}/points/payload/clear`.
async fn clear_payload(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(clear_payload): QdrantJson<PointsSelector>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::ClearPayload { clear_payload },
    )
    .await
}

/// `PUT /collections/{c}/points/vectors`.
async fn update_vectors(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(update_vectors): QdrantJson<UpdateVectors>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::UpdateVectors { update_vectors },
    )
    .await
}

/// `POST /collections/{c}/points/vectors/delete`.
async fn delete_vectors(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(delete_vectors): QdrantJson<DeleteVectors>,
) -> Response {
    write_one(
        gw,
        collection,
        headers,
        params,
        UpdateOperation::DeleteVectors { delete_vectors },
    )
    .await
}

/// One `UpdateResult` per operation, all equal (one atomic write).
async fn batch_update(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<WriteParams>,
    QdrantJson(request): QdrantJson<UpdateOperations>,
) -> Response {
    let g = gw.clone();
    serve_write(&gw, &headers, params.timeout, |ctx| async move {
        let (results, token): (Vec<UpdateResult>, _) =
            writes::update(g, ctx, collection, request.operations, params.wait).await?;
        Ok((results, token))
    })
    .await
}

// ----- point reads (Task 6) -----

/// `POST /collections/{c}/points` (retrieve).
async fn retrieve_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<PointRequest>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        reads::retrieve(gw.clone(), ctx, collection, request)
    })
    .await
}

/// `GET /collections/{c}/points/{id}`.
async fn get_point(
    State(gw): State<QdrantGateway>,
    Path((collection, id)): Path<(String, String)>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| async move {
        let id = PointId::parse_path(&id)?;
        reads::get_point(g, ctx, collection, id).await
    })
    .await
}

/// `POST /collections/{c}/points/scroll`.
async fn scroll(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<ScrollRequest>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        reads::scroll(gw.clone(), ctx, collection, request)
    })
    .await
}

// ----- universal query (Task 7) -----

/// `POST /collections/{c}/points/query`.
async fn query_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<QueryRequest>,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| async move {
        let points = query::run_query(g, ctx, collection, request).await?;
        Ok(QueryResponse { points })
    })
    .await
}

/// `[{points}]`, one per request, in order.
async fn query_batch(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<QueryRequestBatch>,
) -> Response {
    serve(&gw, &headers, params.timeout, |ctx| {
        query::run_batch(gw.clone(), ctx, collection, request.searches)
    })
    .await
}

// ----- legacy search, recommend and discover (Task 8) -----

/// A legacy route: the request as a `QueryRequest`; the result is the bare
/// list of points.
async fn legacy<T: Into<QueryRequest>>(
    gw: QdrantGateway,
    collection: String,
    headers: HeaderMap,
    params: ReadParams,
    request: T,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| {
        query::run_query(g, ctx, collection, request.into())
    })
    .await
}

/// A legacy batch: one list of points per request, in order.
async fn legacy_batch<T: Into<QueryRequest>>(
    gw: QdrantGateway,
    collection: String,
    headers: HeaderMap,
    params: ReadParams,
    batch: Batch<T>,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| async move {
        crate::check_request_len(
            "The query batch",
            batch.searches.len(),
            g.config().max_batch_queries,
        )?;
        let requests: Vec<QueryRequest> = batch.searches.into_iter().map(Into::into).collect();
        let results = query::run_batch(g, ctx, collection, requests).await?;
        Ok(results.into_iter().map(|r| r.points).collect::<Vec<_>>())
    })
    .await
}

/// `POST /collections/{c}/points/search`.
async fn search_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<SearchRequest>,
) -> Response {
    legacy(gw, collection, headers, params, request).await
}

/// `POST /collections/{c}/points/search/batch`.
async fn search_batch(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(batch): QdrantJson<Batch<SearchRequest>>,
) -> Response {
    legacy_batch(gw, collection, headers, params, batch).await
}

/// `POST /collections/{c}/points/recommend`.
async fn recommend_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<RecommendRequest>,
) -> Response {
    legacy(gw, collection, headers, params, request).await
}

/// `POST /collections/{c}/points/recommend/batch`.
async fn recommend_batch(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(batch): QdrantJson<Batch<RecommendRequest>>,
) -> Response {
    legacy_batch(gw, collection, headers, params, batch).await
}

/// `POST /collections/{c}/points/discover`.
async fn discover_points(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<DiscoverRequest>,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| async move {
        request.check()?;
        query::run_query(g, ctx, collection, request.into()).await
    })
    .await
}

/// `POST /collections/{c}/points/discover/batch`.
async fn discover_batch(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(batch): QdrantJson<Batch<DiscoverRequest>>,
) -> Response {
    let error = crate::check_request_len(
        "The query batch",
        batch.searches.len(),
        gw.config().max_batch_queries,
    )
    .err()
    .or_else(|| batch.searches.iter().find_map(|r| r.check().err()));
    if let Some(e) = error {
        return serve(
            &gw,
            &headers,
            params.timeout,
            |_| async move { Err::<(), _>(e) },
        )
        .await;
    }
    legacy_batch(gw, collection, headers, params, batch).await
}

// ----- groups (Task 9) -----

/// A groups route: `{"groups": [...]}`.
async fn groups_route(
    gw: QdrantGateway,
    collection: String,
    headers: HeaderMap,
    params: ReadParams,
    request: QueryGroupsRequest,
) -> Response {
    let g = gw.clone();
    serve(&gw, &headers, params.timeout, |ctx| {
        groups::run_groups(g, ctx, collection, request)
    })
    .await
}

/// `POST /collections/{c}/points/query/groups`.
async fn query_groups(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<QueryGroupsRequest>,
) -> Response {
    groups_route(gw, collection, headers, params, request).await
}

/// `POST /collections/{c}/points/search/groups` (legacy).
async fn search_groups(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<SearchGroupsRequest>,
) -> Response {
    groups_route(gw, collection, headers, params, request.into()).await
}

/// `POST /collections/{c}/points/recommend/groups` (legacy).
async fn recommend_groups(
    State(gw): State<QdrantGateway>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    QdrantQuery(params): QdrantQuery<ReadParams>,
    QdrantJson(request): QdrantJson<RecommendGroupsRequest>,
) -> Response {
    groups_route(gw, collection, headers, params, request.into()).await
}
