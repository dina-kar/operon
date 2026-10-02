//! The info endpoints (Task 1 rule 11): `GET|HEAD /`, `GET /_license`,
//! `GET /_cluster/health[/{index}]` and the trained-model inference routes
//! (Rulings 11, 13).

use std::collections::BTreeMap;

use axum::extract::{Path, State};
use axum::http::Uri;
use axum::response::Response;
use serde_json::{Value, json};

use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, respond};
use crate::names::{IndexExpr, ResolveOptions, resolve};
use crate::{ES_VERSION, EsGateway};

/// The Lucene version ES 8.19 ships with, as `GET /` reports it.
const LUCENE_VERSION: &str = "9.12.2";

/// The parameters `_cluster/health` accepts and answers immediately.
const HEALTH_PARAMS: &[&str] = &[
    "wait_for_status",
    "timeout",
    "level",
    "local",
    "wait_for_no_relocating_shards",
    "wait_for_active_shards",
];

/// Runs `op` after validating the query string against `allowed`, and
/// answers with its body or its error.
async fn serve<F>(ctx: &RequestCtx, uri: &Uri, allowed: &[&str], op: F) -> Response
where
    F: Future<Output = Result<(u16, Value), EsError>>,
{
    if let Err(err) = Params::parse(uri.query(), uri.path(), allowed) {
        return fail(ctx, &err);
    }
    match op.await {
        Ok((status, body)) => respond(ctx, status, &body),
        Err(err) => fail(ctx, &err),
    }
}

/// `GET /` (and `HEAD /`, whose body the product layer drops).
pub(crate) async fn root(State(gw): State<EsGateway>, ctx: RequestCtx, uri: Uri) -> Response {
    serve(&ctx, &uri, &[], async {
        let config = gw.config();
        Ok((
            200,
            json!({
                "name": config.node_name,
                "cluster_name": config.cluster_name,
                "cluster_uuid": "_na_",
                "version": {
                    "number": ES_VERSION,
                    "build_flavor": "default",
                    "build_type": "tar",
                    "build_hash": format!("loams-{}", env!("CARGO_PKG_VERSION")),
                    "build_date": "1970-01-01T00:00:00.000Z",
                    "build_snapshot": false,
                    "lucene_version": LUCENE_VERSION,
                    "minimum_wire_compatibility_version": "7.17.0",
                    "minimum_index_compatibility_version": "7.0.0",
                },
                "tagline": "You Know, for Search",
            }),
        ))
    })
    .await
}

/// `GET /_license`: an active enterprise license (Ruling 13).
pub(crate) async fn license(ctx: RequestCtx, uri: Uri) -> Response {
    serve(&ctx, &uri, &["local", "accept_enterprise"], async {
        Ok((
            200,
            json!({"license": {
                "status": "active",
                "uid": "loams",
                "type": "enterprise",
                "issue_date": "1970-01-01T00:00:00.000Z",
                "issue_date_in_millis": 0,
                "max_nodes": null,
                "max_resource_units": null,
                "issued_to": "loams",
                "issuer": "loams",
                "start_date_in_millis": -1,
            }}),
        ))
    })
    .await
}

/// `GET /_cluster/health`: green, over every index of the namespace.
pub(crate) async fn health(State(gw): State<EsGateway>, ctx: RequestCtx, uri: Uri) -> Response {
    let ns = ctx.namespace.clone();
    serve(&ctx, &uri, HEALTH_PARAMS, health_of(gw, ns, IndexExpr::All)).await
}

/// `GET /_cluster/health/{index}`: green, over the indices `index` covers;
/// with a missing one, 408 and red with `timed_out`, as ES answers once its
/// wait times out (owner ruling O-M15-7, checked against the 8.19 oracle,
/// row T11-3). Loams answers at once: nothing it could wait for turns red
/// green.
pub(crate) async fn health_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    let ns = ctx.namespace.clone();
    let expr = IndexExpr::parse(&index);
    serve(&ctx, &uri, HEALTH_PARAMS, health_of(gw, ns, expr)).await
}

/// The synthetic health body: one node, every shard active, P = the sum of
/// the resolved indices' partitions.
async fn health_of(gw: EsGateway, ns: String, expr: IndexExpr) -> Result<(u16, Value), EsError> {
    let service = gw.service();
    let (resolved, missing) = match resolve(service, &ns, &expr, ResolveOptions::default()).await {
        Ok(resolved) => (resolved, false),
        Err(err) if err.kind == "index_not_found_exception" => (Vec::new(), true),
        Err(err) => return Err(err),
    };
    let partitions: BTreeMap<String, u32> = service
        .collection_records(&ns)
        .await
        .map_err(|err| EsError::from_service(err, ErrorContext::Read))?
        .into_iter()
        .map(|collection| (collection.name, collection.partitions))
        .collect();
    let shards: u64 = resolved
        .iter()
        .filter_map(|r| partitions.get(&r.name))
        .map(|p| u64::from(*p))
        .sum();
    Ok((
        if missing { 408 } else { 200 },
        json!({
            "cluster_name": gw.config().cluster_name,
            "status": if missing { "red" } else { "green" },
            "timed_out": missing,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": shards,
            "active_shards": shards,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "unassigned_primary_shards": 0,
            "delayed_unassigned_shards": 0,
            "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0,
            "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": 100.0,
        }),
    ))
}

/// `POST /_ml/trained_models/{id}/_infer` (and `…/deployment/_infer`):
/// Loams runs no models, so every model is missing (Ruling 13).
pub(crate) async fn infer(ctx: RequestCtx, uri: Uri, Path(id): Path<String>) -> Response {
    serve(&ctx, &uri, &["timeout"], async {
        Err(EsError::new(
            404,
            "resource_not_found_exception",
            format!("Could not find trained model [{id}]"),
        ))
    })
    .await
}
