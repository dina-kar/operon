//! The internal routes (plan M1.3 Tasks 8 and 11): a node that does not
//! own a collection asks its owner for the hot status or to warm it, and
//! forwards reads to it; `GET /internal/v1/node/stats` reports the node.
//! They answer from this node, whatever the placement says, so a call is
//! never forwarded twice. Unauthenticated, like every listener in M1
//! (overview §6.9); merged outside `HotLayer`.

use std::fmt;

use axum::Router;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use operon_common::{CollectionId, NamespaceId};
use operon_hot::{READS_PATH, serve_forwarded};
use serde_json::{Value, json};

use super::hot::{HotTarget, local_status, warm_local};
use super::{ApiResult, AppState, parse_json};

pub const HOT_STATUS_PATH: &str = "/internal/v1/hot/status"; // POST {"ns": u64, "cid": u64} -> DetailedHotStatus JSON
pub const HOT_WARM_PATH: &str = "/internal/v1/hot/warm"; // POST {"ns": u64, "cid": u64} -> 202, DetailedHotStatus JSON

pub const NODE_STATS_PATH: &str = "/internal/v1/node/stats"; // GET -> {"node_id", "roles", "forwarded_out", "forwarded_in", "fallbacks_signalled", "ann_served", "split_files_served", "meta"}

/// The metastore view a cluster node reports in its stats (`meta`); the
/// cluster module implements it, so this module names no metastore type.
pub trait NodeInfo: Send + Sync + fmt::Debug {
    fn meta(&self) -> Value;
}

/// Every internal route: hot, reads and node stats.
pub fn routes() -> Router<AppState> {
    hot_routes().merge(reads_routes()).merge(node_routes())
}

/// POST /internal/v1/reads/{op} → `serve_forwarded` on this node's
/// service; `503` on a node without the `query` role.
pub fn reads_routes() -> Router<AppState> {
    Router::new()
        .route(&format!("{READS_PATH}{{op}}"), post(read))
        .layer(DefaultBodyLimit::max(super::MAX_BODY_BYTES))
}

async fn read(State(state): State<AppState>, Path(op): Path<String>, body: Bytes) -> Response {
    let Some(forwarded) = &state.forwarded else {
        return super::errors::unavailable(format!(
            "node {} does not serve reads (no query role)",
            state.node_id
        ))
        .into_response();
    };
    let (status, body) = serve_forwarded(&forwarded.service, &op, body, &forwarded.stats).await;
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// GET /internal/v1/node/stats.
pub fn node_routes() -> Router<AppState> {
    Router::new().route(NODE_STATS_PATH, get(node_stats))
}

async fn node_stats(State(state): State<AppState>) -> Response {
    let forwarded = state.forward_stats.snapshot();
    let tier = state.hot.as_ref().map(|tier| tier.counters());
    axum::Json(json!({
        "node_id": state.node_id,
        "roles": state.roles.to_string(),
        "forwarded_out": forwarded.forwarded_out,
        "forwarded_in": forwarded.forwarded_in,
        "fallbacks_signalled": forwarded.fallbacks_signalled,
        "ann_served": tier.map_or(0, |c| c.ann_served),
        "split_files_served": tier.map_or(0, |c| c.split_files_served),
        "meta": state.node_info.as_ref().map_or(Value::Null, |info| info.meta()),
    }))
    .into_response()
}

pub fn hot_routes() -> Router<AppState> {
    Router::new()
        .route(HOT_STATUS_PATH, post(status))
        .route(HOT_WARM_PATH, post(warm))
}

fn target(body: &Bytes) -> Result<(NamespaceId, CollectionId), super::ApiError> {
    let target: HotTarget = parse_json(body)?;
    Ok((NamespaceId(target.ns), CollectionId(target.cid)))
}

async fn status(State(state): State<AppState>, body: Result<Bytes, BytesRejection>) -> ApiResult {
    let (ns, cid) = target(&body?)?;
    Ok(axum::Json(local_status(&state, ns, cid).await).into_response())
}

async fn warm(State(state): State<AppState>, body: Result<Bytes, BytesRejection>) -> ApiResult {
    let (ns, cid) = target(&body?)?;
    let status = warm_local(&state, ns, cid).await?;
    Ok((StatusCode::ACCEPTED, axum::Json(status)).into_response())
}
