//! The internal hot routes (plan M1.3 Task 8 rule 4): a node that does not
//! own a collection asks its owner for the hot status or to warm it. They
//! answer from this node, whatever the placement says, so a call is never
//! forwarded twice. Unauthenticated, like every listener in M1 (overview
//! §6.9); merged outside `HotLayer`.

use axum::Router;
use axum::extract::State;
use axum::extract::rejection::BytesRejection;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use bytes::Bytes;
use operon_common::{CollectionId, NamespaceId};

use super::hot::{HotTarget, local_status, warm_local};
use super::{ApiResult, AppState, parse_json};

pub const HOT_STATUS_PATH: &str = "/internal/v1/hot/status"; // POST {"ns": u64, "cid": u64} -> DetailedHotStatus JSON
pub const HOT_WARM_PATH: &str = "/internal/v1/hot/warm"; // POST {"ns": u64, "cid": u64} -> 202, DetailedHotStatus JSON

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
