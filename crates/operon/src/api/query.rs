//! `POST /v1/namespaces/{ns}/query` (plan M1.2 Task 11 rule 1): a
//! `SearchRequest`, or the §05 §4 hybrid body (Task 1 rule 8).

use axum::extract::rejection::{BytesRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use operon_query::json::hybrid::parse_query_body;
use serde_json::Value;

use super::{ApiResult, AppState, parse_json, read_consistency, with_token};

pub(super) async fn search(
    State(state): State<AppState>,
    ns: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path(ns), body) = (ns?, body?);
    let body: Value = parse_json(&body)?;
    // Date math (`now-30d`) is relative to the proposer's clock.
    let mut request = parse_query_body(body, state.meta.now_ms())?;
    request.consistency = read_consistency(&headers, Some(request.consistency))?;
    let response = state.collections.search(&ns, request).await?;
    let token = response.read_token.clone();
    Ok(with_token(axum::Json(response).into_response(), &token))
}
