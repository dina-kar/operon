//! The WAL service's HTTP API: what Loams's control plane calls (§28 §6.9).
//!
//! - `POST /v1/tenant/timeline`: create a timeline, with Neon's
//!   `TimelineCreateRequest` body (`tenant_id`, `timeline_id`, `pg_version`,
//!   `system_id`, `wal_seg_size`, `start_lsn`, `commit_lsn`; `mconf` is
//!   accepted and ignored: one logical acceptor per timeline).
//! - `GET /v1/tenant/{tenant_id}/timeline/{timeline_id}`: the head.
//! - `GET /v1/status`: the node id.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::Error;
use crate::service::WalService;
use crate::store::WalStore;
use crate::types::{AcceptorState, Id, Lsn, ServerInfo, TimelineId};

/// The default WAL segment size (16 MiB).
pub const DEFAULT_WAL_SEG_SIZE: u32 = 16 << 20;

/// Neon's `TimelineCreateRequest`, as the safekeeper HTTP API takes it.
#[derive(Debug, Deserialize)]
pub struct TimelineCreateRequest {
    pub tenant_id: String,
    pub timeline_id: String,
    /// The Postgres version: a major (16) or a full version (160009).
    pub pg_version: u32,
    #[serde(default)]
    pub system_id: Option<u64>,
    #[serde(default)]
    pub wal_seg_size: Option<u32>,
    /// `X/Y`: where the timeline's WAL starts here.
    pub start_lsn: String,
    #[serde(default)]
    pub commit_lsn: Option<String>,
    #[serde(default)]
    pub mconf: Option<serde_json::Value>,
}

/// A head, with LSNs in `X/Y` notation.
#[derive(Debug, Serialize)]
pub struct TimelineStatus {
    pub tenant_id: String,
    pub timeline_id: String,
    pub term: u64,
    pub last_log_term: u64,
    pub flush_lsn: String,
    pub commit_lsn: String,
    pub backup_lsn: String,
    pub remote_consistent_lsn: String,
    pub peer_horizon_lsn: String,
    pub timeline_start_lsn: String,
    pub trimmed_lsn: String,
    pub pg_version: u32,
    pub system_id: u64,
    pub wal_seg_size: u32,
}

impl TimelineStatus {
    fn of(tl: &TimelineId, st: &AcceptorState, commit: Lsn) -> Self {
        Self {
            tenant_id: tl.tenant.to_string(),
            timeline_id: tl.timeline.to_string(),
            term: st.term,
            last_log_term: st.last_log_term(),
            flush_lsn: st.wal_end().to_string(),
            commit_lsn: st.commit_lsn.max(commit).to_string(),
            backup_lsn: st.backup_lsn.to_string(),
            remote_consistent_lsn: st.remote_consistent_lsn.to_string(),
            peer_horizon_lsn: st.peer_horizon_lsn.to_string(),
            timeline_start_lsn: st.timeline_start_lsn.to_string(),
            trimmed_lsn: st.trimmed_lsn.to_string(),
            pg_version: st.server.pg_version,
            system_id: st.server.system_id,
            wal_seg_size: st.server.wal_seg_size,
        }
    }
}

struct ApiError(StatusCode, String);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        let code = match e {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Protocol(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError(code, e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "msg": self.1 }))).into_response()
    }
}

/// The router over a service. With an auth token configured, every route
/// needs `Authorization: Bearer <token>`.
pub fn router<S: WalStore>(svc: Arc<WalService<S>>) -> Router {
    let token = svc.config().auth_token.clone();
    Router::new()
        .route("/v1/status", get(status::<S>))
        .route("/v1/tenant/timeline", post(create::<S>))
        .route(
            "/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            get(timeline::<S>),
        )
        .layer(axum::middleware::from_fn(
            move |req: axum::extract::Request, next: axum::middleware::Next| {
                let token = token.clone();
                async move {
                    if let Some(t) = token {
                        let ok = req
                            .headers()
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.strip_prefix("Bearer "))
                            .is_some_and(|got| {
                                crate::service::constant_time_eq(got.as_bytes(), t.as_bytes())
                            });
                        if !ok {
                            return ApiError(
                                StatusCode::UNAUTHORIZED,
                                "missing or wrong bearer token".into(),
                            )
                            .into_response();
                        }
                    }
                    next.run(req).await
                }
            },
        ))
        .with_state(svc)
}

async fn status<S: WalStore>(State(svc): State<Arc<WalService<S>>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "id": svc.config().node_id }))
}

fn full_pg_version(v: u32) -> u32 {
    if v < 100 { v * 10_000 } else { v }
}

async fn create<S: WalStore>(
    State(svc): State<Arc<WalService<S>>>,
    Json(req): Json<TimelineCreateRequest>,
) -> Result<Json<TimelineStatus>, ApiError> {
    let tl = TimelineId::new(req.tenant_id.parse::<Id>()?, req.timeline_id.parse::<Id>()?);
    let start: Lsn = req.start_lsn.parse()?;
    let server = ServerInfo {
        pg_version: full_pg_version(req.pg_version),
        system_id: req.system_id.unwrap_or(0),
        wal_seg_size: req.wal_seg_size.unwrap_or(DEFAULT_WAL_SEG_SIZE),
    };
    let st = svc.store().create(&tl, server, start).await?;
    if let Some(c) = req.commit_lsn {
        let c: Lsn = c.parse()?;
        if let Err(d) = svc.store().record_commit_lsn(&tl, st.term, c).await? {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!("timeline {tl} moved to term {} meanwhile", d.current),
            ));
        }
    }
    let st = svc.store().load(&tl).await?.unwrap_or(st);
    Ok(Json(TimelineStatus::of(
        &tl,
        &st,
        svc.progress(tl).commit_lsn,
    )))
}

async fn timeline<S: WalStore>(
    State(svc): State<Arc<WalService<S>>>,
    Path((tenant, timeline)): Path<(String, String)>,
) -> Result<Json<TimelineStatus>, ApiError> {
    let tl = TimelineId::new(tenant.parse::<Id>()?, timeline.parse::<Id>()?);
    let st = svc.store().load(&tl).await?.ok_or(Error::NotFound(tl))?;
    Ok(Json(TimelineStatus::of(
        &tl,
        &st,
        svc.progress(tl).commit_lsn,
    )))
}
