//! The hot tier as the query engine sees it (plan M1.2 Task 1; overview
//! A13, A19, A23): the fixed [`HotTier`]/[`HotAnn`] contract M1.3
//! implements, the per-request hot switch carried by a task-local scope
//! (Ruling 11), the [`HotLayer`] that sets it from the `Operon-Hot` header and
//! reports `Operon-Hot-Used`, and what a read reports about the hot
//! structures it used.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use operon_common::{CollectionId, NamespaceId};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::error::ServiceError;

/// The hot tier of this node. The first three methods are the fixed contract
/// (overview §6.9); `status` is an addition with a default.
pub trait HotTier: Send + Sync + std::fmt::Debug {
    /// An ANN index for `column` reflecting manifest `source_version <= manifest_version`; None → durable path.
    fn ann(
        &self,
        ns: NamespaceId,
        cid: CollectionId,
        column: &str,
        manifest_version: u64,
    ) -> Option<Arc<dyn HotAnn>>;
    /// A local file holding the whole split when it is pinned on this node; None → range reads through the cache.
    fn split_file(&self, ns: NamespaceId, cid: CollectionId, split: ulid::Ulid) -> Option<PathBuf>;
    /// Access accounting for promotion (called once per read of a collection).
    fn record_access(&self, ns: NamespaceId, cid: CollectionId);
    /// Addition (provided): the hot status reported by `GET …/collections/{c}`.
    fn status(&self, _ns: NamespaceId, _cid: CollectionId) -> HotStatus {
        HotStatus::default()
    }
}

/// A hot ANN artifact of one vector column.
#[async_trait::async_trait]
pub trait HotAnn: Send + Sync + std::fmt::Debug {
    fn source_version(&self) -> u64;
    /// Row ids covered by the artifact; rows outside are searched on the durable path and merged.
    fn covered(&self) -> &RoaringTreemap;
    /// Approximate top-k over covered rows, restricted to `allow` when given; scores are approximate (the caller rescores exactly, R12).
    async fn search(
        &self,
        query: &[f32],
        k: usize,
        allow: Option<&RoaringTreemap>,
        ef: Option<u32>,
    ) -> Result<Vec<(u64, f32)>, HotError>;
}

/// No hot tier: every read takes the durable path.
#[derive(Debug, Default)]
pub struct NoHotTier;

impl HotTier for NoHotTier {
    fn ann(&self, _: NamespaceId, _: CollectionId, _: &str, _: u64) -> Option<Arc<dyn HotAnn>> {
        None
    }

    fn split_file(&self, _: NamespaceId, _: CollectionId, _: ulid::Ulid) -> Option<PathBuf> {
        None
    }

    fn record_access(&self, _: NamespaceId, _: CollectionId) {}
}

#[derive(Debug, thiserror::Error)]
pub enum HotError {
    #[error("hot artifact unavailable: {0}")]
    Unavailable(String),
    #[error("hot artifact failed: {0}")]
    Failed(String),
}

/// A hot structure a read can use; declaration order is the sorted order of
/// the `Operon-Hot-Used` header. Fragment prefetch (H1) is never reported
/// (overview A19).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotKind {
    Hnsw,
    Splits,
}

/// The `hot` value of `CollectionInfo`. M1.3's `GET …/collections/{c}`
/// handler replaces it with the owner's full status, a superset whose
/// `vectors`, `text` and `fragments` objects each carry these `state` and
/// `source_version` keys.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotStatus {
    pub vectors: HotState,
    pub text: HotState,
    pub fragments: HotState,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotState {
    pub state: HotStateKind,
    /// The manifest version the structure reflects.
    pub source_version: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotStateKind {
    #[default]
    Off,
    Building,
    Ready,
}

impl HotKind {
    /// The snake-case name, as in the `Operon-Hot-Used` header.
    pub fn name(self) -> &'static str {
        match self {
            HotKind::Hnsw => "hnsw",
            HotKind::Splits => "splits",
        }
    }
}

/// The hot structures one request used. Clones share one set.
#[derive(Clone, Debug, Default)]
pub struct HotUsed(Arc<Mutex<BTreeSet<HotKind>>>);

impl HotUsed {
    pub fn record(&self, kind: HotKind) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(kind);
    }

    pub fn kinds(&self) -> BTreeSet<HotKind> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The recorded kinds joined with `,` in [`HotKind`] order, or `none`.
    pub fn header_value(&self) -> String {
        let kinds = self.kinds();
        if kinds.is_empty() {
            return "none".to_string();
        }
        kinds
            .into_iter()
            .map(HotKind::name)
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The hot switch of one request and what it used.
#[derive(Clone, Debug)]
pub struct RequestHot {
    pub enabled: bool,
    pub used: HotUsed,
}

tokio::task_local! {
    static HOT_SCOPE: RequestHot;
}

/// Runs `fut` with `hot` as the request's hot scope.
pub async fn scope<F: Future>(hot: RequestHot, fut: F) -> F::Output {
    HOT_SCOPE.scope(hot, fut).await
}

/// The current request's hot scope; `None` outside one.
pub fn current() -> Option<RequestHot> {
    HOT_SCOPE.try_with(RequestHot::clone).ok()
}

/// The request header that switches the hot tier (gRPC metadata of the same
/// name too).
pub const HOT_HEADER: &str = "operon-hot";
/// The response header naming the hot structures used.
pub const HOT_USED_HEADER: &str = "operon-hot-used";

/// `on` → true, `off` → false (ASCII case-insensitive); else `InvalidArgument`.
pub fn parse_hot_header(value: &str) -> Result<bool, ServiceError> {
    if value.eq_ignore_ascii_case("on") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("off") {
        Ok(false)
    } else {
        Err(ServiceError::InvalidArgument(format!(
            "invalid Operon-Hot header: {value} (expected on or off)"
        )))
    }
}

/// Sets each request's hot scope from `Operon-Hot` and reports
/// `Operon-Hot-Used` (Ruling 11).
#[derive(Clone, Debug)]
pub struct HotLayer {
    default_enabled: bool,
}

impl HotLayer {
    /// `default_enabled` applies to requests without the header.
    pub fn new(default_enabled: bool) -> Self {
        Self { default_enabled }
    }
}

impl<S> tower::Layer<S> for HotLayer {
    type Service = HotService<S>;

    fn layer(&self, inner: S) -> HotService<S> {
        HotService {
            inner,
            default_enabled: self.default_enabled,
        }
    }
}

/// The service [`HotLayer`] makes.
#[derive(Clone, Debug)]
pub struct HotService<S> {
    inner: S,
    default_enabled: bool,
}

/// 400 with the `invalid_argument` body of an invalid header.
fn bad_header<R: From<String>>(err: &ServiceError) -> http::Response<R> {
    let message = match err {
        ServiceError::InvalidArgument(message) => message.clone(),
        other => other.to_string(),
    };
    let body = serde_json::json!({"error": "invalid_argument", "message": message}).to_string();
    let mut response = http::Response::new(R::from(body));
    *response.status_mut() = http::StatusCode::BAD_REQUEST;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}

impl<S, B, R> tower::Service<http::Request<B>> for HotService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
    R: From<String> + Send + 'static,
{
    type Response = http::Response<R>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<http::Response<R>, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let enabled = match request.headers().get(HOT_HEADER) {
            None => Ok(self.default_enabled),
            Some(value) => parse_hot_header(&String::from_utf8_lossy(value.as_bytes())),
        };
        let enabled = match enabled {
            Ok(enabled) => enabled,
            Err(err) => {
                let response = bad_header(&err);
                return Box::pin(async move { Ok(response) });
            }
        };
        // The clone that was polled ready serves this call.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let used = HotUsed::default();
        let hot = RequestHot {
            enabled,
            used: used.clone(),
        };
        Box::pin(async move {
            let mut response = scope(hot, async move { inner.call(request).await }).await?;
            if !response.headers().contains_key(HOT_USED_HEADER)
                && let Ok(value) = http::HeaderValue::from_str(&used.header_value())
            {
                response.headers_mut().insert(HOT_USED_HEADER, value);
            }
            Ok(response)
        })
    }
}
