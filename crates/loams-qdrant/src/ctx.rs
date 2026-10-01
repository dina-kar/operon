//! The per-request context: namespace, read consistency and timeout
//! (overview §6.9; Ruling 14).

use std::future::Future;
use std::time::{Duration, Instant};

use loams_collection::ConsistencyToken;
use loams_query::ReadConsistency;

use crate::error::GatewayError;
use crate::{NAMESPACE_HEADER, QdrantConfig, TOKEN_HEADER};

/// What every handler knows about its request.
#[derive(Clone, Debug)]
pub struct RequestCtx {
    /// `Loams-Namespace`, or the configured namespace.
    pub ns: String,
    /// `AtLeast(token)` with a `Loams-Consistency-Token`, else `Strong`.
    /// Qdrant's own `consistency` parameter is not read (Ruling 14).
    pub consistency: ReadConsistency,
    /// The request's `timeout` (seconds), when given.
    pub timeout: Option<Duration>,
    /// When the handler started; the envelope's `time` counts from here.
    pub started: Instant,
}

impl RequestCtx {
    /// From HTTP headers and the `timeout` query parameter.
    pub fn from_http(
        headers: &http::HeaderMap,
        timeout_s: Option<u64>,
        config: &QdrantConfig,
    ) -> Result<Self, GatewayError> {
        let get = |key: &str| headers.get(key).map(http::HeaderValue::as_bytes);
        Self::build(get(NAMESPACE_HEADER), get(TOKEN_HEADER), timeout_s, config)
    }

    /// From gRPC metadata and the message's `timeout` field (seconds).
    pub fn from_grpc(
        meta: &tonic::metadata::MetadataMap,
        timeout_s: Option<u64>,
        config: &QdrantConfig,
    ) -> Result<Self, GatewayError> {
        let get = |key: &str| {
            meta.get(key)
                .map(tonic::metadata::MetadataValue::as_encoded_bytes)
        };
        Self::build(get(NAMESPACE_HEADER), get(TOKEN_HEADER), timeout_s, config)
    }

    fn build(
        ns: Option<&[u8]>,
        token: Option<&[u8]>,
        timeout_s: Option<u64>,
        config: &QdrantConfig,
    ) -> Result<Self, GatewayError> {
        let started = Instant::now();
        let utf8 = |what: &str, bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|_| GatewayError::BadRequest(format!("{what} is not valid UTF-8")))
        };
        let ns = match ns {
            Some(bytes) => utf8("Loams-Namespace", bytes)?,
            None => String::new(),
        };
        let ns = if ns.is_empty() {
            config.namespace.clone()
        } else {
            ns
        };
        let consistency = match token {
            None => ReadConsistency::Strong,
            Some(bytes) => {
                let text = utf8("Loams-Consistency-Token", bytes)?;
                let token = text.parse::<ConsistencyToken>().map_err(|err| {
                    GatewayError::BadRequest(format!("invalid Loams-Consistency-Token: {err}"))
                })?;
                ReadConsistency::AtLeast(token)
            }
        };
        Ok(Self {
            ns,
            consistency,
            timeout: timeout_s.map(Duration::from_secs),
            started,
        })
    }

    /// Runs `fut`, bounded by the request's timeout: past it the answer is
    /// [`GatewayError::Timeout`].
    pub async fn run<T>(
        &self,
        fut: impl Future<Output = Result<T, GatewayError>>,
    ) -> Result<T, GatewayError> {
        match self.timeout {
            None => fut.await,
            Some(timeout) => tokio::time::timeout(timeout, fut)
                .await
                .unwrap_or(Err(GatewayError::Timeout(timeout))),
        }
    }

    /// Seconds since the handler started: the envelope's `time`.
    pub fn elapsed_secs(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }
}
