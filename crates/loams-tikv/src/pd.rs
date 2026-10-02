//! PD's GC RPCs over gRPC (R1 plan Task 3, rows R5–R6).
//!
//! This module calls PD through the generated `pdpb` client that Loams's
//! `tikv-client` fork exposes (`tikv_client::proto`, R1 plan row F1), over the
//! tonic version that client is generated for (`tikv_client::proto::tonic`).
//! It uses four RPCs, the ones PD v8.5.8 implements for cluster GC:
//! `GetMembers` (to find the leader and the cluster id), `GetGCSafePoint`,
//! `UpdateGCSafePoint` and `UpdateServiceGCSafePoint`. PD master's GC-state
//! RPCs (`AdvanceTxnSafePoint`, `AdvanceGCSafePoint`, `SetGCBarrier`,
//! `GetGCState`) answer `Unimplemented` on v8.5.8 and are not used.

use std::time::Duration;

use tikv_client::proto;
use tikv_client::proto::tonic;
use tikv_client::proto::tonic::transport::{Channel, Endpoint};

use crate::TikvError;

use proto::pdpb::{
    GetGcSafePointRequest, GetMembersRequest, RequestHeader, ResponseHeader,
    UpdateGcSafePointRequest, UpdateServiceGcSafePointRequest, pd_client::PdClient,
};

/// The service id of the cluster GC worker's service safe point, as TiDB's GC
/// worker names it. PD requires its TTL to be infinite (`i64::MAX`).
pub(crate) const GC_WORKER_SERVICE: &str = "gc_worker";

/// The answer of `UpdateServiceGCSafePoint`: the minimum service safe point
/// after the update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MinServiceSafePoint {
    pub(crate) service_id: String,
    pub(crate) safe_point: u64,
}

/// A connection to the PD leader, made on first use and dropped after a
/// transport error, so the next call finds the leader again.
pub(crate) struct Pd {
    endpoints: Vec<String>,
    timeout: Duration,
    conn: tokio::sync::Mutex<Option<Conn>>,
}

#[derive(Clone)]
struct Conn {
    client: PdClient<Channel>,
    cluster_id: u64,
}

impl std::fmt::Debug for Pd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pd")
            .field("endpoints", &self.endpoints)
            .finish_non_exhaustive()
    }
}

impl Pd {
    pub(crate) fn new(endpoints: Vec<String>, timeout: Duration) -> Self {
        Pd {
            endpoints,
            timeout,
            conn: tokio::sync::Mutex::new(None),
        }
    }

    /// `GetGCSafePoint`: the cluster's GC safe point, as a TSO version.
    pub(crate) async fn gc_safe_point(&self) -> Result<u64, TikvError> {
        const OP: &str = "GetGCSafePoint";
        let mut conn = self.conn(OP).await?;
        let header = Some(conn.header());
        let answer = conn
            .client
            .get_gc_safe_point(GetGcSafePointRequest { header })
            .await;
        let resp = self.answer(OP, answer).await?;
        check_header(OP, resp.header.as_ref())?;
        Ok(resp.safe_point)
    }

    /// `UpdateGCSafePoint(safe_point)`: returns the cluster safe point after
    /// the call, which PD never moves backwards.
    pub(crate) async fn update_gc_safe_point(&self, safe_point: u64) -> Result<u64, TikvError> {
        const OP: &str = "UpdateGCSafePoint";
        let mut conn = self.conn(OP).await?;
        let header = Some(conn.header());
        let answer = conn
            .client
            .update_gc_safe_point(UpdateGcSafePointRequest { header, safe_point })
            .await;
        let resp = self.answer(OP, answer).await?;
        check_header(OP, resp.header.as_ref())?;
        Ok(resp.new_safe_point)
    }

    /// `UpdateServiceGCSafePoint(service_id, ttl, safe_point)`. PD saves the
    /// service safe point unless `safe_point` is below the current minimum
    /// (then it saves nothing), deletes it when `ttl ≤ 0`, drops expired ones,
    /// and answers the minimum after the call.
    pub(crate) async fn update_service_safe_point(
        &self,
        service_id: &str,
        ttl_secs: i64,
        safe_point: u64,
    ) -> Result<MinServiceSafePoint, TikvError> {
        const OP: &str = "UpdateServiceGCSafePoint";
        let mut conn = self.conn(OP).await?;
        let header = Some(conn.header());
        let answer = conn
            .client
            .update_service_gc_safe_point(UpdateServiceGcSafePointRequest {
                header,
                service_id: service_id.as_bytes().to_vec(),
                ttl: ttl_secs,
                safe_point,
            })
            .await;
        let resp = self.answer(OP, answer).await?;
        check_header(OP, resp.header.as_ref())?;
        Ok(MinServiceSafePoint {
            service_id: String::from_utf8_lossy(&resp.service_id).into_owned(),
            safe_point: resp.min_safe_point,
        })
    }

    async fn conn(&self, op: &'static str) -> Result<Conn, TikvError> {
        let mut conn = self.conn.lock().await;
        if let Some(conn) = conn.as_ref() {
            return Ok(conn.clone());
        }
        let fresh = self.connect(op).await?;
        *conn = Some(fresh.clone());
        Ok(fresh)
    }

    /// Maps a gRPC answer; a failed call drops the connection, so the next
    /// call finds the leader again.
    async fn answer<T>(
        &self,
        op: &'static str,
        answer: Result<tonic::Response<T>, tonic::Status>,
    ) -> Result<T, TikvError> {
        match answer {
            Ok(resp) => Ok(resp.into_inner()),
            Err(status) => {
                *self.conn.lock().await = None;
                Err(TikvError::PdGrpc {
                    op,
                    message: format!("{:?}: {}", status.code(), status.message()),
                })
            }
        }
    }

    /// Connects to the first PD endpoint that answers `GetMembers`, then to
    /// the leader it names.
    async fn connect(&self, op: &'static str) -> Result<Conn, TikvError> {
        let mut last = String::from("no PD endpoint");
        for endpoint in &self.endpoints {
            let base = url(endpoint);
            let mut client = match self.channel(&base).await {
                Ok(channel) => PdClient::new(channel),
                Err(e) => {
                    last = format!("{base}: {e}");
                    continue;
                }
            };
            let members = client
                .get_members(GetMembersRequest {
                    header: Some(RequestHeader::default()),
                })
                .await;
            let members = match members {
                Ok(resp) => resp.into_inner(),
                Err(status) => {
                    last = format!("{base}: GetMembers: {}", status.message());
                    continue;
                }
            };
            if let Err(e) = check_header("GetMembers", members.header.as_ref()) {
                last = format!("{base}: {e}");
                continue;
            }
            let cluster_id = members.header.as_ref().map_or(0, |h| h.cluster_id);
            let leader_url = members
                .leader
                .as_ref()
                .and_then(|l| l.client_urls.first())
                .map(|u| url(u));
            let client = match leader_url {
                Some(leader) if leader != base => match self.channel(&leader).await {
                    Ok(channel) => PdClient::new(channel),
                    Err(e) => {
                        last = format!("PD leader {leader}: {e}");
                        continue;
                    }
                },
                _ => client,
            };
            return Ok(Conn { client, cluster_id });
        }
        Err(TikvError::PdGrpc { op, message: last })
    }

    async fn channel(&self, url: &str) -> Result<Channel, String> {
        Endpoint::from_shared(url.to_string())
            .map_err(|e| e.to_string())?
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .connect()
            .await
            .map_err(|e| e.to_string())
    }
}

impl Conn {
    fn header(&self) -> RequestHeader {
        RequestHeader {
            cluster_id: self.cluster_id,
            ..RequestHeader::default()
        }
    }
}

/// `http://host:port` for a PD endpoint given as `host:port` or a URL.
fn url(endpoint: &str) -> String {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

/// PD reports request errors in the response header.
fn check_header(op: &'static str, header: Option<&ResponseHeader>) -> Result<(), TikvError> {
    match header.and_then(|h| h.error.as_ref()) {
        Some(e) if e.r#type != 0 => Err(TikvError::PdGrpc {
            op,
            message: format!(
                "{}: {}",
                proto::pdpb::ErrorType::try_from(e.r#type)
                    .map(|t| t.as_str_name().to_string())
                    .unwrap_or_else(|_| e.r#type.to_string()),
                e.message
            ),
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::pdpb::{Error, ErrorType};

    #[test]
    fn urls_get_a_scheme() {
        assert_eq!(url("127.0.0.1:19379"), "http://127.0.0.1:19379");
        assert_eq!(url("http://pd:2379/"), "http://pd:2379");
    }

    #[test]
    fn header_errors_are_reported() {
        assert!(check_header("op", None).is_ok());
        let ok = ResponseHeader {
            cluster_id: 1,
            error: Some(Error {
                r#type: ErrorType::Ok as i32,
                message: String::new(),
            }),
        };
        assert!(check_header("op", Some(&ok)).is_ok());
        let bad = ResponseHeader {
            cluster_id: 1,
            error: Some(Error {
                r#type: ErrorType::InvalidValue as i32,
                message: "ttl".to_string(),
            }),
        };
        match check_header("op", Some(&bad)) {
            Err(TikvError::PdGrpc { message, .. }) => {
                assert_eq!(message, "INVALID_VALUE: ttl");
            }
            other => panic!("expected PdGrpc, got {other:?}"),
        }
    }
}
