//! [`TikvConfig`]: how a [`Tikv`](crate::Tikv) handle reaches its cluster.

use std::time::Duration;

use crate::TikvError;
use crate::runner::CommitMode;

/// The default request timeout.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The default GC life time: versions younger than this are kept (design §20
/// §9.3). Reads older than `now − (life time − 1 min)` are refused (row R7).
pub const DEFAULT_GC_LIFE_TIME: Duration = Duration::from_secs(10 * 60);

/// The margin of the GC safe window: reads are refused one minute before GC
/// may reach them.
pub const GC_SAFE_MARGIN: Duration = Duration::from_secs(60);

/// The default gRPC decoding limit of the client (16 MiB, up from 4 MiB).
/// Paged scans and batch gets, halving on `OutOfRange`, are the guarantee;
/// the raised limit is headroom (row R10).
pub const DEFAULT_GRPC_MAX_DECODING_BYTES: usize = 16 * 1024 * 1024;

/// How a [`Tikv`](crate::Tikv) handle reaches its cluster and which part of it
/// the handle owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TikvConfig {
    /// PD endpoints (`host:port`, or `http://host:port`).
    pub pd: Vec<String>,
    /// The keyspace every key of the handle lives in. Loams needs one: an
    /// empty name is refused with [`TikvError::ApiVersion`] at connect.
    pub keyspace: String,
    /// The prefix every key of the handle lives under, inside the keyspace
    /// (R1 Ruling 1: tests isolate by root, not by keyspace).
    pub root: Vec<u8>,
    /// The timeout of each request to PD and TiKV (default 5 s).
    pub request_timeout: Duration,
    /// PD's HTTP API base URL; `None` means `http://<pd[0]>`.
    pub pd_http: Option<String>,
    /// How transactions commit unless their options say otherwise (default
    /// async commit with 1PC, R1 Ruling 3).
    pub commit_mode: CommitMode,
    /// The cluster's GC life time (default 10 min); must exceed 1 min.
    pub gc_life_time: Duration,
    /// The client's gRPC decoding limit (default 16 MiB, at least 4 MiB).
    pub grpc_max_decoding_bytes: usize,
}

impl TikvConfig {
    /// A configuration with an empty root and the default timeout.
    pub fn new(pd: Vec<String>, keyspace: impl Into<String>) -> Self {
        TikvConfig {
            pd,
            keyspace: keyspace.into(),
            root: Vec::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            pd_http: None,
            commit_mode: CommitMode::default(),
            gc_life_time: DEFAULT_GC_LIFE_TIME,
            grpc_max_decoding_bytes: DEFAULT_GRPC_MAX_DECODING_BYTES,
        }
    }

    /// PD's HTTP API base URL, without a trailing slash.
    pub fn pd_http_url(&self) -> String {
        let base = match &self.pd_http {
            Some(url) => url.clone(),
            None => {
                let first = self.pd.first().map(String::as_str).unwrap_or_default();
                if first.starts_with("http://") || first.starts_with("https://") {
                    first.to_string()
                } else {
                    format!("http://{first}")
                }
            }
        };
        base.trim_end_matches('/').to_string()
    }

    pub(crate) fn validate(&self) -> Result<(), TikvError> {
        if self.pd.is_empty() || self.pd.iter().any(|p| p.trim().is_empty()) {
            return Err(TikvError::Config(
                "TikvConfig.pd needs at least one PD endpoint".to_string(),
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(TikvError::Config(
                "TikvConfig.request_timeout must be positive".to_string(),
            ));
        }
        if self.gc_life_time <= GC_SAFE_MARGIN {
            return Err(TikvError::Config(
                "TikvConfig.gc_life_time must exceed one minute".to_string(),
            ));
        }
        if self.grpc_max_decoding_bytes < 4 * 1024 * 1024 {
            return Err(TikvError::Config(
                "TikvConfig.grpc_max_decoding_bytes must be at least 4 MiB".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pd_http_defaults_to_the_first_endpoint() {
        let c = TikvConfig::new(vec!["127.0.0.1:19379".into(), "h:2".into()], "k");
        assert_eq!(c.pd_http_url(), "http://127.0.0.1:19379");
        let c = TikvConfig::new(vec!["http://pd:2379/".into()], "k");
        assert_eq!(c.pd_http_url(), "http://pd:2379");
        let c = TikvConfig {
            pd_http: Some("http://other:1/".into()),
            ..TikvConfig::new(vec!["pd:2379".into()], "k")
        };
        assert_eq!(c.pd_http_url(), "http://other:1");
    }

    #[test]
    fn validate_refuses_no_pd_and_zero_timeout() {
        assert!(TikvConfig::new(vec![], "k").validate().is_err());
        assert!(TikvConfig::new(vec![" ".into()], "k").validate().is_err());
        let c = TikvConfig {
            request_timeout: Duration::ZERO,
            ..TikvConfig::new(vec!["pd:1".into()], "k")
        };
        assert!(c.validate().is_err());
        assert!(TikvConfig::new(vec!["pd:1".into()], "k").validate().is_ok());
        let c = TikvConfig {
            gc_life_time: GC_SAFE_MARGIN,
            ..TikvConfig::new(vec!["pd:1".into()], "k")
        };
        assert!(c.validate().is_err());
        let c = TikvConfig {
            grpc_max_decoding_bytes: 1024,
            ..TikvConfig::new(vec!["pd:1".into()], "k")
        };
        assert!(c.validate().is_err());
    }
}
