//! [`TikvConfig`]: how a [`Tikv`](crate::Tikv) handle reaches its cluster.

use std::time::Duration;

use crate::TikvError;

/// The default request timeout.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// How a [`Tikv`](crate::Tikv) handle reaches its cluster and which part of it
/// the handle owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TikvConfig {
    /// PD endpoints (`host:port`, or `http://host:port`).
    pub pd: Vec<String>,
    /// The keyspace every key of the handle lives in. Loam needs one: an
    /// empty name is refused with [`TikvError::ApiVersion`] at connect.
    pub keyspace: String,
    /// The prefix every key of the handle lives under, inside the keyspace
    /// (R1 Ruling 1: tests isolate by root, not by keyspace).
    pub root: Vec<u8>,
    /// The timeout of each request to PD and TiKV (default 5 s).
    pub request_timeout: Duration,
    /// PD's HTTP API base URL; `None` means `http://<pd[0]>`.
    pub pd_http: Option<String>,
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
    }
}
