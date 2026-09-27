//! Keyspace bootstrap through PD's HTTP API (`/pd/api/v2/keyspaces`).
//!
//! PD v8.5.8 answers HTTP 500 both for a missing keyspace (`GET`, body
//! "keyspace does not exist") and for a duplicate create (`POST`, body
//! "keyspace already exists"); those bodies are the idempotency signals (R1
//! plan rows R11 and X10).

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use crate::TikvError;
use crate::config::DEFAULT_REQUEST_TIMEOUT;

/// A keyspace as PD's HTTP API reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct KeyspaceMeta {
    /// The keyspace id TiKV prefixes its keys with.
    pub id: u32,
    pub name: String,
    /// `ENABLED`, `DISABLED`, `ARCHIVED` or `TOMBSTONE`.
    pub state: String,
    /// Unix seconds.
    #[serde(default)]
    pub created_at: i64,
    /// Unix seconds.
    #[serde(default)]
    pub state_changed_at: i64,
    /// Empty for pre-allocated keyspaces.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}

/// Returns the keyspace `name`, creating it first if PD has none of that name.
/// Idempotent, and safe against a concurrent create.
pub async fn ensure_keyspace(pd_http: &str, name: &str) -> Result<KeyspaceMeta, TikvError> {
    validate_name(name)?;
    let http = http_client(DEFAULT_REQUEST_TIMEOUT)?;
    let base = pd_http.trim_end_matches('/');
    if let Some(meta) = get(&http, base, name).await? {
        return Ok(meta);
    }
    if let Some(meta) = create(&http, base, name).await? {
        tracing::info!(keyspace = name, id = meta.id, "created TiKV keyspace");
        return Ok(meta);
    }
    // Someone else created it between our GET and POST.
    get(&http, base, name).await?.ok_or_else(|| TikvError::Pd {
        op: "GET keyspace after a concurrent create",
        status: 500,
        body: "keyspace does not exist".to_string(),
    })
}

pub(crate) fn http_client(timeout: Duration) -> Result<reqwest::Client, TikvError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| TikvError::Http {
            op: "build client",
            message: e.to_string(),
        })
}

/// `GET /pd/api/v2/keyspaces/<name>`; `None` when PD says it does not exist.
pub(crate) async fn get(
    http: &reqwest::Client,
    base: &str,
    name: &str,
) -> Result<Option<KeyspaceMeta>, TikvError> {
    const OP: &str = "GET keyspace";
    validate_name(name)?;
    let (status, body) = send(OP, http.get(format!("{base}/pd/api/v2/keyspaces/{name}"))).await?;
    match classify(status, &body) {
        Answer::Ok => parse(OP, &body).map(Some),
        Answer::DoesNotExist => Ok(None),
        Answer::AlreadyExists | Answer::Other => Err(TikvError::Pd {
            op: OP,
            status,
            body,
        }),
    }
}

/// `POST /pd/api/v2/keyspaces`; `None` when PD says it already exists.
async fn create(
    http: &reqwest::Client,
    base: &str,
    name: &str,
) -> Result<Option<KeyspaceMeta>, TikvError> {
    const OP: &str = "POST keyspace";
    let request = http
        .post(format!("{base}/pd/api/v2/keyspaces"))
        .json(&serde_json::json!({ "name": name }));
    let (status, body) = send(OP, request).await?;
    match classify(status, &body) {
        Answer::Ok => parse(OP, &body).map(Some),
        Answer::AlreadyExists => Ok(None),
        Answer::DoesNotExist | Answer::Other => Err(TikvError::Pd {
            op: OP,
            status,
            body,
        }),
    }
}

async fn send(
    op: &'static str,
    request: reqwest::RequestBuilder,
) -> Result<(u16, String), TikvError> {
    let response = request.send().await.map_err(|e| TikvError::Http {
        op,
        message: e.to_string(),
    })?;
    let status = response.status().as_u16();
    let body = response.text().await.map_err(|e| TikvError::Http {
        op,
        message: e.to_string(),
    })?;
    Ok((status, body))
}

fn parse(op: &'static str, body: &str) -> Result<KeyspaceMeta, TikvError> {
    serde_json::from_str(body).map_err(|e| TikvError::Http {
        op,
        message: format!("unreadable keyspace ({e}): {body}"),
    })
}

#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Ok,
    DoesNotExist,
    AlreadyExists,
    Other,
}

/// PD v8.5.8 signals both conditions with HTTP 500 and a text body.
fn classify(status: u16, body: &str) -> Answer {
    match status {
        200 => Answer::Ok,
        500 if body.contains("keyspace does not exist") => Answer::DoesNotExist,
        500 if body.contains("keyspace already exists") => Answer::AlreadyExists,
        _ => Answer::Other,
    }
}

/// PD accepts 1–64 characters of `[A-Za-z0-9_-]`; checking here also keeps
/// the name safe to put in a URL path.
fn validate_name(name: &str) -> Result<(), TikvError> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(TikvError::Config(format!(
            "keyspace name '{name}' must be 1-64 characters of [A-Za-z0-9_-]"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_reads_pd_v8_5_8_bodies() {
        assert_eq!(classify(200, "{}"), Answer::Ok);
        assert_eq!(
            classify(500, "\"keyspace does not exist\""),
            Answer::DoesNotExist
        );
        assert_eq!(
            classify(500, "\"keyspace already exists\""),
            Answer::AlreadyExists
        );
        assert_eq!(classify(500, "\"etcd is down\""), Answer::Other);
        assert_eq!(classify(404, "keyspace does not exist"), Answer::Other);
    }

    #[test]
    fn names_are_checked() {
        assert!(validate_name("loam_test_meta").is_ok());
        assert!(validate_name("a-b_9").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn keyspace_meta_parses_pd_json() {
        let body = r#"{"id":4,"name":"loam_test_meta","state":"ENABLED",
                       "created_at":1790000000,"state_changed_at":1790000000}"#;
        let meta = parse("test", body).unwrap();
        assert_eq!(meta.id, 4);
        assert_eq!(meta.name, "loam_test_meta");
        assert!(meta.config.is_empty());
    }
}
