//! Snapshots are manifest versions (Ruling 19): create names the newest
//! retained version, list returns them all; neither writes anything. Before
//! the first commit the collection is version 0, the empty collection (row
//! T10-3).

use std::time::{Duration, Instant};

use loams_query::{CollectionInfo, ManifestInfo, ServiceError};
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::macros::format_description;

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;

/// `<collection>-<version:020>.snapshot`.
pub fn snapshot_name(collection: &str, m: &ManifestInfo) -> String {
    format!("{collection}-{:020}.snapshot", m.version)
}

/// `created_at_ms` in UTC as `%Y-%m-%dT%H:%M:%S%.6f`.
pub fn creation_time(m: &ManifestInfo) -> String {
    let at = OffsetDateTime::from_unix_timestamp_nanos(i128::from(m.created_at_ms) * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    at.format(format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]"
    ))
    .unwrap_or_default()
}

/// `SnapshotDescription {name, creation_time, size}`; no `checksum`.
pub fn snapshot_description(collection: &str, m: &ManifestInfo) -> Value {
    json!({
        "name": snapshot_name(collection, m),
        "creation_time": creation_time(m),
        "size": m.size_bytes,
    })
}

/// How long `create` waits for a collection's first manifest while the
/// link applies its first records.
const FIRST_MANIFEST_WAIT: Duration = Duration::from_secs(30);

/// Version 0: the collection before its first commit, which is empty.
fn empty_version(info: &CollectionInfo) -> ManifestInfo {
    ManifestInfo {
        version: 0,
        created_at_ms: info.created_at_ms,
        size_bytes: 0,
        live_doc_count: 0,
        lance_version: 0,
    }
}

/// The newest retained manifest. Before the first commit: version 0 when
/// there is nothing to apply, else the first manifest once the link commits
/// it (up to 30 s, then 503).
pub(crate) async fn create(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
) -> Result<ManifestInfo, GatewayError> {
    let service = gw.service();
    let deadline = Instant::now() + FIRST_MANIFEST_WAIT;
    loop {
        let versions = service.versions(&ctx.ns, &collection).await?;
        if let Some(newest) = versions.last() {
            return Ok(*newest);
        }
        let info = service.get_collection(&ctx.ns, &collection).await?;
        if info.manifest_version == 0 && info.link_lag_records == 0 {
            return Ok(empty_version(&info));
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::Unavailable("no committed manifest yet".to_string()).into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every retained manifest, oldest first; version 0 alone before the first
/// commit.
pub(crate) async fn list(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
) -> Result<Vec<ManifestInfo>, GatewayError> {
    let service = gw.service();
    let versions = service.versions(&ctx.ns, &collection).await?;
    if !versions.is_empty() {
        return Ok(versions);
    }
    let info = service.get_collection(&ctx.ns, &collection).await?;
    Ok(if info.manifest_version == 0 {
        vec![empty_version(&info)]
    } else {
        // The first manifest committed in between.
        service.versions(&ctx.ns, &collection).await?
    })
}
