//! Snapshots are manifest versions (Ruling 19): create names the newest
//! retained version, list returns them all; neither writes anything.

use operon_query::{ManifestInfo, ServiceError};
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

/// The newest retained manifest; none yet is 503.
pub(crate) async fn create(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
) -> Result<ManifestInfo, GatewayError> {
    let versions = gw.service().versions(&ctx.ns, &collection).await?;
    versions
        .last()
        .copied()
        .ok_or_else(|| ServiceError::Unavailable("no committed manifest yet".to_string()).into())
}

/// Every retained manifest, oldest first.
pub(crate) async fn list(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
) -> Result<Vec<ManifestInfo>, GatewayError> {
    Ok(gw.service().versions(&ctx.ns, &collection).await?)
}
