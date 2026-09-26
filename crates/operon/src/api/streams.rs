//! The native produce path (plan M1.2 Task 13 rule 7), shared by the HTTP
//! produce route and Flight `DoPut` ingest ([`NativeStreamProducer`]).

use std::sync::Arc;

use axum::http::StatusCode;
use operon_collection::{PrimaryKey, partition_of};
use operon_common::StreamId;
use operon_common::meta::{Collection, Consistency, MetaStore};
use operon_log::{AppendAck, LogWriter, Record};
use operon_query::ServiceError;
use operon_query::flight_ingest::StreamProducer;

use super::{ApiError, IMPLICIT_STREAM_PREFIX, namespace_id, stream_id};

/// Appends `records` (per partition) to stream `stream` of namespace `ns`
/// in one `append_many`, after Task 11 rule 4's check for an implicit
/// stream (`_collection.*`): every record key must be a primary key of its
/// partition.
pub async fn produce_records(
    meta: &Arc<dyn MetaStore>,
    writer: &LogWriter,
    ns: &str,
    stream: &str,
    records: Vec<(u32, Vec<Record>)>,
) -> Result<Vec<AppendAck>, ApiError> {
    let id = stream_id(&**meta, ns, stream).await?;
    if stream.starts_with(IMPLICIT_STREAM_PREFIX) {
        let collection = implicit_collection(&**meta, ns, stream, id).await?;
        for (partition, records) in &records {
            for (i, record) in records.iter().enumerate() {
                check_implicit_key(&collection, *partition, i, record.key.as_deref())?;
            }
        }
    }
    Ok(writer.append_many(id, records).await?)
}

/// The collection whose implicit stream is `stream` (rule 4.1, through
/// `Collection.stream`).
async fn implicit_collection(
    meta: &dyn MetaStore,
    ns: &str,
    stream: &str,
    id: StreamId,
) -> Result<Collection, ApiError> {
    let namespace = namespace_id(meta, ns).await?;
    meta.collections(Consistency::Local, Some(namespace))
        .await?
        .into_iter()
        .find(|collection| collection.stream == id)
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "stream {ns}/{stream} belongs to no collection; it was dropped"
            ))
        })
}

/// Rule 4.2: a record produced onto `collection`'s implicit stream must be
/// keyed by a primary key of `partition` (Ruling 17), or the link would
/// break per-key order.
fn check_implicit_key(
    collection: &Collection,
    partition: u32,
    i: usize,
    key: Option<&[u8]>,
) -> Result<(), ApiError> {
    let belongs = key
        .and_then(|key| PrimaryKey::from_canonical(key).ok())
        .is_some_and(|pk| partition_of(&pk, collection.partitions) == partition);
    if belongs {
        Ok(())
    } else {
        Err(ApiError::invalid(format!(
            "record {i}: key does not belong to partition {partition}"
        )))
    }
}

/// The produce path as Flight ingest's [`StreamProducer`].
#[derive(Clone, Debug)]
pub struct NativeStreamProducer {
    pub meta: Arc<dyn MetaStore>,
    pub writer: LogWriter,
}

/// An [`ApiError`] as a service error, by status (rule 7), keeping the
/// message.
fn service_error(err: ApiError, stream: &str) -> ServiceError {
    let message = err.message().to_string();
    match err.status() {
        StatusCode::BAD_REQUEST => ServiceError::InvalidArgument(message),
        StatusCode::NOT_FOUND => ServiceError::NotFound {
            kind: "stream",
            name: stream.to_string(),
        },
        StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE => {
            ServiceError::Unavailable(message)
        }
        _ => ServiceError::Internal(message),
    }
}

#[async_trait::async_trait]
impl StreamProducer for NativeStreamProducer {
    async fn partitions(&self, ns: &str, stream: &str) -> Result<u32, ServiceError> {
        let not_found = || ServiceError::NotFound {
            kind: "stream",
            name: stream.to_string(),
        };
        let namespace = self
            .meta
            .namespace_by_name(Consistency::Local, ns)
            .await
            .map_err(|err| service_error(err.into(), stream))?
            .ok_or_else(not_found)?;
        self.meta
            .stream_by_name(Consistency::Local, namespace.id, stream)
            .await
            .map_err(|err| service_error(err.into(), stream))?
            .map(|stream| stream.partitions)
            .ok_or_else(not_found)
    }

    async fn produce(
        &self,
        ns: &str,
        stream: &str,
        records: Vec<(u32, Vec<Record>)>,
    ) -> Result<Vec<AppendAck>, ServiceError> {
        produce_records(&self.meta, &self.writer, ns, stream, records)
            .await
            .map_err(|err| service_error(err, stream))
    }
}
