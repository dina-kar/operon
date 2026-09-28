//! Bulk import from object storage (D1 Task 8, design §21 §7.2):
//! `POST /v1/namespaces/{ns}/collections/{c}/import` submits a
//! `collection.import` operation and answers 202 with
//! `Location: /v1/operations/{id}` (200 for an idempotent repeat, D146).
//!
//! [`CollectionSink`] is where the operation writes: M1.2's
//! `CollectionBatchMapper` and `CollectionService::write`, with normal
//! backpressure (`WriteOptions::default()`, T0-1). A 429, `Unavailable` and a
//! timeout are transient (the slice step retries them, X7); a mapper row
//! error or a rejected op is `row_error`.

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use axum::Router;
use axum::extract::rejection::{JsonRejection, PathRejection};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use operon_collection::ConsistencyToken;
use operon_durable::import::{IdType, ImportError, ImportSink, SinkError, SinkWrite};
use operon_query::flight_ingest::{CollectionBatchMapper, IdType as MapperIdType};
use operon_query::types::{OpResult, WriteOptions};
use operon_query::{CollectionService, ServiceError};
use serde_json::Value;

use super::ApiError;
use super::operations::{OperationsSlot, idempotency_key, ops_error, submitted};

/// The import route, over `slot`.
pub fn routes(slot: OperationsSlot) -> Router {
    Router::new()
        .route("/v1/namespaces/{ns}/collections/{c}/import", post(submit))
        .with_state(slot)
}

async fn submit(
    State(slot): State<OperationsSlot>,
    path: Result<Path<(String, String)>, PathRejection>,
    headers: HeaderMap,
    body: Result<axum::Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Path((ns, collection)) = path?;
    let key = idempotency_key(&headers)?;
    let axum::Json(body) = body.map_err(|e| ApiError::invalid(e.body_text()))?;
    let ops = slot.serving()?;
    let env = slot.import().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "durable_unavailable",
            "durable execution is starting",
        )
    })?;
    let (id, created) =
        operon_durable::import::submit(&ops, &env, &ns, &collection, body, key.as_deref())
            .await
            .map_err(import_error)?;
    Ok(submitted(&id, created))
}

/// The API error of an [`ImportError`].
fn import_error(err: ImportError) -> ApiError {
    match err {
        ImportError::Invalid(message) => ApiError::invalid(message),
        ImportError::NotFound(message) => ApiError::not_found(message),
        err @ ImportError::TooManyOperations { .. } => ApiError::new(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_operations",
            err.to_string(),
        ),
        ImportError::Unavailable(message) => {
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
        }
        ImportError::Ops(err) => ops_error(err),
    }
}

/// An import's writes into collections, through the collection service.
#[derive(Debug)]
pub struct CollectionSink {
    service: Arc<CollectionService>,
}

impl CollectionSink {
    pub fn new(service: Arc<CollectionService>) -> Self {
        Self { service }
    }

    /// The collection's schema: its metastore record by name, else through
    /// an alias.
    async fn schema(
        &self,
        ns: &str,
        collection: &str,
    ) -> Result<operon_collection::CollectionSchema, ServiceError> {
        let records = self.service.collection_records(ns).await?;
        match records.into_iter().find(|c| c.name == collection) {
            Some(record) => Ok(record.schema),
            None => Ok(self.service.get_collection(ns, collection).await?.schema),
        }
    }
}

/// A service error as a sink error: backpressure, unavailability and
/// timeouts are transient (X7).
fn sink_error(err: ServiceError) -> SinkError {
    match err {
        ServiceError::ResourceExhausted {
            message,
            retry_after_ms,
        } => SinkError::Retry {
            message,
            after_ms: retry_after_ms,
        },
        ServiceError::Unavailable(message) => SinkError::Retry {
            message,
            after_ms: 0,
        },
        ServiceError::Timeout => SinkError::Retry {
            message: "the write timed out".into(),
            after_ms: 0,
        },
        err @ ServiceError::NotFound { .. } => SinkError::NotFound(err.to_string()),
        err @ (ServiceError::InvalidArgument(_) | ServiceError::SchemaViolation { .. }) => {
            SinkError::Failed {
                code: "row_error".into(),
                message: err.to_string(),
            }
        }
        err => SinkError::Failed {
            code: err.code().to_string(),
            message: err.to_string(),
        },
    }
}

#[async_trait]
impl ImportSink for CollectionSink {
    async fn check(&self, ns: &str, collection: &str) -> Result<(), SinkError> {
        self.schema(ns, collection)
            .await
            .map(|_| ())
            .map_err(sink_error)
    }

    async fn write(
        &self,
        ns: &str,
        collection: &str,
        batch: RecordBatch,
        id_type: Option<IdType>,
    ) -> Result<SinkWrite, SinkError> {
        let schema = self.schema(ns, collection).await.map_err(sink_error)?;
        let arrow = batch.schema();
        let invalid = |err: ServiceError| SinkError::Failed {
            code: "schema_mismatch".into(),
            message: err.to_string(),
        };
        let id_type =
            MapperIdType::of_schema(&arrow, id_type.map(IdType::as_str)).map_err(invalid)?;
        let mapper = CollectionBatchMapper::new(&arrow, &schema, id_type).map_err(invalid)?;
        let ops = mapper.map(&batch).map_err(|err| SinkError::Failed {
            code: "row_error".into(),
            message: format!("row {} column {}: {}", err.row, err.column, err.message),
        })?;
        let rows = ops.len() as u64;
        let result = self
            .service
            .write(ns, collection, ops, WriteOptions::default())
            .await
            .map_err(sink_error)?;
        if let Some((row, err)) = result
            .results
            .iter()
            .enumerate()
            .find_map(|(i, r)| match r {
                OpResult::Rejected(err) => Some((i, err)),
                _ => None,
            })
        {
            return Err(SinkError::Failed {
                code: "row_error".into(),
                message: format!("row {row}: {err}"),
            });
        }
        Ok(SinkWrite {
            rows,
            token: result.token.to_string(),
        })
    }

    fn merge_tokens(&self, tokens: &[String]) -> String {
        let mut merged = ConsistencyToken::default();
        for token in tokens {
            if let Ok(token) = ConsistencyToken::from_str(token) {
                merged.merge(&token);
            }
        }
        merged.to_string()
    }
}
