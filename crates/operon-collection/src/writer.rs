//! `CollectionWriter`: validates a request's ops against the collection's
//! schema and appends the valid ones to its implicit stream in one
//! `append_many` (overview §6.2, plan M1.1 Task 6).

use std::collections::BTreeMap;
use std::sync::Arc;

use operon_common::meta::{Collection, Consistency, MetaError, MetaStore};
use operon_common::{CollectionId, NamespaceId};
use operon_log::{LogError, LogWriter, Record};

use crate::codec::encode;
use crate::doc::DocOp;
use crate::pk::partition_of;
use crate::token::ConsistencyToken;
use crate::values::{DocRejection, check_document, check_patch, invalid_delete_key};

/// The most ops one write may carry.
pub const MAX_WRITE_OPS: usize = 10_000;

/// Writes document ops to collections. Cheap to clone.
#[derive(Clone, Debug)]
pub struct CollectionWriter {
    meta: Arc<dyn MetaStore>,
    log: LogWriter,
}

/// What a write did.
#[derive(Clone, Debug, PartialEq)]
pub struct WriteOutcome {
    /// Per written partition, the next offset after the write (normalized;
    /// empty when nothing was written).
    pub token: ConsistencyToken,
    /// One result per op, in input order.
    pub results: Vec<OpResult>,
    /// The schema version the ops were validated against.
    pub schema_version: u64,
}

/// The fate of one op.
#[derive(Clone, Debug, PartialEq)]
pub enum OpResult {
    Written { partition: u32, offset: u64 },
    Rejected(OpError),
}

/// Why one op was not written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpError {
    /// The op is malformed whatever the schema: an invalid key, a patch
    /// `delete_keys` entry that is not a path, a patch whose upsert document
    /// has another key, or a record that cannot be encoded.
    InvalidArgument(String),
    /// The first violation of the schema.
    SchemaViolation { field: String, message: String },
    /// The source has unmapped paths that dynamic mapping must map first.
    DynamicMappingRequired { paths: Vec<String> },
}

impl OpError {
    /// Whether a newer schema might accept the op.
    fn depends_on_schema(&self) -> bool {
        !matches!(self, OpError::InvalidArgument(_))
    }
}

/// Why a write failed as a whole.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("collection {0} not found")]
    CollectionNotFound(CollectionId),
    #[error("a write holds at most 10 000 operations, got {0}")]
    TooManyOps(usize),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Meta(#[from] MetaError),
}

/// One op after validation: its partition and record, or why not.
type Checked = Result<(u32, Record), OpError>;

impl CollectionWriter {
    pub fn new(meta: impl Into<Arc<dyn MetaStore>>, log: LogWriter) -> Self {
        Self {
            meta: meta.into(),
            log,
        }
    }

    /// Validates `ops` and appends the valid ones to the collection's
    /// implicit stream, all in one WAL object and one commit.
    ///
    /// Each op is validated against the schema the local metastore node
    /// holds. If an op is refused for a reason a newer schema might lift,
    /// the collection is read again linearizably, and if its schema is
    /// newer every op is validated again. Records go to
    /// `partition_of(pk, partitions)`, in input order within a partition.
    ///
    /// A [`LogError`] fails the whole call, and nothing was written unless
    /// it is [`LogError::CommitUnknown`].
    pub async fn write(
        &self,
        namespace: NamespaceId,
        collection: CollectionId,
        ops: Vec<DocOp>,
    ) -> Result<WriteOutcome, WriteError> {
        if ops.len() > MAX_WRITE_OPS {
            return Err(WriteError::TooManyOps(ops.len()));
        }
        let mut target = self
            .collection(Consistency::Local, namespace, collection)
            .await?;
        let mut checked: Vec<Checked> = ops.iter().map(|op| check(&target, op)).collect();
        let schema_dependent = checked
            .iter()
            .any(|c| c.as_ref().is_err_and(OpError::depends_on_schema));
        if schema_dependent {
            let fresh = self
                .collection(Consistency::Linearizable, namespace, collection)
                .await?;
            if fresh.schema.version > target.schema.version {
                target = fresh;
                checked = ops.iter().map(|op| check(&target, op)).collect();
            }
        }

        // Per partition, in input order: (op index, record).
        let mut by_partition: BTreeMap<u32, Vec<(usize, Record)>> = BTreeMap::new();
        let mut results: Vec<Option<OpResult>> = Vec::with_capacity(ops.len());
        for (index, outcome) in checked.into_iter().enumerate() {
            match outcome {
                Ok((partition, record)) => {
                    by_partition
                        .entry(partition)
                        .or_default()
                        .push((index, record));
                    results.push(None);
                }
                Err(err) => results.push(Some(OpResult::Rejected(err))),
            }
        }

        let mut token = ConsistencyToken::default();
        if !by_partition.is_empty() {
            let mut positions = Vec::with_capacity(by_partition.len());
            let mut batches = Vec::with_capacity(by_partition.len());
            for (partition, entries) in by_partition {
                let (indices, records): (Vec<usize>, Vec<Record>) = entries.into_iter().unzip();
                positions.push(indices);
                batches.push((partition, records));
            }
            let acks = self.log.append_many(target.stream, batches).await?;
            for (ack, indices) in acks.iter().zip(positions) {
                for (offset, index) in (ack.base_offset..).zip(indices) {
                    results[index] = Some(OpResult::Written {
                        partition: ack.partition,
                        offset,
                    });
                }
                token
                    .0
                    .push((target.stream, ack.partition, ack.last_offset + 1));
            }
            token = token.normalized();
        }
        // Every batch has an ack, so every accepted op has its offset.
        let results = results
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                LogError::CommitUnknown("a batch of the append was not acknowledged".to_string())
            })?;
        Ok(WriteOutcome {
            token,
            results,
            schema_version: target.schema.version,
        })
    }

    /// Collection `id` of `namespace`, as `consistency` sees it.
    async fn collection(
        &self,
        consistency: Consistency,
        namespace: NamespaceId,
        id: CollectionId,
    ) -> Result<Collection, WriteError> {
        self.meta
            .collection(consistency, id)
            .await?
            .filter(|c| c.namespace == namespace)
            .ok_or(WriteError::CollectionNotFound(id))
    }
}

/// Validates one op against `collection`'s schema (the key, then the
/// document or patch; a delete needs only its key) and encodes it.
fn check(collection: &Collection, op: &DocOp) -> Checked {
    op.pk()
        .validate()
        .map_err(|e| OpError::InvalidArgument(e.to_string()))?;
    let schema = &collection.schema;
    let checked = match op {
        DocOp::Upsert(doc) => check_document(schema, doc).map(|_| ()),
        DocOp::Delete(_) => Ok(()),
        DocOp::Patch {
            pk,
            delete_keys,
            upsert,
            ..
        } => {
            // Not schema matters: reported before the schema is consulted,
            // so a field that happens to be named `delete_keys` is not
            // mistaken for it.
            if let Some(violation) = invalid_delete_key(delete_keys) {
                return Err(OpError::InvalidArgument(format!(
                    "{}: {}",
                    violation.field, violation.message
                )));
            }
            // Apply would never insert it (`apply_patch`), so the patch
            // would be acknowledged and silently do nothing.
            if upsert.as_ref().is_some_and(|doc| doc.pk != *pk) {
                return Err(OpError::InvalidArgument(
                    "the upsert document's primary key differs from the patch's".to_string(),
                ));
            }
            check_patch(schema, op)
        }
    };
    checked.map_err(|rejection| match rejection {
        DocRejection::Violations(violations) => match violations.into_iter().next() {
            Some(first) => OpError::SchemaViolation {
                field: first.field,
                message: first.message,
            },
            None => OpError::InvalidArgument("rejected without a violation".to_string()),
        },
        DocRejection::DynamicMappingRequired { paths } => OpError::DynamicMappingRequired { paths },
    })?;
    let record = encode(op).map_err(|e| OpError::InvalidArgument(e.to_string()))?;
    Ok((partition_of(op.pk(), collection.partitions), record))
}
