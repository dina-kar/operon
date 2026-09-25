//! `CollectionService::write` (plan M1.2 Task 9 rule 8): dynamic mapping
//! (Ruling 18), atomic validation (Ruling 16), existence reporting
//! (Ruling 10) around M1.1's `CollectionWriter`.

use std::collections::{BTreeMap, BTreeSet};

use operon_collection::{
    CollectionSchema, DocOp, DocRejection, Document, DynamicMapping, DynamicMappingError,
    MAX_WRITE_OPS, OpError, PrimaryKey, apply_patch, check_document, check_patch, encode,
    propose_dynamic_fields, unmapped_paths,
};
use operon_common::NamespaceId;
use operon_common::meta::{ApplyError, Collection, Consistency, MetaError};
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::ir::ReadConsistency;
use crate::service::CollectionService;
use crate::types::{OpPosition, OpResult, Projection, SourceFilter, WriteOptions, WriteResult};

/// Every source an op brings: an upsert's document, a patch's source and its
/// upsert document.
fn sources(ops: &[DocOp]) -> Vec<&Map<String, Value>> {
    let mut out = Vec::new();
    for op in ops {
        match op {
            DocOp::Upsert(doc) => out.push(&doc.source),
            DocOp::Delete(_) => {}
            DocOp::Patch { source, upsert, .. } => {
                out.push(source);
                if let Some(doc) = upsert {
                    out.push(&doc.source);
                }
            }
        }
    }
    out
}

/// The first `delete_keys` entry that is not a dot-separated path, as M1.1's
/// writer reports it (`values::invalid_delete_key`, crate-private there).
fn invalid_delete_key(delete_keys: &[String]) -> Option<String> {
    delete_keys
        .iter()
        .find(|key| key.split('.').any(str::is_empty))
        .map(|key| format!("delete_keys: {key:?} is not a dot-separated path"))
}

/// Op `i` against `schema`, in the order of M1.1's writer check
/// (`writer.rs` `check`, row 0.26), so a request this accepts the writer
/// accepts under the same schema.
fn validate_op(schema: &CollectionSchema, i: usize, op: &DocOp) -> Result<(), ServiceError> {
    let invalid = |message: String| ServiceError::InvalidArgument(format!("op {i}: {message}"));
    op.pk().validate().map_err(|err| invalid(err.to_string()))?;
    let checked = match op {
        DocOp::Upsert(doc) => check_document(schema, doc).map(|_| ()),
        DocOp::Delete(_) => Ok(()),
        DocOp::Patch {
            pk,
            delete_keys,
            upsert,
            ..
        } => {
            if let Some(message) = invalid_delete_key(delete_keys) {
                return Err(invalid(message));
            }
            if upsert.as_ref().is_some_and(|doc| doc.pk != *pk) {
                return Err(invalid(
                    "the upsert document's primary key differs from the patch's".to_string(),
                ));
            }
            check_patch(schema, op)
        }
    };
    match checked {
        Ok(()) => {}
        Err(DocRejection::Violations(violations)) => {
            return Err(match violations.into_iter().next() {
                Some(first) if first.field == "delete_keys" => {
                    invalid(format!("{}: {}", first.field, first.message))
                }
                Some(first) => ServiceError::SchemaViolation {
                    field: first.field,
                    message: format!("op {i}: {}", first.message),
                },
                None => invalid("rejected without a violation".to_string()),
            });
        }
        Err(DocRejection::DynamicMappingRequired { paths }) => {
            return Err(ServiceError::SchemaViolation {
                field: paths.first().cloned().unwrap_or_default(),
                message: format!("op {i}: dynamic mapping required for {paths:?}"),
            });
        }
    }
    encode(op).map_err(|err| invalid(err.to_string()))?;
    Ok(())
}

/// A writer's refusal of one op (rule 8.6).
fn rejected(err: OpError) -> ServiceError {
    match err {
        OpError::InvalidArgument(message) => ServiceError::InvalidArgument(message),
        OpError::SchemaViolation { field, message } => {
            ServiceError::SchemaViolation { field, message }
        }
        OpError::DynamicMappingRequired { paths } => ServiceError::SchemaViolation {
            field: paths.first().cloned().unwrap_or_default(),
            message: format!("dynamic mapping required for {paths:?}"),
        },
    }
}

/// What op `op` does to state `state` (rule 8.4), and the state after it.
fn simulate(state: &mut BTreeMap<PrimaryKey, Option<Document>>, op: &DocOp) -> OpResult {
    let slot = state.entry(op.pk().clone()).or_insert(None);
    match op {
        DocOp::Upsert(doc) => {
            // Every upsert writes a new version (M1.1 P32, row 0.53).
            let result = if slot.is_some() {
                OpResult::Updated
            } else {
                OpResult::Created
            };
            *slot = Some(doc.clone());
            result
        }
        DocOp::Delete(_) => {
            let result = if slot.is_some() {
                OpResult::Deleted
            } else {
                OpResult::NotFound
            };
            *slot = None;
            result
        }
        DocOp::Patch { .. } => match slot.take() {
            None => {
                let created = apply_patch(None, op);
                let result = if created.is_some() {
                    OpResult::Created
                } else {
                    OpResult::NotFound
                };
                *slot = created;
                result
            }
            Some(current) => {
                let next = apply_patch(Some(&current), op);
                let result = if next.as_ref() == Some(&current) {
                    OpResult::Noop
                } else {
                    OpResult::Updated
                };
                *slot = next;
                result
            }
        },
    }
}

impl CollectionService {
    /// Writes `ops` to collection (or alias) `name` in one append per
    /// partition (rule 8). Each partition's accepted ops are consecutive
    /// records in request order (M1.5 relies on it for `_seq_no`).
    pub async fn write(
        &self,
        ns: &str,
        name: &str,
        ops: Vec<DocOp>,
        opts: WriteOptions,
    ) -> Result<WriteResult, ServiceError> {
        // 1.
        let (ns_id, collection) = self.resolve(ns, name).await?;
        if ops.len() > MAX_WRITE_OPS {
            return Err(ServiceError::InvalidArgument(format!(
                "a write holds at most 10 000 operations, got {}",
                ops.len()
            )));
        }
        // 2.
        let collection = self.map_dynamically(ns_id, collection, &ops).await?;
        // 3.
        if opts.atomic {
            for (i, op) in ops.iter().enumerate() {
                validate_op(&collection.schema, i, op)?;
            }
        }
        // 4. The state the results describe, read before the append.
        let mut state = if opts.report_existence {
            Some(self.current_state(ns_id, &collection, &ops).await?)
        } else {
            None
        };
        // 5.
        let simulated = state.as_ref().map(|_| ops.clone());
        let outcome = self
            .writer
            .write(ns_id, collection.id, ops)
            .await
            .map_err(|err| match ServiceError::from(err) {
                ServiceError::NotFound { kind, .. } => ServiceError::NotFound {
                    kind,
                    name: name.to_string(),
                },
                other => other,
            })?;
        // 6.
        let mut results = Vec::with_capacity(outcome.results.len());
        let mut positions = Vec::with_capacity(outcome.results.len());
        for (i, result) in outcome.results.into_iter().enumerate() {
            match result {
                operon_collection::OpResult::Written { partition, offset } => {
                    positions.push(Some(OpPosition {
                        partition,
                        seq_no: offset,
                    }));
                    results.push(match (&mut state, &simulated) {
                        (Some(state), Some(ops)) => simulate(state, &ops[i]),
                        _ => OpResult::Accepted,
                    });
                }
                // A rejected op leaves the simulated state unchanged.
                operon_collection::OpResult::Rejected(err) => {
                    positions.push(None);
                    results.push(OpResult::Rejected(rejected(err)));
                }
            }
        }
        // 7.
        self.reads.notify(collection.id);
        // 8.
        Ok(WriteResult {
            token: outcome.token,
            results,
            positions,
        })
    }

    /// Rule 8.2: with `DynamicMapping::Map`, maps the unmapped paths of the
    /// ops' sources before the append, by compare-and-set of the schema
    /// (Ruling 18). Returns the collection with the schema to validate
    /// against.
    async fn map_dynamically(
        &self,
        ns_id: NamespaceId,
        mut collection: Collection,
        ops: &[DocOp],
    ) -> Result<Collection, ServiceError> {
        if collection.schema.dynamic != DynamicMapping::Map {
            return Ok(collection);
        }
        let sources = sources(ops);
        for attempt in 0..self.config.schema_retries {
            if attempt > 0 {
                collection = self
                    .ctx
                    .meta
                    .collection(Consistency::Linearizable, collection.id)
                    .await?
                    .filter(|c| c.namespace == ns_id)
                    .ok_or_else(|| ServiceError::NotFound {
                        kind: "collection",
                        name: collection.name.clone(),
                    })?;
            }
            let schema = &collection.schema;
            let first = sources
                .iter()
                .find_map(|source| unmapped_paths(schema, source).into_iter().next());
            let Some(first) = first else {
                return Ok(collection);
            };
            let proposal = propose_dynamic_fields(schema, &sources).map_err(|err| match err {
                DynamicMappingError::TooManyFields { limit } => ServiceError::SchemaViolation {
                    field: first.clone(),
                    message: format!("mapping would exceed the field limit of {limit}"),
                },
            })?;
            // Paths no field can map stay in `_source` (X11).
            if proposal.is_empty() {
                return Ok(collection);
            }
            let mut next = schema.clone();
            next.fields.extend(proposal);
            match self
                .ctx
                .meta
                .update_collection_schema(collection.id, schema.version, next.clone())
                .await
            {
                Ok(version) => {
                    self.reads.notify(collection.id);
                    collection.schema = CollectionSchema { version, ..next };
                    return Ok(collection);
                }
                Err(MetaError::Rejected(ApplyError::SchemaVersionMismatch { .. })) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Err(ServiceError::Unavailable(format!(
            "schema update raced {} times",
            self.config.schema_retries
        )))
    }

    /// Rule 8.4: the current documents of the ops' distinct valid keys, read
    /// strongly, with their whole source and every vector.
    async fn current_state(
        &self,
        ns_id: NamespaceId,
        collection: &Collection,
        ops: &[DocOp],
    ) -> Result<BTreeMap<PrimaryKey, Option<Document>>, ServiceError> {
        let keys: Vec<PrimaryKey> = ops
            .iter()
            .map(DocOp::pk)
            .filter(|pk| pk.validate().is_ok())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if keys.is_empty() {
            return Ok(BTreeMap::new());
        }
        let schema = &collection.schema;
        let select = Projection {
            source: SourceFilter::All,
            vectors: schema
                .vectors
                .iter()
                .map(|v| v.name.clone())
                .chain(schema.sparse_vectors.iter().map(|v| v.name.clone()))
                .collect(),
            fields: Vec::new(),
        };
        let hot = self.request_hot();
        let docs = self
            .get_in(
                ns_id,
                collection,
                &keys,
                &select,
                &ReadConsistency::Strong,
                &hot,
            )
            .await?;
        Ok(keys
            .into_iter()
            .zip(docs)
            .map(|(pk, doc)| {
                let doc = doc.map(|stored| Document {
                    pk: stored.pk,
                    source: stored.source.unwrap_or_default(),
                    vectors: stored.vectors,
                    sparse_vectors: stored.sparse_vectors,
                });
                (pk, doc)
            })
            .collect())
    }
}
