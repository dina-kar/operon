//! The write engine and the single-document endpoints (plan M1.5 Task 4):
//! `PUT|POST /{index}/_doc/{id}`, `POST /{index}/_doc`,
//! `PUT|POST /{index}/_create/{id}`, `POST /{index}/_update/{id}` and
//! `DELETE /{index}/_doc/{id}`. `_bulk` (Task 5) calls [`execute`] per
//! index.
//!
//! [`execute`] resolves the write index, maps new fields before appending
//! (R17), pre-reads the documents updates and creates need (Ruling 8),
//! simulates the items in order over an overlay, so repeated ids answer as
//! in ES, and turns `OpResult`s and positions into ES results, `_version`
//! and `_seq_no` (Ruling 4).

use std::collections::{BTreeMap, HashMap, HashSet};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, Uri};
use axum::response::Response;
use loams_collection::{
    CollectionSchema, ConsistencyToken, DocOp, DocRejection, Document, FieldSpec, MAX_WRITE_OPS,
    PatchMode, PrimaryKey, VectorSpec, Violation, check_document, check_patch, extract,
};
use loams_query::{
    OpPosition, OpResult, Projection, ReadConsistency, ServiceError,
    SourceFilter as IrSourceFilter, WriteOptions,
};
use serde_json::{Map, Value, json};

use crate::doc::{
    SourceFilter, check_source, field_error, merge_deep, not_an_object, restore_vectors,
    split_vectors, to_document, validate_id,
};
use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, json_body, respond};
use crate::mapping::{IndexView, dynamic_plan, index_uuid, pending_vector_spec, plan_create};
use crate::names::{resolve_write, validate_index_name};
use crate::{EsGateway, TOKEN_HEADER};

/// One write of a request, in request order.
#[derive(Clone, Debug)]
pub enum WriteItem {
    /// `index` (or `create`); an absent id is generated (Ruling 5).
    Index {
        id: Option<String>,
        source: Value,
        create: bool,
    },
    Update {
        id: String,
        body: Value,
    },
    Delete {
        id: String,
    },
}

/// What an item answers: its status, and its body (`_index`, `_id`,
/// `_version`, `result`, `_shards`, `_seq_no`, `_primary_term`, or
/// `_index`, `_id` and `error`). `error` is the error of a failed item, for
/// the single-document envelope and `Retry-After`.
#[derive(Clone, Debug)]
pub struct ItemOutcome {
    pub status: u16,
    pub body: Map<String, Value>,
    pub error: Option<EsError>,
}

/// One `execute` call: the index expression and the request's options.
#[derive(Debug)]
pub struct WriteCall<'a> {
    pub gateway: &'a EsGateway,
    pub ctx: &'a RequestCtx,
    pub index: &'a str,
    pub require_alias: bool,
    /// The `pipeline` parameter: `_none` turns the index's default pipeline
    /// off; any other value fails the item (Ruling 18).
    pub pipeline: Option<String>,
    /// `refresh=true` (or empty): the body says `forced_refresh`.
    pub refresh: bool,
    /// `_update`'s `get`: `None` leaves it out.
    pub source_on_update: Option<SourceFilter>,
}

/// The parameters that ask for optimistic concurrency control (Ruling 4).
pub(crate) const OCC_PARAMS: [&str; 4] =
    ["if_seq_no", "if_primary_term", "version", "version_type"];

/// 400 for optimistic concurrency control (Phase B).
pub(crate) fn occ_unsupported() -> EsError {
    EsError::illegal_argument(
        "optimistic concurrency control (if_seq_no, if_primary_term, version) is not supported \
         by Loams (Phase B)",
    )
}

fn write_error(error: ServiceError) -> EsError {
    EsError::from_service(error, ErrorContext::Write)
}

fn pipeline_missing(pipeline: &str) -> EsError {
    EsError::illegal_argument(format!("pipeline with id [{pipeline}] does not exist"))
}

fn require_alias_error(index: &str) -> EsError {
    EsError::new(
        404,
        "index_not_found_exception",
        format!(
            "no such index [{index}] and [require_alias] request flag is [true] and [{index}] is \
             not an alias"
        ),
    )
    .with("resource.type", "index_or_alias")
    .with("resource.id", index)
    .with("index_uuid", "_na_")
    .with("index", index)
}

fn shard_error(error: EsError, index: &str, uuid: &str) -> EsError {
    error
        .with("index_uuid", uuid)
        .with("shard", "0")
        .with("index", index)
}

fn conflict_error(id: &str, seq_no: u64, index: &str, uuid: &str) -> EsError {
    let error = EsError::new(
        409,
        "version_conflict_engine_exception",
        format!(
            "[{id}]: version conflict, document already exists (current version [{}])",
            seq_no + 1
        ),
    );
    shard_error(error, index, uuid)
}

fn missing_error(id: &str, index: &str, uuid: &str) -> EsError {
    let error = EsError::new(
        404,
        "document_missing_exception",
        format!("[{id}]: document missing"),
    );
    shard_error(error, index, uuid)
}

fn validation(reason: &str) -> EsError {
    EsError::new(
        400,
        "action_request_validation_exception",
        format!("Validation Failed: 1: {reason};"),
    )
}

/// Where the `_seq_no` of an overlay entry comes from.
#[derive(Clone, Copy, Debug)]
enum SeqRef {
    /// The pre-read document's.
    Known(u64),
    /// Op `n` of this request's.
    Op(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpKind {
    Upsert,
    Create,
    /// `detect_noop: false` reports an unchanged document as `updated`.
    Update {
        detect_noop: bool,
    },
    Delete,
}

/// An item once its document is built (steps 4–6).
#[derive(Clone, Debug)]
enum Prepared {
    Index {
        doc: Document,
        create: bool,
    },
    Update {
        source: Map<String, Value>,
        vectors: BTreeMap<String, Option<Vec<f32>>>,
        upsert: Option<Document>,
        doc_as_upsert: bool,
        detect_noop: bool,
        get: Option<SourceFilter>,
    },
    Delete,
}

/// An item's state through the steps.
#[derive(Clone, Debug)]
enum Slot {
    Failed(EsError),
    Ready(Prepared),
    Op {
        op: usize,
        kind: OpKind,
        get: Option<(SourceFilter, Document)>,
    },
    Noop {
        seq: SeqRef,
        get: Option<(SourceFilter, Document)>,
    },
    Conflict(SeqRef),
    Missing,
}

/// A document of the overlay: the current state of a key.
#[derive(Clone, Debug)]
struct Current {
    doc: Document,
    seq: SeqRef,
}

/// The index an `execute` writes to.
struct Target {
    name: String,
    uuid: String,
    view: IndexView,
}

/// Runs `items` against `call.index` (steps 1–12), answering one outcome
/// per item in order, and the write's consistency token.
pub async fn execute(
    call: WriteCall<'_>,
    items: Vec<WriteItem>,
) -> (Vec<ItemOutcome>, Option<ConsistencyToken>) {
    // Step 2: ids. A generated id is new, so its create needs no pre-read.
    let mut ids = Vec::with_capacity(items.len());
    let mut slots: Vec<Option<Slot>> = Vec::with_capacity(items.len());
    for item in &items {
        let (id, generated) = match item {
            WriteItem::Index { id: Some(id), .. }
            | WriteItem::Update { id, .. }
            | WriteItem::Delete { id } => (id.clone(), false),
            WriteItem::Index { id: None, .. } => (call.gateway.next_id(), true),
        };
        slots.push(validate_id(&id).err().map(Slot::Failed));
        ids.push((id, generated));
    }
    // Step 1: the write index.
    let target = match resolve(&call, &items).await {
        Ok(target) => target,
        Err(error) => {
            let outcomes = ids
                .iter()
                .zip(slots)
                .map(|((id, _), slot)| match slot {
                    Some(Slot::Failed(own)) => failed(call.index, id, own),
                    _ => failed(call.index, id, error.clone()),
                })
                .collect();
            return (outcomes, None);
        }
    };
    let mut engine = Engine {
        call: &call,
        target,
        ids,
        slots: slots
            .into_iter()
            .map(|s| s.unwrap_or(Slot::Missing))
            .collect(),
        failed_upfront: Vec::new(),
        created: HashMap::new(),
    };
    engine.failed_upfront = engine
        .slots
        .iter()
        .map(|slot| matches!(slot, Slot::Failed(_)))
        .collect();
    // Boxed: the service futures nest deeply enough to overflow the
    // layout query depth of every caller otherwise.
    Box::pin(engine.run(items)).await
}

/// Step 1: `resolve_write`, `require_alias` and auto-create.
async fn resolve(call: &WriteCall<'_>, items: &[WriteItem]) -> Result<Target, EsError> {
    let service = call.gateway.service();
    let ns = call.ctx.namespace.as_str();
    let name = match resolve_write(service, ns, call.index).await? {
        Some(target) => {
            if call.require_alias && target.via_alias.is_none() {
                return Err(require_alias_error(call.index));
            }
            target.name
        }
        None if call.require_alias => return Err(require_alias_error(call.index)),
        None => {
            // ES auto-creates the index for an index or an update item,
            // even an update that then finds no document (row T11-3).
            let creates = items
                .iter()
                .any(|item| !matches!(item, WriteItem::Delete { .. }));
            if !creates {
                return Err(EsError::index_not_found(call.index));
            }
            validate_index_name(call.index)?;
            let plan = plan_create(None, None)?;
            match service
                .create_collection(ns, call.index, plan.schema(), plan.partitions)
                .await
            {
                // A racing creator made it first.
                Ok(_) | Err(ServiceError::AlreadyExists(_)) => {}
                Err(err) => return Err(write_error(err)),
            }
            call.index.to_string()
        }
    };
    let info = service
        .get_collection(ns, &name)
        .await
        .map_err(write_error)?;
    Ok(Target {
        uuid: index_uuid(info.id),
        view: IndexView::new(info),
        name,
    })
}

/// A failed item's outcome.
fn failed(index: &str, id: &str, error: EsError) -> ItemOutcome {
    let mut body = Map::new();
    body.insert("_index".to_string(), json!(index));
    body.insert("_id".to_string(), json!(id));
    body.insert("error".to_string(), error.cause_value());
    ItemOutcome {
        status: error.status,
        body,
        error: Some(error),
    }
}

/// The state of one `execute` after resolution.
struct Engine<'a, 'c> {
    call: &'a WriteCall<'c>,
    target: Target,
    /// Per item: its id, and whether it was generated.
    ids: Vec<(String, bool)>,
    slots: Vec<Slot>,
    /// Items that failed before step 4 (their id); a re-fold keeps them.
    failed_upfront: Vec<bool>,
    /// The document of each create op, by op.
    created: HashMap<usize, Document>,
}

/// What a fold adds to the schema.
#[derive(Default)]
struct Additions {
    fields: Vec<FieldSpec>,
    vectors: Vec<VectorSpec>,
    annotations: BTreeMap<String, String>,
}

impl Additions {
    fn is_empty(&self) -> bool {
        self.fields.is_empty() && self.vectors.is_empty() && self.annotations.is_empty()
    }
}

impl Engine<'_, '_> {
    fn ns(&self) -> &str {
        &self.call.ctx.namespace
    }

    async fn run(mut self, items: Vec<WriteItem>) -> (Vec<ItemOutcome>, Option<ConsistencyToken>) {
        let service = self.call.gateway.service().clone();
        // Step 3: pipelines.
        let pipeline = match self.call.pipeline.as_deref() {
            Some("_none") => None,
            Some(p) => Some(p.to_string()),
            None => self.target.view.es.default_pipeline.clone(),
        };
        // Steps 4 and 5, re-folded once over a fresh schema when a racing
        // writer mapped a path differently.
        for attempt in 0..2 {
            let additions = self.fold(&items, pipeline.as_deref());
            if additions.is_empty() {
                break;
            }
            let added = service
                .add_fields(
                    self.ns(),
                    &self.target.name,
                    additions.fields,
                    additions.vectors,
                    additions.annotations,
                )
                .await;
            match added {
                Ok(schema) => {
                    self.set_schema(schema);
                    break;
                }
                Err(
                    ServiceError::SchemaViolation { .. }
                    | ServiceError::InvalidArgument(_)
                    | ServiceError::AlreadyExists(_),
                ) => match service.get_collection(self.ns(), &self.target.name).await {
                    Ok(info) => {
                        self.target.view = IndexView::new(info);
                        if attempt == 1 {
                            // Step 6 reports what still does not fit.
                            self.fold(&items, pipeline.as_deref());
                        }
                    }
                    Err(err) => return self.fail_all(write_error(err)),
                },
                Err(err) => return self.fail_all(write_error(err)),
            }
        }
        // Step 6.
        self.validate();
        // Step 7.
        let overlay = match self.pre_read().await {
            Ok(overlay) => overlay,
            Err(err) => return self.fail_all(write_error(err)),
        };
        // Step 8.
        let ops = self.simulate(overlay);
        // Steps 10–12.
        self.append(ops).await
    }

    /// Replaces the view's schema with `schema`.
    fn set_schema(&mut self, schema: CollectionSchema) {
        let mut info = self.target.view.info.clone();
        info.schema = schema;
        self.target.view = IndexView::new(info);
    }

    /// Every item not failed yet gets `error`.
    fn fail_all(self, error: EsError) -> (Vec<ItemOutcome>, Option<ConsistencyToken>) {
        let index = self.target.name.clone();
        let outcomes = self
            .ids
            .iter()
            .zip(self.slots)
            .map(|((id, _), slot)| match slot {
                Slot::Failed(own) => failed(&index, id, own),
                _ => failed(&index, id, error.clone()),
            })
            .collect();
        (outcomes, None)
    }

    /// Steps 3–5 over every item still standing, in order: builds each
    /// document, and folds pending vectors and dynamic mapping over a
    /// working view, so each item sees the fields proposed before it.
    fn fold(&mut self, items: &[WriteItem], pipeline: Option<&str>) -> Additions {
        let mut additions = Additions::default();
        let mut view = self.target.view.clone();
        for (i, item) in items.iter().enumerate() {
            if self.failed_upfront[i] {
                continue;
            }
            let id = self.ids[i].0.clone();
            let generated = self.ids[i].1;
            let prepared = match (item, pipeline) {
                (WriteItem::Delete { .. }, _) => Ok(Prepared::Delete),
                (_, Some(p)) => Err(pipeline_missing(p)),
                (WriteItem::Index { source, create, .. }, None) => {
                    prepare_index(&view, &id, source, *create && !generated)
                }
                (WriteItem::Update { body, .. }, None) => {
                    prepare_update(&view, &id, body, self.call.source_on_update.as_ref())
                }
            };
            let prepared = prepared.and_then(|prepared| {
                let (vectors, fields) = plan_additions(&view, &id, &prepared)?;
                if !vectors.is_empty() || !fields.is_empty() {
                    let mut info = view.info.clone();
                    for (spec, annotation) in vectors {
                        info.schema.annotations.insert(
                            format!("{}{}", crate::mapping::ANN_FIELD, spec.name),
                            annotation.clone(),
                        );
                        additions.annotations.insert(
                            format!("{}{}", crate::mapping::ANN_FIELD, spec.name),
                            annotation,
                        );
                        info.schema.vectors.push(spec.clone());
                        additions.vectors.push(spec);
                    }
                    info.schema.fields.extend(fields.iter().cloned());
                    additions.fields.extend(fields);
                    view = IndexView::new(info);
                }
                Ok(prepared)
            });
            self.slots[i] = match prepared {
                Ok(prepared) => Slot::Ready(prepared),
                Err(error) => Slot::Failed(error),
            };
        }
        additions
    }

    /// Step 6: `check_document` / `check_patch` against the final schema.
    fn validate(&mut self) {
        let view = &self.target.view;
        let schema = &view.info.schema;
        for (i, slot) in self.slots.iter_mut().enumerate() {
            let Slot::Ready(prepared) = slot else {
                continue;
            };
            let id = &self.ids[i].0;
            let rejection = match prepared {
                Prepared::Index { doc, .. } => {
                    check_document(schema, doc).err().map(|r| (r, &doc.source))
                }
                Prepared::Update {
                    source,
                    vectors,
                    upsert,
                    ..
                } => {
                    let patch = DocOp::Patch {
                        pk: PrimaryKey::Str(id.clone()),
                        mode: PatchMode::MergeDeep,
                        source: source.clone(),
                        delete_keys: Vec::new(),
                        vectors: vectors.clone(),
                        sparse_vectors: BTreeMap::new(),
                        upsert: None,
                    };
                    check_patch(schema, &patch)
                        .err()
                        .map(|r| (r, &*source))
                        .or_else(|| {
                            upsert.as_ref().and_then(|doc| {
                                check_document(schema, doc).err().map(|r| (r, &doc.source))
                            })
                        })
                }
                Prepared::Delete => None,
            };
            if let Some((DocRejection::Violations(violations), source)) = rejection
                && let Some(first) = violations.first()
            {
                *slot = Slot::Failed(violation_error(view, id, source, first));
            }
        }
    }

    /// Step 7: one strong batch `get` of the keys updates and creates need.
    async fn pre_read(&self) -> Result<HashMap<PrimaryKey, Option<Current>>, ServiceError> {
        // First-appearance order; `seen` keeps the scan linear (a `_bulk`
        // group can hold hundreds of thousands of items).
        let mut pks: Vec<PrimaryKey> = Vec::new();
        let mut seen: HashSet<PrimaryKey> = HashSet::new();
        for (i, slot) in self.slots.iter().enumerate() {
            let needs = matches!(
                slot,
                Slot::Ready(Prepared::Update { .. } | Prepared::Index { create: true, .. })
            );
            let pk = PrimaryKey::Str(self.ids[i].0.clone());
            if needs && seen.insert(pk.clone()) {
                pks.push(pk);
            }
        }
        let mut overlay = HashMap::new();
        if pks.is_empty() {
            return Ok(overlay);
        }
        let service = self.call.gateway.service();
        let select = Projection {
            source: IrSourceFilter::All,
            vectors: self.target.view.es.vectors.keys().cloned().collect(),
            fields: Vec::new(),
        };
        let chunk = service.config().max_get_keys.max(1);
        for keys in pks.chunks(chunk) {
            let docs = service
                .get(
                    self.ns(),
                    &self.target.name,
                    keys,
                    &select,
                    ReadConsistency::Strong,
                )
                .await?;
            for (pk, doc) in keys.iter().zip(docs) {
                let current = doc.map(|doc| Current {
                    seq: SeqRef::Known(doc.seq_no),
                    doc: Document {
                        pk: pk.clone(),
                        source: doc.source.unwrap_or_default(),
                        vectors: doc.vectors,
                        sparse_vectors: BTreeMap::new(),
                    },
                });
                overlay.insert(pk.clone(), current);
            }
        }
        Ok(overlay)
    }

    /// Step 8: the items in order over the overlay, and the ops to append.
    fn simulate(&mut self, mut overlay: HashMap<PrimaryKey, Option<Current>>) -> Vec<DocOp> {
        let mut ops = Vec::new();
        for (i, slot) in self.slots.iter_mut().enumerate() {
            let Slot::Ready(prepared) = slot else {
                continue;
            };
            let pk = PrimaryKey::Str(self.ids[i].0.clone());
            let op = ops.len();
            let current = overlay.get(&pk).cloned().flatten();
            let next = match std::mem::replace(prepared, Prepared::Delete) {
                Prepared::Index { doc, create: false } => {
                    overlay.insert(
                        pk,
                        Some(Current {
                            doc: doc.clone(),
                            seq: SeqRef::Op(op),
                        }),
                    );
                    ops.push(DocOp::Upsert(doc));
                    Slot::Op {
                        op,
                        kind: OpKind::Upsert,
                        get: None,
                    }
                }
                Prepared::Index { doc, create: true } => match current {
                    Some(current) => Slot::Conflict(current.seq),
                    None => {
                        overlay.insert(
                            pk.clone(),
                            Some(Current {
                                doc: doc.clone(),
                                seq: SeqRef::Op(op),
                            }),
                        );
                        self.created.insert(op, doc.clone());
                        ops.push(DocOp::Patch {
                            pk,
                            mode: PatchMode::MergeDeep,
                            source: Map::new(),
                            delete_keys: Vec::new(),
                            vectors: BTreeMap::new(),
                            sparse_vectors: BTreeMap::new(),
                            upsert: Some(doc),
                        });
                        Slot::Op {
                            op,
                            kind: OpKind::Create,
                            get: None,
                        }
                    }
                },
                Prepared::Update {
                    source,
                    vectors,
                    upsert,
                    doc_as_upsert,
                    detect_noop,
                    get,
                } => {
                    let as_upsert = || Document {
                        pk: pk.clone(),
                        source: source.clone(),
                        vectors: vectors
                            .iter()
                            .filter_map(|(k, v)| v.clone().map(|v| (k.clone(), v)))
                            .collect(),
                        sparse_vectors: BTreeMap::new(),
                    };
                    let upsert_doc = if doc_as_upsert {
                        Some(as_upsert())
                    } else {
                        upsert
                    };
                    let patch = |upsert: Option<Document>| DocOp::Patch {
                        pk: pk.clone(),
                        mode: PatchMode::MergeDeep,
                        source: source.clone(),
                        delete_keys: Vec::new(),
                        vectors: vectors.clone(),
                        sparse_vectors: BTreeMap::new(),
                        upsert,
                    };
                    match current {
                        None => match upsert_doc {
                            None => Slot::Missing,
                            Some(upsert) => {
                                ops.push(patch(Some(upsert.clone())));
                                overlay.insert(
                                    pk.clone(),
                                    Some(Current {
                                        doc: upsert.clone(),
                                        seq: SeqRef::Op(op),
                                    }),
                                );
                                Slot::Op {
                                    op,
                                    kind: OpKind::Update { detect_noop },
                                    get: get.map(|g| (g, upsert)),
                                }
                            }
                        },
                        Some(current) => {
                            let mut merged = current.doc.clone();
                            merge_deep(&mut merged.source, &source);
                            for (name, vector) in &vectors {
                                match vector {
                                    Some(v) => {
                                        merged.vectors.insert(name.clone(), v.clone());
                                    }
                                    None => {
                                        merged.vectors.remove(name);
                                    }
                                }
                            }
                            if detect_noop && merged == current.doc {
                                Slot::Noop {
                                    seq: current.seq,
                                    get: get.map(|g| (g, merged)),
                                }
                            } else {
                                ops.push(patch(upsert_doc));
                                overlay.insert(
                                    pk.clone(),
                                    Some(Current {
                                        doc: merged.clone(),
                                        seq: SeqRef::Op(op),
                                    }),
                                );
                                Slot::Op {
                                    op,
                                    kind: OpKind::Update { detect_noop },
                                    get: get.map(|g| (g, merged)),
                                }
                            }
                        }
                    }
                }
                Prepared::Delete => {
                    overlay.insert(pk.clone(), None);
                    ops.push(DocOp::Delete(pk));
                    Slot::Op {
                        op,
                        kind: OpKind::Delete,
                        get: None,
                    }
                }
            };
            *slot = next;
        }
        ops
    }

    /// The creates reported `Created` whose document is not theirs once
    /// the write is visible: two racing creates whose pre-reads both missed
    /// are both simulated as `Created` by the service, but only the first
    /// applied inserts its document (Ruling 7). Returns op → the stored
    /// document's `_seq_no`. A create followed by another op on its key in
    /// the same request is not checked; a failed read checks nothing.
    async fn lost_creates(
        &self,
        results: &[Result<(OpResult, Option<OpPosition>), EsError>],
        token: Option<&ConsistencyToken>,
    ) -> HashMap<usize, u64> {
        let mut lost = HashMap::new();
        let Some(token) = token else {
            return lost;
        };
        let mut last_op_of: HashMap<&str, usize> = HashMap::new();
        for (i, slot) in self.slots.iter().enumerate() {
            if let Slot::Op { op, .. } = slot {
                last_op_of.insert(self.ids[i].0.as_str(), *op);
            }
        }
        let mut checks: Vec<(usize, PrimaryKey, &Document)> = Vec::new();
        for (i, slot) in self.slots.iter().enumerate() {
            let Slot::Op {
                op,
                kind: OpKind::Create,
                ..
            } = slot
            else {
                continue;
            };
            let id = self.ids[i].0.as_str();
            let created = matches!(results.get(*op), Some(Ok((OpResult::Created, _))));
            if created
                && last_op_of.get(id) == Some(op)
                && let Some(doc) = self.created.get(op)
            {
                checks.push((*op, PrimaryKey::Str(id.to_string()), doc));
            }
        }
        if checks.is_empty() {
            return lost;
        }
        let service = self.call.gateway.service();
        let select = Projection {
            source: IrSourceFilter::All,
            vectors: self.target.view.es.vectors.keys().cloned().collect(),
            fields: Vec::new(),
        };
        let chunk = service.config().max_get_keys.max(1);
        for part in checks.chunks(chunk) {
            let pks: Vec<PrimaryKey> = part.iter().map(|(_, pk, _)| pk.clone()).collect();
            let Ok(docs) = service
                .get(
                    self.ns(),
                    &self.target.name,
                    &pks,
                    &select,
                    ReadConsistency::AtLeast(token.clone()),
                )
                .await
            else {
                continue;
            };
            for ((op, _, ours), stored) in part.iter().zip(docs) {
                if let Some(stored) = stored {
                    let same = stored.source.as_ref() == Some(&ours.source)
                        && stored.vectors == ours.vectors;
                    if !same {
                        lost.insert(*op, stored.seq_no);
                    }
                }
            }
        }
        lost
    }

    /// Steps 10–12: appends `ops` in writes of at most `MAX_WRITE_OPS`, in
    /// order, and answers every item. A refused write fails its ops and
    /// every later one (nothing after it is appended).
    async fn append(self, ops: Vec<DocOp>) -> (Vec<ItemOutcome>, Option<ConsistencyToken>) {
        let service = self.call.gateway.service();
        let total = ops.len();
        let mut results: Vec<Result<(OpResult, Option<OpPosition>), EsError>> =
            Vec::with_capacity(total);
        let mut token: Option<ConsistencyToken> = None;
        let mut rest = ops;
        while !rest.is_empty() {
            let tail = rest.split_off(MAX_WRITE_OPS.min(rest.len()));
            let chunk = std::mem::replace(&mut rest, tail);
            let n = chunk.len();
            let opts = WriteOptions {
                report_existence: true,
                ..Default::default()
            };
            match service
                .write(self.ns(), &self.target.name, chunk, opts)
                .await
            {
                Ok(written) => {
                    match &mut token {
                        Some(token) => token.merge(&written.token),
                        None => token = Some(written.token.clone()),
                    }
                    results.extend(written.results.into_iter().zip(written.positions).map(Ok));
                }
                Err(err) => {
                    let error = write_error(err);
                    let left = n + rest.len();
                    results.extend((0..left).map(|_| Err(error.clone())));
                    rest.clear();
                }
            }
        }
        let lost = self.lost_creates(&results, token.as_ref()).await;
        let seq_of = |seq: SeqRef| match seq {
            SeqRef::Known(seq) => seq,
            SeqRef::Op(op) => match results.get(op) {
                Some(Ok((_, Some(position)))) => position.seq_no,
                _ => 0,
            },
        };
        let index = self.target.name.as_str();
        let uuid = self.target.uuid.as_str();
        let refresh = self.call.refresh;
        let mut outcomes = Vec::with_capacity(self.slots.len());
        for ((id, _), slot) in self.ids.iter().zip(self.slots) {
            let outcome = match slot {
                Slot::Failed(error) => failed(index, id, error),
                Slot::Ready(_) => failed(index, id, EsError::new(500, "exception", "unwritten")),
                Slot::Missing => failed(index, id, missing_error(id, index, uuid)),
                Slot::Conflict(seq) => {
                    failed(index, id, conflict_error(id, seq_of(seq), index, uuid))
                }
                Slot::Noop { seq, get } => {
                    let seq = seq_of(seq);
                    let mut body = success(index, id, seq, "noop", false);
                    add_get(&mut body, seq, get);
                    ItemOutcome {
                        status: 200,
                        body,
                        error: None,
                    }
                }
                Slot::Op { op, kind, get } => match &results[op] {
                    Err(error) => failed(index, id, error.clone()),
                    Ok((OpResult::Rejected(err), _)) => failed(index, id, write_error(err.clone())),
                    Ok((result, position)) => {
                        let seq = position.map_or(0, |p| p.seq_no);
                        let (status, answer) = match (kind, result) {
                            (OpKind::Create, OpResult::Updated | OpResult::Noop) => {
                                // The race is lost: the key existed.
                                let error = conflict_error(id, seq, index, uuid);
                                outcomes.push(failed(index, id, error));
                                continue;
                            }
                            (OpKind::Create, OpResult::Created) if lost.contains_key(&op) => {
                                // Both pre-reads missed; the other create
                                // was applied first (row T4-4).
                                let error = conflict_error(id, lost[&op], index, uuid);
                                outcomes.push(failed(index, id, error));
                                continue;
                            }
                            (OpKind::Update { .. }, OpResult::NotFound) => {
                                // The document vanished after the pre-read.
                                outcomes.push(failed(index, id, missing_error(id, index, uuid)));
                                continue;
                            }
                            (OpKind::Delete, OpResult::NotFound) => (404, "not_found"),
                            (OpKind::Delete, _) => (200, "deleted"),
                            (_, OpResult::Created) => (201, "created"),
                            (OpKind::Update { detect_noop: true }, OpResult::Noop) => (200, "noop"),
                            _ => (200, "updated"),
                        };
                        let mut body = success(index, id, seq, answer, refresh && answer != "noop");
                        add_get(&mut body, seq, get);
                        ItemOutcome {
                            status,
                            body,
                            error: None,
                        }
                    }
                },
            };
            outcomes.push(outcome);
        }
        (outcomes, token)
    }
}

/// Step 12's success body.
fn success(
    index: &str,
    id: &str,
    seq_no: u64,
    result: &str,
    forced_refresh: bool,
) -> Map<String, Value> {
    let shards = if result == "noop" {
        json!({"total": 0, "successful": 0, "failed": 0})
    } else {
        json!({"total": 1, "successful": 1, "failed": 0})
    };
    let mut body = Map::new();
    body.insert("_index".to_string(), json!(index));
    body.insert("_id".to_string(), json!(id));
    body.insert("_version".to_string(), json!(seq_no + 1));
    body.insert("result".to_string(), json!(result));
    if forced_refresh {
        body.insert("forced_refresh".to_string(), json!(true));
    }
    body.insert("_shards".to_string(), shards);
    body.insert("_seq_no".to_string(), json!(seq_no));
    body.insert("_primary_term".to_string(), json!(1));
    body
}

/// `_update`'s `get`: the filtered merged source, vectors restored.
fn add_get(body: &mut Map<String, Value>, seq_no: u64, get: Option<(SourceFilter, Document)>) {
    let Some((filter, doc)) = get else {
        return;
    };
    let mut source = doc.source;
    restore_vectors(&mut source, &doc.vectors);
    let mut get = Map::new();
    get.insert("_seq_no".to_string(), json!(seq_no));
    get.insert("_primary_term".to_string(), json!(1));
    get.insert("found".to_string(), json!(true));
    if filter.enabled {
        get.insert("_source".to_string(), Value::Object(filter.apply(source)));
    }
    body.insert("get".to_string(), Value::Object(get));
}

/// Step 4 for an `index`/`create` item.
fn prepare_index(
    view: &IndexView,
    id: &str,
    source: &Value,
    create: bool,
) -> Result<Prepared, EsError> {
    let Value::Object(source) = source else {
        return Err(not_an_object());
    };
    let doc = to_document(view, id, source.clone())?;
    Ok(Prepared::Index { doc, create })
}

/// The body keys `_update` takes.
const UPDATE_KEYS: [&str; 5] = ["doc", "upsert", "doc_as_upsert", "detect_noop", "_source"];

/// Step 8's body checks and step 4 for an `update` item.
fn prepare_update(
    view: &IndexView,
    id: &str,
    body: &Value,
    param_filter: Option<&SourceFilter>,
) -> Result<Prepared, EsError> {
    let Value::Object(body) = body else {
        return Err(not_an_object());
    };
    if body.contains_key("script") || body.contains_key("scripted_upsert") {
        return Err(EsError::illegal_argument(
            "Loams does not support scripted updates (Elasticsearch API Phase A)",
        ));
    }
    if let Some(key) = body.keys().find(|k| !UPDATE_KEYS.contains(&k.as_str())) {
        return Err(EsError::new(
            400,
            "x_content_parse_exception",
            format!("[1:2] [UpdateRequest] unknown field [{key}]"),
        ));
    }
    let flag = |key: &str, default: bool| -> Result<bool, EsError> {
        match body.get(key) {
            None | Some(Value::Null) => Ok(default),
            Some(Value::Bool(b)) => Ok(*b),
            Some(Value::String(s)) if s == "true" || s == "false" => Ok(s == "true"),
            Some(other) => Err(EsError::new(
                400,
                "x_content_parse_exception",
                format!("[1:2] [UpdateRequest] failed to parse field [{key}]: [{other}]"),
            )),
        }
    };
    let doc_as_upsert = flag("doc_as_upsert", false)?;
    let detect_noop = flag("detect_noop", true)?;
    let mut source = match body.get("doc") {
        None | Some(Value::Null) => return Err(validation("script or doc is missing")),
        Some(Value::Object(doc)) => doc.clone(),
        Some(_) => return Err(not_an_object()),
    };
    check_source(id, &source)?;
    let vectors = split_vectors(view, id, &mut source)?;
    let upsert = match body.get("upsert") {
        None | Some(Value::Null) => None,
        Some(Value::Object(upsert)) => Some(to_document(view, id, upsert.clone())?),
        Some(_) => return Err(not_an_object()),
    };
    let get = match body.get("_source") {
        None => param_filter.cloned(),
        Some(Value::Null) => None,
        Some(value) => Some(SourceFilter::from_body(value)?),
    }
    .filter(|filter| filter.enabled);
    Ok(Prepared::Update {
        source,
        vectors,
        upsert,
        doc_as_upsert,
        detect_noop,
        get,
    })
}

/// A pending vector's new spec and its `es.field` annotation.
type NewVector = (VectorSpec, String);
/// What an item adds to the schema: vectors, then fields.
type ItemAdditions = (Vec<NewVector>, Vec<FieldSpec>);
/// An item's sources, and the dimensions of its vectors by path.
type ItemParts<'p> = (Vec<&'p Map<String, Value>>, Vec<(&'p String, usize)>);

/// The vectors and fields `prepared` adds to `view`'s schema: pending
/// vectors get their `dims` from it, then dynamic mapping of its sources.
fn plan_additions(
    view: &IndexView,
    id: &str,
    prepared: &Prepared,
) -> Result<ItemAdditions, EsError> {
    let (sources, vectors): ItemParts<'_> = match prepared {
        Prepared::Index { doc, .. } => (
            vec![&doc.source],
            doc.vectors.iter().map(|(k, v)| (k, v.len())).collect(),
        ),
        Prepared::Update {
            source,
            vectors,
            upsert,
            ..
        } => {
            let mut sources = vec![source];
            let mut dims: Vec<(&String, usize)> = vectors
                .iter()
                .filter_map(|(k, v)| v.as_ref().map(|v| (k, v.len())))
                .collect();
            if let Some(upsert) = upsert {
                sources.push(&upsert.source);
                dims.extend(upsert.vectors.iter().map(|(k, v)| (k, v.len())));
            }
            (sources, dims)
        }
        Prepared::Delete => return Ok((Vec::new(), Vec::new())),
    };
    let mut specs: Vec<NewVector> = Vec::new();
    for (path, dim) in vectors {
        let Some(declared) = view.es.pending_vectors.get(path) else {
            continue;
        };
        match specs.iter().find(|(spec, _)| &spec.name == path) {
            Some((spec, _)) if spec.dim as usize != dim => {
                return Err(crate::doc::dims_error(path, id, dim, spec.dim as usize));
            }
            Some(_) => {}
            None => specs.push(pending_vector_spec(path, declared, dim)?),
        }
    }
    let mut fields: Vec<FieldSpec> = Vec::new();
    let mut working = view.clone();
    for source in sources {
        let proposed = dynamic_plan(&working, source)?;
        if !proposed.is_empty() {
            working.info.schema.fields.extend(proposed.iter().cloned());
            fields.extend(proposed);
        }
    }
    Ok((specs, fields))
}

/// Step 6's error for the first violation of an item.
fn violation_error(
    view: &IndexView,
    id: &str,
    source: &Map<String, Value>,
    first: &Violation,
) -> EsError {
    let field = first.field.as_str();
    let spec = view.info.schema.field(field);
    let es_type = view
        .es
        .es_types
        .get(field)
        .cloned()
        .unwrap_or_else(|| "object".to_string());
    let path = spec.map_or(field, |s| s.source_path.as_str());
    let value = extract(source, path).into_iter().next();
    // A string a numeric field cannot parse: Java's `NumberFormatException`
    // text, as ES reports it (row T11-3).
    let numeric = matches!(
        es_type.as_str(),
        "long"
            | "integer"
            | "short"
            | "byte"
            | "unsigned_long"
            | "double"
            | "float"
            | "half_float"
            | "scaled_float"
    );
    let reason = match value.as_deref() {
        Some(Value::String(text)) if numeric => format!("For input string: \"{text}\""),
        _ => first.message.clone(),
    };
    let cause = json!({"type": "illegal_argument_exception", "reason": reason});
    field_error(path, &es_type, id, value.as_deref()).with("caused_by", cause)
}

// ----- the single-document endpoints -----

const INDEX_PARAMS: &[&str] = &[
    "op_type",
    "refresh",
    "routing",
    "timeout",
    "wait_for_active_shards",
    "require_alias",
    "pipeline",
    "include_source_on_error",
    "if_seq_no",
    "if_primary_term",
    "version",
    "version_type",
];
const CREATE_PARAMS: &[&str] = &[
    "refresh",
    "routing",
    "timeout",
    "wait_for_active_shards",
    "require_alias",
    "pipeline",
    "include_source_on_error",
    "if_seq_no",
    "if_primary_term",
    "version",
    "version_type",
];
const UPDATE_PARAMS: &[&str] = &[
    "refresh",
    "routing",
    "timeout",
    "wait_for_active_shards",
    "require_alias",
    "retry_on_conflict",
    "_source",
    "_source_includes",
    "_source_excludes",
    "lang",
    "include_source_on_error",
    "if_seq_no",
    "if_primary_term",
];
const DELETE_PARAMS: &[&str] = &[
    "refresh",
    "routing",
    "timeout",
    "wait_for_active_shards",
    "if_seq_no",
    "if_primary_term",
    "version",
    "version_type",
];

/// `refresh`: `true` or empty forces (the body says so), `false` and
/// `wait_for` do not; the gateway never waits (Global Constraints).
pub(crate) fn refresh_param(params: &Params) -> Result<bool, EsError> {
    match params.str("refresh") {
        None | Some("false" | "wait_for") => Ok(false),
        Some("" | "true") => Ok(true),
        Some(other) => Err(EsError::illegal_argument(format!(
            "Unknown value for refresh: [{other}]."
        ))),
    }
}

/// Refuses optimistic concurrency control parameters.
pub(crate) fn check_occ(params: &Params) -> Result<(), EsError> {
    if OCC_PARAMS.iter().any(|p| params.str(p).is_some()) {
        return Err(occ_unsupported());
    }
    Ok(())
}

/// The answer of a single-document write: the item's status and body (the
/// envelope for an error), with the consistency token.
fn single(ctx: &RequestCtx, outcome: ItemOutcome, token: Option<ConsistencyToken>) -> Response {
    let mut response = match &outcome.error {
        Some(error) => fail(ctx, error),
        None => respond(ctx, outcome.status, &Value::Object(outcome.body)),
    };
    if let Some(token) = token
        && let Ok(value) = HeaderValue::from_str(&token.to_string())
    {
        response.headers_mut().insert(TOKEN_HEADER, value);
    }
    response
}

/// Parses a single-document request and runs its one item.
async fn run_single(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
    allowed: &[&str],
    item: impl FnOnce(&Params, Option<Value>) -> Result<WriteItem, EsError>,
    body: &[u8],
) -> Response {
    let prepared = (|| {
        let params = Params::parse(uri.query(), uri.path(), allowed)?;
        check_occ(&params)?;
        let refresh = refresh_param(&params)?;
        let require_alias = params.bool("require_alias")?.unwrap_or(false);
        let source_on_update = SourceFilter::from_params(&params)?;
        let body = json_body(body)?;
        let item = item(&params, body)?;
        Ok::<_, EsError>((params, refresh, require_alias, source_on_update, item))
    })();
    let (params, refresh, require_alias, source_on_update, item) = match prepared {
        Ok(prepared) => prepared,
        Err(err) => return fail(ctx, &err),
    };
    let call = WriteCall {
        gateway: gw,
        ctx,
        index,
        require_alias,
        pipeline: params.str("pipeline").map(str::to_string),
        refresh,
        source_on_update,
    };
    let (mut outcomes, token) = execute(call, vec![item]).await;
    match outcomes.pop() {
        Some(outcome) => single(ctx, outcome, token),
        None => fail(ctx, &EsError::new(500, "exception", "no outcome")),
    }
}

/// The source of an index request.
fn index_source(body: Option<Value>) -> Result<Value, EsError> {
    body.ok_or_else(EsError::body_required)
}

/// `PUT|POST /{index}/_doc/{id}`.
pub(crate) async fn index_doc(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let item = |params: &Params, body: Option<Value>| {
        let create = match params.str("op_type") {
            None | Some("index") => false,
            Some("create") => true,
            Some(other) => {
                return Err(EsError::illegal_argument(format!(
                    "opType must be 'create' or 'index', found: [{other}]"
                )));
            }
        };
        Ok(WriteItem::Index {
            id: Some(id),
            source: index_source(body)?,
            create,
        })
    };
    run_single(&gw, &ctx, &uri, &index, INDEX_PARAMS, item, &body).await
}

/// `POST /{index}/_doc`: an auto id (Ruling 5).
pub(crate) async fn index_auto_id(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    let item = |_: &Params, body: Option<Value>| {
        Ok(WriteItem::Index {
            id: None,
            source: index_source(body)?,
            create: false,
        })
    };
    run_single(&gw, &ctx, &uri, &index, CREATE_PARAMS, item, &body).await
}

/// `PUT|POST /{index}/_create/{id}` (Ruling 7).
pub(crate) async fn create_doc(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let item = |_: &Params, body: Option<Value>| {
        Ok(WriteItem::Index {
            id: Some(id),
            source: index_source(body)?,
            create: true,
        })
    };
    run_single(&gw, &ctx, &uri, &index, CREATE_PARAMS, item, &body).await
}

/// `POST /{index}/_update/{id}` (Ruling 8).
pub(crate) async fn update_doc(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let item = |_: &Params, body: Option<Value>| {
        Ok(WriteItem::Update {
            id,
            // ES checks the update request itself first (row T11-3).
            body: body.ok_or_else(|| validation("script or doc is missing"))?,
        })
    };
    run_single(&gw, &ctx, &uri, &index, UPDATE_PARAMS, item, &body).await
}

/// `DELETE /{index}/_doc/{id}`.
pub(crate) async fn delete_doc(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let item = |_: &Params, _: Option<Value>| Ok(WriteItem::Delete { id });
    run_single(&gw, &ctx, &uri, &index, DELETE_PARAMS, item, &body).await
}
