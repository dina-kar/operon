//! DataFusion operators over a read view (plan M1.2 Tasks 5–7).
//!
//! Every operator produces its rows in its own `execute`, from an async
//! block, through a `RecordBatchStreamAdapter`, with one output partition.
//! Rows live in one row-id space: Lance stable row ids for durable rows,
//! `TAIL_ROWID_BASE + seq` for tail docs (Ruling 7). Deleted split docs and
//! shadowed rows are masked inside every operator, before any top-k cut.
//!
//! - [`schema`]: the Arrow schemas operators exchange;
//! - [`order`]: the effective sort (R10) and `search_after`;
//! - [`mask`]: row sets and split masks;
//! - [`tantivy_search`]: BM25 search over splits and the tail;
//! - [`filter_bitmap`]: a filter as a row-id set;
//! - [`ann`]: dense vector search over Lance, the tail and the hot tier;
//! - [`sparse`]: exact sparse vector search over splits and the tail.

pub mod ann;
pub mod filter_bitmap;
pub mod mask;
pub mod order;
pub mod schema;
pub mod sparse;
pub mod tantivy_search;

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{Partitioning, PlanProperties};
use operon_collection::{CollectionSchema, FieldKind, PK_FIELD, PrimaryKey, ROWID_FIELD};
use operon_quickwit::doc_mapper::FastFieldWarmupInfo;
use roaring::RoaringBitmap;
use tantivy::columnar::{BytesColumn, Column, StrColumn};
use tantivy::schema::{FieldType, Schema};
use tantivy::{DocId, Searcher, SegmentReader};

use crate::error::ServiceError;
use crate::ir::{SortOrder, SortValue};
use crate::read::ReadView;
use crate::text::compile::CompiledQuery;
use crate::text::fields::{ResolvedField, resolve_field};
use crate::text::splits::{OpenSplit, open_splits_with, tail_segment_masks};

pub use ann::AnnExec;
pub use filter_bitmap::FilterBitmapExec;
pub use mask::{RowSet, SplitMask};
pub use order::{EffectiveSort, RankMode};
pub use schema::{Ranked, batch_to_ranked, ranked_schema, ranked_to_batch, rowid_schema};
pub use sparse::SparseExec;
pub use tantivy_search::TantivySearchExec;

/// The plan properties of every operator: one partition, final emission,
/// bounded.
pub(crate) fn plan_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

pub(crate) fn df_error(err: ServiceError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub(crate) fn tantivy_error(err: impl std::fmt::Display) -> ServiceError {
    ServiceError::Internal(format!("tantivy: {err}"))
}

/// Runs `work` on the blocking pool.
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|err| ServiceError::Internal(format!("search task: {err}")))?
}

/// A collection field the effective sort reads from its fast column.
#[derive(Clone, Debug)]
pub(crate) struct SortField {
    pub name: String,
    pub order: SortOrder,
    pub uuid: bool,
}

/// The sort's `Field` keys, checked against the collection schema: each
/// must be a fast field, and JSON paths are refused (rule 5.3).
pub(crate) fn sort_fields(
    schema: &CollectionSchema,
    sort: &EffectiveSort,
) -> Result<Vec<SortField>, ServiceError> {
    sort.field_keys()
        .map(|(name, order, _)| match resolve_field(schema, name) {
            ResolvedField::Plain { spec } if spec.fast && spec.kind != FieldKind::Json => {
                Ok(SortField {
                    name: name.to_string(),
                    order,
                    uuid: spec.kind == FieldKind::Uuid,
                })
            }
            ResolvedField::JsonPath { .. } => Err(ServiceError::InvalidArgument(
                "sorting on JSON paths is not supported in M1".to_string(),
            )),
            _ => Err(ServiceError::InvalidArgument(format!(
                "sort field {name} is not a fast field"
            ))),
        })
        .collect()
}

/// One searchable index of a view: a split or the tail's RAM index, with
/// its query compiled for its schema and its masked docs per segment.
pub(crate) struct Unit {
    pub searcher: Searcher,
    pub masks: Vec<RoaringBitmap>,
    pub query: Box<dyn tantivy::query::Query>,
    pub is_tail: bool,
}

/// A query compiler for one index schema.
pub(crate) type Compile<'a> =
    dyn Fn(&Schema) -> Result<CompiledQuery, ServiceError> + Send + Sync + 'a;

/// Opens the view's splits warmed for `compile`'s query and the sort
/// fields, and the tail; in manifest order, then the tail.
pub(crate) async fn open_units(
    view: &ReadView,
    compile: &Compile<'_>,
    sort_fields: &[SortField],
    parallelism: usize,
) -> Result<(Vec<OpenSplit>, Vec<Unit>), ServiceError> {
    let warm = |schema: &Schema| {
        let mut warmup = compile(schema)?.warmup;
        for field in sort_fields {
            if schema.get_field(&field.name).is_ok() {
                warmup.fast_fields.insert(FastFieldWarmupInfo {
                    name: field.name.clone(),
                    with_subfields: false,
                });
            }
        }
        Ok(warmup)
    };
    let splits = open_splits_with(view, &warm, parallelism).await?;
    let mut units = Vec::with_capacity(splits.len() + 1);
    for split in &splits {
        units.push(Unit {
            searcher: split.searcher.clone(),
            masks: split.segment_masks(),
            query: compile(split.searcher.schema())?.query,
            is_tail: false,
        });
    }
    if let Some(searcher) = view.tail.searcher() {
        units.push(Unit {
            searcher: searcher.clone(),
            masks: tail_segment_masks(searcher, view.tail.live())?,
            query: compile(searcher.schema())?.query,
            is_tail: true,
        });
    }
    Ok((splits, units))
}

/// The fast column of one sort field in one segment.
enum FieldColumn {
    Missing,
    I64(Column<i64>),
    F64(Column<f64>),
    Bool(Column<bool>),
    Date(Column<tantivy::DateTime>),
    Str(StrColumn, bool),
}

/// Picks the minimum for `asc`, the maximum for `desc` (ES's default mode
/// for multi-valued fields).
fn pick<T: PartialOrd>(values: impl Iterator<Item = T>, order: SortOrder) -> Option<T> {
    values.fold(None, |best, value| match best {
        None => Some(value),
        Some(best) => {
            let better = match order {
                SortOrder::Asc => value < best,
                SortOrder::Desc => value > best,
            };
            Some(if better { value } else { best })
        }
    })
}

/// The `_rowid`, `_pk` and sort-field columns of one segment.
pub(crate) struct Columns {
    rowid: Column<u64>,
    pk: BytesColumn,
    fields: Vec<(FieldColumn, SortOrder)>,
}

impl Columns {
    pub fn open(reader: &SegmentReader, fields: &[SortField]) -> Result<Self, ServiceError> {
        let fast = reader.fast_fields();
        let rowid = fast.u64(ROWID_FIELD).map_err(tantivy_error)?;
        let pk = fast
            .bytes(PK_FIELD)
            .map_err(tantivy_error)?
            .ok_or_else(|| ServiceError::Internal("a split without _pk".to_string()))?;
        let schema: &Schema = reader.schema();
        let fields = fields
            .iter()
            .map(|field| {
                let column = match schema.get_field(&field.name) {
                    Err(_) => FieldColumn::Missing,
                    Ok(f) => match schema.get_field_entry(f).field_type() {
                        FieldType::I64(_) => {
                            FieldColumn::I64(fast.i64(&field.name).map_err(tantivy_error)?)
                        }
                        FieldType::F64(_) => {
                            FieldColumn::F64(fast.f64(&field.name).map_err(tantivy_error)?)
                        }
                        FieldType::Bool(_) => {
                            FieldColumn::Bool(fast.bool(&field.name).map_err(tantivy_error)?)
                        }
                        FieldType::Date(_) => {
                            FieldColumn::Date(fast.date(&field.name).map_err(tantivy_error)?)
                        }
                        FieldType::Str(_) => match fast.str(&field.name).map_err(tantivy_error)? {
                            Some(column) => FieldColumn::Str(column, field.uuid),
                            None => FieldColumn::Missing,
                        },
                        _ => FieldColumn::Missing,
                    },
                };
                Ok((column, field.order))
            })
            .collect::<Result<_, ServiceError>>()?;
        Ok(Self { rowid, pk, fields })
    }

    pub fn row_id(&self, doc: DocId) -> Result<u64, ServiceError> {
        self.rowid
            .first(doc)
            .ok_or_else(|| ServiceError::Internal(format!("doc {doc} has no _rowid")))
    }

    pub fn pk(&self, doc: DocId) -> Result<PrimaryKey, ServiceError> {
        let ord = self
            .pk
            .term_ords(doc)
            .next()
            .ok_or_else(|| ServiceError::Internal(format!("doc {doc} has no _pk")))?;
        let mut bytes = Vec::new();
        self.pk
            .ord_to_bytes(ord, &mut bytes)
            .map_err(tantivy_error)?;
        PrimaryKey::from_canonical(&bytes)
            .map_err(|err| ServiceError::Internal(format!("_pk of doc {doc}: {err}")))
    }

    /// One value per sort field (`Null` when the doc has none).
    pub fn sort_values(&self, doc: DocId) -> Vec<SortValue> {
        self.fields
            .iter()
            .map(|(column, order)| {
                let value = match column {
                    FieldColumn::Missing => None,
                    FieldColumn::I64(c) => pick(c.values_for_doc(doc), *order).map(SortValue::I64),
                    FieldColumn::F64(c) => pick(c.values_for_doc(doc), *order).map(SortValue::F64),
                    FieldColumn::Bool(c) => {
                        pick(c.values_for_doc(doc), *order).map(SortValue::Bool)
                    }
                    FieldColumn::Date(c) => pick(c.values_for_doc(doc), *order)
                        .map(|date| SortValue::I64(date.into_timestamp_millis())),
                    FieldColumn::Str(c, uuid) => pick(c.term_ords(doc), *order).and_then(|ord| {
                        let mut text = String::new();
                        c.ord_to_str(ord, &mut text).ok()?;
                        Some(match crate::json::pk::parse_uuid(&text) {
                            Some(bytes) if *uuid => SortValue::Uuid(bytes),
                            _ => SortValue::Str(text),
                        })
                    }),
                };
                value.unwrap_or(SortValue::Null)
            })
            .collect()
    }

    pub fn ranked(&self, doc: DocId, score: f32) -> Result<Ranked, ServiceError> {
        Ok(Ranked {
            row_id: self.row_id(doc)?,
            pk: self.pk(doc)?,
            score,
            sort: self.sort_values(doc),
        })
    }
}
