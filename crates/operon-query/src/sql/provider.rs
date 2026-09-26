//! `CollectionProvider` and `CollectionScanExec` (plan M1.2 Task 10 rules 2
//! and 3): a collection as a DataFusion table, and the Arrow rows of its
//! documents, shared with the search table functions.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Builder, Int64Builder,
    ListBuilder, RecordBatch, RecordBatchOptions, StringBuilder, StructArray,
    TimestampMillisecondBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance::dataset::scanner::{DatasetRecordBatchStream, RowAddrMask, RowAddrTreeMap};
use operon_collection::{
    CollectionError, CollectionSchema, FieldKind, IndexValue, PrimaryKey, SparseVector, coerce,
    extract, sparse_column,
};
use operon_common::NamespaceId;
use operon_common::meta::Collection;
use roaring::RoaringTreemap;
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::exec::doc_fetch::{FetchColumns, FetchedRow, LanceColumns, tail_row};
use crate::exec::filter_bitmap::FilterBitmapExec;
use crate::exec::mask::RowSet;
use crate::exec::{df_error, plan_properties};
use crate::ir::Query;
use crate::json::pk::format_uuid;
use crate::read::{ReadView, collection_error};
use crate::sql::SqlScope;
use crate::sql::exprs::expr_to_query;
use crate::tail::{TAIL_ROWID_BASE, TailDoc};

/// `_id`: the primary key's display form.
pub(crate) const ID_COLUMN: &str = "_id";
/// `_score`: the search table functions' score.
pub(crate) const SCORE_COLUMN: &str = "_score";
const SOURCE_COLUMN: &str = "_source";
const SEQ_NO_COLUMN: &str = "_seq_no";
const PARTITION_COLUMN: &str = "_partition";

/// One output column of a collection table or a search table function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Column {
    Id,
    Score,
    Source,
    SeqNo,
    Partition,
    /// An index into `schema.fields`.
    Field(usize),
    /// An index into `schema.vectors`.
    Vector(usize),
    /// An index into `schema.sparse_vectors`.
    Sparse(usize),
}

/// The column name of a dense vector (`_vector` for the unnamed one).
fn vector_name(name: &str) -> String {
    if name.is_empty() {
        "_vector".to_string()
    } else {
        name.to_string()
    }
}

/// The Arrow type of a field's first-value column.
fn field_type(kind: &FieldKind) -> DataType {
    match kind {
        FieldKind::Text { .. } | FieldKind::Keyword | FieldKind::Uuid | FieldKind::Json => {
            DataType::Utf8
        }
        FieldKind::I64 => DataType::Int64,
        FieldKind::F64 => DataType::Float64,
        FieldKind::Bool => DataType::Boolean,
        FieldKind::Date => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    }
}

fn vector_item() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Float32, true))
}

/// The Lance type of sparse column `j` (row 0.28).
fn sparse_type(schema: &CollectionSchema, j: usize) -> DataType {
    operon_collection::arrow_schema(schema)
        .field_with_name(&sparse_column(j))
        .map(|field| field.data_type().clone())
        .unwrap_or(DataType::Null)
}

/// Every column of `schema`'s table, in rule 2's order (`_score` after
/// `_id` for the search table functions).
pub(crate) fn columns_of(schema: &CollectionSchema, with_score: bool) -> Vec<(Column, Field)> {
    let mut out = vec![(Column::Id, Field::new(ID_COLUMN, DataType::Utf8, false))];
    if with_score {
        out.push((
            Column::Score,
            Field::new(SCORE_COLUMN, DataType::Float32, false),
        ));
    }
    out.push((
        Column::Source,
        Field::new(SOURCE_COLUMN, DataType::Utf8, false),
    ));
    out.push((
        Column::SeqNo,
        Field::new(SEQ_NO_COLUMN, DataType::UInt64, true),
    ));
    out.push((
        Column::Partition,
        Field::new(PARTITION_COLUMN, DataType::UInt32, true),
    ));
    for (i, spec) in schema.fields.iter().enumerate() {
        out.push((
            Column::Field(i),
            Field::new(&spec.name, field_type(&spec.kind), true),
        ));
    }
    for (i, spec) in schema.vectors.iter().enumerate() {
        // Dims are validated far below `i32::MAX`.
        let dim = i32::try_from(spec.dim).unwrap_or(i32::MAX);
        out.push((
            Column::Vector(i),
            Field::new(
                vector_name(&spec.name),
                DataType::FixedSizeList(vector_item(), dim),
                true,
            ),
        ));
    }
    for (j, spec) in schema.sparse_vectors.iter().enumerate() {
        out.push((
            Column::Sparse(j),
            Field::new(&spec.name, sparse_type(schema, j), true),
        ));
    }
    out
}

/// Rule 2: the columns of a collection's table.
pub fn collection_arrow_schema(schema: &CollectionSchema) -> SchemaRef {
    Arc::new(Schema::new(
        columns_of(schema, false)
            .into_iter()
            .map(|(_, field)| field)
            .collect::<Vec<_>>(),
    ))
}

/// The display form of a key: a `U64` in decimal, a `Str` as itself, a
/// `Uuid` lowercase hyphenated.
pub(crate) fn pk_text(pk: &PrimaryKey) -> String {
    match pk {
        PrimaryKey::U64(n) => n.to_string(),
        PrimaryKey::Str(s) => s.clone(),
        PrimaryKey::Uuid(bytes) => format_uuid(bytes),
    }
}

/// One output row, before projection.
#[derive(Clone, Debug, Default)]
pub(crate) struct SqlRow {
    pub pk: Option<PrimaryKey>,
    pub score: Option<f32>,
    pub source: Option<Map<String, Value>>,
    pub seq_no: Option<u64>,
    pub partition: Option<u32>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    pub sparse: BTreeMap<String, SparseVector>,
}

impl From<FetchedRow> for SqlRow {
    fn from(row: FetchedRow) -> Self {
        Self {
            pk: Some(row.pk),
            score: None,
            source: row.source,
            seq_no: Some(row.seq_no),
            partition: Some(row.partition),
            vectors: row.vectors,
            sparse: row.sparse_vectors,
        }
    }
}

/// The JSON value at `segments` in `object`, whole (arrays are transparent
/// before the last segment only); the first match in document order.
fn json_at<'a>(object: &'a Map<String, Value>, segments: &[&str]) -> Option<&'a Value> {
    (1..=segments.len()).find_map(|k| {
        object
            .get(&segments[..k].join("."))
            .and_then(|value| json_in(value, &segments[k..]))
    })
}

fn json_in<'a>(value: &'a Value, segments: &[&str]) -> Option<&'a Value> {
    if segments.is_empty() {
        return Some(value);
    }
    match value {
        Value::Object(object) => json_at(object, segments),
        Value::Array(items) => items.iter().find_map(|item| json_in(item, segments)),
        _ => None,
    }
}

/// Encodes rows into batches of a projection of a table's columns.
#[derive(Debug)]
pub(crate) struct RowEncoder {
    schema: CollectionSchema,
    columns: Vec<Column>,
    output: SchemaRef,
}

impl RowEncoder {
    /// The projection `projection` (indices into `columns_of(schema,
    /// with_score)`; `None`: every column).
    pub(crate) fn new(
        schema: &CollectionSchema,
        with_score: bool,
        projection: Option<&Vec<usize>>,
    ) -> DfResult<Self> {
        let all = columns_of(schema, with_score);
        let picked: Vec<(Column, Field)> = match projection {
            None => all,
            Some(indices) => indices
                .iter()
                .map(|i| {
                    all.get(*i).cloned().ok_or_else(|| {
                        DataFusionError::Internal(format!("column {i} is out of range"))
                    })
                })
                .collect::<DfResult<_>>()?,
        };
        let (columns, fields): (Vec<Column>, Vec<Field>) = picked.into_iter().unzip();
        Ok(Self {
            schema: schema.clone(),
            columns,
            output: Arc::new(Schema::new(fields)),
        })
    }

    pub(crate) fn output(&self) -> SchemaRef {
        self.output.clone()
    }

    pub(crate) fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Whether any projected column is read from `_source`.
    pub(crate) fn needs_source(&self) -> bool {
        self.columns
            .iter()
            .any(|column| matches!(column, Column::Source | Column::Field(_)))
    }

    /// What to fetch for the projected columns.
    pub(crate) fn fetch_columns(&self) -> FetchColumns {
        FetchColumns {
            source: self.needs_source(),
            vectors: self
                .columns
                .iter()
                .filter_map(|column| match column {
                    Column::Vector(i) => Some(*i),
                    _ => None,
                })
                .collect(),
            sparse: self
                .columns
                .iter()
                .filter_map(|column| match column {
                    Column::Sparse(j) => Some(*j),
                    _ => None,
                })
                .collect(),
        }
    }

    /// The projected vector names (dense and sparse), for a search's
    /// `Projection.vectors`.
    pub(crate) fn vector_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|column| match column {
                Column::Vector(i) => self.schema.vectors.get(*i).map(|v| v.name.clone()),
                Column::Sparse(j) => self.schema.sparse_vectors.get(*j).map(|v| v.name.clone()),
                _ => None,
            })
            .collect()
    }

    /// The first value of field `i` in `source`, extracted and coerced.
    fn first_value(&self, i: usize, source: Option<&Map<String, Value>>) -> Option<IndexValue> {
        let spec = &self.schema.fields[i];
        let source = source?;
        extract(source, &spec.source_path)
            .iter()
            .find_map(|value| coerce(&spec.kind, value.as_ref()).ok().flatten())
    }

    /// The JSON text of Json field `i` in `source`.
    fn json_text(&self, i: usize, source: Option<&Map<String, Value>>) -> Option<String> {
        let spec = &self.schema.fields[i];
        let source = source?;
        let value = if spec.source_path.is_empty() {
            return Some(Value::Object(source.clone()).to_string());
        } else {
            let segments: Vec<&str> = spec.source_path.split('.').collect();
            json_at(source, &segments)?
        };
        (!value.is_null()).then(|| value.to_string())
    }

    fn field_column(&self, i: usize, rows: &[SqlRow]) -> ArrayRef {
        let kind = &self.schema.fields[i].kind;
        let values = || {
            rows.iter()
                .map(|row| self.first_value(i, row.source.as_ref()))
        };
        match kind {
            FieldKind::Json => {
                let mut b = StringBuilder::new();
                for row in rows {
                    b.append_option(self.json_text(i, row.source.as_ref()));
                }
                Arc::new(b.finish())
            }
            FieldKind::Text { .. } | FieldKind::Keyword | FieldKind::Uuid => {
                let mut b = StringBuilder::new();
                for value in values() {
                    b.append_option(match value {
                        Some(IndexValue::Text(s) | IndexValue::Keyword(s)) => Some(s),
                        Some(IndexValue::Uuid(bytes)) => Some(format_uuid(&bytes)),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            FieldKind::I64 => {
                let mut b = Int64Builder::new();
                for value in values() {
                    b.append_option(match value {
                        Some(IndexValue::I64(n)) => Some(n),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            FieldKind::F64 => {
                let mut b = Float64Builder::new();
                for value in values() {
                    b.append_option(match value {
                        Some(IndexValue::F64(x)) => Some(x),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            FieldKind::Bool => {
                let mut b = BooleanBuilder::new();
                for value in values() {
                    b.append_option(match value {
                        Some(IndexValue::Bool(x)) => Some(x),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
            FieldKind::Date => {
                let mut b = TimestampMillisecondBuilder::new().with_timezone("UTC");
                for value in values() {
                    b.append_option(match value {
                        Some(IndexValue::Date(ms)) => Some(ms),
                        _ => None,
                    });
                }
                Arc::new(b.finish())
            }
        }
    }

    fn vector_column(&self, i: usize, rows: &[SqlRow]) -> Result<ArrayRef, ServiceError> {
        let spec = &self.schema.vectors[i];
        let dim = i32::try_from(spec.dim).unwrap_or(i32::MAX);
        let mut b = FixedSizeListBuilder::new(Float32Builder::new(), dim).with_field(vector_item());
        for row in rows {
            match row.vectors.get(&spec.name) {
                Some(vector) => {
                    if vector.len() != spec.dim as usize {
                        return Err(ServiceError::Internal(format!(
                            "vector {} has {} dimensions, the schema {}",
                            spec.name,
                            vector.len(),
                            spec.dim
                        )));
                    }
                    b.values().append_slice(vector);
                    b.append(true);
                }
                None => {
                    b.values().append_nulls(spec.dim as usize);
                    b.append(false);
                }
            }
        }
        Ok(Arc::new(b.finish()))
    }

    fn sparse_column(&self, j: usize, rows: &[SqlRow]) -> Result<ArrayRef, ServiceError> {
        let name = &self.schema.sparse_vectors[j].name;
        let DataType::Struct(fields) = sparse_type(&self.schema, j) else {
            return Err(ServiceError::Internal(format!(
                "sparse column {j} is not a struct"
            )));
        };
        let item = |fields: &Fields, child: &str| -> Result<Arc<Field>, ServiceError> {
            match fields
                .find(child)
                .map(|(_, field)| field.data_type().clone())
            {
                Some(DataType::List(item)) => Ok(item),
                _ => Err(ServiceError::Internal(format!(
                    "sparse column {j} lacks list {child}"
                ))),
            }
        };
        let mut indices =
            ListBuilder::new(UInt32Builder::new()).with_field(item(&fields, "indices")?);
        let mut values =
            ListBuilder::new(Float32Builder::new()).with_field(item(&fields, "values")?);
        let mut valid = Vec::with_capacity(rows.len());
        for row in rows {
            let vector = row.sparse.get(name);
            if let Some(vector) = vector {
                indices.values().append_slice(vector.indices());
                values.values().append_slice(vector.values());
            }
            indices.append(true);
            values.append(true);
            valid.push(vector.is_some());
        }
        let array = StructArray::try_new(
            fields,
            vec![Arc::new(indices.finish()), Arc::new(values.finish())],
            Some(NullBuffer::from(valid)),
        )
        .map_err(|err| ServiceError::Internal(format!("sparse column {j}: {err}")))?;
        Ok(Arc::new(array))
    }

    /// `rows` as one batch of the projection.
    pub(crate) fn encode(&self, rows: &[SqlRow]) -> Result<RecordBatch, ServiceError> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let array: ArrayRef = match column {
                Column::Id => {
                    let mut b = StringBuilder::new();
                    for row in rows {
                        b.append_value(row.pk.as_ref().map(pk_text).unwrap_or_default());
                    }
                    Arc::new(b.finish())
                }
                Column::Score => {
                    let mut b = Float32Builder::new();
                    for row in rows {
                        b.append_value(row.score.unwrap_or_default());
                    }
                    Arc::new(b.finish())
                }
                Column::Source => {
                    let mut b = StringBuilder::new();
                    for row in rows {
                        let text = match &row.source {
                            Some(source) => serde_json::to_string(source).map_err(|err| {
                                ServiceError::Internal(format!("encoding _source: {err}"))
                            })?,
                            None => "{}".to_string(),
                        };
                        b.append_value(text);
                    }
                    Arc::new(b.finish())
                }
                Column::SeqNo => {
                    let mut b = UInt64Builder::new();
                    for row in rows {
                        b.append_option(row.seq_no);
                    }
                    Arc::new(b.finish())
                }
                Column::Partition => {
                    let mut b = UInt32Builder::new();
                    for row in rows {
                        b.append_option(row.partition);
                    }
                    Arc::new(b.finish())
                }
                Column::Field(i) => self.field_column(*i, rows),
                Column::Vector(i) => self.vector_column(*i, rows)?,
                Column::Sparse(j) => self.sparse_column(*j, rows)?,
            };
            arrays.push(array);
        }
        RecordBatch::try_new_with_options(
            self.output.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(|err| ServiceError::Internal(format!("encoding a batch: {err}")))
    }
}

/// A collection as a table of `"<ns>".collections`.
#[derive(Debug)]
pub struct CollectionProvider {
    scope: SqlScope,
    ns_id: NamespaceId,
    collection: Collection,
    schema: SchemaRef,
    pushdown: bool,
}

impl CollectionProvider {
    pub(crate) fn new(scope: SqlScope, ns_id: NamespaceId, collection: Collection) -> Self {
        Self {
            schema: collection_arrow_schema(&collection.schema),
            scope,
            ns_id,
            collection,
            pushdown: true,
        }
    }

    /// The same table with filter pushdown switched off (tests compare
    /// pushed-down results with DataFusion's own filtering).
    #[cfg(feature = "test-util")]
    pub fn without_pushdown(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            ns_id: self.ns_id,
            collection: self.collection.clone(),
            schema: self.schema.clone(),
            pushdown: false,
        }
    }
}

#[async_trait]
impl TableProvider for CollectionProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let encoder = RowEncoder::new(&self.collection.schema, false, projection)?;
        let queries: Vec<Query> = if self.pushdown {
            filters
                .iter()
                .filter_map(|filter| expr_to_query(filter, &self.collection.schema))
                .collect()
        } else {
            Vec::new()
        };
        let filter = match queries.len() {
            0 => None,
            1 => queries.into_iter().next(),
            _ => Some(Query::Bool {
                must: Vec::new(),
                should: Vec::new(),
                must_not: Vec::new(),
                filter: queries,
                minimum_should_match: None,
            }),
        };
        Ok(Arc::new(CollectionScanExec {
            properties: plan_properties(encoder.output()),
            inner: Arc::new(ScanInner {
                scope: self.scope.clone(),
                ns_id: self.ns_id,
                collection: self.collection.clone(),
                encoder,
                filter,
                limit,
            }),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.pushdown && expr_to_query(filter, &self.collection.schema).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[derive(Debug)]
struct ScanInner {
    scope: SqlScope,
    ns_id: NamespaceId,
    collection: Collection,
    encoder: RowEncoder,
    filter: Option<Query>,
    limit: Option<usize>,
}

/// Rule 3: the live documents of a collection (in the pushed-down filter's
/// rows when given), durable rows first, then the tail's.
pub struct CollectionScanExec {
    inner: Arc<ScanInner>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for CollectionScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CollectionScanExec")
            .field("collection", &self.inner.collection.name)
            .field("filter", &self.inner.filter)
            .field("limit", &self.inner.limit)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CollectionScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CollectionScanExec: collection={}, filtered={}, limit={:?}",
            self.inner.collection.name,
            self.inner.filter.is_some(),
            self.inner.limit
        )
    }
}

impl ExecutionPlan for CollectionScanExec {
    fn name(&self) -> &str {
        "CollectionScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "CollectionScanExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CollectionScanExec has one partition, not {partition}"
            )));
        }
        let run = ScanRun {
            inner: self.inner.clone(),
            remaining: self.inner.limit,
            phase: Phase::Start,
        };
        let stream =
            futures::stream::try_unfold(
                run,
                |run| async move { run.next().await.map_err(df_error) },
            );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.encoder.output(),
            stream,
        )))
    }
}

/// Where a scan is.
enum Phase {
    Start,
    Durable(Box<Durable>),
    Tail(Box<TailPart>),
    Done,
}

/// The durable side of a started scan.
struct Durable {
    view: Arc<ReadView>,
    allowed: Option<RoaringTreemap>,
    columns: LanceColumns,
    stream: DatasetRecordBatchStream,
    tail: TailPart,
}

/// The tail side of a started scan: its admitted live docs.
struct TailPart {
    view: Arc<ReadView>,
    docs: Vec<Arc<TailDoc>>,
    at: usize,
}

struct ScanRun {
    inner: Arc<ScanInner>,
    remaining: Option<usize>,
    phase: Phase,
}

fn lance_error(err: lance::Error) -> ServiceError {
    collection_error(CollectionError::from(err))
}

impl ScanRun {
    /// The next non-empty batch, and the scan after it.
    async fn next(mut self) -> Result<Option<(RecordBatch, Self)>, ServiceError> {
        loop {
            if self.remaining == Some(0) {
                return Ok(None);
            }
            match std::mem::replace(&mut self.phase, Phase::Done) {
                Phase::Start => self.phase = Self::start(self.inner.clone()).await?,
                Phase::Durable(mut durable) => match durable.stream.next().await {
                    Some(batch) => {
                        let batch = batch.map_err(lance_error)?;
                        let shadow = durable.view.tail.shadow();
                        let rows: Vec<SqlRow> = durable
                            .columns
                            .decode(&batch)?
                            .into_iter()
                            .filter(|row| {
                                !shadow.contains(row.row_id)
                                    && durable
                                        .allowed
                                        .as_ref()
                                        .is_none_or(|allowed| allowed.contains(row.row_id))
                            })
                            .map(SqlRow::from)
                            .collect();
                        self.phase = Phase::Durable(durable);
                        if let Some(batch) = self.emit(rows)? {
                            return Ok(Some((batch, self)));
                        }
                    }
                    None => self.phase = Phase::Tail(Box::new(durable.tail)),
                },
                Phase::Tail(mut tail) => {
                    if tail.at >= tail.docs.len() {
                        return Ok(None);
                    }
                    let batch_rows = self.inner.scope.service.config().sql.batch_rows.max(1);
                    let end = (tail.at + batch_rows).min(tail.docs.len());
                    let columns = self.inner.encoder.fetch_columns();
                    let rows = tail.docs[tail.at..end]
                        .iter()
                        .map(|doc| {
                            tail_row(&tail.view.collection.schema, doc, &columns).map(SqlRow::from)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    tail.at = end;
                    self.phase = Phase::Tail(tail);
                    if let Some(batch) = self.emit(rows)? {
                        return Ok(Some((batch, self)));
                    }
                }
                Phase::Done => return Ok(None),
            }
        }
    }

    /// `rows` cut to the limit, as a batch; `None` when there are none.
    fn emit(&mut self, mut rows: Vec<SqlRow>) -> Result<Option<RecordBatch>, ServiceError> {
        if let Some(remaining) = &mut self.remaining {
            rows.truncate(*remaining);
            *remaining -= rows.len();
        }
        if rows.is_empty() {
            return Ok(None);
        }
        self.inner.encoder.encode(&rows).map(Some)
    }

    /// Steps 1–4: the view, the filter's rows, the Lance scan and the tail's
    /// admitted docs.
    async fn start(inner: Arc<ScanInner>) -> Result<Phase, ServiceError> {
        let scope = &inner.scope;
        let view = Arc::new(scope.view(inner.ns_id, &inner.collection).await?);
        // A split written before a field existed does not index it (A4), so
        // a pushed-down filter could miss its documents: such a manifest is
        // scanned whole and DataFusion's own filter decides.
        let current = view.collection.schema.version;
        let filter = inner.filter.clone().filter(|_| {
            view.snapshot
                .manifest()
                .splits
                .iter()
                .all(|split| split.schema_version >= current)
        });
        let allowed = match filter {
            None => None,
            Some(filter) => {
                let parallelism = scope.service.config().search.parallelism;
                match FilterBitmapExec::new(view.clone(), filter, parallelism)
                    .rows()
                    .await?
                {
                    RowSet::All => None,
                    RowSet::Rows(rows) => Some(rows),
                }
            }
        };
        let docs: Vec<Arc<TailDoc>> = view
            .tail
            .live_docs()
            .into_iter()
            .filter(|doc| {
                allowed
                    .as_ref()
                    .is_none_or(|allowed| allowed.contains(doc.row_id))
            })
            .collect();
        let tail = TailPart {
            view: view.clone(),
            docs,
            at: 0,
        };
        let Some(dataset) = view.snapshot.dataset() else {
            return Ok(Phase::Tail(Box::new(tail)));
        };
        let shadow = view.tail.shadow();
        let durable_allowed = allowed.as_ref().map(|rows| {
            let mut durable = rows.clone();
            durable.remove_range(TAIL_ROWID_BASE..);
            durable -= shadow;
            durable
        });
        let mask = match &durable_allowed {
            Some(rows) if rows.is_empty() => return Ok(Phase::Tail(Box::new(tail))),
            Some(rows) => Some(RowAddrMask::from_allowed(RowAddrTreeMap::from(
                rows.clone(),
            ))),
            None if shadow.is_empty() => None,
            None => Some(RowAddrMask::from_block(RowAddrTreeMap::from(
                shadow.clone(),
            ))),
        };
        let columns = LanceColumns::new(
            dataset,
            &inner.collection.schema,
            &inner.encoder.fetch_columns(),
        );
        let batch_rows = scope.service.config().sql.batch_rows.max(1);
        let mut scanner = dataset.scan();
        scanner
            .project(&columns.names)
            .map_err(lance_error)?
            .with_row_id()
            .batch_size(batch_rows);
        if let Some(mask) = mask {
            scanner.with_row_addr_prefilter(mask);
        }
        let stream = scanner.try_into_stream().await.map_err(lance_error)?;
        Ok(Phase::Durable(Box::new(Durable {
            view: view.clone(),
            allowed: durable_allowed,
            columns,
            stream,
            tail,
        })))
    }
}
