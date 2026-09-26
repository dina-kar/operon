//! `DocFetchExec` and [`fetch_rows`] (plan M1.2 Task 7 rule 9): the
//! documents of ranked rows, durable rows with one coalesced Lance take and
//! tail rows from the tail snapshot, in input order.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Float32Array, ListArray, RecordBatch, StringArray,
    StructArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{Float32Type, SchemaRef, UInt32Type, UInt64Type};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use lance::dataset::{ProjectionRequest, ROW_ID};
use operon_collection::{
    CollectionError, INGEST_OFFSET_COLUMN, INGEST_PARTITION_COLUMN, PK_COLUMN, PrimaryKey,
    SOURCE_COLUMN, SparseVector, sparse_column, vector_column,
};
use serde_json::{Map, Value};

use crate::error::ServiceError;
use crate::exec::fusion::collect_ranked;
use crate::exec::schema::{Ranked, encode_sort, fetched_schema};
use crate::exec::{df_error, plan_properties};
use crate::read::{ReadView, collection_error};
use crate::tail::TAIL_ROWID_BASE;

/// What to fetch of each row besides its key, `seq_no` and partition.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FetchColumns {
    pub source: bool,
    /// Indexes into `schema.vectors`.
    pub vectors: Vec<usize>,
    /// Indexes into `schema.sparse_vectors`.
    pub sparse: Vec<usize>,
}

/// A fetched row: `source` when asked for, and the asked-for vectors the
/// document has.
#[derive(Clone, Debug, PartialEq)]
pub struct FetchedRow {
    pub row_id: u64,
    pub pk: PrimaryKey,
    pub source: Option<Map<String, Value>>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    pub sparse_vectors: BTreeMap<String, SparseVector>,
    pub seq_no: u64,
    pub partition: u32,
}

fn lance_error(err: lance::Error) -> ServiceError {
    collection_error(CollectionError::from(err))
}

fn corrupt(message: String) -> ServiceError {
    ServiceError::Internal(message)
}

fn required<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, ServiceError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| corrupt(format!("a Lance batch lacks {name}")))
}

/// Row `row` of a sparse column, `None` when null.
fn sparse_at(column: &StructArray, row: usize) -> Result<Option<SparseVector>, ServiceError> {
    if column.is_null(row) {
        return Ok(None);
    }
    let list = |child: &str| -> Result<ArrayRef, ServiceError> {
        let list = column
            .column_by_name(child)
            .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| corrupt(format!("a sparse column lacks list {child}")))?;
        Ok(list.value(row))
    };
    let indices = list("indices")?;
    let values = list("values")?;
    let indices = indices
        .as_primitive_opt::<UInt32Type>()
        .ok_or_else(|| corrupt("sparse indices are not UInt32".to_string()))?;
    let values = values
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| corrupt("sparse values are not Float32".to_string()))?;
    SparseVector::new(indices.values().to_vec(), values.values().to_vec())
        .map(Some)
        .map_err(|err| corrupt(format!("a stored sparse vector: {err}")))
}

/// Row `row` of a dense vector column, `None` when null.
fn vector_at(column: &ArrayRef, name: &str, row: usize) -> Result<Option<Vec<f32>>, ServiceError> {
    let list = column
        .as_fixed_size_list_opt()
        .ok_or_else(|| corrupt(format!("{name} is not a FixedSizeList")))?;
    if list.is_null(row) {
        return Ok(None);
    }
    let values = list.value(row);
    let values: &Float32Array = values
        .as_primitive_opt::<Float32Type>()
        .ok_or_else(|| corrupt(format!("{name} is not a list of Float32")))?;
    Ok(Some(values.values().to_vec()))
}

/// The Lance columns that serve [`FetchColumns`] on one Lance version:
/// asked-for vectors the version lacks are left out (M1.1 Ruling 5).
pub(crate) struct LanceColumns {
    /// Column names to read, without `_rowid`.
    pub names: Vec<String>,
    source: bool,
    /// (column, name in the schema) of every dense vector read.
    dense: Vec<(String, String)>,
    /// (column, name in the schema) of every sparse vector read.
    sparse: Vec<(String, String)>,
}

impl LanceColumns {
    pub(crate) fn new(
        dataset: &lance::Dataset,
        schema: &operon_collection::CollectionSchema,
        columns: &FetchColumns,
    ) -> Self {
        let present = |column: &str| dataset.schema().field(column).is_some();
        let mut names: Vec<String> = vec![PK_COLUMN.to_string()];
        if columns.source {
            names.push(SOURCE_COLUMN.to_string());
        }
        names.push(INGEST_PARTITION_COLUMN.to_string());
        names.push(INGEST_OFFSET_COLUMN.to_string());
        let dense: Vec<(String, String)> = columns
            .vectors
            .iter()
            .filter_map(|i| Some((vector_column(*i), schema.vectors.get(*i)?.name.clone())))
            .filter(|(column, _)| present(column))
            .collect();
        let sparse: Vec<(String, String)> = columns
            .sparse
            .iter()
            .filter_map(|i| {
                Some((
                    sparse_column(*i),
                    schema.sparse_vectors.get(*i)?.name.clone(),
                ))
            })
            .filter(|(column, _)| present(column))
            .collect();
        names.extend(dense.iter().map(|(column, _)| column.clone()));
        names.extend(sparse.iter().map(|(column, _)| column.clone()));
        Self {
            names,
            source: columns.source,
            dense,
            sparse,
        }
    }

    /// The rows of a Lance batch read with these columns plus `_rowid`, in
    /// batch order.
    pub(crate) fn decode(&self, batch: &RecordBatch) -> Result<Vec<FetchedRow>, ServiceError> {
        let row_ids = required(batch, ROW_ID)?
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| corrupt(format!("{ROW_ID} is not UInt64")))?;
        let pks: &BinaryArray = required(batch, PK_COLUMN)?
            .as_binary_opt::<i32>()
            .ok_or_else(|| corrupt(format!("{PK_COLUMN} is not Binary")))?;
        let sources: Option<&BinaryArray> = if self.source {
            Some(
                required(batch, SOURCE_COLUMN)?
                    .as_binary_opt::<i32>()
                    .ok_or_else(|| corrupt(format!("{SOURCE_COLUMN} is not Binary")))?,
            )
        } else {
            None
        };
        let partitions: &UInt32Array = required(batch, INGEST_PARTITION_COLUMN)?
            .as_primitive_opt::<UInt32Type>()
            .ok_or_else(|| corrupt(format!("{INGEST_PARTITION_COLUMN} is not UInt32")))?;
        let offsets: &UInt64Array = required(batch, INGEST_OFFSET_COLUMN)?
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| corrupt(format!("{INGEST_OFFSET_COLUMN} is not UInt64")))?;
        let dense_columns: Vec<(&ArrayRef, &str, &str)> = self
            .dense
            .iter()
            .map(|(column, name)| Ok((required(batch, column)?, column.as_str(), name.as_str())))
            .collect::<Result<_, ServiceError>>()?;
        let sparse_columns: Vec<(&StructArray, &str)> = self
            .sparse
            .iter()
            .map(|(column, name)| {
                let array = required(batch, column)?
                    .as_struct_opt()
                    .ok_or_else(|| corrupt(format!("{column} is not a Struct")))?;
                Ok((array, name.as_str()))
            })
            .collect::<Result<_, ServiceError>>()?;
        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let row_id = row_ids.value(row);
            let pk = PrimaryKey::from_canonical(pks.value(row))
                .map_err(|err| corrupt(format!("{PK_COLUMN} of row {row_id}: {err}")))?;
            let source = match sources {
                Some(sources) => Some(
                    serde_json::from_slice::<Map<String, Value>>(sources.value(row)).map_err(
                        |err| corrupt(format!("{SOURCE_COLUMN} of row {row_id}: {err}")),
                    )?,
                ),
                None => None,
            };
            let mut vectors = BTreeMap::new();
            for (column, column_name, name) in &dense_columns {
                if let Some(vector) = vector_at(column, column_name, row)? {
                    vectors.insert(name.to_string(), vector);
                }
            }
            let mut sparse_vectors = BTreeMap::new();
            for (column, name) in &sparse_columns {
                if let Some(vector) = sparse_at(column, row)? {
                    sparse_vectors.insert(name.to_string(), vector);
                }
            }
            out.push(FetchedRow {
                row_id,
                pk,
                source,
                vectors,
                sparse_vectors,
                seq_no: offsets.value(row),
                partition: partitions.value(row),
            });
        }
        Ok(out)
    }
}

/// The durable rows `ids` (distinct, ascending) of the view's Lance version.
async fn fetch_durable(
    view: &ReadView,
    ids: &[u64],
    columns: &FetchColumns,
) -> Result<HashMap<u64, FetchedRow>, ServiceError> {
    let version = view.snapshot.manifest().lance_version;
    let missing =
        |id: u64| ServiceError::Internal(format!("row {id} is not in lance version {version}"));
    let Some(dataset) = view.snapshot.dataset() else {
        return Err(missing(ids[0]));
    };
    let lance_columns = LanceColumns::new(dataset, &view.collection.schema, columns);
    let mut names = lance_columns.names.clone();
    names.push(ROW_ID.to_string());
    let projection = dataset
        .schema()
        .project_preserve_system_columns(&names)
        .map_err(lance_error)?;
    let batch = dataset
        .take_rows(ids, ProjectionRequest::from_schema(projection))
        .await
        .map_err(lance_error)?;
    let out: HashMap<u64, FetchedRow> = lance_columns
        .decode(&batch)?
        .into_iter()
        .map(|row| (row.row_id, row))
        .collect();
    if let Some(id) = ids.iter().find(|id| !out.contains_key(id)) {
        return Err(missing(*id));
    }
    Ok(out)
}

/// The tail row `row_id`, projected to `columns`.
fn fetch_tail(
    view: &ReadView,
    row_id: u64,
    columns: &FetchColumns,
) -> Result<FetchedRow, ServiceError> {
    let entry = view
        .tail
        .doc(row_id)
        .ok_or_else(|| ServiceError::Internal(format!("tail row {row_id} is not in the view")))?;
    tail_row(&view.collection.schema, &entry, columns)
}

/// The tail entry `entry`, projected to `columns`.
pub(crate) fn tail_row(
    schema: &operon_collection::CollectionSchema,
    entry: &crate::tail::TailDoc,
    columns: &FetchColumns,
) -> Result<FetchedRow, ServiceError> {
    let row_id = entry.row_id;
    let doc = entry
        .doc
        .as_ref()
        .ok_or_else(|| ServiceError::Internal(format!("tail row {row_id} is a delete")))?;
    let vectors = columns
        .vectors
        .iter()
        .filter_map(|i| {
            let name = &schema.vectors.get(*i)?.name;
            Some((name.clone(), doc.vectors.get(name)?.clone()))
        })
        .collect();
    let sparse_vectors = columns
        .sparse
        .iter()
        .filter_map(|i| {
            let name = &schema.sparse_vectors.get(*i)?.name;
            Some((name.clone(), doc.sparse_vectors.get(name)?.clone()))
        })
        .collect();
    Ok(FetchedRow {
        row_id,
        pk: entry.pk.clone(),
        source: columns.source.then(|| doc.source.clone()),
        vectors,
        sparse_vectors,
        seq_no: entry.offset,
        partition: entry.partition,
    })
}

/// The rows `row_ids` of the view, in input order: durable rows from one
/// Lance take of the asked-for columns (columns an older Lance version
/// lacks read as absent), tail rows from the tail snapshot. A durable row
/// id the version lacks is `Internal`: ids come from the same view.
pub async fn fetch_rows(
    view: &ReadView,
    row_ids: &[u64],
    columns: &FetchColumns,
) -> Result<Vec<FetchedRow>, ServiceError> {
    let mut durable: Vec<u64> = row_ids
        .iter()
        .copied()
        .filter(|id| *id < TAIL_ROWID_BASE)
        .collect();
    durable.sort_unstable();
    durable.dedup();
    let found = if durable.is_empty() {
        HashMap::new()
    } else {
        fetch_durable(view, &durable, columns).await?
    };
    row_ids
        .iter()
        .map(|id| {
            if *id >= TAIL_ROWID_BASE {
                fetch_tail(view, *id, columns)
            } else {
                found
                    .get(id)
                    .cloned()
                    .ok_or_else(|| ServiceError::Internal(format!("row {id} was not fetched")))
            }
        })
        .collect()
}

/// `hits` with their fetched rows as one batch of [`fetched_schema`].
fn fetched_to_batch(hits: &[Ranked], rows: &[FetchedRow]) -> Result<RecordBatch, ServiceError> {
    let encode = |value: Result<Vec<u8>, postcard::Error>| {
        value.map_err(|err| ServiceError::Internal(format!("encoding fetched vectors: {err}")))
    };
    let row_ids = UInt64Array::from_iter_values(hits.iter().map(|h| h.row_id));
    let pks: Vec<Vec<u8>> = hits.iter().map(|h| h.pk.canonical()).collect();
    let scores = Float32Array::from_iter_values(hits.iter().map(|h| h.score));
    let sorts: Vec<Vec<u8>> = hits.iter().map(|h| encode_sort(&h.sort)).collect();
    let sources: Vec<Option<String>> = rows
        .iter()
        .map(|row| {
            row.source
                .as_ref()
                .map(|source| Value::Object(source.clone()).to_string())
        })
        .collect();
    let vectors: Vec<Vec<u8>> = rows
        .iter()
        .map(|row| encode(postcard::to_allocvec(&row.vectors)))
        .collect::<Result<_, _>>()?;
    let sparse: Vec<Vec<u8>> = rows
        .iter()
        .map(|row| encode(postcard::to_allocvec(&row.sparse_vectors)))
        .collect::<Result<_, _>>()?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(row_ids),
        Arc::new(BinaryArray::from_iter_values(pks.iter().map(Vec::as_slice))),
        Arc::new(scores),
        Arc::new(BinaryArray::from_iter_values(
            sorts.iter().map(Vec::as_slice),
        )),
        Arc::new(StringArray::from(sources)),
        Arc::new(BinaryArray::from_iter_values(
            vectors.iter().map(Vec::as_slice),
        )),
        Arc::new(BinaryArray::from_iter_values(
            sparse.iter().map(Vec::as_slice),
        )),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.seq_no))),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|r| r.partition),
        )),
    ];
    RecordBatch::try_new(fetched_schema(), columns)
        .map_err(|err| ServiceError::Internal(format!("fetched batch: {err}")))
}

/// The documents of its input's ranked rows. Output: [`fetched_schema`],
/// in input order.
pub struct DocFetchExec {
    input: Arc<dyn ExecutionPlan>,
    view: Arc<ReadView>,
    columns: FetchColumns,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for DocFetchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DocFetchExec")
            .field("collection", &self.view.collection.name)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl DocFetchExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, view: Arc<ReadView>, columns: FetchColumns) -> Self {
        Self {
            input,
            view,
            columns,
            properties: plan_properties(fetched_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for DocFetchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DocFetchExec: collection={}, source={}, vectors={:?}, sparse={:?}",
            self.view.collection.name,
            self.columns.source,
            self.columns.vectors,
            self.columns.sparse
        )
    }
}

impl ExecutionPlan for DocFetchExec {
    fn name(&self) -> &str {
        "DocFetchExec"
    }

    fn schema(&self) -> SchemaRef {
        fetched_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(DocFetchExec::new(
                input,
                self.view.clone(),
                self.columns.clone(),
            ))),
            _ => Err(DataFusionError::Internal(
                "DocFetchExec has one input".to_string(),
            )),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DocFetchExec has one partition, not {partition}"
            )));
        }
        let input = self.input.clone();
        let view = self.view.clone();
        let columns = self.columns.clone();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let stream = futures::stream::once(async move {
            let hits = collect_ranked(input, context).await.map_err(df_error)?;
            let ids: Vec<u64> = hits.iter().map(|hit| hit.row_id).collect();
            let rows = fetch_rows(&view, &ids, &columns).await.map_err(df_error)?;
            output_rows.add(rows.len());
            fetched_to_batch(&hits, &rows).map_err(df_error)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            fetched_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
