//! Keys in PK order (plan M1.2 Task 7 rules 6 and 11; Ruling 15): the
//! durable side of scroll and of constant-score top-k, merged with the
//! tail by `TailMergeExec`.
//!
//! The durable side is either a k-way merge of every split's
//! [`PkCursor`](crate::text::PkCursor) (no filter, or a filter with more
//! than `scroll_sort_threshold` durable matches), each key checked against
//! the filter's rows, or the filter's durable matches read from the `_pk`
//! fast fields and sorted.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use operon_collection::{PK_FIELD, PrimaryKey};
use operon_quickwit::doc_mapper::{FastFieldWarmupInfo, WarmupInfo};
use roaring::RoaringTreemap;
use tantivy::ReloadPolicy;

use crate::error::ServiceError;
use crate::exec::mask::RowSet;
use crate::exec::schema::{batch_to_keyed, keyed_schema, keyed_to_batch};
use crate::exec::tail_merge::TailMergeExec;
use crate::exec::{blocking, df_error, from_df, plan_properties, tantivy_error};
use crate::read::{ReadView, collection_error};
use crate::tail::TAIL_ROWID_BASE;
use crate::text::pkdict::{PkCursor, PkDictCache};

/// Most keys per durable batch.
const MAX_BATCH_ROWS: usize = 1_024;

/// Where the durable keys come from.
#[derive(Clone)]
enum Source {
    /// The k-way merge of every split's cursor, keeping the rows in the
    /// filter (every row without one).
    Walk {
        pk_dict: PkDictCache,
        filter: Option<Arc<RoaringTreemap>>,
        after: Option<Vec<u8>>,
    },
    /// Keys already sorted.
    Sorted(Arc<Vec<(u64, PrimaryKey)>>),
}

/// The durable keys of the view in PK order, produced lazily. Output:
/// [`keyed_schema`].
struct DurableKeysExec {
    view: Arc<ReadView>,
    source: Source,
    batch_rows: usize,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for DurableKeysExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match &self.source {
            Source::Walk { filter, .. } => format!("walk(filtered: {})", filter.is_some()),
            Source::Sorted(keys) => format!("sorted({})", keys.len()),
        };
        f.debug_struct("DurableKeysExec")
            .field("collection", &self.view.collection.name)
            .field("source", &source)
            .finish()
    }
}

/// The k-way merge of the splits' cursors, in manifest order on equal keys.
struct Merge {
    cursors: Vec<PkCursor>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize, u64)>>,
}

impl Merge {
    fn new(mut cursors: Vec<PkCursor>) -> Result<Self, ServiceError> {
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (i, cursor) in cursors.iter_mut().enumerate() {
            if let Some((key, row)) = cursor.next()? {
                heap.push(Reverse((key, i, row)));
            }
        }
        Ok(Self { cursors, heap })
    }

    fn next(&mut self) -> Result<Option<(Vec<u8>, u64)>, ServiceError> {
        let Some(Reverse((key, i, row))) = self.heap.pop() else {
            return Ok(None);
        };
        if let Some((next, next_row)) = self.cursors[i].next()? {
            self.heap.push(Reverse((next, i, next_row)));
        }
        Ok(Some((key, row)))
    }
}

enum WalkState {
    Start,
    Open(Merge),
    Done,
}

impl DurableKeysExec {
    fn new(view: Arc<ReadView>, source: Source, batch_rows: usize) -> Self {
        Self {
            view,
            source,
            batch_rows: batch_rows.clamp(1, MAX_BATCH_ROWS),
            properties: plan_properties(keyed_schema()),
        }
    }
}

async fn open_merge(
    view: &ReadView,
    pk_dict: &PkDictCache,
    after: Option<&[u8]>,
) -> Result<Merge, ServiceError> {
    let mut cursors = Vec::with_capacity(view.snapshot.splits().len());
    for split in 0..view.snapshot.splits().len() {
        cursors.push(pk_dict.cursor(view, split, after).await?);
    }
    Merge::new(cursors)
}

impl DisplayAs for DurableKeysExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ExecutionPlan for DurableKeysExec {
    fn name(&self) -> &str {
        "DurableKeysExec"
    }

    fn schema(&self) -> SchemaRef {
        keyed_schema()
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
                "DurableKeysExec has no children".to_string(),
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
                "DurableKeysExec has one partition, not {partition}"
            )));
        }
        let batch_rows = self.batch_rows;
        let stream = match self.source.clone() {
            Source::Sorted(keys) => {
                let batches: Vec<DfResult<_>> = keys
                    .chunks(batch_rows)
                    .map(|chunk| Ok(keyed_to_batch(chunk)))
                    .collect();
                futures::stream::iter(batches).boxed()
            }
            Source::Walk {
                pk_dict,
                filter,
                after,
            } => {
                let view = self.view.clone();
                futures::stream::try_unfold(WalkState::Start, move |state| {
                    let (view, pk_dict, filter, after) =
                        (view.clone(), pk_dict.clone(), filter.clone(), after.clone());
                    async move {
                        let mut merge = match state {
                            WalkState::Start => open_merge(&view, &pk_dict, after.as_deref())
                                .await
                                .map_err(df_error)?,
                            WalkState::Open(merge) => merge,
                            WalkState::Done => return Ok(None),
                        };
                        let mut rows = Vec::with_capacity(batch_rows);
                        let mut exhausted = false;
                        while rows.len() < batch_rows {
                            match merge.next().map_err(df_error)? {
                                Some((key, row)) => {
                                    if filter.as_ref().is_none_or(|rows| rows.contains(row)) {
                                        let pk =
                                            PrimaryKey::from_canonical(&key).map_err(|err| {
                                                df_error(ServiceError::Internal(format!(
                                                    "_pk term: {err}"
                                                )))
                                            })?;
                                        rows.push((row, pk));
                                    }
                                }
                                None => {
                                    exhausted = true;
                                    break;
                                }
                            }
                        }
                        if rows.is_empty() {
                            return Ok(None);
                        }
                        let next = if exhausted {
                            WalkState::Done
                        } else {
                            WalkState::Open(merge)
                        };
                        Ok(Some((keyed_to_batch(&rows), next)))
                    }
                })
                .boxed()
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            keyed_schema(),
            stream,
        )))
    }
}

/// The keys of durable rows `rows` from the `_pk` fast fields of their
/// splits, sorted, after `after`.
async fn sorted_keys(
    view: &ReadView,
    rows: &RoaringTreemap,
    after: Option<&PrimaryKey>,
    parallelism: usize,
) -> Result<Vec<(u64, PrimaryKey)>, ServiceError> {
    let mut by_split: BTreeMap<usize, Vec<(u32, u64)>> = BTreeMap::new();
    for row in rows {
        let (split, doc) = view.snapshot.locate_row(row).ok_or_else(|| {
            ServiceError::Internal(format!("row {row} is in no split of the view"))
        })?;
        by_split.entry(split).or_default().push((doc, row));
    }
    let jobs: Vec<_> = by_split
        .into_iter()
        .map(|(split, docs)| (view.snapshot.splits()[split].clone(), docs))
        .collect();
    let parts: Vec<Vec<(u64, PrimaryKey)>> = futures::stream::iter(jobs)
        .map(|(split, docs)| async move {
            let index = view
                .snapshot
                .open_split(&split)
                .await
                .map_err(collection_error)?;
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .map_err(tantivy_error)?;
            let searcher = reader.searcher();
            let mut warmup = WarmupInfo::default();
            warmup.fast_fields.insert(FastFieldWarmupInfo {
                name: PK_FIELD.to_string(),
                with_subfields: false,
            });
            operon_quickwit::search::warmup(&searcher, &warmup)
                .await
                .map_err(|err| {
                    ServiceError::Unavailable(format!("warming split {}: {err:#}", split.ulid))
                })?;
            blocking(move || {
                let mut out = Vec::with_capacity(docs.len());
                let mut base = 0u32;
                let readers = searcher.segment_readers();
                let mut columns = Vec::with_capacity(readers.len());
                for reader in readers {
                    let column = reader
                        .fast_fields()
                        .bytes(PK_FIELD)
                        .map_err(tantivy_error)?
                        .ok_or_else(|| ServiceError::Internal("a split without _pk".to_string()))?;
                    columns.push((base, base + reader.max_doc(), column));
                    base += reader.max_doc();
                }
                let mut bytes = Vec::new();
                for (doc, row) in docs {
                    let (start, _, column) = columns
                        .iter()
                        .find(|(start, end, _)| (*start..*end).contains(&doc))
                        .ok_or_else(|| {
                            ServiceError::Internal(format!("doc {doc} is past the split"))
                        })?;
                    let ord = column
                        .term_ords(doc - start)
                        .next()
                        .ok_or_else(|| ServiceError::Internal(format!("doc {doc} has no _pk")))?;
                    bytes.clear();
                    column
                        .ord_to_bytes(ord, &mut bytes)
                        .map_err(tantivy_error)?;
                    let pk = PrimaryKey::from_canonical(&bytes).map_err(|err| {
                        ServiceError::Internal(format!("_pk of doc {doc}: {err}"))
                    })?;
                    out.push((row, pk));
                }
                Ok(out)
            })
            .await
        })
        .buffered(parallelism.max(1))
        .try_collect()
        .await?;
    let mut keys: Vec<(u64, PrimaryKey)> = parts
        .into_iter()
        .flatten()
        .filter(|(_, pk)| after.is_none_or(|after| pk > after))
        .collect();
    keys.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
    Ok(keys)
}

/// How key pages are read.
#[derive(Clone, Debug)]
pub(crate) struct KeyPages {
    pub pk_dict: PkDictCache,
    /// 100 000: a filter with at most this many durable matches sorts them.
    pub sort_threshold: u64,
    pub parallelism: usize,
}

impl KeyPages {
    /// The first `limit` keys of the view after `after` (exclusive) in PK
    /// order, of the rows in `rows`: live docs only, tail and durable
    /// alike (rule 6).
    pub async fn page(
        &self,
        view: &Arc<ReadView>,
        rows: RowSet,
        after: Option<&PrimaryKey>,
        limit: usize,
    ) -> Result<Vec<(u64, PrimaryKey)>, ServiceError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (source, tail_rows) = match rows {
            RowSet::All => (
                Source::Walk {
                    pk_dict: self.pk_dict.clone(),
                    filter: None,
                    after: after.map(PrimaryKey::canonical),
                },
                RowSet::All,
            ),
            RowSet::Rows(rows) => {
                let mut durable = rows.clone();
                durable.remove_range(TAIL_ROWID_BASE..);
                let mut tail = rows;
                tail.remove_range(..TAIL_ROWID_BASE);
                let source = if durable.len() > self.sort_threshold {
                    Source::Walk {
                        pk_dict: self.pk_dict.clone(),
                        filter: Some(Arc::new(durable)),
                        after: after.map(PrimaryKey::canonical),
                    }
                } else {
                    Source::Sorted(Arc::new(
                        sorted_keys(view, &durable, after, self.parallelism).await?,
                    ))
                };
                (source, RowSet::Rows(tail))
            }
        };
        let durable: Arc<dyn ExecutionPlan> =
            Arc::new(DurableKeysExec::new(view.clone(), source, limit));
        let merge: Arc<dyn ExecutionPlan> = Arc::new(TailMergeExec::new(
            durable,
            view.clone(),
            tail_rows,
            after.cloned(),
            Some(limit),
        ));
        let context = datafusion::prelude::SessionContext::new().task_ctx();
        let batches = datafusion::physical_plan::collect(merge, context)
            .await
            .map_err(from_df)?;
        let mut out = Vec::new();
        for batch in &batches {
            out.extend(batch_to_keyed(batch)?);
        }
        Ok(out)
    }
}
