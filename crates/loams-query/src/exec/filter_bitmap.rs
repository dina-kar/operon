//! `FilterBitmapExec` (plan M1.2 Task 5 rule 7): a filter as the set of the
//! view's row ids that match it, durable and tail alike.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use roaring::RoaringTreemap;
use tantivy::query::EnableScoring;

use crate::error::ServiceError;
use crate::exec::mask::RowSet;
use crate::exec::schema::rowid_schema;
use crate::exec::{Columns, Unit, blocking, df_error, open_units, plan_properties, tantivy_error};
use crate::ir::Query;
use crate::read::ReadView;
use crate::text::compile::{CompileMode, QueryCompiler};
use crate::text::query_tokenizers;

/// Row ids per output batch.
const BATCH_ROWS: usize = 8_192;

struct Inner {
    view: Arc<ReadView>,
    filter: Query,
    parallelism: usize,
}

/// The rows of the view that match a filter. Output: [`rowid_schema`],
/// ascending; `RowSet::All` expands to every row of the view.
pub struct FilterBitmapExec {
    inner: Arc<Inner>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for FilterBitmapExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterBitmapExec")
            .field("collection", &self.inner.view.collection.name)
            .field("filter", &self.inner.filter)
            .finish_non_exhaustive()
    }
}

/// A filter with no clause: every row.
fn matches_all(filter: &Query) -> bool {
    match filter {
        Query::MatchAll => true,
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => must.is_empty() && should.is_empty() && must_not.is_empty() && filter.is_empty(),
        _ => false,
    }
}

impl FilterBitmapExec {
    pub fn new(view: Arc<ReadView>, filter: Query, parallelism: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                view,
                filter,
                parallelism: parallelism.max(1),
            }),
            properties: plan_properties(rowid_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Durable row ids and tail row ids that match; `All` for a filter
    /// without clauses.
    pub async fn rows(&self) -> Result<RowSet, ServiceError> {
        if matches_all(&self.inner.filter) {
            return Ok(RowSet::All);
        }
        self.inner.evaluate(&self.metrics).await.map(RowSet::Rows)
    }
}

impl Inner {
    /// Every matching row id (a filter without clauses gives every row).
    async fn evaluate(
        &self,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<RoaringTreemap, ServiceError> {
        let timer = MetricBuilder::new(metrics).elapsed_compute(0);
        let started = std::time::Instant::now();
        let tokenizers = query_tokenizers();
        let schema = &self.view.collection.schema;
        let compile = |split: &tantivy::schema::Schema| {
            QueryCompiler::new(schema, split, &tokenizers)
                .compile(&self.filter, CompileMode::Filter)
        };
        let (splits, units) = open_units(&self.view, &compile, &[], self.parallelism).await?;
        MetricBuilder::new(metrics)
            .counter("splits_searched", 0)
            .add(splits.len());
        drop(splits);
        let parts = futures::stream::iter(units)
            .map(|unit| blocking(move || unit_rows(unit)))
            .buffered(self.parallelism)
            .collect::<Vec<_>>()
            .await;
        let mut rows = RoaringTreemap::new();
        for part in parts {
            rows |= part?;
        }
        timer.add_elapsed(started);
        Ok(rows)
    }
}

/// The row ids of one index's unmasked matches.
fn unit_rows(unit: Unit) -> Result<RoaringTreemap, ServiceError> {
    let weight = unit
        .query
        .weight(EnableScoring::disabled_from_searcher(&unit.searcher))
        .map_err(tantivy_error)?;
    let mut rows = RoaringTreemap::new();
    for (segment, reader) in unit.searcher.segment_readers().iter().enumerate() {
        let masked = &unit.masks[segment];
        let columns = Columns::open(reader, &[])?;
        let mut failed = None;
        weight
            .for_each_no_score(reader, &mut |docs| {
                for doc in docs {
                    if masked.contains(*doc) {
                        continue;
                    }
                    match columns.row_id(*doc) {
                        Ok(row) => {
                            rows.insert(row);
                        }
                        Err(err) => failed = Some(err),
                    }
                }
            })
            .map_err(tantivy_error)?;
        if let Some(err) = failed {
            return Err(err);
        }
    }
    Ok(rows)
}

impl DisplayAs for FilterBitmapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FilterBitmapExec: collection={}, filter={:?}",
            self.inner.view.collection.name, self.inner.filter
        )
    }
}

impl ExecutionPlan for FilterBitmapExec {
    fn name(&self) -> &str {
        "FilterBitmapExec"
    }

    fn schema(&self) -> SchemaRef {
        rowid_schema()
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
                "FilterBitmapExec has no children".to_string(),
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
                "FilterBitmapExec has one partition, not {partition}"
            )));
        }
        let inner = self.inner.clone();
        let metrics = self.metrics.clone();
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let stream = futures::stream::once(async move {
            let rows = inner.evaluate(&metrics).await.map_err(df_error)?;
            output_rows.add(rows.len() as usize);
            let rows: Vec<u64> = rows.iter().collect();
            let batches: Vec<DfResult<RecordBatch>> = rows
                .chunks(BATCH_ROWS)
                .map(|chunk| {
                    RecordBatch::try_new(
                        rowid_schema(),
                        vec![Arc::new(UInt64Array::from(chunk.to_vec()))],
                    )
                    .map_err(DataFusionError::from)
                })
                .collect();
            Ok::<_, DataFusionError>(futures::stream::iter(batches))
        })
        .map(|result| match result {
            Ok(batches) => batches.left_stream(),
            Err(err) => futures::stream::once(async move { Err(err) }).right_stream(),
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            rowid_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
