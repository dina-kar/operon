//! `TailMergeExec` (plan M1.2 Task 7 rule 6.3): durable rows in PK order
//! merged with the tail's overlay keys, for scroll, constant-score top-k
//! and SQL scans.
//!
//! The durable input is live and unshadowed already; a durable key that
//! has a tail entry is still dropped (defensively), tail deletes
//! (tombstones) are dropped, and tail docs are kept when their row id is
//! in `tail_rows`.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use operon_collection::PrimaryKey;

use crate::error::ServiceError;
use crate::exec::mask::RowSet;
use crate::exec::schema::{batch_to_keyed, keyed_schema, keyed_to_batch};
use crate::exec::{df_error, from_df, plan_properties};
use crate::read::ReadView;
use crate::tail::TailLookup;

/// A tail overlay key: its doc's row id when it is kept, `None` for a
/// tombstone or a doc outside `tail_rows` (it still shadows its key).
type TailKey = (PrimaryKey, Option<u64>);

/// The PK-ordered merge of a durable input (of [`keyed_schema`], in PK
/// order) with the tail. Output: [`keyed_schema`], in PK order, keys after
/// `after`, at most `limit` rows.
pub struct TailMergeExec {
    durable: Arc<dyn ExecutionPlan>,
    view: Arc<ReadView>,
    tail_rows: RowSet,
    after: Option<PrimaryKey>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for TailMergeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TailMergeExec")
            .field("collection", &self.view.collection.name)
            .field("after", &self.after)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl TailMergeExec {
    pub fn new(
        durable: Arc<dyn ExecutionPlan>,
        view: Arc<ReadView>,
        tail_rows: RowSet,
        after: Option<PrimaryKey>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            durable,
            view,
            tail_rows,
            after,
            limit,
            properties: plan_properties(keyed_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The tail's overlay keys after `after`, in PK order.
    fn tail_keys(&self) -> Vec<TailKey> {
        self.view
            .tail
            .keys_after(self.after.as_ref(), usize::MAX)
            .into_iter()
            .filter_map(|(pk, lookup)| match lookup {
                TailLookup::Present(doc) => {
                    let kept = self.tail_rows.contains(doc.row_id).then_some(doc.row_id);
                    Some((pk, kept))
                }
                TailLookup::Deleted(_) => Some((pk, None)),
                TailLookup::Absent => None,
            })
            .collect()
    }
}

/// Merges `durable` with `tail` until `limit` rows are out.
async fn merge(
    mut durable: SendableRecordBatchStream,
    tail: Vec<TailKey>,
    after: Option<PrimaryKey>,
    limit: usize,
) -> Result<Vec<(u64, PrimaryKey)>, ServiceError> {
    let mut out: Vec<(u64, PrimaryKey)> = Vec::new();
    let mut tail = tail.into_iter().peekable();
    'batches: while out.len() < limit {
        let Some(batch) = durable.next().await else {
            break;
        };
        let batch = batch.map_err(from_df)?;
        for (row_id, pk) in batch_to_keyed(&batch)? {
            if after.as_ref().is_some_and(|after| pk <= *after) {
                continue;
            }
            while let Some((key, kept)) = tail.next_if(|(key, _)| *key < pk) {
                if let Some(row) = kept {
                    out.push((row, key));
                    if out.len() >= limit {
                        break 'batches;
                    }
                }
            }
            if let Some((key, kept)) = tail.next_if(|(key, _)| *key == pk) {
                // Shadowed: the tail's state of the key wins.
                if let Some(row) = kept {
                    out.push((row, key));
                }
            } else {
                out.push((row_id, pk));
            }
            if out.len() >= limit {
                break 'batches;
            }
        }
    }
    if out.len() < limit {
        out.extend(tail.filter_map(|(key, kept)| kept.map(|row| (row, key))));
    }
    out.truncate(limit);
    Ok(out)
}

impl DisplayAs for TailMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TailMergeExec: collection={}, after={:?}, limit={:?}",
            self.view.collection.name, self.after, self.limit
        )
    }
}

impl ExecutionPlan for TailMergeExec {
    fn name(&self) -> &str {
        "TailMergeExec"
    }

    fn schema(&self) -> SchemaRef {
        keyed_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.durable]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(durable), true) => Ok(Arc::new(TailMergeExec::new(
                durable,
                self.view.clone(),
                self.tail_rows.clone(),
                self.after.clone(),
                self.limit,
            ))),
            _ => Err(DataFusionError::Internal(
                "TailMergeExec has one input".to_string(),
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
                "TailMergeExec has one partition, not {partition}"
            )));
        }
        let durable = self.durable.execute(0, context)?;
        let tail = self.tail_keys();
        let after = self.after.clone();
        let limit = self.limit.unwrap_or(usize::MAX);
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let stream = futures::stream::once(async move {
            let rows = merge(durable, tail, after, limit).await.map_err(df_error)?;
            output_rows.add(rows.len());
            Ok(keyed_to_batch(&rows))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            keyed_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
