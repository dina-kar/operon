//! `FusionExec` (plan M1.2 Task 7 rule 2; Rulings 4–5): RRF, DBSF and
//! weighted sums of ranked lists, exactly as overview §6.6 defines them.
//!
//! Every input list is in (score desc, pk asc) order, and a hit's rank is
//! its 1-based position. Documents are identified across lists by row id,
//! and their fused scores are summed in f32, in input order. The fused list
//! is ordered by (fused score desc, pk asc).

use std::collections::BTreeMap;
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

use crate::error::ServiceError;
use crate::exec::schema::{Ranked, batch_to_ranked, ranked_schema, ranked_to_batch};
use crate::exec::{df_error, plan_properties};
use crate::ir::Fusion;

/// Qdrant's `distr_norm` (Ruling 4): f32 Welford mean and sample variance,
/// then `(s − (μ − 3σ)) / ((μ + 3σ) − (μ − 3σ))`, not clamped. A list of one
/// hit, or one whose bounds coincide, scores 0.5.
fn distr_norm(scores: &[f32]) -> Vec<f32> {
    if scores.len() < 2 {
        return vec![0.5; scores.len()];
    }
    let mut mean = 0.0f32;
    let mut aggregate = 0.0f32;
    for (score, k) in scores.iter().zip(1usize..) {
        let old_delta = score - mean;
        mean += old_delta / (k as f32);
        let delta = score - mean;
        aggregate += old_delta * delta;
    }
    let variance = aggregate / (scores.len() as f32 - 1.0);
    let std_dev = variance.sqrt();
    let min = mean - 3.0 * std_dev;
    let max = mean + 3.0 * std_dev;
    if min == max {
        return vec![0.5; scores.len()];
    }
    scores.iter().map(|s| (s - min) / (max - min)).collect()
}

/// The fused list of `lists` (rule 2), ordered by (fused score desc, pk
/// asc); not truncated.
pub fn fuse(lists: &[Vec<Ranked>], fusion: &Fusion) -> Vec<Ranked> {
    // Row id → (the hit, its fused score); summed in input order.
    let mut fused: BTreeMap<u64, Ranked> = BTreeMap::new();
    let mut add = |hit: &Ranked, contribution: f32| {
        fused
            .entry(hit.row_id)
            .and_modify(|entry| entry.score += contribution)
            .or_insert_with(|| Ranked {
                score: contribution,
                ..hit.clone()
            });
    };
    for (i, list) in lists.iter().enumerate() {
        match fusion {
            Fusion::Rrf { k } => {
                for (hit, rank) in list.iter().zip(1usize..) {
                    add(hit, 1.0f32 / (*k as f32 + rank as f32));
                }
            }
            Fusion::Dbsf => {
                let scores: Vec<f32> = list.iter().map(|hit| hit.score).collect();
                for (hit, normalized) in list.iter().zip(distr_norm(&scores)) {
                    add(hit, normalized);
                }
            }
            Fusion::WeightedSum { weights } => {
                let weight = weights.get(i).copied().unwrap_or(1.0);
                for hit in list {
                    add(hit, weight * hit.score);
                }
            }
        }
    }
    let mut out: Vec<Ranked> = fused.into_values().collect();
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.pk.cmp(&b.pk))
            .then_with(|| a.row_id.cmp(&b.row_id))
    });
    out
}

/// Runs `plan` and reads its ranked rows.
pub(crate) async fn collect_ranked(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<Ranked>, ServiceError> {
    let batches = datafusion::physical_plan::collect(plan, context)
        .await
        .map_err(crate::exec::from_df)?;
    let mut out = Vec::new();
    for batch in &batches {
        out.extend(batch_to_ranked(batch)?);
    }
    Ok(out)
}

/// The fusion of its inputs' ranked lists (each a plan of
/// [`ranked_schema`]). Output: [`ranked_schema`], (fused score desc, pk
/// asc), at most `k` rows.
pub struct FusionExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    fusion: Fusion,
    k: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for FusionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusionExec")
            .field("inputs", &self.inputs.len())
            .field("fusion", &self.fusion)
            .field("k", &self.k)
            .finish_non_exhaustive()
    }
}

impl FusionExec {
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>, fusion: Fusion, k: usize) -> Self {
        Self {
            inputs,
            fusion,
            k,
            properties: plan_properties(ranked_schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Runs every input (concurrently) and fuses their lists.
    async fn run(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        fusion: Fusion,
        k: usize,
        context: Arc<TaskContext>,
    ) -> Result<Vec<Ranked>, ServiceError> {
        let lists = futures::future::try_join_all(
            inputs
                .into_iter()
                .map(|input| collect_ranked(input, context.clone())),
        )
        .await?;
        let mut fused = fuse(&lists, &fusion);
        fused.truncate(k);
        Ok(fused)
    }
}

impl DisplayAs for FusionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FusionExec: fusion={:?}, k={}", self.fusion, self.k)
    }
}

impl ExecutionPlan for FusionExec {
    fn name(&self) -> &str {
        "FusionExec"
    }

    fn schema(&self) -> SchemaRef {
        ranked_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.inputs.len() {
            return Err(DataFusionError::Internal(format!(
                "FusionExec has {} inputs, not {}",
                self.inputs.len(),
                children.len()
            )));
        }
        Ok(Arc::new(FusionExec::new(
            children,
            self.fusion.clone(),
            self.k,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FusionExec has one partition, not {partition}"
            )));
        }
        let inputs = self.inputs.clone();
        let fusion = self.fusion.clone();
        let k = self.k;
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let stream = futures::stream::once(async move {
            let hits = FusionExec::run(inputs, fusion, k, context)
                .await
                .map_err(df_error)?;
            output_rows.add(hits.len());
            Ok(ranked_to_batch(&hits))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ranked_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
