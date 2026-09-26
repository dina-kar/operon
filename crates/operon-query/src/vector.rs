//! Dense vectors (plan M1.2 Task 6): Operon's exact scoring kernel
//! (Ruling 3), the configuration of `AnnExec` and the strategies it chooses
//! between.
//!
//! Every vector score a read returns is computed by [`score`]: f64
//! accumulation in index order, rounded to f32 once at the end. Larger is
//! better on every metric (overview §6.6), so the distances are negated.

use operon_collection::Distance;

/// Operon's exact score (Ruling 3): f64 accumulation in index order, rounded to f32 at the end.
///
/// - `Dot`: `Σ qᵢ·vᵢ`;
/// - `Cosine`: `dot / (√Σqᵢ² · √Σvᵢ²)`, and `0.0` when either norm is 0;
/// - `Euclid`: `−√Σ(qᵢ−vᵢ)²` (the distance, not its square);
/// - `Manhattan`: `−Σ|qᵢ−vᵢ|`.
///
/// The vectors have the same length (callers validate it); extra entries
/// of the longer one are ignored.
pub fn score(distance: Distance, query: &[f32], vector: &[f32]) -> f32 {
    let pairs = query
        .iter()
        .zip(vector)
        .map(|(q, v)| (f64::from(*q), f64::from(*v)));
    let value = match distance {
        Distance::Dot => pairs.map(|(q, v)| q * v).sum::<f64>(),
        Distance::Cosine => {
            let (mut dot, mut qq, mut vv) = (0.0f64, 0.0f64, 0.0f64);
            for (q, v) in pairs {
                dot += q * v;
                qq += q * q;
                vv += v * v;
            }
            if qq == 0.0 || vv == 0.0 {
                0.0
            } else {
                dot / (qq.sqrt() * vv.sqrt())
            }
        }
        Distance::Euclid => -pairs.map(|(q, v)| (q - v) * (q - v)).sum::<f64>().sqrt(),
        Distance::Manhattan => -pairs.map(|(q, v)| (q - v).abs()).sum::<f64>(),
    };
    value as f32
}

/// How `AnnExec` chooses and runs its strategy.
#[derive(Clone, Debug, PartialEq)]
pub struct AnnConfig {
    /// 1 000: a filter allowing at most this many durable rows (or Qdrant's
    /// full-scan threshold, when larger) is searched by brute force.
    pub brute_force_min_rows: usize,
    /// 0.10: a filter allowing at most this fraction of the durable rows
    /// prefilters the index search.
    pub prefilter_max_selectivity: f64,
    /// 1.5: a post-filtered search asks for `k · 1.5 / selectivity`.
    pub postfilter_oversample: f64,
    /// 1: retries of a post-filtered search, each asking for 4× more.
    pub postfilter_retries: u32,
    /// 20: the fewest IVF partitions probed by default.
    pub min_nprobes: u32,
    /// 16: default nprobes = max(min_nprobes, ceil(num_partitions / 16)).
    pub nprobes_divisor: u32,
    /// 20: Lance's refine factor unless the request sets one. The plan
    /// said 4, but M1.1's default PQ codes 16 dimensions per byte, and on
    /// the Task 6 corpus (random unit vectors, dim 16) refine 4 recalls
    /// only 0.72–0.75 of the exact top 10, refine 10 about 0.90 and refine
    /// 20 about 0.97, whatever nprobes (20 or all 55 partitions).
    pub default_refine_factor: u32,
    /// 2: a HotAnn is asked for k · 2 + 16.
    pub hot_overfetch: usize,
    /// 2: retries of a hot search, each asking for 4× more.
    pub hot_retries: u32,
    /// 8 192 rows per batch of a brute-force scan.
    pub scan_batch_rows: usize,
}

impl Default for AnnConfig {
    fn default() -> Self {
        Self {
            brute_force_min_rows: 1_000,
            prefilter_max_selectivity: 0.10,
            postfilter_oversample: 1.5,
            postfilter_retries: 1,
            min_nprobes: 20,
            nprobes_divisor: 16,
            default_refine_factor: 20,
            hot_overfetch: 2,
            hot_retries: 2,
            scan_batch_rows: 8_192,
        }
    }
}

impl AnnConfig {
    /// The default nprobes for an index of `num_partitions` partitions:
    /// `max(min_nprobes, ceil(num_partitions / nprobes_divisor))`.
    pub fn default_nprobes(&self, num_partitions: usize) -> usize {
        let divisor = self.nprobes_divisor.max(1) as usize;
        (self.min_nprobes as usize).max(num_partitions.div_ceil(divisor))
    }

    /// Qdrant's full-scan rule: `max(brute_force_min_rows,
    /// full_scan_threshold_kb · 1024 / (4 · dim))`.
    pub fn brute_force_threshold(&self, full_scan_threshold_kb: u32, dim: u32) -> usize {
        let per_kb = u64::from(full_scan_threshold_kb) * 1024 / (4 * u64::from(dim.max(1)));
        self.brute_force_min_rows
            .max(usize::try_from(per_kb).unwrap_or(usize::MAX))
    }
}

/// The strategy an `AnnExec` used (rule 4), recorded once it has run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnStrategy {
    /// Brute force over every live row with the vector: `exact`, a metric
    /// override, `Manhattan`, or no index.
    Exact,
    /// Brute force over a filter's rows, which are few.
    BruteForceAllowed,
    /// Lance's index restricted to the filter's rows.
    Prefilter,
    /// Lance's index, then the filter over its oversampled candidates (also
    /// the strategy without a filter).
    Postfilter,
    /// The hot tier's ANN artifact, merged with the uncovered rows.
    Hot,
}
