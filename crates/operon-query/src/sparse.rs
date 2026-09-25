//! Sparse vectors (plan M1.2 Task 6 rule 7; Ruling 21, overview A29):
//! Qdrant's exact sparse dot product, its IDF modifier, and the live-only
//! statistics the modifier reads.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use operon_collection::{ROWID_FIELD, SPARSE_PRESENT, SparseVector, sparse_postings_field};
use roaring::RoaringTreemap;
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{DocSet, Searcher, TERMINATED, Term};

use crate::error::ServiceError;
use crate::exec::blocking;
use crate::exec::mask::RowSet;
use crate::read::ReadView;
use crate::text::splits::{OpenSplit, tail_segment_masks};
use crate::text::stats::{GlobalStats, StatsCache, StatsTerm};

/// Qdrant's sparse dot product (Ruling 21): shared indices in ascending order, f32 accumulation; None when nothing is shared.
///
/// A shared index whose weight is zero on either side still counts as
/// shared (`qdrant-edge` `score_vectors`).
pub fn sparse_score(query: &SparseVector, doc: &SparseVector) -> Option<f32> {
    let (qi, qv) = (query.indices(), query.values());
    let (di, dv) = (doc.indices(), doc.values());
    let mut score = 0.0f32;
    let mut overlap = false;
    let (mut i, mut j) = (0, 0);
    while i < qi.len() && j < di.len() {
        match qi[i].cmp(&di[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                overlap = true;
                score += qv[i] * dv[j];
                i += 1;
                j += 1;
            }
        }
    }
    overlap.then_some(score)
}

/// Qdrant's `fancy_idf` in f32: ln((n − df + 0.5) / (df + 0.5) + 1).
pub fn idf(n: u64, df: u64) -> f32 {
    let (n, df) = (n as f32, df as f32);
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

/// The dictionary key of the u64 term `value` (`_sparse.<name>` terms).
pub(crate) fn u64_term_bytes(value: u64) -> Vec<u8> {
    Term::from_field_u64(Field::from_field_id(0), value)
        .serialized_value_bytes()
        .to_vec()
}

/// The IDF statistics of one sparse vector over the view's live docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SparseStats {
    /// Live docs with a non-empty vector.
    pub n: u64,
    /// Per query index, the live docs whose vector has it.
    pub df: BTreeMap<u32, u64>,
}

/// How many docs of `term`'s postings in `searcher` are unmasked (`masks`
/// per segment) and have a `_rowid` in `corpus`.
fn corpus_count(
    searcher: &Searcher,
    masks: &[roaring::RoaringBitmap],
    term: &Term,
    corpus: &RoaringTreemap,
) -> Result<u64, ServiceError> {
    let tantivy = |err: tantivy::TantivyError| ServiceError::Internal(format!("tantivy: {err}"));
    let mut count = 0;
    for (segment, reader) in searcher.segment_readers().iter().enumerate() {
        let index = reader.inverted_index(term.field()).map_err(tantivy)?;
        let Some(mut postings) = index
            .read_postings(term, IndexRecordOption::Basic)
            .map_err(|err| ServiceError::Internal(format!("tantivy: {err}")))?
        else {
            continue;
        };
        let rowids = reader.fast_fields().u64(ROWID_FIELD).map_err(tantivy)?;
        let mut doc = postings.doc();
        while doc != TERMINATED {
            if !masks[segment].contains(doc)
                && rowids.first(doc).is_some_and(|row| corpus.contains(row))
            {
                count += 1;
            }
            doc = postings.advance();
        }
    }
    Ok(count)
}

/// One unit's corpus counts per term (`None` when its schema lacks the
/// field).
fn unit_corpus_counts(
    searcher: &Searcher,
    masks: &[roaring::RoaringBitmap],
    field_name: &str,
    terms: &[u64],
    corpus: &RoaringTreemap,
) -> Result<Vec<u64>, ServiceError> {
    let Ok(field) = searcher.schema().get_field(field_name) else {
        return Ok(vec![0; terms.len()]);
    };
    terms
        .iter()
        .map(|value| {
            corpus_count(
                searcher,
                masks,
                &Term::from_field_u64(field, *value),
                corpus,
            )
        })
        .collect()
}

impl SparseStats {
    /// The statistics of sparse vector `field` for `indices` over `splits`
    /// (every split of the view's manifest) and the tail (rule 7.3):
    /// `n` counts the live postings of `SPARSE_PRESENT`, `df(i)` those of
    /// term *i*. Without a corpus the per-split counts come from `cache`
    /// (shadowed docs subtracted per request); with one, every count is
    /// taken over the corpus rows only, uncached.
    pub async fn compute(
        view: &ReadView,
        splits: &[OpenSplit],
        field: &str,
        indices: &[u32],
        corpus: Option<&RowSet>,
        cache: &StatsCache,
        parallelism: usize,
    ) -> Result<Self, ServiceError> {
        use futures::StreamExt;
        let postings = sparse_postings_field(field);
        let indices: BTreeSet<u32> = indices.iter().copied().collect();
        let mut values: Vec<u64> = vec![SPARSE_PRESENT];
        values.extend(indices.iter().map(|i| u64::from(*i)));
        let counts: Vec<u64> = match corpus {
            None | Some(RowSet::All) => {
                let terms: BTreeSet<StatsTerm> = values
                    .iter()
                    .map(|value| (postings.clone(), u64_term_bytes(*value)))
                    .collect();
                let stats = GlobalStats::compute(
                    view,
                    splits,
                    &terms,
                    &BTreeSet::new(),
                    cache,
                    parallelism,
                )
                .await?;
                values
                    .iter()
                    .map(|value| {
                        stats
                            .doc_freq
                            .get(&(postings.clone(), u64_term_bytes(*value)))
                            .copied()
                            .unwrap_or(0)
                    })
                    .collect()
            }
            Some(RowSet::Rows(rows)) => {
                let rows = Arc::new(rows.clone());
                let values = Arc::new(values.clone());
                let units: Vec<(Searcher, Vec<roaring::RoaringBitmap>)> = splits
                    .iter()
                    .map(|split| (split.searcher.clone(), split.segment_masks()))
                    .collect();
                let mut parts = futures::stream::iter(units)
                    .map(|(searcher, masks)| {
                        let (rows, values, postings) =
                            (rows.clone(), values.clone(), postings.clone());
                        blocking(move || {
                            unit_corpus_counts(&searcher, &masks, &postings, &values, &rows)
                        })
                    })
                    .buffered(parallelism.max(1))
                    .collect::<Vec<_>>()
                    .await;
                if let Some(searcher) = view.tail.searcher() {
                    let searcher = searcher.clone();
                    let live = view.tail.live().clone();
                    let (rows, values, postings) = (rows.clone(), values.clone(), postings.clone());
                    parts.push(
                        blocking(move || {
                            let masks = tail_segment_masks(&searcher, &live)?;
                            unit_corpus_counts(&searcher, &masks, &postings, &values, &rows)
                        })
                        .await,
                    );
                }
                let mut totals = vec![0u64; values.len()];
                for part in parts {
                    let part = part?;
                    for (total, count) in totals.iter_mut().zip(part) {
                        *total += count;
                    }
                }
                totals
            }
        };
        let n = counts[0];
        let df = indices
            .iter()
            .copied()
            .zip(counts[1..].iter().copied())
            .collect();
        Ok(Self { n, df })
    }

    /// Each value × idf(n, df[index]); an index absent from df has df 0.
    ///
    /// Live statistics always have `df <= n`; a larger `df` is taken as `n`,
    /// and a product beyond the f32 range saturates, so the result is a
    /// valid sparse vector whatever the counts.
    pub fn weigh(&self, query: &SparseVector) -> SparseVector {
        let values = query
            .indices()
            .iter()
            .zip(query.values())
            .map(|(index, value)| {
                let df = self.df.get(index).copied().unwrap_or(0).min(self.n);
                let weighted = value * idf(self.n, df);
                if weighted.is_finite() {
                    weighted
                } else {
                    f32::MAX.copysign(weighted)
                }
            })
            .collect();
        SparseVector::new(query.indices().to_vec(), values)
            .expect("the query's indices, with finite weights")
    }
}
