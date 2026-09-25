//! Global, live-only BM25 statistics (plan M1.2 Task 5 rule 3; R6,
//! Ruling 2): over every split of the view's manifest plus the tail,
//! counting only live, unshadowed docs, so a score never depends on where a
//! document's history lives.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use roaring::{RoaringBitmap, RoaringTreemap};
use tantivy::query::Bm25StatisticsProvider;
use tantivy::schema::{Field, IndexRecordOption, Schema};
use tantivy::{DocSet, Searcher, TERMINATED, Term};
use ulid::Ulid;

use crate::error::ServiceError;
use crate::read::ReadView;
use crate::text::norms::norm_mid2;
use crate::text::splits::{OpenSplit, tail_segment_masks};

/// A term of the statistics: its field's name and its value bytes
/// (`Term::serialized_value_bytes`, the term dictionary's key).
pub type StatsTerm = (String, Vec<u8>);

/// The statistics of one request.
#[derive(Clone, Debug, Default)]
pub struct GlobalStats {
    pub num_docs: u64,
    pub tokens: BTreeMap<String, u64>,
    pub doc_freq: BTreeMap<StatsTerm, u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum StatsKey {
    /// Twice the live token sum of a field in a split.
    Tokens {
        split: Ulid,
        bitmap: String,
        field: String,
    },
    DocFreq {
        split: Ulid,
        bitmap: String,
        field: String,
        term: Vec<u8>,
    },
}

/// Per-split statistics over the docs its delete bitmap leaves: (split
/// ulid, delete-bitmap path, field) → live token sum; (…, field, term) →
/// live doc frequency. Shadowed docs are subtracted per request.
#[derive(Clone)]
pub struct StatsCache {
    cache: moka::sync::Cache<StatsKey, u64>,
}

impl fmt::Debug for StatsCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsCache")
            .field("entries", &self.cache.entry_count())
            .finish()
    }
}

impl StatsCache {
    /// At most `entries` (1 000 000) values.
    pub fn new(entries: u64) -> Self {
        Self {
            cache: moka::sync::Cache::new(entries),
        }
    }

    fn get_or(
        &self,
        key: StatsKey,
        compute: impl FnOnce() -> Result<u64, ServiceError>,
    ) -> Result<u64, ServiceError> {
        self.cache
            .try_get_with(key, compute)
            .map_err(|err| (*err).clone())
    }
}

fn tantivy_error(err: impl fmt::Display) -> ServiceError {
    ServiceError::Internal(format!("tantivy: {err}"))
}

/// Records what a weight asks of its statistics (the fields and terms that
/// score BM25), answering placeholders.
#[derive(Debug)]
pub(crate) struct StatsRecorder<'a> {
    schema: &'a Schema,
    pub fields: RefCell<BTreeSet<String>>,
    pub terms: RefCell<BTreeSet<StatsTerm>>,
}

impl<'a> StatsRecorder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            fields: RefCell::default(),
            terms: RefCell::default(),
        }
    }
}

impl Bm25StatisticsProvider for StatsRecorder<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        let name = self.schema.get_field_name(field).to_string();
        self.fields.borrow_mut().insert(name);
        Ok(1)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(1)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let name = self.schema.get_field_name(term.field()).to_string();
        self.terms
            .borrow_mut()
            .insert((name, term.serialized_value_bytes().to_vec()));
        Ok(1)
    }
}

/// [`GlobalStats`] answering for one split's schema.
struct Provider<'a> {
    stats: &'a GlobalStats,
    schema: &'a Schema,
}

impl Bm25StatisticsProvider for Provider<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        let name = self.schema.get_field_name(field);
        Ok(self.stats.tokens.get(name).copied().unwrap_or(0))
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.stats.num_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let key = (
            self.schema.get_field_name(term.field()).to_string(),
            term.serialized_value_bytes().to_vec(),
        );
        Ok(self.stats.doc_freq.get(&key).copied().unwrap_or(0))
    }
}

/// Partial sums of one split or the tail: twice the token sums, and doc
/// frequencies.
#[derive(Default)]
struct Partial {
    tokens2: BTreeMap<String, u64>,
    doc_freq: BTreeMap<StatsTerm, u64>,
}

/// A term of `field` with dictionary key `bytes` (lookups read only the
/// value bytes, so the type tag does not matter).
fn term_of(field: Field, bytes: &[u8]) -> Term {
    Term::from_field_bytes(field, bytes)
}

/// Global doc ids of the searcher's segments start at these bases.
fn segment_bases(searcher: &Searcher) -> Vec<u32> {
    let mut base = 0;
    searcher
        .segment_readers()
        .iter()
        .map(|reader| {
            let at = base;
            base += reader.max_doc();
            at
        })
        .collect()
}

/// Twice the token sum of `field` over the docs of `searcher` for which
/// `keep(segment, local doc)` holds.
fn token_sum2(
    searcher: &Searcher,
    field: Field,
    keep: &dyn Fn(usize, u32) -> bool,
) -> Result<u64, ServiceError> {
    let mut sum = 0u64;
    for (segment, reader) in searcher.segment_readers().iter().enumerate() {
        let Some(norms) = reader
            .fieldnorms_readers()
            .get_field(field)
            .map_err(tantivy_error)?
        else {
            continue;
        };
        for doc in 0..reader.max_doc() {
            if keep(segment, doc) {
                sum += norm_mid2(norms.fieldnorm_id(doc));
            }
        }
    }
    Ok(sum)
}

/// The docs of `term`'s postings for which `keep(segment, local doc)` holds.
fn postings_count(
    searcher: &Searcher,
    term: &Term,
    keep: &dyn Fn(usize, u32) -> bool,
) -> Result<u64, ServiceError> {
    let mut count = 0u64;
    for (segment, reader) in searcher.segment_readers().iter().enumerate() {
        let index = reader.inverted_index(term.field()).map_err(tantivy_error)?;
        let Some(mut postings) = index
            .read_postings(term, IndexRecordOption::Basic)
            .map_err(tantivy_error)?
        else {
            continue;
        };
        let mut doc = postings.doc();
        while doc != TERMINATED {
            if keep(segment, doc) {
                count += 1;
            }
            doc = postings.advance();
        }
    }
    Ok(count)
}

/// How many of the global doc ids `docs` `term`'s postings hold (a seek
/// per doc).
fn postings_hold(
    searcher: &Searcher,
    bases: &[u32],
    term: &Term,
    docs: &RoaringBitmap,
) -> Result<u64, ServiceError> {
    let mut count = 0;
    for (segment, reader) in searcher.segment_readers().iter().enumerate() {
        let base = bases[segment];
        let end = base + reader.max_doc();
        let index = reader.inverted_index(term.field()).map_err(tantivy_error)?;
        let Some(mut postings) = index
            .read_postings(term, IndexRecordOption::Basic)
            .map_err(tantivy_error)?
        else {
            continue;
        };
        for doc in docs.range(base..end) {
            let local = doc - base;
            let current = postings.doc();
            if current == TERMINATED {
                break;
            }
            if current > local {
                continue;
            }
            if postings.seek(local) == local {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// One split's live sums: cached over the docs its delete bitmap leaves,
/// minus its shadowed docs.
/// One split as the statistics see it.
struct SplitSource {
    searcher: Searcher,
    ulid: Ulid,
    /// The delete-bitmap path, or "".
    bitmap: String,
    deleted: RoaringBitmap,
    shadowed: RoaringBitmap,
}

fn split_partial(
    split: &SplitSource,
    fields: &BTreeSet<String>,
    terms: &BTreeSet<StatsTerm>,
    cache: &StatsCache,
) -> Result<Partial, ServiceError> {
    let SplitSource {
        searcher,
        ulid,
        bitmap,
        deleted,
        shadowed,
    } = split;
    let (ulid, bitmap) = (*ulid, bitmap.as_str());
    let schema = searcher.schema();
    let bases = segment_bases(searcher);
    let shadowed = shadowed - deleted;
    let not_deleted = |segment: usize, doc: u32| !deleted.contains(bases[segment] + doc);
    let is_shadowed = |segment: usize, doc: u32| shadowed.contains(bases[segment] + doc);
    let mut partial = Partial::default();
    for name in fields {
        let Ok(field) = schema.get_field(name) else {
            continue;
        };
        let key = StatsKey::Tokens {
            split: ulid,
            bitmap: bitmap.to_string(),
            field: name.clone(),
        };
        let base = cache.get_or(key, || token_sum2(searcher, field, &not_deleted))?;
        let minus = if shadowed.is_empty() {
            0
        } else {
            token_sum2(searcher, field, &is_shadowed)?
        };
        partial
            .tokens2
            .insert(name.clone(), base.saturating_sub(minus));
    }
    for (name, bytes) in terms {
        let Ok(field) = schema.get_field(name) else {
            continue;
        };
        let term = term_of(field, bytes);
        let key = StatsKey::DocFreq {
            split: ulid,
            bitmap: bitmap.to_string(),
            field: name.clone(),
            term: bytes.clone(),
        };
        let base = cache.get_or(key, || postings_count(searcher, &term, &not_deleted))?;
        let minus = if shadowed.is_empty() {
            0
        } else {
            postings_hold(searcher, &bases, &term, &shadowed)?
        };
        partial
            .doc_freq
            .insert((name.clone(), bytes.clone()), base.saturating_sub(minus));
    }
    Ok(partial)
}

/// The tail's sums over its live docs.
fn tail_partial(
    searcher: &Searcher,
    live: &RoaringTreemap,
    fields: &BTreeSet<String>,
    terms: &BTreeSet<StatsTerm>,
) -> Result<Partial, ServiceError> {
    let schema = searcher.schema();
    let masks = tail_segment_masks(searcher, live)?;
    let keep = |segment: usize, doc: u32| !masks[segment].contains(doc);
    let mut partial = Partial::default();
    for name in fields {
        if let Ok(field) = schema.get_field(name) {
            partial
                .tokens2
                .insert(name.clone(), token_sum2(searcher, field, &keep)?);
        }
    }
    for (name, bytes) in terms {
        if let Ok(field) = schema.get_field(name) {
            let count = postings_count(searcher, &term_of(field, bytes), &keep)?;
            partial
                .doc_freq
                .insert((name.clone(), bytes.clone()), count);
        }
    }
    Ok(partial)
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ServiceError> + Send + 'static,
) -> Result<T, ServiceError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|err| ServiceError::Internal(format!("statistics task: {err}")))?
}

impl GlobalStats {
    /// The statistics of `fields` and `terms` over `splits` (every split of
    /// the view's manifest) and the view's tail (rule 3):
    /// - `num_docs = view.live_rows()`;
    /// - `tokens(field)`: ½ · Σ over live docs of `norm_mid2(fieldnorm id)`,
    ///   summed in halves and divided once;
    /// - `doc_freq(field, term)`: the term's postings over live docs.
    pub async fn compute(
        view: &ReadView,
        splits: &[OpenSplit],
        terms: &BTreeSet<StatsTerm>,
        fields: &BTreeSet<String>,
        cache: &StatsCache,
    ) -> Result<Self, ServiceError> {
        let fields = Arc::new(fields.clone());
        let terms = Arc::new(terms.clone());
        let mut jobs = Vec::with_capacity(splits.len() + 1);
        for split in splits {
            let source = SplitSource {
                searcher: split.searcher.clone(),
                ulid: split.split.ulid,
                bitmap: split.split.delete_bitmap.clone().unwrap_or_default(),
                deleted: split.mask.deleted.clone(),
                shadowed: split.mask.shadowed.clone(),
            };
            let (fields, terms, cache) = (fields.clone(), terms.clone(), cache.clone());
            jobs.push(tokio::task::spawn_blocking(move || {
                split_partial(&source, &fields, &terms, &cache)
            }));
        }
        let mut stats = GlobalStats {
            num_docs: view.live_rows(),
            ..GlobalStats::default()
        };
        let mut tokens2: BTreeMap<String, u64> = fields.iter().map(|f| (f.clone(), 0)).collect();
        let mut add = |partial: Partial| {
            for (field, sum) in partial.tokens2 {
                *tokens2.entry(field).or_default() += sum;
            }
            for (term, count) in partial.doc_freq {
                *stats.doc_freq.entry(term).or_default() += count;
            }
        };
        for job in jobs {
            let partial = job
                .await
                .map_err(|err| ServiceError::Internal(format!("statistics task: {err}")))??;
            add(partial);
        }
        if let Some(searcher) = view.tail.searcher() {
            let searcher = searcher.clone();
            let live = view.tail.live().clone();
            let (fields, terms) = (fields.clone(), terms.clone());
            add(blocking(move || tail_partial(&searcher, &live, &fields, &terms)).await?);
        }
        for term in terms.iter() {
            stats.doc_freq.entry(term.clone()).or_default();
        }
        stats.tokens = tokens2
            .into_iter()
            .map(|(field, sum2)| (field, sum2 / 2))
            .collect();
        Ok(stats)
    }

    /// A statistics provider for one split's schema: fields and terms by
    /// name; terms the request did not declare have `doc_freq` 0.
    pub fn provider<'a>(&'a self, schema: &'a Schema) -> impl Bm25StatisticsProvider + 'a {
        Provider {
            stats: self,
            schema,
        }
    }
}
