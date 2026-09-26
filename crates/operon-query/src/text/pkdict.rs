//! The per-split primary-key dictionaries (plan M1.2 Task 7 rule 6;
//! Ruling 15): scroll and constant-score top-k walk the `_pk` term
//! dictionaries of the splits in canonical PK order.
//!
//! A cached split keeps its `_pk` dictionary, the `_pk` postings and the
//! `_rowid` fast column warm, so a cursor reads them synchronously. The
//! term dictionary of the raw `_pk` field is already in canonical PK order.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use operon_collection::{PK_FIELD, ROWID_FIELD};
use operon_quickwit::doc_mapper::{FastFieldWarmupInfo, WarmupInfo};
use roaring::RoaringBitmap;
use tantivy::columnar::Column;
use tantivy::fastfield::AliveBitSet;
use tantivy::index::InvertedIndexReader;
use tantivy::schema::IndexRecordOption;
use tantivy::{DocSet, ReloadPolicy, Searcher, TERMINATED};
use ulid::Ulid;

use crate::error::ServiceError;
use crate::read::{ReadView, collection_error};

/// Terms a cursor reads from a segment's dictionary at a time.
const CHUNK: u64 = 256;

fn tantivy_error(err: impl fmt::Display) -> ServiceError {
    ServiceError::Internal(format!("tantivy: {err}"))
}

/// One split, opened with its `_pk` dictionary, postings and `_rowid`
/// column warm.
struct PkDict {
    searcher: Searcher,
}

/// Split ULID → the split's warm PK dictionary, weighted by an estimate of
/// its warm bytes (`_pk` terms and postings, `_rowid` values). Splits are
/// immutable, so entries never go stale; a cursor applies the view's
/// delete bitmap and shadow itself.
#[derive(Clone)]
pub struct PkDictCache {
    cache: moka::future::Cache<Ulid, Arc<PkDict>>,
}

impl fmt::Debug for PkDictCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkDictCache")
            .field("entries", &self.cache.entry_count())
            .field("bytes", &self.cache.weighted_size())
            .finish()
    }
}

/// The warm bytes of a split's PK walk, estimated: ~32 bytes per `_pk`
/// term (key, term info and postings) and 8 per `_rowid` value.
fn weight_of(searcher: &Searcher) -> u32 {
    let mut bytes = 0u64;
    for reader in searcher.segment_readers() {
        let terms = searcher
            .schema()
            .get_field(PK_FIELD)
            .ok()
            .and_then(|field| reader.inverted_index(field).ok())
            .map_or(0, |index| index.terms().num_terms() as u64);
        bytes += terms * 32 + u64::from(reader.max_doc()) * 8;
    }
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

impl PkDictCache {
    /// At most `max_bytes` (256 MiB) of warm dictionaries.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            cache: moka::future::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|_: &Ulid, dict: &Arc<PkDict>| weight_of(&dict.searcher))
                .build(),
        }
    }

    async fn dict(&self, view: &ReadView, split: usize) -> Result<Arc<PkDict>, ServiceError> {
        let split_ref = view
            .snapshot
            .splits()
            .get(split)
            .ok_or_else(|| ServiceError::Internal(format!("no split {split} in the view")))?
            .clone();
        self.cache
            .try_get_with(split_ref.ulid, async {
                let index = view
                    .snapshot
                    .open_split(&split_ref)
                    .await
                    .map_err(collection_error)?;
                let reader = index
                    .reader_builder()
                    .reload_policy(ReloadPolicy::Manual)
                    .try_into()
                    .map_err(tantivy_error)?;
                let searcher = reader.searcher();
                let schema = searcher.schema();
                let mut warmup = WarmupInfo::default();
                if let Ok(field) = schema.get_field(PK_FIELD) {
                    warmup.term_dict_fields.insert(field);
                }
                warmup.fast_fields.insert(FastFieldWarmupInfo {
                    name: ROWID_FIELD.to_string(),
                    with_subfields: false,
                });
                operon_quickwit::search::warmup(&searcher, &warmup)
                    .await
                    .map_err(|err| {
                        ServiceError::Unavailable(format!(
                            "warming the keys of split {}: {err:#}",
                            split_ref.ulid
                        ))
                    })?;
                Ok::<_, ServiceError>(Arc::new(PkDict { searcher }))
            })
            .await
            .map_err(|err| (*err).clone())
    }

    /// A cursor over the live, unshadowed keys of split `split` of the
    /// view's manifest, after `after` (canonical bytes, exclusive).
    pub async fn cursor(
        &self,
        view: &ReadView,
        split: usize,
        after: Option<&[u8]>,
    ) -> Result<PkCursor, ServiceError> {
        let dict = self.dict(view, split).await?;
        let split_ref = &view.snapshot.splits()[split];
        let deleted = view.bitmaps.deleted_docs(&view.snapshot, split_ref).await?;
        let mut masked = (*deleted).clone();
        for row in view.tail.shadow() {
            if let Some((at, doc)) = view.snapshot.locate_row(row)
                && at == split
            {
                masked.insert(doc);
            }
        }
        let searcher = &dict.searcher;
        let pk_field = searcher
            .schema()
            .get_field(PK_FIELD)
            .map_err(tantivy_error)?;
        let mut base = 0u32;
        let mut segments = Vec::with_capacity(searcher.segment_readers().len());
        for reader in searcher.segment_readers() {
            let end = base + reader.max_doc();
            let local: RoaringBitmap = masked.range(base..end).map(|doc| doc - base).collect();
            segments.push(SegmentCursor {
                index: reader.inverted_index(pk_field).map_err(tantivy_error)?,
                rowid: reader
                    .fast_fields()
                    .u64(ROWID_FIELD)
                    .map_err(tantivy_error)?,
                alive: reader.alive_bitset().cloned(),
                masked: local,
                buffer: VecDeque::new(),
                resume: after.map(<[u8]>::to_vec),
                done: false,
            });
            base = end;
        }
        Ok(PkCursor { segments })
    }
}

/// The keys of one segment, read `CHUNK` terms at a time.
struct SegmentCursor {
    index: Arc<InvertedIndexReader>,
    rowid: Column<u64>,
    alive: Option<AliveBitSet>,
    masked: RoaringBitmap,
    buffer: VecDeque<(Vec<u8>, u64)>,
    /// The last key read (exclusive bound of the next chunk).
    resume: Option<Vec<u8>>,
    done: bool,
}

impl SegmentCursor {
    /// Fills the buffer with the next live keys, if any are left.
    fn fill(&mut self) -> Result<(), ServiceError> {
        while self.buffer.is_empty() && !self.done {
            let terms = self.index.terms();
            let builder = match &self.resume {
                Some(key) => terms.range().gt(key),
                None => terms.range(),
            };
            let mut stream = builder.limit(CHUNK).into_stream().map_err(tantivy_error)?;
            let mut read = 0u64;
            while stream.advance() {
                read += 1;
                let key = stream.key().to_vec();
                let mut postings = self
                    .index
                    .read_postings_from_terminfo(stream.value(), IndexRecordOption::Basic)
                    .map_err(tantivy_error)?;
                let mut doc = postings.doc();
                while doc != TERMINATED {
                    let alive = self.alive.as_ref().is_none_or(|a| a.is_alive(doc));
                    if alive && !self.masked.contains(doc) {
                        let row = self.rowid.first(doc).ok_or_else(|| {
                            ServiceError::Internal(format!("doc {doc} has no _rowid"))
                        })?;
                        self.buffer.push_back((key.clone(), row));
                    }
                    doc = postings.advance();
                }
                self.resume = Some(key);
            }
            if read < CHUNK {
                self.done = true;
            }
        }
        Ok(())
    }
}

/// The live, unshadowed keys of one split in canonical PK order, with their
/// row ids.
pub struct PkCursor {
    segments: Vec<SegmentCursor>,
}

impl fmt::Debug for PkCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PkCursor")
            .field("segments", &self.segments.len())
            .finish()
    }
}

impl PkCursor {
    /// The next key (canonical bytes) and its row id.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, u64)>, ServiceError> {
        let mut best: Option<usize> = None;
        for i in 0..self.segments.len() {
            self.segments[i].fill()?;
            let Some((key, _)) = self.segments[i].buffer.front() else {
                continue;
            };
            let better = match best {
                None => true,
                Some(b) => {
                    let (best_key, _) = self.segments[b].buffer.front().expect("filled");
                    key < best_key
                }
            };
            if better {
                best = Some(i);
            }
        }
        Ok(best.and_then(|i| self.segments[i].buffer.pop_front()))
    }
}
