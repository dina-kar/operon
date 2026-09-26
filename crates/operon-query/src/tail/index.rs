//! A tail generation (plan M1.2 Task 3 rule 1): the append-only entries,
//! the RAM Tantivy index with the splits' mapping, and the writer-side
//! overlay state.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use operon_collection::{
    CollectionManifest, CollectionSchema, Document, DynamicMapping, ExtractedDoc, PrimaryKey,
    TantivyLayout, check_document, tantivy_layout, to_tantivy_doc,
};
use roaring::RoaringTreemap;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument, Term};

use super::snapshot::{Entry, GenShared, TailSnapshot};
use super::{TAIL_ROWID_BASE, TailDoc, TailError};

/// Tail row ids are never reused within a process (Ruling 7): every tail
/// draws its sequence numbers from here, so each generation's row ids
/// increase in append order.
static NEXT_SEQ: AtomicU64 = AtomicU64::new(0);

pub(crate) fn next_row_id() -> u64 {
    TAIL_ROWID_BASE + NEXT_SEQ.fetch_add(1, Ordering::Relaxed)
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What an entry costs: its source JSON, 4 bytes per dense float, 8 per
/// sparse entry and 96 of overhead (rule 5.6).
fn entry_bytes(doc: &Option<Arc<Document>>) -> usize {
    let Some(doc) = doc else {
        return 96;
    };
    let source = serde_json::to_vec(&doc.source).map_or(0, |bytes| bytes.len());
    let dense: usize = doc.vectors.values().map(Vec::len).sum();
    let sparse: usize = doc.sparse_vectors.values().map(|v| v.len()).sum();
    source + 4 * dense + 8 * sparse + 96
}

/// `schema` with dynamic mapping ignored: the check the link applies before
/// it writes a document (M1.1 Task 10 rule 3.5).
pub(crate) fn check_schema(schema: &CollectionSchema) -> CollectionSchema {
    let mut check = schema.clone();
    check.dynamic = DynamicMapping::Ignore;
    check
}

/// `schema` with every malformed value skipped: re-indexing a document
/// that an earlier schema accepted never fails.
fn lenient_schema(schema: &CollectionSchema) -> CollectionSchema {
    let mut lenient = check_schema(schema);
    for field in &mut lenient.fields {
        field.ignore_malformed = true;
    }
    lenient
}

/// One generation: its entries and RAM index.
pub(crate) struct Generation {
    pub shared: Arc<GenShared>,
    /// Kept alive with its writer and reader.
    _index: Index,
    writer: IndexWriter<TantivyDocument>,
    reader: IndexReader,
    layout: TantivyLayout,
    /// The collection schema the index maps.
    schema: CollectionSchema,
    pub check: CollectionSchema,
    dirty: bool,
    entry_bytes: usize,
    len: usize,
}

impl Generation {
    /// An empty generation indexing with `schema`, as `build_split` does
    /// (Tantivy's operon-text analyzers for text and fast fields).
    pub fn new(schema: &CollectionSchema, writer_memory: usize) -> Result<Self, TailError> {
        let layout = tantivy_layout(schema);
        let mut index = Index::create_in_ram(layout.schema.clone());
        index.set_tokenizers(operon_text::tokenizer_manager());
        index.set_fast_field_tokenizers(operon_text::tokenizer_manager());
        let writer = index.writer_with_num_threads(1, writer_memory)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            shared: Arc::new(GenShared::default()),
            _index: index,
            writer,
            reader,
            layout,
            schema: schema.clone(),
            check: check_schema(schema),
            dirty: false,
            entry_bytes: 0,
            len: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn entry_bytes(&self) -> usize {
        self.entry_bytes
    }

    fn pk_term(&self, pk: &PrimaryKey) -> Term {
        Term::from_field_bytes(self.layout.pk, &pk.canonical())
    }

    /// The latest entry of `pk`, covered or not.
    pub fn latest_entry(&self, pk: &PrimaryKey) -> Option<Arc<TailDoc>> {
        let entries = self
            .shared
            .entries
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let latest = self.shared.latest.read().unwrap_or_else(|p| p.into_inner());
        latest
            .get(pk)
            .map(|index| entries[*index as usize].doc.clone())
    }

    /// Appends `doc` as its key's latest entry and replaces the key's doc in
    /// the index (`extracted`: its typed values under `schema`).
    pub fn push(
        &mut self,
        doc: TailDoc,
        extracted: Option<&ExtractedDoc>,
    ) -> Result<(), TailError> {
        self.writer.delete_term(self.pk_term(&doc.pk));
        if let (Some(source), Some(extracted)) = (&doc.doc, extracted) {
            let tantivy_doc =
                to_tantivy_doc(&self.layout, &self.schema, source, extracted, doc.row_id);
            self.writer.add_document(tantivy_doc)?;
        }
        self.entry_bytes += entry_bytes(&doc.doc);
        let pk = doc.pk.clone();
        let index = u32::try_from(self.len).expect("fewer than 2^32 tail entries");
        let prev = write(&self.shared.latest).get(&pk).copied();
        write(&self.shared.entries).push(Entry {
            doc: Arc::new(doc),
            prev,
        });
        write(&self.shared.latest).insert(pk, index);
        self.len += 1;
        self.dirty = true;
        Ok(())
    }

    /// Removes `pk`'s doc from the index (its entries stay, as garbage).
    pub fn delete(&mut self, pk: &PrimaryKey) {
        self.writer.delete_term(self.pk_term(pk));
        self.dirty = true;
    }

    /// Commits pending changes (if any) and returns a searcher over them.
    pub fn commit(&mut self) -> Result<Searcher, TailError> {
        if self.dirty {
            self.writer.commit()?;
            self.reader.reload()?;
            self.dirty = false;
        }
        Ok(self.reader.searcher())
    }
}

/// The writer side of a tail: the generation, the manifest M_T it is
/// relative to, how far each partition is folded in, and the overlay.
///
/// Invariant: every overlay key's latest entry has `offset >=
/// manifest.applied[partition]`. The overlay keys are exactly the keys of
/// `shadow_of`.
pub(crate) struct TailIndex {
    pub generation: Generation,
    pub manifest_path: Option<String>,
    pub manifest: Arc<CollectionManifest>,
    pub head: BTreeMap<u32, u64>,
    /// Per overlay key, its row id in M_T.
    pub shadow_of: HashMap<PrimaryKey, Option<u64>>,
    pub live_now: RoaringTreemap,
    pub shadow_now: RoaringTreemap,
    pub schema_version: u64,
    writer_memory: usize,
}

impl TailIndex {
    /// An empty tail over `manifest`, folded in up to its `applied`.
    pub fn new(
        schema: &CollectionSchema,
        manifest_path: Option<String>,
        manifest: Arc<CollectionManifest>,
        writer_memory: usize,
    ) -> Result<Self, TailError> {
        Ok(Self {
            generation: Generation::new(schema, writer_memory)?,
            manifest_path,
            head: manifest.applied.clone(),
            manifest,
            shadow_of: HashMap::new(),
            live_now: RoaringTreemap::new(),
            shadow_now: RoaringTreemap::new(),
            schema_version: schema.version,
            writer_memory,
        })
    }

    /// The overlay state of `pk`: `Some(state)` iff it is an overlay key.
    pub fn overlay_state(&self, pk: &PrimaryKey) -> Option<Option<Document>> {
        if !self.shadow_of.contains_key(pk) {
            return None;
        }
        let entry = self.generation.latest_entry(pk)?;
        Some(entry.doc.as_deref().cloned())
    }

    /// Entries no overlay key points at.
    pub fn garbage(&self) -> usize {
        self.generation.len() - self.shadow_of.len()
    }

    /// Drops every overlay key whose latest entry `applied` covers (rule
    /// 6.2): it leaves `live_now`, `shadow_now`, `shadow_of` and the index;
    /// its entries stay as garbage.
    pub fn drop_covered(&mut self, applied: &BTreeMap<u32, u64>) {
        let covered: Vec<(PrimaryKey, Option<u64>)> = self
            .shadow_of
            .iter()
            .filter_map(|(pk, shadow)| {
                let entry = self.generation.latest_entry(pk)?;
                let applied = applied.get(&entry.partition).copied().unwrap_or(0);
                (entry.offset < applied).then(|| (pk.clone(), *shadow))
            })
            .collect();
        for (pk, shadow) in covered {
            if let Some(entry) = self.generation.latest_entry(&pk) {
                self.live_now.remove(entry.row_id);
            }
            if let Some(row) = shadow {
                self.shadow_now.remove(row);
            }
            self.shadow_of.remove(&pk);
            self.generation.delete(&pk);
        }
    }

    /// Whether compaction would reclaim enough (rule 7).
    pub fn wants_compaction(&self, ratio: f64, min_entries: usize) -> bool {
        let entries = self.generation.len();
        entries >= min_entries && self.garbage() as f64 >= ratio * entries as f64
    }

    /// A new generation with `schema`, holding the overlay keys' latest
    /// entries (in PK order, with fresh row ids); `shadow_of` and
    /// `shadow_now` are unchanged (rule 7). The old generation lives on in
    /// the snapshots that hold it.
    pub fn compact(&mut self, schema: &CollectionSchema) -> Result<(), TailError> {
        let mut next = Generation::new(schema, self.writer_memory)?;
        let lenient = lenient_schema(schema);
        let keys: Vec<PrimaryKey> = self
            .generation
            .shared
            .latest
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .filter(|pk| self.shadow_of.contains_key(*pk))
            .cloned()
            .collect();
        let mut live = RoaringTreemap::new();
        for pk in keys {
            let Some(entry) = self.generation.latest_entry(&pk) else {
                continue;
            };
            let row_id = next_row_id();
            let extracted = entry.doc.as_deref().map(|doc| {
                check_document(&lenient, doc).unwrap_or(ExtractedDoc { values: Vec::new() })
            });
            if entry.doc.is_some() {
                live.insert(row_id);
            }
            next.push(
                TailDoc {
                    row_id,
                    pk,
                    partition: entry.partition,
                    offset: entry.offset,
                    doc: entry.doc.clone(),
                },
                extracted.as_ref(),
            )?;
        }
        next.dirty = true;
        self.generation = next;
        self.live_now = live;
        self.schema_version = schema.version;
        Ok(())
    }

    /// Commits and returns what readers see now (rule 2).
    pub fn snapshot(&mut self) -> Result<Arc<TailSnapshot>, TailError> {
        let searcher = self.generation.commit()?;
        let index_bytes = searcher
            .space_usage()
            .map_or(0, |usage| usage.total().get_bytes() as usize);
        Ok(Arc::new(TailSnapshot {
            manifest_path: self.manifest_path.clone(),
            manifest: self.manifest.clone(),
            head: self.head.clone(),
            schema_version: self.schema_version,
            searcher: Some(searcher),
            generation: Some(self.generation.shared.clone()),
            len: self.generation.len(),
            live: self.live_now.clone(),
            shadow: self.shadow_now.clone(),
            bytes: self.generation.entry_bytes() + index_bytes,
            garbage: self.garbage(),
        }))
    }
}
