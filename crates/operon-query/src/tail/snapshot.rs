//! What a read sees of the tail (plan M1.2 Task 3 rule 2): one published,
//! immutable view relative to one manifest.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use operon_collection::{CollectionManifest, PrimaryKey};
use roaring::RoaringTreemap;
use tantivy::Searcher;

use super::{TailDoc, TailLookup};

/// One entry of a generation: a tail doc and the index of the previous
/// entry of the same key.
#[derive(Debug)]
pub(crate) struct Entry {
    pub doc: Arc<TailDoc>,
    pub prev: Option<u32>,
}

/// The part of a generation that snapshots share with its writer: the
/// append-only entries (in row-id order) and each key's latest entry.
///
/// The writer pushes an entry before it points `latest` at it, and readers
/// take the entries lock before the `latest` lock, so every index a reader
/// finds in `latest` is an entry it can read. An entry never changes once
/// pushed; a snapshot reads only the entries below its length.
#[derive(Default)]
pub(crate) struct GenShared {
    pub entries: RwLock<Vec<Entry>>,
    pub latest: RwLock<BTreeMap<PrimaryKey, u32>>,
}

impl fmt::Debug for GenShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenShared").finish_non_exhaustive()
    }
}

fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A published view of the tail relative to `manifest`.
#[derive(Debug)]
pub struct TailSnapshot {
    pub(crate) manifest_path: Option<String>,
    pub(crate) manifest: Arc<CollectionManifest>,
    pub(crate) head: BTreeMap<u32, u64>,
    pub(crate) schema_version: u64,
    pub(crate) searcher: Option<Searcher>,
    pub(crate) generation: Option<Arc<GenShared>>,
    /// The generation's entries this snapshot reads.
    pub(crate) len: usize,
    pub(crate) live: RoaringTreemap,
    pub(crate) shadow: RoaringTreemap,
    pub(crate) bytes: usize,
    pub(crate) garbage: usize,
}

impl TailSnapshot {
    /// A tail holding nothing over `manifest` (head = its `applied`).
    pub fn empty(
        manifest_path: Option<String>,
        manifest: Arc<CollectionManifest>,
        schema_version: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            manifest_path,
            head: manifest.applied.clone(),
            manifest,
            schema_version,
            searcher: None,
            generation: None,
            len: 0,
            live: RoaringTreemap::new(),
            shadow: RoaringTreemap::new(),
            bytes: 0,
            garbage: 0,
        })
    }

    pub fn manifest(&self) -> &Arc<CollectionManifest> {
        &self.manifest
    }

    pub fn manifest_path(&self) -> Option<&str> {
        self.manifest_path.as_deref()
    }

    /// Per partition: every record below is folded in.
    pub fn head(&self) -> &BTreeMap<u32, u64> {
        &self.head
    }

    /// The collection schema version the index was built with.
    pub fn schema_version(&self) -> u64 {
        self.schema_version
    }

    /// The RAM index; `None` when no overlay doc is present.
    pub fn searcher(&self) -> Option<&Searcher> {
        if self.live.is_empty() {
            None
        } else {
            self.searcher.as_ref()
        }
    }

    /// Row ids of the present overlay docs.
    pub fn live(&self) -> &RoaringTreemap {
        &self.live
    }

    /// Durable row ids superseded by overlay entries (present or deleted).
    pub fn shadow(&self) -> &RoaringTreemap {
        &self.shadow
    }

    pub fn live_count(&self) -> u64 {
        self.live.len()
    }

    /// The overlay state of `pk`.
    pub fn get(&self, pk: &PrimaryKey) -> TailLookup {
        let Some(generation) = &self.generation else {
            return TailLookup::Absent;
        };
        let entries = read(&generation.entries);
        let latest = read(&generation.latest);
        self.lookup(&entries, &latest, pk)
    }

    /// `pk`'s latest entry below `len`, if not covered by the manifest.
    fn lookup(
        &self,
        entries: &[Entry],
        latest: &BTreeMap<PrimaryKey, u32>,
        pk: &PrimaryKey,
    ) -> TailLookup {
        let mut at = latest.get(pk).copied();
        while let Some(index) = at {
            if (index as usize) < self.len {
                break;
            }
            at = entries[index as usize].prev;
        }
        let Some(index) = at else {
            return TailLookup::Absent;
        };
        let doc = &entries[index as usize].doc;
        let applied = self
            .manifest
            .applied
            .get(&doc.partition)
            .copied()
            .unwrap_or(0);
        if doc.offset < applied {
            TailLookup::Absent
        } else if doc.doc.is_some() {
            TailLookup::Present(doc.clone())
        } else {
            TailLookup::Deleted(doc.clone())
        }
    }

    /// The entry with row id `row_id`.
    pub fn doc(&self, row_id: u64) -> Option<Arc<TailDoc>> {
        let generation = self.generation.as_ref()?;
        let entries = read(&generation.entries);
        let entries = &entries[..self.len.min(entries.len())];
        let at = entries
            .binary_search_by_key(&row_id, |entry| entry.doc.row_id)
            .ok()?;
        Some(entries[at].doc.clone())
    }

    /// Overlay keys after `after` in PK order, with their state; at most
    /// `limit`.
    pub fn keys_after(
        &self,
        after: Option<&PrimaryKey>,
        limit: usize,
    ) -> Vec<(PrimaryKey, TailLookup)> {
        let Some(generation) = &self.generation else {
            return Vec::new();
        };
        let entries = read(&generation.entries);
        let latest = read(&generation.latest);
        let lower = match after {
            Some(pk) => Bound::Excluded(pk),
            None => Bound::Unbounded,
        };
        latest
            .range::<PrimaryKey, _>((lower, Bound::Unbounded))
            .filter_map(|(pk, _)| match self.lookup(&entries, &latest, pk) {
                TailLookup::Absent => None,
                found => Some((pk.clone(), found)),
            })
            .take(limit)
            .collect()
    }

    /// The present overlay docs, in row-id order.
    pub fn live_docs(&self) -> Vec<Arc<TailDoc>> {
        let Some(generation) = &self.generation else {
            return Vec::new();
        };
        let entries = read(&generation.entries);
        let entries = &entries[..self.len.min(entries.len())];
        let mut out = Vec::with_capacity(self.live.len() as usize);
        let mut at = 0;
        for row_id in &self.live {
            match entries[at..].binary_search_by_key(&row_id, |entry| entry.doc.row_id) {
                Ok(found) => {
                    at += found;
                    out.push(entries[at].doc.clone());
                }
                Err(skip) => at += skip,
            }
        }
        out
    }

    /// The memory the tail held at publish: its entries plus the RAM index.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Entries of the generation that no overlay key points at any more
    /// (covered or superseded); compaction drops them.
    pub fn garbage(&self) -> usize {
        self.garbage
    }

    /// The generation's entries this snapshot reads.
    pub fn entry_count(&self) -> usize {
        self.len
    }
}
