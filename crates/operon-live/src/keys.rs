//! The key layout of one Loam Live app (design §20 §4.3). Every key is
//! relative to the TiKV handle's root, inside the app's keyspace:
//!
//! ```text
//! app prefix   = ""                               (dedicated keyspace, R1)
//!              | 0xA0 ‖ app_id:u32 BE             (shared keyspace, reserved)
//! catalog      = prefix ‖ 0x01 ‖ kind:u8 ‖ name
//! document     = prefix ‖ 0x02 ‖ table_id:u32 BE ‖ doc_id[16]            → DocumentRecord
//! index entry  = prefix ‖ 0x03 ‖ table_id:u32 BE ‖ index_id:u32 BE
//!                ‖ tuple(values…) ‖ creation_ms:u64 BE ‖ doc_id[16]      → ""
//! journal head = prefix ‖ 0x04 ‖ 0x00 ‖ shard:u16 BE                     → last seq:u64 BE
//! journal entry= prefix ‖ 0x04 ‖ 0x01 ‖ shard:u16 BE ‖ seq:u64 BE        → JournalEntry
//! checkpoint   = prefix ‖ 0x04 ‖ 0x02 ‖ shard:u16 BE ‖ consumer
//!                → seq:u64 BE ‖ expires_ms:u64 BE (0 = never)
//! idempotency  = prefix ‖ 0x05 ‖ key_hash[16]                           → IdempotencyRecord
//! ```
//!
//! `key_hash` is the first 16 bytes of SHA-256 of the idempotency key.
//! Catalog kinds: `0x00` counters (`"table"`: the next table id, u32 BE),
//! `0x01` a table's name (→ its id, u32 BE), `0x02` a table (name =
//! id:u32 BE → `TableDef`), `0x03` the deployment pointer and `0x04` the
//! deployed schema (both Task 13), `0x05` the app's own settings (empty
//! name → `AppDef`: the journal shard count, Task 10).

use operon_tikv::tuple;

use crate::ids::{DOC_ID_BYTES, DocId, IndexId, TableId};

/// The catalog tag.
pub const CATALOG: u8 = 0x01;
/// The document tag.
pub const DOCUMENT: u8 = 0x02;
/// The index entry tag.
pub const INDEX: u8 = 0x03;
/// The journal tag (Task 9).
pub const JOURNAL: u8 = 0x04;
/// Journal sub-kind: a shard's head (its last sequence).
pub const JOURNAL_HEAD: u8 = 0x00;
/// Journal sub-kind: an entry.
pub const JOURNAL_ENTRY: u8 = 0x01;
/// Journal sub-kind: a consumer's checkpoint in one shard.
pub const JOURNAL_CHECKPOINT: u8 = 0x02;
/// The idempotency record tag (Task 10).
pub const IDEMPOTENCY: u8 = 0x05;
/// The first byte of a shared-keyspace app prefix (reserved, unused in R1).
pub const SHARED_APP: u8 = 0xA0;

/// Catalog kind: counters.
pub const KIND_COUNTER: u8 = 0x00;
/// Catalog kind: a table's name record.
pub const KIND_TABLE_NAME: u8 = 0x01;
/// Catalog kind: a table record, by id.
pub const KIND_TABLE: u8 = 0x02;
/// Catalog kind: the deployment pointer (Task 13).
pub const KIND_DEPLOYMENT: u8 = 0x03;
/// Catalog kind: the deployed schema (Task 13).
pub const KIND_SCHEMA: u8 = 0x04;
/// Catalog kind: the app's own settings (Task 10).
pub const KIND_APP: u8 = 0x05;

/// The length of an idempotency record's key hash.
pub const IDEMPOTENCY_HASH_BYTES: usize = 16;

/// A half-open key range `[lo, hi)`, keys relative to the handle's root. An
/// empty `hi` means "to the end of the root" (as [`tuple::successor`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRange {
    pub lo: Vec<u8>,
    pub hi: Vec<u8>,
}

impl KeyRange {
    /// Whether `key` is inside the range.
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.lo.as_slice() && (self.hi.is_empty() || key < self.hi.as_slice())
    }

    /// Whether the range holds no key.
    pub fn is_empty(&self) -> bool {
        !self.hi.is_empty() && self.lo >= self.hi
    }

    /// The range as scan bounds: `hi` is `None` when unbounded.
    pub fn bounds(&self) -> (&[u8], Option<&[u8]>) {
        (
            &self.lo,
            if self.hi.is_empty() {
                None
            } else {
                Some(&self.hi)
            },
        )
    }
}

/// The key builder of one app.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AppKeys {
    prefix: Vec<u8>,
}

impl AppKeys {
    /// An app in its own keyspace: the prefix is empty (R1).
    pub fn dedicated() -> Self {
        AppKeys { prefix: Vec::new() }
    }

    /// The app prefix.
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    fn with(&self, tag: u8, extra: usize) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.prefix.len() + 1 + extra);
        key.extend_from_slice(&self.prefix);
        key.push(tag);
        key
    }

    /// A catalog key.
    pub fn catalog(&self, kind: u8, name: &[u8]) -> Vec<u8> {
        let mut key = self.with(CATALOG, 1 + name.len());
        key.push(kind);
        key.extend_from_slice(name);
        key
    }

    /// The next-table-id counter.
    pub fn table_counter(&self) -> Vec<u8> {
        self.catalog(KIND_COUNTER, b"table")
    }

    /// A table's name record.
    pub fn table_name(&self, name: &str) -> Vec<u8> {
        self.catalog(KIND_TABLE_NAME, name.as_bytes())
    }

    /// A table record.
    pub fn table(&self, id: TableId) -> Vec<u8> {
        self.catalog(KIND_TABLE, &id.0.to_be_bytes())
    }

    /// The deployed schema record (Task 13).
    pub fn schema(&self) -> Vec<u8> {
        self.catalog(KIND_SCHEMA, b"")
    }

    /// The app's settings record (its journal shard count).
    pub fn app_def(&self) -> Vec<u8> {
        self.catalog(KIND_APP, b"")
    }

    /// Every table record.
    pub fn tables(&self) -> KeyRange {
        prefix_range(self.catalog(KIND_TABLE, b""))
    }

    /// A document key.
    pub fn document(&self, id: &DocId) -> Vec<u8> {
        let mut key = self.documents(id.table).lo;
        key.extend_from_slice(&id.bytes);
        key
    }

    /// Every document key of a table (the `by_id` index).
    pub fn documents(&self, table: TableId) -> KeyRange {
        let mut key = self.with(DOCUMENT, 4 + DOC_ID_BYTES);
        key.extend_from_slice(&table.0.to_be_bytes());
        prefix_range(key)
    }

    /// The prefix of an index: tag, table, index.
    pub fn index_prefix(&self, table: TableId, index: IndexId) -> Vec<u8> {
        let mut key = self.with(INDEX, 8);
        key.extend_from_slice(&table.0.to_be_bytes());
        key.extend_from_slice(&index.0.to_be_bytes());
        key
    }

    /// Every index entry of a table, all indexes.
    pub fn table_indexes(&self, table: TableId) -> KeyRange {
        let mut key = self.with(INDEX, 4);
        key.extend_from_slice(&table.0.to_be_bytes());
        prefix_range(key)
    }

    /// An index entry key: the index prefix, the tuple encoding of `values`,
    /// the creation time and the document id's bytes.
    pub fn index_entry(
        &self,
        index: IndexId,
        values: &[tuple::Elem<'_>],
        creation_ms: u64,
        id: &DocId,
    ) -> Vec<u8> {
        let mut key = self.index_prefix(id.table, index);
        for value in values {
            tuple::encode(&mut key, value);
        }
        key.extend_from_slice(&creation_ms.to_be_bytes());
        key.extend_from_slice(&id.bytes);
        key
    }

    fn journal_key(&self, kind: u8, shard: u16, extra: usize) -> Vec<u8> {
        let mut key = self.with(JOURNAL, 3 + extra);
        key.push(kind);
        key.extend_from_slice(&shard.to_be_bytes());
        key
    }

    /// A journal shard's head.
    pub fn journal_head(&self, shard: u16) -> Vec<u8> {
        self.journal_key(JOURNAL_HEAD, shard, 0)
    }

    /// A journal entry.
    pub fn journal_entry(&self, shard: u16, seq: u64) -> Vec<u8> {
        let mut key = self.journal_key(JOURNAL_ENTRY, shard, 8);
        key.extend_from_slice(&seq.to_be_bytes());
        key
    }

    /// Every entry of a journal shard.
    pub fn journal_entries(&self, shard: u16) -> KeyRange {
        prefix_range(self.journal_key(JOURNAL_ENTRY, shard, 0))
    }

    /// The sequence of a journal entry key of `shard`.
    pub fn seq_of_journal_entry(&self, shard: u16, key: &[u8]) -> Option<u64> {
        let prefix = self.journal_key(JOURNAL_ENTRY, shard, 0);
        let seq = key.strip_prefix(prefix.as_slice())?;
        Some(u64::from_be_bytes(seq.try_into().ok()?))
    }

    /// A consumer's checkpoint in a journal shard.
    pub fn journal_checkpoint(&self, shard: u16, consumer: &str) -> Vec<u8> {
        let mut key = self.journal_key(JOURNAL_CHECKPOINT, shard, consumer.len());
        key.extend_from_slice(consumer.as_bytes());
        key
    }

    /// Every consumer checkpoint of a journal shard.
    pub fn journal_checkpoints(&self, shard: u16) -> KeyRange {
        prefix_range(self.journal_key(JOURNAL_CHECKPOINT, shard, 0))
    }

    /// Every journal key: heads, entries and checkpoints.
    pub fn journal(&self) -> KeyRange {
        prefix_range(self.with(JOURNAL, 0))
    }

    /// The idempotency record of a key hash.
    pub fn idempotency(&self, hash: &[u8; IDEMPOTENCY_HASH_BYTES]) -> Vec<u8> {
        let mut key = self.with(IDEMPOTENCY, IDEMPOTENCY_HASH_BYTES);
        key.extend_from_slice(hash);
        key
    }

    /// Every idempotency record.
    pub fn idempotency_records(&self) -> KeyRange {
        prefix_range(self.with(IDEMPOTENCY, 0))
    }

    /// The index entries of every table whose id is `first` or above (the
    /// tables not created yet, when `first` is the table counter): a read
    /// that found no table depends on them.
    pub fn tables_from(&self, first: TableId) -> KeyRange {
        let mut lo = self.with(INDEX, 4);
        lo.extend_from_slice(&first.0.to_be_bytes());
        KeyRange {
            lo,
            hi: tuple::successor(&self.with(INDEX, 0)),
        }
    }

    /// The document id of a document key of `table`.
    pub fn doc_id_of_document(&self, table: TableId, key: &[u8]) -> Option<DocId> {
        let lo = self.documents(table).lo;
        let bytes = key.strip_prefix(lo.as_slice())?;
        Some(DocId {
            table,
            bytes: bytes.try_into().ok()?,
        })
    }

    /// The document id and creation time of an index entry key of `table`
    /// (its last 24 bytes).
    pub fn doc_of_index_entry(&self, table: TableId, key: &[u8]) -> Option<(DocId, u64)> {
        let prefix_len = self.prefix.len() + 1 + 8;
        if key.len() < prefix_len + 8 + DOC_ID_BYTES
            || !key.starts_with(&self.table_indexes(table).lo)
        {
            return None;
        }
        let tail = &key[key.len() - 8 - DOC_ID_BYTES..];
        let creation_ms = u64::from_be_bytes(tail[..8].try_into().ok()?);
        let bytes = tail[8..].try_into().ok()?;
        Some((DocId { table, bytes }, creation_ms))
    }
}

/// The range of keys starting with `prefix`.
pub fn prefix_range(prefix: Vec<u8>) -> KeyRange {
    let hi = tuple::successor(&prefix);
    KeyRange { lo: prefix, hi }
}

/// The smallest key greater than `key`: `key ‖ 0x00`.
pub fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0x00);
    next
}
