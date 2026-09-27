//! Documents and index maintenance inside a TiKV transaction (design §20
//! §4.1–§4.3; R1 plan Task 8). `LiveTxn` (Task 10) calls these.
//!
//! Every write returns a [`WriteRecord`] naming the document and its index
//! keys: `index_keys_removed` lists **every** index entry the document had
//! before the write and `index_keys_added` **every** entry it has after, so
//! the journal matches both the old and the new position of the document
//! (an entry in both lists is kept, not rewritten). Index entries are the
//! `by_creation_time` entry and one per user index; `by_id` is the document
//! key itself. A missing indexed field is indexed as `null`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::ops::Bound;

use buffa::Message;
use operon_tikv::tuple::{self, Elem};
use operon_tikv::{Pair, Snap, Tikv, Txn, TxnError};

use crate::catalog::TableDef;
use crate::ids::{DocId, IndexId, TableId};
use crate::keys::{AppKeys, KeyRange, key_after};
use crate::value::{fields_from_proto, fields_to_proto};
use crate::{Limits, LiveError, LiveValue, pb};

/// A journal write record (§20 §5.3).
pub type WriteRecord = pb::WriteRecord;

/// A document: its id, creation time and user fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    pub id: DocId,
    /// `_creationTime`, ms since the epoch.
    pub creation_ms: u64,
    pub fields: BTreeMap<String, LiveValue>,
}

impl Doc {
    /// The stored record.
    pub fn to_record(&self) -> pb::DocumentRecord {
        pb::DocumentRecord {
            format: 1,
            creation_ms: self.creation_ms,
            fields: fields_to_proto(&self.fields),
            ..Default::default()
        }
    }

    /// The document of a stored record.
    pub fn from_record(id: DocId, record: pb::DocumentRecord) -> Result<Self, LiveError> {
        if record.format != 1 {
            return Err(LiveError::Corrupt(format!(
                "document {id}: record format {} (expected 1)",
                record.format
            )));
        }
        Ok(Doc {
            id,
            creation_ms: record.creation_ms,
            fields: fields_from_proto(record.fields)
                .map_err(|e| LiveError::Corrupt(format!("document {id}: {e}")))?,
        })
    }

    fn decode(id: DocId, bytes: &[u8]) -> Result<Self, LiveError> {
        let record = pb::DocumentRecord::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("document {id}: {e}")))?;
        Doc::from_record(id, record)
    }
}

/// Sort direction of an index range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

/// A read of one index (§20 §4.2): equality on a prefix of the index's
/// fields, then optional bounds on the next field, in either direction, with
/// an optional limit. The fields of an index are its user fields, then
/// `_creationTime` (an `I64`, at least 0). On `by_id` (`IndexId::BY_ID`),
/// `eq` must be empty and the bounds are document ids in their text form
/// (`Str`).
#[derive(Debug, Clone, PartialEq)]
pub struct IndexRange {
    pub table: TableId,
    pub index: IndexId,
    pub eq: Vec<LiveValue>,
    pub lower: Bound<LiveValue>,
    pub upper: Bound<LiveValue>,
    pub order: Order,
    pub limit: Option<u32>,
}

impl IndexRange {
    /// Every entry of `index`, ascending, without a limit.
    pub fn all(table: TableId, index: IndexId) -> Self {
        IndexRange {
            table,
            index,
            eq: Vec::new(),
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            order: Order::Asc,
            limit: None,
        }
    }
}

/// Reads through a [`Txn`] or a [`Snap`]; keys are relative to the root.
pub trait Reads: Send {
    /// The value at `key`.
    fn get(&mut self, key: &[u8])
    -> impl Future<Output = Result<Option<Vec<u8>>, TxnError>> + Send;
    /// The values at `keys`; absent keys are left out; sorted by key.
    fn batch_get(
        &mut self,
        keys: Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<Vec<Pair>, TxnError>> + Send;
    /// Up to `limit` pairs of `range`, in key order or (`reverse`) the
    /// opposite.
    fn scan(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> impl Future<Output = Result<Vec<Pair>, TxnError>> + Send;
}

impl Reads for Txn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        Txn::get(self, key).await
    }

    async fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> Result<Vec<Pair>, TxnError> {
        Txn::batch_get(self, keys).await
    }

    async fn scan(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<Pair>, TxnError> {
        let (lo, hi) = range.bounds();
        if reverse {
            Txn::scan_reverse(self, lo, hi, limit).await
        } else {
            Txn::scan(self, lo, hi, limit).await
        }
    }
}

impl Reads for Snap {
    async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        Snap::get(self, key).await
    }

    async fn batch_get(&mut self, keys: Vec<Vec<u8>>) -> Result<Vec<Pair>, TxnError> {
        Snap::batch_get(self, keys).await
    }

    async fn scan(
        &mut self,
        range: &KeyRange,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<Pair>, TxnError> {
        let (lo, hi) = range.bounds();
        if reverse {
            Snap::scan_reverse(self, lo, hi, limit).await
        } else {
            Snap::scan(self, lo, hi, limit).await
        }
    }
}

/// The document `id`.
pub async fn get(r: &mut impl Reads, app: &AppKeys, id: DocId) -> Result<Option<Doc>, LiveError> {
    match r.get(&app.document(&id)).await? {
        None => Ok(None),
        Some(bytes) => Doc::decode(id, &bytes).map(Some),
    }
}

/// Inserts a document into `table` with a new id from the OS random source.
/// `_creationTime` is the physical time of the transaction's start
/// timestamp.
pub async fn insert(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    fields: BTreeMap<String, LiveValue>,
    limits: &Limits,
) -> Result<(DocId, WriteRecord), LiveError> {
    limits.check_fields(&fields)?;
    let doc = Doc {
        id: DocId::random(table.id)?,
        creation_ms: Tikv::physical_ms(&txn.start_ts()),
        fields,
    };
    let record = encode(&doc, limits)?;
    let added = index_keys(app, table, &doc, limits)?;
    txn.insert(&app.document(&doc.id), record).await?;
    for key in &added {
        txn.put(key, Vec::new()).await?;
    }
    Ok((
        doc.id,
        write_record(&doc.id, pb::WriteKind::WRITE_KIND_INSERT, Vec::new(), added),
    ))
}

/// Replaces the fields of document `id` of `table` (its id and creation
/// time stay). A missing document is [`LiveError::NotFound`].
pub async fn replace(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    id: DocId,
    fields: BTreeMap<String, LiveValue>,
    limits: &Limits,
) -> Result<WriteRecord, LiveError> {
    let old = existing(txn, app, table, id).await?;
    rewrite(txn, app, table, old, fields, limits).await
}

/// Sets the given fields of document `id` of `table`, keeping the others (a
/// shallow merge; R1 cannot remove a field by patching). A missing document
/// is [`LiveError::NotFound`]. The write record is a replace.
pub async fn patch(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    id: DocId,
    fields: BTreeMap<String, LiveValue>,
    limits: &Limits,
) -> Result<WriteRecord, LiveError> {
    let old = existing(txn, app, table, id).await?;
    let mut merged = old.fields.clone();
    merged.extend(fields);
    rewrite(txn, app, table, old, merged, limits).await
}

/// Deletes document `id` of `table` and every index entry of it; `None`
/// when there is no such document.
pub async fn delete(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    id: DocId,
    limits: &Limits,
) -> Result<Option<WriteRecord>, LiveError> {
    check_table(table, &id)?;
    let Some(old) = get(txn, app, id).await? else {
        return Ok(None);
    };
    let removed = index_keys(app, table, &old, limits)?;
    txn.delete(&app.document(&id)).await?;
    for key in &removed {
        txn.delete(key).await?;
    }
    Ok(Some(write_record(
        &id,
        pb::WriteKind::WRITE_KIND_DELETE,
        removed,
        Vec::new(),
    )))
}

/// Reads an index range: the documents in index order and the key range the
/// read depends on (its read-set range, §20 §8.1). With a limit that the
/// range fills, the reported range ends at the last key read (inclusive),
/// so writes past it do not invalidate the read. Without a limit, a range
/// holding more than `limits.max_scanned_docs` documents is refused; a
/// limit above it is refused too.
pub async fn scan(
    r: &mut impl Reads,
    app: &AppKeys,
    table: &TableDef,
    range: &IndexRange,
    limits: &Limits,
) -> Result<(Vec<Doc>, KeyRange), LiveError> {
    let keys = index_key_range(app, table, range)?;
    let max = limits.max_scanned_docs;
    let fetch = match range.limit {
        Some(limit) if limit as usize > max => {
            return Err(LiveError::limit(
                "max_scanned_docs",
                format!("a scan limit of {limit} is more than {max}"),
            ));
        }
        Some(limit) => limit as usize,
        None => max.saturating_add(1),
    };
    let reverse = range.order == Order::Desc;
    let pairs = if keys.is_empty() || fetch == 0 {
        Vec::new()
    } else {
        r.scan(&keys, fetch, reverse).await?
    };
    if range.limit.is_none() && pairs.len() > max {
        return Err(LiveError::limit(
            "max_scanned_docs",
            format!("the range holds more than {max} documents; add a limit"),
        ));
    }
    let read = match (range.limit, pairs.last()) {
        (Some(limit), Some((last, _))) if pairs.len() == limit as usize => {
            if reverse {
                KeyRange {
                    lo: last.clone(),
                    hi: keys.hi.clone(),
                }
            } else {
                KeyRange {
                    lo: keys.lo.clone(),
                    hi: key_after(last),
                }
            }
        }
        _ => keys,
    };
    let docs = if range.index == IndexId::BY_ID {
        pairs
            .into_iter()
            .map(|(key, value)| {
                let id = app.doc_id_of_document(table.id, &key).ok_or_else(|| {
                    LiveError::Corrupt("a document key of the wrong length".into())
                })?;
                Doc::decode(id, &value)
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        let ids = pairs
            .iter()
            .map(|(key, _)| {
                app.doc_of_index_entry(table.id, key)
                    .map(|(id, _)| id)
                    .ok_or_else(|| LiveError::Corrupt("an index entry key is too short".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let found: HashMap<Vec<u8>, Vec<u8>> = r
            .batch_get(ids.iter().map(|id| app.document(id)).collect())
            .await?
            .into_iter()
            .collect();
        ids.into_iter()
            .map(|id| {
                let bytes = found.get(&app.document(&id)).ok_or_else(|| {
                    LiveError::Corrupt(format!("an index entry names a missing document {id}"))
                })?;
                Doc::decode(id, bytes)
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    Ok((docs, read))
}

/// The key range an index range covers, before any limit.
pub fn index_key_range(
    app: &AppKeys,
    table: &TableDef,
    range: &IndexRange,
) -> Result<KeyRange, LiveError> {
    if range.table != table.id {
        return Err(LiveError::invalid(format!(
            "the range is on table {}, not on '{}' ({})",
            range.table.0, table.name, table.id.0
        )));
    }
    if range.index == IndexId::BY_ID {
        if !range.eq.is_empty() {
            return Err(LiveError::invalid(
                "by_id takes bounds on _id, not equality",
            ));
        }
        let base = app.documents(table.id).lo;
        let at = |v: &LiveValue| -> Result<Vec<u8>, LiveError> {
            let LiveValue::Str(text) = v else {
                return Err(LiveError::invalid(format!(
                    "a by_id bound is a document id string, not {}",
                    v.type_name()
                )));
            };
            let id: DocId = text.parse()?;
            check_table(table, &id)?;
            let mut key = base.clone();
            key.extend_from_slice(&id.bytes);
            Ok(key)
        };
        return bounded(&base, &range.lower, &range.upper, at);
    }
    let fields = table.index_fields(range.index).ok_or_else(|| {
        LiveError::NotFound(format!("index {} of table '{}'", range.index.0, table.name))
    })?;
    let n = fields.len();
    if range.eq.len() > n + 1 {
        return Err(LiveError::invalid(format!(
            "{} equality values for an index of {} fields and _creationTime",
            range.eq.len(),
            n
        )));
    }
    let next = range.eq.len();
    let bounded_next = !matches!(
        (&range.lower, &range.upper),
        (Bound::Unbounded, Bound::Unbounded)
    );
    if bounded_next && next > n {
        return Err(LiveError::invalid(
            "an index range cannot bound _id after equality on _creationTime",
        ));
    }
    let mut base = app.index_prefix(table.id, range.index);
    for (pos, value) in range.eq.iter().enumerate() {
        encode_at(&mut base, pos, n, value)?;
    }
    let at = |v: &LiveValue| -> Result<Vec<u8>, LiveError> {
        let mut key = base.clone();
        encode_at(&mut key, next, n, v)?;
        Ok(key)
    };
    bounded(&base, &range.lower, &range.upper, at)
}

/// Appends the encoding of the value at field position `pos` of an index
/// with `n` user fields: the tuple element for a user field, 8 bytes
/// big-endian for `_creationTime`.
fn encode_at(key: &mut Vec<u8>, pos: usize, n: usize, value: &LiveValue) -> Result<(), LiveError> {
    if pos < n {
        tuple::encode(key, &value.index_elem()?);
        return Ok(());
    }
    match value {
        LiveValue::I64(ms) if *ms >= 0 => {
            key.extend_from_slice(&(*ms as u64).to_be_bytes());
            Ok(())
        }
        other => Err(LiveError::invalid(format!(
            "_creationTime is compared with a non-negative int64, not {}",
            other.type_name()
        ))),
    }
}

fn bounded(
    base: &[u8],
    lower: &Bound<LiveValue>,
    upper: &Bound<LiveValue>,
    at: impl Fn(&LiveValue) -> Result<Vec<u8>, LiveError>,
) -> Result<KeyRange, LiveError> {
    let lo = match lower {
        Bound::Unbounded => base.to_vec(),
        Bound::Included(v) => at(v)?,
        Bound::Excluded(v) => tuple::successor(&at(v)?),
    };
    let hi = match upper {
        Bound::Unbounded => tuple::successor(base),
        Bound::Included(v) => tuple::successor(&at(v)?),
        Bound::Excluded(v) => at(v)?,
    };
    Ok(KeyRange { lo, hi })
}

/// Every index entry key of `doc` in `table`: `by_creation_time`, then each
/// user index in definition order. The tuple part of an entry above
/// `limits.max_index_key_bytes` is refused.
pub fn index_keys(
    app: &AppKeys,
    table: &TableDef,
    doc: &Doc,
    limits: &Limits,
) -> Result<Vec<Vec<u8>>, LiveError> {
    let mut keys = Vec::with_capacity(1 + table.indexes.len());
    keys.push(app.index_entry(IndexId::BY_CREATION_TIME, &[], doc.creation_ms, &doc.id));
    let null = LiveValue::Null;
    for index in &table.indexes {
        let values = index
            .fields
            .iter()
            .map(|f| {
                doc.fields
                    .get(f)
                    .unwrap_or(&null)
                    .index_elem()
                    .map_err(|e| {
                        LiveError::invalid(format!("index '{}', field '{f}': {e}", index.name))
                    })
            })
            .collect::<Result<Vec<Elem<'_>>, _>>()?;
        let key = app.index_entry(index.id, &values, doc.creation_ms, &doc.id);
        let tuple_len = key.len() - app.index_prefix(table.id, index.id).len() - 8 - 16;
        if tuple_len > limits.max_index_key_bytes {
            return Err(LiveError::limit(
                "max_index_key_bytes",
                format!(
                    "index '{}': the indexed values encode to {tuple_len} bytes, more than {}",
                    index.name, limits.max_index_key_bytes
                ),
            ));
        }
        keys.push(key);
    }
    Ok(keys)
}

async fn existing(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    id: DocId,
) -> Result<Doc, LiveError> {
    check_table(table, &id)?;
    get(txn, app, id)
        .await?
        .ok_or_else(|| LiveError::NotFound(format!("document {id} in table '{}'", table.name)))
}

async fn rewrite(
    txn: &mut Txn,
    app: &AppKeys,
    table: &TableDef,
    old: Doc,
    fields: BTreeMap<String, LiveValue>,
    limits: &Limits,
) -> Result<WriteRecord, LiveError> {
    limits.check_fields(&fields)?;
    let removed = index_keys(app, table, &old, limits)?;
    let new = Doc { fields, ..old };
    let record = encode(&new, limits)?;
    let added = index_keys(app, table, &new, limits)?;
    txn.put(&app.document(&new.id), record).await?;
    let keep: BTreeSet<&Vec<u8>> = removed.iter().filter(|k| added.contains(k)).collect();
    for key in removed.iter().filter(|k| !keep.contains(k)) {
        txn.delete(key).await?;
    }
    for key in added.iter().filter(|k| !keep.contains(k)) {
        txn.put(key, Vec::new()).await?;
    }
    Ok(write_record(
        &new.id,
        pb::WriteKind::WRITE_KIND_REPLACE,
        removed,
        added,
    ))
}

fn check_table(table: &TableDef, id: &DocId) -> Result<(), LiveError> {
    if id.table != table.id {
        return Err(LiveError::invalid(format!(
            "document {id} belongs to table {}, not to '{}' ({})",
            id.table.0, table.name, table.id.0
        )));
    }
    Ok(())
}

fn encode(doc: &Doc, limits: &Limits) -> Result<Vec<u8>, LiveError> {
    let bytes = doc.to_record().encode_to_vec();
    limits.check_document_bytes(bytes.len())?;
    Ok(bytes)
}

fn write_record(
    id: &DocId,
    kind: pb::WriteKind,
    removed: Vec<Vec<u8>>,
    added: Vec<Vec<u8>>,
) -> WriteRecord {
    WriteRecord {
        table_id: id.table.0,
        doc_id: id.bytes.to_vec(),
        kind: kind.into(),
        index_keys_removed: removed,
        index_keys_added: added,
        ..Default::default()
    }
}
