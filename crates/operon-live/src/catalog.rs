//! Tables and their indexes (design §20 §4.1–§4.2): [`TableDef`],
//! [`IndexDef`], their catalog records and the catalog operations.
//!
//! A table is created on its first insert unless a schema is deployed
//! ([`table_for_insert`]); [`define_table`] creates a table or changes its
//! indexes, and refuses an index change on a non-empty table (R1 plan
//! Ruling 5). Table ids come from the catalog counter, from 1; index ids from
//! the table's `next_index_id`, from 2, and are never reused.

use buffa::Message;
use operon_tikv::Txn;

use crate::docs::Reads;
use crate::ids::{IndexId, TableId};
use crate::journal::MAX_SHARDS;
use crate::keys::AppKeys;
use crate::{Limits, LiveError, pb};

/// The longest table or index name, in bytes.
pub const MAX_NAME_BYTES: usize = 64;

/// The name of the built-in index on the document key.
pub const BY_ID: &str = "by_id";
/// The name of the built-in index on `_creationTime`, then `_id`.
pub const BY_CREATION_TIME: &str = "by_creation_time";
/// The system field of a document's id.
pub const ID_FIELD: &str = "_id";
/// The system field of a document's creation time.
pub const CREATION_TIME_FIELD: &str = "_creationTime";

/// A table and its user indexes. `by_id` and `by_creation_time` are implicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub indexes: Vec<IndexDef>,
    /// The id the next user index gets.
    pub next_index_id: u32,
}

/// A user index: up to 16 top-level fields, then `_creationTime` and `_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub fields: Vec<String>,
}

/// A user index as a schema declares it: a name and its fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    pub fields: Vec<String>,
}

impl TableDef {
    /// The index of this id: `None` for `by_id` (it has no entries) and for
    /// an unknown id; the built-in `by_creation_time` has no fields.
    pub fn index_fields(&self, id: IndexId) -> Option<&[String]> {
        if id == IndexId::BY_CREATION_TIME {
            return Some(&[]);
        }
        self.indexes
            .iter()
            .find(|i| i.id == id)
            .map(|i| i.fields.as_slice())
    }

    /// The id of the index named `name`, built-ins included.
    pub fn index_id(&self, name: &str) -> Option<IndexId> {
        match name {
            BY_ID => Some(IndexId::BY_ID),
            BY_CREATION_TIME => Some(IndexId::BY_CREATION_TIME),
            _ => self.indexes.iter().find(|i| i.name == name).map(|i| i.id),
        }
    }

    /// The catalog record.
    pub fn to_proto(&self) -> pb::TableDef {
        pb::TableDef {
            format: 1,
            id: self.id.0,
            name: self.name.clone(),
            indexes: self
                .indexes
                .iter()
                .map(|i| pb::IndexDef {
                    id: i.id.0,
                    name: i.name.clone(),
                    fields: i.fields.clone(),
                    ..Default::default()
                })
                .collect(),
            next_index_id: self.next_index_id,
            ..Default::default()
        }
    }

    /// The table of a catalog record.
    pub fn from_proto(p: pb::TableDef) -> Result<Self, LiveError> {
        if p.format != 1 {
            return Err(LiveError::Corrupt(format!(
                "table record format {} (expected 1)",
                p.format
            )));
        }
        Ok(TableDef {
            id: TableId(p.id),
            name: p.name,
            indexes: p
                .indexes
                .into_iter()
                .map(|i| IndexDef {
                    id: IndexId(i.id),
                    name: i.name,
                    fields: i.fields,
                })
                .collect(),
            next_index_id: p.next_index_id,
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self, LiveError> {
        let p = pb::TableDef::decode_from_slice(bytes)
            .map_err(|e| LiveError::Corrupt(format!("table record: {e}")))?;
        TableDef::from_proto(p)
    }
}

/// Checks a table or index name: 1 to 64 bytes of ASCII letters, digits and
/// `_`, starting with a letter (names starting with `_` are reserved).
pub fn check_name(what: &str, name: &str) -> Result<(), LiveError> {
    let mut chars = name.chars();
    let ok = name.len() <= MAX_NAME_BYTES
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(LiveError::invalid(format!(
            "{what} name '{name}': use 1 to {MAX_NAME_BYTES} ASCII letters, digits and '_', \
             starting with a letter"
        )))
    }
}

/// The table named `name`.
pub async fn load_table(
    r: &mut impl Reads,
    app: &AppKeys,
    name: &str,
) -> Result<Option<TableDef>, LiveError> {
    let Some(id) = r.get(&app.table_name(name)).await? else {
        return Ok(None);
    };
    let id = decode_u32(&id, "table name record")?;
    let table = load_table_by_id(r, app, TableId(id)).await?;
    match table {
        Some(t) if t.name == name => Ok(Some(t)),
        _ => Err(LiveError::Corrupt(format!(
            "table name '{name}' points at table {id}, which does not name it back"
        ))),
    }
}

/// The table with this id.
pub async fn load_table_by_id(
    r: &mut impl Reads,
    app: &AppKeys,
    id: TableId,
) -> Result<Option<TableDef>, LiveError> {
    match r.get(&app.table(id)).await? {
        None => Ok(None),
        Some(bytes) => TableDef::decode(&bytes).map(Some),
    }
}

/// Every table, by id.
pub async fn list_tables(r: &mut impl Reads, app: &AppKeys) -> Result<Vec<TableDef>, LiveError> {
    r.scan(&app.tables(), usize::MAX, false)
        .await?
        .into_iter()
        .map(|(_, v)| TableDef::decode(&v))
        .collect()
}

/// The table an insert into `name` writes to: the existing table, or a new
/// one without user indexes when no schema is deployed (§20 §4.1). With a
/// deployed schema, a missing table is [`LiveError::NotFound`].
pub async fn table_for_insert(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    limits: &Limits,
) -> Result<TableDef, LiveError> {
    check_name("table", name)?;
    if let Some(table) = load_table(txn, app, name).await? {
        return Ok(table);
    }
    if txn.get(&app.schema()).await?.is_some() {
        return Err(LiveError::NotFound(format!(
            "table '{name}' is not in the deployed schema"
        )));
    }
    create_table(txn, app, name, &[], limits).await
}

/// Creates the table `name` with `indexes`, or changes an existing table's
/// indexes to `indexes` (matched by name; an index whose fields change is
/// replaced by a new id). An index change on a table that holds a document
/// is [`LiveError::FailedPrecondition`] (R1 plan Ruling 5); a table whose
/// indexes already match is returned unchanged.
pub async fn define_table(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    indexes: &[IndexSpec],
    limits: &Limits,
) -> Result<TableDef, LiveError> {
    check_name("table", name)?;
    check_indexes(indexes, limits)?;
    let Some(mut table) = load_table(txn, app, name).await? else {
        return create_table(txn, app, name, indexes, limits).await;
    };
    let same = table.indexes.len() == indexes.len()
        && table
            .indexes
            .iter()
            .zip(indexes)
            .all(|(have, want)| have.name == want.name && have.fields == want.fields);
    if same {
        return Ok(table);
    }
    let (lo, hi) = {
        let docs = app.documents(table.id);
        (docs.lo, docs.hi)
    };
    let any = txn.scan(&lo, Some(&hi), 1).await?;
    if !any.is_empty() {
        return Err(LiveError::FailedPrecondition(format!(
            "index changes need an empty table in R1: table '{name}' has documents"
        )));
    }
    let mut next = table.next_index_id.max(IndexId::FIRST_USER);
    let mut defs = Vec::with_capacity(indexes.len());
    for spec in indexes {
        let kept = table
            .indexes
            .iter()
            .find(|i| i.name == spec.name && i.fields == spec.fields);
        let id = match kept {
            Some(i) => i.id,
            None => {
                next += 1;
                IndexId(next - 1)
            }
        };
        defs.push(IndexDef {
            id,
            name: spec.name.clone(),
            fields: spec.fields.clone(),
        });
    }
    table.indexes = defs;
    table.next_index_id = next;
    txn.put(&app.table(table.id), table.to_proto().encode_to_vec())
        .await?;
    Ok(table)
}

async fn create_table(
    txn: &mut Txn,
    app: &AppKeys,
    name: &str,
    indexes: &[IndexSpec],
    limits: &Limits,
) -> Result<TableDef, LiveError> {
    check_indexes(indexes, limits)?;
    let counter = app.table_counter();
    let id = match txn.get(&counter).await? {
        None => 1,
        Some(bytes) => decode_u32(&bytes, "table counter")?,
    };
    let next = id
        .checked_add(1)
        .ok_or_else(|| LiveError::limit("tables", "the app has 2^32 − 1 tables"))?;
    let mut index_id = IndexId::FIRST_USER;
    let table = TableDef {
        id: TableId(id),
        name: name.to_string(),
        indexes: indexes
            .iter()
            .map(|spec| {
                index_id += 1;
                IndexDef {
                    id: IndexId(index_id - 1),
                    name: spec.name.clone(),
                    fields: spec.fields.clone(),
                }
            })
            .collect(),
        next_index_id: index_id,
    };
    txn.put(&counter, next.to_be_bytes().to_vec()).await?;
    txn.put(&app.table_name(name), id.to_be_bytes().to_vec())
        .await?;
    txn.put(&app.table(table.id), table.to_proto().encode_to_vec())
        .await?;
    Ok(table)
}

fn check_indexes(indexes: &[IndexSpec], limits: &Limits) -> Result<(), LiveError> {
    if indexes.len() > limits.max_indexes {
        return Err(LiveError::limit(
            "max_indexes",
            format!(
                "{} indexes, more than {}",
                indexes.len(),
                limits.max_indexes
            ),
        ));
    }
    for (n, spec) in indexes.iter().enumerate() {
        check_name("index", &spec.name)?;
        if spec.name == BY_ID || spec.name == BY_CREATION_TIME {
            return Err(LiveError::invalid(format!(
                "index name '{}' is a built-in index",
                spec.name
            )));
        }
        if indexes[..n].iter().any(|other| other.name == spec.name) {
            return Err(LiveError::invalid(format!(
                "index '{}' is defined twice",
                spec.name
            )));
        }
        if spec.fields.is_empty() {
            return Err(LiveError::invalid(format!(
                "index '{}' has no fields",
                spec.name
            )));
        }
        if spec.fields.len() > limits.max_index_fields {
            return Err(LiveError::limit(
                "max_index_fields",
                format!(
                    "index '{}' has {} fields, more than {}",
                    spec.name,
                    spec.fields.len(),
                    limits.max_index_fields
                ),
            ));
        }
        for field in &spec.fields {
            if field.is_empty() || field.starts_with('_') {
                return Err(LiveError::invalid(format!(
                    "index '{}': field '{field}' cannot be indexed (system fields are appended \
                     to every index)",
                    spec.name
                )));
            }
            // Documents cannot hold a longer name (Limits::check_fields), so
            // such an index would hold only nulls.
            if field.len() > limits.max_field_name_bytes {
                return Err(LiveError::limit(
                    "max_field_name_bytes",
                    format!(
                        "index '{}': a field name has {} bytes, more than {}",
                        spec.name,
                        field.len(),
                        limits.max_field_name_bytes
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The app's journal shard count, from its settings record; `None` before
/// the app's runner first opened (R1 plan row T10-1).
pub async fn load_journal_shards(
    r: &mut impl Reads,
    app: &AppKeys,
) -> Result<Option<u16>, LiveError> {
    let Some(bytes) = r.get(&app.app_def()).await? else {
        return Ok(None);
    };
    let def = pb::AppDef::decode_from_slice(&bytes)
        .map_err(|e| LiveError::Corrupt(format!("app record: {e}")))?;
    if def.format != 1 {
        return Err(LiveError::Corrupt(format!(
            "app record format {} (expected 1)",
            def.format
        )));
    }
    let shards = u16::try_from(def.journal_shards)
        .ok()
        .filter(|&n| (1..=MAX_SHARDS).contains(&n))
        .ok_or_else(|| {
            LiveError::Corrupt(format!(
                "app record: {} journal shards (1 to {MAX_SHARDS})",
                def.journal_shards
            ))
        })?;
    Ok(Some(shards))
}

/// The app's journal shard count: the stored one, or `shards` written as
/// the app's setting when it has none yet.
pub async fn ensure_journal_shards(
    txn: &mut Txn,
    app: &AppKeys,
    shards: u16,
) -> Result<u16, LiveError> {
    if let Some(stored) = load_journal_shards(txn, app).await? {
        return Ok(stored);
    }
    check_shards(shards)?;
    txn.put(&app.app_def(), app_def(shards)).await?;
    Ok(shards)
}

/// Changes the app's journal shard count to `shards`. Refused with
/// [`LiveError::FailedPrecondition`] unless the journal is empty (no head,
/// entry or checkpoint was ever written; R2 migrates a non-empty one). The
/// transaction also locks every head of the current count, so a mutation
/// that read the old count and commits concurrently conflicts with it.
pub async fn set_journal_shards(
    txn: &mut Txn,
    app: &AppKeys,
    shards: u16,
) -> Result<(), LiveError> {
    check_shards(shards)?;
    let current = load_journal_shards(txn, app).await?;
    if current == Some(shards) {
        return Ok(());
    }
    let journal = app.journal();
    let (lo, hi) = journal.bounds();
    if !txn.scan(lo, hi, 1).await?.is_empty() {
        return Err(LiveError::FailedPrecondition(format!(
            "the journal shard count can change only while the journal is empty in R1: \
             it has {} shards and entries",
            current.unwrap_or(0)
        )));
    }
    if let Some(current) = current {
        txn.lock_keys((0..current).map(|s| app.journal_head(s)))
            .await?;
    }
    txn.put(&app.app_def(), app_def(shards)).await?;
    Ok(())
}

fn check_shards(shards: u16) -> Result<(), LiveError> {
    if shards == 0 || shards > MAX_SHARDS {
        return Err(LiveError::invalid(format!(
            "a journal has 1 to {MAX_SHARDS} shards, not {shards}"
        )));
    }
    Ok(())
}

fn app_def(shards: u16) -> Vec<u8> {
    pb::AppDef {
        format: 1,
        journal_shards: u32::from(shards),
        ..Default::default()
    }
    .encode_to_vec()
}

fn decode_u32(bytes: &[u8], what: &str) -> Result<u32, LiveError> {
    let raw: [u8; 4] = bytes
        .try_into()
        .map_err(|_| LiveError::Corrupt(format!("{what}: {} bytes, expected 4", bytes.len())))?;
    Ok(u32::from_be_bytes(raw))
}
