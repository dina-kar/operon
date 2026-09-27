//! One slice (D1 Task 8 semantics 3 and 4): read it with conditional reads,
//! apply the mapping, and write it through the sink in chunks.
//!
//! **Mapping** (O3, O5, T1-8, X4, X5). Renames run first: every
//! `mapping.columns` entry, and `id_column` → `_id`. The result must hold
//! each name once; a rename onto a column of the file that is not itself
//! renamed fails the file with `mapping_conflict` (two entries with one
//! target are refused before submit). A missing `id_column` fails the file
//! with `id_column_missing`. Then, when the slice has no `_id` column (and
//! no `id_column` was given), an `_id` column of Ruling 7's UUIDs is added:
//! `Utf8`, `operon-id-type = uuid`. A file's own `_id` column is the id (O5).
//! A null `_id` is the mapper's row error, never replaced by a generated id.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use object_store::path::Path;
use object_store::{GetOptions, GetRange};
use operon_store::Store;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    Attempt, FileArgs, Format, IdType, ImportEnv, Mapping, PlannedFile, SinkError, SliceValue,
    retrying,
};
use crate::ids::OperationId;

/// The primary key column.
pub(super) const ID_COLUMN: &str = "_id";
/// The `_id` field metadata naming its id type (M1.2's `ID_TYPE_METADATA`).
pub(super) const ID_TYPE_METADATA: &str = "operon-id-type";

/// The argument of a slice step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceArgs {
    pub file: FileArgs,
    pub slice: u64,
}

/// A batch after the mapping, ready for the sink.
#[derive(Debug, Clone)]
pub struct MappedSlice {
    pub batch: RecordBatch,
    /// The id type of a text `_id` without metadata.
    pub id_type: Option<IdType>,
}

/// Whose rows a generated `_id` belongs to (Ruling 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdSeed<'a> {
    /// File `file` (its index in the plan) of operation `op` (T8-5).
    File { op: &'a OperationId, file: usize },
    /// File `key` of schedule `schedule` (D1 Task 9): every run of the
    /// schedule, and every version of the file, derives the same ids, so a
    /// changed file overwrites its rows by position instead of adding a copy.
    Scheduled { schedule: &'a str, key: &'a str },
}

impl IdSeed<'_> {
    /// The id of row `row` of slice `slice`.
    pub fn id(&self, slice: u64, row: u64) -> String {
        match self {
            Self::File { op, file } => derived_id(op, *file, slice, row),
            Self::Scheduled { schedule, key } => scheduled_id(schedule, key, slice, row),
        }
    }
}

/// Ruling 7's id for row `row` of slice `slice` of file `file` of operation
/// `op`: SHA-256 of `op ‖ 0 ‖ file ‖ slice ‖ row` (big-endian u64s),
/// truncated to 16 bytes as a UUIDv8. A re-run slice derives the same ids.
pub fn derived_id(op: &OperationId, file: usize, slice: u64, row: u64) -> String {
    seeded_id(op.as_str().as_bytes(), file as u64, slice, row)
}

/// The id of row `row` of slice `slice` of file `key` of schedule
/// `schedule` (D1 Task 9): SHA-256 of `schedule ‖ 0 ‖ key ‖ 0 ‖ 0 ‖ slice ‖
/// row`, as [`derived_id`] does it. A schedule id starts with `isched-` and
/// an operation id with `op-`, so the two never share a seed.
pub fn scheduled_id(schedule: &str, key: &str, slice: u64, row: u64) -> String {
    let mut seed = Vec::with_capacity(schedule.len() + 1 + key.len());
    seed.extend_from_slice(schedule.as_bytes());
    seed.push(0);
    seed.extend_from_slice(key.as_bytes());
    seeded_id(&seed, 0, slice, row)
}

fn seeded_id(seed: &[u8], file: u64, slice: u64, row: u64) -> String {
    let mut hash = Sha256::new();
    hash.update(seed);
    hash.update([0u8]);
    hash.update(file.to_be_bytes());
    hash.update(slice.to_be_bytes());
    hash.update(row.to_be_bytes());
    let digest = hash.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x80; // version 8
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// `batch` under `mapping`, the rows numbered from `first_row` within slice
/// `slice` of the file `seed` names; the error is `(code, message)`.
pub fn map_slice(
    batch: &RecordBatch,
    mapping: Option<&Mapping>,
    seed: IdSeed<'_>,
    slice: u64,
    first_row: u64,
) -> Result<MappedSlice, (String, String)> {
    let default = Mapping::default();
    let mapping = mapping.unwrap_or(&default);
    let schema = batch.schema();
    let mut renames: BTreeMap<&str, &str> = mapping
        .columns
        .iter()
        .map(|(s, t)| (s.as_str(), t.as_str()))
        .collect();
    if let Some(id_column) = &mapping.id_column {
        if schema.field_with_name(id_column).is_err() {
            return Err((
                "id_column_missing".into(),
                format!("the file has no column {id_column:?} (mapping.id_column)"),
            ));
        }
        renames.insert(id_column.as_str(), ID_COLUMN);
    }
    let mut seen = BTreeSet::new();
    let mut fields = Vec::with_capacity(schema.fields().len() + 1);
    for field in schema.fields() {
        let name = renames
            .get(field.name().as_str())
            .copied()
            .unwrap_or(field.name().as_str());
        if !seen.insert(name.to_string()) {
            return Err((
                "mapping_conflict".into(),
                format!(
                    "after the renames the file has two columns named {name:?}; rename the \
                     file's own {name:?} column too, or map to another name"
                ),
            ));
        }
        fields.push(
            Field::new(name, field.data_type().clone(), field.is_nullable())
                .with_metadata(field.metadata().clone()),
        );
    }
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let id_type = if seen.contains(ID_COLUMN) {
        mapping.id_type
    } else {
        // No `_id` and no `id_column` (O5): Ruling 7's ids.
        let ids: StringArray = (0..batch.num_rows() as u64)
            .map(|row| Some(seed.id(slice, first_row + row)))
            .collect();
        fields.push(
            Field::new(ID_COLUMN, DataType::Utf8, false).with_metadata(
                [(ID_TYPE_METADATA.to_string(), "uuid".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        columns.push(Arc::new(ids));
        Some(IdType::Uuid)
    };
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| ("internal".to_string(), format!("mapping a batch: {e}")))?;
    Ok(MappedSlice { batch, id_type })
}

/// Read `range` of `file` on `store`, only if it still has the planned etag
/// (`If-Match`, T0-5).
pub(super) async fn read_range(
    store: &Store,
    file: &PlannedFile,
    range: Range<u64>,
) -> Result<Bytes, Attempt> {
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    let path = Path::parse(&file.key).map_err(|e| Attempt::Fail {
        code: "invalid_argument".into(),
        message: format!("{}: {e}", file.key),
    })?;
    let options = GetOptions {
        if_match: file.etag.clone(),
        range: Some(GetRange::Bounded(range)),
        ..GetOptions::default()
    };
    let result = store
        .inner()
        .get_opts(&path, options)
        .await
        .map_err(|e| classify(&file.key, e))?;
    result.bytes().await.map_err(|e| classify(&file.key, e))
}

/// A store error as an attempt: a changed or vanished file fails its
/// branch; anything else is retried.
fn classify(key: &str, e: object_store::Error) -> Attempt {
    match e {
        object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. } => {
            Attempt::Fail {
                code: "file_changed".into(),
                message: format!("{key} changed after the import was planned"),
            }
        }
        object_store::Error::NotFound { .. } => Attempt::Fail {
            code: "file_changed".into(),
            message: format!("{key} was deleted after the import was planned"),
        },
        other => Attempt::Retry {
            message: format!("reading {key}: {other}"),
            after_ms: 0,
        },
    }
}

/// Run slice `args.slice` of `args.file`: read, map, write in chunks.
pub(super) async fn run(env: &ImportEnv, args: &SliceArgs) -> Result<SliceValue, (String, String)> {
    let file = &args.file;
    let store = env
        .sources
        .open(&file.source)
        .map_err(|e| ("source_unavailable".to_string(), e))?;
    let what = format!("{} slice {}", file.file.key, args.slice);
    let (batches, bytes) = retrying(env, &what, || async {
        match file.format {
            Format::Parquet => {
                super::parquet::read_slice(env, &store, &file.file, args.slice).await
            }
            Format::Ndjson => super::ndjson::read_slice(env, &store, &file.file, args.slice).await,
        }
    })
    .await
    .map_err(|(code, message)| (code, format!("{}: {message}", file.file.key)))?;
    let mut value = SliceValue {
        bytes,
        ..SliceValue::default()
    };
    let mut tokens = Vec::new();
    let mut first_row = 0u64;
    for batch in batches {
        let rows = batch.num_rows();
        for start in (0..rows).step_by(env.config.chunk_rows.max(1)) {
            let len = env.config.chunk_rows.min(rows - start);
            let chunk = batch.slice(start, len);
            let mapped = map_slice(
                &chunk,
                file.mapping.as_ref(),
                file.seed(),
                args.slice,
                first_row,
            )
            .map_err(|(code, message)| (code, format!("{}: {message}", file.file.key)))?;
            first_row += len as u64;
            let written = retrying(env, &what, || {
                let batch = mapped.batch.clone();
                async move {
                    env.sink
                        .write(&file.namespace, &file.collection, batch, mapped.id_type)
                        .await
                        .map_err(|e| match e {
                            SinkError::Retry { message, after_ms } => {
                                Attempt::Retry { message, after_ms }
                            }
                            SinkError::NotFound(message) => Attempt::Fail {
                                code: "not_found".into(),
                                message,
                            },
                            SinkError::Failed { code, message } => Attempt::Fail { code, message },
                        })
                }
            })
            .await
            .map_err(|(code, message)| (code, format!("{what}: {message}")))?;
            value.rows += written.rows;
            if !written.token.is_empty() {
                tokens.push(written.token);
            }
        }
    }
    value.token = env.sink.merge_tokens(&tokens);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, StringArray};

    use super::*;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).expect("batch")
    }

    fn op() -> OperationId {
        OperationId::for_key("default", "k")
    }

    #[test]
    fn derived_ids_are_deterministic_v8_uuids() {
        let a = derived_id(&op(), 1, 2, 3);
        assert_eq!(a, derived_id(&op(), 1, 2, 3));
        assert_ne!(a, derived_id(&op(), 1, 2, 4));
        assert_ne!(a, derived_id(&op(), 2, 2, 3));
        assert_eq!(a.len(), 36);
        assert_eq!(&a[14..15], "8", "{a}");
        assert!(matches!(&a[19..20], "8" | "9" | "a" | "b"), "{a}");
    }

    #[test]
    fn scheduled_ids_follow_the_key_not_the_run() {
        let a = scheduled_id("isched-1", "a.ndjson", 0, 3);
        assert_eq!(a, scheduled_id("isched-1", "a.ndjson", 0, 3));
        assert_eq!(
            a,
            IdSeed::Scheduled { schedule: "isched-1", key: "a.ndjson" }.id(0, 3)
        );
        assert_ne!(a, scheduled_id("isched-1", "b.ndjson", 0, 3));
        assert_ne!(a, scheduled_id("isched-2", "a.ndjson", 0, 3));
        assert_ne!(a, scheduled_id("isched-1", "a.ndjson", 1, 3));
        assert_eq!(&a[14..15], "8", "{a}");
    }

    #[test]
    fn renames_run_first_and_ids_follow_the_rules() {
        let text: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let n: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        // No _id: generated, uuid.
        let m = map_slice(
            &batch(vec![("t", text.clone())]),
            None,
            IdSeed::File { op: &op(), file: 0 },
            0,
            0,
        ).expect("map");
        assert_eq!(m.id_type, Some(IdType::Uuid));
        assert_eq!(m.batch.schema().field(1).name(), "_id");
        // The file's own _id (O5): kept, nothing generated.
        let m = map_slice(
            &batch(vec![("_id", n.clone()), ("t", text.clone())]),
            None,
            IdSeed::File { op: &op(), file: 0 },
            0,
            0,
        )
        .expect("map");
        assert_eq!(m.batch.num_columns(), 2);
        // id_column.
        let mapping = Mapping {
            id_column: Some("key".into()),
            id_type: Some(IdType::Str),
            ..Mapping::default()
        };
        let m = map_slice(
            &batch(vec![("key", text.clone())]),
            Some(&mapping),
            IdSeed::File { op: &op(), file: 0 },
            0,
            0,
        )
        .expect("map");
        assert_eq!(m.batch.schema().field(0).name(), "_id");
        assert_eq!(m.id_type, Some(IdType::Str));
        let err = map_slice(
            &batch(vec![("t", text.clone())]),
            Some(&mapping),
            IdSeed::File { op: &op(), file: 0 },
            0,
            0,
        )
        .expect_err("missing");
        assert_eq!(err.0, "id_column_missing");
        // A rename onto an unrenamed column.
        let mapping = Mapping {
            columns: [("t".to_string(), "u".to_string())].into_iter().collect(),
            ..Mapping::default()
        };
        let err = map_slice(
            &batch(vec![("t", text.clone()), ("u", text)]),
            Some(&mapping),
            IdSeed::File { op: &op(), file: 0 },
            0,
            0,
        )
        .expect_err("conflict");
        assert_eq!(err.0, "mapping_conflict");
    }
}
