//! The Arrow schemas the operators exchange (plan M1.2 Task 5): ranked
//! hits and row-id sets.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, BinaryArray, Float32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use operon_collection::PrimaryKey;
use serde::{Deserialize, Serialize};

use crate::error::ServiceError;
use crate::ir::SortValue;

pub const ROWID_COLUMN: &str = "_rowid";
pub const PK_COLUMN: &str = "_pk";
pub const SCORE_COLUMN: &str = "_score";
pub const SORT_COLUMN: &str = "_sort";

static RANKED: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(ROWID_COLUMN, DataType::UInt64, false),
        Field::new(PK_COLUMN, DataType::Binary, false),
        Field::new(SCORE_COLUMN, DataType::Float32, false),
        Field::new(SORT_COLUMN, DataType::Binary, false),
    ]))
});

static ROWIDS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        ROWID_COLUMN,
        DataType::UInt64,
        false,
    )]))
});

/// `_rowid UInt64, _pk Binary (canonical), _score Float32, _sort Binary
/// (postcard Vec<SortValue>)`; all non-null.
pub fn ranked_schema() -> SchemaRef {
    RANKED.clone()
}

/// `_rowid UInt64`, ascending.
pub fn rowid_schema() -> SchemaRef {
    ROWIDS.clone()
}

/// A ranked hit: its row id, key, score and one value per `Field` key of
/// the effective sort.
#[derive(Clone, Debug, PartialEq)]
pub struct Ranked {
    pub row_id: u64,
    pub pk: PrimaryKey,
    pub score: f32,
    pub sort: Vec<SortValue>,
}

/// The postcard form of a [`SortValue`] (its serde is the JSON form).
#[derive(Serialize, Deserialize)]
enum SortWire {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(String),
    Uuid([u8; 16]),
}

fn to_wire(value: &SortValue) -> SortWire {
    match value {
        SortValue::Null => SortWire::Null,
        SortValue::Bool(b) => SortWire::Bool(*b),
        SortValue::I64(n) => SortWire::I64(*n),
        SortValue::U64(n) => SortWire::U64(*n),
        SortValue::F64(x) => SortWire::F64(*x),
        SortValue::Str(s) => SortWire::Str(s.clone()),
        SortValue::Uuid(u) => SortWire::Uuid(*u),
    }
}

fn from_wire(value: SortWire) -> SortValue {
    match value {
        SortWire::Null => SortValue::Null,
        SortWire::Bool(b) => SortValue::Bool(b),
        SortWire::I64(n) => SortValue::I64(n),
        SortWire::U64(n) => SortValue::U64(n),
        SortWire::F64(x) => SortValue::F64(x),
        SortWire::Str(s) => SortValue::Str(s),
        SortWire::Uuid(u) => SortValue::Uuid(u),
    }
}

/// `rows` as one batch of [`ranked_schema`].
pub fn ranked_to_batch(rows: &[Ranked]) -> RecordBatch {
    let row_ids = UInt64Array::from_iter_values(rows.iter().map(|r| r.row_id));
    let pks: Vec<Vec<u8>> = rows.iter().map(|r| r.pk.canonical()).collect();
    let pks = BinaryArray::from_iter_values(pks.iter().map(Vec::as_slice));
    let scores = Float32Array::from_iter_values(rows.iter().map(|r| r.score));
    let sorts: Vec<Vec<u8>> = rows
        .iter()
        .map(|r| {
            let wire: Vec<SortWire> = r.sort.iter().map(to_wire).collect();
            postcard::to_allocvec(&wire).expect("sort values encode")
        })
        .collect();
    let sorts = BinaryArray::from_iter_values(sorts.iter().map(Vec::as_slice));
    RecordBatch::try_new(
        ranked_schema(),
        vec![
            Arc::new(row_ids),
            Arc::new(pks),
            Arc::new(scores),
            Arc::new(sorts),
        ],
    )
    .expect("the ranked columns match their schema")
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, ServiceError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| ServiceError::Internal(format!("a ranked batch lacks column {name}")))
}

/// The hits of a batch of [`ranked_schema`].
pub fn batch_to_ranked(batch: &RecordBatch) -> Result<Vec<Ranked>, ServiceError> {
    let row_ids: &UInt64Array = column(batch, ROWID_COLUMN)?;
    let pks: &BinaryArray = column(batch, PK_COLUMN)?;
    let scores: &Float32Array = column(batch, SCORE_COLUMN)?;
    let sorts: &BinaryArray = column(batch, SORT_COLUMN)?;
    (0..batch.num_rows())
        .map(|i| {
            if pks.is_null(i) || sorts.is_null(i) {
                return Err(ServiceError::Internal(
                    "a null in a ranked batch".to_string(),
                ));
            }
            let pk = PrimaryKey::from_canonical(pks.value(i))
                .map_err(|err| ServiceError::Internal(format!("ranked _pk: {err}")))?;
            let wire: Vec<SortWire> = postcard::from_bytes(sorts.value(i))
                .map_err(|err| ServiceError::Internal(format!("ranked _sort: {err}")))?;
            Ok(Ranked {
                row_id: row_ids.value(i),
                pk,
                score: scores.value(i),
                sort: wire.into_iter().map(from_wire).collect(),
            })
        })
        .collect()
}
