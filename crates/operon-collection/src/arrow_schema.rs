//! The Lance dataset's Arrow schema (plan M1.1 Rulings 4, 5 and 27): four
//! system columns, then one column per dense vector and one per sparse
//! vector, added lazily. Typed fields are not Lance columns; they live in
//! Tantivy and are re-derived from `_source`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder, UInt32Builder};
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, ListArray, RecordBatch,
    StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use serde_json::{Map, Value};

use crate::doc::{Document, SparseVector};
use crate::error::CollectionError;
use crate::pk::PrimaryKey;
use crate::schema::{CollectionSchema, VectorSpec};

/// The document's canonical primary-key bytes.
pub const PK_COLUMN: &str = "_pk";
/// `serde_json::to_vec` of the document's source.
pub const SOURCE_COLUMN: &str = "_source";
/// The partition of the record that last wrote the document.
pub const INGEST_PARTITION_COLUMN: &str = "_ingest_partition";
/// The offset of the record that last wrote the document.
pub const INGEST_OFFSET_COLUMN: &str = "_ingest_offset";

/// The column of the dense vector at `index` in `CollectionSchema.vectors`.
pub fn vector_column(index: usize) -> String {
    format!("_vector_{index}")
}

/// The column of the sparse vector at `index` in
/// `CollectionSchema.sparse_vectors`.
pub fn sparse_column(index: usize) -> String {
    format!("_sparse_{index}")
}

/// The four system columns, in this order: version 1 of every dataset.
pub fn base_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new(PK_COLUMN, DataType::Binary, false),
        Field::new(SOURCE_COLUMN, DataType::Binary, false),
        Field::new(INGEST_PARTITION_COLUMN, DataType::UInt32, false),
        Field::new(INGEST_OFFSET_COLUMN, DataType::UInt64, false),
    ])
}

/// The full schema for `schema`: the system columns, then its dense vectors,
/// then its sparse vectors.
pub fn arrow_schema(schema: &CollectionSchema) -> Schema {
    let mut fields: Vec<Field> = base_arrow_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(schema.vectors.iter().enumerate().map(vector_field));
    fields.extend((0..schema.sparse_vectors.len()).map(sparse_field));
    Schema::new(fields)
}

/// `_vector_<index>`: a nullable `FixedSizeList<Float32>` of the vector's dim.
pub(crate) fn vector_field((index, spec): (usize, &VectorSpec)) -> Field {
    Field::new(vector_column(index), vector_type(spec.dim), true)
}

fn vector_type(dim: u32) -> DataType {
    // `MAX_VECTOR_DIM` is far below `i32::MAX`, and schemas are validated.
    let size = i32::try_from(dim).unwrap_or(i32::MAX);
    DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), size)
}

/// `_sparse_<index>`: a nullable `Struct<indices, values>`; null is absent and
/// an empty vector is two empty lists (Ruling 27).
pub(crate) fn sparse_field(index: usize) -> Field {
    Field::new(
        sparse_column(index),
        DataType::Struct(sparse_fields()),
        true,
    )
}

fn sparse_fields() -> Fields {
    Fields::from(vec![
        Field::new(
            "indices",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
            false,
        ),
    ])
}

/// One document to write, with the record that wrote it.
#[derive(Clone, Copy, Debug)]
pub struct NewRow<'a> {
    pub doc: &'a Document,
    pub partition: u32,
    pub offset: u64,
}

/// A document as stored in Lance.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredRow {
    pub pk: PrimaryKey,
    pub source: Map<String, Value>,
    pub vectors: BTreeMap<String, Vec<f32>>,
    pub sparse_vectors: BTreeMap<String, SparseVector>,
    pub partition: u32,
    pub offset: u64,
}

/// `rows` as one batch with [`arrow_schema`]`(schema)`. A vector that the
/// schema lacks, or a dense vector of the wrong dimension, is an `Internal`
/// error: documents are validated before they are written.
pub fn to_record_batch(
    schema: &CollectionSchema,
    rows: &[NewRow<'_>],
) -> Result<RecordBatch, CollectionError> {
    for row in rows {
        check_known_vectors(schema, row.doc)?;
    }
    let pks: Vec<Vec<u8>> = rows.iter().map(|row| row.doc.pk.canonical()).collect();
    let sources = rows
        .iter()
        .map(|row| serde_json::to_vec(&row.doc.source))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| CollectionError::Internal(format!("encode _source: {err}")))?;
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from_iter_values(&pks)),
        Arc::new(BinaryArray::from_iter_values(&sources)),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.partition),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.offset),
        )),
    ];
    for spec in &schema.vectors {
        columns.push(dense_column(spec, rows)?);
    }
    for spec in &schema.sparse_vectors {
        columns.push(sparse_array(
            rows.iter()
                .map(|row| row.doc.sparse_vectors.get(&spec.name)),
        )?);
    }
    RecordBatch::try_new(Arc::new(arrow_schema(schema)), columns)
        .map_err(|err| CollectionError::Internal(format!("build record batch: {err}")))
}

fn check_known_vectors(schema: &CollectionSchema, doc: &Document) -> Result<(), CollectionError> {
    if let Some(name) = doc
        .vectors
        .keys()
        .find(|name| !schema.vectors.iter().any(|v| &v.name == *name))
    {
        return Err(CollectionError::Internal(format!(
            "document {:?} has vector {name:?}, which the schema lacks",
            doc.pk
        )));
    }
    if let Some(name) = doc
        .sparse_vectors
        .keys()
        .find(|name| !schema.sparse_vectors.iter().any(|v| &v.name == *name))
    {
        return Err(CollectionError::Internal(format!(
            "document {:?} has sparse vector {name:?}, which the schema lacks",
            doc.pk
        )));
    }
    Ok(())
}

fn dense_column(spec: &VectorSpec, rows: &[NewRow<'_>]) -> Result<ArrayRef, CollectionError> {
    let dim = usize::try_from(spec.dim).unwrap_or(usize::MAX);
    let size = i32::try_from(spec.dim).unwrap_or(i32::MAX);
    let mut builder = FixedSizeListBuilder::with_capacity(Float32Builder::new(), size, rows.len())
        .with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    for row in rows {
        match row.doc.vectors.get(&spec.name) {
            Some(vector) if vector.len() == dim => {
                builder.values().append_slice(vector);
                builder.append(true);
            }
            Some(vector) => {
                return Err(CollectionError::Internal(format!(
                    "document {:?} has {} dimensions for vector {:?}, which has {dim}",
                    row.doc.pk,
                    vector.len(),
                    spec.name
                )));
            }
            None => {
                builder.values().append_nulls(dim);
                builder.append(false);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn sparse_array<'a>(
    vectors: impl Iterator<Item = Option<&'a SparseVector>>,
) -> Result<ArrayRef, CollectionError> {
    let fields = sparse_fields();
    let item = |index: usize| match fields[index].data_type() {
        DataType::List(item) => Ok(item.clone()),
        other => Err(CollectionError::Internal(format!(
            "sparse child {} is {other}, not a list",
            fields[index].name()
        ))),
    };
    let mut indices = ListBuilder::new(UInt32Builder::new()).with_field(item(0)?);
    let mut values = ListBuilder::new(Float32Builder::new()).with_field(item(1)?);
    let mut present = Vec::new();
    for vector in vectors {
        // An absent vector is a null struct over two empty lists.
        if let Some(vector) = vector {
            indices.values().append_slice(vector.indices());
            values.values().append_slice(vector.values());
        }
        indices.append(true);
        values.append(true);
        present.push(vector.is_some());
    }
    let children: Vec<ArrayRef> = vec![Arc::new(indices.finish()), Arc::new(values.finish())];
    StructArray::try_new(fields, children, Some(present.into()))
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|err| CollectionError::Internal(format!("build sparse column: {err}")))
}

/// Row `row` of `batch`, read with `schema`'s vectors. A vector column that
/// the batch lacks (a Lance version from before the vector was added) reads
/// as absent, like a null.
pub fn row_from_batch(
    schema: &CollectionSchema,
    batch: &RecordBatch,
    row: usize,
) -> Result<StoredRow, CollectionError> {
    if row >= batch.num_rows() {
        return Err(CollectionError::Internal(format!(
            "row {row} of a batch of {}",
            batch.num_rows()
        )));
    }
    let pk_bytes = required::<BinaryArray>(batch, PK_COLUMN)?.value(row);
    let pk = PrimaryKey::from_canonical(pk_bytes)
        .map_err(|err| CollectionError::Corrupt(format!("{PK_COLUMN}: {err}")))?;
    let source = serde_json::from_slice(required::<BinaryArray>(batch, SOURCE_COLUMN)?.value(row))
        .map_err(|err| CollectionError::Corrupt(format!("{SOURCE_COLUMN} of {pk:?}: {err}")))?;
    let partition = required::<UInt32Array>(batch, INGEST_PARTITION_COLUMN)?.value(row);
    let offset = required::<UInt64Array>(batch, INGEST_OFFSET_COLUMN)?.value(row);
    let mut vectors = BTreeMap::new();
    for (index, spec) in schema.vectors.iter().enumerate() {
        let name = vector_column(index);
        let Some(column) = optional::<FixedSizeListArray>(batch, &name)? else {
            continue;
        };
        if column.is_null(row) {
            continue;
        }
        let values = column.value(row);
        let values = downcast::<Float32Array>(values.as_ref(), &name)?;
        if values.null_count() > 0 {
            return Err(CollectionError::Corrupt(format!(
                "{name} of {pk:?} has a null element"
            )));
        }
        vectors.insert(spec.name.clone(), values.values().to_vec());
    }
    let mut sparse_vectors = BTreeMap::new();
    for (index, spec) in schema.sparse_vectors.iter().enumerate() {
        let name = sparse_column(index);
        let Some(column) = optional::<StructArray>(batch, &name)? else {
            continue;
        };
        if column.is_null(row) {
            continue;
        }
        let vector = sparse_value(column, row, &name)
            .map_err(|err| CollectionError::Corrupt(format!("{name} of {pk:?}: {err}")))?;
        sparse_vectors.insert(spec.name.clone(), vector);
    }
    Ok(StoredRow {
        pk,
        source,
        vectors,
        sparse_vectors,
        partition,
        offset,
    })
}

fn sparse_value(column: &StructArray, row: usize, name: &str) -> Result<SparseVector, String> {
    let list = |child: &str| -> Result<ArrayRef, String> {
        let array = column
            .column_by_name(child)
            .ok_or_else(|| format!("no {child} child"))?;
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| format!("{child} is {}, not a list", array.data_type()))?;
        if list.is_null(row) {
            return Err(format!("{child} is null"));
        }
        Ok(list.value(row))
    };
    let indices = list("indices")?;
    let values = list("values")?;
    let indices = downcast::<UInt32Array>(indices.as_ref(), name).map_err(|err| err.to_string())?;
    let values = downcast::<Float32Array>(values.as_ref(), name).map_err(|err| err.to_string())?;
    if indices.null_count() > 0 || values.null_count() > 0 {
        return Err("a null element".to_string());
    }
    SparseVector::new(indices.values().to_vec(), values.values().to_vec())
        .map_err(|err| err.to_string())
}

fn required<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, CollectionError> {
    optional(batch, name)?
        .ok_or_else(|| CollectionError::Corrupt(format!("the batch has no {name} column")))
}

fn optional<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a T>, CollectionError> {
    batch
        .column_by_name(name)
        .map(|column| downcast(column.as_ref(), name))
        .transpose()
}

fn downcast<'a, T: 'static>(array: &'a dyn Array, name: &str) -> Result<&'a T, CollectionError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CollectionError::Corrupt(format!(
            "{name} has type {}, not the expected one",
            array.data_type()
        ))
    })
}
