//! Flight `DoPut` bulk ingest (plan M1.2 Task 13, D49): Arrow record
//! batches into collections, through [`CollectionService::write`] (the
//! native bulk write path), and into streams, through the native produce
//! path ([`StreamProducer`]). SQL stays read-only.
//!
//! - Targets come from a PATH descriptor ([`put_target_from_path`]) or from
//!   Flight SQL's `CommandStatementIngest` ([`put_target_from_ingest`],
//!   [`ingest_action`]).
//! - Columns map once per stream schema ([`CollectionBatchMapper`],
//!   [`StreamBatchMapper`]); each batch is cut into chunks of
//!   `put_chunk_rows` rows, written one at a time, each whole or not at all.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

use arrow_flight::FlightData;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::sql::{
    CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use bytes::Bytes;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Float16Type, Float32Type, Float64Type, Int32Type, Int64Type, Schema, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt32Type, UInt64Type,
};
use futures::{Stream, StreamExt, TryStreamExt};
use operon_collection::{
    CollectionSchema, ConsistencyToken, Distance, DocOp, Document, DynamicMapping, HnswParams,
    MAX_WRITE_OPS, PrimaryKey, SparseModifier, SparseVector, SparseVectorSpec, VectorElement,
    VectorIndexSpec, VectorSpec,
};
use operon_log::{AppendAck, Record};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::ServiceError;
use crate::service::CollectionService;
use crate::sql::value_to_json;
use crate::types::WriteOptions;
use crate::types::{OpResult, WriteResult};

/// The first element of a collection descriptor path: `["collections", name
/// or alias]`.
pub const PUT_COLLECTIONS: &str = "collections";
/// The first element of a stream descriptor path: `["streams", stream]` or
/// `["streams", stream, partition]`.
pub const PUT_STREAMS: &str = "streams";
pub const ID_COLUMN: &str = "_id";
pub const SOURCE_COLUMN: &str = "_source";
/// The read-only columns of a SQL result, ignored on input.
pub const IGNORED_COLUMNS: [&str; 3] = ["_seq_no", "_partition", "_score"];
/// Field metadata of a string `_id` column: `str` (default), `u64` or
/// `uuid`. Also read from the request metadata of the same name when the
/// field has none.
pub const ID_TYPE_METADATA: &str = "operon-id-type";
/// The column of the unnamed (`""`) dense vector (Task 10 rule 2).
const UNNAMED_VECTOR_COLUMN: &str = "_vector";

/// How a string `_id` column is read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdType {
    #[default]
    Str,
    U64,
    Uuid,
}

impl IdType {
    /// Parses an [`ID_TYPE_METADATA`] value.
    pub fn parse(value: &str) -> Result<Self, ServiceError> {
        match value {
            "str" => Ok(IdType::Str),
            "u64" => Ok(IdType::U64),
            "uuid" => Ok(IdType::Uuid),
            other => Err(ServiceError::InvalidArgument(format!(
                "{ID_TYPE_METADATA} must be str, u64 or uuid, got {other:?}"
            ))),
        }
    }

    /// The id type of `arrow`'s `_id` field metadata, else `fallback`.
    pub fn of_schema(arrow: &Schema, fallback: Option<&str>) -> Result<Self, ServiceError> {
        let field = arrow.fields().iter().find(|f| f.name() == ID_COLUMN);
        match field.and_then(|f| f.metadata().get(ID_TYPE_METADATA)) {
            Some(value) => IdType::parse(value),
            None => fallback.map_or(Ok(IdType::Str), IdType::parse),
        }
    }
}

/// Where a put writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutTarget {
    /// A collection, by name or alias.
    Collection { name: String },
    /// A stream; `partition` pins every row to one partition.
    Stream {
        name: String,
        partition: Option<u32>,
    },
}

fn invalid(message: impl Into<String>) -> ServiceError {
    ServiceError::InvalidArgument(message.into())
}

/// Rule 1.1: the target of a PATH descriptor.
pub fn put_target_from_path(path: &[String]) -> Result<PutTarget, ServiceError> {
    let parts: Vec<&str> = path.iter().map(String::as_str).collect();
    match parts.as_slice() {
        [PUT_COLLECTIONS, name] if !name.is_empty() => Ok(PutTarget::Collection {
            name: name.to_string(),
        }),
        [PUT_STREAMS, name] if !name.is_empty() => Ok(PutTarget::Stream {
            name: name.to_string(),
            partition: None,
        }),
        [PUT_STREAMS, name, partition] if !name.is_empty() => {
            match partition.parse::<u32>() {
                // Canonical decimal: no sign, no leading zeros.
                Ok(p) if p.to_string() == *partition => Ok(PutTarget::Stream {
                    name: name.to_string(),
                    partition: Some(p),
                }),
                _ => Err(unknown_path(path)),
            }
        }
        _ => Err(unknown_path(path)),
    }
}

fn unknown_path(path: &[String]) -> ServiceError {
    invalid(format!(
        "unknown DoPut path {path:?}: use [\"collections\", name] or [\"streams\", name(, partition)]"
    ))
}

/// Rule 1.2: the namespace (`catalog`, else `default_ns`) and target of an
/// ingest command.
pub fn put_target_from_ingest(
    cmd: &CommandStatementIngest,
    default_ns: &str,
) -> Result<(String, PutTarget), ServiceError> {
    if cmd.temporary {
        return Err(invalid("temporary tables are not supported"));
    }
    if cmd.transaction_id.is_some() {
        return Err(invalid("transactions are not supported"));
    }
    let ns = match cmd.catalog.as_deref() {
        Some(catalog) if !catalog.is_empty() => catalog.to_string(),
        _ => default_ns.to_string(),
    };
    let target = match cmd.schema.as_deref() {
        None | Some(PUT_COLLECTIONS) => PutTarget::Collection {
            name: cmd.table.clone(),
        },
        Some(PUT_STREAMS) => PutTarget::Stream {
            name: cmd.table.clone(),
            partition: None,
        },
        Some(other) => {
            return Err(invalid(format!(
                "unknown schema {other}: use collections or streams"
            )));
        }
    };
    Ok((ns, target))
}

/// What an ingest does with its target (rule 1.2's table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestAction {
    /// Load the existing target.
    Append,
    /// Create the collection from the Arrow schema (rule 1.4), then load it.
    Create,
}

/// Rule 1.2's table: `if_exists` decides for an existing target,
/// `if_not_exist` for a missing one.
pub fn ingest_action(
    options: Option<&TableDefinitionOptions>,
    target: &PutTarget,
    exists: bool,
) -> Result<IngestAction, ServiceError> {
    let if_exists = options.map_or(TableExistsOption::Unspecified, |o| {
        TableExistsOption::try_from(o.if_exists).unwrap_or(TableExistsOption::Unspecified)
    });
    let if_not_exist = options.map_or(TableNotExistOption::Unspecified, |o| {
        TableNotExistOption::try_from(o.if_not_exist).unwrap_or(TableNotExistOption::Unspecified)
    });
    let (name, kind) = match target {
        PutTarget::Collection { name } => (name, "collection"),
        PutTarget::Stream { name, .. } => (name, "stream"),
    };
    if if_exists == TableExistsOption::Replace {
        return Err(invalid(
            "replace is not supported: drop the collection through the native API, then ingest with create",
        ));
    }
    if exists {
        return match if_exists {
            TableExistsOption::Fail => Err(ServiceError::AlreadyExists(name.clone())),
            _ => Ok(IngestAction::Append),
        };
    }
    match if_not_exist {
        TableNotExistOption::Create => match target {
            PutTarget::Collection { .. } => Ok(IngestAction::Create),
            PutTarget::Stream { .. } => Err(invalid("create streams through the native API")),
        },
        _ => Err(ServiceError::NotFound {
            kind,
            name: name.clone(),
        }),
    }
}

/// A row that cannot be mapped (0-based row of its batch).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowError {
    pub row: usize,
    pub column: String,
    pub message: String,
}

// ----- Arrow shapes and values -----

/// `FixedSizeList<Float32, _>`.
fn is_fixed_vector(data_type: &DataType) -> bool {
    matches!(data_type, DataType::FixedSizeList(item, _) if item.data_type() == &DataType::Float32)
}

/// A dense vector column: `FixedSizeList`, `List` or `LargeList` of
/// `Float32`.
fn is_vector(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(item, _) | DataType::List(item) | DataType::LargeList(item) => {
            item.data_type() == &DataType::Float32
        }
        _ => false,
    }
}

fn list_item(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(item) | DataType::LargeList(item) => Some(item.data_type()),
        _ => None,
    }
}

/// `Struct<indices: List<UInt32 | Int32 | Int64>, values: List<Float32>>`
/// (Task 10 rule 2, with wider index types).
fn is_sparse(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.len() != 2 {
        return false;
    }
    let child = |name: &str| fields.iter().find(|f| f.name() == name);
    let indices = child("indices").and_then(|f| list_item(f.data_type()));
    let values = child("values").and_then(|f| list_item(f.data_type()));
    matches!(
        indices,
        Some(DataType::UInt32 | DataType::Int32 | DataType::Int64)
    ) && values == Some(&DataType::Float32)
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_bytes(data_type: &DataType) -> bool {
    is_string(data_type)
        || matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
}

/// The string at `row` of a string column.
fn str_at(array: &dyn Array, row: usize) -> Option<&str> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().value(row)),
        DataType::Utf8View => Some(array.as_string_view().value(row)),
        _ => None,
    }
}

/// The bytes at `row` of a string or binary column (a string's UTF-8).
fn bytes_at(array: &dyn Array, row: usize) -> Option<&[u8]> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Binary => Some(array.as_binary::<i32>().value(row)),
        DataType::LargeBinary => Some(array.as_binary::<i64>().value(row)),
        DataType::BinaryView => Some(array.as_binary_view().value(row)),
        _ => str_at(array, row).map(str::as_bytes),
    }
}

/// The child values of list `array` at `row`.
fn list_at(array: &dyn Array, row: usize) -> Option<Arc<dyn Array>> {
    match array.data_type() {
        DataType::List(_) => Some(array.as_list::<i32>().value(row)),
        DataType::LargeList(_) => Some(array.as_list::<i64>().value(row)),
        DataType::FixedSizeList(..) => Some(array.as_fixed_size_list().value(row)),
        _ => None,
    }
}

/// Whether `array[row]` holds a non-finite float anywhere inside it.
fn non_finite(array: &dyn Array, row: usize) -> bool {
    if array.is_null(row) {
        return false;
    }
    let any = |values: Arc<dyn Array>| (0..values.len()).any(|i| non_finite(values.as_ref(), i));
    match array.data_type() {
        DataType::Float16 => !array
            .as_primitive::<Float16Type>()
            .value(row)
            .to_f32()
            .is_finite(),
        DataType::Float32 => !array.as_primitive::<Float32Type>().value(row).is_finite(),
        DataType::Float64 => !array.as_primitive::<Float64Type>().value(row).is_finite(),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => {
            list_at(array, row).is_some_and(any)
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .any(|column| non_finite(column.as_ref(), row)),
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            entries
                .columns()
                .iter()
                .any(|column| (0..column.len()).any(|i| non_finite(column.as_ref(), i)))
        }
        _ => false,
    }
}

/// A UUID in hyphenated (8-4-4-4-12) or 32-hex form.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let hex: String = if text.len() == 36 {
        let groups: Vec<&str> = text.split('-').collect();
        let lengths: Vec<usize> = groups.iter().map(|g| g.len()).collect();
        if lengths != [8, 4, 4, 4, 12] {
            return None;
        }
        groups.concat()
    } else if text.len() == 32 {
        text.to_string()
    } else {
        return None;
    };
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        let pair = hex.get(2 * i..2 * i + 2)?;
        if !pair.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

// ----- Collections -----

/// How an `_id` column is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdColumn {
    Unsigned,
    Signed,
    Text(IdType),
    Binary16,
}

impl IdColumn {
    fn of(data_type: &DataType, id_type: IdType) -> Result<Self, ServiceError> {
        match data_type {
            DataType::UInt64 | DataType::UInt32 => Ok(IdColumn::Unsigned),
            DataType::Int64 | DataType::Int32 => Ok(IdColumn::Signed),
            DataType::FixedSizeBinary(16) => Ok(IdColumn::Binary16),
            t if is_string(t) => Ok(IdColumn::Text(id_type)),
            other => Err(invalid(format!(
                "column {ID_COLUMN}: {other} cannot hold a primary key (use UInt64, Int64, a string or FixedSizeBinary(16))"
            ))),
        }
    }

    fn read(self, array: &dyn Array, row: usize) -> Result<PrimaryKey, String> {
        if array.is_null(row) {
            return Err("the primary key is null".to_string());
        }
        let pk = match self {
            IdColumn::Unsigned => PrimaryKey::U64(match array.data_type() {
                DataType::UInt32 => u64::from(array.as_primitive::<UInt32Type>().value(row)),
                _ => array.as_primitive::<UInt64Type>().value(row),
            }),
            IdColumn::Signed => {
                let value = match array.data_type() {
                    DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
                    _ => array.as_primitive::<Int64Type>().value(row),
                };
                PrimaryKey::U64(
                    u64::try_from(value)
                        .map_err(|_| format!("a primary key cannot be negative, got {value}"))?,
                )
            }
            IdColumn::Binary16 => {
                let bytes = array.as_fixed_size_binary().value(row);
                PrimaryKey::Uuid(bytes.try_into().map_err(|_| "not 16 bytes".to_string())?)
            }
            IdColumn::Text(id_type) => {
                let text = str_at(array, row).unwrap_or_default();
                match id_type {
                    IdType::Str => PrimaryKey::Str(text.to_string()),
                    IdType::U64 => match text.parse::<u64>() {
                        Ok(n) if n.to_string() == text => PrimaryKey::U64(n),
                        _ => return Err(format!("{text:?} is not a canonical decimal u64")),
                    },
                    IdType::Uuid => PrimaryKey::Uuid(
                        parse_uuid(text).ok_or_else(|| format!("{text:?} is not a UUID"))?,
                    ),
                }
            }
        };
        pk.validate().map_err(|err| err.to_string())?;
        Ok(pk)
    }
}

/// What one column of a collection put carries (rule 3).
#[derive(Clone, Debug, PartialEq, Eq)]
enum CollectionRole {
    Id(IdColumn),
    Source,
    /// The dense vector of this name.
    Vector(String),
    /// The sparse vector of this name.
    Sparse(String),
    Ignored,
    /// The top-level source key of this name.
    Key(String),
}

/// Built once from the stream's Arrow schema (rule 3); maps every batch of
/// that schema.
#[derive(Debug)]
pub struct CollectionBatchMapper {
    roles: Vec<(String, CollectionRole)>,
}

/// Refuses a schema with a repeated column name.
fn unique_columns(arrow: &Schema) -> Result<(), ServiceError> {
    let mut seen = BTreeSet::new();
    for field in arrow.fields() {
        if !seen.insert(field.name().as_str()) {
            return Err(invalid(format!("duplicate column {}", field.name())));
        }
    }
    Ok(())
}

/// The vector name a column names (`_vector` is the unnamed one).
fn vector_of_column(column: &str) -> &str {
    if column == UNNAMED_VECTOR_COLUMN {
        ""
    } else {
        column
    }
}

impl CollectionBatchMapper {
    pub fn new(
        arrow: &Schema,
        schema: &CollectionSchema,
        id_type: IdType,
    ) -> Result<Self, ServiceError> {
        unique_columns(arrow)?;
        let with_source = arrow.fields().iter().any(|f| f.name() == SOURCE_COLUMN);
        let mut roles = Vec::with_capacity(arrow.fields().len());
        let mut has_id = false;
        for field in arrow.fields() {
            let name = field.name().as_str();
            let data_type = field.data_type();
            let role = if name == ID_COLUMN {
                has_id = true;
                CollectionRole::Id(IdColumn::of(data_type, id_type)?)
            } else if name == SOURCE_COLUMN {
                if !is_bytes(data_type) || data_type == &DataType::BinaryView {
                    return Err(invalid(format!(
                        "column {SOURCE_COLUMN}: {data_type} cannot hold a JSON object (use a string or binary column)"
                    )));
                }
                CollectionRole::Source
            } else if IGNORED_COLUMNS.contains(&name) {
                CollectionRole::Ignored
            } else if let Some(spec) = schema
                .vectors
                .iter()
                .find(|spec| spec.name == vector_of_column(name))
            {
                if !is_vector(data_type) {
                    return Err(invalid(format!(
                        "column {name}: vector {:?} needs a list of Float32, got {data_type}",
                        spec.name
                    )));
                }
                CollectionRole::Vector(spec.name.clone())
            } else if let Some(spec) = schema.sparse_vectors.iter().find(|spec| spec.name == name) {
                if !is_sparse(data_type) {
                    return Err(invalid(format!(
                        "column {name}: sparse vector {:?} needs Struct<indices: List<UInt32>, values: List<Float32>>, got {data_type}",
                        spec.name
                    )));
                }
                CollectionRole::Sparse(spec.name.clone())
            } else if is_fixed_vector(data_type) || is_sparse(data_type) {
                return Err(invalid(format!(
                    "column {name} looks like a vector, but the collection has no vector {name}"
                )));
            } else if with_source {
                if schema.fields.iter().any(|spec| spec.name == name) {
                    CollectionRole::Ignored
                } else {
                    return Err(invalid(format!(
                        "column {name}: with a _source column, other columns must be vectors or schema fields"
                    )));
                }
            } else {
                CollectionRole::Key(name.to_string())
            };
            roles.push((name.to_string(), role));
        }
        if !has_id {
            return Err(invalid("a collection put needs an _id column"));
        }
        Ok(Self { roles })
    }

    /// One `DocOp::Upsert` per row, in row order.
    pub fn map(&self, batch: &RecordBatch) -> Result<Vec<DocOp>, RowError> {
        if batch.num_columns() != self.roles.len() {
            return Err(RowError {
                row: 0,
                column: String::new(),
                message: format!(
                    "the batch has {} columns, the schema {}",
                    batch.num_columns(),
                    self.roles.len()
                ),
            });
        }
        let mut ops = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut pk = None;
            let mut source = Map::new();
            let mut vectors = BTreeMap::new();
            let mut sparse_vectors = BTreeMap::new();
            for ((column, role), array) in self.roles.iter().zip(batch.columns()) {
                let array = array.as_ref();
                let fail = |message: String| RowError {
                    row,
                    column: column.clone(),
                    message,
                };
                match role {
                    CollectionRole::Id(kind) => pk = Some(kind.read(array, row).map_err(fail)?),
                    CollectionRole::Source => {
                        if let Some(bytes) = bytes_at(array, row) {
                            source = match serde_json::from_slice::<Value>(bytes) {
                                Ok(Value::Object(object)) => object,
                                Ok(_) => return Err(fail("not a JSON object".to_string())),
                                Err(err) => return Err(fail(format!("not JSON: {err}"))),
                            };
                        }
                    }
                    CollectionRole::Vector(name) => {
                        if let Some(vector) = dense_at(array, row).map_err(fail)? {
                            vectors.insert(name.clone(), vector);
                        }
                    }
                    CollectionRole::Sparse(name) => {
                        if let Some(vector) = sparse_at(array, row).map_err(fail)? {
                            sparse_vectors.insert(name.clone(), vector);
                        }
                    }
                    CollectionRole::Ignored => {}
                    CollectionRole::Key(key) => {
                        if array.is_null(row) {
                            continue;
                        }
                        if non_finite(array, row) {
                            return Err(fail("a non-finite float".to_string()));
                        }
                        source.insert(key.clone(), value_to_json(array, row));
                    }
                }
            }
            ops.push(DocOp::Upsert(Document {
                pk: pk.expect("the schema has an _id column"),
                source,
                vectors,
                sparse_vectors,
            }));
        }
        Ok(ops)
    }
}

/// The dense vector at `row`; `None` for a null.
fn dense_at(array: &dyn Array, row: usize) -> Result<Option<Vec<f32>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let values = list_at(array, row).ok_or_else(|| "not a list".to_string())?;
    let values = values.as_primitive::<Float32Type>();
    let mut out = Vec::with_capacity(values.len());
    for i in 0..values.len() {
        if values.is_null(i) {
            return Err(format!("element {i} is null"));
        }
        let value = values.value(i);
        if !value.is_finite() {
            return Err(format!("element {i} is not finite"));
        }
        out.push(value);
    }
    Ok(Some(out))
}

/// The sparse vector at `row`; `None` for a null.
fn sparse_at(array: &dyn Array, row: usize) -> Result<Option<SparseVector>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    let array = array.as_struct();
    let child = |name: &str| {
        let column = array
            .column_by_name(name)
            .ok_or_else(|| format!("no {name}"))?;
        if column.is_null(row) {
            return Err(format!("{name} is null"));
        }
        list_at(column.as_ref(), row).ok_or_else(|| format!("{name} is not a list"))
    };
    let indices_array = child("indices")?;
    let values_array = child("values")?;
    let mut indices = Vec::with_capacity(indices_array.len());
    for i in 0..indices_array.len() {
        if indices_array.is_null(i) {
            return Err(format!("index {i} is null"));
        }
        let index: i64 = match indices_array.data_type() {
            DataType::UInt32 => i64::from(indices_array.as_primitive::<UInt32Type>().value(i)),
            DataType::Int32 => i64::from(indices_array.as_primitive::<Int32Type>().value(i)),
            _ => indices_array.as_primitive::<Int64Type>().value(i),
        };
        indices.push(u32::try_from(index).map_err(|_| format!("index {index} is not a u32"))?);
    }
    let floats = values_array.as_primitive::<Float32Type>();
    let mut values = Vec::with_capacity(floats.len());
    for i in 0..floats.len() {
        if floats.is_null(i) {
            return Err(format!("value {i} is null"));
        }
        values.push(floats.value(i));
    }
    SparseVector::new(indices, values)
        .map(Some)
        .map_err(|err| err.to_string())
}

/// Rule 1.4: the schema of a collection an ingest creates from `arrow`:
/// dynamic mapping `Map` and no fields, one `Cosine` vector per
/// `FixedSizeList<Float32, d>` column and one sparse vector (no modifier)
/// per sparse-shaped struct column, in column order.
pub fn schema_from_arrow(arrow: &Schema) -> Result<CollectionSchema, ServiceError> {
    unique_columns(arrow)?;
    if !arrow.fields().iter().any(|f| f.name() == ID_COLUMN) {
        return Err(invalid("a collection put needs an _id column"));
    }
    let mut vectors = Vec::new();
    let mut sparse = Vec::new();
    for field in arrow.fields() {
        let name = field.name().as_str();
        if name == ID_COLUMN || name == SOURCE_COLUMN || IGNORED_COLUMNS.contains(&name) {
            continue;
        }
        match field.data_type() {
            DataType::FixedSizeList(item, dim) if item.data_type() == &DataType::Float32 => {
                vectors.push(VectorSpec {
                    name: vector_of_column(name).to_string(),
                    dim: u32::try_from(*dim)
                        .map_err(|_| invalid(format!("column {name}: dimension {dim}")))?,
                    distance: Distance::Cosine,
                    element: VectorElement::F32,
                    index: VectorIndexSpec::Auto,
                    hnsw: HnswParams::default(),
                    quantization: None,
                });
            }
            t if is_sparse(t) => sparse.push(SparseVectorSpec {
                name: name.to_string(),
                modifier: SparseModifier::None,
            }),
            _ => {}
        }
    }
    let schema =
        CollectionSchema::new(Vec::new(), vectors, DynamicMapping::Map).with_sparse_vectors(sparse);
    schema.validate().map_err(|err| {
        invalid(format!(
            "the Arrow schema makes an invalid collection: {err}"
        ))
    })?;
    Ok(schema)
}

// ----- Streams -----

/// What one column of a stream put carries (rule 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamRole {
    Key,
    Value,
    Headers,
    Timestamp,
    Partition,
}

/// `List<Struct<key: string, value: binary | string>>`.
fn is_headers(data_type: &DataType) -> bool {
    let Some(DataType::Struct(fields)) = list_item(data_type) else {
        return false;
    };
    let child = |name: &str| {
        fields
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.data_type())
    };
    fields.len() == 2 && child("key").is_some_and(is_string) && child("value").is_some_and(is_bytes)
}

/// Built once from the stream's Arrow schema (rule 4); maps every batch of
/// that schema.
#[derive(Debug)]
pub struct StreamBatchMapper {
    roles: Vec<(String, StreamRole)>,
    fixed_partition: Option<u32>,
    partitions: u32,
}

impl StreamBatchMapper {
    pub fn new(
        arrow: &Schema,
        fixed_partition: Option<u32>,
        partitions: u32,
    ) -> Result<Self, ServiceError> {
        unique_columns(arrow)?;
        let mut roles = Vec::with_capacity(arrow.fields().len());
        for field in arrow.fields() {
            let name = field.name().as_str();
            let data_type = field.data_type();
            let (role, fits) = match name {
                "key" => (StreamRole::Key, is_bytes(data_type)),
                "value" => (StreamRole::Value, is_bytes(data_type)),
                "headers" => (StreamRole::Headers, is_headers(data_type)),
                "timestamp" => (
                    StreamRole::Timestamp,
                    matches!(data_type, DataType::Timestamp(..) | DataType::Int64),
                ),
                "partition" => (
                    StreamRole::Partition,
                    matches!(data_type, DataType::UInt32 | DataType::Int32),
                ),
                _ => {
                    return Err(invalid(format!(
                        "column {name}: a stream put takes the columns key, value, headers, timestamp and partition"
                    )));
                }
            };
            if !fits {
                return Err(invalid(format!(
                    "column {name}: {data_type} is not a valid type for it"
                )));
            }
            roles.push((name.to_string(), role));
        }
        if let Some(p) = fixed_partition {
            if roles.iter().any(|(_, role)| *role == StreamRole::Partition) {
                return Err(invalid(
                    "a partition column and a partition in the descriptor path cannot be combined",
                ));
            }
            if p >= partitions {
                return Err(invalid(format!(
                    "partition {p} is out of range: the stream has {partitions}"
                )));
            }
        }
        Ok(Self {
            roles,
            fixed_partition,
            partitions: partitions.max(1),
        })
    }

    /// One `(partition, record)` per row, in row order.
    pub fn map(&self, batch: &RecordBatch) -> Result<Vec<(u32, Record)>, RowError> {
        if batch.num_columns() != self.roles.len() {
            return Err(RowError {
                row: 0,
                column: String::new(),
                message: format!(
                    "the batch has {} columns, the schema {}",
                    batch.num_columns(),
                    self.roles.len()
                ),
            });
        }
        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut record = Record {
                key: None,
                value: None,
                headers: Vec::new(),
                timestamp_ms: -1,
            };
            let mut partition = self.fixed_partition;
            for ((column, role), array) in self.roles.iter().zip(batch.columns()) {
                let array = array.as_ref();
                let fail = |message: String| RowError {
                    row,
                    column: column.clone(),
                    message,
                };
                match role {
                    StreamRole::Key => {
                        record.key = bytes_at(array, row).map(Bytes::copy_from_slice)
                    }
                    StreamRole::Value => {
                        record.value = bytes_at(array, row).map(Bytes::copy_from_slice)
                    }
                    StreamRole::Headers => record.headers = headers_at(array, row).map_err(fail)?,
                    StreamRole::Timestamp => {
                        if !array.is_null(row) {
                            record.timestamp_ms = timestamp_ms(array, row);
                        }
                    }
                    StreamRole::Partition => {
                        if array.is_null(row) {
                            return Err(fail("the partition is null".to_string()));
                        }
                        let p = match array.data_type() {
                            DataType::UInt32 => {
                                i64::from(array.as_primitive::<UInt32Type>().value(row))
                            }
                            _ => i64::from(array.as_primitive::<Int32Type>().value(row)),
                        };
                        match u32::try_from(p) {
                            Ok(p) if p < self.partitions => partition = Some(p),
                            _ => {
                                return Err(fail(format!(
                                    "partition {p} is out of range: the stream has {}",
                                    self.partitions
                                )));
                            }
                        }
                    }
                }
            }
            let partition = match (partition, &record.key) {
                (Some(p), _) => p,
                // The hash `partition_of` applies to a canonical key.
                (None, Some(key)) => (xxh3_64(key) % u64::from(self.partitions)) as u32,
                (None, None) if self.partitions == 1 => 0,
                (None, None) => {
                    return Err(RowError {
                        row,
                        column: "key".to_string(),
                        message: "a null key needs a partition column".to_string(),
                    });
                }
            };
            out.push((partition, record));
        }
        Ok(out)
    }
}

/// The headers at `row`, in order.
fn headers_at(array: &dyn Array, row: usize) -> Result<Vec<(String, Option<Bytes>)>, String> {
    if array.is_null(row) {
        return Ok(Vec::new());
    }
    let entries = list_at(array, row).ok_or_else(|| "not a list".to_string())?;
    let entries = entries.as_struct();
    let keys = entries.column_by_name("key").ok_or("no key")?;
    let values = entries.column_by_name("value").ok_or("no value")?;
    let mut out = Vec::with_capacity(entries.len());
    for i in 0..entries.len() {
        if entries.is_null(i) {
            return Err(format!("header {i} is null"));
        }
        let key = str_at(keys.as_ref(), i).ok_or_else(|| format!("header {i} has a null key"))?;
        let value = bytes_at(values.as_ref(), i).map(Bytes::copy_from_slice);
        out.push((key.to_string(), value));
    }
    Ok(out)
}

/// A timestamp column's value in milliseconds (truncated).
fn timestamp_ms(array: &dyn Array, row: usize) -> i64 {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_primitive::<TimestampSecondType>()
            .value(row)
            .saturating_mul(1_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            array.as_primitive::<TimestampMillisecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>().value(row) / 1_000
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array.as_primitive::<TimestampNanosecondType>().value(row) / 1_000_000
        }
        _ => array.as_primitive::<Int64Type>().value(row),
    }
}

// ----- Acknowledgements and the produce path -----

/// The `app_metadata` of each `PutResult` of a path-descriptor put, as
/// UTF-8 JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutAck {
    /// The 0-based index of the record batch.
    pub batch: u64,
    /// Its rows.
    pub rows: u64,
    /// Every token acknowledged so far in this put, merged (`v1:…`).
    pub token: String,
    /// Streams only: the offsets this batch appended, per partition.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub offsets: Vec<PartitionOffsets>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionOffsets {
    pub partition: u32,
    pub base_offset: u64,
    pub last_offset: u64,
}

/// The native produce path (rule 7), implemented by the `operon` crate.
#[async_trait::async_trait]
pub trait StreamProducer: Send + Sync + std::fmt::Debug {
    /// The partition count of `stream`; `NotFound { kind: "stream" }` when
    /// absent.
    async fn partitions(&self, ns: &str, stream: &str) -> Result<u32, ServiceError>;
    /// Appends every partition's records in one `append_many`.
    async fn produce(
        &self,
        ns: &str,
        stream: &str,
        records: Vec<(u32, Vec<Record>)>,
    ) -> Result<Vec<AppendAck>, ServiceError>;
}

/// The chunks of a batch of `rows` rows: consecutive ranges of at most
/// `chunk_rows` rows, in row order.
pub fn chunk_ranges(rows: usize, chunk_rows: usize) -> Vec<Range<usize>> {
    let chunk_rows = chunk_rows.clamp(1, MAX_WRITE_OPS);
    (0..rows)
        .step_by(chunk_rows)
        .map(|start| start..(start + chunk_rows).min(rows))
        .collect()
}

// ----- The put driver -----

/// Where a put writes, resolved before its data is read.
#[derive(Debug)]
pub(crate) enum Sink {
    /// `schema: None` creates the collection from the Arrow schema.
    Collection {
        ns: String,
        name: String,
        schema: Option<CollectionSchema>,
        id_type: Option<String>,
    },
    Stream {
        ns: String,
        name: String,
        partitions: u32,
        partition: Option<u32>,
        producer: Arc<dyn StreamProducer>,
    },
}

/// Why a put stopped: a service error, or a transport status of the
/// incoming stream.
#[derive(Debug)]
pub(crate) enum PutError {
    Service(ServiceError),
    Status(Status),
}

impl From<ServiceError> for PutError {
    fn from(err: ServiceError) -> Self {
        PutError::Service(err)
    }
}

impl PutError {
    /// The status, with rule 6's suffix once `written` rows were
    /// acknowledged.
    pub(crate) fn into_status(self, written: u64) -> Status {
        let status = match self {
            PutError::Service(err) => crate::flight::status_of(&err),
            PutError::Status(status) => status,
        };
        if written == 0 {
            return status;
        }
        Status::new(
            status.code(),
            format!(
                "{} ({written} rows were written before the failure)",
                status.message()
            ),
        )
    }
}

enum Mapper {
    Collection(CollectionBatchMapper),
    Stream(StreamBatchMapper),
}

/// Where a path-descriptor put sends each batch's acknowledgement, and
/// finally its error.
pub(crate) type Acks = tokio::sync::mpsc::Sender<Result<PutAck, Status>>;

/// One put: its sink, what it has written and its token so far.
pub(crate) struct Put {
    service: Arc<CollectionService>,
    sink: Sink,
    chunk_rows: usize,
    /// Rows acknowledged so far.
    pub(crate) written: u64,
    token: ConsistencyToken,
    /// Cancelled when the server shuts down: the put stops while it waits
    /// for a message and before its next chunk (the chunk in flight
    /// finishes).
    stop: CancellationToken,
}

/// The status of a put the server's shutdown stopped.
fn shutting_down() -> PutError {
    PutError::Status(Status::unavailable("the server is shutting down"))
}

/// Rewrites a refused chunk's `op {i}` as `batch {b} row {r}` (rule 6).
fn rewrite_op(err: ServiceError, batch: u64, base: usize) -> ServiceError {
    let rewrite = |message: String| -> String {
        let parsed = message.strip_prefix("op ").and_then(|rest| {
            let (index, tail) = rest.split_once(": ")?;
            Some((index.parse::<usize>().ok()?, tail.to_string()))
        });
        match parsed {
            Some((i, tail)) => format!("batch {batch} row {}: {tail}", base + i),
            None => format!("batch {batch}: {message}"),
        }
    };
    match err {
        ServiceError::InvalidArgument(message) => ServiceError::InvalidArgument(rewrite(message)),
        ServiceError::SchemaViolation { field, message } => ServiceError::SchemaViolation {
            field,
            message: rewrite(message),
        },
        other => other,
    }
}

/// Rewrites a refused stream chunk's `record {i}: … partition {p}` as
/// `batch {b} row {r}` through `rows` (partition → batch rows), else
/// prefixes the batch.
fn rewrite_record(err: ServiceError, batch: u64, rows: &BTreeMap<u32, Vec<usize>>) -> ServiceError {
    let rewrite = |message: String| -> String {
        let parsed = message.strip_prefix("record ").and_then(|rest| {
            let (index, tail) = rest.split_once(": ")?;
            let partition = tail.rsplit_once("partition ")?.1.parse::<u32>().ok()?;
            let row = *rows.get(&partition)?.get(index.parse::<usize>().ok()?)?;
            Some((row, tail.to_string()))
        });
        match parsed {
            Some((row, tail)) => format!("batch {batch} row {row}: {tail}"),
            None => format!("batch {batch}: {message}"),
        }
    };
    match err {
        ServiceError::InvalidArgument(message) => ServiceError::InvalidArgument(rewrite(message)),
        other => other,
    }
}

impl Put {
    pub(crate) fn new(
        service: Arc<CollectionService>,
        sink: Sink,
        chunk_rows: usize,
        stop: CancellationToken,
    ) -> Self {
        Self {
            service,
            sink,
            chunk_rows,
            written: 0,
            token: ConsistencyToken::default(),
            stop,
        }
    }

    /// The mapper of the stream's schema; creates the collection first when
    /// the sink says so (rule 1.4).
    async fn mapper(&mut self, arrow: &Schema) -> Result<Mapper, ServiceError> {
        match &mut self.sink {
            Sink::Collection {
                ns,
                name,
                schema,
                id_type,
            } => {
                let id_type = IdType::of_schema(arrow, id_type.as_deref())?;
                let resolved = match schema {
                    Some(schema) => schema.clone(),
                    None => {
                        let derived = schema_from_arrow(arrow)?;
                        // Checked before anything is created.
                        CollectionBatchMapper::new(arrow, &derived, id_type)?;
                        let info = self
                            .service
                            .create_collection(ns, name, derived, None)
                            .await?;
                        *schema = Some(info.schema.clone());
                        info.schema
                    }
                };
                Ok(Mapper::Collection(CollectionBatchMapper::new(
                    arrow, &resolved, id_type,
                )?))
            }
            Sink::Stream {
                partitions,
                partition,
                ..
            } => Ok(Mapper::Stream(StreamBatchMapper::new(
                arrow,
                *partition,
                *partitions,
            )?)),
        }
    }

    /// Reads `data` to its end, writing every batch in chunks. `acks`, when
    /// set, gets each batch's acknowledgement once every chunk of it is
    /// written; once its receiver is gone the put stops before its next
    /// chunk (acknowledged chunks stay written). A shutdown stops it the
    /// same way, with `Unavailable`.
    pub(crate) async fn run<S>(&mut self, data: S, acks: Option<&Acks>) -> Result<(), PutError>
    where
        S: Stream<Item = Result<FlightData, Status>> + Send + 'static,
    {
        let mut decoder =
            FlightDataDecoder::new(data.map_err(|status| FlightError::Tonic(Box::new(status))));
        let mut mapper: Option<Mapper> = None;
        let mut batch_no: u64 = 0;
        loop {
            let item = tokio::select! {
                item = decoder.next() => item,
                () = self.stop.cancelled() => return Err(shutting_down()),
            };
            let Some(item) = item else {
                break;
            };
            let decoded = match item {
                Ok(decoded) => decoded,
                Err(FlightError::Tonic(status)) => return Err(PutError::Status(*status)),
                Err(err) => return Err(invalid(format!("DoPut: {err}")).into()),
            };
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(schema) => {
                    if mapper.is_some() {
                        return Err(invalid("DoPut: a second schema message").into());
                    }
                    mapper = Some(self.mapper(&schema).await?);
                }
                DecodedPayload::RecordBatch(batch) => {
                    let Some(mapper) = &mapper else {
                        return Err(invalid("DoPut: a record batch before the schema").into());
                    };
                    let put_ack = match mapper {
                        Mapper::Collection(mapper) => {
                            self.write_collection(mapper, batch_no, &batch, acks)
                                .await?
                        }
                        Mapper::Stream(mapper) => {
                            self.write_stream(mapper, batch_no, &batch, acks).await?
                        }
                    };
                    let Some(put_ack) = put_ack else {
                        return Ok(());
                    };
                    if let Some(acks) = acks
                        && acks.send(Ok(put_ack)).await.is_err()
                    {
                        return Ok(());
                    }
                    batch_no += 1;
                }
            }
        }
        Ok(())
    }

    fn row_error(batch: u64, err: RowError) -> ServiceError {
        invalid(format!(
            "batch {batch} row {} column {}: {}",
            err.row, err.column, err.message
        ))
    }

    /// Writes one collection batch; `None` when the listener left between
    /// chunks.
    async fn write_collection(
        &mut self,
        mapper: &CollectionBatchMapper,
        batch_no: u64,
        batch: &RecordBatch,
        acks: Option<&Acks>,
    ) -> Result<Option<PutAck>, PutError> {
        let Sink::Collection { ns, name, .. } = &self.sink else {
            unreachable!("a collection mapper writes to a collection");
        };
        let (ns, name) = (ns.clone(), name.clone());
        let mut ops = mapper
            .map(batch)
            .map_err(|err| Self::row_error(batch_no, err))?;
        for range in chunk_ranges(ops.len(), self.chunk_rows) {
            if acks.is_some_and(|acks| acks.is_closed()) {
                return Ok(None);
            }
            if self.stop.is_cancelled() {
                return Err(shutting_down());
            }
            let chunk: Vec<DocOp> = ops.drain(..range.len()).collect();
            let result = self
                .service
                .write(
                    &ns,
                    &name,
                    chunk,
                    WriteOptions {
                        atomic: true,
                        report_existence: false,
                    },
                )
                .await
                .map_err(|err| rewrite_op(err, batch_no, range.start))?;
            let WriteResult { token, results, .. } = result;
            // A writer refusal after validation (a schema change in between).
            if let Some((i, err)) = results.into_iter().enumerate().find_map(|(i, r)| match r {
                OpResult::Rejected(err) => Some((i, err)),
                _ => None,
            }) {
                let err = match err {
                    ServiceError::InvalidArgument(m) => {
                        ServiceError::InvalidArgument(format!("op {i}: {m}"))
                    }
                    ServiceError::SchemaViolation { field, message } => {
                        ServiceError::SchemaViolation {
                            field,
                            message: format!("op {i}: {message}"),
                        }
                    }
                    other => other,
                };
                return Err(rewrite_op(err, batch_no, range.start).into());
            }
            self.token.merge(&token);
            self.written += range.len() as u64;
        }
        Ok(Some(PutAck {
            batch: batch_no,
            rows: batch.num_rows() as u64,
            token: self.token.to_string(),
            offsets: Vec::new(),
        }))
    }

    /// Writes one stream batch; `None` when the listener left between
    /// chunks.
    async fn write_stream(
        &mut self,
        mapper: &StreamBatchMapper,
        batch_no: u64,
        batch: &RecordBatch,
        acks: Option<&Acks>,
    ) -> Result<Option<PutAck>, PutError> {
        let Sink::Stream {
            ns, name, producer, ..
        } = &self.sink
        else {
            unreachable!("a stream mapper writes to a stream");
        };
        let (ns, name, producer) = (ns.clone(), name.clone(), producer.clone());
        let mut rows = mapper
            .map(batch)
            .map_err(|err| Self::row_error(batch_no, err))?;
        let mut offsets: BTreeMap<u32, PartitionOffsets> = BTreeMap::new();
        for range in chunk_ranges(rows.len(), self.chunk_rows) {
            if acks.is_some_and(|acks| acks.is_closed()) {
                return Ok(None);
            }
            if self.stop.is_cancelled() {
                return Err(shutting_down());
            }
            let mut grouped: BTreeMap<u32, (Vec<Record>, Vec<usize>)> = BTreeMap::new();
            for (row, (partition, record)) in rows.drain(..range.len()).enumerate() {
                let entry = grouped.entry(partition).or_default();
                entry.0.push(record);
                entry.1.push(range.start + row);
            }
            let row_map: BTreeMap<u32, Vec<usize>> = grouped
                .iter()
                .map(|(p, (_, rows))| (*p, rows.clone()))
                .collect();
            let records = grouped
                .into_iter()
                .map(|(p, (records, _))| (p, records))
                .collect();
            let acks = producer
                .produce(&ns, &name, records)
                .await
                .map_err(|err| rewrite_record(err, batch_no, &row_map))?;
            let token = ConsistencyToken(
                acks.iter()
                    .map(|a| (a.stream, a.partition, a.last_offset + 1))
                    .collect(),
            );
            self.token.merge(&token);
            for a in acks {
                offsets
                    .entry(a.partition)
                    .and_modify(|o| {
                        o.base_offset = o.base_offset.min(a.base_offset);
                        o.last_offset = o.last_offset.max(a.last_offset);
                    })
                    .or_insert(PartitionOffsets {
                        partition: a.partition,
                        base_offset: a.base_offset,
                        last_offset: a.last_offset,
                    });
            }
            self.written += range.len() as u64;
        }
        Ok(Some(PutAck {
            batch: batch_no,
            rows: batch.num_rows() as u64,
            token: self.token.to_string(),
            offsets: offsets.into_values().collect(),
        }))
    }
}
