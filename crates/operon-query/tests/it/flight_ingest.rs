//! Flight `DoPut` ingest without a network (plan M1.2 Task 13): targets,
//! column mapping, created schemas and chunking.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_flight::sql::{
    CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Float32Type, Schema};
use operon_collection::{
    CollectionSchema, Distance, DocOp, Document, DynamicMapping, FieldKind, PrimaryKey,
    SparseModifier, SparseVector, SparseVectorSpec, partition_of,
};
use operon_query::ServiceError;
use operon_query::flight::FlightConfig;
use operon_query::flight_ingest::{
    CollectionBatchMapper, ID_TYPE_METADATA, IdType, IngestAction, PutTarget, RowError,
    StreamBatchMapper, chunk_ranges, ingest_action, put_target_from_ingest, put_target_from_path,
    schema_from_arrow,
};
use operon_query::sql::collection_arrow_schema;
use serde_json::{Value, json};
use xxhash_rust::xxh3::xxh3_64;

use crate::common::{field, obj, vector};

// ----- builders -----

fn batch(columns: Vec<(Field, ArrayRef)>) -> RecordBatch {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = columns.into_iter().unzip();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("batch")
}

fn col(name: &str, array: ArrayRef) -> (Field, ArrayRef) {
    (Field::new(name, array.data_type().clone(), true), array)
}

fn fixed(dim: i32, rows: Vec<Option<Vec<f32>>>) -> ArrayRef {
    Arc::new(
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            rows.into_iter()
                .map(|row| row.map(|v| v.into_iter().map(Some).collect::<Vec<_>>())),
            dim,
        ),
    )
}

fn floats(rows: Vec<Option<Vec<f32>>>) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.into_iter()
            .map(|row| row.map(|v| v.into_iter().map(Some).collect::<Vec<_>>())),
    ))
}

/// A list column of `item` over `values`, one list per `lengths` entry
/// (`None`: a null list, with no items).
fn list(item: Arc<Field>, values: ArrayRef, lengths: &[Option<usize>]) -> ArrayRef {
    let offsets = OffsetBuffer::from_lengths(lengths.iter().map(|l| l.unwrap_or(0)));
    let nulls = NullBuffer::from(lengths.iter().map(Option::is_some).collect::<Vec<_>>());
    Arc::new(ListArray::try_new(item, offsets, values, Some(nulls)).expect("list"))
}

/// A sparse column of Arrow type `data_type` (a `Struct<indices, values>`).
fn sparse(data_type: &DataType, rows: Vec<Option<(Vec<i64>, Vec<f32>)>>) -> ArrayRef {
    let DataType::Struct(fields) = data_type else {
        panic!("a struct type");
    };
    let item = |name: &str| match fields
        .iter()
        .find(|f| f.name() == name)
        .expect("child")
        .data_type()
    {
        DataType::List(item) => item.clone(),
        other => panic!("a list, got {other}"),
    };
    let (indices_item, values_item) = (item("indices"), item("values"));
    let all_indices: Vec<i64> = rows.iter().flatten().flat_map(|(i, _)| i.clone()).collect();
    let indices_values: ArrayRef = match indices_item.data_type() {
        DataType::UInt32 => Arc::new(UInt32Array::from(
            all_indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            all_indices.iter().map(|&i| i as i32).collect::<Vec<_>>(),
        )),
        _ => Arc::new(Int64Array::from(all_indices)),
    };
    let all_values: Vec<f32> = rows.iter().flatten().flat_map(|(_, v)| v.clone()).collect();
    let lengths_i: Vec<Option<usize>> = rows
        .iter()
        .map(|r| Some(r.as_ref().map_or(0, |(i, _)| i.len())))
        .collect();
    let lengths_v: Vec<Option<usize>> = rows
        .iter()
        .map(|r| Some(r.as_ref().map_or(0, |(_, v)| v.len())))
        .collect();
    let indices = list(indices_item, indices_values, &lengths_i);
    let values = list(
        values_item,
        Arc::new(Float32Array::from(all_values)),
        &lengths_v,
    );
    let columns: Vec<ArrayRef> = fields
        .iter()
        .map(|f| {
            if f.name() == "indices" {
                indices.clone()
            } else {
                values.clone()
            }
        })
        .collect();
    let nulls = NullBuffer::from(rows.iter().map(Option::is_some).collect::<Vec<_>>());
    Arc::new(StructArray::try_new(fields.clone(), columns, Some(nulls)).expect("struct"))
}

/// `Struct<indices: List<index>, values: List<Float32>>`.
fn sparse_type(index: DataType) -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new(
            "indices",
            DataType::List(Arc::new(Field::new("item", index, true))),
            true,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
    ]))
}

fn sv(indices: Vec<u32>, values: Vec<f32>) -> SparseVector {
    SparseVector::new(indices, values).expect("sparse vector")
}

fn upsert(pk: PrimaryKey, source: Value, vectors: &[(&str, Vec<f32>)]) -> Document {
    Document {
        pk,
        source: obj(source),
        vectors: vectors
            .iter()
            .map(|(name, v)| (name.to_string(), v.clone()))
            .collect(),
        sparse_vectors: BTreeMap::new(),
    }
}

/// Vector `v` (dim 4) and sparse vector `s`, dynamic mapping `ignore`.
fn vectors_schema(fields: Vec<operon_collection::FieldSpec>) -> CollectionSchema {
    CollectionSchema::new(fields, vec![vector("v", 4)], DynamicMapping::Ignore).with_sparse_vectors(
        vec![SparseVectorSpec {
            name: "s".to_string(),
            modifier: SparseModifier::None,
        }],
    )
}

fn mapper(arrow: &Schema, schema: &CollectionSchema) -> CollectionBatchMapper {
    CollectionBatchMapper::new(arrow, schema, IdType::Str).expect("mapper")
}

fn refused(result: Result<impl std::fmt::Debug, ServiceError>, needle: &str) {
    match result {
        Err(ServiceError::InvalidArgument(message)) => {
            assert!(message.contains(needle), "{message:?} lacks {needle:?}")
        }
        other => panic!("expected InvalidArgument({needle}), got {other:?}"),
    }
}

fn row_error(result: Result<impl std::fmt::Debug, RowError>, row: usize, column: &str) -> String {
    match result {
        Err(err) => {
            assert_eq!((err.row, err.column.as_str()), (row, column), "{err:?}");
            err.message
        }
        Ok(ok) => panic!("expected a row error at {row}/{column}, got {ok:?}"),
    }
}

// ----- collections -----

#[test]
fn batches_map_to_upserts() {
    let schema = vectors_schema(vec![]);
    let tags = list(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        Arc::new(StringArray::from(vec!["x", "y"])),
        &[Some(2), None],
    );
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1, 2]))),
        col("title", Arc::new(StringArray::from(vec![Some("a"), None]))),
        col("n", Arc::new(Int64Array::from(vec![Some(5), None]))),
        col("tags", tags),
        col(
            "ts",
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_700_000_000_123),
                None,
            ])),
        ),
        col("v", fixed(4, vec![Some(vec![1.0, 2.0, 3.0, 4.0]), None])),
        col(
            "s",
            sparse(
                &sparse_type(DataType::UInt32),
                vec![Some((vec![3, 1], vec![0.5, 0.25])), None],
            ),
        ),
    ]);
    let ops = mapper(input.schema().as_ref(), &schema)
        .map(&input)
        .expect("ops");
    let mut first = upsert(
        PrimaryKey::U64(1),
        json!({"title": "a", "n": 5, "tags": ["x", "y"], "ts": "2023-11-14T22:13:20.123Z"}),
        &[("v", vec![1.0, 2.0, 3.0, 4.0])],
    );
    first
        .sparse_vectors
        .insert("s".to_string(), sv(vec![1, 3], vec![0.25, 0.5]));
    let second = upsert(PrimaryKey::U64(2), json!({}), &[]);
    assert_eq!(ops, vec![DocOp::Upsert(first), DocOp::Upsert(second)]);
}

#[test]
fn a_source_column_is_the_document() {
    let schema = vectors_schema(vec![field("title", FieldKind::Keyword)]);
    let input = batch(vec![
        col("_id", Arc::new(StringArray::from(vec!["k1"]))),
        col(
            "_source",
            Arc::new(StringArray::from(vec![
                r#"{"title":"t","nested":{"a":[1,2]}}"#,
            ])),
        ),
        col("v", fixed(4, vec![Some(vec![0.0, 1.0, 0.0, 0.0])])),
        // A schema field (derived from `_source` on the way out) and the
        // read-only columns of a SQL result are ignored.
        col("title", Arc::new(StringArray::from(vec!["other"]))),
        col("_seq_no", Arc::new(UInt64Array::from(vec![9]))),
        col("_partition", Arc::new(UInt32Array::from(vec![1]))),
        col("_score", Arc::new(Float32Array::from(vec![0.5]))),
    ]);
    let ops = mapper(input.schema().as_ref(), &schema)
        .map(&input)
        .expect("ops");
    assert_eq!(
        ops,
        vec![DocOp::Upsert(upsert(
            PrimaryKey::Str("k1".to_string()),
            json!({"title": "t", "nested": {"a": [1, 2]}}),
            &[("v", vec![0.0, 1.0, 0.0, 0.0])],
        ))]
    );
    // A binary `_source` works too; a non-object is a row error.
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1, 2]))),
        col(
            "_source",
            Arc::new(BinaryArray::from(vec![
                br#"{"a":1}"#.as_slice(),
                b"[1]".as_slice(),
            ])),
        ),
    ]);
    let m = mapper(input.schema().as_ref(), &schema);
    let message = row_error(m.map(&input), 1, "_source");
    assert!(message.contains("JSON object"), "{message}");
    // Any other column next to `_source` is refused.
    let arrow = Schema::new(vec![
        Field::new("_id", DataType::UInt64, false),
        Field::new("_source", DataType::Utf8, false),
        Field::new("extra", DataType::Utf8, true),
    ]);
    refused(
        CollectionBatchMapper::new(&arrow, &schema, IdType::Str),
        "column extra: with a _source column, other columns must be vectors or schema fields",
    );
    // No `_id`, and duplicate names, are refused.
    refused(
        CollectionBatchMapper::new(
            &Schema::new(vec![Field::new("title", DataType::Utf8, true)]),
            &schema,
            IdType::Str,
        ),
        "_id",
    );
    refused(
        CollectionBatchMapper::new(
            &Schema::new(vec![
                Field::new("_id", DataType::UInt64, false),
                Field::new("a", DataType::Utf8, true),
                Field::new("a", DataType::Utf8, true),
            ]),
            &schema,
            IdType::Str,
        ),
        "duplicate column a",
    );
    // A non-finite float in a source column is a row error.
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1]))),
        col("x", Arc::new(Float32Array::from(vec![f32::INFINITY]))),
    ]);
    let m = mapper(input.schema().as_ref(), &schema);
    row_error(m.map(&input), 0, "x");
}

/// The keys `ids` map to under `id_type`, or the row error.
fn ids_of(ids: ArrayRef, id_type: IdType) -> Result<Vec<PrimaryKey>, RowError> {
    let schema = vectors_schema(vec![]);
    let input = batch(vec![col("_id", ids)]);
    let m = CollectionBatchMapper::new(input.schema().as_ref(), &schema, id_type).expect("mapper");
    Ok(m.map(&input)?.iter().map(|op| op.pk().clone()).collect())
}

#[test]
fn id_columns_map_by_type() {
    let uuid = "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e";
    let uuid_bytes: [u8; 16] = [
        0x01, 0x90, 0xf5, 0xc4, 0x6c, 0x1e, 0x7b, 0x3a, 0x9d, 0x2e, 0x4f, 0x5a, 0x6b, 0x7c, 0x8d,
        0x9e,
    ];
    assert_eq!(
        ids_of(Arc::new(UInt64Array::from(vec![7, u64::MAX])), IdType::Str).unwrap(),
        [PrimaryKey::U64(7), PrimaryKey::U64(u64::MAX)]
    );
    assert_eq!(
        ids_of(Arc::new(UInt32Array::from(vec![7])), IdType::Str).unwrap(),
        [PrimaryKey::U64(7)]
    );
    assert_eq!(
        ids_of(Arc::new(Int64Array::from(vec![3])), IdType::Str).unwrap(),
        [PrimaryKey::U64(3)]
    );
    let message = row_error(
        ids_of(Arc::new(Int64Array::from(vec![3, -1])), IdType::Str),
        1,
        "_id",
    );
    assert!(message.contains("negative"), "{message}");
    assert_eq!(
        ids_of(Arc::new(StringArray::from(vec!["abc", "42"])), IdType::Str).unwrap(),
        [
            PrimaryKey::Str("abc".to_string()),
            PrimaryKey::Str("42".to_string())
        ]
    );
    assert_eq!(
        ids_of(Arc::new(StringArray::from(vec!["42"])), IdType::U64).unwrap(),
        [PrimaryKey::U64(42)]
    );
    row_error(
        ids_of(Arc::new(StringArray::from(vec!["1", "042"])), IdType::U64),
        1,
        "_id",
    );
    assert_eq!(
        ids_of(
            Arc::new(StringArray::from(vec![
                uuid,
                "0190f5c46c1e7b3a9d2e4f5a6b7c8d9e"
            ])),
            IdType::Uuid
        )
        .unwrap(),
        [PrimaryKey::Uuid(uuid_bytes), PrimaryKey::Uuid(uuid_bytes)]
    );
    row_error(
        ids_of(
            Arc::new(StringArray::from(vec![uuid, "nope"])),
            IdType::Uuid,
        ),
        1,
        "_id",
    );
    assert_eq!(
        ids_of(
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![uuid_bytes.to_vec()].into_iter()).unwrap()
            ),
            IdType::Str
        )
        .unwrap(),
        [PrimaryKey::Uuid(uuid_bytes)]
    );
    let message = row_error(
        ids_of(
            Arc::new(UInt64Array::from(vec![Some(1), None])),
            IdType::Str,
        ),
        1,
        "_id",
    );
    assert!(message.contains("null"), "{message}");
    // The id type comes from the `_id` field's metadata, else the fallback.
    let arrow = Schema::new(vec![
        Field::new("_id", DataType::Utf8, false)
            .with_metadata([(ID_TYPE_METADATA.to_string(), "uuid".to_string())].into()),
    ]);
    assert_eq!(
        IdType::of_schema(&arrow, Some("u64")).unwrap(),
        IdType::Uuid
    );
    let plain = Schema::new(vec![Field::new("_id", DataType::Utf8, false)]);
    assert_eq!(IdType::of_schema(&plain, Some("u64")).unwrap(), IdType::U64);
    assert_eq!(IdType::of_schema(&plain, None).unwrap(), IdType::Str);
    refused(IdType::of_schema(&plain, Some("int")), "operon-id-type");
    // A column that cannot hold a key is refused.
    refused(
        CollectionBatchMapper::new(
            &Schema::new(vec![Field::new("_id", DataType::Float64, false)]),
            &vectors_schema(vec![]),
            IdType::Str,
        ),
        "cannot hold a primary key",
    );
}

#[test]
fn vector_columns_are_checked() {
    let schema = vectors_schema(vec![]);
    // `List<Float32>` is accepted for a dense vector.
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1]))),
        col("v", floats(vec![Some(vec![1.0, 0.0, 0.0, 0.0])])),
    ]);
    let ops = mapper(input.schema().as_ref(), &schema)
        .map(&input)
        .expect("ops");
    assert_eq!(
        ops,
        vec![DocOp::Upsert(upsert(
            PrimaryKey::U64(1),
            json!({}),
            &[("v", vec![1.0, 0.0, 0.0, 0.0])]
        ))]
    );
    // A NaN element.
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1, 2]))),
        col(
            "v",
            fixed(
                4,
                vec![
                    Some(vec![1.0, 0.0, 0.0, 0.0]),
                    Some(vec![f32::NAN, 0.0, 0.0, 0.0]),
                ],
            ),
        ),
    ]);
    let message = row_error(mapper(input.schema().as_ref(), &schema).map(&input), 1, "v");
    assert!(message.contains("not finite"), "{message}");
    // A sparse vector with a repeated index (Int64 indices are accepted).
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1, 2]))),
        col(
            "s",
            sparse(
                &sparse_type(DataType::Int64),
                vec![
                    Some((vec![1], vec![1.0])),
                    Some((vec![4, 4], vec![1.0, 2.0])),
                ],
            ),
        ),
    ]);
    let m = mapper(input.schema().as_ref(), &schema);
    let message = row_error(m.map(&input), 1, "s");
    assert!(message.contains('4'), "{message}");
    // An index outside u32.
    let input = batch(vec![
        col("_id", Arc::new(UInt64Array::from(vec![1]))),
        col(
            "s",
            sparse(
                &sparse_type(DataType::Int64),
                vec![Some((vec![-1], vec![1.0]))],
            ),
        ),
    ]);
    row_error(mapper(input.schema().as_ref(), &schema).map(&input), 0, "s");
    // Vector-shaped columns that name no vector.
    for (name, data_type) in [
        (
            "w",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
        ),
        ("t", sparse_type(DataType::UInt32)),
    ] {
        let arrow = Schema::new(vec![
            Field::new("_id", DataType::UInt64, false),
            Field::new(name, data_type, true),
        ]);
        refused(
            CollectionBatchMapper::new(&arrow, &schema, IdType::Str),
            &format!("column {name} looks like a vector, but the collection has no vector {name}"),
        );
    }
    // A vector column of the wrong type.
    let arrow = Schema::new(vec![
        Field::new("_id", DataType::UInt64, false),
        Field::new("v", DataType::Utf8, true),
    ]);
    refused(
        CollectionBatchMapper::new(&arrow, &schema, IdType::Str),
        "column v",
    );
}

#[test]
fn a_sql_result_maps_back_to_its_documents() {
    let schema = vectors_schema(vec![
        field("title", FieldKind::Keyword),
        field("n", FieldKind::I64),
    ]);
    let mut originals = vec![
        upsert(
            PrimaryKey::U64(1),
            json!({"title": "a", "n": 1, "extra": {"deep": true}}),
            &[("v", vec![1.0, 0.0, 0.0, 0.0])],
        ),
        upsert(PrimaryKey::U64(2), json!({"title": "b"}), &[]),
    ];
    originals[0]
        .sparse_vectors
        .insert("s".to_string(), sv(vec![2, 7], vec![0.5, 1.5]));
    // The table's shape (Task 10 rule 2), with the key type as metadata.
    let table = collection_arrow_schema(&schema);
    let fields: Vec<Field> = table
        .fields()
        .iter()
        .map(|f| {
            let f = f.as_ref().clone();
            if f.name() == "_id" {
                f.with_metadata([(ID_TYPE_METADATA.to_string(), "u64".to_string())].into())
            } else {
                f
            }
        })
        .collect();
    let arrow = Arc::new(Schema::new(fields));
    let columns: Vec<ArrayRef> = arrow
        .fields()
        .iter()
        .map(|f| -> ArrayRef {
            match f.name().as_str() {
                "_id" => Arc::new(StringArray::from(vec!["1", "2"])),
                "_source" => Arc::new(StringArray::from(
                    originals
                        .iter()
                        .map(|d| serde_json::to_string(&d.source).unwrap())
                        .collect::<Vec<_>>(),
                )),
                "_seq_no" => Arc::new(UInt64Array::from(vec![Some(4), Some(5)])),
                "_partition" => Arc::new(UInt32Array::from(vec![Some(0), Some(1)])),
                "title" => Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                "n" => Arc::new(Int64Array::from(vec![Some(1), None])),
                "v" => fixed(4, vec![Some(vec![1.0, 0.0, 0.0, 0.0]), None]),
                "s" => sparse(
                    f.data_type(),
                    vec![Some((vec![2, 7], vec![0.5, 1.5])), None],
                ),
                other => panic!("unexpected column {other}"),
            }
        })
        .collect();
    let input = RecordBatch::try_new(arrow.clone(), columns).expect("batch");
    let id_type = IdType::of_schema(&arrow, None).expect("id type");
    let ops = CollectionBatchMapper::new(&arrow, &schema, id_type)
        .expect("mapper")
        .map(&input)
        .expect("ops");
    assert_eq!(
        ops,
        originals.into_iter().map(DocOp::Upsert).collect::<Vec<_>>()
    );
}

// ----- streams -----

/// One record header: key and value.
type Header<'a> = (&'a str, Option<&'a [u8]>);

/// `List<Struct<key: Utf8, value: Binary>>` of one header list per row.
fn headers(rows: Vec<Vec<Header<'_>>>) -> ArrayRef {
    let entry_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Binary, true),
    ]);
    let keys: Vec<&str> = rows.iter().flatten().map(|(k, _)| *k).collect();
    let values: Vec<Option<&[u8]>> = rows.iter().flatten().map(|(_, v)| *v).collect();
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(BinaryArray::from(values)) as ArrayRef,
        ],
        None,
    )
    .expect("entries");
    let lengths: Vec<Option<usize>> = rows.iter().map(|r| Some(r.len())).collect();
    list(
        Arc::new(Field::new("item", DataType::Struct(entry_fields), true)),
        Arc::new(entries),
        &lengths,
    )
}

/// A mapped record: partition, key, value, headers and timestamp.
type RecordParts = (
    u32,
    Option<Bytes>,
    Option<Bytes>,
    Vec<(String, Option<Bytes>)>,
    i64,
);

fn stream_mapper(arrow: &Schema, fixed: Option<u32>, partitions: u32) -> StreamBatchMapper {
    StreamBatchMapper::new(arrow, fixed, partitions).expect("mapper")
}

#[test]
fn stream_batches_map_to_records() {
    let input = batch(vec![
        col(
            "key",
            Arc::new(BinaryArray::from(vec![
                Some(b"k0".as_slice()),
                None,
                Some(b"k2".as_slice()),
            ])),
        ),
        col(
            "value",
            Arc::new(StringArray::from(vec![Some("v0"), Some("v1"), None])),
        ),
        col(
            "headers",
            headers(vec![
                vec![("a", Some(b"1".as_slice())), ("a", None)],
                vec![],
                vec![("b", Some(b"2".as_slice()))],
            ]),
        ),
        col(
            "timestamp",
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_123_456),
                None,
                Some(5_999),
            ])),
        ),
        col("partition", Arc::new(UInt32Array::from(vec![1, 0, 1]))),
    ]);
    let rows = stream_mapper(input.schema().as_ref(), None, 2)
        .map(&input)
        .expect("rows");
    let got: Vec<RecordParts> = rows
        .into_iter()
        .map(|(p, r)| (p, r.key, r.value, r.headers, r.timestamp_ms))
        .collect();
    let b = |s: &str| Some(Bytes::copy_from_slice(s.as_bytes()));
    assert_eq!(
        got,
        vec![
            (
                1,
                b("k0"),
                b("v0"),
                vec![("a".to_string(), b("1")), ("a".to_string(), None)],
                1_700_000_000_123
            ),
            (0, None, b("v1"), vec![], -1),
            (1, b("k2"), None, vec![("b".to_string(), b("2"))], 5),
        ]
    );
    // Utf8 keys and Int64 millisecond timestamps.
    let input = batch(vec![
        col("key", Arc::new(StringArray::from(vec!["k"]))),
        col("value", Arc::new(BinaryArray::from(vec![b"v".as_slice()]))),
        col("timestamp", Arc::new(Int64Array::from(vec![42]))),
        col("partition", Arc::new(Int32Array::from(vec![0]))),
    ]);
    let rows = stream_mapper(input.schema().as_ref(), None, 1)
        .map(&input)
        .expect("rows");
    assert_eq!(rows[0].1.key, b("k"));
    assert_eq!(rows[0].1.timestamp_ms, 42);
    // Refusals: an unknown column, a partition out of range, a partition
    // column with a descriptor partition.
    let unknown = Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("other", DataType::Utf8, true),
    ]);
    refused(StreamBatchMapper::new(&unknown, None, 2), "column other");
    let input = batch(vec![
        col("key", Arc::new(StringArray::from(vec!["k", "k"]))),
        col("partition", Arc::new(Int32Array::from(vec![0, 2]))),
    ]);
    let message = row_error(
        stream_mapper(input.schema().as_ref(), None, 2).map(&input),
        1,
        "partition",
    );
    assert!(message.contains("out of range"), "{message}");
    refused(
        StreamBatchMapper::new(input.schema().as_ref(), Some(0), 2),
        "cannot be combined",
    );
    refused(
        StreamBatchMapper::new(
            &Schema::new(vec![Field::new("key", DataType::Utf8, true)]),
            Some(2),
            2,
        ),
        "out of range",
    );
    // A descriptor partition pins every row.
    let input = batch(vec![col(
        "key",
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    )]);
    let rows = stream_mapper(input.schema().as_ref(), Some(1), 3)
        .map(&input)
        .expect("rows");
    assert!(rows.iter().all(|(p, _)| *p == 1));
}

#[test]
fn stream_rows_without_a_partition_go_by_key_hash() {
    let pk = PrimaryKey::Str("doc-7".to_string());
    let canonical = pk.canonical();
    let keys: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta", &canonical];
    let input = batch(vec![col("key", Arc::new(BinaryArray::from(keys.clone())))]);
    let rows = stream_mapper(input.schema().as_ref(), None, 3)
        .map(&input)
        .expect("rows");
    for ((p, record), key) in rows.iter().zip(&keys) {
        assert_eq!(*p, (xxh3_64(key) % 3) as u32);
        assert_eq!(record.key.as_deref(), Some(*key));
    }
    // A canonical primary key lands where `partition_of` puts it.
    assert_eq!(rows[4].0, partition_of(&pk, 3));
    // A null key: a row error, except on a one-partition stream.
    let input = batch(vec![col(
        "key",
        Arc::new(BinaryArray::from(vec![Some(b"a".as_slice()), None])),
    )]);
    let message = row_error(
        stream_mapper(input.schema().as_ref(), None, 3).map(&input),
        1,
        "key",
    );
    assert_eq!(message, "a null key needs a partition column");
    let rows = stream_mapper(input.schema().as_ref(), None, 1)
        .map(&input)
        .expect("rows");
    assert_eq!(rows[1].0, 0);
}

// ----- targets -----

fn path(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| p.to_string()).collect()
}

fn ingest(table: &str) -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: None,
        table: table.to_string(),
        schema: None,
        catalog: None,
        temporary: false,
        transaction_id: None,
        options: Default::default(),
    }
}

fn options(
    if_not_exist: TableNotExistOption,
    if_exists: TableExistsOption,
) -> TableDefinitionOptions {
    TableDefinitionOptions {
        if_not_exist: if_not_exist as i32,
        if_exists: if_exists as i32,
    }
}

#[test]
fn targets_come_from_paths_and_ingest_commands() {
    // Paths.
    assert_eq!(
        put_target_from_path(&path(&["collections", "kb"])).unwrap(),
        PutTarget::Collection {
            name: "kb".to_string()
        }
    );
    assert_eq!(
        put_target_from_path(&path(&["streams", "events"])).unwrap(),
        PutTarget::Stream {
            name: "events".to_string(),
            partition: None
        }
    );
    assert_eq!(
        put_target_from_path(&path(&["streams", "events", "3"])).unwrap(),
        PutTarget::Stream {
            name: "events".to_string(),
            partition: Some(3)
        }
    );
    for bad in [
        vec![],
        path(&["collections"]),
        path(&["collections", "a", "b"]),
        path(&["streams", "e", "03"]),
        path(&["streams", "e", "-1"]),
        path(&["streams", "e", "x"]),
        path(&["tables", "t"]),
    ] {
        refused(put_target_from_path(&bad), "unknown DoPut path");
    }
    // Ingest commands: the namespace is the catalog, else the default.
    let (ns, target) = put_target_from_ingest(&ingest("kb"), "w").unwrap();
    assert_eq!(
        (ns.as_str(), target),
        (
            "w",
            PutTarget::Collection {
                name: "kb".to_string()
            }
        )
    );
    let cmd = CommandStatementIngest {
        catalog: Some("other".to_string()),
        schema: Some("collections".to_string()),
        ..ingest("kb")
    };
    assert_eq!(put_target_from_ingest(&cmd, "w").unwrap().0, "other");
    let cmd = CommandStatementIngest {
        schema: Some("streams".to_string()),
        ..ingest("events")
    };
    assert_eq!(
        put_target_from_ingest(&cmd, "w").unwrap().1,
        PutTarget::Stream {
            name: "events".to_string(),
            partition: None
        }
    );
    let cmd = CommandStatementIngest {
        schema: Some("public".to_string()),
        ..ingest("kb")
    };
    refused(
        put_target_from_ingest(&cmd, "w"),
        "unknown schema public: use collections or streams",
    );
    let cmd = CommandStatementIngest {
        temporary: true,
        ..ingest("kb")
    };
    refused(
        put_target_from_ingest(&cmd, "w"),
        "temporary tables are not supported",
    );
    let cmd = CommandStatementIngest {
        transaction_id: Some(Bytes::from_static(b"t")),
        ..ingest("kb")
    };
    refused(
        put_target_from_ingest(&cmd, "w"),
        "transactions are not supported",
    );
    // Rule 1.2's table.
    use TableExistsOption as E;
    use TableNotExistOption as N;
    let collection = PutTarget::Collection {
        name: "kb".to_string(),
    };
    let stream = PutTarget::Stream {
        name: "events".to_string(),
        partition: None,
    };
    let action = |o: Option<TableDefinitionOptions>, t: &PutTarget, exists: bool| {
        ingest_action(o.as_ref(), t, exists)
    };
    // Exists.
    for if_exists in [E::Unspecified, E::Append] {
        for target in [&collection, &stream] {
            assert_eq!(
                action(Some(options(N::Create, if_exists)), target, true).unwrap(),
                IngestAction::Append
            );
        }
    }
    assert_eq!(
        action(None, &collection, true).unwrap(),
        IngestAction::Append
    );
    assert!(matches!(
        action(Some(options(N::Create, E::Fail)), &collection, true),
        Err(ServiceError::AlreadyExists(name)) if name == "kb"
    ));
    for exists in [true, false] {
        refused(
            action(Some(options(N::Create, E::Replace)), &collection, exists),
            "replace is not supported",
        );
    }
    // Missing.
    assert_eq!(
        action(Some(options(N::Create, E::Fail)), &collection, false).unwrap(),
        IngestAction::Create
    );
    refused(
        action(Some(options(N::Create, E::Append)), &stream, false),
        "create streams through the native API",
    );
    for if_not_exist in [N::Unspecified, N::Fail] {
        assert!(matches!(
            action(Some(options(if_not_exist, E::Append)), &collection, false),
            Err(ServiceError::NotFound {
                kind: "collection",
                ..
            })
        ));
        assert!(matches!(
            action(Some(options(if_not_exist, E::Append)), &stream, false),
            Err(ServiceError::NotFound { kind: "stream", .. })
        ));
    }
    assert!(matches!(
        action(None, &collection, false),
        Err(ServiceError::NotFound { .. })
    ));
}

#[test]
fn an_ingest_created_schema_follows_the_arrow_schema() {
    let arrow = Schema::new(vec![
        Field::new("_id", DataType::UInt64, false),
        Field::new("title", DataType::Utf8, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 8),
            true,
        ),
        Field::new(
            "loose",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
        Field::new("terms", sparse_type(DataType::UInt32), true),
        Field::new("n", DataType::Int64, true),
    ]);
    let schema = schema_from_arrow(&arrow).expect("schema");
    assert!(schema.fields.is_empty());
    assert_eq!(schema.dynamic, DynamicMapping::Map);
    assert_eq!(schema.vectors.len(), 1);
    let v = &schema.vectors[0];
    assert_eq!(
        (v.name.as_str(), v.dim, v.distance),
        ("embedding", 8, Distance::Cosine)
    );
    assert_eq!(v, &vector("embedding", 8));
    assert_eq!(
        schema.sparse_vectors,
        vec![SparseVectorSpec {
            name: "terms".to_string(),
            modifier: SparseModifier::None,
        }]
    );
    // The mapper accepts the stream against the created schema: the scalar
    // and `List<Float32>` columns become source keys.
    CollectionBatchMapper::new(&arrow, &schema, IdType::Str).expect("mapper");
    // `_id` is still required.
    refused(
        schema_from_arrow(&Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        "_id",
    );
}

#[test]
fn batches_are_written_in_chunks() {
    let chunks = chunk_ranges(25_000, 10_000);
    assert_eq!(chunks, vec![0..10_000, 10_000..20_000, 20_000..25_000]);
    // Every row once, in row order.
    let rows: Vec<usize> = chunks.into_iter().flatten().collect();
    assert_eq!(rows, (0..25_000).collect::<Vec<_>>());
    assert!(chunk_ranges(0, 10_000).is_empty());
    assert_eq!(chunk_ranges(3, 1), vec![0..1, 1..2, 2..3]);
    // The configured chunk size is bounded by MAX_WRITE_OPS.
    let config = FlightConfig::default();
    assert_eq!(config.put_chunk_rows, 10_000);
    assert_eq!(config.max_message_bytes, 64 << 20);
    config.validate().expect("valid");
    for bad in [0, 10_001] {
        let config = FlightConfig {
            put_chunk_rows: bad,
            ..FlightConfig::default()
        };
        assert!(config.validate().is_err(), "{bad}");
    }
}
