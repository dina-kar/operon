//! Collection schemas map to Tantivy (plan M1.1 Task 8 rules 3–4, Rulings
//! 26–27), and every filter kind works on a Json field.

use std::ops::Bound;
use std::sync::Arc;

use crate::common::{doc, field, json, sparse, text};
use operon_cache::{RangeCache, RangeCacheConfig};
use operon_collection::{
    CollectionError, CollectionSchema, DynamicMapping, FIELD_PRESENCE_FIELD, FieldKind, FieldMap,
    PK_FIELD, PrimaryKey, ROWID_FIELD, SPARSE_PRESENT, SparseModifier, SparseVectorSpec,
    TantivyLayout, check_document, decode_sparse_weights, encode_sparse_weights, tantivy_layout,
    to_tantivy_doc,
};
use operon_quickwit::query::query_ast::{BuildTantivyAstContext, FieldPresenceQuery, QueryAst};
use operon_quickwit::shim::consts::FIELD_PRESENCE_FIELD_NAME;
use operon_store::Store;
use operon_text::{OperonStorage, build_split, open_split, warm_up_all};
use serde_json::{Value, json};
use tantivy::collector::DocSetCollector;
use tantivy::query::{ExistsQuery, Query, RangeQuery, TermQuery};
use tantivy::schema::document::Value as _;
use tantivy::schema::{
    BytesOptions, DateOptions, DateTimePrecision, FieldEntry, IndexRecordOption, JsonObjectOptions,
    NumericOptions, TextFieldIndexing, TextOptions,
};
use tantivy::{DateTime, Searcher, TantivyDocument, Term};

fn raw_basic() -> TextFieldIndexing {
    TextFieldIndexing::default()
        .set_tokenizer("raw")
        .set_index_option(IndexRecordOption::Basic)
}

fn english_body() -> operon_collection::FieldSpec {
    let mut body = text("body");
    body.kind = FieldKind::Text {
        analyzer: "english".to_string(),
        positions: false,
    };
    body
}

fn kinds_schema() -> CollectionSchema {
    let mut label = field("label", FieldKind::Keyword);
    label.indexed = false;
    let mut day = field("day", FieldKind::Date);
    day.fast = false;
    let mut count = field("count", FieldKind::I64);
    count.fast = false;
    let schema = CollectionSchema::new(
        vec![
            text("title"),
            english_body(),
            field("tag", FieldKind::Keyword),
            label,
            field("id", FieldKind::Uuid),
            field("n", FieldKind::I64),
            count,
            field("x", FieldKind::F64),
            field("b", FieldKind::Bool),
            field("d", FieldKind::Date),
            day,
            json("payload", ""),
        ],
        vec![],
        DynamicMapping::Ignore,
    )
    .with_sparse_vectors(vec![SparseVectorSpec {
        name: "s".to_string(),
        modifier: SparseModifier::None,
    }]);
    schema.validate().expect("valid schema");
    schema
}

#[test]
fn each_kind_maps_to_its_tantivy_options() {
    let layout = tantivy_layout(&kinds_schema());
    assert_eq!(FIELD_PRESENCE_FIELD, FIELD_PRESENCE_FIELD_NAME);

    let json_typed = || {
        JsonObjectOptions::default()
            .set_indexing_options(raw_basic())
            .set_expand_dots_enabled()
            .set_fast(None)
    };
    let expected = [
        FieldEntry::new_bytes(
            PK_FIELD.into(),
            BytesOptions::default().set_indexed().set_fast(),
        ),
        FieldEntry::new_u64(ROWID_FIELD.into(), NumericOptions::default().set_fast()),
        FieldEntry::new_u64(
            FIELD_PRESENCE_FIELD.into(),
            NumericOptions::default().set_indexed(),
        ),
        FieldEntry::new_text(
            "title".into(),
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("standard")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                    .set_fieldnorms(true),
            ),
        ),
        FieldEntry::new_text(
            "body".into(),
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("english")
                    .set_index_option(IndexRecordOption::WithFreqs)
                    .set_fieldnorms(true),
            ),
        ),
        FieldEntry::new_text(
            "tag".into(),
            TextOptions::default()
                .set_indexing_options(raw_basic().set_fieldnorms(false))
                .set_fast(Some("raw")),
        ),
        FieldEntry::new_text("label".into(), TextOptions::default().set_fast(Some("raw"))),
        FieldEntry::new_text(
            "id".into(),
            TextOptions::default()
                .set_indexing_options(raw_basic().set_fieldnorms(false))
                .set_fast(Some("raw")),
        ),
        FieldEntry::new_i64(
            "n".into(),
            NumericOptions::default().set_indexed().set_fast(),
        ),
        FieldEntry::new_i64("count".into(), NumericOptions::default().set_indexed()),
        FieldEntry::new_f64(
            "x".into(),
            NumericOptions::default().set_indexed().set_fast(),
        ),
        FieldEntry::new_bool(
            "b".into(),
            NumericOptions::default().set_indexed().set_fast(),
        ),
        FieldEntry::new_date(
            "d".into(),
            DateOptions::default()
                .set_precision(DateTimePrecision::Milliseconds)
                .set_indexed()
                .set_fast(),
        ),
        FieldEntry::new_date(
            "day".into(),
            DateOptions::default()
                .set_precision(DateTimePrecision::Milliseconds)
                .set_indexed(),
        ),
        FieldEntry::new_json(
            "payload".into(),
            JsonObjectOptions::default()
                .set_indexing_options(raw_basic())
                .set_expand_dots_enabled()
                .set_fast(Some("raw")),
        ),
        FieldEntry::new_json(
            "_text.payload".into(),
            JsonObjectOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("standard")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                )
                .set_expand_dots_enabled(),
        ),
        FieldEntry::new_json("_date.payload".into(), json_typed()),
        FieldEntry::new_text(
            "_null.payload".into(),
            TextOptions::default().set_indexing_options(raw_basic()),
        ),
        FieldEntry::new_json("_count.payload".into(), json_typed()),
        FieldEntry::new_u64("_sparse.s".into(), NumericOptions::default().set_indexed()),
        FieldEntry::new_bytes("_sparse_w.s".into(), BytesOptions::default().set_fast()),
    ];
    let entries: Vec<&FieldEntry> = layout.schema.fields().map(|(_, entry)| entry).collect();
    assert_eq!(entries.len(), expected.len());
    for (entry, expected) in entries.iter().zip(&expected) {
        assert_eq!(*entry, expected, "{}", expected.name());
    }
    for (entry, expected) in entries.iter().zip(&expected) {
        assert!(!entry.is_stored(), "{} is not stored", expected.name());
    }

    // The layout points at those fields.
    let name = |field| layout.schema.get_field_name(field).to_string();
    assert_eq!(name(layout.pk), PK_FIELD);
    assert_eq!(name(layout.rowid), ROWID_FIELD);
    assert_eq!(name(layout.presence), FIELD_PRESENCE_FIELD);
    assert_eq!(layout.fields.len(), 12);
    let FieldMap::Plain(title) = layout.fields[0] else {
        panic!("title is a plain field");
    };
    assert_eq!(name(title), "title");
    let FieldMap::Json {
        main,
        text,
        date,
        nulls,
        counts,
    } = layout.fields[11]
    else {
        panic!("payload is a Json field");
    };
    assert_eq!(
        [main, text, date, nulls, counts].map(name),
        [
            "payload",
            "_text.payload",
            "_date.payload",
            "_null.payload",
            "_count.payload"
        ]
    );
    assert_eq!(name(layout.sparse[0].postings), "_sparse.s");
    assert_eq!(name(layout.sparse[0].weights), "_sparse_w.s");
}

/// The Tantivy documents of `sources` (row id 100 + i).
fn tantivy_docs(
    schema: &CollectionSchema,
    layout: &TantivyLayout,
    docs: &[operon_collection::Document],
) -> Vec<TantivyDocument> {
    docs.iter()
        .enumerate()
        .map(|(i, doc)| {
            let extracted = check_document(schema, doc).expect("valid document");
            to_tantivy_doc(layout, schema, doc, &extracted, 100 + i as u64)
        })
        .collect()
}

/// Builds a split of `docs`, puts it, opens it and warms it.
async fn searcher(layout: &TantivyLayout, docs: Vec<TantivyDocument>) -> Searcher {
    let split = build_split(layout.schema.clone(), docs).expect("split builds");
    let store = Store::in_memory();
    store
        .put_if_absent("c/text/splits/s.split", split.bytes.clone())
        .await
        .expect("put split");
    let cache = RangeCache::new(store.clone(), RangeCacheConfig::default())
        .await
        .expect("cache builds");
    let storage = Arc::new(OperonStorage::new(store, cache, "c/"));
    let index = open_split(
        storage,
        "text/splits/s.split",
        split.bytes.len() as u64,
        split.footer_range,
    )
    .await
    .expect("split opens");
    warm_up_all(&index).await.expect("split warms")
}

fn hits(searcher: &Searcher, query: &dyn Query) -> Vec<u32> {
    let mut docs: Vec<u32> = searcher
        .search(query, &DocSetCollector)
        .expect("search")
        .into_iter()
        .map(|address| address.doc_id)
        .collect();
    docs.sort_unstable();
    docs
}

fn json_term(field: tantivy::schema::Field, path: &str) -> Term {
    Term::from_field_json_path(field, path, true)
}

fn json_str(field: tantivy::schema::Field, path: &str, value: &str) -> TermQuery {
    let mut term = json_term(field, path);
    term.append_type_and_str(value);
    TermQuery::new(term, IndexRecordOption::Basic)
}

fn json_range<T: tantivy::fastfield::FastValue>(
    field: tantivy::schema::Field,
    path: &str,
    low: T,
    high: Bound<T>,
) -> RangeQuery {
    let mut lower = json_term(field, path);
    lower.append_type_and_fast_value(low);
    let upper = high.map(|high| {
        let mut upper = json_term(field, path);
        upper.append_type_and_fast_value(high);
        upper
    });
    RangeQuery::new(Bound::Included(lower), upper)
}

fn pk(i: u64) -> PrimaryKey {
    PrimaryKey::U64(i)
}

#[tokio::test]
async fn json_paths_support_every_filter_kind() {
    let schema = CollectionSchema::new(vec![json("payload", "")], vec![], DynamicMapping::Ignore);
    schema.validate().expect("valid schema");
    let layout = tantivy_layout(&schema);
    let docs = [
        doc(
            pk(0),
            json!({"city":"Berlin","tags":["a","b"],"n":3,"x":null,"when":"2024-01-02","o":{"k":[1,2]},"at":"2024-01-02T03:04:05Z"}),
        ),
        doc(
            pk(1),
            json!({"city":"Paris","tags":"c","n":10,"o":{"k":1},"when":"2023-12-31"}),
        ),
        // A numeric string is a string, and a digits string is not a date.
        doc(pk(2), json!({"city":"Rome","n":"3","when":"1704153600000"})),
    ];
    let searcher = searcher(&layout, tantivy_docs(&schema, &layout, &docs)).await;
    let FieldMap::Json {
        main,
        text,
        date,
        nulls,
        counts,
    } = layout.fields[0]
    else {
        panic!("payload is a Json field");
    };

    // Term on `n`: strings are raw terms.
    assert_eq!(hits(&searcher, &json_str(main, "city", "Berlin")), [0]);
    assert!(hits(&searcher, &json_str(main, "city", "berlin")).is_empty());
    // An RFC 3339 string stays a string in `n` (Tantivy's own conversion
    // would make it a date).
    assert_eq!(
        hits(&searcher, &json_str(main, "at", "2024-01-02T03:04:05Z")),
        [0]
    );
    assert_eq!(hits(&searcher, &json_str(main, "tags", "b")), [0]);
    // Full text on `_text.n`.
    let berlin = json_str(text, "city", "berlin");
    assert_eq!(hits(&searcher, &berlin), [0]);
    // A numeric range on `n` is type-strict.
    assert_eq!(
        hits(
            &searcher,
            &json_range(main, "n", 2i64, Bound::Included(4i64))
        ),
        [0]
    );
    assert!(
        hits(
            &searcher,
            &json_range(main, "city", i64::MIN, Bound::Unbounded)
        )
        .is_empty()
    );
    // `IsNull` is a term of `_null.n`.
    let null_x = TermQuery::new(Term::from_field_text(nulls, "x"), IndexRecordOption::Basic);
    assert_eq!(hits(&searcher, &null_x), [0]);
    // `ValuesCount` is a range on `_count.n`.
    assert_eq!(
        hits(
            &searcher,
            &json_range(counts, "tags", 2u64, Bound::Included(2u64))
        ),
        [0]
    );
    assert_eq!(
        hits(
            &searcher,
            &json_range(counts, "o.k", 2u64, Bound::Included(2u64))
        ),
        [0]
    );
    assert_eq!(
        hits(
            &searcher,
            &json_range(counts, "tags", 1u64, Bound::Included(1u64))
        ),
        [1]
    );
    assert_eq!(
        hits(
            &searcher,
            &json_range(counts, "o", 1u64, Bound::Included(1u64))
        ),
        [0, 1]
    );
    // `Exists` is an `ExistsQuery` on `n` with JSON subpaths.
    assert_eq!(
        hits(&searcher, &ExistsQuery::new("payload.o.k".into(), true)),
        [0, 1]
    );
    assert_eq!(
        hits(&searcher, &ExistsQuery::new("payload.o".into(), true)),
        [0, 1]
    );
    assert!(hits(&searcher, &ExistsQuery::new("payload.missing".into(), true)).is_empty());
    assert!(hits(&searcher, &ExistsQuery::new("payload.x".into(), true)).is_empty());
    // A date range on `_date.n`.
    let since_2024 = DateTime::from_timestamp_millis(1_704_067_200_000);
    assert_eq!(
        hits(
            &searcher,
            &json_range(date, "when", since_2024, Bound::Unbounded)
        ),
        [0]
    );
    assert_eq!(
        hits(
            &searcher,
            &json_range(date, "at", since_2024, Bound::Unbounded)
        ),
        [0]
    );
    let since_2023 = DateTime::from_timestamp_millis(1_672_531_200_000);
    assert_eq!(
        hits(
            &searcher,
            &json_range(date, "when", since_2023, Bound::Unbounded)
        ),
        [0, 1]
    );
}

#[tokio::test]
async fn exists_via_field_presence_matches_the_vendored_query_ast() {
    let mut schema = CollectionSchema::new(
        vec![
            text("title"),
            field("tag", FieldKind::Keyword),
            field("label", FieldKind::Keyword),
            field("n", FieldKind::I64),
        ],
        vec![],
        DynamicMapping::Ignore,
    );
    // `title` and `tag` are not fast (the presence field answers), `label`
    // and `n` are (Tantivy's `ExistsQuery` on the fast column answers).
    schema.fields[1].fast = false;
    assert!(schema.fields[2].fast && schema.fields[3].fast);
    schema.validate().expect("valid schema");
    let layout = tantivy_layout(&schema);
    let docs = [
        doc(pk(0), json!({"title": "a", "label": "l", "n": 1})),
        doc(pk(1), json!({"tag": "x", "n": [2, 3]})),
        doc(pk(2), json!({"title": ["b", "c"], "tag": "y"})),
        doc(pk(3), json!({"title": null, "tag": [], "label": ["m"]})),
    ];
    let searcher = searcher(&layout, tantivy_docs(&schema, &layout, &docs)).await;
    let context = BuildTantivyAstContext::for_test(&layout.schema);
    for (field, expected) in [
        ("title", vec![0, 2]),
        ("tag", vec![1, 2]),
        ("label", vec![0, 3]),
        ("n", vec![0, 1]),
    ] {
        let ast = QueryAst::FieldPresence(FieldPresenceQuery {
            field: field.to_string(),
        });
        let query = ast.build_tantivy_query(&context).expect("query builds");
        assert_eq!(hits(&searcher, query.as_ref()), expected, "{field}");
    }
}

#[tokio::test]
async fn sparse_vectors_index_their_indices_and_weights() {
    let schema =
        CollectionSchema::new(vec![], vec![], DynamicMapping::Ignore).with_sparse_vectors(vec![
            SparseVectorSpec {
                name: "s".to_string(),
                modifier: SparseModifier::Idf,
            },
        ]);
    schema.validate().expect("valid schema");
    let layout = tantivy_layout(&schema);
    let vectors = [
        Some(sparse(&[1, 5], &[2.0, 0.0])),
        Some(sparse(&[5], &[0.5])),
        Some(sparse(&[], &[])),
        None,
    ];
    let docs: Vec<_> = vectors
        .iter()
        .enumerate()
        .map(|(i, vector)| {
            let mut d = doc(pk(i as u64), json!({}));
            if let Some(vector) = vector {
                d.sparse_vectors.insert("s".to_string(), vector.clone());
            }
            d
        })
        .collect();
    let searcher = searcher(&layout, tantivy_docs(&schema, &layout, &docs)).await;
    let map = layout.sparse[0];

    let postings = |term: u64| {
        let query = TermQuery::new(
            Term::from_field_u64(map.postings, term),
            IndexRecordOption::Basic,
        );
        hits(&searcher, &query)
    };
    assert_eq!(postings(5), [0, 1], "the zero weight is indexed");
    assert_eq!(postings(1), [0]);
    assert_eq!(postings(SPARSE_PRESENT), [0, 1]);
    assert!(postings(2).is_empty());

    let weights = searcher
        .segment_reader(0)
        .fast_fields()
        .bytes("_sparse_w.s")
        .expect("fast field")
        .expect("bytes column");
    for (doc, vector) in vectors.iter().enumerate() {
        let mut ords = weights.term_ords(doc as u32);
        match vector.as_ref().filter(|v| !v.is_empty()) {
            Some(vector) => {
                let mut bytes = Vec::new();
                weights
                    .ord_to_bytes(ords.next().expect("a value"), &mut bytes)
                    .expect("bytes");
                assert_eq!(decode_sparse_weights(&bytes).unwrap(), *vector, "doc {doc}");
                assert!(ords.next().is_none());
            }
            None => assert!(ords.next().is_none(), "doc {doc} has no weights"),
        }
    }

    // The encoding is exact, and anything else is corrupt.
    let v = sparse(&[1, 5], &[2.0, 0.0]);
    let bytes = encode_sparse_weights(&v);
    assert_eq!(bytes.len(), 4 + 2 * 8);
    assert_eq!(&bytes[..4], &2u32.to_le_bytes());
    assert_eq!(&bytes[4..8], &1u32.to_le_bytes());
    assert_eq!(&bytes[12..16], &2.0f32.to_le_bytes());
    for bad in [
        &bytes[..bytes.len() - 1],
        &bytes[..bytes.len() - 4],
        &bytes[..0],
    ] {
        assert!(
            matches!(decode_sparse_weights(bad), Err(CollectionError::Corrupt(_))),
            "{bad:?}"
        );
    }
    let mut unsorted = bytes.clone();
    unsorted[4..8].copy_from_slice(&7u32.to_le_bytes());
    assert!(matches!(
        decode_sparse_weights(&unsorted),
        Err(CollectionError::Corrupt(_))
    ));
    let mut nan = bytes.clone();
    nan[12..16].copy_from_slice(&f32::NAN.to_le_bytes());
    assert!(matches!(
        decode_sparse_weights(&nan),
        Err(CollectionError::Corrupt(_))
    ));
    assert_eq!(
        decode_sparse_weights(&encode_sparse_weights(&sparse(&[], &[]))).unwrap(),
        sparse(&[], &[])
    );
}

#[test]
fn documents_carry_pk_rowid_presence_and_typed_values() {
    let schema = kinds_schema();
    let layout = tantivy_layout(&schema);
    let source = json!({
        "title": "Hello", "tag": ["a", "b"], "label": "L",
        "id": "67E55044-10B1-426F-9247-BB680E5FE0C8", "n": 7, "x": 1.5, "b": true,
        "d": "2024-01-02", "day": 0, "count": null
    });
    let d = doc(PrimaryKey::Str("k".to_string()), source);
    let extracted = check_document(&schema, &d).expect("valid document");
    let tantivy = to_tantivy_doc(&layout, &schema, &d, &extracted, 42);

    let values = |name: &str| -> Vec<Value> {
        let field = layout.schema.get_field(name).expect("field");
        tantivy
            .get_all(field)
            .map(|value| match value.as_value() {
                tantivy::schema::document::ReferenceValue::Leaf(leaf) => match leaf {
                    tantivy::schema::document::ReferenceValueLeaf::Str(s) => json!(s),
                    tantivy::schema::document::ReferenceValueLeaf::U64(v) => json!(v),
                    tantivy::schema::document::ReferenceValueLeaf::I64(v) => json!(v),
                    tantivy::schema::document::ReferenceValueLeaf::F64(v) => json!(v),
                    tantivy::schema::document::ReferenceValueLeaf::Bool(v) => json!(v),
                    tantivy::schema::document::ReferenceValueLeaf::Date(v) => {
                        json!(v.into_timestamp_millis())
                    }
                    tantivy::schema::document::ReferenceValueLeaf::Bytes(v) => json!(v),
                    other => panic!("unexpected {other:?}"),
                },
                _ => json!("object"),
            })
            .collect()
    };
    assert_eq!(
        values(PK_FIELD),
        [json!(PrimaryKey::Str("k".to_string()).canonical())]
    );
    assert_eq!(values(ROWID_FIELD), [json!(42)]);
    assert_eq!(values("tag"), [json!("a"), json!("b")]);
    assert_eq!(
        values("id"),
        [json!("67e55044-10b1-426f-9247-bb680e5fe0c8")]
    );
    assert_eq!(values("n"), [json!(7)]);
    assert_eq!(values("d"), [json!(1_704_153_600_000_i64)]);
    assert_eq!(values("day"), [json!(0)]);
    assert!(values("count").is_empty());
    assert_eq!(values("payload"), [json!("object")]);
    // One presence term per field with a value: title, tag, label, id, n, x,
    // b, d, day (not `count`, which is null, nor the Json field).
    assert_eq!(values(FIELD_PRESENCE_FIELD).len(), 9);
}
