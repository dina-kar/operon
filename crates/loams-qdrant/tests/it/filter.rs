//! Qdrant filters compiled to the search IR (plan M1.4 Task 4), and the
//! payload-index field mapping.

use loams_collection::{CollectionSchema, DynamicMapping, FieldKind, FieldSpec, PrimaryKey};
use loams_qdrant::GatewayError;
use loams_qdrant::filter::{and, compile_filter, exclude_ids, parse_datetime};
use loams_qdrant::model::collections::PayloadFieldSchema;
use loams_qdrant::model::filter::Filter;
use loams_qdrant::schema::payload_index_field;
use loams_query::{BoolOperator, FieldValue, Query};
use serde_json::{Value, json};

fn field(name: &str, source: &str, kind: FieldKind) -> FieldSpec {
    FieldSpec {
        name: name.into(),
        source_path: source.into(),
        kind,
        indexed: true,
        fast: true,
        ignore_malformed: name != "payload",
    }
}

/// The `payload` field, plus a `text` and a `datetime` payload index when
/// `indexes`.
fn schema(indexes: bool) -> CollectionSchema {
    let mut fields = vec![field("payload", "", FieldKind::Json)];
    if indexes {
        fields.push(field(
            "payload_index.metadata.body",
            "metadata.body",
            FieldKind::Text {
                analyzer: "standard".into(),
                positions: true,
            },
        ));
        fields.push(field("payload_index.ts", "ts", FieldKind::Date));
    }
    CollectionSchema::new(fields, Vec::new(), DynamicMapping::Ignore)
}

fn parse(v: Value) -> Filter {
    serde_json::from_value(v.clone()).unwrap_or_else(|e| panic!("{v}: {e}"))
}

fn compile(v: Value) -> Result<Query, GatewayError> {
    compile_filter(&parse(v), &schema(true))
}

fn ok(v: Value) -> Query {
    compile(v.clone()).unwrap_or_else(|e| panic!("{v}: {e}"))
}

/// The query of a filter with one `must` condition.
fn must1(condition: Value) -> Query {
    match ok(json!({"must": [condition]})) {
        Query::Bool {
            mut filter,
            must,
            should,
            must_not,
            minimum_should_match: None,
        } if must.is_empty() && should.is_empty() && must_not.is_empty() && filter.len() == 1 => {
            filter.remove(0)
        }
        other => panic!("{other:?}"),
    }
}

fn bool_q(filter: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
    Query::Bool {
        must: Vec::new(),
        should,
        must_not,
        filter,
        minimum_should_match: None,
    }
}

fn term(f: &str, value: FieldValue) -> Query {
    Query::Term {
        field: f.into(),
        value,
    }
}

fn terms(f: &str, values: Vec<FieldValue>) -> Query {
    Query::Terms {
        field: f.into(),
        values,
    }
}

fn exists(f: &str) -> Query {
    Query::Exists { field: f.into() }
}

#[test]
fn compile_match_value_is_type_strict() {
    assert_eq!(
        must1(json!({"key": "a", "match": {"value": 1}})),
        term("payload.a", FieldValue::I64(1))
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"value": "1"}})),
        term("payload.a", FieldValue::Str("1".into()))
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"value": true}})),
        term("payload.a", FieldValue::Bool(true))
    );
    // A float is not a match value (Qdrant's serde refuses it too).
    assert!(
        serde_json::from_value::<Filter>(json!({"must": [{"key": "a", "match": {"value": 1.5}}]}))
            .is_err()
    );
}

#[test]
fn compile_any_and_except() {
    assert_eq!(
        must1(json!({"key": "a", "match": {"any": [1, 2]}})),
        terms("payload.a", vec![FieldValue::I64(1), FieldValue::I64(2)])
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"any": ["x"]}})),
        terms("payload.a", vec![FieldValue::Str("x".into())])
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"any": []}})),
        Query::MatchNone
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"except": [1, 2]}})),
        bool_q(
            vec![exists("payload.a")],
            Vec::new(),
            vec![terms(
                "payload.a",
                vec![FieldValue::I64(1), FieldValue::I64(2)]
            )]
        )
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"except": []}})),
        exists("payload.a")
    );
    assert_eq!(
        must1(json!({"key": "a", "match": {"prefix": "ab"}})),
        Query::Prefix {
            field: "payload.a".into(),
            value: "ab".into()
        }
    );
    // A list mixing types is a format error.
    assert!(
        serde_json::from_value::<Filter>(
            json!({"must": [{"key": "a", "match": {"any": [1, "x"]}}]})
        )
        .is_err()
    );
}

#[test]
fn compile_empty_lists_match_all() {
    assert_eq!(ok(json!({"should": []})), Query::MatchAll);
    assert_eq!(ok(json!({"must": [], "must_not": []})), Query::MatchAll);
    assert_eq!(ok(json!({})), Query::MatchAll);
    // A pure must_not keeps the IR's "everything else" reading.
    assert_eq!(
        ok(json!({"must_not": [{"key": "a", "match": {"value": 1}}]})),
        bool_q(
            Vec::new(),
            Vec::new(),
            vec![term("payload.a", FieldValue::I64(1))]
        )
    );
}

#[test]
fn compile_should_and_single_conditions() {
    let c1 = json!({"key": "a", "match": {"value": 1}});
    let c2 = json!({"key": "b", "match": {"value": "x"}});
    let (q1, q2) = (
        term("payload.a", FieldValue::I64(1)),
        term("payload.b", FieldValue::Str("x".into())),
    );
    assert_eq!(
        ok(json!({"must": [c1], "should": [c1, c2], "must_not": [c2]})),
        bool_q(
            vec![
                q1.clone(),
                bool_q(Vec::new(), vec![q1.clone(), q2.clone()], Vec::new())
            ],
            Vec::new(),
            vec![q2.clone()]
        )
    );
    // A single condition stands for a list of one.
    assert_eq!(ok(json!({"must": c1})), ok(json!({"must": [c1]})));
    assert_eq!(ok(json!({"should": c2})), ok(json!({"should": [c2]})));
}

#[test]
fn compile_min_should() {
    let c1 = json!({"key": "a", "match": {"value": 1}});
    let c2 = json!({"key": "b", "match": {"value": 2}});
    let q = ok(json!({"min_should": {"conditions": [c1, c2], "min_count": 2}}));
    assert_eq!(
        q,
        bool_q(
            vec![Query::Bool {
                must: Vec::new(),
                should: vec![
                    term("payload.a", FieldValue::I64(1)),
                    term("payload.b", FieldValue::I64(2))
                ],
                must_not: Vec::new(),
                filter: Vec::new(),
                minimum_should_match: Some("2".into()),
            }],
            Vec::new(),
            Vec::new()
        )
    );
    // More than there are conditions never matches (the IR would clamp).
    assert_eq!(
        ok(json!({"min_should": {"conditions": [c1], "min_count": 2}})),
        bool_q(vec![Query::MatchNone], Vec::new(), Vec::new())
    );
    assert!(matches!(
        compile(json!({"min_should": {"conditions": [c1], "min_count": 0}})),
        Err(GatewayError::BadRequest(m)) if m.contains("min_count")
    ));
}

#[test]
fn compile_multi_subcondition_is_or() {
    assert_eq!(
        must1(json!({"key": "a", "match": {"value": 1}, "range": {"gt": 5.0}, "is_null": true})),
        bool_q(
            Vec::new(),
            vec![
                term("payload.a", FieldValue::I64(1)),
                Query::Range {
                    field: "payload.a".into(),
                    gt: Some(FieldValue::F64(5.0)),
                    gte: None,
                    lt: None,
                    lte: None
                },
                Query::IsNull {
                    field: "payload.a".into()
                },
            ],
            Vec::new()
        )
    );
    assert!(matches!(
        compile(json!({"must": [{"key": "a"}]})),
        Err(GatewayError::BadRequest(m)) if m == "At least one field condition must be specified"
    ));
}

#[test]
fn compile_empty_null_count_and_ids() {
    let empty = Query::IsEmpty {
        field: "payload.a".into(),
    };
    let null = Query::IsNull {
        field: "payload.a".into(),
    };
    assert_eq!(must1(json!({"is_empty": {"key": "a"}})), empty);
    assert_eq!(must1(json!({"key": "a", "is_empty": true})), empty);
    assert_eq!(
        must1(json!({"key": "a", "is_empty": false})),
        bool_q(vec![Query::MatchAll], Vec::new(), vec![empty])
    );
    assert_eq!(must1(json!({"is_null": {"key": "a"}})), null);
    assert_eq!(
        must1(json!({"key": "a", "is_null": false})),
        bool_q(vec![Query::MatchAll], Vec::new(), vec![null])
    );
    assert_eq!(
        must1(json!({"key": "a", "values_count": {"gte": 2, "lt": 5}})),
        Query::ValuesCount {
            field: "payload.a".into(),
            gt: None,
            gte: Some(2),
            lt: Some(5),
            lte: None
        }
    );
    assert_eq!(
        must1(json!({"has_id": [7, "550E8400-E29B-41D4-A716-446655440000"]})),
        Query::Ids(vec![
            PrimaryKey::U64(7),
            PrimaryKey::Uuid(
                uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")
                    .unwrap()
                    .into_bytes()
            ),
        ])
    );
    assert!(matches!(
        compile(json!({"must": [{"has_id": ["123"]}]})),
        Err(GatewayError::Format { .. })
    ));
}

#[test]
fn compile_nested_filters() {
    let inner = json!({"should": [{"key": "a", "match": {"value": 1}}, {"key": "a", "match": {"value": 2}}]});
    let q = ok(
        json!({"must": [inner], "must_not": [{"must": [{"key": "b", "match": {"value": "x"}}]}]}),
    );
    let inner_q = bool_q(
        vec![bool_q(
            Vec::new(),
            vec![
                term("payload.a", FieldValue::I64(1)),
                term("payload.a", FieldValue::I64(2)),
            ],
            Vec::new(),
        )],
        Vec::new(),
        Vec::new(),
    );
    let not_q = bool_q(
        vec![term("payload.b", FieldValue::Str("x".into()))],
        Vec::new(),
        Vec::new(),
    );
    assert_eq!(q, bool_q(vec![inner_q], Vec::new(), vec![not_q]));
    // An empty nested filter matches everything.
    assert_eq!(must1(json!({})), Query::MatchAll);
}

#[test]
fn compile_nested_key_paths() {
    assert_eq!(
        must1(json!({"key": "metadata.details.page", "match": {"value": 3}})),
        term("payload.metadata.details.page", FieldValue::I64(3))
    );
    assert_eq!(
        must1(json!({"key": "a[].b", "match": {"value": 3}})),
        term("payload.a.b", FieldValue::I64(3))
    );
    assert_eq!(
        must1(json!({"key": "\"quoted\".b", "match": {"value": 3}})),
        term("payload.quoted.b", FieldValue::I64(3))
    );
    assert!(matches!(
        compile(json!({"must": [{"key": "a b", "match": {"value": 3}}]})),
        Err(GatewayError::Format { .. })
    ));
}

#[test]
fn compile_text_conditions_use_the_payload_field() {
    for indexes in [true, false] {
        let s = schema(indexes);
        let one = |c: Value| compile_filter(&parse(json!({"must": [c]})), &s).expect("compiles");
        let text = |text: &str, operator| {
            bool_q(
                vec![Query::Match {
                    field: "payload.metadata.body".into(),
                    text: text.into(),
                    operator,
                    minimum_should_match: None,
                    fuzziness: None,
                    analyzer: None,
                }],
                Vec::new(),
                Vec::new(),
            )
        };
        assert_eq!(
            one(json!({"key": "metadata.body", "match": {"text": "quick fox"}})),
            text("quick fox", BoolOperator::And)
        );
        assert_eq!(
            one(json!({"key": "metadata.body", "match": {"text_any": "quick fox"}})),
            text("quick fox", BoolOperator::Or)
        );
        assert_eq!(
            one(json!({"key": "metadata.body", "match": {"phrase": "quick fox"}})),
            bool_q(
                vec![Query::MatchPhrase {
                    field: "payload.metadata.body".into(),
                    text: "quick fox".into(),
                    slop: 0
                }],
                Vec::new(),
                Vec::new()
            )
        );
    }
}

#[test]
fn compile_datetime_ranges_use_the_payload_field() {
    for indexes in [true, false] {
        let q = compile_filter(
            &parse(json!({"must": [{"key": "ts", "range": {"gte": "2023-02-08T10:49:00Z", "lt": "2023-02-09"}}]})),
            &schema(indexes),
        )
        .expect("compiles");
        assert_eq!(
            q,
            bool_q(
                vec![Query::Range {
                    field: "payload.ts".into(),
                    gt: None,
                    gte: Some(FieldValue::Date(1_675_853_340_000_000)),
                    lt: Some(FieldValue::Date(1_675_900_800_000_000)),
                    lte: None,
                }],
                Vec::new(),
                Vec::new()
            )
        );
    }
    assert!(matches!(
        compile(json!({"must": [{"key": "ts", "range": {"gt": "yesterday"}}]})),
        Err(GatewayError::BadRequest(m)) if m == "Unable to parse datetime yesterday"
    ));
}

#[test]
fn compile_unsupported_conditions_are_501() {
    let cases = [
        (
            json!({"key": "g", "geo_radius": {"center": {"lon": 0.0, "lat": 0.0}, "radius": 1.0}}),
            "geo_radius condition",
        ),
        (
            json!({"key": "g", "geo_bounding_box": {}}),
            "geo_bounding_box condition",
        ),
        (
            json!({"key": "g", "geo_polygon": {}}),
            "geo_polygon condition",
        ),
        (
            json!({"nested": {"key": "n", "filter": {}}}),
            "nested condition",
        ),
        (json!({"has_vector": "v"}), "has_vector condition"),
        (
            json!({"slice": {"total": 2, "index": 0}}),
            "slice condition",
        ),
        (json!({"key": "a[0]", "match": {"value": 1}}), "array index"),
    ];
    for (condition, what) in cases {
        match compile(json!({"must": [condition]})) {
            Err(GatewayError::Unsupported(f)) => assert!(f.contains(what), "{f}"),
            other => panic!("{condition}: {other:?}"),
        }
    }
    // A collection without the `payload` field (made natively).
    let native = CollectionSchema::new(Vec::new(), Vec::new(), DynamicMapping::Ignore);
    assert!(matches!(
        compile_filter(&parse(json!({})), &native),
        Err(GatewayError::Unsupported(_))
    ));
}

#[test]
fn unknown_filter_fields_are_errors() {
    for bad in [
        json!({"must": [{"key": "a", "mtch": {"value": 1}}]}),
        json!({"mst": []}),
        json!({"must": [{"key": "a", "match": {"value": 1, "x": 2}}]}),
        json!({"must": [{"key": "a", "range": {"gt": 1, "gtt": 2}}]}),
        json!({"must": [{"is_empty": {"key": "a", "x": 1}}]}),
        json!({"must": [{"has_id": [1], "x": 1}]}),
        json!({"min_should": {"conditions": [], "min_count": 1, "x": 0}}),
    ] {
        assert!(
            serde_json::from_value::<Filter>(bad.clone()).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn datetime_formats_parse_like_qdrant() {
    let base = 1_675_853_340_000_000; // 2023-02-08T10:49:00Z
    for (s, us) in [
        ("2023-02-08T10:49:00Z", base),
        ("2023-02-08T10:49:00+01:00", base - 3_600_000_000),
        ("2023-02-08 10:49:00+0100", base - 3_600_000_000),
        ("2023-02-08T10:49:00-01", base + 3_600_000_000),
        ("2023-02-08T10:49:00", base),
        ("2023-02-08 10:49:00.5", base + 500_000),
        ("2023-02-08T10:49:00.123456789Z", base + 123_456),
        ("2023-02-08T10:49", base),
        ("2023-02-08 10:49", base),
        ("2023-02-08", base - (10 * 3600 + 49 * 60) * 1_000_000),
    ] {
        assert_eq!(
            parse_datetime(s).unwrap_or_else(|e| panic!("{s}: {e}")),
            us,
            "{s}"
        );
    }
    for bad in [
        "2023-02-30",
        "2023-2-08",
        "2023-02-08T10",
        "2023-02-08T10:49Z",
        "08/02/2023",
        "2023-02-08T25:00:00",
        "",
    ] {
        assert!(parse_datetime(bad).is_err(), "{bad}");
    }
}

#[test]
fn and_and_exclude_ids_compose() {
    let q = term("payload.a", FieldValue::I64(1));
    assert_eq!(and(None, None), None);
    assert_eq!(and(Some(q.clone()), None), Some(q.clone()));
    assert_eq!(and(None, Some(q.clone())), Some(q.clone()));
    assert_eq!(
        and(Some(q.clone()), Some(Query::MatchNone)),
        Some(bool_q(
            vec![q.clone(), Query::MatchNone],
            Vec::new(),
            Vec::new()
        ))
    );
    let ids = [PrimaryKey::U64(1)];
    assert_eq!(exclude_ids(Some(q.clone()), &[]), Some(q.clone()));
    assert_eq!(
        exclude_ids(Some(q.clone()), &ids),
        Some(bool_q(vec![q], Vec::new(), vec![Query::Ids(ids.to_vec())]))
    );
    assert_eq!(
        exclude_ids(None, &ids),
        Some(bool_q(
            vec![Query::MatchAll],
            Vec::new(),
            vec![Query::Ids(ids.to_vec())]
        ))
    );
}

fn index(key: &str, schema: Value) -> Result<FieldSpec, GatewayError> {
    let schema: PayloadFieldSchema = serde_json::from_value(schema).expect("parses");
    payload_index_field(key, &schema)
}

#[test]
fn payload_index_types_map_to_lenient_fields() {
    for (name, kind) in [
        ("keyword", FieldKind::Keyword),
        ("integer", FieldKind::I64),
        ("float", FieldKind::F64),
        ("bool", FieldKind::Bool),
        ("datetime", FieldKind::Date),
        ("uuid", FieldKind::Uuid),
    ] {
        let f = index("meta.tenant", json!(name)).expect(name);
        assert_eq!(f.name, "payload_index.meta.tenant");
        assert_eq!(f.source_path, "meta.tenant");
        assert_eq!(f.kind, kind);
        assert!(f.indexed && f.fast && f.ignore_malformed, "{name}");
        // The params form gives the same field.
        assert_eq!(
            index("meta.tenant", json!({"type": name, "on_disk": true})).expect(name),
            f
        );
    }
    let text = index("body", json!("text")).expect("text");
    assert_eq!(
        text.kind,
        FieldKind::Text {
            analyzer: "standard".into(),
            positions: false
        }
    );
    assert!(!text.fast && text.ignore_malformed);
    let phrase = index(
        "body",
        json!({"type": "text", "tokenizer": "word", "lowercase": true, "phrase_matching": true}),
    )
    .expect("text");
    assert_eq!(
        phrase.kind,
        FieldKind::Text {
            analyzer: "standard".into(),
            positions: true
        }
    );
    assert_eq!(index("a[].b", json!("keyword")).unwrap().source_path, "a.b");
}

#[test]
fn non_default_payload_indexes_are_501() {
    for (schema, what) in [
        (json!("geo"), "geo index"),
        (
            json!({"type": "text", "tokenizer": "whitespace"}),
            "tokenizer whitespace",
        ),
        (
            json!({"type": "text", "tokenizer": "prefix"}),
            "tokenizer prefix",
        ),
        (json!({"type": "text", "lowercase": false}), "lowercase"),
        (json!({"type": "text", "min_token_len": 2}), "min_token_len"),
        (
            json!({"type": "text", "max_token_len": 20}),
            "max_token_len",
        ),
        (json!({"type": "text", "stopwords": "english"}), "stopwords"),
        (
            json!({"type": "text", "stemmer": {"type": "snowball", "language": "english"}}),
            "stemmer",
        ),
        (
            json!({"type": "text", "ascii_folding": true}),
            "ascii_folding",
        ),
    ] {
        match index("body", schema.clone()) {
            Err(GatewayError::Unsupported(f)) => assert!(f.contains(what), "{f}"),
            other => panic!("{schema}: {other:?}"),
        }
    }
    assert!(matches!(
        index("a[0]", json!("keyword")),
        Err(GatewayError::Unsupported(_))
    ));
    assert!(matches!(
        index("a", json!("nope")),
        Err(GatewayError::Format { .. })
    ));
    assert!(matches!(
        index("a", json!({"on_disk": true})),
        Err(GatewayError::Format { .. })
    ));
}

#[test]
fn compile_double_bounds_keep_the_stricter() {
    let range = |gt, gte, lt, lte| Query::Range {
        field: "payload.a".into(),
        gt,
        gte,
        lt,
        lte,
    };
    let f = |x: f64| Some(FieldValue::F64(x));
    assert_eq!(
        must1(json!({"key": "a", "range": {"gt": 1.0, "gte": 1.0, "lt": 5.0, "lte": 4.0}})),
        range(f(1.0), None, None, f(4.0))
    );
    assert_eq!(
        must1(json!({"key": "a", "range": {"gt": 0.5, "gte": 1.0, "lt": 3.0, "lte": 3.0}})),
        range(None, f(1.0), f(3.0), None)
    );
    assert_eq!(
        must1(json!({"key": "a", "values_count": {"gt": 1, "gte": 3, "lt": 2, "lte": 5}})),
        Query::ValuesCount {
            field: "payload.a".into(),
            gt: None,
            gte: Some(3),
            lt: Some(2),
            lte: None
        }
    );
}
