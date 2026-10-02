//! R1 plan Task 8: values, document ids, limits and the index key layout.
//! None of these tests needs a cluster.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;

use loams_live::docs::{self, index_key_range};
use loams_live::{
    AppKeys, Doc, DocId, IndexDef, IndexId, IndexRange, Limits, LiveError, LiveValue, Order,
    TableDef, TableId, pb,
};
use proptest::prelude::*;

// ---- the reference order of design §20 §4.3 ----

fn rank(v: &LiveValue) -> u8 {
    match v {
        LiveValue::Null => 0,
        LiveValue::I64(_) => 1,
        LiveValue::F64(_) => 2,
        LiveValue::Bool(_) => 3,
        LiveValue::Str(_) => 4,
        LiveValue::Bytes(_) => 5,
        LiveValue::Array(_) => 6,
        LiveValue::Object(_) => 7,
    }
}

/// null < int64 < float64 < bool < string < bytes < array; floats in
/// numeric order with `-0.0 == 0.0` and every NaN equal and last; strings
/// and bytes bytewise; arrays element by element, a prefix first.
fn reference(a: &LiveValue, b: &LiveValue) -> Ordering {
    match (a, b) {
        (LiveValue::I64(x), LiveValue::I64(y)) => x.cmp(y),
        (LiveValue::F64(x), LiveValue::F64(y)) => match (x.is_nan(), y.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        },
        (LiveValue::Bool(x), LiveValue::Bool(y)) => x.cmp(y),
        (LiveValue::Str(x), LiveValue::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (LiveValue::Bytes(x), LiveValue::Bytes(y)) => x.cmp(y),
        (LiveValue::Array(x), LiveValue::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                let o = reference(p, q);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

fn reference_all(a: &[LiveValue], b: &[LiveValue]) -> Ordering {
    for (p, q) in a.iter().zip(b) {
        let o = reference(p, q);
        if o != Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

// ---- strategies ----

fn float() -> impl Strategy<Value = f64> {
    prop_oneof![
        any::<f64>(),
        Just(0.0),
        Just(-0.0),
        Just(f64::NAN),
        Just(-f64::NAN),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(1.0),
        Just(-1.0),
    ]
}

/// Strings and bytes from a small alphabet with NUL and 0xFF, so the
/// escaping and prefixes get exercised.
fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            Just('\0'),
            Just('a'),
            Just('b'),
            Just('\u{ff}'),
            any::<char>()
        ],
        0..4,
    )
    .prop_map(|c| c.into_iter().collect())
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![Just(0u8), Just(1), Just(0xff), any::<u8>()],
        0..4,
    )
}

/// Indexable values (no objects).
fn indexable() -> impl Strategy<Value = LiveValue> {
    let leaf = prop_oneof![
        Just(LiveValue::Null),
        prop_oneof![any::<i64>(), -2i64..3].prop_map(LiveValue::I64),
        float().prop_map(LiveValue::F64),
        any::<bool>().prop_map(LiveValue::Bool),
        text().prop_map(LiveValue::Str),
        bytes().prop_map(LiveValue::Bytes),
    ];
    leaf.prop_recursive(3, 12, 3, |inner| {
        proptest::collection::vec(inner, 0..3).prop_map(LiveValue::Array)
    })
}

/// Any value, objects included.
fn any_value() -> impl Strategy<Value = LiveValue> {
    let leaf = indexable();
    leaf.prop_recursive(3, 16, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..3).prop_map(LiveValue::Array),
            proptest::collection::btree_map("[a-c]{0,2}", inner, 0..3).prop_map(LiveValue::Object),
        ]
    })
}

fn same(a: &LiveValue, b: &LiveValue) -> bool {
    a == b
}

fn table() -> TableDef {
    TableDef {
        id: TableId(7),
        name: "t".to_string(),
        indexes: vec![IndexDef {
            id: IndexId(2),
            name: "by_a_b".to_string(),
            fields: vec!["a".to_string(), "b".to_string()],
        }],
        next_index_id: 3,
    }
}

fn doc(a: LiveValue, b: LiveValue, creation_ms: u64, id: [u8; 16]) -> Doc {
    Doc {
        id: DocId {
            table: TableId(7),
            bytes: id,
        },
        creation_ms,
        fields: BTreeMap::from([("a".to_string(), a), ("b".to_string(), b)]),
    }
}

/// The `by_a_b` entry of a document.
fn entry(d: &Doc) -> Vec<u8> {
    let keys = docs::index_keys(&AppKeys::dedicated(), &table(), d, &Limits::default())
        .expect("indexable");
    assert_eq!(keys.len(), 2, "by_creation_time and by_a_b");
    keys[1].clone()
}

// ---- values ----

#[test]
fn int_and_float_are_distinct() {
    let one = LiveValue::I64(1);
    let one_f = LiveValue::F64(1.0);
    assert_ne!(one, one_f);
    // Distinct on the wire and in storage.
    let back = LiveValue::from_proto(one.to_proto()).expect("valid");
    assert!(matches!(back, LiveValue::I64(1)));
    let back = LiveValue::from_proto(one_f.to_proto()).expect("valid");
    assert!(matches!(back, LiveValue::F64(f) if f == 1.0));
    // Distinct as index keys: every int64 sorts before every float64.
    let i = entry(&doc(LiveValue::I64(i64::MAX), LiveValue::Null, 0, [0; 16]));
    let f = entry(&doc(
        LiveValue::F64(f64::NEG_INFINITY),
        LiveValue::Null,
        0,
        [0; 16],
    ));
    assert!(i < f);
    let i = entry(&doc(one, LiveValue::Null, 0, [0; 16]));
    let f = entry(&doc(one_f, LiveValue::Null, 0, [0; 16]));
    assert_ne!(i, f);
}

#[test]
fn index_elem_rejects_objects() {
    let object = LiveValue::Object(BTreeMap::from([("x".to_string(), LiveValue::I64(1))]));
    assert!(matches!(
        object.index_elem(),
        Err(LiveError::InvalidArgument(_))
    ));
    let nested = LiveValue::Array(vec![LiveValue::Null, object.clone()]);
    assert!(matches!(
        nested.index_elem(),
        Err(LiveError::InvalidArgument(_))
    ));
    // A document whose indexed field holds one is refused, naming the index.
    let err = docs::index_keys(
        &AppKeys::dedicated(),
        &table(),
        &doc(object, LiveValue::Null, 0, [0; 16]),
        &Limits::default(),
    )
    .expect_err("an object is not indexable");
    assert!(err.to_string().contains("by_a_b"), "{err}");
    // Unindexed fields may hold objects.
    let mut d = doc(LiveValue::Null, LiveValue::Null, 0, [0; 16]);
    d.fields
        .insert("c".to_string(), LiveValue::Object(BTreeMap::new()));
    assert!(docs::index_keys(&AppKeys::dedicated(), &table(), &d, &Limits::default()).is_ok());
}

#[test]
fn a_value_without_a_kind_is_invalid() {
    let empty = pb::Value::default();
    assert!(matches!(
        LiveValue::from_proto(empty),
        Err(LiveError::InvalidArgument(_))
    ));
}

proptest! {
    #[test]
    fn value_proto_roundtrip(v in any_value()) {
        let back = LiveValue::from_proto(v.to_proto()).expect("valid");
        prop_assert!(same(&v, &back), "{v:?} came back as {back:?}");
    }

    #[test]
    fn index_keys_sort_in_value_order(
        items in proptest::collection::vec(
            (indexable(), indexable(), 0u64..4, any::<[u8; 16]>()),
            1..24,
        )
    ) {
        let docs: Vec<Doc> = items
            .into_iter()
            .map(|(a, b, ms, id)| doc(a, b, ms, id))
            .collect();
        let mut by_key: Vec<(Vec<u8>, &Doc)> = docs.iter().map(|d| (entry(d), d)).collect();
        by_key.sort_by(|x, y| x.0.cmp(&y.0));
        for pair in by_key.windows(2) {
            let (x, y) = (pair[0].1, pair[1].1);
            let vx = [x.fields["a"].clone(), x.fields["b"].clone()];
            let vy = [y.fields["a"].clone(), y.fields["b"].clone()];
            let o = reference_all(&vx, &vy)
                .then(x.creation_ms.cmp(&y.creation_ms))
                .then(x.id.bytes.cmp(&y.id.bytes));
            prop_assert_ne!(o, Ordering::Greater, "{:?} sorts before {:?}", vx, vy);
            // Keys are equal exactly when the reference says equal.
            prop_assert_eq!(pair[0].0 == pair[1].0, o == Ordering::Equal);
        }
    }

    #[test]
    fn index_range_contains_matches_bounds(
        values in proptest::collection::vec(indexable(), 1..16),
        eq in proptest::option::of(indexable()),
        lower in bound(),
        upper in bound(),
    ) {
        // Equality on `a` (when given), bounds on `b`; or bounds on `a`.
        let t = table();
        let app = AppKeys::dedicated();
        let range = IndexRange {
            table: t.id,
            index: IndexId(2),
            eq: eq.iter().cloned().collect(),
            lower: lower.clone(),
            upper: upper.clone(),
            order: Order::Asc,
            limit: None,
        };
        let keys = index_key_range(&app, &t, &range).expect("a valid range");
        for (n, v) in values.iter().enumerate() {
            for (a, b) in [
                (v.clone(), v.clone()),
                (eq.clone().unwrap_or(LiveValue::Null), v.clone()),
            ] {
                let d = doc(a.clone(), b.clone(), n as u64, [n as u8; 16]);
                let (first, bounded) = match &eq {
                    Some(e) => (reference(&a, e) == Ordering::Equal, b.clone()),
                    None => (true, a.clone()),
                };
                let inside = first
                    && match &lower {
                        Bound::Unbounded => true,
                        Bound::Included(l) => reference(&bounded, l) != Ordering::Less,
                        Bound::Excluded(l) => reference(&bounded, l) == Ordering::Greater,
                    }
                    && match &upper {
                        Bound::Unbounded => true,
                        Bound::Included(u) => reference(&bounded, u) != Ordering::Greater,
                        Bound::Excluded(u) => reference(&bounded, u) == Ordering::Less,
                    };
                prop_assert_eq!(keys.contains(&entry(&d)), inside, "{:?} {:?}", a, b);
            }
        }
    }

    #[test]
    fn doc_id_text_roundtrip(table in any::<u32>(), bytes in any::<[u8; 16]>()) {
        let id = DocId { table: TableId(table), bytes };
        let text = id.to_string();
        prop_assert_eq!(text.parse::<DocId>().expect("parses"), id);
        prop_assert_eq!(text.to_uppercase().parse::<DocId>().expect("any case"), id);
    }
}

fn bound() -> impl Strategy<Value = Bound<LiveValue>> {
    prop_oneof![
        Just(Bound::Unbounded),
        indexable().prop_map(Bound::Included),
        indexable().prop_map(Bound::Excluded),
    ]
}

// ---- document ids ----

#[test]
fn doc_id_text_roundtrip_and_checksum() {
    for table in [0, 1, 127, 128, 16_383, 16_384, u32::MAX] {
        let id = DocId::random(TableId(table)).expect("the OS random source");
        let text = id.to_string();
        assert_eq!(text.parse::<DocId>().expect("parses"), id, "{text}");
        assert!(
            text.chars()
                .all(|c| "0123456789abcdefghjkmnpqrstvwxyz".contains(c)),
            "{text}"
        );
        // Crockford's aliases: upper case, and o/i/l for 0/1.
        let aliased: String = text
            .chars()
            .map(|c| match c {
                '0' => 'O',
                '1' => 'l',
                c => c.to_ascii_uppercase(),
            })
            .collect();
        assert_eq!(aliased.parse::<DocId>().expect("aliases parse"), id);
        // Every single-symbol change is caught (the checksum, or the
        // encoding's trailing bits for the last symbol).
        for at in 0..text.len() {
            for sub in "0123456789abcdefghjkmnpqrstvwxyz".chars() {
                if text.as_bytes()[at] as char == sub {
                    continue;
                }
                let mut bad = text.clone().into_bytes();
                bad[at] = sub as u8;
                let bad = String::from_utf8(bad).expect("ascii");
                assert!(bad.parse::<DocId>().is_err(), "{bad} parsed");
            }
        }
    }
    // Two ids of one table differ, and name their table.
    let a = DocId::random(TableId(3)).expect("rng");
    let b = DocId::random(TableId(3)).expect("rng");
    assert_ne!(a, b);
    assert_eq!(
        a.to_string().parse::<DocId>().expect("parses").table,
        TableId(3)
    );
    for junk in ["", "u", "0000", "!!!!", &a.to_string()[1..]] {
        assert!(
            matches!(junk.parse::<DocId>(), Err(LiveError::InvalidArgument(_))),
            "'{junk}'"
        );
    }
}

// ---- limits ----

#[test]
fn limits_name_the_limit() {
    let limits = Limits::default();
    let limit_of = |fields: BTreeMap<String, LiveValue>| match limits.check_fields(&fields) {
        Err(LiveError::LimitExceeded { limit, .. }) => Some(limit),
        Err(e) => panic!("not a limit error: {e}"),
        Ok(()) => None,
    };
    let one = |v: LiveValue| BTreeMap::from([("f".to_string(), v)]);
    let mut deep = LiveValue::Null;
    for _ in 0..15 {
        deep = LiveValue::Array(vec![deep]);
    }
    assert_eq!(limit_of(one(deep.clone())), None, "depth 16 is allowed");
    assert_eq!(
        limit_of(one(LiveValue::Array(vec![deep]))),
        Some("max_depth")
    );
    assert_eq!(
        limit_of(one(LiveValue::Array(vec![LiveValue::Null; 8193]))),
        Some("max_array_len")
    );
    let wide: BTreeMap<String, LiveValue> = (0..1025)
        .map(|n| (format!("f{n}"), LiveValue::Null))
        .collect();
    assert_eq!(limit_of(wide.clone()), Some("max_fields"));
    assert_eq!(limit_of(one(LiveValue::Object(wide))), Some("max_fields"));
    assert_eq!(
        limit_of(BTreeMap::from([("x".repeat(1025), LiveValue::Null)])),
        Some("max_field_name_bytes")
    );
    for reserved in ["_id", "_creationTime", "_x", ""] {
        assert!(matches!(
            limits.check_fields(&BTreeMap::from([(reserved.to_string(), LiveValue::Null)])),
            Err(LiveError::InvalidArgument(_))
        ));
    }
    assert!(matches!(
        limits.check_document_bytes(1024 * 1024 + 1),
        Err(LiveError::LimitExceeded {
            limit: "max_document_bytes",
            ..
        })
    ));
    assert_eq!(
        LiveError::LimitExceeded {
            limit: "max_depth",
            message: String::new()
        }
        .code(),
        pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED
    );
}

#[test]
fn index_values_over_the_key_limit_are_refused() {
    let big = LiveValue::Str("x".repeat(4096));
    let err = docs::index_keys(
        &AppKeys::dedicated(),
        &table(),
        &doc(big, LiveValue::Null, 0, [0; 16]),
        &Limits::default(),
    )
    .expect_err("over 4 KiB");
    assert!(
        matches!(
            err,
            LiveError::LimitExceeded {
                limit: "max_index_key_bytes",
                ..
            }
        ),
        "{err}"
    );
}

// ---- the key layout ----

#[test]
fn keys_follow_the_design_layout() {
    let app = AppKeys::dedicated();
    let id = DocId {
        table: TableId(0x0102_0304),
        bytes: [0xAB; 16],
    };
    let mut want = vec![0x02, 1, 2, 3, 4];
    want.extend_from_slice(&[0xAB; 16]);
    assert_eq!(app.document(&id), want);
    let key = app.index_entry(IndexId(9), &[loams_tikv::tuple::Elem::Null], 0x0A0B, &id);
    let mut want = vec![0x03, 1, 2, 3, 4, 0, 0, 0, 9, 0x05];
    want.extend_from_slice(&0x0A0Bu64.to_be_bytes());
    want.extend_from_slice(&[0xAB; 16]);
    assert_eq!(key, want);
    assert_eq!(app.doc_of_index_entry(id.table, &key), Some((id, 0x0A0B)));
    assert_eq!(app.table(TableId(5)), vec![0x01, 0x02, 0, 0, 0, 5]);
    assert_eq!(app.table_name("msgs"), b"\x01\x01msgs".to_vec());
    assert_eq!(app.table_counter(), b"\x01\x00table".to_vec());
    // The by_creation_time entry has no tuple part.
    let d = doc(LiveValue::Null, LiveValue::Null, 0x0A0B, [0xAB; 16]);
    let d = Doc { id, ..d };
    let t = TableDef {
        id: id.table,
        ..table()
    };
    let keys = docs::index_keys(&app, &t, &d, &Limits::default()).expect("keys");
    let mut want = vec![0x03, 1, 2, 3, 4, 0, 0, 0, 1];
    want.extend_from_slice(&0x0A0Bu64.to_be_bytes());
    want.extend_from_slice(&[0xAB; 16]);
    assert_eq!(keys[0], want);
}

#[test]
fn creation_time_ranges_and_invalid_ranges() {
    let t = table();
    let app = AppKeys::dedicated();
    // Equality on both fields, then a bound on _creationTime.
    let range = IndexRange {
        eq: vec![LiveValue::I64(1), LiveValue::Null],
        lower: Bound::Included(LiveValue::I64(10)),
        upper: Bound::Excluded(LiveValue::I64(20)),
        ..IndexRange::all(t.id, IndexId(2))
    };
    let keys = index_key_range(&app, &t, &range).expect("valid");
    for (ms, inside) in [(9, false), (10, true), (19, true), (20, false)] {
        let d = doc(LiveValue::I64(1), LiveValue::Null, ms, [0; 16]);
        assert_eq!(keys.contains(&entry(&d)), inside, "{ms}");
    }
    let bad = [
        // _creationTime is an int64.
        IndexRange {
            eq: vec![LiveValue::I64(1), LiveValue::Null],
            lower: Bound::Included(LiveValue::F64(1.0)),
            ..IndexRange::all(t.id, IndexId(2))
        },
        // No bounds on _id.
        IndexRange {
            eq: vec![LiveValue::I64(1), LiveValue::Null, LiveValue::I64(0)],
            lower: Bound::Included(LiveValue::I64(1)),
            ..IndexRange::all(t.id, IndexId(2))
        },
        // Too many equality values.
        IndexRange {
            eq: vec![LiveValue::Null; 4],
            ..IndexRange::all(t.id, IndexId(2))
        },
        // Another table.
        IndexRange::all(TableId(8), IndexId(2)),
        // by_id takes no equality.
        IndexRange {
            eq: vec![LiveValue::Null],
            ..IndexRange::all(t.id, IndexId::BY_ID)
        },
    ];
    for range in bad {
        assert!(
            matches!(
                index_key_range(&app, &t, &range),
                Err(LiveError::InvalidArgument(_))
            ),
            "{range:?}"
        );
    }
    assert!(matches!(
        index_key_range(&app, &t, &IndexRange::all(t.id, IndexId(99))),
        Err(LiveError::NotFound(_))
    ));
}

#[test]
fn table_def_record_roundtrip() {
    let t = table();
    assert_eq!(TableDef::from_proto(t.to_proto()).expect("valid"), t);
    let mut wrong = t.to_proto();
    wrong.format = 2;
    assert!(matches!(
        TableDef::from_proto(wrong),
        Err(LiveError::Corrupt(_))
    ));
    assert_eq!(t.index_id("by_id"), Some(IndexId::BY_ID));
    assert_eq!(
        t.index_id("by_creation_time"),
        Some(IndexId::BY_CREATION_TIME)
    );
    assert_eq!(t.index_id("by_a_b"), Some(IndexId(2)));
    assert_eq!(t.index_fields(IndexId::BY_CREATION_TIME), Some(&[][..]));
    assert_eq!(t.index_fields(IndexId::BY_ID), None);
}
