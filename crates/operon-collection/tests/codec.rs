//! The `DocOp` record codec, canonical sparse vectors, patches and the
//! per-key fold.

use std::collections::BTreeMap;

use bytes::Bytes;
use operon_collection::{
    CODEC_VERSION, CodecError, DocOp, Document, MAX_RECORD_VALUE_BYTES, PatchMode, PrimaryKey,
    SparseVector, SparseVectorError, apply_patch, decode, encode, fold, needs_current,
};
use operon_log::Record;
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use serde::Serialize;
use serde_json::{Map, Value, json};

// ---------------------------------------------------------------------------
// Helpers

fn obj(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("not an object: {other}"),
    }
}

fn sparse(indices: &[u32], values: &[f32]) -> SparseVector {
    SparseVector::new(indices.to_vec(), values.to_vec()).expect("canonical sparse vector")
}

fn doc(pk: PrimaryKey, source: Value) -> Document {
    Document {
        pk,
        source: obj(source),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
    }
}

fn patch(pk: PrimaryKey, mode: PatchMode, source: Value) -> DocOp {
    patch_with(pk, mode, source, &[], None)
}

fn patch_with(
    pk: PrimaryKey,
    mode: PatchMode,
    source: Value,
    delete_keys: &[&str],
    upsert: Option<Document>,
) -> DocOp {
    DocOp::Patch {
        pk,
        mode,
        source: obj(source),
        delete_keys: delete_keys.iter().map(|key| key.to_string()).collect(),
        vectors: BTreeMap::new(),
        sparse_vectors: BTreeMap::new(),
        upsert,
    }
}

fn pk1() -> PrimaryKey {
    PrimaryKey::Str("doc-1".to_string())
}

/// The value bytes of an encoded record.
fn value_of(record: &Record) -> Vec<u8> {
    record.value.as_ref().expect("record value").to_vec()
}

fn with_value(mut record: Record, value: Vec<u8>) -> Record {
    record.value = Some(Bytes::from(value));
    record
}

// A copy of the private wire types (overview §6.2): variant order and field
// order are the format, so these must encode exactly like the codec's own.
// Sparse vectors are raw here, so tests can build non-canonical ones.

#[derive(Serialize)]
struct RawSparse {
    indices: Vec<u32>,
    values: Vec<f32>,
}

#[derive(Serialize)]
struct WireDoc {
    pk: PrimaryKey,
    source: Vec<u8>,
    vectors: BTreeMap<String, Vec<f32>>,
    sparse_vectors: BTreeMap<String, RawSparse>,
}

#[derive(Serialize)]
enum WireOp {
    Upsert(WireDoc),
    #[allow(dead_code)]
    Delete(PrimaryKey),
    #[allow(dead_code)]
    Patch {
        pk: PrimaryKey,
        mode: PatchMode,
        source: Vec<u8>,
        delete_keys: Vec<String>,
        vectors: BTreeMap<String, Option<Vec<f32>>>,
        sparse_vectors: BTreeMap<String, Option<RawSparse>>,
        upsert: Option<WireDoc>,
    },
}

/// A record carrying `op` exactly as the codec lays it out, bypassing
/// `encode`'s checks.
fn hand_built(pk: &PrimaryKey, op: &WireOp) -> Record {
    let mut value = vec![CODEC_VERSION];
    value.extend(postcard::to_stdvec(op).expect("postcard"));
    Record {
        key: Some(Bytes::from(pk.canonical())),
        value: Some(Bytes::from(value)),
        headers: Vec::new(),
        timestamp_ms: -1,
    }
}

fn wire_upsert(source: &[u8], vectors: BTreeMap<String, Vec<f32>>) -> WireOp {
    WireOp::Upsert(WireDoc {
        pk: pk1(),
        source: source.to_vec(),
        vectors,
        sparse_vectors: BTreeMap::new(),
    })
}

// ---------------------------------------------------------------------------
// Strategies

fn any_pk() -> impl Strategy<Value = PrimaryKey> {
    prop_oneof![
        any::<u64>().prop_map(PrimaryKey::U64),
        any::<[u8; 16]>().prop_map(PrimaryKey::Uuid),
        "\\PC{1,24}".prop_map(PrimaryKey::Str),
    ]
}

/// JSON leaves whose text form round-trips exactly (floats are dyadic
/// fractions, which every float parser reads back bit for bit).
fn json_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<i32>().prop_map(|n| Value::from(f64::from(n) / 8.0)),
        "\\PC{0,8}".prop_map(Value::String),
    ]
}

/// JSON values nested up to depth 4, with a small key alphabet so patches
/// collide with existing keys.
fn json_value() -> impl Strategy<Value = Value> {
    json_leaf().prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(Value::Array),
            btree_map("[a-d]{1,2}", inner, 0..4)
                .prop_map(|map| Value::Object(map.into_iter().collect())),
        ]
    })
}

fn json_object() -> impl Strategy<Value = Map<String, Value>> {
    btree_map("[a-d]{1,2}", json_value(), 0..5).prop_map(|map| map.into_iter().collect())
}

fn finite_f32() -> impl Strategy<Value = f32> {
    prop_oneof![Just(0.0f32), Just(-0.0f32), -1.0e30f32..1.0e30f32]
}

fn dense() -> impl Strategy<Value = Vec<f32>> {
    vec(finite_f32(), 0..=8)
}

/// Sparse vectors of 0–8 entries, zero weights included, built from pairs in
/// random order.
fn any_sparse() -> impl Strategy<Value = SparseVector> {
    btree_map(0u32..64, prop_oneof![Just(0.0f32), finite_f32()], 0..=8)
        .prop_flat_map(|map| Just(map.into_iter().collect::<Vec<_>>()).prop_shuffle())
        .prop_map(|pairs| {
            let (indices, values) = pairs.into_iter().unzip();
            SparseVector::new(indices, values).expect("canonical sparse vector")
        })
}

fn vector_name() -> impl Strategy<Value = String> {
    "[a-c]{0,2}"
}

fn any_document(pk: PrimaryKey) -> impl Strategy<Value = Document> {
    (
        json_object(),
        btree_map(vector_name(), dense(), 0..3),
        btree_map(vector_name(), any_sparse(), 0..3),
    )
        .prop_map(move |(source, vectors, sparse_vectors)| Document {
            pk: pk.clone(),
            source,
            vectors,
            sparse_vectors,
        })
}

fn any_mode() -> impl Strategy<Value = PatchMode> {
    prop_oneof![
        Just(PatchMode::MergeDeep),
        Just(PatchMode::MergeTop),
        Just(PatchMode::Replace)
    ]
}

fn any_patch(pk: PrimaryKey) -> impl Strategy<Value = DocOp> {
    (
        any_mode(),
        json_object(),
        vec("[a-d]{1,2}(\\.[a-d]{1,2}){0,2}", 0..3),
        btree_map(vector_name(), proptest::option::of(dense()), 0..3),
        btree_map(vector_name(), proptest::option::of(any_sparse()), 0..3),
        proptest::option::of(any_document(pk.clone())),
    )
        .prop_map(
            move |(mode, source, delete_keys, vectors, sparse_vectors, upsert)| DocOp::Patch {
                pk: pk.clone(),
                mode,
                source,
                delete_keys,
                vectors,
                sparse_vectors,
                upsert,
            },
        )
}

fn any_op_on(pk: PrimaryKey) -> impl Strategy<Value = DocOp> {
    prop_oneof![
        any_document(pk.clone()).prop_map(DocOp::Upsert),
        Just(DocOp::Delete(pk.clone())),
        any_patch(pk),
    ]
}

fn any_doc_op() -> impl Strategy<Value = DocOp> {
    any_pk().prop_flat_map(any_op_on)
}

// ---------------------------------------------------------------------------
// Codec

proptest! {
    #[test]
    fn every_doc_op_round_trips(op in any_doc_op()) {
        let record = encode(&op).unwrap();
        prop_assert_eq!(record.key.as_deref(), Some(&op.pk().canonical()[..]));
        prop_assert!(record.headers.is_empty());
        prop_assert_eq!(record.timestamp_ms, -1);
        prop_assert_eq!(value_of(&record)[0], 0x01);
        prop_assert_eq!(decode(&record).unwrap(), op);
    }
}

#[test]
fn the_wire_layout_matches_the_documented_types() {
    let mut vectors = BTreeMap::new();
    vectors.insert("v".to_string(), vec![1.0, -2.5]);
    let op = DocOp::Upsert(Document {
        vectors: vectors.clone(),
        ..doc(pk1(), json!({"a": 1}))
    });
    let expected = hand_built(&pk1(), &wire_upsert(br#"{"a":1}"#, vectors));
    assert_eq!(encode(&op).unwrap(), expected);
}

#[test]
fn a_record_with_a_mismatched_key_is_rejected() {
    let mut record = encode(&DocOp::Delete(PrimaryKey::U64(1))).unwrap();
    record.key = Some(Bytes::from(PrimaryKey::U64(2).canonical()));
    assert!(matches!(decode(&record), Err(CodecError::KeyMismatch)));

    record.key = Some(Bytes::from_static(&[0x03]));
    assert!(matches!(decode(&record), Err(CodecError::InvalidKey(_))));

    record.key = None;
    assert!(matches!(decode(&record), Err(CodecError::MissingKey)));
}

#[test]
fn an_unknown_codec_version_is_rejected() {
    let record = encode(&DocOp::Delete(pk1())).unwrap();
    let mut value = value_of(&record);
    value[0] = 0x02;
    let record = with_value(record, value);
    assert!(matches!(
        decode(&record),
        Err(CodecError::UnknownVersion(0x02))
    ));

    let empty = with_value(record.clone(), Vec::new());
    assert!(matches!(decode(&empty), Err(CodecError::MissingValue)));
    let mut none = record;
    none.value = None;
    assert!(matches!(decode(&none), Err(CodecError::MissingValue)));
}

#[test]
fn trailing_bytes_are_rejected() {
    let record = encode(&DocOp::Delete(pk1())).unwrap();
    let mut value = value_of(&record);
    value.push(0);
    assert!(matches!(
        decode(&with_value(record.clone(), value)),
        Err(CodecError::Malformed(_))
    ));

    // A truncated body is malformed too.
    let value = value_of(&record);
    assert!(matches!(
        decode(&with_value(record, value[..value.len() - 1].to_vec())),
        Err(CodecError::Malformed(_))
    ));
}

#[test]
fn a_non_object_source_is_rejected() {
    for source in [
        &b"[1, 2]"[..],
        b"\"text\"",
        b"null",
        b"{",
        b"{} {}",
        b"\xff",
    ] {
        let record = hand_built(&pk1(), &wire_upsert(source, BTreeMap::new()));
        assert!(
            matches!(decode(&record), Err(CodecError::Malformed(_))),
            "{source:?}"
        );
    }
    let record = hand_built(&pk1(), &wire_upsert(b"{\"a\": [1]}", BTreeMap::new()));
    decode(&record).unwrap();
}

#[test]
fn nan_vectors_are_rejected() {
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut vectors = BTreeMap::new();
        vectors.insert("v".to_string(), vec![1.0, bad]);
        let record = hand_built(&pk1(), &wire_upsert(b"{}", vectors.clone()));
        assert!(matches!(decode(&record), Err(CodecError::Malformed(_))));

        let upsert = DocOp::Upsert(Document {
            vectors: vectors.clone(),
            ..doc(pk1(), json!({}))
        });
        assert!(matches!(encode(&upsert), Err(CodecError::Malformed(_))));

        let patched = DocOp::Patch {
            pk: pk1(),
            mode: PatchMode::MergeDeep,
            source: Map::new(),
            delete_keys: Vec::new(),
            vectors: vectors
                .into_iter()
                .map(|(name, vector)| (name, Some(vector)))
                .collect(),
            sparse_vectors: BTreeMap::new(),
            upsert: None,
        };
        assert!(matches!(encode(&patched), Err(CodecError::Malformed(_))));
    }
}

#[test]
fn encode_refuses_invalid_keys_and_oversized_values() {
    let empty_key = DocOp::Delete(PrimaryKey::Str(String::new()));
    assert!(matches!(encode(&empty_key), Err(CodecError::InvalidKey(_))));

    let big = "x".repeat(MAX_RECORD_VALUE_BYTES);
    let upsert = DocOp::Upsert(doc(pk1(), json!({ "big": big })));
    assert!(matches!(encode(&upsert), Err(CodecError::TooLarge(n)) if n > MAX_RECORD_VALUE_BYTES));
}

#[test]
fn sparse_vectors_are_canonical() {
    let sv = sparse(&[5, 1], &[0.5, 2.0]);
    assert_eq!(sv.indices(), [1, 5]);
    assert_eq!(sv.values(), [2.0, 0.5]);
    assert_eq!(sv.len(), 2);
    assert!(!sv.is_empty());
    assert!(sparse(&[], &[]).is_empty());

    assert_eq!(
        SparseVector::new(vec![1, 1], vec![1.0, 2.0]),
        Err(SparseVectorError::DuplicateIndex(1))
    );
    assert_eq!(
        SparseVector::new(vec![1], vec![]),
        Err(SparseVectorError::LengthMismatch {
            indices: 1,
            values: 0
        })
    );
    assert_eq!(
        SparseVector::new(vec![1], vec![f32::NAN]),
        Err(SparseVectorError::NonFinite(1))
    );

    // The JSON form.
    let json = serde_json::to_value(&sv).unwrap();
    assert_eq!(json, json!({"indices": [1, 5], "values": [2.0, 0.5]}));
    assert_eq!(serde_json::from_value::<SparseVector>(json).unwrap(), sv);
    let unsorted: SparseVector =
        serde_json::from_value(json!({"indices": [5, 1], "values": [0.5, 2.0]})).unwrap();
    assert_eq!(unsorted, sv);
    assert!(
        serde_json::from_value::<SparseVector>(json!({"indices": [1, 1], "values": [1, 2]}))
            .is_err()
    );

    // A record can never carry a non-canonical sparse vector.
    let wire = |indices: Vec<u32>, values: Vec<f32>| {
        let mut sparse_vectors = BTreeMap::new();
        sparse_vectors.insert("s".to_string(), RawSparse { indices, values });
        hand_built(
            &pk1(),
            &WireOp::Upsert(WireDoc {
                pk: pk1(),
                source: b"{}".to_vec(),
                vectors: BTreeMap::new(),
                sparse_vectors,
            }),
        )
    };
    assert!(matches!(
        decode(&wire(vec![3, 3], vec![1.0, 2.0])),
        Err(CodecError::Malformed(_))
    ));
    assert!(matches!(
        decode(&wire(vec![3], vec![1.0, 2.0])),
        Err(CodecError::Malformed(_))
    ));
    assert!(matches!(
        decode(&wire(vec![3], vec![f32::INFINITY])),
        Err(CodecError::Malformed(_))
    ));
    let DocOp::Upsert(decoded) = decode(&wire(vec![5, 1], vec![0.5, 2.0])).unwrap() else {
        panic!("not an upsert");
    };
    assert_eq!(decoded.sparse_vectors["s"], sv);
}

// ---------------------------------------------------------------------------
// Patches

fn current() -> Document {
    let mut vectors = BTreeMap::new();
    vectors.insert("dense".to_string(), vec![1.0, 2.0]);
    vectors.insert("other".to_string(), vec![3.0]);
    let mut sparse_vectors = BTreeMap::new();
    sparse_vectors.insert("words".to_string(), sparse(&[1, 2], &[0.5, 0.0]));
    sparse_vectors.insert("more".to_string(), sparse(&[7], &[1.0]));
    Document {
        vectors,
        sparse_vectors,
        ..doc(
            pk1(),
            json!({
                "a": {"b": 1, "c": [1, 2], "n": {"x": 1}},
                "d": 1,
                "s": "keep"
            }),
        )
    }
}

#[test]
fn merge_deep_merges_nested_objects_and_replaces_arrays() {
    let op = patch(
        pk1(),
        PatchMode::MergeDeep,
        json!({
            "a": {"c": [3], "e": {"f": 1}, "n": 5},
            "d": null,
            "s": {"now": "object"},
            "new": [1]
        }),
    );
    let patched = apply_patch(Some(&current()), &op).unwrap();
    assert_eq!(
        Value::Object(patched.source),
        json!({
            "a": {"b": 1, "c": [3], "e": {"f": 1}, "n": 5},
            "d": null,
            "s": {"now": "object"},
            "new": [1]
        })
    );
    assert_eq!(patched.pk, pk1());
    assert_eq!(patched.vectors, current().vectors);
    assert_eq!(patched.sparse_vectors, current().sparse_vectors);
}

#[test]
fn merge_top_replaces_top_level_keys() {
    let op = patch(pk1(), PatchMode::MergeTop, json!({"a": {"c": 3}, "z": 1}));
    let patched = apply_patch(Some(&current()), &op).unwrap();
    assert_eq!(
        Value::Object(patched.source),
        json!({"a": {"c": 3}, "d": 1, "s": "keep", "z": 1})
    );
}

#[test]
fn replace_overwrites_the_source_but_keeps_vectors() {
    let op = patch(pk1(), PatchMode::Replace, json!({"only": true}));
    let patched = apply_patch(Some(&current()), &op).unwrap();
    assert_eq!(Value::Object(patched.source), json!({"only": true}));
    assert_eq!(patched.vectors, current().vectors);
    assert_eq!(patched.sparse_vectors, current().sparse_vectors);
}

#[test]
fn delete_keys_remove_nested_paths_and_ignore_missing_ones() {
    let current = doc(
        pk1(),
        json!({
            "a": {"b": {"c": 1, "d": 2}},
            "e": [{"f": 1}],
            "g": 1,
            "h": 2
        }),
    );
    // "added" shows the deletes run after the merge; "e.f" shows arrays are
    // not descended; "h.x" walks into a number.
    let op = patch_with(
        pk1(),
        PatchMode::MergeDeep,
        json!({"added": 1}),
        &["a.b.c", "missing.x", "e.f", "g", "h.x", "added", "a.zz"],
        None,
    );
    let patched = apply_patch(Some(&current), &op).unwrap();
    assert_eq!(
        Value::Object(patched.source),
        json!({"a": {"b": {"d": 2}}, "e": [{"f": 1}], "h": 2})
    );
}

#[test]
fn a_patch_of_a_missing_key_is_a_noop() {
    let op = patch(pk1(), PatchMode::MergeDeep, json!({"a": 1}));
    assert_eq!(apply_patch(None, &op), None);
    assert_eq!(fold(None, [&op]), None);
    assert!(needs_current([&op]));
}

#[test]
fn a_patch_of_a_missing_key_with_upsert_inserts_it() {
    let upsert = doc(pk1(), json!({"fresh": true}));
    let op = patch_with(
        pk1(),
        PatchMode::MergeDeep,
        json!({"a": 1}),
        &[],
        Some(upsert.clone()),
    );
    // Missing: the upsert document is inserted as it is.
    assert_eq!(apply_patch(None, &op), Some(upsert.clone()));
    // Present: the upsert document is ignored and the patch applies.
    let existing = doc(pk1(), json!({"b": 2}));
    assert_eq!(
        apply_patch(Some(&existing), &op).map(|d| Value::Object(d.source)),
        Some(json!({"a": 1, "b": 2}))
    );

    // An upsert document for another key never inserts anything.
    let wrong = patch_with(
        pk1(),
        PatchMode::MergeDeep,
        json!({"a": 1}),
        &[],
        Some(doc(PrimaryKey::U64(9), json!({}))),
    );
    assert_eq!(apply_patch(None, &wrong), None);
}

#[test]
fn apply_patch_applies_any_other_op_as_it_is() {
    let upsert = doc(pk1(), json!({"x": 1}));
    assert_eq!(
        apply_patch(Some(&current()), &DocOp::Upsert(upsert.clone())),
        Some(upsert)
    );
    assert_eq!(apply_patch(Some(&current()), &DocOp::Delete(pk1())), None);
}

fn vector_patch(
    vectors: BTreeMap<String, Option<Vec<f32>>>,
    sparse_vectors: BTreeMap<String, Option<SparseVector>>,
) -> DocOp {
    DocOp::Patch {
        pk: pk1(),
        mode: PatchMode::MergeDeep,
        source: Map::new(),
        delete_keys: Vec::new(),
        vectors,
        sparse_vectors,
        upsert: None,
    }
}

#[test]
fn a_none_vector_deletes_it() {
    let mut vectors = BTreeMap::new();
    vectors.insert("dense".to_string(), None);
    vectors.insert("missing".to_string(), None);
    vectors.insert("added".to_string(), Some(vec![9.0]));
    let patched = apply_patch(Some(&current()), &vector_patch(vectors, BTreeMap::new())).unwrap();
    let mut expected = BTreeMap::new();
    expected.insert("added".to_string(), vec![9.0]);
    expected.insert("other".to_string(), vec![3.0]);
    assert_eq!(patched.vectors, expected);
    assert_eq!(patched.sparse_vectors, current().sparse_vectors);
    assert_eq!(patched.source, current().source);
}

#[test]
fn a_none_sparse_vector_deletes_it() {
    let mut sparse_vectors = BTreeMap::new();
    sparse_vectors.insert("words".to_string(), None);
    sparse_vectors.insert("more".to_string(), Some(sparse(&[8], &[2.0])));
    let patched = apply_patch(
        Some(&current()),
        &vector_patch(BTreeMap::new(), sparse_vectors),
    )
    .unwrap();
    let mut expected = BTreeMap::new();
    expected.insert("more".to_string(), sparse(&[8], &[2.0]));
    assert_eq!(patched.sparse_vectors, expected);
    assert_eq!(patched.vectors, current().vectors);
}

// ---------------------------------------------------------------------------
// The per-key fold

/// An independent latest-wins model: the last upsert or delete decides the
/// base state (the committed `current` when there is none), and only the
/// patches after it apply, in order.
fn latest_wins(current: Option<Document>, ops: &[DocOp]) -> Option<Document> {
    let last_write = ops
        .iter()
        .rposition(|op| !matches!(op, DocOp::Patch { .. }));
    let (base, patches) = match last_write {
        Some(i) => {
            let base = match &ops[i] {
                DocOp::Upsert(doc) => Some(doc.clone()),
                _ => None,
            };
            (base, &ops[i + 1..])
        }
        None => (current, ops),
    };
    patches
        .iter()
        .fold(base, |state, op| apply_patch(state.as_ref(), op))
}

/// Whether some patch precedes every upsert and delete.
fn a_patch_comes_first(ops: &[DocOp]) -> bool {
    let first_patch = ops.iter().position(|op| matches!(op, DocOp::Patch { .. }));
    let first_write = ops.iter().position(|op| !matches!(op, DocOp::Patch { .. }));
    match (first_patch, first_write) {
        (Some(patch), Some(write)) => patch < write,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

fn one_key_history() -> impl Strategy<Value = (Option<Document>, Option<Document>, Vec<DocOp>)> {
    let pk = pk1();
    (
        proptest::option::of(any_document(pk.clone())),
        proptest::option::of(any_document(pk.clone())),
        vec(any_op_on(pk), 0..=20),
    )
}

proptest! {
    #[test]
    fn fold_equals_sequential_application((current, other, ops) in one_key_history()) {
        let folded = fold(current.clone(), &ops);
        prop_assert_eq!(&folded, &latest_wins(current, &ops));

        let needs = needs_current(&ops);
        prop_assert_eq!(needs, a_patch_comes_first(&ops));
        // With no ops there is nothing to resolve (the fold is `current`);
        // otherwise a first upsert or delete makes `current` irrelevant.
        if !needs && !ops.is_empty() {
            prop_assert_eq!(&fold(other, &ops), &folded);
            prop_assert_eq!(&fold(None, &ops), &folded);
        }
    }
}
