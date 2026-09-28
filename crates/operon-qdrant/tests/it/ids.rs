//! Point ids (plan M1.4 Task 1; Ruling 18; Review Focus 1).

use operon_collection::PrimaryKey;
use operon_qdrant::ids::{pk_predecessor, pk_to_json, point_id_from_grpc};
use operon_qdrant::proto::qdrant as pb;
use operon_qdrant::{GatewayError, PointId};
use proptest::prelude::*;
use serde_json::{Value, json};
use uuid::Uuid;

#[test]
fn u64_ids_round_trip() {
    for n in [0, 42, u64::MAX] {
        let v = json!(n);
        let id = PointId::from_json(&v).expect("valid");
        assert_eq!(id, PointId::Num(n));
        assert_eq!(pk_to_json(&id.to_pk()), v);
    }
}

#[test]
fn every_uuid_form_is_accepted_and_canonicalized() {
    let cases = [
        (
            "FA38D5724C314579AEDC1960D79DF6DF",
            "fa38d572-4c31-4579-aedc-1960d79df6df",
        ),
        (
            "{fa38d572-4c31-4579-aedc-1960d79df6df}",
            "fa38d572-4c31-4579-aedc-1960d79df6df",
        ),
        (
            "urn:uuid:fa38d572-4c31-4579-aedc-1960d79df6df",
            "fa38d572-4c31-4579-aedc-1960d79df6df",
        ),
        (
            "11111111-1111-1111-1111-111111111111",
            "11111111-1111-1111-1111-111111111111",
        ),
    ];
    for (input, canonical) in cases {
        let id = PointId::from_json(&json!(input)).expect(input);
        assert!(matches!(id, PointId::Uuid(_)), "{input}");
        assert_eq!(pk_to_json(&id.to_pk()), json!(canonical), "{input}");
    }
}

#[test]
fn invalid_ids_are_rejected_with_qdrant_text() {
    for v in [
        json!("123"),
        json!(-1),
        json!(1.5),
        json!(1.0),
        json!("x"),
        Value::Null,
    ] {
        let err = PointId::from_json(&v).expect_err(&v.to_string());
        assert!(matches!(err, GatewayError::Format { .. }), "{v}: {err:?}");
        let text = err.to_string();
        assert!(
            text.contains(
                "is not a valid point ID, valid values are either an unsigned integer or a UUID"
            ),
            "{v}: {text}"
        );
        assert!(
            text.starts_with("Format error in JSON body: value "),
            "{text}"
        );
    }
}

#[test]
fn path_ids_parse_digits_first() {
    assert_eq!(PointId::parse_path("7").expect("digits"), PointId::Num(7));
    let uuid = "fa38d572-4c31-4579-aedc-1960d79df6df";
    assert_eq!(
        PointId::parse_path(uuid).expect("uuid"),
        PointId::Uuid(Uuid::parse_str(uuid).unwrap())
    );
    let err = PointId::parse_path("abc").expect_err("not an id");
    assert_eq!(err.http_status(), http::StatusCode::BAD_REQUEST);
    assert!(
        err.to_string()
            .contains("Can not recognize \"abc\" as point id"),
        "{err}"
    );
}

#[test]
fn string_keys_are_output_as_their_text() {
    assert_eq!(pk_to_json(&PrimaryKey::Str("doc-1".into())), json!("doc-1"));
}

#[test]
fn grpc_uuid_errors_use_qdrant_text() {
    let id = pb::PointId {
        point_id_options: Some(pb::point_id::PointIdOptions::Uuid("zz".into())),
    };
    let status = point_id_from_grpc(&id).expect_err("bad uuid").grpc_status();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("Unable to parse UUID: zz"),
        "{status:?}"
    );
    let num = pb::PointId {
        point_id_options: Some(pb::point_id::PointIdOptions::Num(9)),
    };
    assert_eq!(point_id_from_grpc(&num).expect("num"), PointId::Num(9));
}

#[test]
fn predecessor_edges() {
    assert_eq!(pk_predecessor(&PrimaryKey::U64(0)), None);
    assert_eq!(
        pk_predecessor(&PrimaryKey::Uuid([0; 16])),
        Some(PrimaryKey::U64(u64::MAX))
    );
    let mut one = [0; 16];
    one[15] = 1;
    assert_eq!(
        pk_predecessor(&PrimaryKey::Uuid(one)),
        Some(PrimaryKey::Uuid([0; 16]))
    );
    let mut carry = [0; 16];
    carry[14] = 1;
    let mut below = [0; 16];
    below[15] = 0xff;
    assert_eq!(
        pk_predecessor(&PrimaryKey::Uuid(carry)),
        Some(PrimaryKey::Uuid(below))
    );
}

fn point_id() -> impl Strategy<Value = PointId> {
    prop_oneof![
        // Small values make neighbours and ties likely.
        (0u64..4).prop_map(PointId::Num),
        any::<u64>().prop_map(PointId::Num),
        Just(PointId::Num(u64::MAX)),
        (0u128..4).prop_map(|n| PointId::Uuid(Uuid::from_u128(n))),
        any::<u128>().prop_map(|n| PointId::Uuid(Uuid::from_u128(n))),
    ]
}

/// The next U64/Uuid key in canonical order (the test's own model).
fn successor(pk: &PrimaryKey) -> Option<PrimaryKey> {
    match pk {
        PrimaryKey::U64(u64::MAX) => Some(PrimaryKey::Uuid([0; 16])),
        PrimaryKey::U64(n) => Some(PrimaryKey::U64(n + 1)),
        PrimaryKey::Uuid(b) => u128::from_be_bytes(*b)
            .checked_add(1)
            .map(|n| PrimaryKey::Uuid(n.to_be_bytes())),
        PrimaryKey::Str(_) => None,
    }
}

proptest! {
    #[test]
    fn point_id_order_matches_canonical_pk_order(a in point_id(), b in point_id()) {
        prop_assert_eq!(a.cmp(&b), a.to_pk().canonical().cmp(&b.to_pk().canonical()));
        prop_assert_eq!(a.cmp(&b), a.to_pk().cmp(&b.to_pk()));
    }

    #[test]
    fn predecessor_is_the_previous_key(k in point_id()) {
        let k = k.to_pk();
        match pk_predecessor(&k) {
            None => prop_assert_eq!(k, PrimaryKey::U64(0)),
            Some(p) => {
                prop_assert!(p.canonical() < k.canonical());
                // Nothing lies between: the key after `p` is `k`.
                prop_assert_eq!(successor(&p), Some(k));
            }
        }
    }
}
