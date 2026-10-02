//! The order-preserving tuple codec (R1 plan Task 2; design §20 §4.3).

use std::borrow::Cow;
use std::cmp::Ordering;

use loams_tikv::tuple::{self, Elem};
use proptest::prelude::*;

/// The reference order: null < int64 < float64 < bool < string < bytes <
/// array; floats totally ordered with NaN last and `-0.0 == 0.0`; strings and
/// bytes bytewise; arrays element by element, a shorter prefix first.
fn cmp_ref(a: &Elem, b: &Elem) -> Ordering {
    fn rank(e: &Elem) -> u8 {
        match e {
            Elem::Null => 0,
            Elem::I64(_) => 1,
            Elem::F64(_) => 2,
            Elem::Bool(_) => 3,
            Elem::Str(_) => 4,
            Elem::Bytes(_) => 5,
            Elem::Array(_) => 6,
        }
    }
    match (a, b) {
        (Elem::I64(x), Elem::I64(y)) => x.cmp(y),
        (Elem::F64(x), Elem::F64(y)) => match (x.is_nan(), y.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => x.partial_cmp(y).expect("neither is NaN"),
        },
        (Elem::Bool(x), Elem::Bool(y)) => x.cmp(y),
        (Elem::Str(x), Elem::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Elem::Bytes(x), Elem::Bytes(y)) => x.cmp(y),
        (Elem::Array(x), Elem::Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                match cmp_ref(p, q) {
                    Ordering::Equal => {}
                    other => return other,
                }
            }
            x.len().cmp(&y.len())
        }
        _ => rank(a).cmp(&rank(b)),
    }
}

fn enc(e: &Elem) -> Vec<u8> {
    let mut out = Vec::new();
    tuple::encode(&mut out, e);
    out
}

fn leaf() -> impl Strategy<Value = Elem<'static>> {
    let int = prop_oneof![
        any::<i64>(),
        Just(i64::MIN),
        Just(i64::MAX),
        Just(0i64),
        Just(-1i64),
        Just(1i64),
    ];
    let float = prop_oneof![
        any::<f64>(),
        Just(f64::NAN),
        Just(-f64::NAN),
        Just(0.0f64),
        Just(-0.0f64),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::MIN_POSITIVE),
        Just(-f64::MIN_POSITIVE),
    ];
    let text = prop::collection::vec(prop_oneof![Just('\0'), Just('a'), any::<char>()], 0..6)
        .prop_map(String::from_iter);
    let bytes = prop::collection::vec(prop_oneof![Just(0x00u8), Just(0xFFu8), any::<u8>()], 0..6);
    prop_oneof![
        Just(Elem::Null),
        int.prop_map(Elem::I64),
        float.prop_map(Elem::F64),
        any::<bool>().prop_map(Elem::Bool),
        text.prop_map(|s| Elem::Str(Cow::Owned(s))),
        bytes.prop_map(|b| Elem::Bytes(Cow::Owned(b))),
    ]
}

fn elem() -> impl Strategy<Value = Elem<'static>> {
    leaf().prop_recursive(3, 24, 4, |inner| {
        prop::collection::vec(inner, 0..4).prop_map(Elem::Array)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    #[test]
    fn tuple_order_matches_reference(a in elem(), b in elem()) {
        prop_assert_eq!(enc(&a).cmp(&enc(&b)), cmp_ref(&a, &b), "{:?} vs {:?}", a, b);
    }

    #[test]
    fn decode_inverts_encode(a in elem(), b in elem()) {
        let mut buf = enc(&a);
        let first = buf.len();
        tuple::encode(&mut buf, &b);
        let (x, used) = tuple::decode(&buf).expect("decodes");
        prop_assert_eq!(used, first);
        prop_assert_eq!(&x, &a);
        let (y, rest) = tuple::decode(&buf[used..]).expect("decodes");
        prop_assert_eq!(used + rest, buf.len());
        prop_assert_eq!(&y, &b);
    }

    #[test]
    fn successor_bounds_every_extension(
        prefix in prop::collection::vec(prop_oneof![Just(0xFFu8), Just(0x00u8), any::<u8>()], 0..6),
        ext in prop::collection::vec(any::<u8>(), 0..6),
        other in prop::collection::vec(prop_oneof![Just(0xFFu8), any::<u8>()], 0..8),
    ) {
        let end = tuple::successor(&prefix);
        let mut key = prefix.clone();
        key.extend_from_slice(&ext);
        if end.is_empty() {
            // No upper bound exists: the prefix is empty or all 0xFF.
            prop_assert!(prefix.iter().all(|&b| b == 0xFF));
        } else {
            prop_assert!(key < end);
            prop_assert!(prefix < end);
            prop_assert!(!end.starts_with(&prefix));
            // The bound is tight: any key in [prefix, end) has the prefix,
            // and any key with the prefix is in [prefix, end).
            prop_assert_eq!(
                other.starts_with(&prefix),
                other >= prefix && other < end,
                "{:?} prefix {:?} end {:?}", other, prefix, end
            );
        }
    }
}

#[test]
fn the_type_order_is_the_design_order() {
    let ordered = [
        Elem::Null,
        Elem::I64(i64::MAX),
        Elem::F64(f64::NEG_INFINITY),
        Elem::Bool(false),
        Elem::Str(Cow::Borrowed("")),
        Elem::Bytes(Cow::Borrowed(&[])),
        Elem::Array(vec![]),
    ];
    for pair in ordered.windows(2) {
        assert!(
            enc(&pair[0]) < enc(&pair[1]),
            "{:?} < {:?}",
            pair[0],
            pair[1]
        );
    }
}
