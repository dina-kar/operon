use operon_common::{NamespaceId, StreamId};

#[test]
fn ids_display_as_plain_decimal() {
    assert_eq!(NamespaceId(42).to_string(), "42");
    assert_eq!(StreamId(0).to_string(), "0");
    assert_eq!(StreamId(u64::MAX).to_string(), "18446744073709551615");
}

#[test]
fn ids_parse_their_own_display_form() {
    for n in [0, 1, 7, 42, 1_000_000, u64::MAX] {
        let id = NamespaceId(n);
        assert_eq!(id.to_string().parse::<NamespaceId>().unwrap(), id);
        let id = StreamId(n);
        assert_eq!(id.to_string().parse::<StreamId>().unwrap(), id);
    }
}

#[test]
fn ids_reject_non_canonical_spellings() {
    for bad in [
        "",
        "+1",
        "-1",
        "01",
        "00",
        " 1",
        "1 ",
        "1a",
        "0x10",
        "18446744073709551616",
    ] {
        assert!(bad.parse::<NamespaceId>().is_err(), "accepted {bad:?}");
        assert!(bad.parse::<StreamId>().is_err(), "accepted {bad:?}");
    }
}

#[test]
fn ids_order_numerically_and_serialize_as_bare_integers() {
    assert!(StreamId(2) < StreamId(10));
    let bytes = postcard::to_stdvec(&NamespaceId(300)).unwrap();
    assert_eq!(bytes, postcard::to_stdvec(&300u64).unwrap());
    let back: NamespaceId = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(back, NamespaceId(300));
}

#[test]
fn collection_ids_round_trip_display_and_parse() {
    use operon_common::CollectionId;
    for n in [0, 1, 42, u64::MAX] {
        let id = CollectionId(n);
        assert_eq!(id.to_string(), n.to_string());
        assert_eq!(id.to_string().parse::<CollectionId>().unwrap(), id);
    }
    assert!("01".parse::<CollectionId>().is_err());
    assert!(CollectionId(2) < CollectionId(10));
    let bytes = postcard::to_stdvec(&CollectionId(300)).unwrap();
    assert_eq!(bytes, postcard::to_stdvec(&300u64).unwrap());
}
