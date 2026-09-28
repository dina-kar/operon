//! MCP document ids (plan M1.6 Task 7 rule 8; Review Focus 1).

use operon_collection::PrimaryKey;
use operon_mcp::DocId;
use serde_json::json;

fn parse(value: serde_json::Value) -> Result<DocId, serde_json::Error> {
    serde_json::from_value(value)
}

#[test]
fn doc_ids_parse_numbers_strings_and_uuids() {
    let max = parse(json!(18446744073709551615u64)).expect("u64");
    assert_eq!(max, DocId::Number(u64::MAX));
    assert_eq!(
        PrimaryKey::try_from(max).unwrap(),
        PrimaryKey::U64(u64::MAX)
    );

    let s = parse(json!("k")).expect("string");
    assert_eq!(
        PrimaryKey::try_from(s).unwrap(),
        PrimaryKey::Str("k".into())
    );

    let uuid = parse(json!({"uuid": "0190F5C4-6C1E-7B3A-9D2E-4F5A6B7C8D9E"})).expect("uuid");
    let pk = PrimaryKey::try_from(uuid).unwrap();
    assert_eq!(
        pk,
        PrimaryKey::Uuid([
            0x01, 0x90, 0xf5, 0xc4, 0x6c, 0x1e, 0x7b, 0x3a, 0x9d, 0x2e, 0x4f, 0x5a, 0x6b, 0x7c,
            0x8d, 0x9e
        ])
    );
    let back = DocId::from(&pk);
    assert_eq!(
        back,
        DocId::Uuid {
            uuid: "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e".into()
        }
    );
    assert_eq!(
        serde_json::to_value(&back).unwrap(),
        json!({"uuid": "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"})
    );
    assert_eq!(
        serde_json::to_value(DocId::from(&PrimaryKey::U64(u64::MAX))).unwrap(),
        json!(18446744073709551615u64)
    );
    assert_eq!(
        serde_json::to_value(DocId::from(&PrimaryKey::Str("k".into()))).unwrap(),
        json!("k")
    );
}

#[test]
fn a_bool_or_fraction_is_not_a_doc_id() {
    for value in [json!(true), json!(1.5), json!(-1), json!(null), json!([1])] {
        assert!(parse(value.clone()).is_err(), "{value}");
    }
}

#[test]
fn a_malformed_uuid_is_an_invalid_argument() {
    for bad in [
        "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9",
        "0190f5c46c1e7b3a9d2e4f5a6b7c8d9e",
        "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9g",
        "{0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e}",
        "",
    ] {
        let err = PrimaryKey::try_from(DocId::Uuid { uuid: bad.into() }).unwrap_err();
        assert_eq!(err.code, "invalid_argument", "{bad:?}");
    }
    let err = PrimaryKey::try_from(DocId::String(String::new())).unwrap_err();
    assert_eq!(err.code, "invalid_argument");
}
