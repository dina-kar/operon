//! `PrimaryKey` canonical bytes, ordering, validation and `partition_of`.

use operon_collection::{CodecError, MAX_STR_PK_BYTES, PrimaryKey, partition_of};
use proptest::prelude::*;

fn uuid_one() -> PrimaryKey {
    let mut bytes = [0u8; 16];
    bytes[15] = 1;
    PrimaryKey::Uuid(bytes)
}

#[test]
fn canonical_bytes_are_tagged_as_specified() {
    assert_eq!(
        PrimaryKey::U64(42).canonical(),
        [0x01, 0, 0, 0, 0, 0, 0, 0, 0x2a]
    );
    assert_eq!(PrimaryKey::Str("a".to_string()).canonical(), [0x03, 0x61]);
    let mut uuid = vec![0x02];
    uuid.extend([0u8; 15]);
    uuid.push(0x01);
    assert_eq!(uuid_one().canonical(), uuid);
}

#[test]
fn partition_of_matches_the_xxh3_reference() {
    // Golden values from the reference xxHash `xxh3_64` (python-xxhash) over
    // the canonical bytes (plan M1.1 Task 5).
    let golden = [
        (PrimaryKey::U64(0), 0x313c_2dd2_ec99_6e80_u64, 1, 0),
        (PrimaryKey::U64(42), 0x46bb_72a9_ed34_27e4, 1, 4),
        (
            PrimaryKey::Str("a".to_string()),
            0x14f1_ec51_356e_7e9e,
            2,
            6,
        ),
        (
            PrimaryKey::Str("doc-1".to_string()),
            0xe387_4f86_5254_e70e,
            2,
            6,
        ),
        (uuid_one(), 0xcb19_967b_88ec_296b, 1, 3),
    ];
    for (pk, hash, mod3, mod8) in golden {
        assert_eq!(xxhash_rust::xxh3::xxh3_64(&pk.canonical()), hash, "{pk:?}");
        assert_eq!(partition_of(&pk, 3), mod3, "{pk:?} mod 3");
        assert_eq!(partition_of(&pk, 8), mod8, "{pk:?} mod 8");
        assert_eq!(partition_of(&pk, 1), 0, "{pk:?} mod 1");
    }
}

#[test]
fn a_string_key_over_512_bytes_is_invalid() {
    let longest = PrimaryKey::Str("x".repeat(MAX_STR_PK_BYTES));
    longest.validate().unwrap();
    assert_eq!(
        PrimaryKey::from_canonical(&longest.canonical()).unwrap(),
        longest
    );

    let too_long = PrimaryKey::Str("x".repeat(MAX_STR_PK_BYTES + 1));
    assert!(matches!(
        too_long.validate(),
        Err(CodecError::InvalidKey(_))
    ));
    assert!(matches!(
        PrimaryKey::from_canonical(&too_long.canonical()),
        Err(CodecError::InvalidKey(_))
    ));
    // The limit counts bytes, not chars: 171 three-byte chars are 513 bytes.
    assert!(PrimaryKey::Str("€".repeat(171)).validate().is_err());
}

#[test]
fn an_empty_string_key_is_invalid() {
    let empty = PrimaryKey::Str(String::new());
    assert!(matches!(empty.validate(), Err(CodecError::InvalidKey(_))));
    assert!(matches!(
        PrimaryKey::from_canonical(&[0x03]),
        Err(CodecError::InvalidKey(_))
    ));
}

#[test]
fn malformed_canonical_bytes_are_invalid() {
    for bytes in [
        &[][..],
        &[0x00, 1],
        &[0x04, b'a'],
        &[0x01, 0, 0, 0, 0, 0, 0, 0],
        &[0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        &[0x02, 1, 2, 3],
        &[0x03, 0xff, 0xfe],
    ] {
        assert!(
            matches!(
                PrimaryKey::from_canonical(bytes),
                Err(CodecError::InvalidKey(_))
            ),
            "{bytes:?}"
        );
    }
}

fn any_pk() -> impl Strategy<Value = PrimaryKey> {
    prop_oneof![
        // Small values collide often, so equal keys are generated too.
        (0u64..4).prop_map(PrimaryKey::U64),
        any::<u64>().prop_map(PrimaryKey::U64),
        prop_oneof![
            Just([0u8; 16]),
            any::<[u8; 16]>(),
            (0u8..3).prop_map(|b| [b; 16]),
        ]
        .prop_map(PrimaryKey::Uuid),
        "[a-c]{1,3}".prop_map(PrimaryKey::Str),
        "\\PC{1,40}".prop_map(PrimaryKey::Str),
    ]
}

proptest! {
    #[test]
    fn derived_order_equals_canonical_byte_order(a in any_pk(), b in any_pk()) {
        prop_assert_eq!(a.cmp(&b), a.canonical().cmp(&b.canonical()));
    }

    #[test]
    fn canonical_round_trips(pk in any_pk()) {
        prop_assert_eq!(PrimaryKey::from_canonical(&pk.canonical()).unwrap(), pk);
    }
}
