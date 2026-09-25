//! The delete-bitmap format (plan M1.1 Task 8 rule 8, Ruling 19).

use operon_text::{
    DELETE_BITMAP_MAGIC, DELETE_BITMAP_VERSION, TextError, decode_delete_bitmap,
    encode_delete_bitmap,
};
use proptest::prelude::*;
use roaring::RoaringBitmap;
use ulid::Ulid;

/// Recomputes the crc32c trailer of `bytes` after a deliberate edit.
fn reseal(bytes: &mut [u8]) {
    let body = bytes.len() - 4;
    let crc = crc32c(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
}

fn crc32c(bytes: &[u8]) -> u32 {
    // The Castagnoli CRC, bit by bit (independent of the implementation).
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn assert_corrupt(result: Result<(Ulid, u32, RoaringBitmap), TextError>, what: &str) {
    match result {
        Err(TextError::Corrupt(_)) => {}
        other => panic!("{what}: expected Corrupt, got {other:?}"),
    }
}

proptest! {
    #[test]
    fn delete_bitmaps_round_trip(
        split in any::<u128>(),
        doc_count in 1u32..200_000,
        ids in proptest::collection::vec(any::<u32>(), 0..2_000),
    ) {
        let deleted: RoaringBitmap = ids.into_iter().map(|id| id % doc_count).collect();
        let bytes = encode_delete_bitmap(Ulid(split), doc_count, &deleted).unwrap();
        let (decoded_split, decoded_count, decoded) = decode_delete_bitmap(&bytes).unwrap();
        prop_assert_eq!(decoded_split, Ulid(split));
        prop_assert_eq!(decoded_count, doc_count);
        prop_assert_eq!(decoded, deleted);
    }
}

#[test]
fn a_flipped_byte_is_corrupt() {
    let deleted: RoaringBitmap = [1, 5, 9, 1_000].into_iter().collect();
    let bytes = encode_delete_bitmap(Ulid::generate(), 2_000, &deleted).unwrap();
    for at in 0..bytes.len() {
        for bit in [0x01u8, 0x80] {
            let mut flipped = bytes.to_vec();
            flipped[at] ^= bit;
            assert_corrupt(
                decode_delete_bitmap(&flipped),
                &format!("byte {at} ^ {bit:#x}"),
            );
        }
    }
    for len in 0..bytes.len() {
        assert_corrupt(decode_delete_bitmap(&bytes[..len]), &format!("{len} bytes"));
    }
    let mut longer = bytes.to_vec();
    longer.push(0);
    assert_corrupt(decode_delete_bitmap(&longer), "a trailing byte");
}

#[test]
fn a_doc_id_beyond_doc_count_is_corrupt() {
    let deleted: RoaringBitmap = [3, 12].into_iter().collect();
    assert!(encode_delete_bitmap(Ulid::generate(), 12, &deleted).is_err());

    // A well-sealed bitmap whose doc count is too small for its ids.
    let mut bytes = encode_delete_bitmap(Ulid::generate(), 20, &deleted)
        .unwrap()
        .to_vec();
    bytes[22..26].copy_from_slice(&12u32.to_le_bytes());
    reseal(&mut bytes);
    assert_corrupt(decode_delete_bitmap(&bytes), "doc id 12 of 12 docs");
    bytes[22..26].copy_from_slice(&13u32.to_le_bytes());
    reseal(&mut bytes);
    assert_eq!(decode_delete_bitmap(&bytes).unwrap().2, deleted);
}

#[test]
fn the_header_layout_is_exact() {
    assert_eq!(DELETE_BITMAP_MAGIC, b"OPDB");
    assert_eq!(DELETE_BITMAP_VERSION, 1);
    let split = Ulid(0x0102_0304_0506_0708_090A_0B0C_0D0E_0F10_u128);
    let deleted: RoaringBitmap = [0, 7, 65_536].into_iter().collect();
    let bytes = encode_delete_bitmap(split, 70_000, &deleted).unwrap();

    assert_eq!(&bytes[0..4], b"OPDB");
    assert_eq!(&bytes[4..6], &1u16.to_le_bytes());
    assert_eq!(&bytes[6..22], &split.0.to_be_bytes());
    assert_eq!(&bytes[22..26], &70_000u32.to_le_bytes());
    assert_eq!(&bytes[26..34], &3u64.to_le_bytes());
    let mut portable = Vec::new();
    deleted.serialize_into(&mut portable).unwrap();
    assert_eq!(&bytes[34..bytes.len() - 4], &portable[..]);
    let body = bytes.len() - 4;
    assert_eq!(&bytes[body..], &crc32c(&bytes[..body]).to_le_bytes());

    // An unknown version and a wrong cardinality are corrupt even when sealed.
    let mut other = bytes.to_vec();
    other[4..6].copy_from_slice(&2u16.to_le_bytes());
    reseal(&mut other);
    assert_corrupt(decode_delete_bitmap(&other), "version 2");
    let mut other = bytes.to_vec();
    other[26..34].copy_from_slice(&4u64.to_le_bytes());
    reseal(&mut other);
    assert_corrupt(decode_delete_bitmap(&other), "cardinality 4");
}
