//! The segment format.

use bytes::Bytes;
use operon_common::StreamId;
use operon_log::segment::{self, SegmentBuilder, TRAILER_LEN};
use operon_log::{Encoding, LogError, Record, batch};
use proptest::prelude::*;

fn records(tag: usize, n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: Some(Bytes::from(format!("{tag}/{i}"))),
            value: Some(Bytes::from(vec![b'x'; (tag + i) % 7])),
            headers: vec![("t".to_string(), None)],
            timestamp_ms: (tag * 100 + i) as i64,
        })
        .collect()
}

/// A segment from base 100 with one batch per entry of `sizes`.
fn build(sizes: &[usize]) -> (Bytes, segment::SegmentFooter, Vec<Record>) {
    let mut builder = SegmentBuilder::new(StreamId(3), 1, 100, Encoding::Kafka);
    let mut all = Vec::new();
    for (tag, &n) in sizes.iter().enumerate() {
        let batch_records = records(tag, n);
        let max_ts = (tag * 100 + n - 1) as i64;
        let encoded = batch::encode(&batch_records).expect("encode");
        builder
            .push_batch(encoded, n as u32, max_ts)
            .expect("push batch");
        all.extend(batch_records);
    }
    let (bytes, footer) = builder.finish();
    (bytes, footer, all)
}

/// Reads the footer the way a reader does: trailer, then header and index.
fn read_footer(bytes: &[u8]) -> Result<segment::SegmentFooter, LogError> {
    let tail: [u8; 40] = bytes[bytes.len() - TRAILER_LEN as usize..]
        .try_into()
        .expect("40 bytes");
    let trailer = segment::parse_trailer(&tail)?;
    let index = trailer.index_range(bytes.len() as u64)?;
    segment::parse_footer(
        &bytes[..40],
        &bytes[index.start as usize..index.end as usize],
        &trailer,
    )
}

proptest! {
    #[test]
    fn segments_round_trip_with_absolute_offsets(sizes in prop::collection::vec(1usize..6, 1..12)) {
        let (bytes, footer, all) = build(&sizes);
        prop_assert_eq!(segment::parse(&bytes).unwrap(), footer.clone());
        prop_assert_eq!(read_footer(&bytes).unwrap(), footer.clone());
        prop_assert_eq!(footer.stream, StreamId(3));
        prop_assert_eq!(footer.partition, 1);
        prop_assert_eq!(footer.base_offset, 100);
        prop_assert_eq!(footer.end_offset, 100 + all.len() as u64);
        prop_assert_eq!(footer.batches.len(), sizes.len());

        let data = &bytes[footer.data.start as usize..footer.data.end as usize];
        let decoded = batch::decode(data, 100).unwrap();
        prop_assert_eq!(decoded.len(), all.len());
        for (i, (got, want)) in decoded.iter().zip(&all).enumerate() {
            prop_assert_eq!(got.offset, 100 + i as u64);
            prop_assert_eq!(&got.record, want);
        }
        // Every batch carries its absolute base offset.
        for (i, entry) in footer.batches.iter().enumerate() {
            let range = footer.batch_range(i);
            let batch_bytes = &bytes[range.start as usize..range.end as usize];
            prop_assert_eq!(batch::base_offset(batch_bytes), Some(entry.base_offset));
            let one = batch::decode(batch_bytes, entry.base_offset).unwrap();
            prop_assert_eq!(one.len() as u64, footer.batch_end_offset(i) - entry.base_offset);
        }
    }
}

#[test]
fn position_for_finds_the_batch_holding_an_offset() {
    let (_, footer, _) = build(&[3, 2, 4]);
    // Batches hold [100, 103), [103, 105) and [105, 109).
    let positions: Vec<u64> = footer.batches.iter().map(|b| b.position).collect();
    assert_eq!(footer.position_for(100), Some(positions[0]));
    assert_eq!(footer.position_for(102), Some(positions[0]));
    assert_eq!(footer.position_for(103), Some(positions[1]));
    assert_eq!(footer.position_for(104), Some(positions[1]));
    assert_eq!(footer.position_for(106), Some(positions[2]));
    assert_eq!(footer.position_for(108), Some(positions[2]));
    assert_eq!(footer.position_for(99), None);
    assert_eq!(footer.position_for(109), None);
    assert_eq!(positions[0], segment::HEADER_LEN);
}

#[test]
fn every_single_byte_corruption_is_detected() {
    let (bytes, _, _) = build(&[2, 1, 3]);
    for at in 0..bytes.len() {
        let mut corrupted = bytes.to_vec();
        corrupted[at] ^= 0x10;
        let err = segment::parse(&corrupted).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "byte {at}: {err:?}");
    }
    // The footer path (trailer, header, index) catches everything outside the
    // data and its checksum, which only a full read can verify (a reader
    // relies on each batch's own CRC instead).
    let (_, footer, _) = build(&[2, 1, 3]);
    let data_crc = bytes.len() - 24..bytes.len() - 20;
    let outside = |at: &usize| !footer.data.contains(&(*at as u64)) && !data_crc.contains(at);
    for at in (0..bytes.len()).filter(outside) {
        let mut corrupted = bytes.to_vec();
        corrupted[at] ^= 0x10;
        let err = read_footer(&corrupted).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "byte {at}: {err:?}");
    }
}

#[test]
fn truncated_segments_are_rejected() {
    let (bytes, _, _) = build(&[2, 3]);
    for len in 0..bytes.len() {
        let err = segment::parse(&bytes[..len]).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "len {len}: {err:?}");
    }
}

#[test]
fn an_unknown_version_is_rejected() {
    let (bytes, _, _) = build(&[2]);
    let mut changed = bytes.to_vec();
    changed[8..10].copy_from_slice(&2u16.to_le_bytes());
    // Fix up the header checksum so the version check itself is exercised.
    let crc = crc32c::crc32c(&changed[..40]);
    let at = changed.len() - 20;
    changed[at..at + 4].copy_from_slice(&crc.to_le_bytes());
    match segment::parse(&changed) {
        Err(LogError::Corrupt(message)) => assert!(message.contains("version"), "{message}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_arrow_segment_is_unsupported() {
    let mut builder = SegmentBuilder::new(StreamId(1), 0, 0, Encoding::Arrow);
    builder
        .push_batch(batch::encode(&records(0, 1)).unwrap(), 1, 0)
        .unwrap();
    let (bytes, _) = builder.finish();
    assert!(matches!(
        segment::parse(&bytes),
        Err(LogError::UnsupportedEncoding(1))
    ));
}

#[test]
fn the_builder_rejects_batches_that_would_break_offset_contiguity() {
    let mut builder = SegmentBuilder::new(StreamId(1), 0, 0, Encoding::Kafka);
    let two = batch::encode(&records(0, 2)).unwrap();
    // A record count that disagrees with the batch.
    let err = builder.push_batch(two.clone(), 3, 0).unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    // Two batches passed as one.
    let joined = Bytes::from([two.as_ref(), two.as_ref()].concat());
    let err = builder.push_batch(joined, 4, 0).unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    // Garbage.
    let err = builder
        .push_batch(Bytes::from_static(b"nope"), 1, 0)
        .unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
    // Nothing was added by the rejected pushes.
    assert_eq!(builder.next_offset(), 0);
    builder.push_batch(two, 2, 0).unwrap();
    assert_eq!(builder.next_offset(), 2);
}

proptest! {
    #[test]
    fn garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
        let _ = segment::parse(&bytes);
        let mut framed = b"OPNSEG\0\0\x01\0\0\0".to_vec();
        framed.extend_from_slice(&bytes);
        framed.extend_from_slice(b"OPNSEGFT");
        let _ = segment::parse(&framed);
    }
}
