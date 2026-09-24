//! Kafka `RecordBatch` v2 encoding, checked against `kafka-protocol` as an oracle.

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use operon_log::{LogError, Record, batch};
use proptest::prelude::*;

fn record() -> impl Strategy<Value = Record> {
    let bytes =
        || prop::option::of(prop::collection::vec(any::<u8>(), 0..24).prop_map(Bytes::from));
    (
        bytes(),
        bytes(),
        prop::collection::vec(("[a-z]{0,6}", bytes()), 0..4),
        -(1i64 << 50)..(1i64 << 50),
    )
        .prop_map(|(key, value, headers, timestamp_ms)| Record {
            key,
            value,
            headers,
            timestamp_ms,
        })
}

fn sample(n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record {
            key: Some(Bytes::from(format!("k{i}"))),
            value: (i % 3 != 0).then(|| Bytes::from(vec![i as u8; i])),
            headers: vec![
                ("h".to_string(), Some(Bytes::from_static(b"v"))),
                ("n".to_string(), None),
            ],
            timestamp_ms: 1_700_000_000_000 + i as i64,
        })
        .collect()
}

proptest! {
    #[test]
    fn records_round_trip_with_contiguous_offsets(
        records in prop::collection::vec(record(), 1..20),
        base in 0u64..(1 << 60),
    ) {
        let encoded = batch::encode(&records).unwrap();
        let decoded = batch::decode(&encoded, base).unwrap();
        prop_assert_eq!(decoded.len(), records.len());
        for (i, (got, want)) in decoded.iter().zip(&records).enumerate() {
            prop_assert_eq!(got.offset, base + i as u64);
            prop_assert_eq!(&got.record, want);
        }
        let batches: Vec<_> = batch::batches(&encoded).collect::<Result<_, _>>().unwrap();
        prop_assert_eq!(batches.len(), 1);
        prop_assert_eq!(batches[0].record_count as usize, records.len());
        let max = records.iter().map(|r| r.timestamp_ms).max().unwrap();
        prop_assert_eq!(batches[0].max_timestamp_ms, max);
    }

    /// Our batches decode with `kafka-protocol` to the same records (header keys
    /// are unique here: `kafka-protocol` keeps headers in a map).
    #[test]
    fn kafka_protocol_decodes_our_batches(
        records in prop::collection::vec(record(), 1..20),
        base in 0u64..(1 << 60),
    ) {
        let records: Vec<Record> = records
            .into_iter()
            .map(|mut r| {
                let mut seen = std::collections::BTreeSet::new();
                r.headers.retain(|(k, _)| seen.insert(k.clone()));
                r
            })
            .collect();
        let mut encoded = batch::encode(&records).unwrap().to_vec();
        batch::patch_base_offset(&mut encoded, base);
        let mut buf = Bytes::from(encoded);
        let set = RecordBatchDecoder::decode(&mut buf).unwrap();
        prop_assert_eq!(set.records.len(), records.len());
        for (i, (got, want)) in set.records.iter().zip(&records).enumerate() {
            prop_assert_eq!(got.offset, (base + i as u64) as i64);
            prop_assert_eq!(got.timestamp, want.timestamp_ms);
            prop_assert_eq!(got.timestamp_type, TimestampType::Creation);
            prop_assert_eq!(&got.key, &want.key);
            prop_assert_eq!(&got.value, &want.value);
            let headers: Vec<(String, Option<Bytes>)> = got
                .headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            prop_assert_eq!(&headers, &want.headers);
        }
    }
}

#[test]
fn we_decode_batches_written_by_kafka_protocol() {
    let ours = sample(5);
    let theirs: Vec<kafka_protocol::records::Record> = ours
        .iter()
        .enumerate()
        .map(|(i, r)| kafka_protocol::records::Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 100 + i as i64,
            sequence: i as i32,
            timestamp: r.timestamp_ms,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| (StrBytes::from_string(k.clone()), v.clone()))
                .collect::<IndexMap<_, _>>(),
        })
        .collect();
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut buf, &theirs, &options).unwrap();
    assert_eq!(batch::base_offset(&buf), Some(100));
    let decoded = batch::decode(&buf, 100).unwrap();
    let records: Vec<Record> = decoded.iter().map(|r| r.record.clone()).collect();
    assert_eq!(records, ours);
    assert_eq!(decoded[4].offset, 104);
}

#[test]
fn patching_the_base_offset_keeps_the_batch_valid() {
    let records = sample(4);
    let mut encoded = batch::encode(&records).unwrap().to_vec();
    assert_eq!(batch::base_offset(&encoded), Some(0));
    batch::patch_base_offset(&mut encoded, 1234);
    assert_eq!(batch::base_offset(&encoded), Some(1234));
    let decoded = batch::decode(&encoded, 1234).unwrap();
    assert_eq!(decoded.first().unwrap().offset, 1234);
    assert_eq!(decoded.last().unwrap().offset, 1237);
}

#[test]
fn concatenated_batches_split_and_get_contiguous_offsets() {
    let a = batch::encode(&sample(3)).unwrap();
    let b = batch::encode(&sample(2)).unwrap();
    let joined = [a.as_ref(), b.as_ref()].concat();
    let refs: Vec<_> = batch::batches(&joined).collect::<Result<_, _>>().unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0].bytes, a.as_ref());
    assert_eq!(refs[1].bytes, b.as_ref());
    let decoded = batch::decode(&joined, 10).unwrap();
    let offsets: Vec<u64> = decoded.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, [10, 11, 12, 13, 14]);
}

#[test]
fn every_single_byte_corruption_is_detected_or_harmless() {
    let records = sample(3);
    let encoded = batch::encode(&records).unwrap();
    let original = batch::decode(&encoded, 0).unwrap();
    for at in 0..encoded.len() {
        let mut corrupted = encoded.to_vec();
        corrupted[at] ^= 0x5a;
        match batch::decode(&corrupted, 0) {
            Err(LogError::Corrupt(_)) => {}
            // `baseOffset` and `partitionLeaderEpoch` are outside the CRC and
            // not used by decoding.
            Ok(decoded) if at < 8 || (12..16).contains(&at) => assert_eq!(decoded, original),
            other => panic!("corruption at byte {at} gave {other:?}"),
        }
    }
}

#[test]
fn truncated_batches_are_rejected() {
    let encoded = batch::encode(&sample(3)).unwrap();
    for len in 1..encoded.len() {
        let err = batch::decode(&encoded[..len], 0).unwrap_err();
        assert!(matches!(err, LogError::Corrupt(_)), "len {len}: {err:?}");
    }
    assert!(batch::decode(&[], 0).unwrap().is_empty());
}

#[test]
fn an_empty_batch_is_an_invalid_argument() {
    let err = batch::encode(&[]).unwrap_err();
    assert!(matches!(err, LogError::InvalidArgument(_)), "{err:?}");
}

proptest! {
    /// Arbitrary bytes never make the decoder panic.
    #[test]
    fn garbage_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..200), flip in any::<prop::sample::Index>()) {
        let _ = batch::decode(&bytes, 0);
        // A valid header with a garbage body exercises the record decoder.
        let mut encoded = batch::encode(&sample(2)).unwrap().to_vec();
        let at = flip.index(encoded.len());
        encoded[at] = encoded[at].wrapping_add(1);
        let _ = batch::decode(&encoded, 0);
    }
}
