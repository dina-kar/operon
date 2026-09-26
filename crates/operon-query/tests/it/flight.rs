//! Flight SQL without a network (plan M1.2 Task 12): statement tickets,
//! the declared SQL info, and the pinned reads of request metadata (Task 14
//! rule 6).

use arrow_flight::sql::{CommandGetSqlInfo, SqlInfo, SqlSupportedTransaction};
use datafusion::arrow::array::{
    Array, BooleanArray, Int32Array, StringArray, UInt32Array, UnionArray,
};
use operon_collection::ConsistencyToken;
use operon_common::StreamId;
use operon_query::flight::{
    PutTasks, StatementTicket, TICKET_MAGIC, TICKET_VERSION, decode_ticket, encode_ticket,
    metadata_consistency, pace_accept_errors, sql_info_data, ticket_consistency,
};
use operon_query::{PIN_MANIFEST_METADATA, ReadConsistency, ServiceError};
use proptest::prelude::*;
use tonic::metadata::MetadataMap;

fn invalid(bytes: &[u8]) {
    match decode_ticket(bytes) {
        Err(ServiceError::InvalidArgument(message)) => {
            assert_eq!(message, "invalid statement ticket")
        }
        other => panic!("expected an invalid ticket, got {other:?}"),
    }
}

/// Recomputes the trailing checksum of a ticket whose body was edited.
fn reseal(mut body: Vec<u8>) -> Vec<u8> {
    let crc = crc32c::crc32c(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn tickets_round_trip_and_reject_corruption(
        namespace in "[a-z0-9_-]{0,24}",
        query in ".{0,200}",
        token in proptest::option::of("v1:[0-9a-z:.,]{0,40}"),
        pinned_manifest in proptest::option::of(any::<u64>()),
        flip in any::<prop::sample::Index>(),
        bit in 0u8..8,
    ) {
        let ticket = StatementTicket {
            namespace,
            query,
            token,
            pinned_manifest,
        };
        let bytes = encode_ticket(&ticket);
        prop_assert_eq!(&bytes[..4], TICKET_MAGIC);
        prop_assert_eq!(decode_ticket(&bytes).expect("decodes"), ticket);
        // Any flipped bit is caught by the magic, the version or the crc.
        let mut flipped = bytes.to_vec();
        let at = flip.index(flipped.len());
        flipped[at] ^= 1 << bit;
        prop_assert!(matches!(
            decode_ticket(&flipped),
            Err(ServiceError::InvalidArgument(_))
        ));
    }
}

#[test]
fn tickets_reject_versions_trailing_bytes_and_truncation() {
    let ticket = StatementTicket {
        namespace: "w".to_string(),
        query: "SELECT 1".to_string(),
        token: None,
        pinned_manifest: None,
    };
    let bytes = encode_ticket(&ticket).to_vec();
    let body = bytes[..bytes.len() - 4].to_vec();
    // A wrong version with a valid checksum.
    let mut other = body.clone();
    other[4..6].copy_from_slice(&(TICKET_VERSION + 1).to_le_bytes());
    invalid(&reseal(other));
    // A wrong magic with a valid checksum.
    let mut magic = body.clone();
    magic[0] = b'X';
    invalid(&reseal(magic));
    // Trailing bytes after the ticket, with a valid checksum.
    let mut trailing = body.clone();
    trailing.push(0);
    invalid(&reseal(trailing));
    // Truncated.
    invalid(&bytes[..bytes.len() - 1]);
    invalid(&bytes[..5]);
    invalid(&[]);
}

#[test]
fn sql_info_declares_read_only() {
    let data = sql_info_data();
    let batch = CommandGetSqlInfo {
        info: vec![
            SqlInfo::FlightSqlServerName as u32,
            SqlInfo::FlightSqlServerVersion as u32,
            SqlInfo::FlightSqlServerArrowVersion as u32,
            SqlInfo::FlightSqlServerReadOnly as u32,
            SqlInfo::FlightSqlServerTransaction as u32,
        ],
    }
    .into_builder(&data)
    .build()
    .expect("batch");
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("info names");
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("values");
    let value_of = |info: SqlInfo| {
        let row = (0..names.len())
            .find(|&row| names.value(row) == info as u32)
            .unwrap_or_else(|| panic!("{info:?} is declared"));
        values.value(row)
    };
    let string = |info: SqlInfo| {
        value_of(info)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a string")
            .value(0)
            .to_string()
    };
    assert_eq!(string(SqlInfo::FlightSqlServerName), "operon");
    assert_eq!(
        string(SqlInfo::FlightSqlServerVersion),
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(string(SqlInfo::FlightSqlServerArrowVersion), "58.4");
    let read_only = value_of(SqlInfo::FlightSqlServerReadOnly);
    assert!(
        read_only
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("a bool")
            .value(0)
    );
    let transaction = value_of(SqlInfo::FlightSqlServerTransaction);
    assert_eq!(
        transaction
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("an int")
            .value(0),
        SqlSupportedTransaction::None as i32
    );
}

fn metadata(entries: &[(&'static str, &str)]) -> MetadataMap {
    let mut map = MetadataMap::new();
    for (key, value) in entries {
        map.insert(*key, value.parse().expect("ASCII metadata"));
    }
    map
}

#[test]
fn pin_metadata_selects_a_pinned_read() {
    const TOKEN: &str = "operon-consistency-token";
    let token = ConsistencyToken(vec![(StreamId(9), 0, 4), (StreamId(9), 1, 2)]);
    let text = token.to_string();
    let message = format!(
        "{PIN_MANIFEST_METADATA} needs a canonical manifest version and operon-consistency-token"
    );
    let refused = |entries: &[(&'static str, &str)]| match metadata_consistency(&metadata(entries))
    {
        Err(ServiceError::InvalidArgument(got)) => assert_eq!(got, message, "{entries:?}"),
        other => panic!("{entries:?}: expected InvalidArgument, got {other:?}"),
    };

    // Both keys: a pinned read.
    let pinned = ReadConsistency::Pinned {
        manifest_version: 3,
        token: token.clone(),
    };
    let both = metadata(&[(PIN_MANIFEST_METADATA, "3"), (TOKEN, &text)]);
    assert_eq!(metadata_consistency(&both).expect("pinned"), pinned);
    assert_eq!(
        metadata_consistency(&metadata(&[(PIN_MANIFEST_METADATA, "0"), (TOKEN, &text)]))
            .expect("pinned at 0"),
        ReadConsistency::Pinned {
            manifest_version: 0,
            token: token.clone(),
        }
    );
    // The token alone reads at least it; nothing reads strong (rule 2).
    assert_eq!(
        metadata_consistency(&metadata(&[(TOKEN, &text)])).expect("at least"),
        ReadConsistency::AtLeast(token.clone())
    );
    assert_eq!(
        metadata_consistency(&MetadataMap::new()).expect("strong"),
        ReadConsistency::Strong
    );

    // The manifest key alone, a non-canonical version, or a token that does
    // not parse: refused.
    refused(&[(PIN_MANIFEST_METADATA, "3")]);
    refused(&[(PIN_MANIFEST_METADATA, "01"), (TOKEN, &text)]);
    refused(&[(PIN_MANIFEST_METADATA, "+3"), (TOKEN, &text)]);
    refused(&[(PIN_MANIFEST_METADATA, ""), (TOKEN, &text)]);
    refused(&[
        (PIN_MANIFEST_METADATA, "18446744073709551616"),
        (TOKEN, &text),
    ]);
    refused(&[(PIN_MANIFEST_METADATA, "3"), (TOKEN, "garbage")]);

    // The ticket carries the pin, so DoGet reads the same state.
    let ticket = StatementTicket {
        namespace: "w".to_string(),
        query: "SELECT 1".to_string(),
        token: Some(text.clone()),
        pinned_manifest: Some(3),
    };
    let decoded = decode_ticket(&encode_ticket(&ticket)).expect("decodes");
    assert_eq!(ticket_consistency(&decoded).expect("pinned"), pinned);
    let unpinned = StatementTicket {
        token: None,
        ..ticket
    };
    assert!(matches!(
        ticket_consistency(&unpinned),
        Err(ServiceError::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn failed_accepts_are_paced() {
    use futures::StreamExt;
    let accepts = vec![
        Err(std::io::Error::other("EMFILE")),
        Err(std::io::Error::other("EMFILE")),
        Ok(7),
        Err(std::io::Error::other("EMFILE")),
    ];
    let pause = std::time::Duration::from_millis(50);
    let started = std::time::Instant::now();
    let paced: Vec<std::io::Result<i32>> =
        pace_accept_errors(futures::stream::iter(accepts), pause)
            .collect()
            .await;
    // Every accept is handed on, in order, each failure after a pause.
    assert_eq!(paced.len(), 4);
    assert_eq!(paced[2].as_ref().ok(), Some(&7));
    assert!(paced.iter().filter(|a| a.is_err()).count() == 3);
    assert!(started.elapsed() >= pause * 3, "{:?}", started.elapsed());
}

/// A put stuck inside a write is dropped by `abort`, so a shutdown that
/// gave up waiting stops it before the writer closes (review of #29).
#[tokio::test]
async fn aborted_puts_are_dropped_and_answer_unavailable() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let puts = PutTasks::new();
    let finished = puts.spawn(async { Ok::<u64, tonic::Status>(3) });
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Dropped(dropped.clone());
    let stuck = puts.spawn(async move {
        let _guard = guard;
        std::future::pending::<Result<u64, tonic::Status>>().await
    });
    assert_eq!(finished.await.expect("join").expect("put"), 3);

    puts.close();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), puts.wait())
            .await
            .is_err(),
        "a stuck put keeps wait pending"
    );
    puts.abort();
    tokio::time::timeout(Duration::from_secs(5), puts.wait())
        .await
        .expect("wait returns once the stuck put is aborted");
    assert!(dropped.load(Ordering::SeqCst), "the stuck put was dropped");
    let status = stuck.await.expect("join").expect_err("aborted");
    assert_eq!(status.code(), tonic::Code::Unavailable);
}
