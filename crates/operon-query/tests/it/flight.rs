//! Flight SQL without a network (plan M1.2 Task 12): statement tickets and
//! the declared SQL info.

use arrow_flight::sql::{CommandGetSqlInfo, SqlInfo, SqlSupportedTransaction};
use datafusion::arrow::array::{
    Array, BooleanArray, Int32Array, StringArray, UInt32Array, UnionArray,
};
use operon_query::ServiceError;
use operon_query::flight::{
    StatementTicket, TICKET_MAGIC, TICKET_VERSION, decode_ticket, encode_ticket, sql_info_data,
};
use proptest::prelude::*;

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
        flip in any::<prop::sample::Index>(),
        bit in 0u8..8,
    ) {
        let ticket = StatementTicket { namespace, query, token };
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
