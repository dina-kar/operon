//! Task 5: `_bulk` NDJSON parsing.

use loams_es::bulk::{BulkAction, parse_ndjson};
use serde_json::json;

#[test]
fn a_body_without_a_final_newline_is_refused() {
    let e = parse_ndjson(b"{\"delete\": {\"_id\": \"1\"}}").expect_err("no newline");
    assert_eq!(e.status, 400);
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "The bulk request must be terminated by a newline [\\n]"
    );
}

#[test]
fn crlf_and_blank_lines_are_tolerated() {
    let body =
        b"\r\n{\"index\": {\"_id\": \"1\"}}\r\n{\"a\": 1}\r\n\n\n{\"delete\": {\"_id\": \"2\"}}\n";
    let lines = parse_ndjson(body).expect("parse");
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].action, BulkAction::Index);
    assert_eq!(lines[0].id.as_deref(), Some("1"));
    assert_eq!(lines[0].source, Some(Ok(json!({"a": 1}))));
    assert_eq!(lines[0].line_no, 2);
    assert_eq!(lines[1].action, BulkAction::Delete);
    assert_eq!(lines[1].source, None);
}

#[test]
fn an_unknown_action_is_malformed() {
    let e = parse_ndjson(b"{\"upsert\": {\"_id\": \"1\"}}\n{}\n").expect_err("upsert");
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "Malformed action/metadata line [1], expected field [create], [delete], [index] or \
         [update] but found [upsert]"
    );
    let e = parse_ndjson(b"[1]\n").expect_err("array");
    assert_eq!(
        e.reason,
        "Malformed action/metadata line [1], expected START_OBJECT but found [START_ARRAY]"
    );
}

#[test]
fn delete_takes_no_source_line() {
    let body = b"{\"delete\": {\"_index\": \"i\", \"_id\": \"1\"}}\n{\"index\": {\"_index\": \"i\"}}\n{\"a\": 1}\n";
    let lines = parse_ndjson(body).expect("parse");
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].index.as_deref(), Some("i"));
    assert_eq!(lines[1].action, BulkAction::Index);
    assert_eq!(lines[1].id, None);
}

#[test]
fn an_invalid_source_fails_only_its_item() {
    let body =
        b"{\"index\": {\"_id\": \"1\"}}\n{\"a\": \n{\"index\": {\"_id\": \"2\"}}\n{\"a\": 2}\n";
    let lines = parse_ndjson(body).expect("parse");
    assert_eq!(lines.len(), 2);
    let error = lines[0].source.clone().expect("line").expect_err("invalid");
    assert!(error.contains("failed to parse"), "{error}");
    assert_eq!(lines[1].source, Some(Ok(json!({"a": 2}))));
}

#[test]
fn an_unknown_metadata_key_fails_the_request() {
    let e = parse_ndjson(b"{\"index\": {\"_id\": \"1\", \"colour\": 1}}\n{}\n").expect_err("key");
    assert_eq!(e.kind, "illegal_argument_exception");
    assert_eq!(
        e.reason,
        "Action/metadata line [1] contains an unknown parameter [colour]"
    );
}

#[test]
fn op_type_create_and_occ_keys_are_read() {
    let body = b"{\"index\": {\"_id\": \"1\", \"op_type\": \"create\"}}\n{}\n{\"delete\": {\"_id\": \"2\", \"if_seq_no\": 3}}\n";
    let lines = parse_ndjson(body).expect("parse");
    assert_eq!(lines[0].action, BulkAction::Create);
    assert!(!lines[0].occ);
    assert!(lines[1].occ);
}

#[test]
fn a_missing_source_line_fails_the_request() {
    let e = parse_ndjson(b"{\"index\": {\"_id\": \"1\"}}\n").expect_err("missing");
    assert_eq!(e.kind, "action_request_validation_exception");
    assert_eq!(e.reason, "Validation Failed: 1: source is missing;");
}
