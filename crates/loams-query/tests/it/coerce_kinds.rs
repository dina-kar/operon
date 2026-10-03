//! Bound-coercion panic messages carry a variant tag, never a payload
//! (issue #298, CodeQL `rust/cleartext-logging`).
//!
//! `compile` panics when a bound's coerced kind disagrees with the field
//! class that consumes it. The bound arrives from a caller, so
//! `Coerced::Str` can hold that caller's own text, and a UUID bound is
//! rendered as its canonical 32-hex-digit form by `format_uuid`. Formatting
//! the value with `{:?}` would copy that text into the process log when the
//! panic fires. These tests pin the invariant that the sink can only ever
//! emit a variant name.
//!
//! The two arms that reach those panics are statically unreachable — see
//! `source_arms_report_a_kind_and_never_a_value` — so no runtime test can
//! fire them. The value-carrying tests below therefore pin the helper the
//! arms call, and the source test pins the arms themselves.

use loams_collection::{FieldKind, PrimaryKey};
use loams_query::FieldValue;
use loams_query::text::{Coerced, coerce_term};

/// A UUID that is also recognisable as a credential, so a leak of the value
/// in any form is caught below.
const SECRET: &str = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";

/// The panic message `compile` writes for a date bound that is not
/// `DateMs` (the `Class::Date` arm of `text::compile`).
fn date_bound_message(bound: &Coerced) -> String {
    bound.mismatch("a date bound is DateMs")
}

/// The panic message `compile` writes for a number bound that is not a
/// number (the `Class::Number` arm of `text::compile`).
fn number_bound_message(bound: &Coerced) -> String {
    bound.mismatch("a numeric bound")
}

/// Both sinks, for one bound.
fn messages(bound: &Coerced) -> [String; 2] {
    [date_bound_message(bound), number_bound_message(bound)]
}

#[test]
fn kind_reports_the_variant_name() {
    assert_eq!(Coerced::Str(String::from("x")).kind(), "Str");
    assert_eq!(Coerced::I64(1).kind(), "I64");
    assert_eq!(Coerced::F64(1.0).kind(), "F64");
    assert_eq!(Coerced::Bool(true).kind(), "Bool");
    assert_eq!(Coerced::DateMs(0).kind(), "DateMs");
    assert_eq!(Coerced::Never.kind(), "Never");
}

#[test]
fn a_uuid_bound_is_reduced_to_its_tag() {
    // The exact production shape of a UUID-bearing `Coerced::Str`: a UUID
    // field coerces through `parse_uuid`, which canonicalises the value.
    let bound = coerce_term(&FieldKind::Uuid, "id", &FieldValue::Str(SECRET.to_string()))
        .expect("a well-formed UUID coerces");
    assert_eq!(bound, Coerced::Str(SECRET.to_string()));

    // Equality against the whole expected string, so neither the UUID nor
    // any part of it can be present without failing.
    assert_eq!(
        messages(&bound),
        [
            "a date bound is DateMs, not Str".to_string(),
            "a numeric bound, not Str".to_string(),
        ]
    );
}

#[test]
fn arbitrary_bound_text_is_reduced_to_its_tag() {
    // `Str` carries any caller text, not only UUIDs: passwords, DSNs,
    // bearer tokens.
    for secret in [
        "hunter2",
        "postgres://loams:s3cr3t@db.internal:5432/loams",
        "Bearer mock-access-token",
        "0123456789abcdef0123456789abcdef",
    ] {
        let messages = messages(&Coerced::Str(secret.to_string()));
        assert_eq!(messages[0], "a date bound is DateMs, not Str");
        assert_eq!(messages[1], "a numeric bound, not Str");
    }
}

#[test]
fn every_coercion_keeps_its_payload_out_of_the_tag() {
    // Sweep the values `coerce_term` can return for the field kinds whose
    // coercion is `Str`, the payload-bearing variant CodeQL traced.
    let values = [
        FieldValue::Str(SECRET.to_string()),
        FieldValue::Str("hunter2".to_string()),
        FieldValue::I64(7),
        FieldValue::U64(7),
        FieldValue::F64(1.5),
        FieldValue::Bool(true),
        FieldValue::Date(1_700_000_000_000_000),
    ];
    let kinds = [
        FieldKind::Uuid,
        FieldKind::Text {
            analyzer: "raw".to_string(),
            positions: false,
        },
        FieldKind::Keyword,
    ];
    for kind in kinds {
        for value in &values {
            let Ok(coerced) = coerce_term(&kind, "f", value) else {
                continue;
            };
            let tag = coerced.kind();
            // A tag is a bare identifier, so it can hold no value at all.
            assert!(
                tag.chars().all(|c| c.is_ascii_alphanumeric()),
                "{kind:?}: a tag is a bare identifier, got {tag:?}"
            );
            // The payload a `{:?}` sink would have written. `Bool` and
            // `Never` carry none, so they have nothing to check.
            let payload = match &coerced {
                Coerced::Str(text) => text.clone(),
                Coerced::I64(n) | Coerced::DateMs(n) => n.to_string(),
                Coerced::F64(x) => x.to_string(),
                Coerced::Bool(_) | Coerced::Never => continue,
            };
            for message in messages(&coerced) {
                assert!(
                    !payload.is_empty() && !message.contains(&payload),
                    "{kind:?}: the payload must not reach the panic message {message:?}"
                );
            }
        }
    }
}

#[test]
fn a_primary_key_uuid_renders_the_same_tag() {
    // The other `parse_uuid` consumer CodeQL reaches is `PrimaryKey::Uuid`,
    // named as `...::Uuid(...)` on the test-side sinks. Its `Debug` carries
    // the 16 bytes, so it must never be the thing a panic message formats.
    let mut bytes = [0u8; 16];
    let hex: String = SECRET.chars().filter(|c| *c != '-').collect();
    for (i, part) in hex.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(part).expect("ascii hex");
        bytes[i] = u8::from_str_radix(pair, 16).expect("a hex byte");
    }
    let uuid = PrimaryKey::Uuid(bytes);

    let bound = coerce_term(&FieldKind::Uuid, "id", &FieldValue::Str(SECRET.to_string()))
        .expect("a well-formed UUID coerces");
    assert_eq!(
        messages(&bound),
        [
            "a date bound is DateMs, not Str".to_string(),
            "a numeric bound, not Str".to_string(),
        ]
    );

    // The bytes are what the old `{other:?}` sink would have written; the
    // tag carries none of them.
    let debug = format!("{uuid:?}");
    assert!(debug.contains('[') && debug.contains(']'), "{debug}");
    assert!(!messages(&Coerced::Str(SECRET.to_string()))[0].contains(&debug));
}

/// Both arms that panic on a bound-kind mismatch must report the kind, not
/// the value.
///
/// `Class::Date` builds its bound with `coerce_bound(&FieldKind::Date, ..)`,
/// which yields only `DateMs` or `Never`; `Never` is dropped by the
/// `is_never` guard above. `Class::Number` uses `I64` or `F64`, again only
/// `I64`/`F64`/`Never`. So `Coerced::Str` — the one variant that carries a
/// payload, and the one `parse_uuid` feeds — cannot arrive at either arm.
/// CodeQL over-approximates `coerce_bound` and cannot see that, and the
/// arms are unreachable at runtime, so this reads the source instead: it
/// fails if either arm is reverted to formatting the value.
#[test]
fn source_arms_report_a_kind_and_never_a_value() {
    let source = include_str!("../../src/text/compile.rs");

    // Any arm that binds the unexpected bound and then panics on it, by any
    // macro, has to report the kind.
    let arm = source
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("other =>"))
        .filter(|line| {
            ["panic!(", "unreachable!(", "todo!("]
                .iter()
                .any(|mac| line.contains(mac))
        })
        .collect::<Vec<_>>();

    assert_eq!(arm.len(), 2, "both bound-kind mismatch arms are covered");
    for line in arm {
        assert!(
            line.contains(".mismatch("),
            "a mismatch arm formats the bound instead of its kind: {line}"
        );
        assert!(
            !line.contains("other:?") && !line.contains("{other}"),
            "a mismatch arm formats the bound's value: {line}"
        );
    }
}
