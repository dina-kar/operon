//! The `ErrorInfo.reason` registry, read from `docs/api/reasons.md`.
//!
//! Design §44 §7.4, D611: every failed RPC carries one
//! `loams.errors.v1.ErrorInfo` whose `reason` is a stable `snake_case` string,
//! and the page is the single registry of the reasons that exist. It is a
//! Markdown table, not a descriptor, so it reaches a client through its
//! generated SDK rather than through `buf`.
//!
//! An SDK is generated from this page for one reason: `reason` is what callers
//! branch on, so it has to be a *type* in the SDK, not a string they compare
//! against. Generating the union from the registry means a reason the server
//! can return is a reason the compiler knows, and a reason the page has lost
//! is a compile error rather than a runtime surprise.
//!
//! The parser reads the same table shape the Rust test
//! `reasons_are_snake_case_and_unique` (`crates/loams/tests/connect_api.rs`)
//! validates, so the two cannot drift on what a row is.

use std::collections::BTreeSet;
use std::path::Path;

/// One row of the registry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Reason {
    /// The stable cause, `snake_case`: the thing callers branch on.
    pub reason: String,
    /// The Connect code the reason is raised under, for example `not_found`.
    pub code: String,
    /// Which RPCs raise it, free text from the page.
    pub raised_by: String,
    /// The metadata keys the server promises alongside it, empty when the row
    /// promises none.
    pub metadata: Vec<String>,
}

/// Reads the registry page.
///
/// # Errors
///
/// Returns the page's path and what went wrong when it cannot be read, when a
/// row is not the `| reason | code | raised by | metadata |` shape, or when two
/// rows claim the same reason. The last of those is the page's own rule: a
/// reason may be added but never renamed or removed within a major version, so
/// a duplicate means the page was edited by hand and must not be generated
/// from.
pub fn parse(text: &str, path: &Path) -> Result<Vec<Reason>, String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let Some(reason) = row_reason(line) else {
            continue;
        };
        let line_number = number + 1;
        if !reason
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            || reason.is_empty()
        {
            return Err(format!(
                "{}:{line_number}: {reason:?} is not lower_snake_case",
                path.display()
            ));
        }
        if !seen.insert(reason.clone()) {
            return Err(format!(
                "{}:{line_number}: {reason:?} is registered twice",
                path.display()
            ));
        }
        let cells = cells(line);
        let code = cells
            .get(1)
            .map(|cell| cell.trim_matches('`').to_owned())
            .unwrap_or_default();
        if code.is_empty() {
            return Err(format!(
                "{}:{line_number}: the row has no Connect code",
                path.display()
            ));
        }
        let raised_by = cells.get(2).copied().unwrap_or_default().to_owned();
        let metadata = cells
            .get(3)
            .map(|cell| {
                cell.split(',')
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(|key| key.trim_matches('`').to_owned())
                    .collect()
            })
            .unwrap_or_default();
        out.push(Reason {
            reason,
            code,
            raised_by,
            metadata,
        });
    }
    if out.is_empty() {
        return Err(format!("{}: the registry has no rows", path.display()));
    }
    Ok(out)
}

/// Reads the registry from a file.
pub fn read(path: &Path) -> Result<Vec<Reason>, String> {
    let text = std::fs::read_to_string(path).map_err(|err| format!("{}: {err}", path.display()))?;
    parse(&text, path)
}

/// The first cell of a row, if the line is a registry row at all: it starts
/// with `|`, has at least four cells, and its first cell is a `backticked`
/// name. Table headers and the prose around the table are skipped.
fn row_reason(line: &str) -> Option<String> {
    let parts = cells(line);
    if parts.len() < 4 {
        return None;
    }
    let first = parts[0].trim();
    let inner = first.strip_prefix('`')?.strip_suffix('`')?;
    if inner.is_empty() {
        return None;
    }
    Some(inner.to_owned())
}

/// A Markdown table row's cells, split on `|` and trimmed.
fn cells(line: &str) -> Vec<&str> {
    if !line.trim_start().starts_with('|') {
        return Vec::new();
    }
    line.trim()
        .trim_start_matches('|')
        .trim_end_matches('|')
        .split('|')
        .map(str::trim)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = "\
# The registry

| reason | Connect code | Raised by | Metadata |
|---|---|---|---|
| `not_found` | not_found | any RPC | |
| `feature_not_in_variant` | unimplemented | a package | `variant` |
| `resource_exhausted` | resource_exhausted | a quota | `retry_after_ms` |

Not reasons: `cursor_expired` is not an error.
";

    #[test]
    fn reads_every_row_and_ignores_the_prose() {
        let reasons = parse(PAGE, Path::new("docs/api/reasons.md")).expect("parse");
        assert_eq!(reasons.len(), 3, "{reasons:?}");
        assert_eq!(reasons[0].reason, "not_found");
        assert_eq!(reasons[0].code, "not_found");
        assert_eq!(reasons[1].metadata, vec!["variant".to_owned()]);
        assert_eq!(reasons[2].metadata, vec!["retry_after_ms".to_owned()]);
        // `cursor_expired` is in the prose, not in a row, so it is not a reason.
        assert!(
            !reasons
                .iter()
                .any(|reason| reason.reason == "cursor_expired")
        );
    }

    #[test]
    fn rejects_a_duplicate() {
        let page = "\
| reason | Connect code | Raised by | Metadata |
|---|---|---|---|
| `not_found` | not_found | a | |
| `not_found` | not_found | b | |
";
        let err = parse(page, Path::new("reasons.md")).expect_err("a duplicate must fail");
        assert!(err.contains("registered twice"), "{err}");
    }

    #[test]
    fn rejects_a_name_outside_snake_case() {
        let page = "\
| reason | Connect code | Raised by | Metadata |
|---|---|---|---|
| `NotFound` | not_found | a | |
";
        let err = parse(page, Path::new("reasons.md")).expect_err("must fail");
        assert!(err.contains("lower_snake_case"), "{err}");
    }

    #[test]
    fn rejects_an_empty_registry() {
        assert!(parse("# Nothing here\n", Path::new("reasons.md")).is_err());
    }
}
