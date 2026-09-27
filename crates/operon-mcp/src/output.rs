//! The output cap of every tool result (plan M1.6 Ruling 13, Task 7 rule
//! 12).

use rmcp::model::CallToolResult;
use serde_json::Value;

/// The JSON of the tool result that `structured` becomes: rmcp writes it
/// twice, as the text content and as `structuredContent`, so this is the
/// size Ruling 13 caps (PR #98 review).
pub fn tool_result_json(structured: Value) -> Value {
    serde_json::to_value(CallToolResult::structured(structured)).unwrap_or(Value::Null)
}

/// Keeps the longest prefix of `items` whose `render(prefix)` serializes to
/// at most `max_bytes`; returns whether it dropped any. A binary search over
/// the prefix length: O(log len) serializations.
pub fn cap_items<T>(
    items: &mut Vec<T>,
    max_bytes: usize,
    render: impl Fn(&[T]) -> serde_json::Value,
) -> bool {
    let fits = |n: usize| render(&items[..n]).to_string().len() <= max_bytes;
    if items.is_empty() || fits(items.len()) {
        return false;
    }
    // Invariant: `lo` fits (or is 0) and `hi` does not.
    let (mut lo, mut hi) = (0, items.len());
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    items.truncate(lo);
    true
}
