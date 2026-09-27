//! The output cap (plan M1.6 Task 7 rule 12, Ruling 13).

use operon_mcp::output::cap_items;
use serde_json::{Value, json};

fn render(items: &[String]) -> Value {
    json!({ "items": items, "truncated": false })
}

fn size(items: &[String]) -> usize {
    render(items).to_string().len()
}

#[test]
fn cap_items_keeps_the_longest_prefix_that_fits() {
    let all: Vec<String> = (0..100).map(|i| format!("{i:0>98}")).collect();
    let mut items = all.clone();
    assert!(cap_items(&mut items, 2_000, render));
    let kept = items.len();
    assert!(kept > 0 && kept < 100);
    assert!(size(&items) <= 2_000);
    assert!(size(&all[..kept + 1]) > 2_000);
    assert_eq!(items, all[..kept]);
}

#[test]
fn cap_items_keeps_everything_that_fits() {
    let all: Vec<String> = (0..10).map(|i| i.to_string()).collect();
    let mut items = all.clone();
    assert!(!cap_items(&mut items, 2_000, render));
    assert_eq!(items, all);
    let mut empty: Vec<String> = Vec::new();
    assert!(!cap_items(&mut empty, 1, render));
}

#[test]
fn cap_items_can_drop_everything() {
    let mut items = vec!["x".repeat(100)];
    assert!(cap_items(&mut items, 10, render));
    assert!(items.is_empty());
}
