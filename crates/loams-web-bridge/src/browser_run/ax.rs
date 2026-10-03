//! Turning a CDP accessibility tree into the tool contract's nodes.
//!
//! `Accessibility.getFullAXTree` returns a flat list of nodes with `childIds`;
//! this walks it into trees, prunes what the browser marks ignored, carries
//! each node's `backendDOMNodeId` (the handle an action needs) and decides
//! which values are credentials that must never be shown.

use std::collections::HashMap;

use regex::Regex;
use serde::Deserialize;
use serde_json::Value;

use crate::tool::AxNode;

/// Names that mark a credential field even when the tree does not.
fn credential_name() -> &'static Regex {
    static PATTERN: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
        Regex::new(
            r"(?i)\b(pass(word|code|phrase)?|one[\s-]?time|otp|2fa|two[\s-]?factor|verification\s+code|security\s+code|pin|cvv|cvc|card\s+number|credit\s+card|account\s+number|auth(?:entication)?\s+code)\b",
        )
        .expect("the credential-name pattern compiles")
    });
    &PATTERN
}

/// The `autocomplete` tokens that mark a credential field.
const CREDENTIAL_AUTOCOMPLETE: [&str; 6] = [
    "current-password",
    "new-password",
    "one-time-code",
    "cc-name",
    "cc-number",
    "cc-csc",
];

/// The roles that can hold a credential.
const FIELD_ROLES: [&str; 5] = ["textbox", "searchbox", "combobox", "spinbutton", "text"];

#[derive(Debug, Deserialize)]
struct AxTree {
    #[serde(default)]
    nodes: Vec<AxNodeJson>,
}

#[derive(Debug, Deserialize)]
struct AxNodeJson {
    #[serde(rename = "nodeId", default)]
    #[allow(dead_code)]
    node_id: String,
    #[serde(default)]
    ignored: bool,
    #[serde(default)]
    role: Option<AxValue>,
    #[serde(default)]
    name: Option<AxValue>,
    #[serde(default)]
    value: Option<AxValue>,
    #[serde(default)]
    properties: Vec<AxProperty>,
    #[serde(default, rename = "childIds")]
    child_ids: Vec<String>,
    #[serde(default, rename = "backendDOMNodeId")]
    backend_dom_node_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AxValue {
    #[serde(default)]
    value: Value,
}

#[derive(Debug, Deserialize)]
struct AxProperty {
    #[serde(default)]
    name: String,
    #[serde(default)]
    value: Option<AxValue>,
}

impl AxProperty {
    fn text(&self) -> String {
        self.value
            .as_ref()
            .map(|value| match &value.value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default()
    }

    fn flag(&self) -> bool {
        matches!(
            self.value.as_ref().map(|value| &value.value),
            Some(Value::Bool(true))
        )
    }
}

/// Parse `Accessibility.getFullAXTree`'s result into roots.
pub fn parse_ax_tree(result: &Value) -> Result<Vec<AxNode>, crate::error::BridgeError> {
    let tree: AxTree = serde_json::from_value(result.clone()).map_err(|error| {
        crate::error::BridgeError::cdp(format!("the accessibility tree is unreadable: {error}"))
    })?;

    let mut by_id: HashMap<&str, &AxNodeJson> = HashMap::new();
    for node in &tree.nodes {
        by_id.entry(node.node_id.as_str()).or_insert(node);
    }

    let mut child_of: HashMap<&str, &str> = HashMap::new();
    for node in &tree.nodes {
        for child in &node.child_ids {
            child_of.insert(child.as_str(), node.node_id.as_str());
        }
    }

    let roots: Vec<&AxNodeJson> = tree
        .nodes
        .iter()
        .filter(|node| !child_of.contains_key(node.node_id.as_str()))
        .collect();

    Ok(roots
        .iter()
        .filter_map(|root| convert(root, &by_id, 0))
        .collect())
}

/// The depth at which we stop walking, so a hostile or broken tree cannot make
/// the provider recurse without bound.
const MAX_DEPTH: usize = 64;

fn convert<'a>(
    node: &'a AxNodeJson,
    by_id: &HashMap<&'a str, &'a AxNodeJson>,
    depth: usize,
) -> Option<AxNode> {
    if depth > MAX_DEPTH {
        return None;
    }
    let role = node
        .role
        .as_ref()
        .map(|value| plain(&value.value))
        .unwrap_or_default();
    let name = node
        .name
        .as_ref()
        .map(|value| plain(&value.value))
        .unwrap_or_default();
    let value = node.value.as_ref().map(|value| plain(&value.value));

    let mut states: Vec<String> = Vec::new();
    let mut protected = false;
    let mut autocomplete = String::new();
    for property in &node.properties {
        match property.name.as_str() {
            "disabled" if property.flag() => states.push("disabled".to_string()),
            "focused" if property.flag() => states.push("focused".to_string()),
            "required" if property.flag() => states.push("required".to_string()),
            "readonly" if property.flag() => states.push("readonly".to_string()),
            "expanded" if property.flag() => states.push("expanded".to_string()),
            "protected" if property.flag() => {
                protected = true;
                states.push("protected".to_string());
            }
            "checked" => {
                let text = property.text();
                if text == "true" {
                    states.push("checked".to_string());
                } else if text == "false" {
                    states.push("unchecked".to_string());
                }
            }
            "level" => {
                let text = property.text();
                if !text.is_empty() {
                    states.push(format!("level={text}"));
                }
            }
            "autocomplete" => autocomplete = property.text(),
            _ => {}
        }
    }

    let sensitive = protected
        || CREDENTIAL_AUTOCOMPLETE
            .iter()
            .any(|token| autocomplete.to_lowercase().contains(token))
        || (FIELD_ROLES.contains(&role.as_str()) && credential_name().is_match(&name));

    let children = node
        .child_ids
        .iter()
        .filter_map(|child| by_id.get(child.as_str()))
        .filter_map(|child| convert(child, by_id, depth + 1))
        .collect();

    Some(AxNode {
        role,
        name,
        value: value.filter(|text| !text.is_empty()),
        states,
        sensitive,
        backend_id: node.backend_dom_node_id.unwrap_or(0),
        ignored: node.ignored,
        children,
    })
}

fn plain(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_tree_becomes_a_tree_with_backend_handles() {
        let result = json!({
            "nodes": [
                {"nodeId": "1", "role": {"value": "RootWebArea"}, "name": {"value": "Example"},
                 "childIds": ["2"], "backendDOMNodeId": 10},
                {"nodeId": "2", "role": {"value": "button"}, "name": {"value": "Save"},
                 "backendDOMNodeId": 11}
            ]
        });
        let roots = parse_ax_tree(&result).unwrap_or_else(|_| panic!("parses"));
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].backend_id, 10);
        assert_eq!(roots[0].children[0].name, "Save");
        assert_eq!(roots[0].children[0].backend_id, 11);
    }

    #[test]
    fn a_protected_password_field_is_marked_and_keeps_no_value() {
        let result = json!({
            "nodes": [
                {"nodeId": "1", "role": {"value": "textbox"}, "name": {"value": "Password"},
                 "value": {"value": "hunter2"},
                 "properties": [{"name": "protected", "value": {"value": true}}],
                 "backendDOMNodeId": 12}
            ]
        });
        let roots = parse_ax_tree(&result).unwrap_or_else(|_| panic!("parses"));
        assert!(roots[0].sensitive);
        let snapshot =
            crate::tool::Snapshot::build("s1", "https://example.com", "Example", &roots, false);
        assert!(
            !snapshot
                .render(crate::tool::DEFAULT_BUDGET_CHARS)
                .contains("hunter2")
        );
    }

    #[test]
    fn a_field_named_like_a_credential_is_marked_without_the_protected_flag() {
        let result = json!({
            "nodes": [
                {"nodeId": "1", "role": {"value": "textbox"}, "name": {"value": "One-time code"},
                 "value": {"value": "123456"}, "backendDOMNodeId": 13}
            ]
        });
        let roots = parse_ax_tree(&result).unwrap_or_else(|_| panic!("parses"));
        assert!(roots[0].sensitive);
    }
}
