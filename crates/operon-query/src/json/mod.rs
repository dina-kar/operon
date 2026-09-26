//! Serde adapters for M1.1 types whose derived serde is a wire form or that
//! have none, and the hand-written JSON forms of the IR (plan M1.2 Task 1
//! rules 1, 2, 5 and 8).
//!
//! Every IR and service field holding a `PrimaryKey`, `CollectionSchema`,
//! `Distance`, `AliasAction` or `ConsistencyToken` goes through these, never
//! the derive (plan rows 0.2, 0.9, 0.14 and 0.21).

pub mod hybrid;
pub mod pk;
pub mod schema;
pub mod token;
pub mod values;

use operon_common::meta::AliasAction;
use serde_json::Value;

use crate::error::ServiceError;

/// `[{"create": {"alias", "collection"}} | {"delete": {"alias"}}, …]`, the
/// native form of alias actions.
pub fn alias_actions_from_json(v: &Value) -> Result<Vec<AliasAction>, ServiceError> {
    let invalid = |message: String| ServiceError::InvalidArgument(message);
    let actions = v
        .as_array()
        .ok_or_else(|| invalid("alias actions must be a list".to_string()))?;
    actions
        .iter()
        .map(|action| {
            let object = action
                .as_object()
                .filter(|object| object.len() == 1)
                .ok_or_else(|| {
                    invalid(format!(
                        "an alias action must be {{\"create\": …}} or {{\"delete\": …}}, got {action}"
                    ))
                })?;
            let (kind, body) = object.iter().next().expect("one key");
            let body = body
                .as_object()
                .ok_or_else(|| invalid(format!("{kind} must be an object")))?;
            let text = |key: &str| {
                body.get(key)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| invalid(format!("{kind}.{key} must be a string")))
            };
            let allowed: &[&str] = match kind.as_str() {
                "create" => &["alias", "collection"],
                "delete" => &["alias"],
                other => return Err(invalid(format!("unknown alias action {other}"))),
            };
            if let Some(key) = body.keys().find(|key| !allowed.contains(&key.as_str())) {
                return Err(invalid(format!("unknown key {key} in {kind}")));
            }
            Ok(match kind.as_str() {
                "create" => AliasAction::Create {
                    alias: text("alias")?,
                    collection: text("collection")?,
                },
                _ => AliasAction::Delete {
                    alias: text("alias")?,
                },
            })
        })
        .collect()
}
