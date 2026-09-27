//! The flat MCP filter (plan M1.6 Task 8 rule 2, Ruling 8): an object of
//! field conditions that must all hold, instead of the query IR.

use operon_query::{FieldValue, Query};
use serde_json::{Map, Value};

use crate::error::ToolError;

const BOUNDS: [&str; 4] = ["gt", "gte", "lt", "lte"];

fn refused(field: &str) -> ToolError {
    ToolError::invalid(format!(
        "filter on `{field}`: expected a value, a list of values, {{gt, gte, lt, lte}} or {{exists}}"
    ))
}

/// The flat MCP filter: every entry must hold (Ruling 8). Entries are read
/// in key order (sorted here: the workspace's `serde_json` keeps insertion
/// order); no entries is no filter.
pub fn translate_filter(filter: &Map<String, Value>) -> Result<Option<Query>, ToolError> {
    let mut entries: Vec<(&String, &Value)> = filter.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut clauses = entries
        .into_iter()
        .map(|(field, value)| clause(field, value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(match clauses.len() {
        0 => None,
        1 => clauses.pop(),
        _ => Some(Query::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: clauses,
            minimum_should_match: None,
        }),
    })
}

fn clause(field: &str, value: &Value) -> Result<Query, ToolError> {
    let scalar = |v: &Value| field_value(v).map_err(|_| refused(field));
    let field_name = field.to_string();
    match value {
        Value::Null => Ok(Query::IsNull { field: field_name }),
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(Query::Term {
            field: field_name,
            value: scalar(value)?,
        }),
        Value::Array(values) if !values.is_empty() => Ok(Query::Terms {
            field: field_name,
            values: values.iter().map(scalar).collect::<Result<_, _>>()?,
        }),
        Value::Object(object) if object.len() == 1 && object.contains_key("exists") => {
            match object["exists"] {
                Value::Bool(true) => Ok(Query::Exists { field: field_name }),
                Value::Bool(false) => Ok(Query::Bool {
                    must: Vec::new(),
                    should: Vec::new(),
                    must_not: vec![Query::Exists { field: field_name }],
                    filter: Vec::new(),
                    minimum_should_match: None,
                }),
                _ => Err(refused(field)),
            }
        }
        Value::Object(object)
            if !object.is_empty() && object.keys().all(|k| BOUNDS.contains(&k.as_str())) =>
        {
            let bound = |key: &str| object.get(key).map(scalar).transpose();
            Ok(Query::Range {
                field: field_name,
                gt: bound("gt")?,
                gte: bound("gte")?,
                lt: bound("lt")?,
                lte: bound("lte")?,
            })
        }
        _ => Err(refused(field)),
    }
}

/// A scalar JSON value as a query value: a string is `Str`, an integer is
/// `I64` (`U64` above `i64::MAX`, row E18), another number `F64`, a bool
/// `Bool`.
pub fn field_value(value: &Value) -> Result<FieldValue, ToolError> {
    match value {
        Value::String(s) => Ok(FieldValue::Str(s.clone())),
        Value::Bool(b) => Ok(FieldValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(FieldValue::I64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(FieldValue::U64(u))
            } else {
                n.as_f64()
                    .filter(|f| f.is_finite())
                    .map(FieldValue::F64)
                    .ok_or_else(|| ToolError::invalid(format!("{n} is not a finite number")))
            }
        }
        _ => Err(ToolError::invalid(format!(
            "expected a string, number or bool, got {value}"
        ))),
    }
}
