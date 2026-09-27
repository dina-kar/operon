//! The argument and result shapes of the built-in functions (R1 plan
//! Task 10): documents as values, and index ranges from query arguments.
//!
//! A document is an object of its fields plus `_id` (the id's text form) and
//! `_creationTime` (an `int64`, ms since the epoch). `_system:query` takes
//!
//! ```text
//! { table: string, index?: string = "by_creation_time", eq?: [value…],
//!   lower?: { value, inclusive?: bool = true }, upper?: { value, inclusive?: bool = true },
//!   order?: "asc" | "desc", limit?: int64 }
//! ```

use std::collections::BTreeMap;
use std::ops::Bound;

use crate::catalog::{BY_CREATION_TIME, CREATION_TIME_FIELD, ID_FIELD, TableDef};
use crate::docs::{Doc, IndexRange, Order};
use crate::{DocId, LiveError, LiveValue};

/// A document as a value: its fields, `_id` and `_creationTime`.
pub fn doc_value(doc: &Doc) -> LiveValue {
    let mut fields = doc.fields.clone();
    fields.insert(ID_FIELD.to_string(), LiveValue::Str(doc.id.to_string()));
    fields.insert(
        CREATION_TIME_FIELD.to_string(),
        LiveValue::I64(i64::try_from(doc.creation_ms).unwrap_or(i64::MAX)),
    );
    LiveValue::Object(fields)
}

/// The arguments of a function as an object, refusing any key outside
/// `allowed`.
pub fn object_args(
    function: &str,
    args: LiveValue,
    allowed: &[&str],
) -> Result<BTreeMap<String, LiveValue>, LiveError> {
    let LiveValue::Object(args) = args else {
        return Err(LiveError::invalid(format!(
            "{function} takes an object of arguments, not {}",
            args.type_name()
        )));
    };
    if let Some(unknown) = args.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(LiveError::invalid(format!(
            "{function}: unknown argument '{unknown}' (expected {})",
            allowed.join(", ")
        )));
    }
    Ok(args)
}

/// A required string argument.
pub fn str_arg(
    function: &str,
    args: &mut BTreeMap<String, LiveValue>,
    name: &str,
) -> Result<String, LiveError> {
    match args.remove(name) {
        Some(LiveValue::Str(s)) => Ok(s),
        Some(other) => Err(LiveError::invalid(format!(
            "{function}: '{name}' is a string, not {}",
            other.type_name()
        ))),
        None => Err(LiveError::invalid(format!(
            "{function}: '{name}' is required"
        ))),
    }
}

/// A required document id argument (its text form).
pub fn id_arg(
    function: &str,
    args: &mut BTreeMap<String, LiveValue>,
    name: &str,
) -> Result<DocId, LiveError> {
    str_arg(function, args, name)?.parse()
}

/// A required object argument: document fields.
pub fn fields_arg(
    function: &str,
    args: &mut BTreeMap<String, LiveValue>,
    name: &str,
) -> Result<BTreeMap<String, LiveValue>, LiveError> {
    match args.remove(name) {
        Some(LiveValue::Object(fields)) => Ok(fields),
        Some(other) => Err(LiveError::invalid(format!(
            "{function}: '{name}' is an object of fields, not {}",
            other.type_name()
        ))),
        None => Err(LiveError::invalid(format!(
            "{function}: '{name}' is required"
        ))),
    }
}

/// `_system:query`'s arguments, before the table is looked up.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryArgs {
    pub table: String,
    pub index: String,
    pub eq: Vec<LiveValue>,
    pub lower: Bound<LiveValue>,
    pub upper: Bound<LiveValue>,
    pub order: Order,
    pub limit: Option<u32>,
}

/// The keys `_system:query` accepts.
pub const QUERY_ARGS: [&str; 7] = ["table", "index", "eq", "lower", "upper", "order", "limit"];

impl QueryArgs {
    /// Parses `_system:query`'s arguments.
    pub fn parse(function: &str, args: LiveValue) -> Result<Self, LiveError> {
        let mut args = object_args(function, args, &QUERY_ARGS)?;
        let table = str_arg(function, &mut args, "table")?;
        let index = match args.remove("index") {
            None => BY_CREATION_TIME.to_string(),
            Some(LiveValue::Str(s)) => s,
            Some(other) => {
                return Err(LiveError::invalid(format!(
                    "{function}: 'index' is a string, not {}",
                    other.type_name()
                )));
            }
        };
        let eq = match args.remove("eq") {
            None => Vec::new(),
            Some(LiveValue::Array(values)) => values,
            Some(other) => {
                return Err(LiveError::invalid(format!(
                    "{function}: 'eq' is an array, not {}",
                    other.type_name()
                )));
            }
        };
        let lower = bound(function, "lower", args.remove("lower"))?;
        let upper = bound(function, "upper", args.remove("upper"))?;
        let order = match args.remove("order") {
            None => Order::Asc,
            Some(LiveValue::Str(s)) if s == "asc" => Order::Asc,
            Some(LiveValue::Str(s)) if s == "desc" => Order::Desc,
            Some(other) => {
                return Err(LiveError::invalid(format!(
                    "{function}: 'order' is \"asc\" or \"desc\", not {other:?}"
                )));
            }
        };
        let limit = match args.remove("limit") {
            None => None,
            Some(LiveValue::I64(n)) => Some(u32::try_from(n).map_err(|_| {
                LiveError::invalid(format!("{function}: 'limit' is 0 to 2^32 − 1, not {n}"))
            })?),
            Some(other) => {
                return Err(LiveError::invalid(format!(
                    "{function}: 'limit' is an int64, not {}",
                    other.type_name()
                )));
            }
        };
        Ok(QueryArgs {
            table,
            index,
            eq,
            lower,
            upper,
            order,
            limit,
        })
    }

    /// The index range on `table`; an unknown index is
    /// [`LiveError::NotFound`].
    pub fn range(self, table: &TableDef) -> Result<IndexRange, LiveError> {
        let index = table.index_id(&self.index).ok_or_else(|| {
            LiveError::NotFound(format!("index '{}' of table '{}'", self.index, table.name))
        })?;
        Ok(IndexRange {
            table: table.id,
            index,
            eq: self.eq,
            lower: self.lower,
            upper: self.upper,
            order: self.order,
            limit: self.limit,
        })
    }
}

fn bound(
    function: &str,
    name: &str,
    arg: Option<LiveValue>,
) -> Result<Bound<LiveValue>, LiveError> {
    let Some(arg) = arg else {
        return Ok(Bound::Unbounded);
    };
    let mut fields = object_args(
        &format!("{function} '{name}'"),
        arg,
        &["value", "inclusive"],
    )?;
    let value = fields
        .remove("value")
        .ok_or_else(|| LiveError::invalid(format!("{function}: '{name}.value' is required")))?;
    let inclusive = match fields.remove("inclusive") {
        None => true,
        Some(LiveValue::Bool(b)) => b,
        Some(other) => {
            return Err(LiveError::invalid(format!(
                "{function}: '{name}.inclusive' is a bool, not {}",
                other.type_name()
            )));
        }
    };
    Ok(if inclusive {
        Bound::Included(value)
    } else {
        Bound::Excluded(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(pairs: Vec<(&str, LiveValue)>) -> LiveValue {
        LiveValue::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    #[test]
    fn query_args_parse_with_defaults_and_bounds() {
        let args = QueryArgs::parse(
            "_system:query",
            obj(vec![
                ("table", LiveValue::Str("messages".into())),
                (
                    "eq",
                    LiveValue::Array(vec![LiveValue::Str("general".into())]),
                ),
                (
                    "lower",
                    obj(vec![
                        ("value", LiveValue::I64(5)),
                        ("inclusive", LiveValue::Bool(false)),
                    ]),
                ),
                ("order", LiveValue::Str("desc".into())),
                ("limit", LiveValue::I64(10)),
            ]),
        )
        .expect("valid arguments");
        assert_eq!(args.table, "messages");
        assert_eq!(args.index, BY_CREATION_TIME);
        assert_eq!(args.lower, Bound::Excluded(LiveValue::I64(5)));
        assert_eq!(args.upper, Bound::Unbounded);
        assert_eq!(args.order, Order::Desc);
        assert_eq!(args.limit, Some(10));
    }

    #[test]
    fn query_args_refuse_unknown_keys_and_bad_types() {
        for bad in [
            obj(vec![]),
            obj(vec![("table", LiveValue::I64(1))]),
            obj(vec![
                ("table", LiveValue::Str("t".into())),
                ("filter", LiveValue::Null),
            ]),
            obj(vec![
                ("table", LiveValue::Str("t".into())),
                ("limit", LiveValue::I64(-1)),
            ]),
            obj(vec![
                ("table", LiveValue::Str("t".into())),
                ("order", LiveValue::Str("up".into())),
            ]),
            LiveValue::Null,
        ] {
            assert!(
                matches!(
                    QueryArgs::parse("_system:query", bad.clone()),
                    Err(LiveError::InvalidArgument(_))
                ),
                "{bad:?}"
            );
        }
    }
}
