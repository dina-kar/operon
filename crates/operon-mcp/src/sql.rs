//! The `sql` tool (plan M1.6 Task 8 rule 3, Ruling 10, row E18): planned
//! read-only through `operon_query::sql::plan_read_only`, as REST and
//! Flight SQL plan, with a row cap, a time limit and an output cap.

use std::time::Duration;

use datafusion::prelude::SessionContext;
use operon_query::{ServiceError, SqlConfig};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::ToolError;
use crate::output::{cap_items, tool_result_json};

/// The bounds of one `sql` call.
#[derive(Clone, Debug)]
pub struct SqlLimits {
    /// The most rows returned.
    pub max_rows: usize,
    /// How long the statement may run.
    pub timeout: Duration,
    /// The most JSON bytes of the result (Ruling 13).
    pub max_output_bytes: usize,
}

/// A `sql` answer: the rows as objects keyed by column name.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SqlOutput {
    pub columns: Vec<ColumnOut>,
    pub rows: Vec<Map<String, Value>>,
    /// The rows kept.
    pub row_count: usize,
    /// More rows existed than `rows` holds.
    pub truncated: bool,
}

/// A result column: its name and Arrow `DataType` Display.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ColumnOut {
    pub name: String,
    pub data_type: String,
}

/// Runs `query` read-only in `ctx` (rule 3): DDL, DML and statements are
/// refused anywhere in the plan (`invalid_argument`), at most `max_rows`
/// rows are kept, `timeout` bounds planning and execution, and the rows are
/// capped so the whole tool result is at most `max_output_bytes` of JSON.
pub async fn run_read_only(
    ctx: &SessionContext,
    query: &str,
    limits: &SqlLimits,
) -> Result<SqlOutput, ToolError> {
    let config = SqlConfig {
        max_rows: limits.max_rows,
        timeout: limits.timeout,
        ..SqlConfig::default()
    };
    let result = operon_query::sql::run_read_only(ctx, query, &config)
        .await
        .map_err(|err| match err {
            ServiceError::Timeout => ToolError::new(
                "timeout",
                format!(
                    "query exceeded {}",
                    humantime::format_duration(limits.timeout)
                ),
            ),
            other => ToolError::from(other),
        })?;
    // Rows become objects keyed by column name, so two columns of one
    // name (`a.id, b.id` of a join) would collapse (PR #99 review).
    let mut names = std::collections::BTreeSet::new();
    for field in result.schema.fields() {
        if !names.insert(field.name().as_str()) {
            return Err(ToolError::invalid(format!(
                "the result has two columns named `{}`; give them distinct aliases",
                field.name()
            )));
        }
    }
    let json = operon_query::sql::rows_to_json(&result);
    let columns: Vec<ColumnOut> = result
        .schema
        .fields()
        .iter()
        .map(|field| ColumnOut {
            name: field.name().clone(),
            data_type: format!("{}", field.data_type()),
        })
        .collect();
    let mut rows: Vec<Map<String, Value>> = match json {
        Value::Object(mut object) => match object.remove("rows") {
            Some(Value::Array(rows)) => rows
                .into_iter()
                .map(|row| match row {
                    Value::Array(values) => {
                        columns.iter().map(|c| c.name.clone()).zip(values).collect()
                    }
                    _ => Map::new(),
                })
                .collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let render = |rows: &[Map<String, Value>]| {
        tool_result_json(serde_json::json!({
            "columns": columns,
            "rows": rows,
            "row_count": rows.len(),
            "truncated": true,
        }))
    };
    // No row can be dropped to fit a result whose columns alone are over
    // the cap (PR #99 review).
    if render(&[]).to_string().len() > limits.max_output_bytes {
        return Err(ToolError::new(
            "resource_exhausted",
            format!(
                "the result's {} columns alone exceed the {}-byte output cap; select fewer columns",
                columns.len(),
                limits.max_output_bytes
            ),
        ));
    }
    let capped = cap_items(&mut rows, limits.max_output_bytes, render);
    Ok(SqlOutput {
        row_count: rows.len(),
        truncated: result.truncated || capped,
        columns,
        rows,
    })
}
