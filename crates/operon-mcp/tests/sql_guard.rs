//! The `sql` tool's guard (plan M1.6 Task 8 rule 3, Ruling 10, rows E18
//! and E19) over a plain `SessionContext` with a `MemTable` `kb(a BIGINT)`
//! of five rows.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use operon_mcp::sql::{SqlLimits, run_read_only};
use serde_json::json;

fn context() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .expect("batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("table");
    let ctx = SessionContext::new();
    ctx.register_table("kb", Arc::new(table)).expect("register");
    ctx
}

fn limits() -> SqlLimits {
    SqlLimits {
        max_rows: 1_000,
        timeout: Duration::from_secs(30),
        max_output_bytes: 1_048_576,
    }
}

async fn count(ctx: &SessionContext) -> serde_json::Value {
    let out = run_read_only(ctx, "SELECT count(*) AS n FROM kb", &limits())
        .await
        .expect("count");
    out.rows[0]["n"].clone()
}

#[tokio::test]
async fn read_only_sql_runs_queries() {
    let ctx = context();
    let out = run_read_only(&ctx, "SELECT count(*) AS n FROM kb", &limits())
        .await
        .expect("runs");
    assert_eq!(out.rows, vec![json!({"n": 5}).as_object().unwrap().clone()]);
    assert_eq!(out.columns.len(), 1);
    assert_eq!(out.columns[0].name, "n");
    assert_eq!(out.columns[0].data_type, "Int64");
    assert_eq!(out.row_count, 1);
    assert!(!out.truncated);
}

#[tokio::test]
async fn read_only_sql_refuses_writes_and_statements() {
    let ctx = context();
    for sql in [
        "CREATE TABLE t (a INT)",
        "CREATE VIEW v AS SELECT 1",
        "DROP TABLE kb",
        "INSERT INTO kb VALUES (1)",
        "COPY (SELECT 1) TO 'out.csv'",
        "SET datafusion.execution.batch_size = 1",
        "EXPLAIN ANALYZE INSERT INTO kb VALUES (1)",
        "SELECT 1; SELECT 2",
    ] {
        let err = run_read_only(&ctx, sql, &limits()).await.expect_err(sql);
        assert_eq!(err.code, "invalid_argument", "{sql}: {}", err.message);
    }
    assert_eq!(count(&ctx).await, 5);
    assert!(!ctx.table_exist("t").unwrap());
    assert!(!ctx.table_exist("v").unwrap());
    assert!(!std::path::Path::new("out.csv").exists());
}

#[tokio::test]
async fn read_only_sql_refuses_url_tables() {
    let err = run_read_only(&context(), "SELECT * FROM '/etc/passwd'", &limits())
        .await
        .expect_err("no url tables");
    assert_eq!(err.code, "invalid_argument");
}

#[tokio::test]
async fn max_rows_truncates() {
    let limits = SqlLimits {
        max_rows: 2,
        ..limits()
    };
    let out = run_read_only(&context(), "SELECT a FROM kb ORDER BY a", &limits)
        .await
        .expect("runs");
    assert_eq!(out.rows.len(), 2);
    assert_eq!(out.row_count, 2);
    assert_eq!(out.rows[1]["a"], 2);
    assert!(out.truncated);
}

#[tokio::test]
async fn a_slow_query_times_out() {
    let limits = SqlLimits {
        timeout: Duration::from_millis(50),
        ..limits()
    };
    let err = run_read_only(
        &context(),
        "SELECT count(*) FROM range(1, 100000000000)",
        &limits,
    )
    .await
    .expect_err("times out");
    assert_eq!(err.code, "timeout");
    assert_eq!(err.message, "query exceeded 50ms");
}

#[tokio::test]
async fn output_is_capped_by_bytes() {
    let limits = SqlLimits {
        max_output_bytes: 64,
        ..limits()
    };
    let out = run_read_only(&context(), "SELECT a FROM kb ORDER BY a", &limits)
        .await
        .expect("runs");
    assert!(out.rows.len() < 5, "{:?}", out.rows);
    assert_eq!(out.row_count, out.rows.len());
    assert!(out.truncated);
}
