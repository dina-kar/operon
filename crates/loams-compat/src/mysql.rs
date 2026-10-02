//! The MySQL connector (mysql_async, text protocol). Used for MySQL 8.0.46 and for WeSQL.

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Value};

use crate::classify::{DbError, Outcome, ResultSet};
use crate::replay::{Connect, Connector, Mode, Request};

#[derive(Debug)]
pub struct MysqlConnect {
    opts: Opts,
}

impl MysqlConnect {
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            opts: Opts::from_url(url)?,
        })
    }
}

fn err(e: &mysql_async::Error) -> Outcome {
    match e {
        mysql_async::Error::Server(s) => Outcome::Failed(DbError {
            code: s.code.to_string(),
            message: s.message.clone(),
        }),
        other => Outcome::Failed(DbError {
            code: "CLIENT".into(),
            message: other.to_string(),
        }),
    }
}

fn cell(v: &Value) -> Option<String> {
    match v {
        Value::NULL => None,
        Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        other => Some(other.as_sql(true)),
    }
}

/// Runs a statement and gathers every result set, the affected-row counts and the warnings.
async fn run_text(conn: &mut Conn, sql: &str, ordered: bool) -> Outcome {
    let mut rs = ResultSet {
        ordered,
        ..ResultSet::default()
    };
    let mut warnings = 0u16;
    {
        let mut result = match conn.query_iter(sql).await {
            Ok(r) => r,
            Err(e) => return err(&e),
        };
        loop {
            if rs.columns.is_empty() {
                rs.columns = result
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().into_owned())
                    .collect();
            }
            match result.collect::<mysql_async::Row>().await {
                Ok(rows) => {
                    for r in rows {
                        rs.rows.push(r.unwrap().iter().map(cell).collect());
                    }
                }
                Err(e) => return err(&e),
            }
            rs.tags.push(format!("affected:{}", result.affected_rows()));
            warnings = warnings.max(result.warnings());
            if result.is_empty() {
                break;
            }
        }
    }
    if warnings > 0
        && let Ok(rows) = conn
            .query::<(String, u32, String), _>("SHOW WARNINGS")
            .await
    {
        rs.warnings = rows
            .into_iter()
            .map(|(level, code, _msg)| format!("{level}:{code}"))
            .collect();
    }
    Outcome::Rows(rs)
}

struct MysqlConn {
    conn: Conn,
}

/// Statements that would end the replay itself: they are not run, on either engine, and the row says so.
fn not_executed(sql: &str) -> bool {
    let u = sql.trim_start().to_ascii_uppercase();
    [
        "SHUTDOWN",
        "RESTART",
        "DROP DATABASE",
        "DROP SCHEMA",
        "ALTER INSTANCE",
        "CLONE",
    ]
    .iter()
    .any(|p| u.starts_with(p))
}

#[async_trait]
impl Connector for MysqlConn {
    async fn run(&mut self, req: &Request<'_>) -> Outcome {
        if not_executed(req.statement) {
            return Outcome::Failed(DbError {
                code: "NOT-EXECUTED".into(),
                message: "not run: it would end the replay".into(),
            });
        }
        let tx = req.mode == Mode::RolledBack;
        if tx && let Err(e) = self.conn.query_drop("START TRANSACTION").await {
            return err(&e);
        }
        for s in req.session {
            if let Err(e) = self.conn.query_drop(s).await {
                if tx {
                    let _ = self.conn.query_drop("ROLLBACK").await;
                }
                return err(&e);
            }
        }
        let out = run_text(&mut self.conn, req.statement, req.ordered).await;
        let _ = self.conn.query_drop("ROLLBACK").await;
        if !tx {
            // Undo what a global or locking statement may have left behind for the next replay.
            for fix in [
                "UNLOCK TABLES",
                "SET GLOBAL super_read_only = OFF",
                "SET GLOBAL read_only = OFF",
            ] {
                let _ = self.conn.query_drop(fix).await;
            }
        }
        out
    }
}

#[async_trait]
impl Connect for MysqlConnect {
    async fn connect(&self, db: Option<&str>) -> anyhow::Result<Box<dyn Connector>> {
        let b = OptsBuilder::from_opts(self.opts.clone());
        let b = match db {
            Some(d) => b.db_name(Some(d.to_string())),
            None => b,
        };
        let conn = Conn::new(b).await?;
        Ok(Box::new(MysqlConn { conn }))
    }
}
