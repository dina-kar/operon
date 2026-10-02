//! The Postgres connector (tokio-postgres, text protocol for plain statements, the extended
//! protocol for statements with `$n` placeholders as `pg_stat_statements` stores them).

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::BytesMut;
use sha2::Digest;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

use crate::classify::{DbError, Outcome, ResultSet};
use crate::replay::{Connect, Connector, Mode, Request};

static SCRATCH: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct PgConnect {
    config: Config,
}

impl PgConnect {
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            config: url.parse()?,
        })
    }
}

async fn open(config: &Config, db: Option<&str>) -> anyhow::Result<Client> {
    let mut c = config.clone();
    if let Some(db) = db {
        c.dbname(db);
    }
    let (client, conn) = c.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn err(e: &tokio_postgres::Error) -> Outcome {
    match e.as_db_error() {
        Some(d) => Outcome::Failed(DbError {
            code: d.code().code().to_string(),
            message: d.message().to_string(),
        }),
        None => Outcome::Failed(DbError {
            code: "CLIENT".into(),
            message: e.to_string(),
        }),
    }
}

/// A parameter that is always NULL, of whatever type the server inferred.
#[derive(Debug)]
struct NullParam;

impl ToSql for NullParam {
    fn to_sql(
        &self,
        _: &Type,
        _: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    to_sql_checked!();
}

/// `pg_stat_statements` stores utility statements with `$n` where the client sent a literal
/// (`COMMIT PREPARED $1`), and a simple query cannot bind them: each becomes the text literal `'1'`.
fn fill_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut it = sql.chars().peekable();
    while let Some(c) = it.next() {
        if c == '$' && it.peek().is_some_and(char::is_ascii_digit) {
            while it.peek().is_some_and(char::is_ascii_digit) {
                it.next();
            }
            out.push_str("'1'");
        } else {
            out.push(c);
        }
    }
    out
}

fn has_replication_command(sql: &str) -> bool {
    sql.split(";;").any(|p| is_replication_command(p.trim()))
}

fn is_replication_command(s: &str) -> bool {
    let u = s.trim_start().to_ascii_uppercase();
    [
        "START_REPLICATION",
        "CREATE_REPLICATION_SLOT",
        "DROP_REPLICATION_SLOT",
        "IDENTIFY_SYSTEM",
        "READ_REPLICATION_SLOT",
        "TIMELINE_HISTORY",
    ]
    .iter()
    .any(|p| u.starts_with(p))
}

/// Runs a statement. An example may hold several commands separated by ` ;; `; each is sent as
/// its own simple query on the same connection (commands such as `COMMIT PREPARED` cannot share
/// a multi-command string). `COPY … TO STDOUT` is read with the copy API and recorded as a hash.
async fn simple(client: &Client, sql: &str, ordered: bool) -> Outcome {
    let mut rs = ResultSet {
        ordered,
        ..ResultSet::default()
    };
    let sql = &fill_placeholders(sql);
    for part in sql.split(";;").map(str::trim).filter(|p| !p.is_empty()) {
        if let Some(out) = copy_out(client, part, &mut rs).await {
            return out;
        }
        if let Some(out) = copy_in(client, part, &mut rs).await {
            return out;
        }
        let msgs = match client.simple_query(part).await {
            Ok(m) => m,
            Err(e) => return err(&e),
        };
        for m in msgs {
            match m {
                SimpleQueryMessage::Row(r) => {
                    if rs.columns.is_empty() {
                        rs.columns = r.columns().iter().map(|c| c.name().to_string()).collect();
                    }
                    rs.rows
                        .push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect());
                }
                SimpleQueryMessage::CommandComplete(n) => rs.tags.push(format!("rows:{n}")),
                _ => {}
            }
        }
    }
    Outcome::Rows(rs)
}

/// `COPY … FROM STDIN`: the command is sent and finished with no data, which checks the syntax, the
/// table, the columns and the format option on the server (a binary copy fails on the missing
/// header, on both engines). `None` when the command is not a copy-in.
async fn copy_in(client: &Client, sql: &str, rs: &mut ResultSet) -> Option<Outcome> {
    use futures::SinkExt;
    let u = sql.trim_start().to_ascii_uppercase();
    if !(u.starts_with("COPY") && u.contains("FROM STDIN")) {
        return None;
    }
    let sink = match client.copy_in::<_, bytes::Bytes>(sql).await {
        Ok(s) => s,
        Err(e) => return Some(err(&e)),
    };
    futures::pin_mut!(sink);
    if let Err(e) = sink.close().await {
        return Some(err(&e));
    }
    rs.tags.push("copy-in:0 rows".to_string());
    Some(Outcome::Rows(std::mem::take(rs)))
}

/// `COPY … TO STDOUT`: the bytes the server sends, hashed into one cell. `None` when the command
/// is not a copy-out, so the caller sends it as a plain query.
async fn copy_out(client: &Client, sql: &str, rs: &mut ResultSet) -> Option<Outcome> {
    use futures::StreamExt;
    let u = sql.trim_start().to_ascii_uppercase();
    if !(u.starts_with("COPY") && u.contains("TO STDOUT")) {
        return None;
    }
    let stream = match client.copy_out(sql).await {
        Ok(s) => s,
        Err(e) => return Some(err(&e)),
    };
    futures::pin_mut!(stream);
    let mut h = sha2::Sha256::new();
    let mut n = 0usize;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(b) => {
                h.update(&b);
                n += b.len();
            }
            Err(e) => return Some(err(&e)),
        }
    }
    rs.columns = vec!["copy_out_sha256".into(), "bytes".into()];
    rs.rows
        .push(vec![Some(hex::encode(h.finalize())), Some(n.to_string())]);
    Some(Outcome::Rows(std::mem::take(rs)))
}

/// Prepares the statement and runs it with all-NULL parameters. The result records the
/// parameter and column types (what the server inferred) and the row count, not row values.
async fn extended(client: &Client, sql: &str) -> Outcome {
    let stmt = match client.prepare(sql).await {
        Ok(s) => s,
        // A utility statement with `$n` where a literal was: run it with literals.
        Err(e) if e.code().is_some_and(|c| c.code() == "42601") => {
            return simple(client, sql, false).await;
        }
        Err(e) => return err(&e),
    };
    let mut rs = ResultSet::default();
    rs.tags.push(format!(
        "params:{}",
        stmt.params()
            .iter()
            .map(|t| t.name())
            .collect::<Vec<_>>()
            .join(",")
    ));
    rs.columns = stmt
        .columns()
        .iter()
        .map(|c| format!("{}:{}", c.name(), c.type_().name()))
        .collect();
    let params: Vec<NullParam> = stmt.params().iter().map(|_| NullParam).collect();
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    if stmt.columns().is_empty() {
        match client.execute(&stmt, &refs).await {
            Ok(n) => rs.tags.push(format!("rows:{n}")),
            Err(e) => return err(&e),
        }
    } else {
        match client.query(&stmt, &refs).await {
            Ok(rows) => rs.tags.push(format!("rows:{}", rows.len())),
            Err(e) => return err(&e),
        }
    }
    Outcome::Rows(rs)
}

#[derive(Debug)]
struct PgConn {
    config: Config,
    db: Option<String>,
}

#[async_trait]
impl Connector for PgConn {
    async fn run(&mut self, req: &Request<'_>) -> Outcome {
        match req.mode {
            Mode::Scratch => self.run_scratch(req).await,
            Mode::RolledBack if has_replication_command(req.statement) => {
                self.run_scratch(req).await
            }
            Mode::RolledBack => {
                let client = match open(&self.config, self.db.as_deref()).await {
                    Ok(c) => c,
                    Err(e) => {
                        return Outcome::Failed(DbError {
                            code: "CONNECT".into(),
                            message: e.to_string(),
                        });
                    }
                };
                run_in_tx(&client, req).await
            }
        }
    }
}

async fn run_in_tx(client: &Client, req: &Request<'_>) -> Outcome {
    if let Err(e) = client.batch_execute("BEGIN").await {
        return err(&e);
    }
    for s in req.session {
        if let Err(e) = client.batch_execute(s).await {
            let _ = client.batch_execute("ROLLBACK").await;
            return err(&e);
        }
    }
    let out = if req.statement.contains("$1") {
        extended(client, req.statement).await
    } else {
        simple(client, req.statement, req.ordered).await
    };
    let _ = client.batch_execute("ROLLBACK").await;
    out
}

impl PgConn {
    /// Replication commands need a `replication=database` connection, which tokio-postgres
    /// 0.7 cannot open, so they run through `psql`. `START_REPLICATION` enters copy-both mode,
    /// which `psql` reports as an unexpected result status after the server accepted it; that
    /// report is the observable result.
    async fn run_replication(&self, db: &str, stmt: &str) -> Outcome {
        let host = match self.config.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
            _ => "localhost".to_string(),
        };
        let port = self.config.get_ports().first().copied().unwrap_or(5432);
        let user = self.config.get_user().unwrap_or("postgres").to_string();
        let conninfo =
            format!("host={host} port={port} user={user} dbname={db} replication=database");
        let mut cmd = tokio::process::Command::new("psql");
        cmd.args(["-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", &conninfo]);
        for part in stmt.split(";;").map(str::trim).filter(|p| !p.is_empty()) {
            cmd.args(["-c", part]);
        }
        cmd.kill_on_drop(true);
        if let Some(p) = self.config.get_password() {
            cmd.env("PGPASSWORD", String::from_utf8_lossy(p).into_owned());
        }
        let out = match tokio::time::timeout(std::time::Duration::from_secs(20), cmd.output()).await
        {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => {
                return Outcome::Failed(DbError {
                    code: "CLIENT".into(),
                    message: format!("psql: {e}"),
                });
            }
            Err(_) => {
                return Outcome::Failed(DbError {
                    code: "TIMEOUT".into(),
                    message: "psql timed out".into(),
                });
            }
        };
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        if stderr.contains("PQresultStatus: 8") || stderr.contains("COPY_BOTH") {
            return Outcome::Rows(ResultSet {
                columns: vec!["copy-both".into()],
                rows: vec![vec![Some("started".into())]],
                ..ResultSet::default()
            });
        }
        if !out.status.success() {
            // "ERROR:  message" carries no SQLSTATE in psql's default verbosity; the first
            // line, with names and numbers masked, stands in for the code.
            let first = stderr
                .lines()
                .find(|l| l.contains("ERROR") || l.contains("error"))
                .unwrap_or("psql failed")
                .to_string();
            let code: String = first
                .split(':')
                .next_back()
                .unwrap_or("")
                .trim()
                .chars()
                .map(|c| if c.is_ascii_digit() { '#' } else { c })
                .take(60)
                .collect();
            return Outcome::Failed(DbError {
                code,
                message: first,
            });
        }
        let mut rs = ResultSet::default();
        for line in stdout.lines() {
            rs.rows
                .push(line.split('|').map(|c| Some(c.to_string())).collect());
        }
        Outcome::Rows(rs)
    }

    /// Transaction control, `PREPARE TRANSACTION` and the like: in a scratch database cloned
    /// from the statement's database, dropped afterwards (with any prepared transaction).
    async fn run_scratch(&self, req: &Request<'_>) -> Outcome {
        let admin = match open(&self.config, Some("postgres")).await {
            Ok(c) => c,
            Err(e) => {
                return Outcome::Failed(DbError {
                    code: "CONNECT".into(),
                    message: e.to_string(),
                });
            }
        };
        let template = self.db.clone().unwrap_or_else(|| "postgres".into());
        let name = format!(
            "compat_scratch_{}_{}",
            std::process::id(),
            SCRATCH.fetch_add(1, Ordering::Relaxed)
        );
        if let Err(e) = admin
            .batch_execute(&format!(
                "CREATE DATABASE {name} TEMPLATE \"{}\"",
                template.replace('"', "")
            ))
            .await
        {
            return err(&e);
        }
        // Names the example gives to cluster-wide objects (prepared transaction ids, replication
        // slots) get this run's suffix, so that two replays never meet.
        let statement = req.statement.replace(
            "loams_inv_",
            &format!("loams_inv{}_", SCRATCH.load(Ordering::Relaxed)),
        );
        let out = match open(&self.config, Some(&name)).await {
            Err(e) => Outcome::Failed(DbError {
                code: "CONNECT".into(),
                message: e.to_string(),
            }),
            Ok(client) if has_replication_command(&statement) => {
                // The SQL commands in front of the first replication command (a publication, say)
                // go through the ordinary connection; the rest needs a replication connection.
                let parts: Vec<&str> = statement.split(";;").map(str::trim).collect();
                let first = parts
                    .iter()
                    .position(|p| is_replication_command(p))
                    .unwrap_or(0);
                let lead = parts[..first].join(" ;; ");
                if !lead.is_empty()
                    && let Outcome::Failed(e) = simple(&client, &lead, false).await
                {
                    Outcome::Failed(e)
                } else {
                    drop(client);
                    self.run_replication(&name, &parts[first..].join(" ;; "))
                        .await
                }
            }
            Ok(client) => {
                let mut out = None;
                for s in req.session {
                    if let Err(e) = client.batch_execute(s).await {
                        out = Some(err(&e));
                        break;
                    }
                }
                let out = match out {
                    Some(o) => o,
                    None => simple(&client, &statement, req.ordered).await,
                };
                let _ = client.batch_execute("ROLLBACK").await;
                out
            }
        };
        // A prepared transaction can only be finished from the database it was prepared in.
        if let Ok(inner) = open(&self.config, Some(&name)).await
            && let Ok(rows) = inner
                .query(
                    "select gid from pg_prepared_xacts where database = current_database()",
                    &[],
                )
                .await
        {
            for r in rows {
                let gid: String = r.get(0);
                let _ = inner
                    .batch_execute(&format!("ROLLBACK PREPARED '{}'", gid.replace('\'', "''")))
                    .await;
            }
        }
        let _ = admin
            .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .await;
        out
    }
}

#[async_trait]
impl Connect for PgConnect {
    async fn connect(&self, db: Option<&str>) -> anyhow::Result<Box<dyn Connector>> {
        // Fail early when the server is unreachable, so the row says CONNECT, not a late error.
        drop(open(&self.config, db).await?);
        Ok(Box::new(PgConn {
            config: self.config.clone(),
            db: db.map(str::to_string),
        }))
    }
}
