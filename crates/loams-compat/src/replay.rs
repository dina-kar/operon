//! The replay loop: capture entries in, TSV rows out. Engine access is behind [`Connect`].

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;

use crate::classify::{self, Class, Observation, Outcome};
use crate::tsv::{Engine, Row};

/// One statement of a capture (`capture.jsonl`, one JSON object per line).
#[derive(Clone, Debug, Deserialize)]
pub struct CaptureEntry {
    pub digest: String,
    pub component: String,
    /// `path:line` for static rows (Vitess), `dynamic:<scenario>` for captured ones; `;`-joined
    /// when both found the digest. Never PgDog source text.
    pub source: String,
    pub example: String,
    /// Session statements to run first (`SET …`).
    #[serde(default)]
    pub session: Vec<String>,
    /// The database (Postgres) or schema (MySQL) the statement ran in.
    #[serde(default)]
    pub db: Option<String>,
    /// `scratch` forces a scratch run; `rolled-back` forces a rolled-back transaction.
    #[serde(default)]
    pub mode: Option<String>,
    /// A tracking reference for the row (the design's `C-n` items); copied to the `issue` column.
    #[serde(default)]
    pub issue: Option<String>,
}

/// How a statement is isolated from the next one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Inside a transaction that is rolled back at the end.
    RolledBack,
    /// Transaction-control, replication and DDL-like commands: in a scratch database where the
    /// engine allows, run on a connection of their own.
    Scratch,
}

#[derive(Debug)]
pub struct Request<'a> {
    pub session: &'a [String],
    pub statement: &'a str,
    pub mode: Mode,
    pub ordered: bool,
}

/// One live connection to one engine.
#[async_trait]
pub trait Connector: Send {
    async fn run(&mut self, req: &Request<'_>) -> Outcome;
}

/// A way to open fresh connections to one engine.
#[async_trait]
pub trait Connect: Send + Sync {
    async fn connect(&self, db: Option<&str>) -> anyhow::Result<Box<dyn Connector>>;
}

pub fn mode_for(engine: Engine, entry: &CaptureEntry) -> Mode {
    match entry.mode.as_deref() {
        Some("scratch") => return Mode::Scratch,
        Some("rolled-back") => return Mode::RolledBack,
        _ => {}
    }
    let s = entry.example.trim_start().to_ascii_uppercase();
    let starts = |ps: &[&str]| ps.iter().any(|p| s.starts_with(p));
    let scratch = match engine {
        Engine::Postgres => starts(&[
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "END",
            "PREPARE TRANSACTION",
            "ABORT",
            "START_REPLICATION",
            "CREATE_REPLICATION_SLOT",
            "DROP_REPLICATION_SLOT",
            "IDENTIFY_SYSTEM",
            "READ_REPLICATION_SLOT",
            "TIMELINE_HISTORY",
            "CREATE DATABASE",
            "DROP DATABASE",
            "VACUUM",
            "CREATE SUBSCRIPTION",
            "CREATE PUBLICATION",
            "DROP PUBLICATION",
            "LISTEN",
            "UNLISTEN",
            "NOTIFY",
            "DISCARD",
            "RESET",
            "CHECKPOINT",
            "CLUSTER",
            "REINDEX",
            "CREATE INDEX CONCURRENTLY",
        ]),
        Engine::Mysql => starts(&[
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "XA ",
            "LOCK ",
            "UNLOCK",
            "CREATE DATABASE",
            "DROP DATABASE",
            "CREATE SCHEMA",
            "DROP SCHEMA",
            "RESET ",
            "FLUSH",
            "CHANGE ",
            "START REPLICA",
            "STOP REPLICA",
            "START SLAVE",
            "STOP SLAVE",
            "SET GLOBAL",
            "SET PERSIST",
            "INSTALL",
            "UNINSTALL",
            "CREATE TABLE",
            "ALTER TABLE",
            "DROP TABLE",
            "TRUNCATE",
            "RENAME",
            "CREATE INDEX",
            "DROP INDEX",
            "CREATE VIEW",
            "DROP VIEW",
            "ALTER VIEW",
        ]),
    };
    if scratch {
        Mode::Scratch
    } else {
        Mode::RolledBack
    }
}

async fn run_once(c: &dyn Connect, entry: &CaptureEntry, engine: Engine) -> Outcome {
    let ordered = entry.example.to_ascii_lowercase().contains("order by");
    let mode = mode_for(engine, entry);
    match c.connect(entry.db.as_deref()).await {
        Err(e) => Outcome::Failed(classify::DbError {
            code: "CONNECT".into(),
            message: e.to_string(),
        }),
        Ok(mut conn) => {
            conn.run(&Request {
                session: &entry.session,
                statement: &entry.example,
                mode,
                ordered,
            })
            .await
        }
    }
}

/// Error codes that mean "the object the statement creates is already there", per engine.
pub fn already_exists(engine: Engine, code: &str) -> bool {
    match engine {
        // duplicate_table, duplicate_object, duplicate_function, duplicate_schema, duplicate_database
        Engine::Postgres => matches!(code, "42P07" | "42710" | "42723" | "42P06" | "42P04"),
        // ER_TABLE_EXISTS_ERROR, ER_DB_CREATE_EXISTS, ER_DUP_FIELDNAME, ER_DUP_KEYNAME
        Engine::Mysql => matches!(code, "1050" | "1007" | "1060" | "1061"),
    }
}

/// Replay settings.
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// A database without the captured schema. A statement whose reference run fails because its
    /// object already exists is replayed there, on both engines, so that `CREATE` statements are
    /// compared by what they do and not by an "already exists" error.
    pub empty_db: Option<String>,
    /// `(substring, reason)` pairs: a target error whose message contains the substring is a refusal
    /// by design, so the row is `unsupported` with the reason (the owner's rulings, such as
    /// SmartEngine's isolation levels). A digest in the `unsupported` map takes precedence.
    pub unsupported_rules: Vec<(String, String)>,
    /// Lower-case substrings of statements that read an instance's identity; they compare by column names.
    pub shape_only: Vec<String>,
}

/// Replays every entry. `target = None` makes every row `pending-target` (Ruling 7), with the
/// reference result recorded. `unsupported` maps a digest to the reason the owner has ruled
/// that the target refuses it by design.
pub async fn replay_all(
    engine: Engine,
    reference: &dyn Connect,
    target: Option<&dyn Connect>,
    entries: &[CaptureEntry],
    unsupported: &HashMap<String, String>,
    opts: &Options,
) -> Vec<Row> {
    let mut rows = Vec::with_capacity(entries.len());
    // Statements that change the engine for good (DDL, globals, replication commands) go last, so that a
    // `DROP TABLE` early in the list does not turn every later statement on that table into an error.
    let (scratch, rest): (Vec<&CaptureEntry>, Vec<&CaptureEntry>) = entries
        .iter()
        .partition(|e| mode_for(engine, e) == Mode::Scratch);
    for entry in rest.into_iter().chain(scratch) {
        let mut e = entry.clone();
        let mut r1 = run_once(reference, &e, engine).await;
        let mut moved = false;
        if let (Outcome::Failed(err), Some(empty)) = (&r1, &opts.empty_db)
            && already_exists(engine, &err.code)
        {
            e.db = Some(empty.clone());
            r1 = run_once(reference, &e, engine).await;
            moved = true;
        }
        let r2 = run_once(reference, &e, engine).await;
        let t = match target {
            Some(t) => Some(run_once(t, &e, engine).await),
            None => None,
        };
        let rule_note = match &t {
            Some(Outcome::Failed(err)) => opts
                .unsupported_rules
                .iter()
                .find(|(sub, _)| err.message.contains(sub.as_str()))
                .map(|(_, reason)| reason.as_str()),
            _ => None,
        };
        let lower = e.example.to_ascii_lowercase();
        let shape_only = opts
            .shape_only
            .iter()
            .any(|sub| lower.contains(sub.as_str()));
        let mut verdict = classify::classify(&Observation {
            reference: &r1,
            reference_again: Some(&r2),
            target: t.as_ref(),
            unsupported_note: unsupported.get(&e.digest).map(String::as_str).or(rule_note),
            shape_only,
        });
        if moved {
            let n = "replayed in an empty database: the object exists in the captured schema";
            verdict.note = if verdict.note.is_empty() {
                n.to_string()
            } else {
                format!("{n}; {}", verdict.note)
            };
        }
        rows.push(Row {
            digest: e.digest.clone(),
            component: e.component.clone(),
            source: e.source.clone(),
            example: e.example.clone(),
            class: verdict.class,
            ref_hash: verdict.ref_hash,
            target_hash: verdict.target_hash,
            note: verdict.note,
            issue: e.issue.clone().unwrap_or_default(),
        });
    }
    debug_assert!(
        rows.iter()
            .all(|r| r.class != Class::Unsupported || !r.note.is_empty())
    );
    rows
}

/// Reads a `capture.jsonl`, skipping blank lines.
pub fn read_capture(text: &str) -> anyhow::Result<Vec<CaptureEntry>> {
    let mut v = Vec::new();
    for (i, l) in text.lines().enumerate() {
        if l.trim().is_empty() {
            continue;
        }
        v.push(
            serde_json::from_str(l).map_err(|e| anyhow::anyhow!("capture line {}: {e}", i + 1))?,
        );
    }
    Ok(v)
}

/// Reads an unsupported list: `digest<TAB>reason` per line, `#` comments allowed.
pub fn read_unsupported(text: &str) -> HashMap<String, String> {
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            l.split_once('\t')
                .map(|(d, n)| (d.to_string(), n.to_string()))
        })
        .collect()
}

/// Reads unsupported rules: `substring<TAB>reason` per line, `#` comments allowed.
pub fn read_unsupported_rules(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            l.split_once('\t')
                .map(|(a, b)| (a.to_string(), b.to_string()))
        })
        .collect()
}
