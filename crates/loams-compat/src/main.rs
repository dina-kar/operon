//! `compat-replay --engine postgres|mysql --reference <url> [--target <url>] --input capture.jsonl --out statements.tsv`

use std::collections::HashMap;

use anyhow::Context;
use clap::{Parser, ValueEnum};
use loams_compat::replay::{self, Connect};
use loams_compat::tsv::{self, Engine};
use loams_compat::{mysql::MysqlConnect, pg::PgConnect};

#[derive(Clone, Copy, ValueEnum)]
enum EngineArg {
    Postgres,
    Mysql,
}

#[derive(Parser)]
#[command(
    about = "Replay captured router statements on a reference and a target engine and classify the results"
)]
struct Args {
    #[arg(long, value_enum)]
    engine: EngineArg,
    /// Connection URL of the reference engine (Postgres 17.11 or MySQL 8.0.46).
    #[arg(long)]
    reference: String,
    /// Connection URL of the target (Loams Postgres compute, WeSQL). Omit when the target is
    /// not runnable: every row is then `pending-target`, with the reference result recorded.
    #[arg(long)]
    target: Option<String>,
    #[arg(long)]
    input: std::path::PathBuf,
    #[arg(long)]
    out: std::path::PathBuf,
    /// `digest<TAB>reason` lines for statements the target refuses by design.
    #[arg(long)]
    unsupported: Option<std::path::PathBuf>,
    /// `substring<TAB>reason` lines: a target error containing the substring is `unsupported` by design.
    #[arg(long)]
    unsupported_rules: Option<std::path::PathBuf>,
    /// Lower-case substrings (one per line) of statements that read an instance's identity (version, uuid,
    /// binlog position): they compare by column names.
    #[arg(long)]
    shape_only: Option<std::path::PathBuf>,
    /// A database without the captured schema, for statements whose object already exists.
    #[arg(long)]
    empty_db: Option<String>,
}

fn connector(engine: EngineArg, url: &str) -> anyhow::Result<Box<dyn Connect>> {
    Ok(match engine {
        EngineArg::Postgres => Box::new(PgConnect::from_url(url)?),
        EngineArg::Mysql => Box::new(MysqlConnect::from_url(url)?),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let engine = match a.engine {
        EngineArg::Postgres => Engine::Postgres,
        EngineArg::Mysql => Engine::Mysql,
    };
    let entries = replay::read_capture(
        &std::fs::read_to_string(&a.input).with_context(|| a.input.display().to_string())?,
    )?;
    let unsupported = match &a.unsupported {
        Some(p) => replay::read_unsupported(&std::fs::read_to_string(p)?),
        None => HashMap::new(),
    };
    let unsupported_rules = match &a.unsupported_rules {
        Some(p) => replay::read_unsupported_rules(&std::fs::read_to_string(p)?),
        None => Vec::new(),
    };
    let shape_only: Vec<String> = match &a.shape_only {
        Some(p) => std::fs::read_to_string(p)?
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
            .map(|l| l.trim().to_ascii_lowercase())
            .collect(),
        None => Vec::new(),
    };
    let reference = connector(a.engine, &a.reference)?;
    let target = a
        .target
        .as_deref()
        .map(|u| connector(a.engine, u))
        .transpose()?;
    let rows = replay::replay_all(
        engine,
        reference.as_ref(),
        target.as_deref(),
        &entries,
        &unsupported,
        &replay::Options {
            empty_db: a.empty_db.clone(),
            unsupported_rules,
            shape_only,
        },
    )
    .await;
    std::fs::write(&a.out, tsv::write(&rows, engine)?)?;
    for ((component, class), n) in tsv::summarize(&rows) {
        eprintln!("{component}\t{class}\t{n}");
    }
    eprintln!("{} rows written to {}", rows.len(), a.out.display());
    Ok(())
}
