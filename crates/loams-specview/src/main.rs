//! `loams-specview serve | replay`: see the crate README.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use loams_specview::event::Event;
use loams_specview::runner::RunConfig;
use loams_specview::server::{ServeConfig, Source, start};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser)]
#[command(about = "Watch the Loams router's TLA+ and Rust test runs in the browser")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args)]
struct Common {
    /// Port on 127.0.0.1.
    #[arg(long, default_value_t = 7740)]
    port: u16,
    /// The built frontend (default: crates/loams-specview/dist).
    #[arg(long)]
    dist: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the suite and stream it to the browser.
    Serve {
        #[command(flatten)]
        common: Common,
        /// Save the run's events to this JSON-lines file.
        #[arg(long)]
        record: Option<PathBuf>,
        /// The repository root (default: found from the current directory).
        #[arg(long)]
        repo: Option<PathBuf>,
        /// Only these specs (repeatable), e.g. --spec ShardMap.
        #[arg(long = "spec")]
        specs: Vec<String>,
        /// The nightly variant set instead of the PR set.
        #[arg(long)]
        nightly: bool,
        /// Also run the Apalache checks.
        #[arg(long)]
        apalache: bool,
        /// Skip the TLA+ specs.
        #[arg(long)]
        no_spec: bool,
        /// Skip the Rust tests.
        #[arg(long)]
        no_rust: bool,
        /// Cargo packages to test (repeatable; default: the router crate once it exists).
        #[arg(long = "rust-package")]
        rust_packages: Vec<String>,
        /// Wait for the Run button instead of starting at launch.
        #[arg(long)]
        no_autorun: bool,
    },
    /// Replay a run saved with `serve --record`.
    Replay {
        file: PathBuf,
        #[command(flatten)]
        common: Common,
        /// Milliseconds between events.
        #[arg(long, default_value_t = 40)]
        delay_ms: u64,
    },
}

fn find_root(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        return Ok(p);
    }
    let mut dir = std::env::current_dir()?;
    loop {
        if dir.join("scripts/spec/check.py").exists() {
            return Ok(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    let baked = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    if baked.join("scripts/spec/check.py").exists() {
        return Ok(baked.canonicalize()?);
    }
    bail!("cannot find the repository root (scripts/spec/check.py); pass --repo")
}

fn default_dist() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("dist")
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let (common, source, record, autorun) = match cli.cmd {
        Cmd::Serve {
            common,
            record,
            repo,
            specs,
            nightly,
            apalache,
            no_spec,
            no_rust,
            rust_packages,
            no_autorun,
        } => {
            let root = find_root(repo)?;
            let rust_packages = if no_rust {
                Vec::new()
            } else if rust_packages.is_empty() {
                RunConfig::default_rust_packages(&root)
            } else {
                rust_packages
            };
            let cfg = RunConfig {
                root,
                tla: !no_spec,
                only: specs,
                nightly,
                apalache,
                rust_packages,
            };
            (common, Source::Suite(cfg), record, !no_autorun)
        }
        Cmd::Replay {
            file,
            common,
            delay_ms,
        } => {
            let text = std::fs::read_to_string(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            let events = text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .enumerate()
                .map(|(i, l)| {
                    Event::from_line(l).with_context(|| format!("{}:{}", file.display(), i + 1))
                })
                .collect::<Result<Vec<_>>>()?;
            let source = Source::Replay {
                events: Arc::new(events),
                delay: Duration::from_millis(delay_ms),
            };
            (common, source, None, true)
        }
    };
    let started = start(ServeConfig {
        port: common.port,
        source,
        record,
        dist: common.dist.unwrap_or_else(default_dist),
        autorun,
    })
    .await?;
    println!("loams-specview: http://{}", started.addr);
    started.task.await?;
    Ok(())
}
