//! Runs the suite as subprocesses and turns their output into [`Event`]s:
//! TLC per spec variant (with counterexample traces), optionally Apalache,
//! then the Rust tests (`cargo nextest` when installed, else `cargo test`).

use crate::event::{Event, RunEvent, TestEvent};
use crate::testout::TestParser;
use crate::tlc::{Outcome, TlcParser};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::UnboundedSender;

pub type Tx = UnboundedSender<Event>;

#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The repository root (holds `scripts/spec/` and `spec/tla/`).
    pub root: PathBuf,
    /// Run the TLA+ variants.
    pub tla: bool,
    /// Only this spec (by name), when set.
    pub only: Vec<String>,
    /// The nightly variant set instead of the PR set.
    pub nightly: bool,
    /// Also run the Apalache checks listed in `specs.toml`.
    pub apalache: bool,
    /// Cargo packages whose tests run after the specs.
    pub rust_packages: Vec<String>,
}

impl RunConfig {
    /// The router test crate, once it exists (RT1).
    pub fn default_rust_packages(root: &Path) -> Vec<String> {
        ["loams-sqlrouter", "operon-sqlrouter"]
            .iter()
            .filter(|p| root.join("crates").join(p).join("Cargo.toml").exists())
            .map(|p| (*p).to_owned())
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct SpecsFile {
    #[serde(default)]
    spec: Vec<SpecDef>,
}

#[derive(Debug, Deserialize)]
struct SpecDef {
    name: String,
    model: String,
    #[serde(default)]
    parse_only: bool,
    #[serde(default)]
    variant: Vec<VariantDef>,
}

#[derive(Debug, Deserialize)]
struct VariantDef {
    cfg: String,
    expect: String,
    #[serde(default)]
    pr: bool,
    #[serde(default)]
    nightly: bool,
    #[serde(default)]
    apalache: Vec<String>,
    apalache_cinit: Option<String>,
    apalache_length: Option<u32>,
}

/// Run everything configured. Returns whether every check passed.
pub async fn run_suite(cfg: &RunConfig, tx: &Tx) -> bool {
    let start = Instant::now();
    let mut pass = true;
    if cfg.tla {
        pass &= run_tla(cfg, tx).await;
    }
    if !cfg.rust_packages.is_empty() {
        pass &= run_rust(cfg, tx).await;
    }
    let _ = tx.send(Event::Done {
        pass,
        secs: start.elapsed().as_secs_f64(),
    });
    pass
}

fn cache_dir() -> PathBuf {
    std::env::var_os("LOAMS_SPEC_TOOLS")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".cache/loam/spec-tools")
        })
}

/// The cached `tla2tools` jar named by `tools.lock` (the layout `check.py` uses).
fn tla2tools(root: &Path) -> Result<PathBuf> {
    let lock = std::fs::read_to_string(root.join("scripts/spec/tools.lock"))
        .context("reading tools.lock")?;
    for line in lock
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if let [name, version, url, _sha] = parts[..]
            && name == "tla2tools"
        {
            let ext = Path::new(url)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("bin");
            return Ok(cache_dir().join(format!("{name}-{version}.{ext}")));
        }
    }
    bail!("tools.lock has no tla2tools entry")
}

fn apalache_bin(root: &Path) -> Result<PathBuf> {
    let lock = std::fs::read_to_string(root.join("scripts/spec/tools.lock"))?;
    for line in lock
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if let [name, version, ..] = parts[..]
            && name == "apalache"
        {
            return Ok(cache_dir().join(format!("apalache-{version}/bin/apalache-mc")));
        }
    }
    bail!("tools.lock has no apalache entry")
}

fn load_specs(root: &Path) -> Result<Vec<(PathBuf, SpecDef)>> {
    let mut out = Vec::new();
    for dir in ["spec/tla/router", "spec/tla/selftest"] {
        let dir = root.join(dir);
        let file = dir.join("specs.toml");
        if file.exists() {
            let parsed: SpecsFile = toml::from_str(&std::fs::read_to_string(&file)?)
                .with_context(|| format!("parsing {}", file.display()))?;
            out.extend(parsed.spec.into_iter().map(|s| (dir.clone(), s)));
        }
    }
    Ok(out)
}

fn send_failure(tx: &Tx, suite: &str, spec: &str, variant: &str, msg: &str) {
    let _ = tx.send(Event::Run(RunEvent::RunStarted {
        suite: suite.into(),
        spec: spec.into(),
        variant: variant.into(),
        expect: "ok".into(),
    }));
    eprintln!("loams-specview: {spec} {variant}: {msg}");
    let _ = tx.send(Event::Run(RunEvent::Result {
        actual: format!("error: {msg}"),
        expect: "ok".into(),
        states: 0,
        secs: 0.0,
        pass: false,
    }));
}

async fn run_tla(cfg: &RunConfig, tx: &Tx) -> bool {
    let specs = match load_specs(&cfg.root) {
        Ok(s) => s,
        Err(e) => {
            send_failure(tx, "tla", "specs", "specs.toml", &format!("{e:#}"));
            return false;
        }
    };
    // `check.sh` downloads the pinned tools and verifies their checksums.
    let check = cfg.root.join("scripts/spec/check.sh");
    let ready = Command::new(&check)
        .args(["Selftest", "--parse-only"])
        .current_dir(&cfg.root)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await;
    match ready {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            let msg = format!(
                "check.sh could not provide the pinned tools: {}",
                String::from_utf8_lossy(&o.stderr).trim()
            );
            send_failure(tx, "tla", "tools", "scripts/spec/check.sh", &msg);
            return false;
        }
        Err(e) => {
            send_failure(tx, "tla", "tools", "scripts/spec/check.sh", &format!("{e}"));
            return false;
        }
    }
    let jar = match tla2tools(&cfg.root) {
        Ok(j) => j,
        Err(e) => {
            send_failure(tx, "tla", "tools", "tools.lock", &format!("{e:#}"));
            return false;
        }
    };

    let mut all_ok = true;
    for (dir, spec) in &specs {
        if !cfg.only.is_empty() && !cfg.only.iter().any(|n| n == &spec.name) {
            continue;
        }
        if spec.parse_only {
            continue;
        }
        for v in &spec.variant {
            if !(if cfg.nightly { v.nightly } else { v.pr }) {
                continue;
            }
            all_ok &= run_tlc(&jar, dir, spec, v, tx).await;
            if cfg.apalache {
                for inv in &v.apalache {
                    all_ok &= run_apalache(&cfg.root, dir, spec, v, inv, tx).await;
                }
            }
        }
    }
    all_ok
}

async fn run_tlc(jar: &Path, dir: &Path, spec: &SpecDef, v: &VariantDef, tx: &Tx) -> bool {
    let started = Instant::now();
    let _ = tx.send(Event::Run(RunEvent::RunStarted {
        suite: "tla".into(),
        spec: spec.name.clone(),
        variant: v.cfg.clone(),
        expect: v.expect.clone(),
    }));
    let outcome = tlc_child(jar, dir, &spec.model, &v.cfg, tx).await;
    let (actual, distinct) = match outcome {
        Ok((o, d)) => (o.label(), d),
        Err(e) => (format!("error: {e:#}"), 0),
    };
    let pass = actual == v.expect;
    let _ = tx.send(Event::Run(RunEvent::Result {
        actual,
        expect: v.expect.clone(),
        states: distinct,
        secs: started.elapsed().as_secs_f64(),
        pass,
    }));
    pass
}

async fn tlc_child(
    jar: &Path,
    dir: &Path,
    model: &str,
    cfg: &str,
    tx: &Tx,
) -> Result<(Outcome, u64)> {
    // Under the tools cache, not /tmp: nightly state queues can be large.
    let runs = cache_dir().join("runs");
    std::fs::create_dir_all(&runs)?;
    let meta = tempfile::tempdir_in(&runs)?;
    let mut child = Command::new("java")
        .args(["-XX:+UseParallelGC"])
        .arg(format!("-Djava.io.tmpdir={}", meta.path().display()))
        .arg("-cp")
        .arg(jar)
        .args([
            "tlc2.TLC",
            "-workers",
            &std::env::var("TLC_WORKERS").unwrap_or_else(|_| "2".into()),
        ])
        .args(["-deadlock", "-metadir"])
        .arg(meta.path())
        .args(["-config", cfg, model])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("starting java (Java 21+ is required)")?;
    let mut lines = BufReader::new(child.stdout.take().expect("piped")).lines();
    let mut parser = TlcParser::new();
    let mut tail: std::collections::VecDeque<String> = Default::default();
    while let Some(line) = lines.next_line().await? {
        if !line.trim().is_empty() {
            if tail.len() == 4 {
                tail.pop_front();
            }
            tail.push_back(line.chars().take(200).collect());
        }
        for e in parser.push_line(&line) {
            let _ = tx.send(Event::Run(e));
        }
    }
    for e in parser.finish() {
        let _ = tx.send(Event::Run(e));
    }
    let status = child.wait().await?;
    // "No error" is only believed when TLC also exited cleanly.
    if parser.outcome() == Outcome::Ok && !status.success() {
        anyhow::bail!("TLC reported no error but exited with {status}");
    }
    if parser.outcome() == Outcome::Error {
        anyhow::bail!(
            "TLC gave no verdict (exit {status}); last output: {}",
            tail.iter().cloned().collect::<Vec<_>>().join(" | ")
        );
    }
    Ok((parser.outcome(), parser.distinct))
}

async fn run_apalache(
    root: &Path,
    dir: &Path,
    spec: &SpecDef,
    v: &VariantDef,
    inv: &str,
    tx: &Tx,
) -> bool {
    let started = Instant::now();
    let variant = format!("{} apalache {inv}", v.cfg);
    let _ = tx.send(Event::Run(RunEvent::RunStarted {
        suite: "apalache".into(),
        spec: spec.name.clone(),
        variant,
        expect: "ok".into(),
    }));
    let actual = async {
        let bin = apalache_bin(root)?;
        let out = tempfile::tempdir_in({
            let p = cache_dir().join("runs");
            std::fs::create_dir_all(&p)?;
            p
        })?;
        let o = Command::new(bin)
            .arg("check")
            .arg(format!("--out-dir={}", out.path().display()))
            .arg(format!(
                "--cinit={}",
                v.apalache_cinit.as_deref().unwrap_or("CInit")
            ))
            .args(["--init=Init", "--next=Next"])
            .arg(format!("--inv={inv}"))
            .arg(format!("--length={}", v.apalache_length.unwrap_or(8)))
            .arg(&spec.model)
            .current_dir(dir)
            .kill_on_drop(true)
            .output()
            .await?;
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        anyhow::Ok(if text.contains("The outcome is: NoError") {
            "ok".to_owned()
        } else if text.contains("The outcome is: Error") {
            format!("violation:{inv}")
        } else {
            "error".to_owned()
        })
    }
    .await
    .unwrap_or_else(|e| format!("error: {e:#}"));
    let pass = actual == "ok";
    let _ = tx.send(Event::Run(RunEvent::Result {
        actual,
        expect: "ok".into(),
        states: 0,
        secs: started.elapsed().as_secs_f64(),
        pass,
    }));
    pass
}

async fn has_nextest(root: &Path) -> bool {
    Command::new("cargo")
        .args(["nextest", "--version"])
        .current_dir(root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|s| s.success())
}

async fn run_rust(cfg: &RunConfig, tx: &Tx) -> bool {
    let nextest = has_nextest(&cfg.root).await;
    let mut cmd = Command::new("cargo");
    if nextest {
        cmd.args([
            "nextest",
            "run",
            "--no-fail-fast",
            "--message-format",
            "libtest-json",
        ])
        .env("NEXTEST_EXPERIMENTAL_LIBTEST_JSON", "1");
    } else {
        cmd.args(["test", "--no-fail-fast"]);
    }
    for p in &cfg.rust_packages {
        cmd.args(["-p", p]);
    }
    let child = cmd
        .current_dir(&cfg.root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn();
    let mut child = match child {
        Ok(c) => c,
        Err(e) => {
            eprintln!("loams-specview: cannot start cargo: {e}");
            let _ = tx.send(Event::Test(TestEvent::Failed {
                name: "cargo (could not start)".into(),
                secs: 0.0,
                output: e.to_string(),
            }));
            return false;
        }
    };
    // cargo's own output (compile errors) goes to stderr: keep its tail.
    let mut stderr = BufReader::new(child.stderr.take().expect("piped"));
    let err_tail = tokio::spawn(async move {
        let mut buf = String::new();
        let mut line = String::new();
        while stderr.read_line(&mut line).await.is_ok_and(|n| n > 0) {
            buf.push_str(&line);
            line.clear();
            if buf.len() > 16_000 {
                buf.drain(..buf.len() - 8_000);
            }
        }
        buf
    });
    let mut lines = BufReader::new(child.stdout.take().expect("piped")).lines();
    let mut parser = TestParser::new();
    let mut seen = 0usize;
    while let Ok(Some(line)) = lines.next_line().await {
        for e in parser.push_line(&line) {
            seen += 1;
            let _ = tx.send(Event::Test(e));
        }
    }
    for e in parser.finish() {
        seen += 1;
        let _ = tx.send(Event::Test(e));
    }
    let ok = child.wait().await.is_ok_and(|s| s.success());
    if !ok && seen == 0 {
        let output = err_tail.await.unwrap_or_default();
        let _ = tx.send(Event::Test(TestEvent::Failed {
            name: "cargo (did not run tests)".into(),
            secs: 0.0,
            output,
        }));
    }
    ok
}
