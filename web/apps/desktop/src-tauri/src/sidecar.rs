//! The `loams` sidecar supervisor (§37 §3.1, §6.2; AP1 Tasks 2–3).
//!
//! The harness's host spawned one runtime with `--port 0` and accepted the
//! URL from a stdout line, then stopped watching it. This supervisor keeps
//! the patterns that worked and fixes the gaps (§37 §3.1):
//!
//! - **Loopback only, OS-assigned port.** The port comes from binding
//!   `127.0.0.1:0` (the OS picks it), and the server is told to listen
//!   there. Every URL the supervisor accepts is `http://127.0.0.1:<port>`.
//! - **A readiness handshake**: the stack is ready when `GET /ready` answers
//!   200 on that port, or earlier if the process prints
//!   `loams ready: http://127.0.0.1:<port>` (accepted only for the chosen
//!   port). Exiting, or the timeout, before readiness is an error that
//!   carries the log tail.
//! - **Supervision after readiness**: a crash is noticed and restarted with
//!   backoff (1 s, 2 s, 4 s … 30 s, at most 5 restarts in 10 minutes, AP1
//!   Ruling 5); then the sidecar stays `crashed` with its log tail.
//! - **A clean stop**: on Unix the child runs in its own process group and
//!   gets SIGTERM, then SIGKILL after a grace period; on Windows it is
//!   killed (TODO: a Job Object with KILL_ON_JOB_CLOSE, AP1 Task 2, once a
//!   Windows `loams` exists, Q437).
//!
//! TODO(AP1 Task 2): when CLI1's `loams stack … --output json` lands, local
//! stacks move to the CLI (see `cli.rs`), and this supervisor runs only the
//! desktop's own default stack through `loams stack run`. The engine could
//! then also print the readiness line itself and accept `--listen
//! 127.0.0.1:0`, which removes the window between choosing the port and the
//! server binding it.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use url::Url;

/// The stdout line a sidecar may print once it serves (optional).
pub const READY_PREFIX: &str = "loams ready: ";

/// How the sidecar is started.
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    /// The `loams` binary (bundled, or `LOAMS_DESKTOP_SIDECAR`).
    pub binary: PathBuf,
    /// Arguments; `{port}` is replaced with the chosen port and `{data_dir}`
    /// with `data_dir`.
    pub args: Vec<String>,
    pub data_dir: PathBuf,
    /// Extra environment for the child; values may use `{port}` too.
    pub env: Vec<(String, String)>,
    /// The readiness path polled on the chosen port.
    pub ready_path: String,
    pub ready_timeout: Duration,
    pub poll_interval: Duration,
    /// How long `stop` waits after SIGTERM before SIGKILL.
    pub grace: Duration,
    /// Lines of output kept for the status page and error reports.
    pub log_lines: usize,
    pub restart: RestartPolicy,
}

impl SidecarConfig {
    /// `loams dev` on loopback with only the native API (no Flight SQL,
    /// Qdrant or Elasticsearch listeners, which would take fixed ports).
    #[must_use]
    pub fn loams_dev(binary: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            binary,
            args: [
                "dev",
                "--data-dir",
                "{data_dir}",
                "--listen",
                "127.0.0.1:{port}",
                "--no-flight-sql",
                "--no-qdrant",
                "--no-es",
            ]
            .map(String::from)
            .to_vec(),
            data_dir,
            env: vec![
                ("LOAMS_NO_UPDATE_CHECK".into(), "1".into()),
                ("LOAMS_DESKTOP".into(), "1".into()),
            ],
            ready_path: "/ready".into(),
            ready_timeout: Duration::from_secs(60),
            poll_interval: Duration::from_millis(200),
            grace: Duration::from_secs(10),
            log_lines: 200,
            restart: RestartPolicy::default(),
        }
    }
}

/// What the status page and the tray show.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SidecarState {
    Stopped,
    Starting {
        port: u16,
    },
    Running {
        url: String,
        pid: Option<u32>,
    },
    Restarting {
        attempt: u32,
        delay_ms: u64,
    },
    Crashed {
        exit: Option<i32>,
        reason: String,
    },
    /// No sidecar on this platform (Windows is remote-only, Q437) or no binary.
    Unavailable {
        reason: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    #[error("no loams binary at {0}")]
    Missing(PathBuf),
    #[error("could not start {binary}: {source}")]
    Spawn {
        binary: PathBuf,
        source: std::io::Error,
    },
    #[error("loams exited before it was ready ({exit:?})")]
    ExitedEarly {
        exit: Option<i32>,
        log_tail: Vec<String>,
    },
    #[error("loams was not ready after {0:?}")]
    Timeout(Duration, Vec<String>),
    #[error("could not choose a loopback port: {0}")]
    Port(std::io::Error),
    #[error("the sidecar is already running")]
    AlreadyRunning,
}

/// Restarts after a crash: exponential backoff from `base` to `max`, at most
/// `budget` restarts within `window` (AP1 Ruling 5).
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    pub base: Duration,
    pub max: Duration,
    pub budget: usize,
    pub window: Duration,
    history: VecDeque<Instant>,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(30),
            budget: 5,
            window: Duration::from_secs(600),
            history: VecDeque::new(),
        }
    }
}

impl RestartPolicy {
    /// The delay before the next restart, or `None` once the budget is spent.
    pub fn next_delay(&mut self, now: Instant) -> Option<Duration> {
        while self
            .history
            .front()
            .is_some_and(|t| now.duration_since(*t) > self.window)
        {
            self.history.pop_front();
        }
        if self.history.len() >= self.budget {
            return None;
        }
        let exponent = u32::try_from(self.history.len())
            .unwrap_or(u32::MAX)
            .min(16);
        let delay = self
            .base
            .saturating_mul(2u32.saturating_pow(exponent))
            .min(self.max);
        self.history.push_back(now);
        Some(delay)
    }

    /// Forgets past restarts (after a manual start).
    pub fn reset(&mut self) {
        self.history.clear();
    }
}

/// Lets the OS choose a free loopback port.
///
/// # Errors
///
/// If nothing can bind `127.0.0.1:0`.
pub fn pick_port() -> Result<u16, SidecarError> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).map_err(SidecarError::Port)?;
    Ok(listener.local_addr().map_err(SidecarError::Port)?.port())
}

/// Accepts a readiness URL only if it is `http://127.0.0.1:<port>` for the
/// port the supervisor chose (the harness's loopback check, tightened).
#[must_use]
pub fn ready_url_from_line(line: &str, port: u16) -> Option<Url> {
    let candidate = line
        .trim()
        .strip_prefix(READY_PREFIX)?
        .split_whitespace()
        .next()?;
    let url = Url::parse(candidate).ok()?;
    (url.scheme() == "http"
        && url.host_str() == Some("127.0.0.1")
        && url.port() == Some(port)
        && url.username().is_empty()
        && url.password().is_none())
    .then_some(url)
}

/// A bounded log tail shared between the output readers and the status.
#[derive(Debug, Clone, Default)]
pub struct LogTail(Arc<Mutex<VecDeque<String>>>);

impl LogTail {
    fn push(&self, line: String, cap: usize) {
        let mut lines = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lines.push_back(line);
        while lines.len() > cap {
            lines.pop_front();
        }
    }

    /// The last `n` lines.
    #[must_use]
    pub fn last(&self, n: usize) -> Vec<String> {
        let lines = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lines
            .iter()
            .skip(lines.len().saturating_sub(n))
            .cloned()
            .collect()
    }
}

fn pump<R: AsyncRead + Unpin + Send + 'static>(
    reader: R,
    tail: LogTail,
    cap: usize,
    lines: Option<tokio::sync::mpsc::UnboundedSender<String>>,
) {
    tokio::spawn(async move {
        let mut reader = BufReader::new(reader).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if let Some(tx) = &lines {
                let _ = tx.send(line.clone());
            }
            tail.push(line, cap);
        }
    });
}

/// Polls `GET http://127.0.0.1:<port><path>` once; true on 200.
async fn probe(client: &reqwest::Client, port: u16, path: &str) -> bool {
    let url = format!("http://127.0.0.1:{port}{path}");
    matches!(client.get(url).send().await, Ok(r) if r.status() == reqwest::StatusCode::OK)
}

/// Waits until the child is ready, exits, or the timeout passes.
async fn handshake(
    child: &mut Child,
    port: u16,
    config: &SidecarConfig,
    tail: &LogTail,
    mut lines: tokio::sync::mpsc::UnboundedReceiver<String>,
) -> Result<Url, SidecarError> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|e| SidecarError::Port(std::io::Error::other(e)))?;
    let deadline = tokio::time::Instant::now() + config.ready_timeout;
    let mut tick = tokio::time::interval(config.poll_interval);
    let fallback = Url::parse(&format!("http://127.0.0.1:{port}/"))
        .map_err(|e| SidecarError::Port(std::io::Error::other(e)))?;
    loop {
        tokio::select! {
            status = child.wait() => {
                let exit = status.ok().and_then(|s| s.code());
                // Let the readers drain the last lines.
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(SidecarError::ExitedEarly { exit, log_tail: tail.last(20) });
            }
            Some(line) = lines.recv() => {
                if let Some(url) = ready_url_from_line(&line, port) {
                    return Ok(url);
                }
            }
            _ = tick.tick() => {
                if probe(&client, port, &config.ready_path).await {
                    return Ok(fallback);
                }
            }
            () = tokio::time::sleep_until(deadline) => {
                return Err(SidecarError::Timeout(config.ready_timeout, tail.last(20)));
            }
        }
    }
}

struct Running {
    child: Child,
    url: Url,
}

struct Inner {
    config: SidecarConfig,
    running: Option<Running>,
    /// Set by `stop`, so the monitor does not restart a requested exit.
    stopping: bool,
    generation: u64,
}

/// Supervises one sidecar process.
#[derive(Clone)]
pub struct Supervisor {
    inner: Arc<tokio::sync::Mutex<Inner>>,
    state: Arc<watch::Sender<SidecarState>>,
    tail: LogTail,
}

impl std::fmt::Debug for Supervisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Supervisor")
            .field("state", &*self.state.borrow())
            .finish()
    }
}

impl Supervisor {
    #[must_use]
    pub fn new(config: SidecarConfig) -> Self {
        let (state, _) = watch::channel(SidecarState::Stopped);
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(Inner {
                config,
                running: None,
                stopping: false,
                generation: 0,
            })),
            state: Arc::new(state),
            tail: LogTail::default(),
        }
    }

    /// A supervisor that never starts anything (no sidecar on this platform).
    #[must_use]
    pub fn unavailable(reason: impl Into<String>) -> Self {
        let this = Self::new(SidecarConfig::loams_dev(PathBuf::new(), PathBuf::new()));
        this.state.send_replace(SidecarState::Unavailable {
            reason: reason.into(),
        });
        this
    }

    #[must_use]
    pub fn state(&self) -> SidecarState {
        self.state.borrow().clone()
    }

    /// Subscribe to state changes (the tray and the console).
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<SidecarState> {
        self.state.subscribe()
    }

    #[must_use]
    pub fn log_tail(&self, n: usize) -> Vec<String> {
        self.tail.last(n)
    }

    /// The URL of the running sidecar, if it is ready.
    pub async fn url(&self) -> Option<Url> {
        self.inner
            .lock()
            .await
            .running
            .as_ref()
            .map(|r| r.url.clone())
    }

    /// Starts the sidecar and waits for readiness.
    ///
    /// # Errors
    ///
    /// See [`SidecarError`]; the state becomes `crashed` with the reason.
    pub async fn start(&self) -> Result<Url, SidecarError> {
        if matches!(self.state(), SidecarState::Unavailable { .. }) {
            let SidecarState::Unavailable { reason } = self.state() else {
                unreachable!()
            };
            return Err(SidecarError::Missing(PathBuf::from(reason)));
        }
        let mut inner = self.inner.lock().await;
        inner.config.restart.reset();
        self.spawn_locked(&mut inner).await
    }

    async fn spawn_locked(&self, inner: &mut Inner) -> Result<Url, SidecarError> {
        if inner.running.is_some() {
            return Err(SidecarError::AlreadyRunning);
        }
        let config = inner.config.clone();
        if !config.binary.is_file() {
            let err = SidecarError::Missing(config.binary.clone());
            self.state.send_replace(SidecarState::Crashed {
                exit: None,
                reason: err.to_string(),
            });
            return Err(err);
        }
        let port = pick_port()?;
        self.state.send_replace(SidecarState::Starting { port });
        let data_dir = config.data_dir.display().to_string();
        let args: Vec<String> = config
            .args
            .iter()
            .map(|a| {
                a.replace("{port}", &port.to_string())
                    .replace("{data_dir}", &data_dir)
            })
            .collect();
        let env: Vec<(String, String)> = config
            .env
            .iter()
            .map(|(k, v)| (k.clone(), v.replace("{port}", &port.to_string())))
            .collect();
        let mut command = Command::new(&config.binary);
        command
            .args(&args)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|source| SidecarError::Spawn {
            binary: config.binary.clone(),
            source,
        })?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if let Some(out) = child.stdout.take() {
            pump(out, self.tail.clone(), config.log_lines, Some(tx.clone()));
        }
        if let Some(err) = child.stderr.take() {
            pump(err, self.tail.clone(), config.log_lines, Some(tx));
        }
        match handshake(&mut child, port, &config, &self.tail, rx).await {
            Ok(url) => {
                let pid = child.id();
                inner.generation += 1;
                inner.stopping = false;
                inner.running = Some(Running {
                    child,
                    url: url.clone(),
                });
                self.state.send_replace(SidecarState::Running {
                    url: url.to_string(),
                    pid,
                });
                self.monitor(inner.generation);
                Ok(url)
            }
            Err(err) => {
                let _ = child.start_kill();
                let exit = match &err {
                    SidecarError::ExitedEarly { exit, .. } => *exit,
                    _ => None,
                };
                self.state.send_replace(SidecarState::Crashed {
                    exit,
                    reason: err.to_string(),
                });
                Err(err)
            }
        }
    }

    /// Watches the running child; restarts it with backoff if it crashes.
    fn monitor(&self, generation: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                // Poll the child without holding the lock across the wait.
                tokio::time::sleep(Duration::from_millis(250)).await;
                let mut inner = this.inner.lock().await;
                if inner.generation != generation {
                    return;
                }
                let Some(running) = inner.running.as_mut() else {
                    return;
                };
                let Ok(Some(status)) = running.child.try_wait() else {
                    continue;
                };
                inner.running = None;
                if inner.stopping {
                    this.state.send_replace(SidecarState::Stopped);
                    return;
                }
                let exit = status.code();
                let Some(delay) = inner.config.restart.next_delay(Instant::now()) else {
                    this.state.send_replace(SidecarState::Crashed {
                        exit,
                        reason: "crashed repeatedly; the restart budget is spent".into(),
                    });
                    return;
                };
                let attempt = u32::try_from(inner.config.restart.history.len()).unwrap_or(0);
                this.state.send_replace(SidecarState::Restarting {
                    attempt,
                    delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                });
                drop(inner);
                tokio::time::sleep(delay).await;
                let mut inner = this.inner.lock().await;
                if inner.stopping || inner.generation != generation {
                    return;
                }
                // A failed restart leaves the state `crashed`; the next crash
                // check happens under the new generation's monitor.
                let _ = this.spawn_locked(&mut inner).await;
                return;
            }
        });
    }

    /// Stops the sidecar: SIGTERM to its process group, then SIGKILL after
    /// the grace period (Unix); kill (Windows).
    pub async fn stop(&self) {
        let mut inner = self.inner.lock().await;
        inner.stopping = true;
        let grace = inner.config.grace;
        let Some(mut running) = inner.running.take() else {
            if !matches!(self.state(), SidecarState::Unavailable { .. }) {
                self.state.send_replace(SidecarState::Stopped);
            }
            return;
        };
        terminate(&mut running.child);
        if tokio::time::timeout(grace, running.child.wait())
            .await
            .is_err()
        {
            let _ = running.child.kill().await;
        }
        self.state.send_replace(SidecarState::Stopped);
    }

    /// Stops, then starts again.
    ///
    /// # Errors
    ///
    /// As [`Supervisor::start`].
    pub async fn restart(&self) -> Result<Url, SidecarError> {
        self.stop().await;
        self.start().await
    }
}

#[cfg(unix)]
fn terminate(child: &mut Child) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;
    if let Some(pid) = child.id().and_then(|p| i32::try_from(p).ok()) {
        let _ = killpg(Pid::from_raw(pid), Signal::SIGTERM);
    }
}

#[cfg(not(unix))]
fn terminate(child: &mut Child) {
    // TODO(AP1 Task 2): a Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    // so the whole tree goes; Windows has no bundled loams yet (Q437).
    let _ = child.start_kill();
}

/// The loopback address a ready sidecar serves on.
#[must_use]
pub fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_line_is_accepted_only_for_our_loopback_port() {
        let url = ready_url_from_line("loams ready: http://127.0.0.1:49152", 49152).unwrap();
        assert_eq!(url.as_str(), "http://127.0.0.1:49152/");
        for line in [
            "loams ready: http://127.0.0.1:49153",
            "loams ready: http://0.0.0.0:49152",
            "loams ready: http://localhost:49152",
            "loams ready: https://127.0.0.1:49152",
            "loams ready: http://user@127.0.0.1:49152",
            "noise http://127.0.0.1:49152",
        ] {
            assert!(ready_url_from_line(line, 49152).is_none(), "{line}");
        }
    }

    #[test]
    fn crashed_stack_restarts_with_backoff() {
        let mut policy = RestartPolicy::default();
        let t0 = Instant::now();
        let delays: Vec<_> = (0..5)
            .map(|i| policy.next_delay(t0 + Duration::from_secs(i)))
            .collect();
        assert_eq!(
            delays,
            [1, 2, 4, 8, 16]
                .map(|s| Some(Duration::from_secs(s)))
                .to_vec()
        );
    }

    #[test]
    fn restart_budget_exhausted_stays_crashed() {
        let mut policy = RestartPolicy::default();
        let t0 = Instant::now();
        for i in 0..5 {
            assert!(policy.next_delay(t0 + Duration::from_secs(i)).is_some());
        }
        assert_eq!(policy.next_delay(t0 + Duration::from_secs(6)), None);
        // Outside the 10-minute window the budget comes back.
        assert!(policy.next_delay(t0 + Duration::from_secs(700)).is_some());
    }

    #[test]
    fn backoff_is_capped() {
        let mut policy = RestartPolicy {
            budget: 100,
            ..RestartPolicy::default()
        };
        let t0 = Instant::now();
        let last = (0..10).filter_map(|_| policy.next_delay(t0)).last();
        assert_eq!(last, Some(Duration::from_secs(30)));
    }

    #[test]
    fn picked_ports_are_free_loopback_ports() {
        let port = pick_port().unwrap();
        assert_ne!(port, 0);
        assert!(TcpListener::bind(loopback(port)).is_ok());
    }

    #[test]
    fn log_tail_keeps_the_last_lines() {
        let tail = LogTail::default();
        for i in 0..10 {
            tail.push(format!("line {i}"), 3);
        }
        assert_eq!(tail.last(2), ["line 8", "line 9"]);
        assert_eq!(tail.last(10).len(), 3);
    }
}
