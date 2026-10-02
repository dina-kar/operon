//! The HTTP side: an event hub (history + live), SSE at `/api/events`, a
//! `POST /api/run` to start another run, and the built frontend as static files.
//! Binds 127.0.0.1 only.

use crate::event::Event;
use crate::runner::{RunConfig, run_suite};
use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event as Sse, KeepAlive};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tower_http::services::ServeDir;

/// Where a run's events come from.
#[derive(Clone)]
pub enum Source {
    /// Run the suite as subprocesses.
    Suite(RunConfig),
    /// Replay a recorded run, `delay` between events.
    Replay {
        events: Arc<Vec<Event>>,
        delay: Duration,
    },
}

pub struct ServeConfig {
    pub port: u16,
    pub source: Source,
    /// Save each run's events to this file (JSON lines).
    pub record: Option<PathBuf>,
    /// The built frontend (`trunk build` output).
    pub dist: PathBuf,
    /// Start a run as soon as the server is up.
    pub autorun: bool,
}

#[derive(Default)]
struct HubState {
    /// Bumped when a new run clears the history.
    epoch: u64,
    events: Vec<Event>,
    running: bool,
}

pub struct Hub {
    state: Mutex<HubState>,
    /// Bumped on every change; subscribers wake on it.
    tick: watch::Sender<u64>,
    source: Source,
    record: Option<PathBuf>,
}

impl Hub {
    fn new(source: Source, record: Option<PathBuf>) -> Arc<Hub> {
        Arc::new(Hub {
            state: Mutex::new(HubState::default()),
            tick: watch::channel(0).0,
            source,
            record,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn bump(&self) {
        self.tick.send_modify(|t| *t += 1);
    }

    /// Start a run unless one is in progress. Returns whether it started.
    pub fn start_run(self: &Arc<Self>) -> bool {
        {
            let mut st = self.lock();
            if st.running {
                return false;
            }
            st.running = true;
            st.epoch += 1;
            st.events.clear();
        }
        let mut record = self.record.as_ref().and_then(|p| match create_record(p) {
            Ok(f) => Some(f),
            Err(e) => {
                eprintln!("loams-specview: cannot record to {}: {e}", p.display());
                None
            }
        });
        self.bump();
        let (tx, mut rx) = mpsc::unbounded_channel::<Event>();
        let hub = Arc::clone(self);
        let source = self.source.clone();
        tokio::spawn(async move {
            let producer = tokio::spawn(async move {
                match source {
                    Source::Suite(cfg) => {
                        run_suite(&cfg, &tx).await;
                    }
                    Source::Replay { events, delay } => {
                        for e in events.iter() {
                            if tx.send(e.clone()).is_err() {
                                break;
                            }
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            });
            while let Some(e) = rx.recv().await {
                if let Some(f) = record.as_mut() {
                    let _ = writeln!(f, "{}", e.to_line());
                }
                hub.lock().events.push(e);
                hub.bump();
            }
            let _ = producer.await;
            hub.lock().running = false;
            hub.bump();
        });
        true
    }

    pub fn is_running(&self) -> bool {
        self.lock().running
    }

    /// A snapshot of the current run's events.
    pub fn events(&self) -> Vec<Event> {
        self.lock().events.clone()
    }
}

fn create_record(p: &std::path::Path) -> std::io::Result<std::fs::File> {
    if let Some(dir) = p.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::File::create(p)
}

pub struct Started {
    pub addr: SocketAddr,
    pub hub: Arc<Hub>,
    pub task: tokio::task::JoinHandle<()>,
}

/// Bind 127.0.0.1:`port` (0 picks a free port) and serve.
pub async fn start(cfg: ServeConfig) -> Result<Started> {
    let hub = Hub::new(cfg.source, cfg.record);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", cfg.port))
        .await
        .with_context(|| format!("binding 127.0.0.1:{}", cfg.port))?;
    let addr = listener.local_addr()?;

    let api = Router::new()
        .route("/api/events", get(events))
        .route("/api/run", post(run))
        .route("/api/status", get(status))
        .with_state(Arc::clone(&hub));
    let app = if cfg.dist.join("index.html").exists() {
        api.fallback_service(ServeDir::new(&cfg.dist).append_index_html_on_directories(true))
    } else {
        eprintln!(
            "loams-specview: no frontend at {} (build it: see crates/loams-specview/README.md); serving the raw event stream only",
            cfg.dist.display()
        );
        api.fallback(get(missing_frontend))
    };
    if cfg.autorun {
        hub.start_run();
    }
    let task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("loams-specview: server error: {e}");
        }
    });
    Ok(Started { addr, hub, task })
}

async fn missing_frontend() -> Html<&'static str> {
    Html(
        "<!doctype html><title>loams-specview</title><body style=\"font:16px system-ui;max-width:42rem;margin:3rem auto\">\
         <h1>The frontend is not built</h1>\
         <p>Run <code>trunk build</code> in <code>crates/loams-specview</code>, then reload. \
         The event stream is at <a href=\"/api/events\">/api/events</a>.</p></body>",
    )
}

async fn run(State(hub): State<Arc<Hub>>) -> Response {
    if hub.start_run() {
        StatusCode::ACCEPTED.into_response()
    } else {
        (StatusCode::CONFLICT, "a run is in progress").into_response()
    }
}

async fn status(State(hub): State<Arc<Hub>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "running": hub.is_running() }))
}

/// SSE: every event of the current run from the start, then live ones. An SSE
/// event named `reset` tells the client that a new run replaced the history.
async fn events(State(hub): State<Arc<Hub>>) -> impl IntoResponse {
    struct Cursor {
        hub: Arc<Hub>,
        rx: watch::Receiver<u64>,
        epoch: u64,
        next: usize,
        queue: std::collections::VecDeque<Result<Sse, std::convert::Infallible>>,
    }
    let rx = hub.tick.subscribe();
    let epoch = hub.lock().epoch;
    let cursor = Cursor {
        hub,
        rx,
        epoch,
        next: 0,
        queue: Default::default(),
    };
    let stream = futures::stream::unfold(cursor, |mut c| async move {
        loop {
            if let Some(item) = c.queue.pop_front() {
                return Some((item, c));
            }
            {
                let st = c.hub.lock();
                if st.epoch != c.epoch {
                    c.epoch = st.epoch;
                    c.next = 0;
                    c.queue
                        .push_back(Ok(Sse::default().event("reset").data("reset")));
                }
                for e in st.events.iter().skip(c.next) {
                    c.queue.push_back(Ok(Sse::default().data(e.to_line())));
                }
                c.next = st.events.len();
            }
            if c.queue.is_empty() && c.rx.changed().await.is_err() {
                return None;
            }
        }
    });
    axum::response::Sse::new(stream).keep_alive(KeepAlive::default())
}
