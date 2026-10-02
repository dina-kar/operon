//! Headless server tests: the SSE stream of a real TLC run, and of a replay.

use loams_specview::event::{Event, RunEvent};
use loams_specview::runner::RunConfig;
use loams_specview::server::{ServeConfig, Source, start};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn config(source: Source) -> ServeConfig {
    ServeConfig {
        port: 0,
        source,
        record: None,
        dist: repo_root().join("crates/loams-specview/no-such-dist"),
        autorun: true,
    }
}

/// GET /api/events and collect events until `stop` says so (or 120 s pass).
async fn read_sse(addr: std::net::SocketAddr, stop: impl Fn(&Event) -> bool) -> Vec<Event> {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /api/events HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .unwrap();
    let mut lines = BufReader::new(s).lines();
    let mut out = Vec::new();
    let read = async {
        while let Some(line) = lines.next_line().await.unwrap() {
            if let Some(data) = line.strip_prefix("data:") {
                if data.trim() == "reset" {
                    continue;
                }
                let e = Event::from_line(data.trim()).expect("a valid event");
                let done = stop(&e);
                out.push(e);
                if done {
                    return;
                }
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(120), read)
        .await
        .expect("the stream finished in time");
    out
}

#[tokio::test]
#[ignore = "runs TLC: needs Java 21 and the pinned tools (cargo test -- --include-ignored)"]
async fn selftest_streams_run_started_then_result() {
    let cfg = RunConfig {
        root: repo_root(),
        tla: true,
        only: vec!["Selftest".into()],
        nightly: false,
        apalache: false,
        rust_packages: Vec::new(),
    };
    let started = start(config(Source::Suite(cfg))).await.unwrap();
    assert!(started.addr.ip().is_loopback());
    let events = read_sse(started.addr, |e| matches!(e, Event::Done { .. })).await;

    let run: Vec<&RunEvent> = events
        .iter()
        .filter_map(|e| if let Event::Run(r) = e { Some(r) } else { None })
        .collect();
    assert!(
        matches!(run.first(), Some(RunEvent::RunStarted { spec, .. }) if spec == "Selftest"),
        "{run:?}"
    );
    let results: Vec<_> = run
        .iter()
        .filter(|r| matches!(r, RunEvent::Result { .. }))
        .collect();
    assert!(!results.is_empty(), "{run:?}");
    for r in &results {
        assert!(matches!(r, RunEvent::Result { pass: true, .. }), "{r:?}");
    }
    // The Violation variant carries a counterexample trace.
    assert!(
        run.iter().any(|r| matches!(r, RunEvent::State { .. })),
        "no trace in {run:?}"
    );
    assert!(matches!(
        events.last(),
        Some(Event::Done { pass: true, .. })
    ));
    started.task.abort();
}

#[tokio::test]
async fn replay_serves_the_recorded_events_and_records_them() {
    let recorded = vec![
        Event::Run(RunEvent::RunStarted {
            suite: "tla".into(),
            spec: "Selftest".into(),
            variant: "v.cfg".into(),
            expect: "ok".into(),
        }),
        Event::Run(RunEvent::Result {
            actual: "ok".into(),
            expect: "ok".into(),
            states: 3,
            secs: 0.1,
            pass: true,
        }),
        Event::Done {
            pass: true,
            secs: 0.1,
        },
    ];
    let file =
        std::env::temp_dir().join(format!("loams-specview-test-{}.jsonl", std::process::id()));
    let mut cfg = config(Source::Replay {
        events: Arc::new(recorded.clone()),
        delay: Duration::from_millis(1),
    });
    cfg.record = Some(file.clone());
    let started = start(cfg).await.unwrap();
    let got = read_sse(started.addr, |e| matches!(e, Event::Done { .. })).await;
    assert_eq!(got, recorded);
    // A second subscriber after the run still gets the whole history.
    let again = read_sse(started.addr, |e| matches!(e, Event::Done { .. })).await;
    assert_eq!(again, recorded);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let saved: Vec<Event> = std::fs::read_to_string(&file)
        .unwrap()
        .lines()
        .map(|l| Event::from_line(l).unwrap())
        .collect();
    assert_eq!(saved, recorded);
    std::fs::remove_file(&file).ok();
    started.task.abort();
}
