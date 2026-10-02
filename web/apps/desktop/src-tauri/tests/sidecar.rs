//! The supervisor against a fake `loams`: this test binary, re-run with
//! `LOAMS_FAKE_SIDECAR` set, plays the server (AP1 Tasks 2–3).

use std::time::Duration;

use loams_desktop_lib::sidecar::{SidecarConfig, SidecarError, SidecarState, Supervisor};

/// When re-run as the fake sidecar, this "test" is the server's main.
#[test]
fn fake_sidecar_entry() {
    let Ok(mode) = std::env::var("LOAMS_FAKE_SIDECAR") else {
        return;
    };
    let listen = std::env::var("LOAMS_FAKE_LISTEN").unwrap();
    println!("fake loams starting ({mode})");
    match mode.as_str() {
        "exit" => {
            eprintln!("fatal: bad config");
            std::process::exit(3);
        }
        "never-ready" => std::thread::sleep(Duration::from_secs(60)),
        _ => {}
    }
    let listener = std::net::TcpListener::bind(&listen).unwrap();
    if mode == "line" {
        println!("loams ready: http://{listen}");
    }
    let started = std::time::Instant::now();
    listener.set_nonblocking(true).unwrap();
    loop {
        if mode == "crash-later" && started.elapsed() > Duration::from_millis(600) {
            eprintln!("panicked: simulated crash");
            std::process::exit(101);
        }
        match listener.accept() {
            Ok((mut socket, _)) => {
                use std::io::{Read as _, Write as _};
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf);
                let ready = mode != "line";
                let status = if ready {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let _ = socket.write_all(
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                        .as_bytes(),
                );
            }
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn config(mode: &str) -> SidecarConfig {
    let exe = std::env::current_exe().unwrap();
    let mut config = SidecarConfig::loams_dev(exe, std::env::temp_dir());
    config.args = [
        "fake_sidecar_entry",
        "--exact",
        "--nocapture",
        "--test-threads=1",
    ]
    .map(String::from)
    .to_vec();
    config.env = vec![
        ("LOAMS_FAKE_SIDECAR".into(), mode.into()),
        ("LOAMS_FAKE_LISTEN".into(), "127.0.0.1:{port}".into()),
    ];
    config.ready_timeout = Duration::from_secs(10);
    config.grace = Duration::from_secs(2);
    config.restart.base = Duration::from_millis(100);
    config
}

#[tokio::test]
async fn starts_on_loopback_and_is_ready_by_probe() {
    let supervisor = Supervisor::new(config("serve"));
    let url = supervisor.start().await.unwrap();
    assert_eq!(url.host_str(), Some("127.0.0.1"));
    assert!(matches!(supervisor.state(), SidecarState::Running { .. }));
    assert!(matches!(
        supervisor.start().await,
        Err(SidecarError::AlreadyRunning)
    ));
    supervisor.stop().await;
    assert_eq!(supervisor.state(), SidecarState::Stopped);
    // The port is free again once the sidecar stopped.
    let port = url.port().unwrap();
    assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_ok());
}

#[tokio::test]
async fn the_readiness_line_is_accepted() {
    // "line" never answers /ready with 200: only the line makes it ready.
    let supervisor = Supervisor::new(config("line"));
    let url = supervisor.start().await.unwrap();
    assert!(url.as_str().starts_with("http://127.0.0.1:"));
    supervisor.stop().await;
}

#[tokio::test]
async fn exiting_before_ready_reports_the_log_tail() {
    let supervisor = Supervisor::new(config("exit"));
    let err = supervisor.start().await.unwrap_err();
    let SidecarError::ExitedEarly { exit, log_tail } = err else {
        panic!("{err:?}")
    };
    assert_eq!(exit, Some(3));
    assert!(
        log_tail.iter().any(|l| l.contains("bad config")),
        "{log_tail:?}"
    );
    assert!(matches!(supervisor.state(), SidecarState::Crashed { .. }));
}

#[tokio::test]
async fn a_slow_server_times_out() {
    let mut cfg = config("never-ready");
    cfg.ready_timeout = Duration::from_millis(500);
    let supervisor = Supervisor::new(cfg);
    assert!(matches!(
        supervisor.start().await,
        Err(SidecarError::Timeout(..))
    ));
}

#[tokio::test]
async fn a_crash_after_ready_is_restarted() {
    let supervisor = Supervisor::new(config("crash-later"));
    let first = supervisor.start().await.unwrap();
    let mut states = supervisor.subscribe();
    // Wait for a restart and a new readiness.
    let restarted = tokio::time::timeout(Duration::from_secs(10), async {
        let mut saw_restarting = false;
        loop {
            states.changed().await.unwrap();
            match states.borrow().clone() {
                SidecarState::Restarting { .. } => saw_restarting = true,
                SidecarState::Running { url, .. } if saw_restarting => return url,
                _ => {}
            }
        }
    })
    .await
    .expect("restarted within 10 s");
    assert_ne!(restarted, first.to_string(), "a fresh OS-assigned port");
    supervisor.stop().await;
}

#[tokio::test]
async fn a_missing_binary_is_reported() {
    let mut cfg = config("serve");
    cfg.binary = "/nonexistent/loams".into();
    let supervisor = Supervisor::new(cfg);
    assert!(matches!(
        supervisor.start().await,
        Err(SidecarError::Missing(_))
    ));
}
