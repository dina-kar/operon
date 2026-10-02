//! Loams Desktop (design §37 §6, plan AP1).
//!
//! One window loads the bundled cordis console with the `desktop` plugin
//! set. The Rust host owns everything privileged and exposes it only as the
//! typed commands below, each granted by name in `capabilities/main.json`:
//!
//! | Module | What |
//! |---|---|
//! | [`sidecar`] | The bundled `loams` on 127.0.0.1 with an OS-assigned port, a readiness handshake and supervision |
//! | [`cli`] | The `loams --output json` contract (D283), for CLI1's stacks |
//! | [`net`] | The fetch bridge: origin allowlist, bearer injection, no tokens in JavaScript |
//! | [`envs`] | The environments the console can talk to |
//! | [`auth`] | Authentik sign-in: system browser, PKCE, loopback redirect, RFC 8693 exchange |
//! | [`keychain`] | Refresh tokens in the OS keychain (`keyring`) |
//! | [`deeplink`] | `loams://` links, navigation only |

pub mod auth;
pub mod cli;
pub mod deeplink;
pub mod envs;
pub mod keychain;
pub mod net;
pub mod sidecar;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use tauri::ipc::Channel;
use tauri::{AppHandle, Emitter as _, Manager as _, RunEvent, State};

use crate::deeplink::DeepLink;
use crate::envs::{EnvKind, Environment, Envs};
use crate::keychain::KeyStore;
use crate::net::{FetchEvent, FetchRequest, Net};
use crate::sidecar::{SidecarConfig, SidecarState, Supervisor};

/// Everything the commands share.
pub struct AppState {
    pub supervisor: Supervisor,
    pub envs: Mutex<Envs>,
    pub net: Net,
    pub keystore: Box<dyn KeyStore>,
    pub http: reqwest::Client,
    inflight: Mutex<HashMap<u64, tokio::task::AbortHandle>>,
    next_request: AtomicU64,
    pending_link: Mutex<Option<DeepLink>>,
    signed_in: Mutex<HashMap<String, auth::SignedIn>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An environment as the console sees it: no tokens, ever.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EnvView {
    pub id: String,
    pub name: String,
    pub kind: EnvKind,
    pub base_url: Option<String>,
    pub active: bool,
    pub signed_in: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SidecarStatus {
    #[serde(flatten)]
    pub state: SidecarState,
    pub log_tail: Vec<String>,
    pub binary: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthStatus {
    pub signed_in: bool,
    pub persistent: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DeepLinkView {
    #[serde(flatten)]
    pub link: DeepLink,
    pub route: String,
}

fn view(env: &Environment, active: Option<&str>, net: &Net) -> EnvView {
    EnvView {
        id: env.id.clone(),
        name: env.name.clone(),
        kind: env.kind,
        base_url: env.base_url.as_ref().map(ToString::to_string),
        active: active == Some(env.id.as_str()),
        signed_in: net.has_token(&env.id),
    }
}

impl AppState {
    fn env_views(&self) -> Vec<EnvView> {
        let envs = lock(&self.envs);
        let active = envs.active().map(|e| e.id.clone());
        envs.list()
            .iter()
            .map(|e| view(e, active.as_deref(), &self.net))
            .collect()
    }

    fn active_view(&self) -> Option<EnvView> {
        self.env_views().into_iter().find(|e| e.active)
    }

    async fn sidecar_status(&self, binary: Option<String>) -> SidecarStatus {
        SidecarStatus {
            state: self.supervisor.state(),
            log_tail: self.supervisor.log_tail(40),
            binary,
        }
    }

    /// After the sidecar starts: record its URL; make it active if nothing is.
    async fn local_ready(&self) {
        let url = self.supervisor.url().await;
        let mut envs = lock(&self.envs);
        envs.set_local(url.clone());
        if envs.active().is_none() && url.is_some() {
            let _ = envs.select(envs::LOCAL);
        }
    }
}

type CmdResult<T> = Result<T, String>;

fn sidecar_binary_label(state: &AppState) -> Option<String> {
    let _ = state;
    std::env::var("LOAMS_DESKTOP_SIDECAR").ok()
}

#[tauri::command]
async fn sidecar_status(state: State<'_, AppState>) -> CmdResult<SidecarStatus> {
    Ok(state.sidecar_status(sidecar_binary_label(&state)).await)
}

#[tauri::command]
async fn sidecar_start(state: State<'_, AppState>) -> CmdResult<SidecarStatus> {
    let result = state.supervisor.start().await;
    state.local_ready().await;
    result.map_err(|e| e.to_string())?;
    Ok(state.sidecar_status(sidecar_binary_label(&state)).await)
}

#[tauri::command]
async fn sidecar_stop(state: State<'_, AppState>) -> CmdResult<SidecarStatus> {
    state.supervisor.stop().await;
    state.local_ready().await;
    Ok(state.sidecar_status(sidecar_binary_label(&state)).await)
}

#[tauri::command]
async fn sidecar_restart(state: State<'_, AppState>) -> CmdResult<SidecarStatus> {
    let result = state.supervisor.restart().await;
    state.local_ready().await;
    result.map_err(|e| e.to_string())?;
    Ok(state.sidecar_status(sidecar_binary_label(&state)).await)
}

/// Starts a request; its events stream over `on_event`. Returns an id for
/// `net_abort`.
#[tauri::command]
async fn net_fetch(
    request: FetchRequest,
    on_event: Channel<FetchEvent>,
    app: AppHandle,
    state: State<'_, AppState>,
) -> CmdResult<u64> {
    let env = lock(&state.envs)
        .active()
        .cloned()
        .ok_or_else(|| net::NetError::NoEnvironment.to_string())?;
    let id = state.next_request.fetch_add(1, Ordering::Relaxed);
    let task = tokio::spawn(async move {
        let state = app.state::<AppState>();
        let sink = |event: FetchEvent| {
            let _ = on_event.send(event);
        };
        if let Err(err) = state.net.perform(&env, request, sink).await {
            let _ = on_event.send(FetchEvent::Error {
                message: err.to_string(),
            });
        }
        lock(&state.inflight).remove(&id);
    });
    lock(&state.inflight).insert(id, task.abort_handle());
    Ok(id)
}

#[tauri::command]
fn net_abort(id: u64, state: State<'_, AppState>) {
    if let Some(handle) = lock(&state.inflight).remove(&id) {
        handle.abort();
    }
}

#[tauri::command]
fn envs_list(state: State<'_, AppState>) -> Vec<EnvView> {
    state.env_views()
}

#[tauri::command]
fn envs_active(state: State<'_, AppState>) -> Option<EnvView> {
    state.active_view()
}

#[tauri::command]
fn envs_select(id: String, state: State<'_, AppState>) -> CmdResult<EnvView> {
    lock(&state.envs).select(&id).map_err(|e| e.to_string())?;
    state
        .active_view()
        .ok_or_else(|| "no active environment".into())
}

/// Signs in to a remote environment in the system browser. Returns whether
/// the sign-in persists; never a token.
#[tauri::command]
async fn auth_sign_in(
    env_id: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> CmdResult<auth::SignedIn> {
    use tauri_plugin_opener::OpenerExt as _;
    let env = lock(&state.envs)
        .get(&env_id)
        .cloned()
        .ok_or("unknown environment")?;
    if env.kind != EnvKind::Remote {
        return Err("local environments need no sign-in before the auth plan (D111)".into());
    }
    let base = env
        .base_url
        .clone()
        .ok_or("the environment has no address")?;
    let opener = app.clone();
    let (signed_in, tokens) = auth::sign_in(
        &state.http,
        &base,
        "me",
        state.keystore.as_ref(),
        move |url| {
            let _ = opener.opener().open_url(url.as_str(), None::<&str>);
        },
        false,
        auth::SIGN_IN_TIMEOUT,
    )
    .await
    .map_err(|e| e.to_string())?;
    state.net.set_token(&env.id, Some(tokens.access_token));
    lock(&state.signed_in).insert(env.id.clone(), signed_in.clone());
    Ok(signed_in)
}

#[tauri::command]
fn auth_sign_out(env_id: String, state: State<'_, AppState>) -> CmdResult<AuthStatus> {
    state.net.set_token(&env_id, None);
    if let Some(signed) = lock(&state.signed_in).remove(&env_id) {
        state
            .keystore
            .delete(&keychain::account(&signed.instance_id, "me"))
            .map_err(|e| e.to_string())?;
    }
    Ok(AuthStatus {
        signed_in: false,
        persistent: state.keystore.persistent(),
    })
}

#[tauri::command]
fn auth_status(env_id: String, state: State<'_, AppState>) -> AuthStatus {
    AuthStatus {
        signed_in: state.net.has_token(&env_id),
        persistent: state.keystore.persistent(),
    }
}

/// The deep link the app was opened with, once.
#[tauri::command]
fn deeplink_take(state: State<'_, AppState>) -> Option<DeepLinkView> {
    lock(&state.pending_link).take().map(|link| DeepLinkView {
        route: link.route(),
        link,
    })
}

/// Parses incoming `loams://` URLs and tells the console where to go.
fn handle_links(app: &AppHandle, urls: impl IntoIterator<Item = String>) {
    for raw in urls {
        match deeplink::parse(&raw) {
            Ok(link) => {
                let view = DeepLinkView {
                    route: link.route(),
                    link: link.clone(),
                };
                *lock(&app.state::<AppState>().pending_link) = Some(link);
                let _ = app.emit("desktop/deeplink", view);
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.set_focus();
                }
            }
            Err(err) => eprintln!("loams desktop: ignored a deep link ({err})"),
        }
    }
}

/// The `loams` binary: `LOAMS_DESKTOP_SIDECAR`, else the bundled sidecar
/// next to the executable (Tauri's `externalBin`), else `loams` on PATH.
fn sidecar_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("LOAMS_DESKTOP_SIDECAR") {
        return Some(PathBuf::from(path));
    }
    let name = if cfg!(windows) { "loams.exe" } else { "loams" };
    let bundled = std::env::current_exe().ok()?.parent()?.join(name);
    if bundled.is_file() {
        return Some(bundled);
    }
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|p| p.join(name))
            .find(|p| p.is_file())
    })
}

fn loams_home(app: &AppHandle) -> PathBuf {
    std::env::var_os("LOAMS_HOME").map_or_else(
        || app.path().home_dir().unwrap_or_default().join(".loams"),
        PathBuf::from,
    )
}

fn supervisor_for(app: &AppHandle) -> Supervisor {
    if cfg!(windows) && std::env::var_os("LOAMS_DESKTOP_SIDECAR").is_none() {
        // Windows has no server variant yet: remote-only (Ruling 11, Q437).
        return Supervisor::unavailable(
            "Loams Desktop on Windows is remote-only: there is no Windows loams server yet",
        );
    }
    match sidecar_binary() {
        Some(binary) => Supervisor::new(SidecarConfig::loams_dev(
            binary,
            loams_home(app).join("stacks").join("desktop").join("data"),
        )),
        None => Supervisor::unavailable("no loams binary is bundled or on PATH"),
    }
}

/// Runs the app.
///
/// # Panics
///
/// If Tauri cannot start (no webview runtime).
pub fn run() {
    let builder = tauri::Builder::default()
        // First, so a second launch hands its arguments (deep links) to this one.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_opener::init());
    // TODO(owner, Q282): the updater needs its own signing key and the
    // release channel (`tauri.updater.conf.json`); see docs/guides/desktop.md.
    #[cfg(feature = "updater")]
    let builder = builder.plugin(tauri_plugin_updater::Builder::new().build());

    let app = builder
        .setup(|app| {
            use tauri_plugin_deep_link::DeepLinkExt as _;
            let handle = app.handle().clone();
            let net = Net::new().map_err(|e| e.to_string())?;
            let http = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| e.to_string())?;
            let envs = Envs::new(std::env::var("LOAMS_DESKTOP_REMOTE").ok().as_deref())
                .map_err(|e| e.to_string())?;
            app.manage(AppState {
                supervisor: supervisor_for(&handle),
                envs: Mutex::new(envs),
                net,
                keystore: keychain::best_available(),
                http,
                inflight: Mutex::new(HashMap::new()),
                next_request: AtomicU64::new(1),
                pending_link: Mutex::new(None),
                signed_in: Mutex::new(HashMap::new()),
            });

            #[cfg(any(windows, target_os = "linux"))]
            {
                // Development builds register the scheme at run time;
                // installers register it at install (macOS only at install).
                let _ = app.deep_link().register_all();
            }
            if let Ok(Some(urls)) = app.deep_link().get_current() {
                handle_links(&handle, urls.into_iter().map(|u| u.to_string()));
            }
            let links = handle.clone();
            app.deep_link().on_open_url(move |event| {
                handle_links(&links, event.urls().into_iter().map(|u| u.to_string()));
            });

            if std::env::var("LOAMS_DESKTOP_AUTOSTART").as_deref() != Ok("0") {
                let start = handle.clone();
                tauri::async_runtime::spawn(async move {
                    let state = start.state::<AppState>();
                    if !matches!(state.supervisor.state(), SidecarState::Unavailable { .. }) {
                        let _ = state.supervisor.start().await;
                    }
                    state.local_ready().await;
                    let _ = start.emit("desktop/sidecar", state.supervisor.state());
                });
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            sidecar_status,
            sidecar_start,
            sidecar_stop,
            sidecar_restart,
            net_fetch,
            net_abort,
            envs_list,
            envs_active,
            envs_select,
            auth_sign_in,
            auth_sign_out,
            auth_status,
            deeplink_take,
        ])
        .build(tauri::generate_context!())
        .expect("failed to build Loams Desktop");

    app.run(|handle, event| {
        if let RunEvent::Exit = event {
            // Until CLI1's stacks, the desktop's sidecar is the app's own
            // child and stops with it; CLI stacks will outlive the app
            // (Ruling 4).
            let supervisor = handle.state::<AppState>().supervisor.clone();
            tauri::async_runtime::block_on(supervisor.stop());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_command_returns_a_secret() {
        // The canary (AP1 Task 7): with a token held for every environment,
        // nothing a command returns contains it.
        let net = Net::new().unwrap();
        let envs = Envs::new(Some("https://loams.example/")).unwrap();
        for env in envs.list() {
            net.set_token(&env.id, Some(format!("canary-token-{}", env.id)));
        }
        let views: Vec<EnvView> = envs
            .list()
            .iter()
            .map(|e| view(e, Some("remote"), &net))
            .collect();
        let outputs = [
            serde_json::to_string(&views).unwrap(),
            serde_json::to_string(&AuthStatus {
                signed_in: true,
                persistent: true,
            })
            .unwrap(),
            serde_json::to_string(&auth::SignedIn {
                instance_id: "01J9".into(),
                persistent: false,
            })
            .unwrap(),
            serde_json::to_string(&SidecarStatus {
                state: SidecarState::Running {
                    url: "http://127.0.0.1:1/".into(),
                    pid: Some(1),
                },
                log_tail: vec![],
                binary: None,
            })
            .unwrap(),
        ];
        for output in outputs {
            assert!(!output.contains("canary-token"), "{output}");
        }
        assert!(views.iter().all(|v| v.signed_in));
    }

    #[test]
    fn deep_link_views_carry_routes() {
        let link = deeplink::parse("loams://approvals/apr_1").unwrap();
        let view = DeepLinkView {
            route: link.route(),
            link,
        };
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            serde_json::json!({"kind": "approval", "id": "apr_1", "route": "/approvals"})
        );
    }
}
