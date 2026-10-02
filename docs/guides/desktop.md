# Loams Desktop

Loams Desktop is the Loams console in a [Tauri 2](https://v2.tauri.app) window, plus what only a desktop app can do: run a local `loams` server, keep credentials in the OS keychain, and open `loams://` links (design [§37 §6](../design/37-desktop-and-mobile-apps.md), plan [AP1](../plans/2026-10-01-ap1-desktop-tauri.md)). This is the scaffold: the pieces below work and are tested; the rest of AP1 is listed at the end.

## Run it from source

### Linux (Debian, Ubuntu; Fedora and Arch have the same packages under their names)

```bash
# Once: Rust (rustup), Node 22+, pnpm 11 (corepack enable), protoc, and the webview.
sudo apt install libwebkit2gtk-4.1-dev libayatana-appindicator3-dev librsvg2-dev protobuf-compiler

cd web && pnpm install && cd ..

# The sidecar: build the engine once and copy it where Tauri expects it.
cargo build -p loams
node scripts/desktop/prepare-sidecar.mjs --from-path <cargo target dir>/debug/loams
#   the target dir is ./target, or /mnt/Projects/rust-cache/target on the shared dev machine
# (or: node scripts/desktop/prepare-sidecar.mjs --placeholder, to run the app without a server)

cd web/apps/desktop && pnpm tauri dev
```

The app starts the sidecar on `127.0.0.1` with a port the OS picks, waits for `GET /ready`, and shows it under **This computer**. `LOAMS_DESKTOP_SIDECAR=/path/to/loams` uses another binary; `LOAMS_DESKTOP_AUTOSTART=0` does not start it.

### Windows (10 or 11, x86_64)

```powershell
# Once: Rust (rustup, with the MSVC build tools), Node 22+, pnpm (corepack enable),
# protoc (winget install protobuf). WebView2 ships with Windows 11.
cd web; pnpm install; cd apps\desktop
pnpm tauri dev
```

Windows is **remote-only** for now: there is no Windows `loams` server, so the app bundles no sidecar and **This computer** says so (Q437). It opens in demo mode (an in-app mock).

### The app-protos mock (both platforms)

```bash
cargo run -p loams-apps-mock     # http://127.0.0.1:8084
```

Then **This computer → Environments → loams-apps-mock → Use**: the console reloads and every request now goes through the Rust bridge to the mock (approvals, namespaces, instance status).

### Unsigned builds from CI

Run the **Desktop** workflow by hand (Actions → Desktop → Run workflow), or use the latest run on `main`: the artifacts `loams-desktop-Linux-unsigned` (`.deb`, `.rpm`, `.AppImage`), `loams-desktop-Windows-unsigned` (the NSIS `.exe`; SmartScreen warns, because it is not signed) and `loams-desktop-macOS-unsigned` (`.dmg`; Gatekeeper refuses it until notarization). CI bundles a placeholder instead of a real `loams` on Linux and macOS, so those builds show the sidecar as crashed; use `LOAMS_DESKTOP_SIDECAR`.

### Checks

```bash
cd web/apps/desktop/src-tauri && cargo test          # the supervisor, bridge, sign-in, keychain, deep links
cd web && pnpm test                                  # tauriFetch, the stacks page, the console
node scripts/desktop/check-capabilities.mjs          # the capability allowlist and the CSP
```

## How it fits together

| Piece | Where | What |
|---|---|---|
| The window | `web/apps/desktop` (`@loams/desktop`) | The cordis console (`@loams/console/cordis`) with the `desktop` patch (`catalog/desktop.patch.yml`) and `@loams/platform-tauri` |
| `platform`, `transport` | `web/packages/platform-tauri` | `tauriFetch`: the fetch signature over the `net_fetch` command and a Tauri `Channel`, so connect-es server streams work |
| This computer | `web/plugins/stacks` | The sidecar's state, start/stop/restart, its log tail, and the environment switcher |
| The Rust host | `web/apps/desktop/src-tauri` (`loams-desktop`, its own Cargo workspace) | `sidecar` (supervisor), `net` (bridge), `auth` (Authentik sign-in), `keychain`, `deeplink`, `envs`, `cli` (D283) |
| Lockdown | `capabilities/main.json`, `tauri.conf.json` | One local capability with the app's commands by name and a scoped opener; no shell, fs, http or process permission; a CSP with no remote source. `scripts/desktop/check-capabilities.mjs` fails CI on anything else |

**Tokens never reach JavaScript.** The console's requests go to Rust (`net_fetch`), which allows only the active environment's origin, refuses plain `http` except to loopback, drops `Authorization`, `Cookie` and `Proxy-*` headers set by JavaScript, adds the bearer token itself for remote environments, and follows redirects only within the same origin and scheme. No command returns a token (a canary test checks).

**Sign-in** (remote environments, `LOAMS_DESKTOP_REMOTE=https://…` for now) is OIDC authorization code with PKCE at the instance's Authentik, in the system browser, with a one-shot loopback redirect (`http://127.0.0.1:<port>/callback`); the Authentik token is exchanged at the Loams gateway (RFC 8693) for Loams's tokens, and the refresh token goes to the OS keychain (`keyring`: macOS Keychain, Windows Credential Manager, Linux Secret Service). Without a Secret Service, sign-in lasts the session. The gateway side is the unified auth plan's (Q438); the tests run the whole flow against mocks.

**Deep links** `loams://open/<env>/<path>`, `loams://approvals/<id>` and `loams://stacks/<name>` only navigate; Rust parses them against an allowlist.

## Owner actions (not done; no secrets in the repository)

| Action | Question | Where it plugs in |
|---|---|---|
| Apple Developer ID Application certificate and notarization credentials | Q420 | `bundle.macOS.signingIdentity` (`tauri.macos.conf.json`); secrets `APPLE_*` for the release job in `.github/workflows/desktop.yml` |
| Windows code signing: Azure Artifact Signing (needs organisation validation) or a Key Vault certificate | Q421 | `bundle.windows.signCommand` (`tauri.windows.conf.json`) |
| The updater key (`tauri signer generate`), separate from the CLI's release key, and the `loams.dev/desktop/…` manifest redirect | Q282 | `tauri.updater.conf.json` (`pubkey`, `endpoints`), the `updater` cargo feature, secrets `TAURI_SIGNING_PRIVATE_KEY*` |
| Windows: remote-only, WSL2 stacks, or a Windows server variant | Q437 | `supervisor_for` in `src-tauri/src/lib.rs`; `externalBin` in `tauri.windows.conf.json` |
| Bundle the `standard` `loams` (tens of MB) or download it on first run | Q428 | `scripts/desktop/prepare-sidecar.mjs --from-release` |

## Not built yet (the rest of AP1)

Stacks through CLI1's `loams stack … --output json` (the parser and D283's exit-code table are in `cli.rs`, tested); the tray; `tauri-plugin-log` with rotation; native notifications for approvals (Task 8); pairing a phone (Task 9); sandboxed third-party plugins on the `loams-plugin://` scheme (disabled in the desktop patch); environment profiles saved on disk; token refresh in the background and DPoP (Q438); a Job Object for the sidecar on Windows; WebDriver end-to-end tests; the signed release job.
