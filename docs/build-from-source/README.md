# Building Loams from source

Loams ships **no binaries yet**. The release pipeline that would produce them
(D292) is still a plan, so there is nothing to download on any platform — signed
or unsigned. Building from source is currently the only way to run Loams.

| Platform | Page | Extra prerequisites beyond Linux |
|---|---|---|
| **Linux** | [`README.md`](../../README.md#quick-start) | none beyond `rustup`, `protoc` and patience |
| **Windows** | [`windows.md`](windows.md) | Visual Studio Build Tools (MSVC + Windows SDK), **NASM**, `protoc` for Windows |
| **macOS** | [`macos.md`](macos.md) | Xcode **Command Line Tools** (not full Xcode), `protoc` via Homebrew |

## Before you start

Two things worth knowing, because both are surprising:

1. **There is no desktop or mobile app in this repository.** No Tauri crate
   appears in `Cargo.lock` and no Tauri dependency in `web/pnpm-lock.yaml`; there
   is no `android/`, no `ios/`, and no Xcode or Gradle project. The desktop,
   Android and iOS apps are plans
   ([`docs/plans/2026-10-01-ap1-desktop-tauri.md`](../plans/2026-10-01-ap1-desktop-tauri.md)
   and its siblings) and live in other repositories. **[verified]** by reading
   the manifests.
2. **So a WebView2 or WKWebView requirement does not apply** — not on Windows,
   not on macOS. Those are Tauri's embedded-browser engines and there is no Tauri
   app here to load one. **[verified]** same basis.

What *is* here: the `loams` server binary (`crates/loams`) and the TypeScript
web console (`web/`). Both pages build both.

## Honest marking

Every command on the Windows and macOS pages carries a marker saying how much it
is worth: **[verified]** (it runs in this repository's own CI),
**[verified, platform-pending]** (it is the CI command and it is right for that
platform, but it has never been executed there), or **[unverified]** (reasoned
from the pinned toolchain or a vendor's documented behaviour, never run).

No Windows or macOS machine took part in writing those pages, so nothing there
is presented as tested on those platforms. Where a command could not be
verified it says so rather than guessing — see the `winget` and `lipo` lines in
particular.

## Signing

The three platforms are in three different states, by decision of 2026-10-03
([D620](../design/13-decision-log.md)):

- **Linux** is signed through SignPath once the organisation-level
  configuration exists. Until then every run says loudly that it signed nothing.
- **Windows and macOS are unsigned**, and building from source is the supported
  path. What that means on the user's machine — SmartScreen, Gatekeeper — and
  how to sign a local build, is on each platform's page under *artifacts are
  unsigned*.

[`docs/release/signing.md`](../release/signing.md) is the whole picture,
including what is still blocked on a human with organisation access.

## The commands CI runs

Both platform pages deliberately derive their build and test commands from
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) rather than from
plausibility. Each page also lists the CI jobs that **cannot** run on that
platform — the ones needing a Linux host, Docker, `tiup`, Java or `trunk` — so
it is clear which parts of the suite are not being exercised locally.