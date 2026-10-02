//! Declares the app's commands in the app manifest, so each one needs an
//! explicit grant in `capabilities/main.json` (AP1 Task 4, D430).

fn main() {
    tauri_build::try_build(tauri_build::Attributes::new().app_manifest(
        tauri_build::AppManifest::new().commands(&[
            "sidecar_status",
            "sidecar_start",
            "sidecar_stop",
            "sidecar_restart",
            "net_fetch",
            "net_abort",
            "envs_list",
            "envs_active",
            "envs_select",
            "auth_sign_in",
            "auth_sign_out",
            "auth_status",
            "deeplink_take",
        ]),
    ))
    .expect("tauri-build failed");
}
