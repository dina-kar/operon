//! Generates the app protos' messages, service traits and clients from
//! `proto/loams/{instance,devices,approvals,operations,notifications,errors}/v1`
//! at the workspace root (design §37 §8.2), with connect-rust's code
//! generator and the system `protoc`.

fn main() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let files: Vec<String> = [
        "errors/v1/errors",
        "instance/v1/instance",
        "devices/v1/devices",
        "approvals/v1/approvals",
        "operations/v1/operations",
        "notifications/v1/notifications",
    ]
    .iter()
    .map(|name| format!("{root}/loams/{name}.proto"))
    .collect();
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }
    if let Err(err) = connectrpc_build::Config::new()
        .files(&files)
        .includes(&[root])
        .include_file("_connectrpc.rs")
        .gate_client_feature(false)
        .compile()
    {
        panic!("generating the app proto code failed (is protoc installed?): {err:#}");
    }
}
