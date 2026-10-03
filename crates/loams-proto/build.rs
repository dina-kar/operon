//! Generates the public `loams.*.v1` packages of the unified Connect API
//! (design §44 §8) from `proto/` at the workspace root, with connect-rust's
//! code generator and the system `protoc`, and writes their descriptor set
//! for `grpc.reflection.v1`.

/// The proto files, relative to `proto/`. Every package served on the main
/// port is listed here (API1 Tasks 1-7 add theirs).
const FILES: &[&str] = &[
    "loams/options/v1/options.proto",
    "loams/errors/v1/errors.proto",
    "loams/instance/v1/instance.proto",
    "loams/live/v1/live.proto",
];

fn main() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let files: Vec<String> = FILES.iter().map(|f| format!("{root}/{f}")).collect();
    for file in &files {
        println!("cargo:rerun-if-changed={file}");
    }
    if let Err(err) = connectrpc_build::Config::new()
        .files(&files)
        .includes(&[root])
        .include_file("_connectrpc.rs")
        .emit_descriptor_set("loams_api.fds.bin")
        .gate_client_feature(true)
        .compile()
    {
        panic!("generating the loams API code failed (is protoc installed?): {err:#}");
    }
}
