//! Generates the public `loams.*.v1` packages of the unified Connect API
//! (design §44 §7–§8) from `proto/` at the workspace root, with
//! connect-rust's code generator and the system `protoc`, and writes their
//! descriptor set for `grpc.reflection.v1`.
//!
//! API1 Task 1 starts the list with the three packages the server needs
//! before any application service exists: the facade options the SDK
//! generator reads (`loams.options.v1`), the shared error detail and the
//! instance discovery service. Tasks 2–8 append
//! `loams.collection.v1`, `loams.sql.v1`, `loams.link.v1`,
//! `loams.admin.v1`, `loams.auth.v1` and `loams.internal.v1`.
//!
//! `loams.live.v1` is **not** here: R1's `loams-live-proto` already
//! generates it, and a proto package's Rust types are generated exactly
//! once in this workspace. The server reaches the `LiveService` trait through
//! that crate; `loams-proto` is the crate the SDKs of Task 2 onwards build
//! on.

/// The proto files, as paths under the workspace's `proto/` directory.
const FILES: &[&str] = &[
    "loams/options/v1/options.proto",
    "loams/errors/v1/errors.proto",
    "loams/instance/v1/instance.proto",
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
        .emit_descriptor_set("loams_api_descriptor.bin")
        .gate_client_feature(true)
        .compile()
    {
        panic!("generating the loams API code failed (is protoc installed?): {err:#}");
    }
}
