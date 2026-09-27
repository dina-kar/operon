//! Generates the `loam.live.v1` messages, the `LiveService` trait and its
//! client from `proto/loam/live/v1/*.proto` at the workspace root (R1 plan
//! Task 7), with connect-rust's code generator and the system `protoc`.

fn main() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../proto");
    let files: Vec<String> = ["value", "live", "journal", "catalog"]
        .iter()
        .map(|name| format!("{root}/loam/live/v1/{name}.proto"))
        .collect();
    if let Err(err) = connectrpc_build::Config::new()
        .files(&files)
        .includes(&[root])
        .include_file("_connectrpc.rs")
        .gate_client_feature(true)
        .compile()
    {
        panic!("generating the loam.live.v1 code failed (is protoc installed?): {err:#}");
    }
}
