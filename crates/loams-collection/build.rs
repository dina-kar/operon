//! Compiles the collection manifest's protobuf (plan M1.1 Task 9) with the
//! system `protoc`.

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/collection_manifest.proto");
    prost_build::compile_protos(&["proto/collection_manifest.proto"], &["proto/"])
}
