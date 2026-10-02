//! Compiles the vendored Qdrant protos (plan M1.4 Ruling 2). The client
//! stubs are used only by tests.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir("proto")? {
        println!("cargo:rerun-if-changed={}", entry?.path().display());
    }
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/qdrant.proto"], &["proto"])?;
    Ok(())
}
