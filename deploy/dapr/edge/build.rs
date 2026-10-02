fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../../crates/loams-stream-grpc/proto/loams/stream/v1/stream.proto";
    let include = "../../../crates/loams-stream-grpc/proto";
    tonic_prost_build::configure().compile_protos(&[proto], &[include])?;
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={include}");
    Ok(())
}
