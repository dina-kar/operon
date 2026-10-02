fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        // A map's entries come out in name order, so a converted event's
        // extension attributes are ordered.
        .btree_map(".")
        .compile_protos(&["proto/loams/stream/v1/stream.proto"], &["proto"])?;
    Ok(())
}
