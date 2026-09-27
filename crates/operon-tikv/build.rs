//! Compiles the vendored kvproto protos (R1 plan Task 3, row R5): `pdpb.proto`
//! and its imports from pingcap/kvproto `release-8.5` at
//! `07aa8c6a46fab0a4cd577119c249c9c8e718c553`, unchanged. Only the PD client
//! is generated; the GC loop calls `GetMembers`, `GetGCSafePoint`,
//! `UpdateGCSafePoint` and `UpdateServiceGCSafePoint`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        // kvproto comments are Go-flavoured prose, not rustdoc.
        .disable_comments(["."])
        .compile_protos(&["proto/kvproto/pdpb.proto"], &["proto/kvproto"])?;
    Ok(())
}
