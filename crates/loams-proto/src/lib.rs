//! The public `loams.*.v1` packages of the unified Connect API (design §44).
//!
//! Generated at build time from `proto/` (see `build.rs`): buffa messages
//! with the proto3 JSON mapping, the connect-rust service traits the server
//! implements and, with the `client` feature, their clients. The packages
//! are re-exported at the crate root as `loams_proto::loams::<pkg>::v1`.

#[allow(missing_debug_implementations, missing_docs, unreachable_pub, clippy::all, clippy::pedantic)]
mod generated {
    connectrpc::include_generated!();
}

pub use generated::*;

/// The encoded `FileDescriptorSet` of every generated file and its imports,
/// for `grpc.reflection.v1` (`connectrpc_reflection::Reflector`).
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/loams_api.fds.bin"));
