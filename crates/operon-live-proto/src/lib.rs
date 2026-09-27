//! The `loam.live.v1` protocol of Loam Live (design §20 §4.3, §5.3, §7;
//! D121): values and stored documents (`value.proto`), the `LiveService`
//! sync API (`live.proto`) and the commit journal (`journal.proto`,
//! internal to the server).
//!
//! Everything is generated at build time from `proto/loam/live/v1/` by
//! `connectrpc-build`: buffa message types (with their borrowed views and
//! the proto3 JSON mapping, in which 64-bit integers are strings), the
//! `LiveService` server trait, and `LiveServiceClient` behind the `client`
//! feature. Oneof enums live under `loam::live::v1::__buffa::oneof`. The
//! TypeScript client is generated from the same files
//! (`sdks/live-typescript/src/gen/`).

// The generated service marker and server types derive no `Debug`.
#[allow(missing_debug_implementations)]
mod generated {
    connectrpc::include_generated!();
}

pub use generated::*;
