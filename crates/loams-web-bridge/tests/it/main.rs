//! The provider conformance suite and the remote provider's own tests (AP1c).
//!
//! Everything here runs against the crate's deterministic drivers, so the whole
//! suite is seconds long and needs no Cloudflare account. What that leaves
//! unverified is listed in `docs/remote-browser-provider.md`.

mod config;
mod contract;
mod egress;
mod remote;
mod secrets;
