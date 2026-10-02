//! `loams-es`'s pure suites (plan M1.5, row E3): one binary, a module per
//! suite.

// `EsError` is large by design (the crate allows it too).
#![allow(clippy::result_large_err)]

mod bulk_parse;
mod compile;
mod dsl;
mod fixture;
mod mapping;
mod source_filter;
