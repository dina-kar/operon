//! The MCP server's end-to-end tests (plan M1.6 Tasks 7–8, row E2): one
//! binary, an in-process `Server` with MCP on its own listener.

#[cfg(feature = "mcp")]
mod harness;
#[cfg(feature = "mcp")]
mod protocol;
#[cfg(feature = "mcp")]
mod tools;
