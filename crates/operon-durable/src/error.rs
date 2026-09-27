//! What can go wrong starting, stopping and calling the embedded server.

use std::fmt;
use std::net::SocketAddr;

/// An error from the embedded durable server.
///
/// `Display` by hand: thiserror would take `Bind`'s `source` field (the
/// plan's name, a message) for an error source.
#[derive(Debug)]
pub enum DurableError {
    /// `--durable-listen` named an address other peers could reach (D138).
    NotLoopback { addr: SocketAddr },
    /// The listen address is taken, or cannot be bound.
    Bind { addr: SocketAddr, source: String },
    /// The configuration was refused: a protected or unknown key, a bad value,
    /// or a store this build does not carry.
    Config(String),
    /// The server did not start: the store could not be opened or locked.
    Start(String),
    /// The server gave no answer (Resonate's `Unavailable`): retry later.
    Unavailable(String),
    /// The server answered with a non-2xx status. `body` is the response's
    /// `data`.
    Protocol {
        status: u16,
        body: serde_json::Value,
    },
}

impl fmt::Display for DurableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLoopback { addr } => write!(
                f,
                "durable listener must be loopback until authentication is configured (D111); \
                 got {addr}"
            ),
            Self::Bind { addr, source } => write!(
                f,
                "the durable listener cannot bind {addr}: {source}; choose another address with \
                 --durable-listen or turn the listener off with --no-durable"
            ),
            Self::Config(message) => write!(f, "durable configuration: {message}"),
            Self::Start(message) => f.write_str(message),
            Self::Unavailable(message) => {
                write!(f, "the durable server is unavailable: {message}")
            }
            Self::Protocol { status, body } => {
                write!(f, "the durable server answered {status}: {body}")
            }
        }
    }
}

impl std::error::Error for DurableError {}
