//! The durable listener's address: loopback only (D138), probed before
//! Resonate's gateway binds it, and free again after a stop.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::time::{Duration, Instant};

use crate::error::DurableError;

/// Whether `addr` is reachable only from this host: 127.0.0.0/8 or `::1`
/// (an IPv4-mapped loopback counts too).
pub fn is_loopback(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.to_canonical().is_loopback(),
    }
}

/// Parse `--durable-listen`: an IP socket address, or `localhost:<port>`
/// (which is 127.0.0.1). Anything that is not loopback is refused.
pub fn parse_listen(text: &str) -> Result<SocketAddr, DurableError> {
    let text = text.trim();
    let addr = match text.parse::<SocketAddr>() {
        Ok(addr) => addr,
        Err(_) => match text.rsplit_once(':') {
            Some((host, port)) if host.eq_ignore_ascii_case("localhost") => {
                let port: u16 = port.parse().map_err(|_| {
                    DurableError::Config(format!("--durable-listen: bad port in {text:?}"))
                })?;
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
            }
            // Never resolve a name: only `localhost` is known to be
            // loopback without asking a resolver.
            Some(_) => {
                return Err(DurableError::Config(format!(
                    "--durable-listen takes an IP address or localhost; got {text:?}"
                )));
            }
            None => {
                return Err(DurableError::Config(format!(
                    "--durable-listen: {text:?} is not an address"
                )));
            }
        },
    };
    check_loopback(addr)?;
    Ok(addr)
}

pub(crate) fn check_loopback(addr: SocketAddr) -> Result<(), DurableError> {
    if is_loopback(&addr) {
        Ok(())
    } else {
        Err(DurableError::NotLoopback { addr })
    }
}

/// Bind `addr` once and let it go, so a taken port is a [`DurableError::Bind`]
/// naming the flags rather than a gateway error. Resonate's gateway binds the
/// port itself afterwards; the window between the two is accepted.
pub(crate) fn probe(addr: SocketAddr) -> Result<(), DurableError> {
    TcpListener::bind(addr)
        .map(drop)
        .map_err(|e| DurableError::Bind {
            addr,
            source: e.to_string(),
        })
}

/// Wait until `addr` can be bound again, for at most `limit`.
pub(crate) async fn wait_free(addr: SocketAddr, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if TcpListener::bind(addr).is_ok() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
