//! D111's unauthenticated-listener warning (plan M1.6 Task 7 rule 14, owner
//! ruling O-M16-1). M1 has no authentication or TLS on any listener, so
//! every listener bound to a non-loopback address logs one startup warning.

use std::net::SocketAddr;

/// Whether `addr` is reachable from other hosts: true unless its IP is a
/// loopback address (`0.0.0.0` and `::` are exposed).
pub fn is_exposed(addr: SocketAddr) -> bool {
    !addr.ip().is_loopback()
}

/// Logs D111's warning for the `surface` listener bound at `addr` when it is
/// exposed; returns whether it did.
pub fn warn_if_exposed(surface: &'static str, addr: SocketAddr) -> bool {
    if !is_exposed(addr) {
        return false;
    }
    tracing::warn!(
        surface,
        %addr,
        "the {surface} listener on {addr} is unauthenticated (D111): anyone who can reach it can read and write data; bind a loopback address or restrict network access"
    );
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_addresses_are_not_exposed() {
        for addr in ["127.0.0.1:0", "127.1.2.3:0", "[::1]:0"] {
            let addr: SocketAddr = addr.parse().unwrap();
            assert!(!is_exposed(addr), "{addr}");
            assert!(!warn_if_exposed("native", addr), "{addr}");
        }
        for addr in ["0.0.0.0:0", "[::]:0", "192.168.1.10:0"] {
            let addr: SocketAddr = addr.parse().unwrap();
            assert!(is_exposed(addr), "{addr}");
            assert!(warn_if_exposed("native", addr), "{addr}");
        }
    }
}
