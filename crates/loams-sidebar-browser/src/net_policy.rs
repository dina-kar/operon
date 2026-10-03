//! Which targets the embed may reach, and when the engine's private-network
//! switch has to be on.
//!
//! # The problem
//!
//! Obscura refuses loopback, RFC1918, link-local and IPv6 unique-local targets
//! by default, and enforces it *after* DNS resolution so a public name that
//! resolves to a private address is refused too. That default is right for a
//! scraping engine and wrong for Loams: a development Loams instance embeds
//! `https://console.localhost:8443`, or a developer's console runs on
//! `http://127.0.0.1:8084`, and the embed would render a network error for
//! every page.
//!
//! `--allow-private-network` (equivalently `OBSCURA_ALLOW_PRIVATE_NETWORK=1`)
//! is the documented way to relax it, and [`EngineLaunchSpec`](crate::engine)
//! sets it when — and only when — the embedded origin needs it.
//!
//! # The two things this module adds on top of the engine
//!
//! 1. **Loams keeps a hard-deny set that the engine's switch does not open.**
//!    [`PrivateNetworkDecision::Refused`] is decided here and never
//!    reconsulted by the launch spec, so a link-local or unspecified target
//!    stays refused even with `--allow-private-network` set. The most valuable
//!    member of that set is the cloud metadata endpoint `169.254.169.254`.
//! 2. **A private-network permission is per-profile, not per-request, and the
//!    permission is spent by the whole profile.** One `obscura serve` serves
//!    every origin the page can navigate to, so relaxing it for a localhost
//!    console relaxes it for anything that console can link to. The
//!    compensating control is the navigation allowlist in
//!    [`crate::navigate`], not this module: this module decides whether the
//!    switch may be on at all, and the allowlist decides what it can be used
//!    for.

use std::net::Ipv4Addr;

use url::{Host, Url};

use crate::error::{Result, SidebarBrowserError};
use crate::session::EmbeddedOrigin;

/// The cloud instance metadata endpoint. Always refused.
pub const CLOUD_METADATA_ADDR: Ipv4Addr = Ipv4Addr::new(169, 254, 169, 254);

/// What Loams decided about one target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrivateNetworkDecision {
    /// A public address. The engine's private-network switch stays off.
    Public,
    /// A private address that the embed is allowed to reach. The engine's
    /// switch must be on for the profile.
    Permitted {
        /// Why it was permitted, for a log line and for the PR's audit trail.
        because: &'static str,
    },
    /// Refused by Loams, whatever the engine's switch says.
    Refused {
        /// Why it was refused.
        reason: &'static str,
    },
}

impl PrivateNetworkDecision {
    /// Whether the engine must be launched with `--allow-private-network`.
    pub fn requires_engine_relaxation(&self) -> bool {
        matches!(self, PrivateNetworkDecision::Permitted { .. })
    }

    /// Whether the target may be reached at all.
    pub fn is_permitted(&self) -> bool {
        !matches!(self, PrivateNetworkDecision::Refused { .. })
    }
}

/// Decide whether a URL is reachable, and whether the engine must be relaxed
/// to reach it.
///
/// Only `http` and `https` are reachable at all: Obscura accepts `file` too
/// when `--allow-file-access` is set, and that flag is never set here, because
/// a CDP connection that could read `file://` would read Loams's own
/// credential store off disk.
pub fn decide(url: &Url) -> Result<PrivateNetworkDecision> {
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(SidebarBrowserError::NetworkRefused {
                reason: format!("scheme {other:?} is not embeddable; only http and https are"),
            });
        }
    }
    let host = url
        .host()
        .ok_or_else(|| SidebarBrowserError::NetworkRefused {
            reason: "the URL has no host".to_string(),
        })?;
    Ok(decide_host(&host))
}

/// [`decide`], for a host that has already been parsed out of a URL.
pub fn decide_host(host: &Host<&str>) -> PrivateNetworkDecision {
    match host {
        Host::Domain(domain) => decide_domain(domain),
        Host::Ipv4(ip) => decide_ipv4(*ip),
        Host::Ipv6(ip) => decide_ipv6(ip),
    }
}

fn decide_domain(domain: &str) -> PrivateNetworkDecision {
    let lower = domain.to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") {
        return PrivateNetworkDecision::Permitted {
            because: "a loopback name (RFC 6761 reserves .localhost for 127.0.0.1)",
        };
    }
    // `.local` is mDNS and resolves inside the LAN, so it is private in
    // effect even though it is not an IP literal.
    if lower.ends_with(".local") || lower.ends_with(".internal") {
        return PrivateNetworkDecision::Permitted {
            because: "a private-use DNS suffix that resolves inside the local network",
        };
    }
    PrivateNetworkDecision::Public
}

fn decide_ipv4(ip: Ipv4Addr) -> PrivateNetworkDecision {
    if ip == CLOUD_METADATA_ADDR {
        return PrivateNetworkDecision::Refused {
            reason: "the cloud instance metadata endpoint is never an embed target",
        };
    }
    if ip.is_unspecified() {
        return PrivateNetworkDecision::Refused {
            reason: "the unspecified address routes to localhost, so it is not a target",
        };
    }
    if ip.is_link_local() {
        return PrivateNetworkDecision::Refused {
            reason: "link-local addresses carry the instance metadata service and are not \
                     an embed target",
        };
    }
    if ip.is_loopback() {
        return PrivateNetworkDecision::Permitted {
            because: "loopback, which is how a local Loams console is reached",
        };
    }
    if ip.is_private() {
        return PrivateNetworkDecision::Permitted {
            because: "an RFC 1918 address inside the deployment's own network",
        };
    }
    if ip.is_broadcast() {
        return PrivateNetworkDecision::Refused {
            reason: "the broadcast address is not a target",
        };
    }
    PrivateNetworkDecision::Public
}

fn decide_ipv6(ip: &std::net::Ipv6Addr) -> PrivateNetworkDecision {
    let segments = ip.segments();
    if ip.is_unspecified() {
        return PrivateNetworkDecision::Refused {
            reason: "the unspecified address routes to localhost, so it is not a target",
        };
    }
    if ip.is_loopback() {
        return PrivateNetworkDecision::Permitted {
            because: "IPv6 loopback, which is how a local Loams console is reached",
        };
    }
    // fc00::/7, the IPv6 unique-local range. `Ipv6Addr::is_unique_local` is
    // still unstable, so the mask is spelled out.
    if (segments[0] & 0xfe00) == 0xfc00 {
        return PrivateNetworkDecision::Permitted {
            because: "an IPv6 unique-local address inside the deployment's own network",
        };
    }
    if ip.is_unicast_link_local() {
        return PrivateNetworkDecision::Refused {
            reason: "IPv6 link-local addresses carry the instance metadata service and are not \
                     an embed target",
        };
    }
    // fe80::/10 is link-local; ::ffff:0:0/96 and ::a.b.c.d are IPv4-mapped, and
    // the mapped address inherits the IPv4 answer so a mapped 127.0.0.1 is not
    // a way around the loopback branch above.
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return decide_ipv4(mapped);
    }
    PrivateNetworkDecision::Public
}

/// The private-network decision for an embedded origin, plus whether the
/// launch spec may enable the engine's relaxation.
pub fn decide_origin(origin: &EmbeddedOrigin) -> Result<PrivateNetworkDecision> {
    decide(origin.url())
}

/// Whether a decision may be turned into a launch flag, refusing to launch an
/// engine whose relaxation was asked for by a target Loams itself refused.
///
/// This is the belt to [`crate::net_policy::decide`]'s braces: a bug that
/// mapped a `Refused` to "set the flag anyway" fails here rather than starting
/// a process with the metadata endpoint reachable.
pub fn engine_relaxation(
    origin: &EmbeddedOrigin,
    decision: &PrivateNetworkDecision,
) -> Result<bool> {
    match decision {
        PrivateNetworkDecision::Refused { reason } => Err(SidebarBrowserError::NetworkRefused {
            reason: format!("{origin} was refused ({reason}), so no engine may be relaxed for it"),
        }),
        other => Ok(other.requires_engine_relaxation()),
    }
}
