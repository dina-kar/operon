//! [`LiveConfig`]: one Loam Live app on TiKV, and how this node serves it.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use operon_tikv::TikvConfig;

use crate::session::SessionConfig;
use crate::subs::SubsConfig;
use crate::{Limits, LiveError, catalog};

/// The prefix of a Live app's keyspace: app `chat` lives in `loam_live_chat`
/// (R1 plan Ruling 7).
pub const KEYSPACE_PREFIX: &str = "loam_live_";

/// The journal shard count a new app gets (R1 plan Ruling 4, raised from 16
/// to 64 by the owner, row T11-1; stored per app, row T10-1).
pub const DEFAULT_JOURNAL_SHARDS: u16 = 64;

/// Where the Live sync API listens by default (§20 §7.1).
pub const DEFAULT_LISTEN: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 7710);

/// How often the journal janitor runs (it trims entries older than the
/// retention and not held by a checkpoint, and sweeps expired idempotency
/// records).
pub const DEFAULT_JANITOR_INTERVAL: Duration = Duration::from_secs(60);

/// One Live app: its TiKV handle configuration (keyspace and root prefix,
/// R1 plan Ruling 1), its limits and its journal shard count; and how this
/// node serves it ([`LiveServer`](crate::LiveServer)).
#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub app: String,
    pub tikv: TikvConfig,
    pub limits: Limits,
    pub journal_shards: u16,
    /// This node's id: the journal consumer its subscription manager
    /// checkpoints under, and the prefix of its session ids.
    pub node: String,
    /// The sync API's address; loopback only in R1 (D111, [`check_listen`]).
    pub listen: SocketAddr,
    /// The subscription manager (its `consumer` defaults to `node`).
    pub subs: SubsConfig,
    /// Sessions: queue, heartbeat, blocked limit.
    pub session: SessionConfig,
    /// How often the journal janitor runs.
    pub janitor_interval: Duration,
}

impl LiveConfig {
    /// App `app` on the cluster whose PD endpoints are `pd`, in the keyspace
    /// `loam_live_<app>`, with R1's default limits, served on
    /// 127.0.0.1:7710 as node `1`.
    pub fn new(pd: Vec<String>, app: &str) -> Result<Self, LiveError> {
        catalog::check_name("app", app)?;
        Ok(LiveConfig::with_tikv(
            app,
            TikvConfig::new(pd, keyspace_of(app)),
        ))
    }

    /// App `app` on the handle `tikv` (whose keyspace and root the caller
    /// chose), with the defaults of [`LiveConfig::new`]. The name is not
    /// checked.
    pub fn with_tikv(app: &str, tikv: TikvConfig) -> Self {
        LiveConfig {
            app: app.to_string(),
            tikv,
            limits: Limits::default(),
            journal_shards: DEFAULT_JOURNAL_SHARDS,
            node: "1".to_string(),
            listen: DEFAULT_LISTEN,
            subs: SubsConfig::default(),
            session: SessionConfig::default(),
            janitor_interval: DEFAULT_JANITOR_INTERVAL,
        }
    }
}

/// The keyspace of app `app`.
pub fn keyspace_of(app: &str) -> String {
    format!("{KEYSPACE_PREFIX}{app}")
}

/// Refuses a non-loopback listen address (127.0.0.0/8, `::1` and their
/// IPv4-mapped forms pass): `Mutate`, the `_system:*` writes and `Deploy`
/// have no authentication in R1 (D111, §20 §7.1).
pub fn check_listen(addr: SocketAddr) -> Result<(), LiveError> {
    if addr.ip().to_canonical().is_loopback() {
        Ok(())
    } else {
        Err(LiveError::NotLoopback(addr))
    }
}
