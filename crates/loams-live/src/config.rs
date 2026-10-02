//! [`LiveConfig`]: one Loams Live app on TiKV.

use loams_tikv::TikvConfig;

use crate::{Limits, LiveError, catalog};

/// The prefix of a Live app's keyspace: app `chat` lives in `loams_live_chat`
/// (R1 plan Ruling 7).
pub const KEYSPACE_PREFIX: &str = "loams_live_";

/// The journal shard count a new app gets (R1 plan Ruling 4, raised from 16
/// to 64 by the owner, row T11-1; stored per app, row T10-1).
pub const DEFAULT_JOURNAL_SHARDS: u16 = 64;

/// One Live app: its TiKV handle configuration (keyspace and root prefix,
/// R1 plan Ruling 1), its limits and its journal shard count.
#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub app: String,
    pub tikv: TikvConfig,
    pub limits: Limits,
    pub journal_shards: u16,
}

impl LiveConfig {
    /// App `app` on the cluster whose PD endpoints are `pd`, in the keyspace
    /// `loams_live_<app>`, with R1's default limits.
    pub fn new(pd: Vec<String>, app: &str) -> Result<Self, LiveError> {
        catalog::check_name("app", app)?;
        Ok(LiveConfig {
            app: app.to_string(),
            tikv: TikvConfig::new(pd, keyspace_of(app)),
            limits: Limits::default(),
            journal_shards: DEFAULT_JOURNAL_SHARDS,
        })
    }
}

/// The keyspace of app `app`.
pub fn keyspace_of(app: &str) -> String {
    format!("{KEYSPACE_PREFIX}{app}")
}
