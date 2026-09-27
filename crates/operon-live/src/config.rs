//! [`LiveConfig`]: one Loam Live app on TiKV.

use operon_tikv::TikvConfig;

use crate::{Limits, LiveError, catalog};

/// The prefix of a Live app's keyspace: app `chat` lives in `loam_live_chat`
/// (R1 plan Ruling 7).
pub const KEYSPACE_PREFIX: &str = "loam_live_";

/// The journal shard count of an app in R1 (R1 plan Ruling 4).
pub const DEFAULT_JOURNAL_SHARDS: u16 = 16;

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
    /// `loam_live_<app>`, with R1's default limits.
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
