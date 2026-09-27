//! Loam's durable settings, and the one Resonate configuration they make.
//!
//! Resonate's configuration is built here from [`Loader::new`] alone: no
//! `resonate.toml`, no `RESONATE_*` environment. Everything an operator can
//! change beyond Loam's flags goes through `--durable-set key=value`
//! ([`DurableConfig::overrides`]), in Resonate's own key space, except the
//! keys Loam owns (see [`PROTECTED`]).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use resonate_plugin::{Configuration, Loader};

use crate::error::DurableError;

/// The durable listener's default address: Resonate's SDK default (D138).
pub const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);

/// Resonate's default task retry timeout.
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a stop waits for in-flight work.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The embedded server's settings: what `operon`'s `--durable-*` flags say.
#[derive(Debug, Clone)]
pub struct DurableConfig {
    /// Where the durable API listens. Loopback only (D138).
    pub listen: SocketAddr,
    /// Where durable state lives.
    pub store: DurableStore,
    /// Deliver to `http://` and `https://` targets (`--durable-push`). Off by
    /// default: a caller-chosen URL is a server-side request forgery risk.
    pub push: bool,
    /// The hidden `--durable-debug`: the clock belongs to the caller. It is
    /// `Running::start`'s argument, not a configuration key (T0-9).
    pub debug: bool,
    /// How long a pending task waits before it is redispatched.
    pub retry_timeout: Duration,
    /// How long a stop waits for in-flight work.
    pub shutdown_timeout: Duration,
    /// `--durable-set key=value`, in Resonate's key space, applied in order
    /// after Loam's own keys. The value is TOML; a bare word is a string.
    pub overrides: Vec<(String, String)>,
}

impl DurableConfig {
    /// The defaults, on a SQLite store at `path`.
    pub fn sqlite(path: impl Into<PathBuf>) -> Self {
        Self::new(DurableStore::Sqlite { path: path.into() })
    }

    /// The defaults, on `store`.
    pub fn new(store: DurableStore) -> Self {
        Self {
            listen: DEFAULT_LISTEN,
            store,
            push: false,
            debug: false,
            retry_timeout: DEFAULT_RETRY_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            overrides: Vec::new(),
        }
    }
}

/// Where durable state lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableStore {
    /// A SQLite file (`operon dev` and `standalone`): single node.
    Sqlite { path: PathBuf },
    /// A MySQL-protocol database (TiDB) through Resonate's MySQL plugin. Needs
    /// the `mysql` feature (operon's `durable-mysql`).
    Mysql { url: String, tls: MysqlTls },
}

/// TLS towards the MySQL store (D1 Task 4 maps it onto the URL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MysqlTls {
    #[default]
    Required,
    Disabled,
}

/// Keys Loam sets itself and `--durable-set` may not change, with the flag
/// that owns each. A key is refused when it is one of these, lies under one,
/// or is a table that contains one.
pub const PROTECTED: &[(&str, &str)] = &[
    (
        "gateways.gateway_http.bind",
        "the listen address is --durable-listen",
    ),
    (
        "gateways.gateway_http.abort_on_panic",
        "a handler panic must answer 500, never abort the host process",
    ),
    (
        "gateways.gateway_http.auth",
        "authentication waits for the unified auth plan (D111, D142)",
    ),
    (
        "gateways.gateway_http.workos",
        "authentication waits for the unified auth plan (D111, D142)",
    ),
    ("servers.active", "the backend is --durable-store"),
    ("servers.server_sqlite.path", "the store is --durable-store"),
    ("servers.server_mysql.url", "the store is --durable-store"),
    (
        "workers.transport_http_push.enabled",
        "push delivery is --durable-push",
    ),
];

/// The sections the embed reads. Resonate's process section (`level`,
/// `debug`, `shutdown_timeout`) belongs to `resonate_base::run`, which Loam
/// never calls, so a key there would be silently ignored.
const SECTIONS: &[&str] = &["servers", "workers", "gateways"];

/// A dotted key as its segments, or `None` if it quotes a segment (quoted
/// keys could spell a protected key another way).
fn segments(key: &str) -> Option<Vec<&str>> {
    if key.contains(['"', '\'']) {
        return None;
    }
    let parts: Vec<&str> = key.split('.').map(str::trim).collect();
    if parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    Some(parts)
}

/// Refuse an override Loam owns or nothing would read.
pub(crate) fn check_override(key: &str, carried: &[String]) -> Result<(), DurableError> {
    let parts = segments(key).ok_or_else(|| {
        DurableError::Config(format!(
            "--durable-set {key}: write the key as plain dotted segments, without quotes"
        ))
    })?;
    for (protected, why) in PROTECTED {
        let owned: Vec<&str> = protected.split('.').collect();
        let n = parts.len().min(owned.len());
        if parts[..n] == owned[..n] {
            return Err(DurableError::Config(format!(
                "--durable-set {key}: {protected} is set by Loam ({why})"
            )));
        }
    }
    if !SECTIONS.contains(&parts[0]) {
        return Err(DurableError::Config(format!(
            "--durable-set {key}: the embedded server reads only servers.*, workers.* and \
             gateways.*"
        )));
    }
    if let Some(id) = parts.get(1) {
        let full = format!("{}.{id}", parts[0]);
        if !carried.contains(&full) {
            return Err(DurableError::Config(format!(
                "--durable-set {key}: this build carries no plugin {full}; it has {}",
                carried.join(", ")
            )));
        }
    }
    Ok(())
}

fn quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// The server plugin id `store` selects.
pub(crate) fn server_id(store: &DurableStore) -> &'static str {
    match store {
        DurableStore::Sqlite { .. } => "server_sqlite",
        DurableStore::Mysql { .. } => "server_mysql",
    }
}

/// The Resonate configuration for `config`. `carried` is every
/// `<section>.<plugin id>` the registry holds.
pub(crate) fn configuration(
    config: &DurableConfig,
    carried: &[String],
) -> Result<Configuration, DurableError> {
    let bad = |e: resonate_plugin::ConfigError| DurableError::Config(e.to_string());
    let listen = config.listen.to_string();
    let server_url = format!("http://{listen}");
    let retry_timeout = i64::try_from(config.retry_timeout.as_millis())
        .map_err(|_| DurableError::Config("retry_timeout is too large".into()))?;
    let server = server_id(&config.store);
    let mut loader = Loader::new()
        .set("gateways.gateway_http.bind", &quote(&listen))
        .map_err(bad)?
        .set("gateways.gateway_http.abort_on_panic", "false")
        .map_err(bad)?
        .set("servers.active", &quote(server))
        .map_err(bad)?
        .set(
            "workers.transport_http_push.enabled",
            if config.push { "true" } else { "false" },
        )
        .map_err(bad)?;
    match &config.store {
        DurableStore::Sqlite { path } => {
            let path = path.to_str().ok_or_else(|| {
                DurableError::Config(format!(
                    "the durable store path {} is not UTF-8",
                    path.display()
                ))
            })?;
            loader = loader
                .set("servers.server_sqlite.path", &quote(path))
                .map_err(bad)?
                .set("servers.server_sqlite.migrate", "true")
                .map_err(bad)?;
        }
        DurableStore::Mysql { url, tls: _ } => {
            loader = loader
                .set("servers.server_mysql.url", &quote(url))
                .map_err(bad)?;
        }
    }
    loader = loader
        .set(&format!("servers.{server}.server_url"), &quote(&server_url))
        .map_err(bad)?
        .set(
            &format!("servers.{server}.retry_timeout"),
            &retry_timeout.to_string(),
        )
        .map_err(bad)?;
    for (key, value) in &config.overrides {
        check_override(key, carried)?;
        loader = loader.set(key, value).map_err(bad)?;
    }
    Ok(loader.load())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn carried() -> Vec<String> {
        [
            "servers.server_sqlite",
            "workers.transport_http_push",
            "workers.transport_http_poll",
            "gateways.gateway_http",
        ]
        .map(String::from)
        .to_vec()
    }

    #[test]
    fn protected_keys_and_their_parents_are_refused() {
        for key in [
            "gateways.gateway_http.bind",
            "gateways.gateway_http",
            "gateways",
            "gateways.gateway_http.auth.publickey",
            "servers.active",
            "servers",
            "  servers . active ",
            "servers.\"active\"",
            "level",
            "debug",
            "shutdown_timeout",
            "workers.worker_kafka.enabled",
            "a..b",
        ] {
            assert!(check_override(key, &carried()).is_err(), "{key}");
        }
    }

    #[test]
    fn plugin_settings_are_allowed() {
        for key in [
            "servers.server_sqlite.preload_limit",
            "workers.transport_http_poll.enabled",
            "workers.transport_http_push.concurrency",
            "gateways.gateway_http.cors_allow_origins",
        ] {
            check_override(key, &carried()).expect(key);
        }
    }
}
