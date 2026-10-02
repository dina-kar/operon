//! Loams's durable settings, and the one Resonate configuration they make.
//!
//! Resonate's configuration is built here from [`Loader::new`] alone: no
//! `resonate.toml`, no `RESONATE_*` environment. Everything an operator can
//! change beyond Loams's flags goes through `--durable-set key=value`
//! ([`DurableConfig::overrides`]), in Resonate's own key space, except the
//! keys Loams owns (see [`PROTECTED`]).

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use resonate_plugin::{Configuration, Loader};
use url::{Host, Url};

use crate::error::DurableError;

/// The durable listener's default address: Resonate's SDK default (D138).
pub const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);

/// Resonate's default task retry timeout.
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a stop waits for in-flight work.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// The embedded server's settings: what `loams`'s `--durable-*` flags say.
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
    /// after Loams's own keys. The value is TOML; a bare word is a string.
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
///
/// `Debug` and `Display` hide a MySQL URL's password ([`redact_url`]), so a
/// store (or a [`DurableConfig`], or `loams`'s `ServerConfig`) can be logged.
#[derive(Clone, PartialEq, Eq)]
pub enum DurableStore {
    /// A SQLite file (`loams dev` and `standalone`): single node.
    Sqlite { path: PathBuf },
    /// A MySQL-protocol database (TiDB) through Resonate's MySQL plugin. Needs
    /// the `mysql` feature (loams's `durable-mysql`). `tls` decides the
    /// connection's `ssl-mode`, whatever `url` says ([`DurableStore::mysql`]
    /// reads it from the URL).
    Mysql { url: String, tls: MysqlTls },
    /// Native TiKV API v2 store, isolated by keyspace and root prefix.
    Tikv {
        pd: Vec<String>,
        keyspace: String,
        root: Vec<u8>,
    },
}

impl DurableStore {
    /// A native TiKV store. All durable servers in one deployment must use
    /// the same keyspace and root; unrelated applications should use another
    /// root within the keyspace.
    pub fn tikv(pd: Vec<String>, keyspace: impl Into<String>) -> Result<Self, DurableError> {
        if pd.is_empty() || pd.iter().any(|endpoint| endpoint.trim().is_empty()) {
            return Err(DurableError::Config(
                "TiKV needs at least one PD endpoint".into(),
            ));
        }
        let keyspace = keyspace.into();
        if keyspace.is_empty() {
            return Err(DurableError::Config("TiKV needs an API v2 keyspace".into()));
        }
        Ok(Self::Tikv {
            pd,
            keyspace,
            root: b"resonate-durable/".to_vec(),
        })
    }

    /// A MySQL store from `mysql://user:pass@host:port/db?ssl-mode=…`
    /// (D1 Task 4). `ssl-mode` (or `sslmode`) is `required`, `disabled`,
    /// `verify_ca` or `verify_identity` (owner ruling Q8); without it TLS is
    /// required unless the host is `localhost` or a loopback address.
    /// `ssl-ca=<path>` names the CA file the verifying modes check the
    /// server's certificate against (without it, the driver's built-in
    /// roots); it must be a file, and it needs a verifying mode. The URL is
    /// kept as given.
    pub fn mysql(url: &str) -> Result<Self, DurableError> {
        let parsed = parse_mysql(url)?;
        let bad = |why: String| {
            DurableError::Config(format!("--durable-store {}: {why}", redact_url(url)))
        };
        let mut tls = None;
        let mut ca = None;
        for (key, value) in parsed.query_pairs() {
            if key == "ssl-mode" || key == "sslmode" {
                tls = Some(match value.to_ascii_lowercase().as_str() {
                    "required" => MysqlTls::Required,
                    "disabled" => MysqlTls::Disabled,
                    "verify_ca" => MysqlTls::VerifyCa,
                    "verify_identity" => MysqlTls::VerifyIdentity,
                    _ => {
                        return Err(bad(format!(
                            "{key}={value} is not supported; use ssl-mode=required, \
                             disabled, verify_ca or verify_identity"
                        )));
                    }
                });
            } else if key == "ssl-ca" {
                ca = Some(PathBuf::from(value.into_owned()));
            }
        }
        if let Some(ca) = &ca {
            if !tls.is_some_and(MysqlTls::verifies) {
                return Err(bad(format!(
                    "ssl-ca={} needs ssl-mode=verify_ca or ssl-mode=verify_identity",
                    ca.display()
                )));
            }
            if !ca.is_file() {
                return Err(bad(format!(
                    "ssl-ca={} is not a readable file",
                    ca.display()
                )));
            }
        }
        let tls = tls.unwrap_or_else(|| {
            let local = match parsed.host() {
                // A mysql:// URL's host is opaque to the url crate, so an IP
                // address arrives as a domain.
                Some(Host::Domain(name)) => {
                    name.eq_ignore_ascii_case("localhost")
                        || name.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
                }
                Some(Host::Ipv4(ip)) => ip.is_loopback(),
                Some(Host::Ipv6(ip)) => ip.is_loopback(),
                None => false,
            };
            if local {
                MysqlTls::Disabled
            } else {
                MysqlTls::Required
            }
        });
        Ok(Self::Mysql {
            url: url.to_string(),
            tls,
        })
    }
}

impl fmt::Debug for DurableStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite { path } => f.debug_struct("Sqlite").field("path", path).finish(),
            Self::Mysql { url, tls } => f
                .debug_struct("Mysql")
                .field("url", &redact_url(url))
                .field("tls", tls)
                .finish(),
            Self::Tikv { pd, keyspace, root } => f
                .debug_struct("Tikv")
                .field("pd", pd)
                .field("keyspace", keyspace)
                .field("root", root)
                .finish(),
        }
    }
}

impl fmt::Display for DurableStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite { path } => write!(f, "sqlite:{}", path.display()),
            Self::Mysql { url, .. } => f.write_str(&redact_url(url)),
            Self::Tikv { pd, keyspace, .. } => {
                write!(f, "tikv://{}/{}", pd.join(","), keyspace)
            }
        }
    }
}

/// `url` with its password replaced by `***`, for a log line or an error.
/// A URL that does not parse loses everything between `://` and its last
/// `@` instead.
pub fn redact_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            if parsed.password().is_some() && parsed.set_password(Some("***")).is_ok() {
                parsed.to_string()
            } else {
                url.to_string()
            }
        }
        Err(_) => match (url.find("://"), url.rfind('@')) {
            (Some(scheme), Some(at)) if at > scheme => {
                format!("{}***{}", &url[..scheme + 3], &url[at..])
            }
            _ => url.to_string(),
        },
    }
}

/// `message` with every spelling of `url`'s password replaced by `***`: for
/// an error from a driver or from Resonate that might quote the URL.
pub(crate) fn scrub(message: &str, url: &str) -> String {
    let mut out = message.to_string();
    let Ok(parsed) = Url::parse(url) else {
        return out.replace(url, &redact_url(url));
    };
    let Some(password) = parsed.password() else {
        return out;
    };
    // The URL as given, as the url crate writes it, and as Resonate got it.
    let mut urls = vec![url.to_string(), parsed.to_string()];
    for tls in MysqlTls::ALL {
        urls.extend(mysql_url(url, tls));
    }
    for form in urls {
        out = out.replace(&form, &redact_url(&form));
    }
    // A short password is not scrubbed on its own: replacing every "ab" in a
    // message would garble it, and the URL forms are covered above.
    for secret in [password.to_string(), percent_decode(password)] {
        if secret.len() >= 4 {
            out = out.replace(&secret, "***");
        }
    }
    out
}

/// `%XX` escapes decoded, as sqlx decodes a URL's password; anything else is
/// kept as it is.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The URL Resonate is handed for `url`: `tls` as `ssl-mode` (replacing any
/// `ssl-mode` or `sslmode` the URL has), and the database
/// `loams_durable_default` (Ruling 2) when the URL names none. Every other
/// parameter, `ssl-ca` among them, is passed through to sqlx.
pub(crate) fn mysql_url(url: &str, tls: MysqlTls) -> Result<String, DurableError> {
    let mut parsed = parse_mysql(url)?;
    let kept: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(key, _)| key != "ssl-mode" && key != "sslmode")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let mode = tls.ssl_mode();
    parsed
        .query_pairs_mut()
        .clear()
        .extend_pairs(kept)
        .append_pair("ssl-mode", mode);
    if parsed.path().trim_matches('/').is_empty() {
        parsed.set_path(&format!("/{DEFAULT_DATABASE}"));
    }
    Ok(parsed.to_string())
}

/// The database of the default namespace on a MySQL store (Ruling 2).
pub const DEFAULT_DATABASE: &str = "loams_durable_default";

/// `url` as a `mysql://` URL with a host. The error never carries the
/// password.
fn parse_mysql(url: &str) -> Result<Url, DurableError> {
    let bad =
        |why: &str| DurableError::Config(format!("--durable-store {}: {why}", redact_url(url)));
    let parsed = Url::parse(url).map_err(|e| bad(&format!("not a URL ({e})")))?;
    if parsed.scheme() != "mysql" {
        return Err(bad("expected a mysql:// URL"));
    }
    if parsed.host_str().is_none_or(str::is_empty) {
        return Err(bad("the URL names no host"));
    }
    Ok(parsed)
}

/// TLS towards the MySQL store (D1 Task 4 maps it onto the URL's
/// `ssl-mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MysqlTls {
    /// Encrypted, the certificate unchecked.
    #[default]
    Required,
    /// Plain text: loopback hosts by default, or `ssl-mode=disabled`.
    Disabled,
    /// Encrypted, the certificate checked against the CA (`ssl-ca`, else the
    /// driver's built-in roots), the host name not (owner ruling Q8).
    VerifyCa,
    /// As `VerifyCa`, and the certificate must name the host: managed TiDB
    /// such as TiDB Cloud (owner ruling Q8).
    VerifyIdentity,
}

impl MysqlTls {
    /// Every mode, for scrubbing each URL form Resonate may have quoted.
    pub(crate) const ALL: [Self; 4] = [
        Self::Required,
        Self::Disabled,
        Self::VerifyCa,
        Self::VerifyIdentity,
    ];

    /// Whether the server's certificate is checked.
    pub fn verifies(self) -> bool {
        matches!(self, Self::VerifyCa | Self::VerifyIdentity)
    }

    /// The `ssl-mode` value sqlx reads.
    pub(crate) fn ssl_mode(self) -> &'static str {
        match self {
            Self::Required => "REQUIRED",
            Self::Disabled => "DISABLED",
            Self::VerifyCa => "VERIFY_CA",
            Self::VerifyIdentity => "VERIFY_IDENTITY",
        }
    }
}

/// Keys Loams sets itself and `--durable-set` may not change, with the flag
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
    ("servers.server_tikv.pd", "the store is --durable-store"),
    (
        "servers.server_tikv.keyspace",
        "the store is --durable-store",
    ),
    ("servers.server_tikv.root", "the store is --durable-store"),
    (
        "servers.server_mysql.migrate",
        "only loams durable migrate changes the schema",
    ),
    (
        "workers.transport_http_push.enabled",
        "push delivery is --durable-push",
    ),
    (
        "workers.worker_inproc",
        "the in-process worker is Loams's own runtime's (D1 Task 6)",
    ),
];

/// The sections the embed reads. Resonate's process section (`level`,
/// `debug`, `shutdown_timeout`) belongs to `resonate_base::run`, which Loams
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

/// Refuse an override Loams owns or nothing would read.
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
                "--durable-set {key}: {protected} is set by Loams ({why})"
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
        DurableStore::Tikv { .. } => "server_tikv",
    }
}

/// The Resonate configuration for `config`. `carried` is every
/// `<section>.<plugin id>` the registry holds.
/// What the embedded server is built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// `loams dev`, `standalone` and `cluster`: serve the durable API. A
    /// MySQL store is never migrated (D1 Task 4).
    Serve,
    /// `loams durable migrate`: open the store with `migrate = true`, with
    /// no listener, then stop.
    Migrate,
}

///
/// `inproc` is the instance number `worker_inproc` parks its worker under
/// (`None`: the worker is off).
pub(crate) fn configuration(
    config: &DurableConfig,
    carried: &[String],
    mode: Mode,
    inproc: Option<u64>,
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
        DurableStore::Mysql { url, tls } => {
            loader = loader
                .set("servers.server_mysql.url", &quote(&mysql_url(url, *tls)?))
                .map_err(bad)?
                .set(
                    "servers.server_mysql.migrate",
                    if mode == Mode::Migrate {
                        "true"
                    } else {
                        "false"
                    },
                )
                .map_err(bad)?;
        }
        DurableStore::Tikv { pd, keyspace, root } => {
            let pd = format!(
                "[{}]",
                pd.iter().map(|p| quote(p)).collect::<Vec<_>>().join(", ")
            );
            let root = format!(
                "[{}]",
                root.iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            loader = loader
                .set("servers.server_tikv.pd", &pd)
                .map_err(bad)?
                .set("servers.server_tikv.keyspace", &quote(keyspace))
                .map_err(bad)?
                .set("servers.server_tikv.root", &root)
                .map_err(bad)?;
        }
    }
    if let Some(instance) = inproc {
        loader = loader
            .set("workers.worker_inproc.instance", &instance.to_string())
            .map_err(bad)?;
    }
    if mode == Mode::Migrate {
        loader = loader
            .set("gateways.gateway_http.enabled", "false")
            .map_err(bad)?
            .set("workers.transport_http_poll.enabled", "false")
            .map_err(bad)?;
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

    #[test]
    fn native_tikv_store_selects_its_server() {
        let store = DurableStore::tikv(vec!["127.0.0.1:2379".into()], "durable")
            .expect("valid TiKV address");
        assert_eq!(server_id(&store), "server_tikv");
        assert!(matches!(store, DurableStore::Tikv { .. }));
        assert!(DurableStore::tikv(Vec::new(), "durable").is_err());
        assert!(DurableStore::tikv(vec!["127.0.0.1:2379".into()], "").is_err());
    }

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
            "workers.worker_inproc",
            "workers.worker_inproc.instance",
            "a..b",
        ] {
            assert!(check_override(key, &carried()).is_err(), "{key}");
        }
    }

    fn mysql(url: &str) -> DurableStore {
        DurableStore::Mysql {
            url: url.into(),
            tls: MysqlTls::Required,
        }
    }

    /// The security fix of Task 4: a MySQL URL's password never reaches a
    /// Debug or Display rendering (and so no log line that formats one).
    #[test]
    fn debug_and_display_redact_the_password() {
        let url = "mysql://loams:s3cr%40t-pw@db.internal:4000/loams_durable_default";
        let store = mysql(url);
        let config = DurableConfig::new(store.clone());
        for text in [
            format!("{store:?}"),
            format!("{store}"),
            format!("{config:?}"),
            format!("{config:#?}"),
            redact_url(url),
        ] {
            assert!(!text.contains("s3cr"), "{text}");
            assert!(text.contains("db.internal:4000"), "{text}");
            assert!(text.contains("loams"), "the user name stays: {text}");
        }
        assert_eq!(
            redact_url(url),
            "mysql://loams:***@db.internal:4000/loams_durable_default"
        );
        // No password: nothing to hide, the URL is unchanged.
        assert_eq!(
            redact_url("mysql://root@127.0.0.1:4000/x"),
            "mysql://root@127.0.0.1:4000/x"
        );
        // A URL that does not parse still loses everything before the host.
        assert_eq!(
            redact_url("mysql://u:p w@@bad host/x"),
            "mysql://***@bad host/x"
        );
        let sqlite = DurableStore::Sqlite {
            path: "/data/durable/default.db".into(),
        };
        assert_eq!(sqlite.to_string(), "sqlite:/data/durable/default.db");
    }

    /// Task 4 semantics 1: `ssl-mode` on the URL, else TLS unless the host is
    /// this machine.
    #[test]
    fn mysql_urls_map_tls_onto_ssl_mode() {
        let tls = |url: &str| match DurableStore::mysql(url) {
            Ok(DurableStore::Mysql { tls, url: kept }) => {
                assert_eq!(kept, url, "the URL is kept as given");
                tls
            }
            other => panic!("{url}: {other:?}"),
        };
        assert_eq!(
            tls("mysql://loams:pw@tidb.internal:4000/loams_durable_default"),
            MysqlTls::Required
        );
        assert_eq!(tls("mysql://loams@10.0.0.7:4000/d"), MysqlTls::Required);
        for local in [
            "mysql://root@127.0.0.1:4000/d",
            "mysql://root@localhost:4000/d",
            "mysql://root@LOCALHOST/d",
            "mysql://root@[::1]:4000/d",
        ] {
            assert_eq!(tls(local), MysqlTls::Disabled, "{local}");
        }
        assert_eq!(
            tls("mysql://u@tidb.internal:4000/d?ssl-mode=disabled"),
            MysqlTls::Disabled
        );
        assert_eq!(
            tls("mysql://u@127.0.0.1:4000/d?ssl-mode=REQUIRED"),
            MysqlTls::Required
        );
        assert_eq!(
            tls("mysql://u@127.0.0.1:4000/d?sslmode=required"),
            MysqlTls::Required
        );
        for bad in [
            "mysql://u:hunter22@tidb:4000/d?ssl-mode=preferred",
            "mysql://u:hunter22@tidb:4000/d?ssl-mode=nonsense",
            "mysql://u:hunter22@/d",
            "postgres://u:hunter22@tidb/d",
            "mysql://u:hunter22@tidb:notaport/d",
        ] {
            let err = DurableStore::mysql(bad).expect_err(bad).to_string();
            assert!(!err.contains("hunter22"), "{bad}: {err}");
        }
    }

    /// What Resonate is handed: `tls` as `ssl-mode`, whatever the URL said,
    /// and the default database (Ruling 2) when the URL names none.
    #[test]
    fn the_resonate_url_carries_the_tls_mode() {
        assert_eq!(
            mysql_url(
                "mysql://u:p@tidb:4000/db?ssl-mode=disabled&charset=utf8mb4",
                MysqlTls::Required
            )
            .expect("url"),
            "mysql://u:p@tidb:4000/db?charset=utf8mb4&ssl-mode=REQUIRED"
        );
        assert_eq!(
            mysql_url("mysql://u@127.0.0.1:4000", MysqlTls::Disabled).expect("url"),
            "mysql://u@127.0.0.1:4000/loams_durable_default?ssl-mode=DISABLED"
        );
        assert_eq!(
            mysql_url("mysql://u@127.0.0.1:4000/", MysqlTls::Disabled).expect("url"),
            "mysql://u@127.0.0.1:4000/loams_durable_default?ssl-mode=DISABLED"
        );
    }

    /// Owner ruling Q8: `verify_ca` and `verify_identity` verify the server's
    /// certificate, against `ssl-ca=<path>` when the URL names one (else the
    /// driver's built-in roots, which is what a managed TiDB with a public
    /// certificate needs). `ssl-ca` without a verifying mode is refused: it
    /// would look like verification and be none.
    #[test]
    fn verifying_tls_modes_take_a_ca_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&ca, "not parsed here").expect("write ca");
        let ca = ca.display().to_string();
        let tls = |url: &str| match DurableStore::mysql(url) {
            Ok(DurableStore::Mysql { tls, url: kept }) => {
                assert_eq!(kept, url, "the URL is kept as given");
                tls
            }
            other => panic!("{url}: {other:?}"),
        };
        assert_eq!(
            tls("mysql://u:pw@gateway.tidbcloud.com:4000/d?ssl-mode=verify_identity"),
            MysqlTls::VerifyIdentity
        );
        assert_eq!(
            tls("mysql://u:pw@tidb:4000/d?sslmode=VERIFY_CA"),
            MysqlTls::VerifyCa
        );
        assert_eq!(
            tls(&format!(
                "mysql://u:pw@tidb:4000/d?ssl-mode=verify_ca&ssl-ca={ca}"
            )),
            MysqlTls::VerifyCa
        );
        assert_eq!(
            tls(&format!(
                "mysql://u:pw@127.0.0.1:4000/d?ssl-ca={ca}&ssl-mode=verify_identity"
            )),
            MysqlTls::VerifyIdentity
        );

        let missing = dir.path().join("absent.pem").display().to_string();
        let err = DurableStore::mysql(&format!(
            "mysql://u:hunter22@tidb:4000/d?ssl-mode=verify_ca&ssl-ca={missing}"
        ))
        .expect_err("a CA file that is not there")
        .to_string();
        assert!(err.contains(&missing) && err.contains("ssl-ca"), "{err}");
        assert!(!err.contains("hunter22"), "{err}");

        for mode in ["", "&ssl-mode=required", "&ssl-mode=disabled"] {
            let url = format!("mysql://u:hunter22@tidb:4000/d?ssl-ca={ca}{mode}");
            let err = DurableStore::mysql(&url).expect_err(&url).to_string();
            assert!(
                err.contains("ssl-mode=verify_ca or ssl-mode=verify_identity"),
                "{err}"
            );
            assert!(!err.contains("hunter22"), "{err}");
        }
    }

    /// The verifying modes reach Resonate (and the schema check) as sqlx
    /// spells them, with `ssl-ca` passed through.
    #[test]
    fn the_resonate_url_carries_the_verifying_modes() {
        assert_eq!(
            mysql_url(
                "mysql://u:p@tidb:4000/db?ssl-ca=%2Fetc%2Fca.pem&ssl-mode=verify_ca",
                MysqlTls::VerifyCa
            )
            .expect("url"),
            "mysql://u:p@tidb:4000/db?ssl-ca=%2Fetc%2Fca.pem&ssl-mode=VERIFY_CA"
        );
        assert_eq!(
            mysql_url("mysql://u:p@tidb:4000/db", MysqlTls::VerifyIdentity).expect("url"),
            "mysql://u:p@tidb:4000/db?ssl-mode=VERIFY_IDENTITY"
        );
    }

    #[test]
    fn scrub_removes_the_password_from_driver_messages() {
        let url = "mysql://loams:s3cr%40t-pw@db.internal:4000/d";
        let resonate = mysql_url(url, MysqlTls::Required).expect("url");
        let message = format!(
            "cannot connect to {url} ({resonate}): access denied for loams using s3cr@t-pw \
             or s3cr%40t-pw"
        );
        let clean = scrub(&message, url);
        assert!(!clean.contains("s3cr"), "{clean}");
        assert!(
            clean.contains("mysql://loams:***@db.internal:4000/d"),
            "{clean}"
        );
        // Nothing to hide without a password.
        assert_eq!(scrub("x mysql://u@h/d", "mysql://u@h/d"), "x mysql://u@h/d");
    }

    #[test]
    fn migrate_is_owned_by_the_migrate_command() {
        let err = check_override("servers.server_mysql.migrate", &carried())
            .expect_err("owned")
            .to_string();
        assert!(err.contains("loams durable migrate"), "{err}");
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
