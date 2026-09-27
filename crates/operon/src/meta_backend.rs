//! Which metastore a single-process Operon runs on (R1 plan Task 6, D124):
//! the embedded openraft store (the default) or TiKV, chosen with
//! `--meta tikv://<pd-host:port>[,<pd…>]/<keyspace>[?root=<hex>]` on
//! `operon dev` and `operon standalone`.

/// The metastore of [`ServerConfig::meta`](crate::ServerConfig::meta).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MetaBackend {
    /// The embedded single-node openraft store in `<data_dir>/meta`.
    #[default]
    Raft,
    /// The TiKV metastore in a keyspace (`loam_meta` in production). The
    /// server also runs the cluster MVCC GC loop on its handle (R1 plan
    /// Task 3).
    #[cfg(feature = "meta-tikv")]
    Tikv(operon_meta_tikv::TikvMetaConfig),
}

/// The URL scheme of the TiKV metastore.
pub const TIKV_SCHEME: &str = "tikv://";

impl MetaBackend {
    /// Parses `--meta`: `tikv://<pd-host:port>[,<pd-host:port>…]/<keyspace>`,
    /// optionally followed by `?root=<hex>`, a key prefix inside the keyspace
    /// (tests and gates isolate by it; production leaves it empty).
    pub fn parse(url: &str) -> Result<Self, String> {
        let Some(rest) = url.strip_prefix(TIKV_SCHEME) else {
            return Err(format!(
                "--meta {url:?}: expected tikv://<pd-host:port>[,<pd…>]/<keyspace>"
            ));
        };
        let (path, query) = match rest.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (rest, None),
        };
        let Some((hosts, keyspace)) = path.split_once('/') else {
            return Err(format!("--meta {url:?}: the keyspace is missing"));
        };
        let pd: Vec<String> = hosts
            .split(',')
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .map(str::to_string)
            .collect();
        if pd.is_empty() {
            return Err(format!("--meta {url:?}: no PD endpoint"));
        }
        if let Some(bad) = pd.iter().find(|h| !crate::cluster::is_host_port(h)) {
            return Err(format!(
                "--meta {url:?}: PD endpoint {bad:?} is not host:port"
            ));
        }
        let keyspace = keyspace.trim_end_matches('/');
        if keyspace.is_empty() || keyspace.contains('/') {
            return Err(format!("--meta {url:?}: expected one keyspace name"));
        }
        let mut root = Vec::new();
        for pair in query.into_iter().flat_map(|q| q.split('&')) {
            match pair.split_once('=') {
                Some(("root", hex)) => {
                    root = decode_hex(hex)
                        .ok_or_else(|| format!("--meta {url:?}: root {hex:?} is not hex"))?;
                }
                _ => return Err(format!("--meta {url:?}: unknown parameter {pair:?}")),
            }
        }
        Self::tikv(pd, keyspace, root)
    }

    #[cfg(feature = "meta-tikv")]
    fn tikv(pd: Vec<String>, keyspace: &str, root: Vec<u8>) -> Result<Self, String> {
        let tikv = operon_tikv::TikvConfig {
            root,
            ..operon_tikv::TikvConfig::new(pd, keyspace)
        };
        Ok(MetaBackend::Tikv(operon_meta_tikv::TikvMetaConfig::new(
            tikv,
        )))
    }

    #[cfg(not(feature = "meta-tikv"))]
    fn tikv(_pd: Vec<String>, _keyspace: &str, _root: Vec<u8>) -> Result<Self, String> {
        Err("this build has no TiKV metastore (the meta-tikv feature is off)".to_string())
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(all(test, feature = "meta-tikv"))]
mod tests {
    use super::*;

    fn tikv(url: &str) -> operon_meta_tikv::TikvMetaConfig {
        match MetaBackend::parse(url).expect("parse") {
            MetaBackend::Tikv(config) => config,
            MetaBackend::Raft => panic!("expected TiKV"),
        }
    }

    #[test]
    fn a_tikv_url_names_pd_the_keyspace_and_an_optional_root() {
        let config = tikv("tikv://127.0.0.1:2379/loam_meta");
        assert_eq!(config.tikv.pd, ["127.0.0.1:2379"]);
        assert_eq!(config.tikv.keyspace, "loam_meta");
        assert!(config.tikv.root.is_empty());

        let config = tikv("tikv://pd-0:2379,pd-1:2379/loam_meta/?root=00ff1a");
        assert_eq!(config.tikv.pd, ["pd-0:2379", "pd-1:2379"]);
        assert_eq!(config.tikv.keyspace, "loam_meta");
        assert_eq!(config.tikv.root, [0x00, 0xff, 0x1a]);
    }

    #[test]
    fn bad_tikv_urls_are_refused_with_the_reason() {
        for (url, reason) in [
            ("raft://x", "expected tikv://"),
            ("tikv://127.0.0.1:2379", "keyspace is missing"),
            ("tikv:///loam_meta", "no PD endpoint"),
            ("tikv://127.0.0.1/loam_meta", "not host:port"),
            ("tikv://127.0.0.1:2379/", "one keyspace name"),
            ("tikv://127.0.0.1:2379/a/b", "one keyspace name"),
            ("tikv://127.0.0.1:2379/m?root=abc", "not hex"),
            ("tikv://127.0.0.1:2379/m?roots=ab", "unknown parameter"),
        ] {
            let err = MetaBackend::parse(url).expect_err(url);
            assert!(err.contains(reason), "{url}: {err}");
        }
    }
}
