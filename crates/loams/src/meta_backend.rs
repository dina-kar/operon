//! Which metastore a single-process Loams runs on (R1 plan Task 6, D124):
//! the embedded openraft store (the default) or TiKV, chosen with
//! `--meta tikv://<pd-host:port>[,<pd…>]/<keyspace>[?root=<hex>]` on
//! `loams dev` and `loams standalone`.

/// The metastore of [`ServerConfig::meta`](crate::ServerConfig::meta).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MetaBackend {
    /// The embedded single-node openraft store in `<data_dir>/meta`.
    #[default]
    Raft,
    /// The TiKV metastore in a keyspace (`loams_meta` in production). The
    /// server also runs the cluster MVCC GC loop on its handle (R1 plan
    /// Task 3).
    #[cfg(feature = "tikv")]
    Tikv(loams_meta_tikv::TikvMetaConfig),
}

/// Why a build without the `tikv` feature refuses a `tikv://` URL.
pub const NO_TIKV_FEATURE: &str = "--meta tikv://…: this loams was built without the tikv feature; \
     rebuild with `cargo build -p loams --features tikv` to use the TiKV metastore";

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

    #[cfg(feature = "tikv")]
    fn tikv(pd: Vec<String>, keyspace: &str, root: Vec<u8>) -> Result<Self, String> {
        let tikv = loams_tikv::TikvConfig {
            root,
            ..loams_tikv::TikvConfig::new(pd, keyspace)
        };
        Ok(MetaBackend::Tikv(loams_meta_tikv::TikvMetaConfig::new(
            tikv,
        )))
    }

    #[cfg(not(feature = "tikv"))]
    fn tikv(_pd: Vec<String>, _keyspace: &str, _root: Vec<u8>) -> Result<Self, String> {
        Err(NO_TIKV_FEATURE.to_string())
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

#[cfg(all(test, not(feature = "tikv")))]
mod no_tikv_tests {
    use super::*;

    /// Owner ruling T7-3: without the feature, a well-formed `tikv://` URL is
    /// refused with an error naming the feature.
    #[test]
    fn a_tikv_url_without_the_feature_names_the_feature() {
        let err = MetaBackend::parse("tikv://127.0.0.1:2379/loams_meta").expect_err("refused");
        assert_eq!(err, NO_TIKV_FEATURE);
        assert!(err.contains("built without the tikv feature"), "{err}");
        assert!(err.contains("--features tikv"), "{err}");
    }
}

#[cfg(all(test, feature = "tikv"))]
mod tests {
    use super::*;

    fn tikv(url: &str) -> loams_meta_tikv::TikvMetaConfig {
        match MetaBackend::parse(url).expect("parse") {
            MetaBackend::Tikv(config) => config,
            MetaBackend::Raft => panic!("expected TiKV"),
        }
    }

    #[test]
    fn a_tikv_url_names_pd_the_keyspace_and_an_optional_root() {
        let config = tikv("tikv://127.0.0.1:2379/loams_meta");
        assert_eq!(config.tikv.pd, ["127.0.0.1:2379"]);
        assert_eq!(config.tikv.keyspace, "loams_meta");
        assert!(config.tikv.root.is_empty());

        let config = tikv("tikv://pd-0:2379,pd-1:2379/loams_meta/?root=00ff1a");
        assert_eq!(config.tikv.pd, ["pd-0:2379", "pd-1:2379"]);
        assert_eq!(config.tikv.keyspace, "loams_meta");
        assert_eq!(config.tikv.root, [0x00, 0xff, 0x1a]);
    }

    #[test]
    fn bad_tikv_urls_are_refused_with_the_reason() {
        for (url, reason) in [
            ("raft://x", "expected tikv://"),
            ("tikv://127.0.0.1:2379", "keyspace is missing"),
            ("tikv:///loams_meta", "no PD endpoint"),
            ("tikv://127.0.0.1/loams_meta", "not host:port"),
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
