//! The cluster test harness (R1 plan Global Constraints).
//!
//! Every test that needs TiKV starts with
//!
//! ```ignore
//! let Some(cluster) = loams_tikv::testing::cluster().await else { return };
//! let tikv = cluster.connect(loams_tikv::testing::TEST_META).await;
//! ```
//!
//! [`cluster`] returns `None`, printing `skipped: <test> needs LOAMS_TEST_PD`,
//! when `LOAMS_TEST_PD` is unset, so the suites pass on a machine without a
//! cluster. When it is set (CI's `tikv` job sets it), an unreachable PD fails
//! the test instead of skipping it.
//!
//! Tests share the three test keyspaces the playground pre-allocates and
//! isolate by a random 8-byte root prefix: [`TestCluster::config`] draws a new
//! root on every call. A test never creates or deletes a keyspace.

use std::time::Duration;

use rand::RngCore;

use crate::{Tikv, TikvConfig};

/// The environment variable naming the test cluster's PD endpoints
/// (`host:port[,host:port…]`).
pub const PD_ENV: &str = "LOAMS_TEST_PD";
/// Overrides PD's HTTP API base URL (default `http://<first endpoint>`).
pub const PD_HTTP_ENV: &str = "LOAMS_TEST_PD_HTTP";
/// The TiKV status address (`host:port`) of the test cluster's store, for
/// its metrics and online config (default [`DEFAULT_TIKV_STATUS`]).
pub const TIKV_STATUS_ENV: &str = "LOAMS_TEST_TIKV_STATUS";
/// The playground's TiKV status address at port offset 17000.
pub const DEFAULT_TIKV_STATUS: &str = "127.0.0.1:37180";
/// Set (to anything non-empty) by CI's nightly job: nightly-only cluster
/// tests run only then.
pub const NIGHTLY_ENV: &str = "LOAMS_TEST_NIGHTLY";

/// Whether nightly-only tests run; prints a `skipped:` line naming the
/// current test when they do not.
pub fn nightly() -> bool {
    let on = std::env::var(NIGHTLY_ENV).is_ok_and(|v| !v.trim().is_empty());
    if !on {
        eprintln!(
            "skipped: {} is nightly-only ({NIGHTLY_ENV})",
            current_test()
        );
    }
    on
}

/// The test cluster's TiKV status base URL (`http://host:port`).
pub fn tikv_status() -> String {
    let addr = std::env::var(TIKV_STATUS_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_TIKV_STATUS.to_string());
    let addr = addr.trim().trim_end_matches('/');
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// The metastore test keyspace.
pub const TEST_META: &str = "loams_test_meta";
/// The Live test keyspace.
pub const TEST_LIVE: &str = "loams_test_live";
/// The keyspace of the test TiDB.
pub const TEST_SQL: &str = "loams_test_sql";
/// The keyspaces tests may use, pre-allocated by `deploy/tikv/pd.toml`.
pub const TEST_KEYSPACES: [&str; 3] = [TEST_META, TEST_LIVE, TEST_SQL];

/// The length of a test's random root prefix.
pub const ROOT_LEN: usize = 8;

/// A reachable test cluster.
#[derive(Debug, Clone)]
pub struct TestCluster {
    /// PD endpoints.
    pub pd: Vec<String>,
    /// PD's HTTP API base URL.
    pub pd_http: String,
}

/// The test cluster named by `LOAMS_TEST_PD`, or `None` (and a `skipped:`
/// line naming the current test) when the variable is unset.
///
/// # Panics
///
/// When the variable is set but PD's HTTP API does not answer: a configured
/// cluster that is down must fail the suite, not skip it.
pub async fn cluster() -> Option<TestCluster> {
    let pd: Vec<String> = std::env::var(PD_ENV)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect();
    if pd.is_empty() {
        eprintln!("skipped: {} needs {PD_ENV}", current_test());
        return None;
    }
    let pd_http = match std::env::var(PD_HTTP_ENV) {
        Ok(url) if !url.trim().is_empty() => url.trim().trim_end_matches('/').to_string(),
        _ => TikvConfig::new(pd.clone(), "").pd_http_url(),
    };
    let url = format!("{pd_http}/pd/api/v1/version");
    let answer = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("an HTTP client")
        .get(&url)
        .send()
        .await;
    match answer {
        Ok(response) if response.status().is_success() => {}
        Ok(response) => panic!(
            "{PD_ENV} is set but PD answered {} at {url}",
            response.status()
        ),
        Err(e) => panic!(
            "{PD_ENV} is set but PD does not answer at {url} ({e}); start the playground \
             (scripts/tikv/playground.sh start) or unset {PD_ENV}"
        ),
    }
    Some(TestCluster { pd, pd_http })
}

impl TestCluster {
    /// A configuration for `keyspace` (one of [`TEST_KEYSPACES`]) under a new
    /// random root.
    ///
    /// # Panics
    ///
    /// When `keyspace` is not a test keyspace.
    pub fn config(&self, keyspace: &str) -> TikvConfig {
        assert!(
            TEST_KEYSPACES.contains(&keyspace),
            "tests use only the keyspaces {TEST_KEYSPACES:?}, not '{keyspace}'"
        );
        TikvConfig {
            root: random_root(),
            pd_http: Some(self.pd_http.clone()),
            ..TikvConfig::new(self.pd.clone(), keyspace)
        }
    }

    /// Connects a handle on `keyspace` under a new random root.
    ///
    /// # Panics
    ///
    /// When the connect fails.
    pub async fn connect(&self, keyspace: &str) -> Tikv {
        let config = self.config(keyspace);
        match Tikv::connect(config).await {
            Ok(tikv) => tikv,
            Err(e) => panic!("connect to the test keyspace {keyspace}: {e}"),
        }
    }
}

/// A random root prefix of [`ROOT_LEN`] bytes.
pub fn random_root() -> Vec<u8> {
    let mut root = vec![0; ROOT_LEN];
    rand::rng().fill_bytes(&mut root);
    root
}

/// The running test's name: libtest names each test's thread after it.
fn current_test() -> String {
    match std::thread::current().name() {
        Some(name) if name != "main" => name.to_string(),
        _ => "this test".to_string(),
    }
}
