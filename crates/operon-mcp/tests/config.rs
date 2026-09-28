//! `McpConfig` defaults and validation (plan M1.6 Task 7).

use std::time::Duration;

use operon_mcp::{McpConfig, McpConfigError};

#[test]
fn default_config_is_loopback_only() {
    let config = McpConfig::default();
    assert_eq!(config.allowed_hosts, ["localhost", "127.0.0.1", "::1"]);
    assert!(config.allowed_origins.is_empty());
    assert!(!config.strict_stateless);
    assert_eq!(config.path, "/mcp");
    assert_eq!(config.namespace, "default");
    assert_eq!(config.memory_collection, "memories");
    assert_eq!(config.search_max_limit, 100);
    assert_eq!(config.get_max_ids, 100);
    assert_eq!(config.sql_max_rows, 1_000);
    assert_eq!(config.sql_timeout, Duration::from_secs(30));
    assert_eq!(config.max_output_bytes, 1_048_576);
    assert_eq!(config.tools_ttl, Duration::from_secs(600));
    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn config_rejects_bad_paths() {
    for path in ["mcp", "/", "/v1/mcp", "/health", "/ready", ""] {
        let config = McpConfig {
            path: path.to_string(),
            ..McpConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(McpConfigError::Path(path.to_string())),
            "{path:?}"
        );
    }
    let config = McpConfig {
        path: "/tools/mcp".to_string(),
        ..McpConfig::default()
    };
    assert_eq!(config.validate(), Ok(()));
}

#[test]
fn config_rejects_zero_limits() {
    type Set = fn(&mut McpConfig);
    let cases: [(&str, Set); 5] = [
        ("search_max_limit", |c| c.search_max_limit = 0),
        ("get_max_ids", |c| c.get_max_ids = 0),
        ("sql_max_rows", |c| c.sql_max_rows = 0),
        ("sql_timeout", |c| c.sql_timeout = Duration::ZERO),
        ("max_output_bytes", |c| c.max_output_bytes = 0),
    ];
    for (name, set) in cases {
        let mut config = McpConfig::default();
        set(&mut config);
        assert_eq!(
            config.validate(),
            Err(McpConfigError::Limit(name)),
            "{name}"
        );
    }
}

#[test]
fn an_empty_namespace_is_rejected() {
    let config = McpConfig {
        namespace: String::new(),
        ..McpConfig::default()
    };
    assert_eq!(config.validate(), Err(McpConfigError::Namespace));
}
