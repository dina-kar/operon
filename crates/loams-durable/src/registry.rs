//! The plugins the embedded server is built from.

use resonate_base::Registry;
use resonate_plugin::WorkerPlugin;

/// SQLite, native TiKV (feature `tikv`), MySQL (feature `mysql`), the poll and push transports, Loams's
/// in-process worker and the HTTP gateway. Push is linked but configured off
/// unless `--durable-push` is given.
pub fn registry() -> Registry {
    with_workers(&[])
}

/// [`registry`] plus `extra` workers (tests inject routes this way).
pub(crate) fn with_workers(extra: &[&'static WorkerPlugin]) -> Registry {
    let registry = Registry::new().server(&resonate_server_sqlite::PLUGIN);
    #[cfg(feature = "mysql")]
    let registry = registry.server(&resonate_server_mysql::PLUGIN);
    #[cfg(feature = "tikv")]
    let registry = registry.server(&crate::tikv::PLUGIN);
    let mut registry = registry
        .worker(&resonate_transport_http_poll::PLUGIN)
        .worker(&resonate_transport_http_push::PLUGIN)
        .worker(&crate::inproc::PLUGIN);
    for plugin in extra {
        registry = registry.worker(plugin);
    }
    registry.gateway(&resonate_gateway_http::PLUGIN)
}

/// Every `<section>.<plugin id>` in `registry`, for `--durable-set` checks.
pub(crate) fn carried(registry: &Registry) -> Vec<String> {
    let servers = registry
        .servers()
        .iter()
        .map(|p| format!("servers.{}", p.id()));
    let workers = registry
        .workers()
        .iter()
        .map(|p| format!("workers.{}", p.id()));
    let gateways = registry
        .gateways()
        .iter()
        .map(|p| format!("gateways.{}", p.id()));
    servers.chain(workers).chain(gateways).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_is_consistent() {
        let registry = registry();
        registry.check().expect("no duplicate ids or schemes");
        let carried = carried(&registry);
        for id in [
            "servers.server_sqlite",
            "workers.transport_http_poll",
            "workers.transport_http_push",
            "workers.worker_inproc",
            "gateways.gateway_http",
        ] {
            assert!(carried.iter().any(|c| c == id), "{id} in {carried:?}");
        }
        assert_eq!(
            carried.iter().any(|c| c == "servers.server_mysql"),
            cfg!(feature = "mysql")
        );
        assert_eq!(
            carried.iter().any(|c| c == "servers.server_tikv"),
            cfg!(feature = "tikv")
        );
    }
}
