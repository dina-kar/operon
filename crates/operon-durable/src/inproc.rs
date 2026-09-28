//! `worker_inproc`: Loam's in-process worker for the `inproc` scheme.
//!
//! A placeholder until D1 Task 6: it claims the scheme and configures to
//! nothing, so the router reports `inproc://` addresses undeliverable and a
//! task's retry timeout recovers them once the real worker exists.

use std::sync::Arc;

use resonate_plugin::{ConfigError, ResonateWorker, Settings, WorkerDependencies, WorkerPlugin};

/// The plugin, id `worker_inproc`, scheme `inproc`.
pub static PLUGIN: WorkerPlugin = WorkerPlugin::new("worker-inproc", &["inproc"], configure);

// The signature is the plugin ABI's; Task 6 returns a worker here.
#[allow(clippy::unnecessary_wraps)]
fn configure(
    _settings: &Settings<'_>,
    _deps: WorkerDependencies,
) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_id_is_worker_inproc() {
        assert_eq!(super::PLUGIN.id(), "worker_inproc");
        assert_eq!(super::PLUGIN.schemes, &["inproc"]);
    }
}
