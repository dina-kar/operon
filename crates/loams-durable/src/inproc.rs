//! `worker_inproc`: Loams's in-process worker for the `inproc` scheme, and
//! [`InProcNetwork`], the Resonate SDK's network over it (D1 Task 6, D141).
//!
//! The server routes a message for `inproc://uni@loams/<node>` or
//! `inproc://any@loams/<node>` to [`InProcWorker::process`], which hands the
//! exact JSON the poll transport would write in an SSE `data:` frame to the
//! SDK's `recv` callbacks, on a spawned task. The SDK's requests go straight
//! to [`ResonateServer::process`]. No socket is involved either way.
//!
//! A worker belongs to one [`DurableServer`](crate::DurableServer): its
//! `configure` (a plain `fn`, the plugin ABI) parks the worker under the
//! instance number the server put in `workers.worker_inproc.instance`, and
//! the server takes it back right after `build` ([`take`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use resonate_plugin::types::{Message, RequestEnvelope};
use resonate_plugin::{
    ConfigError, ResonateServer, ResonateWorker, Settings, Unavailable, WorkerDependencies,
    WorkerPlugin,
};
use serde_json::Value;
use tokio::sync::watch;

/// The address scheme this worker serves.
pub const SCHEME: &str = "inproc";

/// The group Loams's runtime subscribes as (D1 Task 6 semantics 1).
pub const GROUP: &str = "loams";

/// The plugin, id `worker_inproc`, scheme `inproc`. The crate name is set
/// explicitly: a crate outside `resonate-*` keeps its whole name as its id.
pub static PLUGIN: WorkerPlugin = WorkerPlugin::new("worker-inproc", &[SCHEME], configure);

/// Everything under `[workers.worker_inproc]`. Loams sets it; `--durable-set`
/// may not (`PROTECTED`).
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Which server's slot to park the worker in. Without one (the migrate
    /// command) the worker is off and `inproc://` is undeliverable.
    #[serde(default)]
    instance: Option<u64>,
}

/// Workers built by `configure`, waiting for their server to take them.
static SLOTS: Mutex<Vec<(u64, Arc<InProcWorker>)>> = Mutex::new(Vec::new());

/// The next instance number (one per server start).
static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

/// A fresh instance number for one server's build.
pub(crate) fn next_instance() -> u64 {
    NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed)
}

/// The worker `configure` parked under `instance`, if any, removed from the
/// slots (so a failed build leaves nothing behind once this runs).
pub(crate) fn take(instance: u64) -> Option<Arc<InProcWorker>> {
    let mut slots = SLOTS.lock().unwrap_or_else(PoisonError::into_inner);
    let at = slots.iter().position(|(n, _)| *n == instance)?;
    Some(slots.swap_remove(at).1)
}

fn configure(
    settings: &Settings<'_>,
    _deps: WorkerDependencies,
) -> Result<Option<Arc<dyn ResonateWorker>>, ConfigError> {
    let config: Config = settings.extract()?;
    let Some(instance) = config.instance else {
        return Ok(None);
    };
    let worker = Arc::new(InProcWorker::default());
    SLOTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((instance, Arc::clone(&worker)));
    Ok(Some(worker))
}

/// A `recv` callback of the SDK.
type Callback = Arc<dyn Fn(String) + Send + Sync>;

/// One subscriber: a runtime's process id and where its messages go.
struct Subscriber {
    key: u64,
    id: String,
    callback: Callback,
}

/// The `inproc` worker: `group → [subscribers]`.
#[derive(Default)]
pub struct InProcWorker {
    groups: Mutex<HashMap<String, Vec<Subscriber>>>,
    next_key: AtomicU64,
    /// Round-robin position for anycast without a preferred id.
    next_any: AtomicU64,
}

impl std::fmt::Debug for InProcWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcWorker")
            .field("subscribers", &self.subscriber_count())
            .finish_non_exhaustive()
    }
}

impl InProcWorker {
    /// Deliver messages for `group`/`id` to `callback` until
    /// [`unsubscribe`](Self::unsubscribe) with the returned key.
    pub fn subscribe(&self, group: &str, id: &str, callback: Callback) -> u64 {
        let key = self.next_key.fetch_add(1, Ordering::Relaxed);
        self.lock()
            .entry(group.to_string())
            .or_default()
            .push(Subscriber {
                key,
                id: id.to_string(),
                callback,
            });
        key
    }

    /// Stop delivering to the subscriber `key`.
    pub fn unsubscribe(&self, key: u64) {
        let mut groups = self.lock();
        for subscribers in groups.values_mut() {
            subscribers.retain(|s| s.key != key);
        }
        groups.retain(|_, subscribers| !subscribers.is_empty());
    }

    /// How many subscribers there are, across every group.
    pub fn subscriber_count(&self) -> usize {
        self.lock().values().map(Vec::len).sum()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<Subscriber>>> {
        self.groups.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The callback `address` reaches: unicast, that id only; anycast, the
    /// named id if it subscribed, else the next subscriber of the group.
    fn route(&self, address: &InProcAddress) -> Option<Callback> {
        let groups = self.lock();
        let subscribers = groups.get(&address.group).filter(|s| !s.is_empty())?;
        let named = address
            .id
            .as_ref()
            .and_then(|id| subscribers.iter().find(|s| &s.id == id));
        match (address.cast, named) {
            (_, Some(subscriber)) => Some(Arc::clone(&subscriber.callback)),
            (Cast::Uni, None) => None,
            (Cast::Any, None) => {
                let n = self.next_any.fetch_add(1, Ordering::Relaxed);
                let at = usize::try_from(n).unwrap_or(0) % subscribers.len();
                Some(Arc::clone(&subscribers[at].callback))
            }
        }
    }
}

#[async_trait::async_trait]
impl ResonateWorker for InProcWorker {
    /// Drop every subscriber; later messages are undeliverable.
    async fn stop(&self) -> Result<(), Unavailable> {
        self.lock().clear();
        Ok(())
    }

    async fn process(&self, address: &str, msg: &Message) -> Result<(), Unavailable> {
        let parsed = InProcAddress::parse(address)?;
        // Exactly the poll transport's bytes: through a `Value`, as it does.
        let body = serde_json::to_value(msg)
            .and_then(|v| serde_json::to_string(&v))
            .map_err(|e| Unavailable::new(format!("cannot serialize message: {e}")))?;
        let Some(callback) = self.route(&parsed) else {
            return Err(Unavailable::new(format!(
                "no in-process subscriber accepted delivery for {address}"
            )));
        };
        tokio::spawn(async move { callback(body) });
        Ok(())
    }
}

/// Unicast or anycast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cast {
    Uni,
    Any,
}

/// An `inproc://` destination: `inproc://<uni|any>@<group>[/<id>]`, parsed
/// as the poll transport parses `poll://`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InProcAddress {
    cast: Cast,
    group: String,
    id: Option<String>,
}

impl InProcAddress {
    fn parse(address: &str) -> Result<Self, Unavailable> {
        let bad = || Unavailable::new(format!("malformed inproc address: {address}"));
        let parsed = url::Url::parse(address).map_err(|_| bad())?;
        if parsed.scheme() != SCHEME {
            return Err(bad());
        }
        let cast = match parsed.username() {
            "uni" => Cast::Uni,
            "any" => Cast::Any,
            _ => return Err(bad()),
        };
        let group = parsed
            .host_str()
            .filter(|g| !g.is_empty())
            .ok_or_else(bad)?
            .to_string();
        let path = parsed.path();
        let id = (path.len() > 1).then(|| path[1..].to_string());
        if cast == Cast::Uni && id.is_none() {
            return Err(bad());
        }
        Ok(Self { cast, group, id })
    }
}

/// The Resonate SDK's [`Network`](resonate_sdk::network::Network) over the
/// embedded server, in process.
///
/// `start` subscribes to the server's [`InProcWorker`] and cannot fail; it
/// waits until the runtime has registered its functions ([`arm`]) so no task
/// arrives before its function exists. `stop` unsubscribes, and every later
/// `send` fails, as it would for a process that went away.
///
/// The server is held weakly: the server holds its router, the router this
/// worker, the worker the SDK's callbacks, and they this network.
///
/// [`arm`]: InProcNetwork::arm
pub struct InProcNetwork {
    server: Weak<dyn ResonateServer>,
    worker: Arc<InProcWorker>,
    pid: String,
    unicast: String,
    anycast: String,
    callbacks: Arc<Mutex<Vec<Callback>>>,
    subscription: Mutex<Option<u64>>,
    stopped: AtomicBool,
    armed: watch::Sender<bool>,
    ready: watch::Sender<bool>,
}

impl std::fmt::Debug for InProcNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcNetwork")
            .field("pid", &self.pid)
            .field("unicast", &self.unicast)
            .finish_non_exhaustive()
    }
}

impl InProcNetwork {
    /// A network for process `pid` of group `loams` on `server` through
    /// `worker`.
    pub fn new(server: Weak<dyn ResonateServer>, worker: Arc<InProcWorker>, pid: &str) -> Self {
        Self {
            server,
            worker,
            pid: pid.to_string(),
            unicast: format!("{SCHEME}://uni@{GROUP}/{pid}"),
            anycast: format!("{SCHEME}://any@{GROUP}/{pid}"),
            callbacks: Arc::new(Mutex::new(Vec::new())),
            subscription: Mutex::new(None),
            stopped: AtomicBool::new(false),
            armed: watch::channel(false).0,
            ready: watch::channel(false).0,
        }
    }

    /// Let `start` subscribe: the functions are registered.
    pub fn arm(&self) {
        self.armed.send_replace(true);
    }

    /// A receiver that turns `true` once `start` has subscribed.
    pub fn ready(&self) -> watch::Receiver<bool> {
        self.ready.subscribe()
    }

    /// One request, in process. The token the SDK read from `RESONATE_TOKEN`
    /// (if any) is dropped: the in-process path has no authentication, and
    /// the server has no use for it.
    async fn call(&self, req: &str) -> Result<String, String> {
        let server = self
            .server
            .upgrade()
            .ok_or_else(|| "the durable server has stopped".to_string())?;
        let mut req: Value =
            serde_json::from_str(req).map_err(|e| format!("a malformed request: {e}"))?;
        if let Some(head) = req.get_mut("head").and_then(Value::as_object_mut) {
            head.remove("auth");
        }
        let envelope: RequestEnvelope =
            serde_json::from_value(req).map_err(|e| format!("a malformed request: {e}"))?;
        let response = server
            .process(&envelope)
            .await
            .map_err(|e| format!("the durable server is unavailable: {e}"))?;
        serde_json::to_string(&response).map_err(|e| format!("an unreadable response: {e}"))
    }
}

#[async_trait::async_trait]
impl resonate_sdk::network::Network for InProcNetwork {
    fn pid(&self) -> &str {
        &self.pid
    }

    fn group(&self) -> &str {
        GROUP
    }

    fn unicast(&self) -> &str {
        &self.unicast
    }

    fn anycast(&self) -> &str {
        &self.anycast
    }

    async fn start(&self) -> resonate_sdk::error::Result<()> {
        let mut armed = self.armed.subscribe();
        // The sender lives in `self`, so this ends only when armed.
        let _ = armed.wait_for(|armed| *armed).await;
        if self.stopped.load(Ordering::SeqCst) {
            return Ok(());
        }
        let callbacks = Arc::clone(&self.callbacks);
        let deliver: Callback = Arc::new(move |msg: String| {
            let callbacks = callbacks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for callback in callbacks {
                callback(msg.clone());
            }
        });
        let key = self.worker.subscribe(GROUP, &self.pid, deliver);
        *self
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(key);
        self.ready.send_replace(true);
        Ok(())
    }

    async fn stop(&self) -> resonate_sdk::error::Result<()> {
        self.stopped.store(true, Ordering::SeqCst);
        // A start still waiting to be armed wakes up and sees `stopped`.
        self.armed.send_replace(true);
        let key = self
            .subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(key) = key {
            self.worker.unsubscribe(key);
        }
        self.callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Ok(())
    }

    async fn send(&self, req: String) -> resonate_sdk::error::Result<String> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(resonate_sdk::error::Error::NetworkError(
                "the durable runtime has stopped".into(),
            ));
        }
        self.call(&req)
            .await
            .map_err(resonate_sdk::error::Error::NetworkError)
    }

    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        self.callbacks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::from(callback));
    }

    fn target_resolver(&self, target: &str) -> String {
        format!("{SCHEME}://any@{target}")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use resonate_plugin::types::{ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, MessageHead};
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn the_id_is_worker_inproc() {
        assert_eq!(PLUGIN.id(), "worker_inproc");
        assert_eq!(PLUGIN.schemes, &["inproc"]);
    }

    #[test]
    fn addresses_parse_like_poll_addresses() {
        let uni = InProcAddress::parse("inproc://uni@loams/7").expect("unicast");
        assert_eq!(
            uni,
            InProcAddress {
                cast: Cast::Uni,
                group: "loams".into(),
                id: Some("7".into())
            }
        );
        let any = InProcAddress::parse("inproc://any@loams").expect("anycast");
        assert_eq!(any.cast, Cast::Any);
        assert_eq!(any.id, None);
        for bad in [
            "inproc://loams/7",
            "inproc://all@loams/7",
            "poll://uni@loams/7",
            "inproc://uni@loams",
            "not a url",
        ] {
            assert!(InProcAddress::parse(bad).is_err(), "{bad}");
        }
    }

    fn execute(id: &str) -> Message {
        Message::Execute(ExecuteMsg {
            kind: "execute".into(),
            head: MessageHead {
                server_url: "http://127.0.0.1:8001".into(),
            },
            data: ExecuteMsgData {
                task: ExecuteMsgTask {
                    id: id.into(),
                    version: 3,
                },
            },
        })
    }

    /// A subscriber that forwards what it gets to a channel.
    fn channel() -> (Callback, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Arc::new(move |msg: String| {
                let _ = tx.send(msg);
            }),
            rx,
        )
    }

    async fn next(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("delivered in time")
            .expect("open")
    }

    struct NoServer;

    #[async_trait::async_trait]
    impl ResonateServer for NoServer {
        async fn process(
            &self,
            _req: &RequestEnvelope,
        ) -> Result<resonate_plugin::types::ResponseEnvelope, Unavailable> {
            unreachable!("never called")
        }
    }

    /// The bytes a subscriber gets are the bytes the poll transport puts in
    /// an SSE `data:` frame for the same message.
    #[tokio::test]
    async fn delivers_the_poll_transport_bytes() {
        let poll = resonate_transport_http_poll::PollRegistry::new(
            Weak::<NoServer>::new() as Weak<dyn ResonateServer>,
            resonate_transport_http_poll::Config::default(),
        );
        let (_conn, mut sse) = poll.register("loams", "7").await.expect("capacity");
        poll.process("poll://uni@loams/7", &execute("t1"))
            .await
            .expect("poll delivery");
        let expected = sse.recv().await.expect("an SSE frame");

        let worker = InProcWorker::default();
        let (callback, mut rx) = channel();
        worker.subscribe("loams", "7", callback);
        worker
            .process("inproc://uni@loams/7", &execute("t1"))
            .await
            .expect("inproc delivery");
        assert_eq!(next(&mut rx).await, expected);
    }

    #[tokio::test]
    async fn unicast_reaches_that_node_only_and_anycast_prefers_it() {
        let worker = InProcWorker::default();
        let (one, mut rx1) = channel();
        let (two, mut rx2) = channel();
        worker.subscribe("loams", "1", one);
        let key2 = worker.subscribe("loams", "2", two);

        worker
            .process("inproc://uni@loams/2", &execute("a"))
            .await
            .expect("to node 2");
        assert!(next(&mut rx2).await.contains("\"a\""));
        assert!(
            worker
                .process("inproc://uni@loams/3", &execute("b"))
                .await
                .is_err(),
            "no node 3"
        );
        worker
            .process("inproc://any@loams/1", &execute("c"))
            .await
            .expect("preferred node 1");
        assert!(next(&mut rx1).await.contains("\"c\""));
        // An anycast naming a node that is not here goes to one that is.
        worker.unsubscribe(key2);
        worker
            .process("inproc://any@loams/2", &execute("d"))
            .await
            .expect("any node");
        assert!(next(&mut rx1).await.contains("\"d\""));
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn no_subscriber_is_unavailable() {
        let worker = InProcWorker::default();
        let err = worker
            .process("inproc://any@loams", &execute("t"))
            .await
            .expect_err("nobody listens");
        assert!(
            err.to_string().contains("no in-process subscriber"),
            "{err}"
        );
        let (callback, _rx) = channel();
        worker.subscribe("loams", "1", callback);
        worker.stop().await.expect("stop");
        assert_eq!(worker.subscriber_count(), 0);
        assert!(
            worker
                .process("inproc://any@loams", &execute("t"))
                .await
                .is_err()
        );
    }

    #[test]
    fn configure_parks_the_worker_under_its_instance() {
        let instance = next_instance();
        let configuration = resonate_plugin::Loader::new()
            .set("workers.worker_inproc.instance", &instance.to_string())
            .expect("key")
            .load();
        let deps = WorkerDependencies::new(
            Weak::<NoServer>::new() as Weak<dyn ResonateServer>,
            resonate_plugin::Routes::new(),
        );
        let built = configure(&configuration.worker(&PLUGIN.id()), deps).expect("configure");
        assert!(built.is_some());
        assert!(take(instance).is_some(), "parked");
        assert!(take(instance).is_none(), "taken once");

        // No instance: off, nothing parked.
        let configuration = resonate_plugin::Loader::new().load();
        let deps = WorkerDependencies::new(
            Weak::<NoServer>::new() as Weak<dyn ResonateServer>,
            resonate_plugin::Routes::new(),
        );
        assert!(
            configure(&configuration.worker(&PLUGIN.id()), deps)
                .expect("configure")
                .is_none()
        );
    }
}
