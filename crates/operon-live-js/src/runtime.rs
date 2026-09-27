//! The engine's workers (R1 plan Task 13 semantics 2a and 3; design §20
//! §6.3).
//!
//! A bundle gets `contexts` worker threads. Each owns a QuickJS runtime
//! (with the memory limit, the stack limit, the interrupt handler and the
//! `loam:server` loader) and keeps one fresh context warm: the prelude and
//! the bundle already evaluated. A context runs exactly one call and is
//! then dropped, whether the call returned or threw, and the worker warms
//! the next one before it takes another call; after a timeout or an
//! out-of-memory error the runtime is recreated too.
//!
//! A call is a conversation between the caller's future (which owns the
//! [`Host`], a `LiveTxn`) and the worker: the worker sends each `ctx.db`
//! call over a channel and blocks, with the CPU clock paused, until the
//! caller answers; the caller answers calls in order. An error that must
//! reach the runner (a storage error, an exceeded limit) ends the call at
//! the caller at once: the worker is told to abort and runs no more of the
//! function, so a JavaScript `catch` never sees it.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use operon_live::{FnKind, LiveError, LiveValue};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rquickjs::loader::{BuiltinResolver, ImportAttributes, Loader};
use rquickjs::module::Declared;
use rquickjs::object::Filter;
use rquickjs::promise::PromiseState;
use rquickjs::{
    ArrayBuffer, BigInt, Context, Ctx, Exception, Function, Module, Object, Persistent, Promise,
    Runtime, Type, Value,
};

use crate::host::{Host, Invocation, is_fatal};
use crate::limits::{Budget, JS_STACK, JsConfig, WORKER_STACK};

/// The module name of the server API.
pub const SERVER_MODULE: &str = "loam:server";
/// The module name the bundle is evaluated under.
pub const BUNDLE_MODULE: &str = "bundle.js";
const PRELUDE: &str = include_str!("prelude.js");

/// How deep a value may nest.
const MAX_DEPTH: usize = 64;

/// How many parts (scalars, arrays and objects) one value copied out of
/// JavaScript may have. Shared references are copied at every use, so a
/// value QuickJS holds in a few arrays can have 2^64 parts; the copy stops
/// here rather than growing outside the runtime's memory limit (review of
/// #93). 32 000 scanned documents of a few dozen fields each fit.
const MAX_VALUE_PARTS: usize = 1 << 21;

/// The most bytes of strings and byte buffers copied out of one JavaScript
/// value: the runtime's default memory limit. A value that repeats one
/// large string is small in QuickJS and large once copied (review of #97).
const MAX_VALUE_BYTES: usize = 64 << 20;

/// What copying one value out of JavaScript has used so far.
#[derive(Debug, Default)]
struct Copied {
    parts: usize,
    bytes: usize,
}

impl Copied {
    /// Counts `n` more copied bytes, refusing the value past its budget.
    fn bytes(&mut self, n: usize) -> Result<(), String> {
        self.bytes = self.bytes.saturating_add(n);
        if self.bytes > MAX_VALUE_BYTES {
            return Err(format!(
                "a value of more than {MAX_VALUE_BYTES} bytes of strings and buffers"
            ));
        }
        Ok(())
    }
}

/// A bundle's functions, `(path, kind)`.
type Functions = Vec<(String, FnKind)>;

/// Where the first worker reports whether the bundle loaded.
type Ready = mpsc::Sender<Result<Functions, LiveError>>;

/// One call handed to a worker.
struct Job {
    path: String,
    kind: FnKind,
    args: LiveValue,
    invocation: Invocation,
    to_caller: tokio::sync::mpsc::UnboundedSender<ToCaller>,
    replies: mpsc::Receiver<Reply>,
}

/// From a worker to the caller.
enum ToCaller {
    /// A `ctx.db` call.
    Host {
        id: u64,
        op: String,
        args: LiveValue,
    },
    /// The call's result.
    Done(Result<LiveValue, LiveError>),
}

/// From the caller to a worker.
enum Reply {
    Ok(u64, LiveValue),
    Err(u64, LiveError),
    /// Stop: the caller has its answer (a fatal host error).
    Abort,
}

/// A bundle's workers.
pub(crate) struct Pool {
    jobs: mpsc::Sender<Job>,
    functions: Vec<(String, FnKind)>,
}

impl Pool {
    /// Starts `config.contexts` workers for `source`. The first one
    /// evaluates the bundle before this returns, which validates it.
    pub(crate) fn start(source: &str, config: &JsConfig) -> Result<Self, LiveError> {
        let source: Arc<str> = Arc::from(source);
        let (jobs, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let (ready_tx, ready_rx) = mpsc::channel();
        spawn_worker(source.clone(), config.clone(), rx.clone(), Some(ready_tx))?;
        let functions = ready_rx
            .recv()
            .map_err(|_| LiveError::Internal("the first function worker stopped".into()))??;
        for _ in 1..config.contexts.max(1) {
            spawn_worker(source.clone(), config.clone(), rx.clone(), None)?;
        }
        Ok(Pool { jobs, functions })
    }

    pub(crate) fn functions(&self) -> &[(String, FnKind)] {
        &self.functions
    }

    /// Runs the call `path` with `args`, answering its `ctx.db` calls with
    /// `host`.
    pub(crate) async fn invoke(
        &self,
        path: &str,
        kind: FnKind,
        host: &mut dyn Host,
        invocation: Invocation,
        args: LiveValue,
    ) -> Result<LiveValue, LiveError> {
        let (to_caller, mut from_worker) = tokio::sync::mpsc::unbounded_channel();
        let (reply_tx, replies) = mpsc::channel();
        self.jobs
            .send(Job {
                path: path.to_string(),
                kind,
                args,
                invocation,
                to_caller,
                replies,
            })
            .map_err(|_| LiveError::Internal("the function workers have stopped".into()))?;
        // Dropping this future drops `reply_tx`, which stops the worker.
        while let Some(msg) = from_worker.recv().await {
            match msg {
                ToCaller::Done(result) => return result,
                ToCaller::Host { id, op, args } => {
                    let answer = match host.call(&op, args).await {
                        Ok(v) => Reply::Ok(id, v),
                        Err(e) if is_fatal(&e) => {
                            let _ = reply_tx.send(Reply::Abort);
                            return Err(e);
                        }
                        Err(e) => Reply::Err(id, e),
                    };
                    // A worker that has gone away shows as the end of the
                    // channel on the next receive.
                    let _ = reply_tx.send(answer);
                }
            }
        }
        Err(LiveError::Internal(
            "the function worker stopped without an answer".into(),
        ))
    }
}

fn spawn_worker(
    source: Arc<str>,
    config: JsConfig,
    jobs: Arc<Mutex<mpsc::Receiver<Job>>>,
    ready: Option<Ready>,
) -> Result<(), LiveError> {
    std::thread::Builder::new()
        .name("loam-js".into())
        .stack_size(WORKER_STACK)
        .spawn(move || worker(&source, &config, &jobs, ready))
        .map(|_| ())
        .map_err(|e| LiveError::Internal(format!("starting a function worker: {e}")))
}

/// A worker's loop: warm a context, take a call, run it, drop the context.
fn worker(
    source: &str,
    config: &JsConfig,
    jobs: &Mutex<mpsc::Receiver<Job>>,
    mut ready: Option<Ready>,
) {
    let mut engine = match Engine::new(config) {
        Ok(e) => e,
        Err(e) => {
            if let Some(ready) = ready.take() {
                let _ = ready.send(Err(e));
            }
            return;
        }
    };
    loop {
        let warm = engine.warm(source);
        if let Some(ready) = ready.take() {
            let sent = warm
                .as_ref()
                .map(|w| w.functions.clone())
                .map_err(Clone::clone);
            let failed = sent.is_err();
            let _ = ready.send(sent);
            if failed {
                return;
            }
        }
        let job = {
            let rx = jobs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match rx.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };
        let fresh_runtime = match warm {
            Ok(warm) => engine.run(warm, job),
            Err(e) => {
                let _ = job.to_caller.send(ToCaller::Done(Err(e)));
                true
            }
        };
        if fresh_runtime {
            match Engine::new(config) {
                Ok(e) => engine = e,
                Err(e) => {
                    tracing::error!(error = %e, "a function worker could not recreate its runtime");
                    return;
                }
            }
        }
    }
}

/// The per-call state the host functions share.
struct State {
    now_ms: u64,
    rng: ChaCha8Rng,
    to_caller: Option<tokio::sync::mpsc::UnboundedSender<ToCaller>>,
    next_id: u64,
    /// Host calls awaiting an answer: their promise's resolve and reject.
    pending: HashMap<u64, (Persistent<Function<'static>>, Persistent<Function<'static>>)>,
    /// Host call errors handed to the function, by id.
    errors: HashMap<u64, LiveError>,
    helpers: Option<Helpers>,
}

/// Prelude functions the conversions need.
#[derive(Clone)]
struct Helpers {
    bytes_of: Persistent<Function<'static>>,
    fits_i64: Persistent<Function<'static>>,
}

impl State {
    fn new() -> Self {
        State {
            now_ms: 0,
            rng: ChaCha8Rng::from_seed([0; 32]),
            to_caller: None,
            next_id: 0,
            pending: HashMap::new(),
            errors: HashMap::new(),
            helpers: None,
        }
    }
}

/// Loads `loam:server` into every context (rquickjs's `BuiltinLoader`
/// hands a module out once per runtime); every other import is refused by
/// the resolver.
struct ServerLoader;

impl Loader for ServerLoader {
    fn load<'js>(
        &mut self,
        ctx: &Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<Module<'js, Declared>> {
        if name == SERVER_MODULE {
            Module::declare(ctx.clone(), name, PRELUDE)
        } else {
            Err(rquickjs::Error::new_loading(name))
        }
    }
}

/// One worker's runtime.
struct Engine {
    runtime: Runtime,
    budget: Rc<Budget>,
    config: JsConfig,
}

/// A warm context: the prelude and the bundle evaluated.
struct Warm {
    context: Context,
    host: Persistent<Object<'static>>,
    state: Rc<RefCell<State>>,
    functions: Vec<(String, FnKind)>,
}

impl Drop for Warm {
    /// The host functions' closures hold the state, and the state holds
    /// prelude functions: break the cycle so the context's objects are freed
    /// before the runtime.
    fn drop(&mut self) {
        let mut s = self.state.borrow_mut();
        s.to_caller = None;
        s.pending.clear();
        s.helpers = None;
    }
}

impl Engine {
    fn new(config: &JsConfig) -> Result<Self, LiveError> {
        let runtime = Runtime::new().map_err(|e| internal("creating a QuickJS runtime", &e))?;
        runtime.set_memory_limit(config.memory_limit);
        runtime.set_max_stack_size(JS_STACK);
        let budget = Rc::new(Budget::default());
        let check = budget.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || check.interrupt())));
        runtime.set_loader(
            BuiltinResolver::default().with_module(SERVER_MODULE),
            ServerLoader,
        );
        Ok(Engine {
            runtime,
            budget,
            config: config.clone(),
        })
    }

    /// A fresh context with the prelude and `source` evaluated. Errors are
    /// the bundle's ([`LiveError::InvalidArgument`]).
    fn warm(&self, source: &str) -> Result<Warm, LiveError> {
        let context =
            Context::full(&self.runtime).map_err(|e| internal("creating a context", &e))?;
        let state = Rc::new(RefCell::new(State::new()));
        self.budget.arm(self.config.cpu_limit);
        let warmed = context.with(|ctx| warm_in(&ctx, &state, source));
        self.budget.disarm();
        let (host, functions) = warmed.map_err(|e| {
            if self.budget.fired() {
                LiveError::InvalidArgument(format!(
                    "the bundle's top-level code ran past the CPU limit of {:?}",
                    self.budget.limit()
                ))
            } else {
                e
            }
        })?;
        Ok(Warm {
            context,
            host,
            state,
            functions,
        })
    }

    /// Runs `job` in `warm` and drops the context; returns whether the
    /// runtime must be recreated.
    fn run(&self, warm: Warm, job: Job) -> bool {
        {
            let mut s = warm.state.borrow_mut();
            s.now_ms = job.invocation.now_ms;
            s.rng = ChaCha8Rng::from_seed(job.invocation.seed);
            s.to_caller = Some(job.to_caller.clone());
        }
        self.budget.arm(self.config.cpu_limit);
        let outcome = warm
            .context
            .with(|ctx| run_in(&ctx, &warm, &job, &self.budget));
        self.budget.disarm();
        let fresh = match &outcome {
            Outcome::Done(Err(
                LiveError::FunctionTimeout(_) | LiveError::FunctionOutOfMemory(_),
            ))
            | Outcome::Aborted => true,
            Outcome::Done(_) => false,
        };
        drop(warm);
        self.runtime.run_gc();
        if let Outcome::Done(result) = outcome {
            let _ = job.to_caller.send(ToCaller::Done(result));
        }
        fresh
    }
}

enum Outcome {
    Done(Result<LiveValue, LiveError>),
    /// The caller has its answer or has gone away.
    Aborted,
}

fn warm_in<'js>(
    ctx: &Ctx<'js>,
    state: &Rc<RefCell<State>>,
    source: &str,
) -> Result<(Persistent<Object<'static>>, Functions), LiveError> {
    let bundle = |e: rquickjs::Error| js_error(ctx, e, "the bundle failed to load");
    let host = Object::new(ctx.clone()).map_err(bundle)?;
    host.set("call", host_call_fn(ctx, state.clone()).map_err(bundle)?)
        .map_err(bundle)?;
    let now_state = state.clone();
    host.set(
        "now",
        Function::new(ctx.clone(), move || now_state.borrow().now_ms as f64).map_err(bundle)?,
    )
    .map_err(bundle)?;
    let random_state = state.clone();
    host.set(
        "random",
        Function::new(ctx.clone(), move || {
            random_state.borrow_mut().rng.random::<f64>()
        })
        .map_err(bundle)?,
    )
    .map_err(bundle)?;
    ctx.globals().set("__loam", host.clone()).map_err(bundle)?;
    let prelude = Module::import(ctx, SERVER_MODULE).map_err(bundle)?;
    settle(ctx, &prelude).map_err(|e| invalid_bundle(ctx, e))?;
    let module = Module::declare(ctx.clone(), BUNDLE_MODULE, source).map_err(bundle)?;
    let (module, evaluated) = module.eval().map_err(bundle)?;
    settle(ctx, &evaluated).map_err(|e| invalid_bundle(ctx, e))?;
    let ns = module.namespace().map_err(bundle)?;
    let index: Function = host.get("index").map_err(bundle)?;
    let listed: Vec<Vec<String>> = index.call((ns,)).map_err(bundle)?;
    let mut functions = Vec::with_capacity(listed.len());
    for pair in listed {
        let [path, kind] = <[String; 2]>::try_from(pair)
            .map_err(|_| LiveError::Internal("the prelude listed a malformed function".into()))?;
        let kind = if kind == "mutation" {
            FnKind::Mutation
        } else {
            FnKind::Query
        };
        functions.push((path, kind));
    }
    state.borrow_mut().helpers = Some(Helpers {
        bytes_of: Persistent::save(ctx, host.get::<_, Function>("bytesOf").map_err(bundle)?),
        fits_i64: Persistent::save(ctx, host.get::<_, Function>("fitsI64").map_err(bundle)?),
    });
    Ok((Persistent::save(ctx, host), functions))
}

/// The host's `call(op, args)`: queues a `ctx.db` call and returns its
/// promise.
fn host_call_fn<'js>(ctx: &Ctx<'js>, state: Rc<RefCell<State>>) -> rquickjs::Result<Function<'js>> {
    Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, op: String, args: Value<'js>| -> rquickjs::Result<Promise<'js>> {
            let helpers = state.borrow().helpers.clone();
            let Some(helpers) = helpers else {
                return Err(Exception::throw_message(
                    &ctx,
                    "the database is available inside a handler only",
                ));
            };
            let args = match from_js(&ctx, &helpers, args, 0, &mut Copied::default()) {
                Ok(v) => v,
                Err(message) => {
                    return Err(Exception::throw_type(&ctx, &format!("db.{op}: {message}")));
                }
            };
            let mut s = state.borrow_mut();
            let Some(to_caller) = s.to_caller.clone() else {
                return Err(Exception::throw_message(
                    &ctx,
                    "the database is available inside a handler only",
                ));
            };
            let (promise, resolve, reject) = ctx.promise()?;
            let id = s.next_id;
            s.next_id += 1;
            s.pending.insert(
                id,
                (
                    Persistent::save(&ctx, resolve),
                    Persistent::save(&ctx, reject),
                ),
            );
            // A caller that has gone away shows when the worker waits for
            // the answer.
            let _ = to_caller.send(ToCaller::Host { id, op, args });
            Ok(promise)
        },
    )
}

/// Runs the queued jobs until `promise` settles.
fn settle<'js>(ctx: &Ctx<'js>, promise: &Promise<'js>) -> Result<(), Value<'js>> {
    while ctx.execute_pending_job() {}
    match promise.state() {
        PromiseState::Resolved => Ok(()),
        PromiseState::Rejected => Err(rejection(ctx, promise)),
        PromiseState::Pending => Err(Exception::from_message(
            ctx.clone(),
            "the module awaits something that never settles",
        )
        .map(Exception::into_value)
        .unwrap_or_else(|_| Value::new_undefined(ctx.clone()))),
    }
}

fn rejection<'js>(ctx: &Ctx<'js>, promise: &Promise<'js>) -> Value<'js> {
    match promise.result::<Value>() {
        Some(Err(rquickjs::Error::Exception)) => ctx.catch(),
        Some(Ok(v)) => v,
        _ => Value::new_undefined(ctx.clone()),
    }
}

fn run_in<'js>(ctx: &Ctx<'js>, warm: &Warm, job: &Job, budget: &Budget) -> Outcome {
    match call_in(ctx, warm, job, budget) {
        Ok(outcome) => outcome,
        Err(e) => Outcome::Done(Err(e)),
    }
}

fn call_in<'js>(
    ctx: &Ctx<'js>,
    warm: &Warm,
    job: &Job,
    budget: &Budget,
) -> Result<Outcome, LiveError> {
    let host = warm
        .host
        .clone()
        .restore(ctx)
        .map_err(|e| internal("restoring the host", &e))?;
    let helpers = warm
        .state
        .borrow()
        .helpers
        .clone()
        .ok_or_else(|| LiveError::Internal("a warm context without helpers".into()))?;
    let invoke: Function = host
        .get("invoke")
        .map_err(|e| internal("the prelude's invoke", &e))?;
    let args = to_js(ctx, &job.args).map_err(|e| internal("converting the arguments", &e))?;
    let kind = match job.kind {
        FnKind::Query => "query",
        FnKind::Mutation => "mutation",
    };
    let promise: Promise = match invoke.call((job.path.as_str(), kind, args)) {
        Ok(p) => p,
        Err(e) => {
            let thrown = caught(ctx, e);
            return Ok(Outcome::Done(Err(failure(
                ctx, &host, warm, budget, thrown,
            ))));
        }
    };
    loop {
        while ctx.execute_pending_job() {}
        if budget.fired() {
            return Ok(Outcome::Done(Err(timeout(budget))));
        }
        match promise.state() {
            PromiseState::Resolved => {
                let value = match promise.result::<Value>() {
                    Some(Ok(v)) => v,
                    _ => {
                        return Err(LiveError::Internal(
                            "a resolved promise without a value".into(),
                        ));
                    }
                };
                // Converting runs getters, which may loop: a conversion
                // that fails once the CPU budget fired is a timeout (review
                // of #97).
                return Ok(Outcome::Done(
                    from_js(ctx, &helpers, value, 0, &mut Copied::default()).map_err(|m| {
                        if budget.fired() {
                            timeout(budget)
                        } else {
                            LiveError::FunctionError(format!("{} returned {m}", job.path))
                        }
                    }),
                ));
            }
            PromiseState::Rejected => {
                let thrown = rejection(ctx, &promise);
                return Ok(Outcome::Done(Err(failure(
                    ctx, &host, warm, budget, thrown,
                ))));
            }
            PromiseState::Pending => {
                if warm.state.borrow().pending.is_empty() {
                    return Ok(Outcome::Done(Err(LiveError::FunctionError(format!(
                        "{} awaits a promise that never settles (there are no timers)",
                        job.path
                    )))));
                }
                budget.pause();
                let reply = job.replies.recv();
                budget.resume();
                let (id, answer) = match reply {
                    Ok(Reply::Ok(id, v)) => (id, Ok(v)),
                    Ok(Reply::Err(id, e)) => (id, Err(e)),
                    Ok(Reply::Abort) | Err(_) => {
                        budget.abort();
                        return Ok(Outcome::Aborted);
                    }
                };
                settle_host_call(ctx, &host, warm, id, answer)?;
            }
        }
    }
}

/// Resolves or rejects the promise of host call `id`.
fn settle_host_call<'js>(
    ctx: &Ctx<'js>,
    host: &Object<'js>,
    warm: &Warm,
    id: u64,
    answer: Result<LiveValue, LiveError>,
) -> Result<(), LiveError> {
    let Some((resolve, reject)) = warm.state.borrow_mut().pending.remove(&id) else {
        return Err(LiveError::Internal(format!(
            "an answer to unknown host call {id}"
        )));
    };
    let restore = |p: Persistent<Function<'static>>| {
        p.restore(ctx)
            .map_err(|e| internal("restoring a host call's promise", &e))
    };
    let settled = match answer {
        Ok(v) => {
            let v = to_js(ctx, &v).map_err(|e| internal("converting a host call's result", &e))?;
            restore(resolve)?.call::<_, ()>((v,))
        }
        Err(e) => {
            let make: Function = host
                .get("error")
                .map_err(|e| internal("the prelude's error", &e))?;
            let code = code_name(&e);
            let message = e.to_string();
            warm.state.borrow_mut().errors.insert(id, e);
            let error: Value = make
                .call((id as f64, code, message))
                .map_err(|e| internal("making a host call's error", &e))?;
            restore(reject)?.call::<_, ()>((error,))
        }
    };
    settled.map_err(|e| internal("settling a host call", &e))
}

/// The error a failed call ends with.
fn failure<'js>(
    ctx: &Ctx<'js>,
    host: &Object<'js>,
    warm: &Warm,
    budget: &Budget,
    thrown: Value<'js>,
) -> LiveError {
    if budget.fired() {
        return timeout(budget);
    }
    // The budget stays armed: `errorId` and `describe` may run the thrown
    // value's getters or `toString` (review of #93).
    if let Ok(id_of) = host.get::<_, Function>("errorId")
        && let Ok(id) = id_of.call::<_, f64>((thrown.clone(),))
        && id >= 0.0
        && let Some(e) = warm.state.borrow_mut().errors.remove(&(id as u64))
    {
        return e;
    }
    let text = describe(ctx, host, &thrown);
    if budget.fired() {
        return timeout(budget);
    }
    if text.contains("out of memory") {
        return LiveError::FunctionOutOfMemory(text);
    }
    LiveError::FunctionError(text)
}

fn timeout(budget: &Budget) -> LiveError {
    LiveError::FunctionTimeout(format!(
        "the function ran past its CPU limit of {:?}",
        budget.limit()
    ))
}

fn describe<'js>(ctx: &Ctx<'js>, host: &Object<'js>, thrown: &Value<'js>) -> String {
    if let Some(s) = thrown.as_string().and_then(|s| s.to_string().ok()) {
        return s;
    }
    if let Ok(describe) = host.get::<_, Function>("describe")
        && let Ok(text) = describe.call::<_, String>((thrown.clone(),))
    {
        return text;
    }
    let _ = ctx.catch();
    if let Some(e) = thrown.as_exception() {
        return e.message().unwrap_or_else(|| "an error".to_string());
    }
    format!("a thrown {}", thrown.type_name())
}

/// The value an `Err(Exception)` threw.
fn caught<'js>(ctx: &Ctx<'js>, e: rquickjs::Error) -> Value<'js> {
    match e {
        rquickjs::Error::Exception => ctx.catch(),
        other => rquickjs::String::from_str(ctx.clone(), &other.to_string())
            .map(|s| s.into_value())
            .unwrap_or_else(|_| Value::new_undefined(ctx.clone())),
    }
}

fn js_error(ctx: &Ctx<'_>, e: rquickjs::Error, what: &str) -> LiveError {
    let thrown = caught(ctx, e);
    invalid_bundle_with(ctx, &thrown, what)
}

fn invalid_bundle<'js>(ctx: &Ctx<'js>, thrown: Value<'js>) -> LiveError {
    invalid_bundle_with(ctx, &thrown, "the bundle failed to load")
}

fn invalid_bundle_with<'js>(ctx: &Ctx<'js>, thrown: &Value<'js>, what: &str) -> LiveError {
    let text = if let Some(s) = thrown.as_string().and_then(|s| s.to_string().ok()) {
        s
    } else if let Some(e) = thrown.as_exception() {
        let head = e.message().unwrap_or_default();
        match e.stack() {
            Some(stack) if !stack.is_empty() => format!("{head}\n{stack}"),
            _ => head,
        }
    } else {
        let _ = ctx.catch();
        format!("a thrown {}", thrown.type_name())
    };
    LiveError::InvalidArgument(format!("{what}: {text}"))
}

fn internal(what: &str, e: &rquickjs::Error) -> LiveError {
    LiveError::Internal(format!("{what}: {e}"))
}

/// A Live value as JavaScript: `I64` is a `bigint`, `F64` a number, `Bytes`
/// an `ArrayBuffer`.
fn to_js<'js>(ctx: &Ctx<'js>, v: &LiveValue) -> rquickjs::Result<Value<'js>> {
    Ok(match v {
        LiveValue::Null => Value::new_null(ctx.clone()),
        LiveValue::Bool(b) => Value::new_bool(ctx.clone(), *b),
        LiveValue::I64(n) => BigInt::from_i64(ctx.clone(), *n)?.into_value(),
        LiveValue::F64(f) => Value::new_float(ctx.clone(), *f),
        LiveValue::Str(s) => rquickjs::String::from_str(ctx.clone(), s)?.into_value(),
        LiveValue::Bytes(b) => ArrayBuffer::new(ctx.clone(), b.clone())?.into_value(),
        LiveValue::Array(items) => {
            let array = rquickjs::Array::new(ctx.clone())?;
            for (n, item) in items.iter().enumerate() {
                array.set(n, to_js(ctx, item)?)?;
            }
            array.into_value()
        }
        LiveValue::Object(fields) => {
            let object = Object::new(ctx.clone())?;
            for (k, item) in fields {
                object.set(k.as_str(), to_js(ctx, item)?)?;
            }
            object.into_value()
        }
    })
}

/// A JavaScript value as a Live value, or why it is not one. `undefined`
/// is `null`, and an object property holding `undefined` is left out.
fn from_js<'js>(
    ctx: &Ctx<'js>,
    helpers: &Helpers,
    v: Value<'js>,
    depth: usize,
    copied: &mut Copied,
) -> Result<LiveValue, String> {
    if depth > MAX_DEPTH {
        return Err(format!("a value nested deeper than {MAX_DEPTH} levels"));
    }
    copied.parts += 1;
    if copied.parts > MAX_VALUE_PARTS {
        return Err(format!("a value of more than {MAX_VALUE_PARTS} parts"));
    }
    let restore = |p: &Persistent<Function<'static>>| {
        p.clone()
            .restore(ctx)
            .map_err(|e| format!("the prelude: {e}"))
    };
    match v.type_of() {
        Type::Undefined | Type::Uninitialized | Type::Null => Ok(LiveValue::Null),
        Type::Bool => Ok(LiveValue::Bool(v.as_bool().unwrap_or_default())),
        Type::Int | Type::Float => Ok(LiveValue::F64(v.as_number().unwrap_or_default())),
        Type::String => {
            let s = v
                .as_string()
                .and_then(|s| s.to_string().ok())
                .ok_or_else(|| "an unreadable string".to_string())?;
            copied.bytes(s.len())?;
            Ok(LiveValue::Str(s))
        }
        Type::BigInt => {
            let fits: bool = restore(&helpers.fits_i64)?
                .call((v.clone(),))
                .map_err(|e| e.to_string())?;
            if !fits {
                return Err("a bigint outside the int64 range".to_string());
            }
            v.into_big_int()
                .ok_or_else(|| "an unreadable bigint".to_string())?
                .to_i64()
                .map(LiveValue::I64)
                .map_err(|e| e.to_string())
        }
        Type::Array => {
            let array = v
                .into_array()
                .ok_or_else(|| "an unreadable array".to_string())?;
            // A sparse array's length reserves no more than the parts
            // left (review of #97).
            let mut items = Vec::with_capacity(
                array
                    .len()
                    .min(MAX_VALUE_PARTS.saturating_sub(copied.parts)),
            );
            for item in array.iter::<Value>() {
                items.push(from_js(
                    ctx,
                    helpers,
                    item.map_err(|e| e.to_string())?,
                    depth + 1,
                    copied,
                )?);
            }
            Ok(LiveValue::Array(items))
        }
        Type::Object => {
            let bytes: Option<Vec<u8>> = restore(&helpers.bytes_of)?
                .call((v.clone(),))
                .map_err(|e| e.to_string())?;
            if let Some(bytes) = bytes {
                copied.bytes(bytes.len())?;
                return Ok(LiveValue::Bytes(bytes));
            }
            let object = v
                .into_object()
                .ok_or_else(|| "an unreadable object".to_string())?;
            let mut fields = BTreeMap::new();
            for prop in object.own_props::<String, Value>(Filter::new().string().enum_only()) {
                let (k, item) = prop.map_err(|e| e.to_string())?;
                if item.is_undefined() {
                    continue;
                }
                copied.bytes(k.len())?;
                fields.insert(k, from_js(ctx, helpers, item, depth + 1, copied)?);
            }
            Ok(LiveValue::Object(fields))
        }
        other => Err(format!("a {}, which is not a Live value", other.as_str())),
    }
}

/// The wire code of `e` without its `ERROR_CODE_` prefix, as the `code` of
/// the error a function sees.
fn code_name(e: &LiveError) -> String {
    let name = format!("{:?}", e.code());
    name.strip_prefix("ERROR_CODE_")
        .unwrap_or(&name)
        .to_string()
}
