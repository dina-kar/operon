use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Object store operation class a fault applies to.
///
/// PUTs are classified by their mode: [`Op::PutCreate`] is a create-only
/// write ([`Store::put_if_absent`](crate::Store::put_if_absent)),
/// [`Op::PutIfMatch`] a compare-and-swap write
/// ([`Store::put_if_match`](crate::Store::put_if_match)). [`Op::Put`]
/// covers *every* PUT: a fault queued for `Put` applies to the next PUT of
/// any mode, and `calls(Put)` counts them all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Put,
    PutCreate,
    PutIfMatch,
    /// Covers [`Store::get`](crate::Store::get), [`Store::get_range`](crate::Store::get_range)
    /// and [`Store::head`](crate::Store::head): `object_store` routes all three through
    /// `get_opts`, so a fault queued for `Get` applies to any of them.
    Get,
    Delete,
    List,
}

/// A single injected failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Fail without touching the inner store.
    Error,
    /// Apply the operation to the inner store, then report failure.
    /// Models a lost acknowledgement: the write happened, the caller thinks it did not.
    /// For reads and lists it is the same as [`Fault::Error`].
    ErrorAfterApply,
    /// Report a failed precondition without applying: `412 Precondition
    /// Failed` for a compare-and-swap PUT, `409`/already-exists for a
    /// create-only PUT. For other operations it is the same as
    /// [`Fault::Error`].
    Precondition,
    /// Wait this long, then perform the operation normally.
    Delay(Duration),
}

/// Probabilities (0.0..=1.0) of a fault on each call, for
/// [`FaultyStore::random`]. Faults that do not apply to an operation (such as
/// `precondition` on a GET) are drawn as plain errors.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FaultRates {
    pub error: f64,
    pub error_after_apply: f64,
    pub precondition: f64,
    pub delay: f64,
    /// Delays are drawn uniformly from zero to this.
    pub max_delay: Duration,
}

impl FaultRates {
    /// No faults.
    pub fn none() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
struct Rules {
    queued: HashMap<Op, VecDeque<Fault>>,
    calls: HashMap<Op, u64>,
    rates: FaultRates,
    rng: ChaCha8Rng,
}

/// An [`ObjectStore`] wrapper that injects faults, for tests.
///
/// Queued faults are consumed in FIFO order per [`Op`] (a mode-specific PUT
/// queue before the [`Op::Put`] queue); calls with no queued fault may draw a
/// random fault from the store's [`FaultRates`] (none by default), and
/// otherwise pass through. Multipart uploads and copies always pass through.
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    rules: Arc<Mutex<Rules>>,
}

impl FaultyStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self::random(inner, 0, FaultRates::none())
    }

    /// A store that also draws random faults at `rates`, from a generator
    /// seeded with `seed` (the draws follow the order of calls, so a
    /// concurrent workload does not replay exactly).
    pub fn random(inner: Arc<dyn ObjectStore>, seed: u64, rates: FaultRates) -> Self {
        Self {
            inner,
            rules: Arc::new(Mutex::new(Rules {
                queued: HashMap::new(),
                calls: HashMap::new(),
                rates,
                rng: ChaCha8Rng::seed_from_u64(seed),
            })),
        }
    }

    /// Changes the random fault rates from now on (for fault bursts).
    pub fn set_rates(&self, rates: FaultRates) {
        self.rules().rates = rates;
    }

    fn rules(&self) -> std::sync::MutexGuard<'_, Rules> {
        self.rules.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `fault` for the next call of `op`.
    pub fn inject(&self, op: Op, fault: Fault) {
        self.rules().queued.entry(op).or_default().push_back(fault);
    }

    /// Drops every queued fault.
    pub fn clear(&self) {
        self.rules().queued.clear();
    }

    /// Number of calls of `op` seen so far, including failed ones. `Put`
    /// counts PUTs of every mode.
    pub fn calls(&self, op: Op) -> u64 {
        self.rules().calls.get(&op).copied().unwrap_or(0)
    }

    /// The fault for this call of `ops` (most specific first): a queued one,
    /// else a random one.
    fn next_fault(&self, ops: &[Op]) -> Option<Fault> {
        let mut rules = self.rules();
        for op in ops {
            *rules.calls.entry(*op).or_default() += 1;
        }
        for op in ops {
            if let Some(fault) = rules.queued.get_mut(op).and_then(VecDeque::pop_front) {
                return Some(fault);
            }
        }
        let rates = rules.rates;
        let total = rates.error + rates.error_after_apply + rates.precondition + rates.delay;
        if total <= 0.0 {
            return None;
        }
        let draw: f64 = rules.rng.random();
        let mut edge = rates.error;
        if draw < edge {
            return Some(Fault::Error);
        }
        edge += rates.error_after_apply;
        if draw < edge {
            return Some(Fault::ErrorAfterApply);
        }
        edge += rates.precondition;
        if draw < edge {
            return Some(Fault::Precondition);
        }
        edge += rates.delay;
        if draw < edge {
            let max = u64::try_from(rates.max_delay.as_micros()).unwrap_or(u64::MAX);
            let micros = if max == 0 {
                0
            } else {
                rules.rng.random_range(0..=max)
            };
            return Some(Fault::Delay(Duration::from_micros(micros)));
        }
        None
    }
}

fn injected(op: Op) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultyStore",
        source: format!("injected fault on {op:?}").into(),
    }
}

fn precondition(op: Op, location: &Path) -> object_store::Error {
    let source = format!("injected precondition failure on {op:?}").into();
    match op {
        Op::PutCreate => object_store::Error::AlreadyExists {
            path: location.to_string(),
            source,
        },
        Op::PutIfMatch => object_store::Error::Precondition {
            path: location.to_string(),
            source,
        },
        other => injected(other),
    }
}

impl fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultyStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let op = match opts.mode {
            PutMode::Create => Op::PutCreate,
            PutMode::Update(_) => Op::PutIfMatch,
            PutMode::Overwrite => Op::Put,
        };
        let ops: &[Op] = if op == Op::Put {
            &[Op::Put]
        } else {
            &[op, Op::Put]
        };
        match self.next_fault(ops) {
            None => self.inner.put_opts(location, payload, opts).await,
            Some(Fault::Error) => Err(injected(op)),
            Some(Fault::Precondition) => Err(precondition(op, location)),
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                self.inner.put_opts(location, payload, opts).await
            }
            Some(Fault::ErrorAfterApply) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(injected(op))
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        match self.next_fault(&[Op::Get]) {
            None => self.inner.get_opts(location, options).await,
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                self.inner.get_opts(location, options).await
            }
            Some(_) => Err(injected(Op::Get)),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        match self.next_fault(&[Op::Delete]) {
            None => self.inner.delete_stream(locations),
            Some(Fault::Delay(delay)) => {
                let inner = self.inner.clone();
                futures::stream::once(async move {
                    tokio::time::sleep(delay).await;
                    inner.delete_stream(locations)
                })
                .flatten()
                .boxed()
            }
            Some(Fault::ErrorAfterApply) => {
                let applied = self.inner.delete_stream(locations);
                applied
                    .map(|result| result.and(Err(injected(Op::Delete))))
                    .boxed()
            }
            Some(Fault::Error | Fault::Precondition) => {
                futures::stream::once(async { Err(injected(Op::Delete)) }).boxed()
            }
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.next_fault(&[Op::List]) {
            None => self.inner.list(prefix),
            Some(Fault::Delay(delay)) => {
                let listed = self.inner.list(prefix);
                futures::stream::once(async move {
                    tokio::time::sleep(delay).await;
                    listed
                })
                .flatten()
                .boxed()
            }
            Some(_) => futures::stream::once(async { Err(injected(Op::List)) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        match self.next_fault(&[Op::List]) {
            None => self.inner.list_with_delimiter(prefix).await,
            Some(Fault::Delay(delay)) => {
                tokio::time::sleep(delay).await;
                self.inner.list_with_delimiter(prefix).await
            }
            Some(_) => Err(injected(Op::List)),
        }
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
