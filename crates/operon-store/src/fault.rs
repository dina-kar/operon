use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Object store operation class a fault applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Put,
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
    ErrorAfterApply,
}

#[derive(Debug, Default)]
struct Rules {
    queued: HashMap<Op, VecDeque<Fault>>,
    calls: HashMap<Op, u64>,
}

/// An [`ObjectStore`] wrapper that injects queued faults, for tests.
///
/// Faults are consumed in FIFO order per [`Op`]; calls with no queued fault pass through.
/// Multipart uploads and copies always pass through.
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    rules: Arc<Mutex<Rules>>,
}

impl FaultyStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            rules: Arc::default(),
        }
    }

    /// Queues `fault` for the next call of `op`.
    pub fn inject(&self, op: Op, fault: Fault) {
        let mut rules = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
        rules.queued.entry(op).or_default().push_back(fault);
    }

    /// Number of calls of `op` seen so far, including failed ones.
    pub fn calls(&self, op: Op) -> u64 {
        let rules = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
        rules.calls.get(&op).copied().unwrap_or(0)
    }

    fn next_fault(&self, op: Op) -> Option<Fault> {
        let mut rules = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
        *rules.calls.entry(op).or_default() += 1;
        rules.queued.get_mut(&op).and_then(VecDeque::pop_front)
    }
}

fn injected(op: Op) -> object_store::Error {
    object_store::Error::Generic {
        store: "FaultyStore",
        source: format!("injected fault on {op:?}").into(),
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
        match self.next_fault(Op::Put) {
            None => self.inner.put_opts(location, payload, opts).await,
            Some(Fault::Error) => Err(injected(Op::Put)),
            Some(Fault::ErrorAfterApply) => {
                self.inner.put_opts(location, payload, opts).await?;
                Err(injected(Op::Put))
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
        match self.next_fault(Op::Get) {
            None => self.inner.get_opts(location, options).await,
            Some(_) => Err(injected(Op::Get)),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        match self.next_fault(Op::Delete) {
            None => self.inner.delete_stream(locations),
            Some(Fault::Error) => {
                futures::stream::once(async { Err(injected(Op::Delete)) }).boxed()
            }
            Some(Fault::ErrorAfterApply) => {
                let applied = self.inner.delete_stream(locations);
                applied
                    .map(|result| result.and(Err(injected(Op::Delete))))
                    .boxed()
            }
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.next_fault(Op::List) {
            None => self.inner.list(prefix),
            Some(_) => futures::stream::once(async { Err(injected(Op::List)) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        match self.next_fault(Op::List) {
            None => self.inner.list_with_delimiter(prefix).await,
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
