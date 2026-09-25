//! Where a collection's reads run (plan M1.2 Task 1; overview §6.9): the
//! fixed [`Placement`] contract, the [`RemoteReads`] transport M1.3
//! implements, and their single-node defaults.

use operon_collection::PrimaryKey;
use operon_common::{CollectionId, NamespaceId};

use crate::error::ServiceError;
use crate::ir::{Query, ReadConsistency, SearchRequest, SearchResponse};
use crate::types::{Projection, StoredDoc};

/// Which node owns a collection's reads.
pub trait Placement: Send + Sync + std::fmt::Debug {
    fn owner(&self, ns: NamespaceId, cid: CollectionId) -> Owner;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Owner {
    Local,
    Remote {
        node_id: u64,
        addr: std::net::SocketAddr,
    },
}

/// Forwards a read to its owner; M1.3 implements it over an internal HTTP
/// route.
///
/// An implementation forwards `hot::current()`'s `enabled` flag and records
/// the owner's reported `Operon-Hot-Used` kinds into `current().used`.
#[async_trait::async_trait]
pub trait RemoteReads: Send + Sync + std::fmt::Debug {
    async fn search(
        &self,
        to: &Owner,
        ns: &str,
        req: SearchRequest,
    ) -> Result<SearchResponse, ServiceError>;

    async fn get(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        pks: Vec<PrimaryKey>,
        select: Projection,
        consistency: ReadConsistency,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError>;

    async fn count(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        consistency: ReadConsistency,
    ) -> Result<u64, ServiceError>;

    #[allow(clippy::too_many_arguments)]
    async fn scroll(
        &self,
        to: &Owner,
        ns: &str,
        name: &str,
        filter: Option<Query>,
        after: Option<PrimaryKey>,
        limit: usize,
        select: Projection,
        consistency: ReadConsistency,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError>;
}

/// Every collection is owned by this node.
#[derive(Debug, Default)]
pub struct LocalOnly;

impl Placement for LocalOnly {
    fn owner(&self, _: NamespaceId, _: CollectionId) -> Owner {
        Owner::Local
    }
}

/// No transport: every forwarded read is `Unavailable`.
#[derive(Debug, Default)]
pub struct NoRemoteReads;

fn no_transport() -> ServiceError {
    ServiceError::Unavailable("no remote read transport is configured".to_string())
}

#[async_trait::async_trait]
impl RemoteReads for NoRemoteReads {
    async fn search(
        &self,
        _: &Owner,
        _: &str,
        _: SearchRequest,
    ) -> Result<SearchResponse, ServiceError> {
        Err(no_transport())
    }

    async fn get(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Vec<PrimaryKey>,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<Vec<Option<StoredDoc>>, ServiceError> {
        Err(no_transport())
    }

    async fn count(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Option<Query>,
        _: ReadConsistency,
    ) -> Result<u64, ServiceError> {
        Err(no_transport())
    }

    async fn scroll(
        &self,
        _: &Owner,
        _: &str,
        _: &str,
        _: Option<Query>,
        _: Option<PrimaryKey>,
        _: usize,
        _: Projection,
        _: ReadConsistency,
    ) -> Result<(Vec<StoredDoc>, Option<PrimaryKey>), ServiceError> {
        Err(no_transport())
    }
}

/// Set by `RemoteReads` implementations on a forwarded request; the
/// receiving route calls the `*_local` methods.
pub const FORWARDED_HEADER: &str = "operon-forwarded";
