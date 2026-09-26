//! The shared metastore types moved from `operon-meta` (M1.2a plan, Task 2),
//! and the `MetaStore` trait's own types (Task 3).

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use operon_common::meta::{
    ApplyError, ChangeWait, EntryKind, Freshness, IndexEntry, Lease, MetaChanges, MetaError,
    MetaStopped, PartitionIndex, StaleLag, Tracked, collection_pk_prefix, collection_pointer_key,
    collection_prefix, implicit_name, link_pointer_key,
};
use operon_common::{CollectionId, NamespaceId};

/// Runs a future that never waits (every future in these tests is ready at
/// once), so `operon-common` needs no async runtime.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the future was not ready"),
    }
}

#[test]
fn stale_lag_reports_the_proposers_lag() {
    let stale = ApplyError::StaleObject {
        object: "ns/1/streams/1/0/seg".to_string(),
        created_at_ms: 1_000,
        max_age_ms: 500,
        clock_ms: 2_000,
    };
    assert_eq!(
        stale.stale_lag(1_900),
        Some(StaleLag {
            deadline_ms: 1_500,
            clock_ms: 2_000,
            proposer_now_ms: 1_900,
            late_by_ms: 500,
            proposer_lag_ms: 100,
        })
    );
    let fenced = ApplyError::Fenced {
        lease: "task/gc".to_string(),
    };
    assert_eq!(fenced.stale_lag(1_900), None);
}

#[test]
fn implicit_names_and_keys_have_their_documented_form() {
    let ns = NamespaceId(3);
    let collection = CollectionId(7);
    assert_eq!(implicit_name("docs", collection), "_collection.docs.7");
    assert_eq!(collection_pointer_key(collection), "collection/7");
    assert_eq!(collection_prefix(ns, collection), "ns/3/collections/7/");
    assert_eq!(
        collection_pk_prefix(ns, collection),
        "ns/3/pk/collection-7/"
    );
    assert_eq!(link_pointer_key(operon_common::meta::LinkId(4)), "link/4");
}

#[test]
fn freshness_expires_strictly_after_its_deadline() {
    let fresh = Freshness {
        created_at_ms: 1_000,
        max_age_ms: 500,
    };
    // The deadline itself (1_500) is still fresh; only strictly after it is expired.
    assert!(!fresh.expired_at(1_500));
    assert!(fresh.expired_at(1_501));
}

#[test]
fn a_lease_is_held_until_its_deadline_and_not_after_release() {
    let held = Lease {
        epoch: 1,
        owner: Some("worker-a".to_string()),
        deadline_ms: 1_000,
    };
    assert!(held.is_held_at(999));
    assert!(!held.is_held_at(1_000));
    assert!(!held.is_held_at(1_001));

    let released = Lease {
        owner: None,
        ..held
    };
    assert!(!released.is_held_at(500));
}

#[test]
fn unexpected_reply_display_is_unchanged() {
    assert_eq!(
        MetaError::UnexpectedReply("SegmentSwapped".into()).to_string(),
        "unexpected reply: SegmentSwapped"
    );
}

#[test]
fn tracked_into_result_drops_the_flag() {
    let ok = Tracked {
        result: Ok(7u64),
        earlier_unknown: true,
    };
    assert_eq!(ok.into_result().unwrap(), 7);
    let rejected: Tracked<u64> = Tracked {
        result: Err(MetaError::Rejected(ApplyError::VersionMismatch {
            current: None,
        })),
        earlier_unknown: false,
    };
    assert!(matches!(
        rejected.into_result(),
        Err(MetaError::Rejected(ApplyError::VersionMismatch {
            current: None
        }))
    ));
}

/// Completes `remaining` times, then reports that the metastore stopped.
#[derive(Debug)]
struct Countdown {
    remaining: u32,
}

#[async_trait]
impl ChangeWait for Countdown {
    async fn changed(&mut self) -> Result<(), MetaStopped> {
        if self.remaining == 0 {
            return Err(MetaStopped);
        }
        self.remaining -= 1;
        Ok(())
    }
}

#[test]
fn meta_changes_forwards_to_its_wait() {
    let mut changes = MetaChanges::new(Countdown { remaining: 2 });
    assert!(ready(changes.changed()).is_ok());
    assert!(ready(changes.changed()).is_ok());
    let stopped = ready(changes.changed()).unwrap_err();
    assert_eq!(stopped.to_string(), "the metastore stopped");
    assert!(ready(changes.changed()).is_err());
}

fn entry(base_offset: u64, records: u32, byte_range: std::ops::Range<u64>) -> IndexEntry {
    IndexEntry {
        kind: EntryKind::Wal,
        base_offset,
        records,
        object: format!("wal/{base_offset}.wal"),
        byte_range,
        max_timestamp_ms: 0,
    }
}

#[test]
fn partition_index_accessors_report_what_it_was_built_with() {
    let entries = vec![entry(5, 5, 0..100), entry(10, 3, 100..130)];
    let index = PartitionIndex::new(7, 13, 4_096, entries.clone());
    assert_eq!(index.log_start_offset(), 7);
    assert_eq!(index.next_offset(), 13);
    assert_eq!(index.high_watermark(), 13);
    assert_eq!(index.bytes(), 4_096);
    assert_eq!(index.entries().cloned().collect::<Vec<_>>(), entries);
    assert_eq!(index.clone().into_entries(), entries);
    let empty = PartitionIndex::new(0, 0, 0, Vec::new());
    assert_eq!(empty.entries().count(), 0);
}

/// A `MetaStore` that only has to compile: the trait must stay usable as
/// `Arc<dyn MetaStore>` (M1.2a Ruling 2).
mod dyn_compatible {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use operon_common::meta::{
        AliasAction, Collection, CollectionHead, CollectionRoots, Consistency, Fence, HotConfig,
        Lease, LeaseGrant, Link, LinkHead, LinkId, MetaChanges, MetaResult, MetaStore, Namespace,
        PartitionIndex, Pointer, PointerCas, Retention, SegmentSwap, Stream, StreamState,
        TargetRef, Tracked, WalClass, WalCommit,
    };
    use operon_common::schema::CollectionSchema;
    use operon_common::{CollectionId, NamespaceId, StreamId};

    #[derive(Debug)]
    struct Stub;

    #[async_trait]
    impl MetaStore for Stub {
        fn now_ms(&self) -> u64 {
            unimplemented!()
        }
        fn watch_changes(&self) -> MetaChanges {
            unimplemented!()
        }
        fn is_ready(&self) -> bool {
            unimplemented!()
        }
        async fn clock_ms(&self, _: Consistency) -> MetaResult<u64> {
            unimplemented!()
        }
        async fn create_namespace(&self, _: &str) -> MetaResult<NamespaceId> {
            unimplemented!()
        }
        async fn namespace_by_name(
            &self,
            _: Consistency,
            _: &str,
        ) -> MetaResult<Option<Namespace>> {
            unimplemented!()
        }
        async fn namespaces(&self, _: Consistency) -> MetaResult<Vec<Namespace>> {
            unimplemented!()
        }
        async fn create_stream(
            &self,
            _: NamespaceId,
            _: &str,
            _: u32,
            _: WalClass,
            _: Retention,
        ) -> MetaResult<StreamId> {
            unimplemented!()
        }
        async fn set_retention(&self, _: StreamId, _: Retention) -> MetaResult<()> {
            unimplemented!()
        }
        async fn stream(&self, _: Consistency, _: StreamId) -> MetaResult<Option<Stream>> {
            unimplemented!()
        }
        async fn stream_by_name(
            &self,
            _: Consistency,
            _: NamespaceId,
            _: &str,
        ) -> MetaResult<Option<Stream>> {
            unimplemented!()
        }
        async fn streams(&self, _: Consistency, _: Option<NamespaceId>) -> MetaResult<Vec<Stream>> {
            unimplemented!()
        }
        async fn stream_state(
            &self,
            _: Consistency,
            _: StreamId,
        ) -> MetaResult<Option<StreamState>> {
            unimplemented!()
        }
        async fn create_link(
            &self,
            _: NamespaceId,
            _: &str,
            _: StreamId,
            _: TargetRef,
            _: BTreeMap<String, String>,
        ) -> MetaResult<LinkId> {
            unimplemented!()
        }
        async fn link_by_name(
            &self,
            _: Consistency,
            _: NamespaceId,
            _: &str,
        ) -> MetaResult<Option<Link>> {
            unimplemented!()
        }
        async fn links(&self, _: Consistency, _: Option<NamespaceId>) -> MetaResult<Vec<Link>> {
            unimplemented!()
        }
        async fn links_with_pointers(
            &self,
            _: Consistency,
            _: NamespaceId,
        ) -> MetaResult<Vec<LinkHead>> {
            unimplemented!()
        }
        async fn commit_wal(&self, _: WalCommit) -> Tracked<Vec<u64>> {
            unimplemented!()
        }
        async fn swap_segment(&self, _: SegmentSwap) -> Tracked<()> {
            unimplemented!()
        }
        async fn trim_partition(
            &self,
            _: StreamId,
            _: u32,
            _: u64,
            _: Option<Fence>,
        ) -> MetaResult<u64> {
            unimplemented!()
        }
        async fn partition_index(
            &self,
            _: Consistency,
            _: StreamId,
            _: u32,
            _: u64,
            _: Option<u64>,
        ) -> MetaResult<Option<PartitionIndex>> {
            unimplemented!()
        }
        async fn acquire_lease(&self, _: &str, _: &str, _: Duration) -> MetaResult<LeaseGrant> {
            unimplemented!()
        }
        async fn renew_lease(
            &self,
            _: &str,
            _: &str,
            _: u64,
            _: Duration,
        ) -> MetaResult<LeaseGrant> {
            unimplemented!()
        }
        async fn reacquire_lease(
            &self,
            _: &str,
            _: &str,
            _: u64,
            _: Duration,
        ) -> MetaResult<LeaseGrant> {
            unimplemented!()
        }
        async fn release_lease(&self, _: &str, _: &str, _: u64) -> MetaResult<()> {
            unimplemented!()
        }
        async fn lease(&self, _: Consistency, _: &str) -> MetaResult<Option<Lease>> {
            unimplemented!()
        }
        async fn leases_with_prefix(
            &self,
            _: Consistency,
            _: &str,
        ) -> MetaResult<Vec<(String, Lease)>> {
            unimplemented!()
        }
        async fn cas_pointer(&self, _: PointerCas) -> Tracked<u64> {
            unimplemented!()
        }
        async fn pointer(
            &self,
            _: Consistency,
            _: NamespaceId,
            _: &str,
        ) -> MetaResult<Option<Pointer>> {
            unimplemented!()
        }
        async fn create_collection(
            &self,
            _: NamespaceId,
            _: &str,
            _: CollectionSchema,
            _: u32,
        ) -> MetaResult<(CollectionId, StreamId, LinkId)> {
            unimplemented!()
        }
        async fn drop_collection(
            &self,
            _: NamespaceId,
            _: &str,
        ) -> MetaResult<Option<CollectionId>> {
            unimplemented!()
        }
        async fn update_collection_schema(
            &self,
            _: CollectionId,
            _: u64,
            _: CollectionSchema,
        ) -> MetaResult<u64> {
            unimplemented!()
        }
        async fn update_aliases(&self, _: NamespaceId, _: Vec<AliasAction>) -> MetaResult<()> {
            unimplemented!()
        }
        async fn collection(
            &self,
            _: Consistency,
            _: CollectionId,
        ) -> MetaResult<Option<Collection>> {
            unimplemented!()
        }
        async fn resolve_collection(
            &self,
            _: Consistency,
            _: NamespaceId,
            _: &str,
        ) -> MetaResult<Option<Collection>> {
            unimplemented!()
        }
        async fn collection_for_link(
            &self,
            _: Consistency,
            _: LinkId,
        ) -> MetaResult<Option<Collection>> {
            unimplemented!()
        }
        async fn collections(
            &self,
            _: Consistency,
            _: Option<NamespaceId>,
        ) -> MetaResult<Vec<Collection>> {
            unimplemented!()
        }
        async fn aliases(
            &self,
            _: Consistency,
            _: NamespaceId,
        ) -> MetaResult<Vec<(String, CollectionId)>> {
            unimplemented!()
        }
        async fn collection_head(
            &self,
            _: Consistency,
            _: CollectionId,
        ) -> MetaResult<Option<CollectionHead>> {
            unimplemented!()
        }
        async fn collection_heads(
            &self,
            _: Consistency,
            _: Option<NamespaceId>,
        ) -> MetaResult<Vec<CollectionHead>> {
            unimplemented!()
        }
        async fn set_collection_hot(
            &self,
            _: NamespaceId,
            _: CollectionId,
            _: HotConfig,
        ) -> MetaResult<()> {
            unimplemented!()
        }
        async fn collection_hot(
            &self,
            _: Consistency,
            _: NamespaceId,
            _: CollectionId,
        ) -> MetaResult<HotConfig> {
            unimplemented!()
        }
        async fn retired_expired(&self, _: u64) -> MetaResult<Vec<String>> {
            unimplemented!()
        }
        async fn forget_objects(&self, _: Vec<String>, _: Option<Fence>) -> MetaResult<u32> {
            unimplemented!()
        }
        async fn prune_wal_commits(&self, _: Option<Fence>) -> MetaResult<u32> {
            unimplemented!()
        }
        async fn orphan_wal_objects(
            &self,
            _: Vec<(String, u64)>,
            _: u64,
            _: usize,
        ) -> MetaResult<Vec<String>> {
            unimplemented!()
        }
        async fn orphan_segments(
            &self,
            _: NamespaceId,
            _: Vec<(String, u64)>,
            _: u64,
            _: usize,
        ) -> MetaResult<Vec<String>> {
            unimplemented!()
        }
        async fn segment_referenced(&self, _: StreamId, _: u32, _: &str) -> MetaResult<bool> {
            unimplemented!()
        }
        async fn collection_roots(&self, _: NamespaceId, _: &str) -> MetaResult<CollectionRoots> {
            unimplemented!()
        }
    }

    /// A component holding the metastore, as downstream structs will.
    #[derive(Debug)]
    struct Component {
        meta: Arc<dyn MetaStore>,
    }

    fn assert_shareable<T: Send + Sync + 'static>(_: &T) {}

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn the_trait_is_dyn_compatible() {
        let meta: Arc<dyn MetaStore> = Arc::new(Stub);
        let component = Component { meta };
        assert_shareable(&component);
        assert_shareable(&component.meta);
        assert!(format!("{component:?}").contains("Stub"));
        // Created, never polled: the stub would panic.
        let call = component.meta.clock_ms(Consistency::Linearizable);
        assert_send(&call);
    }
}
