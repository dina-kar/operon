use std::collections::{BTreeMap, BTreeSet};

use openraft::entry::RaftEntry;
use openraft::impls::leader_id_adv::LeaderId;
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};
use openraft::testing::log::{StoreBuilder, Suite};
use openraft::type_config::alias::{EntryOf, LogIdOf};
use openraft::{BasicNode, ErrorSubject, ErrorVerb, LogId, Membership, StorageError, Vote};
use operon_meta::{Command, LocalDb, LogStore, StateMachineStore, TypeConfig};
use operon_store::Store;
use tempfile::TempDir;

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachineStore, TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(TempDir, LogStore, StateMachineStore), StorageError<TypeConfig>> {
        let io_err = |e| StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Read, e);
        let dir = TempDir::new().map_err(io_err)?;
        let db = LocalDb::open(dir.path()).map_err(io_err)?;
        let sm = StateMachineStore::open(0, Store::in_memory(), "meta/snapshots", db.clone())
            .await
            .map_err(io_err)?;
        Ok((dir, LogStore::new(db), sm))
    }
}

#[tokio::test]
async fn log_store_and_state_machine_pass_the_openraft_suite() {
    Suite::test_all(Builder).await.unwrap();
}

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
    LogId::new(LeaderId { term, node_id: 1 }, index)
}

fn entries(range: std::ops::Range<u64>) -> Vec<EntryOf<TypeConfig>> {
    range
        .map(|index| {
            if index == 0 {
                let membership = Membership::new(
                    vec![BTreeSet::from([1])],
                    BTreeMap::from([(1, BasicNode::default())]),
                )
                .expect("membership");
                EntryOf::<TypeConfig>::new_membership(log_id(1, 0), membership)
            } else {
                EntryOf::<TypeConfig>::new_normal(
                    log_id(1, index),
                    Command::CreateNamespace {
                        name: format!("ns{index}"),
                    },
                )
            }
        })
        .collect()
}

#[tokio::test]
async fn log_vote_and_markers_survive_reopening_the_database() {
    let dir = TempDir::new().unwrap();
    let vote = Vote::new_committed(3, 1);
    {
        let mut log = LogStore::new(LocalDb::open(dir.path()).unwrap());
        log.append(entries(0..10), IOFlushed::noop()).await.unwrap();
        log.save_vote(&vote).await.unwrap();
        log.save_committed(Some(log_id(1, 8))).await.unwrap();
        log.purge(log_id(1, 3)).await.unwrap();
        log.truncate_after(Some(log_id(1, 8))).await.unwrap();
    }

    let mut log = LogStore::new(LocalDb::open(dir.path()).unwrap());
    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)));
    assert_eq!(state.last_log_id, Some(log_id(1, 8)));
    assert_eq!(log.read_committed().await.unwrap(), Some(log_id(1, 8)));
    assert_eq!(log.read_vote().await.unwrap(), Some(vote));

    let mut reader = log.get_log_reader().await;
    let kept = reader.try_get_log_entries(0..100).await.unwrap();
    let indexes: Vec<u64> = kept.iter().map(|e| e.log_id.index).collect();
    assert_eq!(indexes, (4..=8).collect::<Vec<_>>());
    assert_eq!(kept, entries(4..9));
}

#[tokio::test]
async fn a_fully_purged_log_reports_the_purge_point_as_its_last_log_id() {
    let dir = TempDir::new().unwrap();
    let mut log = LogStore::new(LocalDb::open(dir.path()).unwrap());
    log.append(entries(0..5), IOFlushed::noop()).await.unwrap();
    log.purge(log_id(1, 4)).await.unwrap();

    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
    assert_eq!(state.last_log_id, Some(log_id(1, 4)));
    let mut reader = log.get_log_reader().await;
    assert!(reader.try_get_log_entries(..).await.unwrap().is_empty());
}
