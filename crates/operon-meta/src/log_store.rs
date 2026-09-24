//! The Raft log, vote and commit marker, kept in the node-local database.

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};

use openraft::OptionalSend;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, VoteOf};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use crate::codec;
use crate::db::{LocalDb, META_TABLE};
use crate::raft::{LogId, TypeConfig};

/// Log entries by index, postcard-encoded.
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
pub(crate) const VOTE_KEY: &str = "vote";
const COMMITTED_KEY: &str = "committed";
const PURGED_KEY: &str = "purged";

type Entry = EntryOf<TypeConfig>;
type Vote = VoteOf<TypeConfig>;

/// The metastore's openraft log storage. Every write is a durable (fsynced)
/// redb transaction. Cheap to clone; clones share the database.
#[derive(Clone, Debug)]
pub struct LogStore {
    db: LocalDb,
}

impl LogStore {
    pub fn new(db: LocalDb) -> Self {
        Self { db }
    }

    async fn read_record<T: serde::de::DeserializeOwned>(
        &self,
        key: &'static str,
    ) -> io::Result<Option<T>> {
        match self.db.get_meta(key).await? {
            Some(bytes) => codec::decode(&bytes).map(Some),
            None => Ok(None),
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> io::Result<Vec<Entry>> {
        let bounds: (Bound<u64>, Bound<u64>) =
            (range.start_bound().cloned(), range.end_bound().cloned());
        let raw = self
            .db
            .run(move |db| {
                let txn = db.begin_read()?;
                let table = match txn.open_table(LOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(e) => return Err(e.into()),
                };
                let mut out = Vec::new();
                for item in table.range::<u64>(bounds)? {
                    let (_, value) = item?;
                    out.push(value.value().to_vec());
                }
                Ok(out)
            })
            .await?;
        raw.iter().map(|bytes| codec::decode(bytes)).collect()
    }

    async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
        self.read_record(VOTE_KEY).await
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
        let last_raw = self
            .db
            .run(|db| {
                let txn = db.begin_read()?;
                let table = match txn.open_table(LOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                Ok(table.last()?.map(|(_, value)| value.value().to_vec()))
            })
            .await?;
        let last_purged_log_id: Option<LogId> = self.read_record(PURGED_KEY).await?;
        let last_log_id = match last_raw {
            Some(bytes) => Some(codec::decode::<Entry>(&bytes)?.log_id),
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
        self.db.put_meta(VOTE_KEY, codec::encode(vote)?).await
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
        self.db
            .put_meta(COMMITTED_KEY, codec::encode(&committed)?)
            .await
    }

    async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
        Ok(self
            .read_record::<Option<LogId>>(COMMITTED_KEY)
            .await?
            .flatten())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| Ok((entry.log_id.index, codec::encode(&entry)?)))
            .collect::<io::Result<Vec<_>>>()?;
        let result = self
            .db
            .run(move |db| {
                let txn = db.begin_write()?;
                {
                    let mut table = txn.open_table(LOG_TABLE)?;
                    for (index, bytes) in &encoded {
                        table.insert(*index, bytes.as_slice())?;
                    }
                }
                txn.commit()?;
                Ok(())
            })
            .await;
        match result {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                callback.io_completed(Err(io::Error::new(err.kind(), err.to_string())));
                Err(err)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
        let start = last_log_id.map_or(0, |id| id.index + 1);
        self.db
            .run(move |db| {
                let txn = db.begin_write()?;
                txn.open_table(LOG_TABLE)?
                    .retain_in(start.., |_, _| false)?;
                txn.commit()?;
                Ok(())
            })
            .await
    }

    async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
        let marker = codec::encode(&log_id)?;
        let upto = log_id.index;
        self.db
            .run(move |db| {
                let txn = db.begin_write()?;
                txn.open_table(META_TABLE)?
                    .insert(PURGED_KEY, marker.as_slice())?;
                txn.open_table(LOG_TABLE)?
                    .retain_in(..=upto, |_, _| false)?;
                txn.commit()?;
                Ok(())
            })
            .await
    }
}
