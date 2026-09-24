//! The node-local database: `<data_dir>/meta.redb`.

use std::io;
use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableDatabase, TableDefinition};
use tokio::sync::watch;

/// Small node-local records (Raft vote, commit and purge markers, the current
/// snapshot pointer), keyed by name, postcard-encoded.
pub(crate) const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// Reports `true` once dropped. It is the last field of [`Handle`], so it is
/// dropped after the database, and the report means the database file is
/// closed and its lock released.
#[derive(Debug)]
struct CloseSignal(watch::Sender<bool>);

impl Drop for CloseSignal {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

#[derive(Debug)]
struct Handle {
    // Field order matters: `db` is dropped (closing the file) before `closed`.
    db: Database,
    closed: CloseSignal,
}

/// The node-local metadata database. Everything the node keeps on local disk
/// lives here; snapshots themselves live in object storage.
#[derive(Clone, Debug)]
pub struct LocalDb {
    handle: Arc<Handle>,
}

impl LocalDb {
    /// Opens or creates `<data_dir>/meta.redb`, creating `data_dir` if needed.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let db = Database::create(data_dir.join("meta.redb")).map_err(io::Error::other)?;
        let (closed, _) = watch::channel(false);
        Ok(Self {
            handle: Arc::new(Handle {
                db,
                closed: CloseSignal(closed),
            }),
        })
    }

    /// Becomes `true` (or its sender is gone) once every handle is dropped
    /// *and* the database file is closed. A `Weak` count reaching zero is not
    /// enough: `Arc` decrements it before it drops the database, so the file
    /// lock can outlive it for a moment (M0.3 re-review N4).
    pub(crate) fn closed(&self) -> watch::Receiver<bool> {
        self.handle.closed.0.subscribe()
    }

    /// Runs `f` on a blocking thread, so redb's file I/O and fsyncs do not stall
    /// the async runtime.
    pub(crate) async fn run<T, F>(&self, f: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T, redb::Error> + Send + 'static,
    {
        let handle = self.handle.clone();
        tokio::task::spawn_blocking(move || f(&handle.db).map_err(io::Error::other))
            .await
            .map_err(io::Error::other)?
    }

    /// Reads one record from the meta table.
    pub(crate) async fn get_meta(&self, key: &'static str) -> io::Result<Option<Vec<u8>>> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let table = match txn.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            Ok(table.get(key)?.map(|v| v.value().to_vec()))
        })
        .await
    }

    /// Writes one record to the meta table, durably.
    pub(crate) async fn put_meta(&self, key: &'static str, value: Vec<u8>) -> io::Result<()> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            txn.open_table(META_TABLE)?.insert(key, value.as_slice())?;
            txn.commit()?;
            Ok(())
        })
        .await
    }
}
