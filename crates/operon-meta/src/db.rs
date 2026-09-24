//! The node-local database: `<data_dir>/meta.redb`.

use std::io;
use std::path::Path;
use std::sync::{Arc, Weak};

use redb::{Database, ReadableDatabase, TableDefinition};

/// Small node-local records (Raft vote, commit and purge markers, the current
/// snapshot pointer), keyed by name, postcard-encoded.
pub(crate) const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

/// The node-local metadata database. Everything the node keeps on local disk
/// lives here; snapshots themselves live in object storage.
#[derive(Clone, Debug)]
pub struct LocalDb {
    db: Arc<Database>,
}

impl LocalDb {
    /// Opens or creates `<data_dir>/meta.redb`, creating `data_dir` if needed.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let db = Database::create(data_dir.join("meta.redb")).map_err(io::Error::other)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// A handle that does not keep the database open, to detect when every
    /// user has let go of it.
    pub(crate) fn downgrade(&self) -> Weak<Database> {
        Arc::downgrade(&self.db)
    }

    /// Runs `f` on a blocking thread, so redb's file I/O and fsyncs do not stall
    /// the async runtime.
    pub(crate) async fn run<T, F>(&self, f: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Database) -> Result<T, redb::Error> + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db).map_err(io::Error::other))
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
