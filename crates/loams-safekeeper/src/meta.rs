//! Acceptor metadata for the local-journal store (§28 §7.2, D268): the head
//! fields that change only on timeline creation, votes, elections and trims.
//!
//! The WAL itself, and the LSNs that move with it (`flush_lsn`, and the
//! lazily kept `commit_lsn`, `backup_lsn`, `remote_consistent_lsn`), live in
//! the journal. What a [`MetaStore`] keeps must be durable before the call
//! returns: a vote is a promise.
//!
//! - [`LocalMeta`]: one control file per timeline (write a temporary file,
//!   `fsync`, rename, `fsync` the directory), for single-node setups and tests.
//! - TiKV (feature `tikv`): one key per `(node, timeline)`, written by 2PC.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

use crate::Error;
use crate::types::{AcceptorState, Id, TimelineId};

/// Durable acceptor metadata, per timeline.
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Every timeline's last stored head.
    async fn load_all(&self) -> Result<Vec<(TimelineId, AcceptorState)>, Error>;
    /// Store `st` for `tl`, durably, before returning.
    async fn put(&self, tl: &TimelineId, st: &AcceptorState) -> Result<(), Error>;
}

const VERSION: u8 = 1;

/// Encode a head: version, postcard, CRC32C of both.
pub fn encode(st: &AcceptorState) -> Result<Vec<u8>, Error> {
    let mut out = vec![VERSION];
    out.extend(postcard::to_stdvec(st).map_err(|e| Error::Store(format!("encode head: {e}")))?);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Decode what [`encode`] wrote.
pub fn decode(b: &[u8]) -> Result<AcceptorState, Error> {
    let bad = |why: &str| Error::Store(format!("acceptor metadata: {why}"));
    if b.len() < 5 {
        return Err(bad("too short"));
    }
    let (body, crc) = b.split_at(b.len() - 4);
    let crc = u32::from_le_bytes(crc.try_into().map_err(|_| bad("crc"))?);
    if crc32c::crc32c(body) != crc {
        return Err(bad("checksum mismatch"));
    }
    match body.split_first() {
        Some((&VERSION, rest)) => {
            postcard::from_bytes(rest).map_err(|e| bad(&format!("decode: {e}")))
        }
        _ => Err(bad("unknown version")),
    }
}

/// One control file per timeline in a directory.
#[derive(Debug, Clone)]
pub struct LocalMeta {
    dir: PathBuf,
}

impl LocalMeta {
    pub fn open(dir: &Path) -> Result<LocalMeta, Error> {
        fs::create_dir_all(dir).map_err(|e| Error::Store(format!("meta dir: {e}")))?;
        Ok(LocalMeta {
            dir: dir.to_path_buf(),
        })
    }

    fn path(&self, tl: &TimelineId) -> PathBuf {
        self.dir.join(format!("{}-{}.meta", tl.tenant, tl.timeline))
    }
}

fn io(e: std::io::Error) -> Error {
    Error::Store(format!("meta: {e}"))
}

#[async_trait]
impl MetaStore for LocalMeta {
    async fn load_all(&self) -> Result<Vec<(TimelineId, AcceptorState)>, Error> {
        let mut out = Vec::new();
        for e in fs::read_dir(&self.dir).map_err(io)? {
            let p = e.map_err(io)?.path();
            let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".tmp") {
                let _ = fs::remove_file(&p);
                continue;
            }
            let Some(stem) = name.strip_suffix(".meta") else {
                continue;
            };
            let Some((t, l)) = stem.split_once('-') else {
                continue;
            };
            let tl = TimelineId::new(t.parse::<Id>()?, l.parse::<Id>()?);
            out.push((tl, decode(&fs::read(&p).map_err(io)?)?));
        }
        Ok(out)
    }

    async fn put(&self, tl: &TimelineId, st: &AcceptorState) -> Result<(), Error> {
        // Blocking file I/O on the caller's thread: this runs a few times per
        // election, never per append.
        let bytes = encode(st)?;
        let path = self.path(tl);
        let tmp = path.with_extension("tmp");
        let mut f = fs::File::create(&tmp).map_err(io)?;
        f.write_all(&bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
        drop(f);
        fs::rename(&tmp, &path).map_err(io)?;
        fs::File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Lsn, ServerInfo};

    #[tokio::test]
    async fn local_meta_round_trips_and_overwrites() {
        let d = tempfile::tempdir().unwrap();
        let m = LocalMeta::open(d.path()).unwrap();
        let tl = TimelineId::new(Id([1; 16]), Id([2; 16]));
        let mut st = AcceptorState::new(ServerInfo::default(), Lsn(100));
        m.put(&tl, &st).await.unwrap();
        st.term = 4;
        m.put(&tl, &st).await.unwrap();
        let all = m.load_all().await.unwrap();
        assert_eq!(all, vec![(tl, st)]);
    }

    #[test]
    fn a_corrupt_file_is_an_error() {
        let st = AcceptorState::new(ServerInfo::default(), Lsn(1));
        let mut b = encode(&st).unwrap();
        b[2] ^= 1;
        assert!(decode(&b).is_err());
    }
}
