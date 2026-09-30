//! Segment files: the journal's directory, preparation (`fallocate`, pre-zero,
//! header, `fdatasync`, rename), recycling, and what the filesystem and the
//! block device underneath can do (§28 §7.2, D265–D266).

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, warn};

use super::AlignedBuf;
use super::format::{self, BLOCK, SEGMENT_HEADER};

/// One segment file, open for writing (direct or buffered) and for reads.
#[derive(Debug)]
pub struct SegmentFile {
    pub seq: u64,
    pub path: PathBuf,
    /// The write handle: `O_DIRECT` for the direct tiers.
    pub write: File,
    /// A buffered handle for reads and recovery.
    pub read: File,
    /// Every block has been written once (so an append needs no extent
    /// conversion). False for a segment prepared without pre-zeroing.
    pub zeroed: AtomicBool,
}

impl SegmentFile {
    /// `fdatasync` through the write handle.
    pub fn sync(&self) -> io::Result<()> {
        self.write.sync_data()
    }
}

/// The filesystem a directory is on (from `statfs`), for logging and for
/// choosing a tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsKind {
    Btrfs,
    Ext4,
    Xfs,
    Tmpfs,
    Overlay,
    Other(i64),
}

impl FsKind {
    pub fn of(dir: &Path) -> io::Result<FsKind> {
        let st = rustix::fs::statfs(dir)?;
        #[allow(clippy::unnecessary_cast)]
        Ok(match st.f_type as i64 {
            0x9123_683E => FsKind::Btrfs,
            0xEF53 => FsKind::Ext4,
            0x5846_5342 => FsKind::Xfs,
            0x0102_1994 => FsKind::Tmpfs,
            0x794C_7630 => FsKind::Overlay,
            other => FsKind::Other(other),
        })
    }
}

/// What the block device under a directory reports in sysfs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeviceCaps {
    /// The device's name in sysfs (`nvme0n1`), if found.
    pub device: Option<String>,
    /// `queue/fua`: the device honours Force Unit Access writes.
    pub fua: Option<bool>,
    /// `queue/write_cache` is `write back` (a volatile cache that needs
    /// flushes); `false` for `write through` (PLP drives report this).
    pub volatile_cache: Option<bool>,
    /// `queue/io_poll`: polled queues exist, so IOPOLL works.
    pub io_poll: Option<bool>,
    pub logical_block_size: Option<u32>,
}

impl DeviceCaps {
    /// Read the capabilities of the device holding `dir`. Missing sysfs
    /// entries (containers, network filesystems) leave fields unknown.
    pub fn of(dir: &Path) -> DeviceCaps {
        use std::os::unix::fs::MetadataExt;
        let Ok(meta) = fs::metadata(dir) else {
            return DeviceCaps::default();
        };
        let dev = meta.dev();
        let (major, minor) = (rustix::fs::major(dev), rustix::fs::minor(dev));
        let mut bases = vec![PathBuf::from(format!("/sys/dev/block/{major}:{minor}"))];
        // btrfs (and other multi-device filesystems) report an anonymous
        // device number; the mount's source names the block device.
        if let Some(src) = mount_source(dir)
            && let Some(name) = Path::new(&src).file_name()
        {
            bases.push(Path::new("/sys/class/block").join(name));
        }
        // A partition has no queue/ of its own: its parent's is the disk's.
        let queue = bases
            .iter()
            .flat_map(|b| [b.join("queue"), b.join("../queue")])
            .find(|q| q.is_dir());
        let Some(queue) = queue else {
            return DeviceCaps::default();
        };
        let read = |name: &str| {
            fs::read_to_string(queue.join(name))
                .ok()
                .map(|s| s.trim().to_string())
        };
        let device = fs::canonicalize(queue.join(".."))
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()));
        DeviceCaps {
            device,
            fua: read("fua").map(|v| v == "1"),
            volatile_cache: read("write_cache").map(|v| v.starts_with("write back")),
            io_poll: read("io_poll").map(|v| v == "1"),
            logical_block_size: read("logical_block_size").and_then(|v| v.parse().ok()),
        }
    }
}

/// The source device of the mount holding `dir` (from `/proc/self/mountinfo`:
/// the longest mount point that prefixes the path).
fn mount_source(dir: &Path) -> Option<String> {
    let dir = fs::canonicalize(dir).ok()?;
    let info = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let mut best: Option<(usize, String)> = None;
    for line in info.lines() {
        let (pre, post) = line.split_once(" - ")?;
        let mount_point = pre.split(' ').nth(4)?;
        let source = post.split(' ').nth(1)?;
        if dir.starts_with(mount_point)
            && best
                .as_ref()
                .is_none_or(|(len, _)| mount_point.len() > *len)
        {
            best = Some((mount_point.len(), source.to_string()));
        }
    }
    best.map(|(_, s)| s).filter(|s| s.starts_with("/dev/"))
}

/// Set `FS_NOCOW_FL` on a file or directory (btrfs: overwrite in place; new
/// files in a directory inherit it). Errors are for the caller to log.
pub fn set_nocow(f: &File) -> io::Result<()> {
    use rustix::fs::{IFlags, ioctl_getflags, ioctl_setflags};
    let flags = ioctl_getflags(f)?;
    if !flags.contains(IFlags::NOCOW) {
        ioctl_setflags(f, flags | IFlags::NOCOW)?;
    }
    Ok(())
}

/// `O_DIRECT`, for `OpenOptionsExt::custom_flags`.
pub fn o_direct() -> i32 {
    rustix::fs::OFlags::DIRECT.bits() as i32
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// The journal's directory of segment files, `<seq:016x>.seg`.
#[derive(Debug)]
pub struct SegmentDir {
    pub dir: PathBuf,
    pub size: u64,
    /// Open write handles with `O_DIRECT`.
    pub direct: bool,
    pub fs: FsKind,
}

impl SegmentDir {
    /// Create (if needed) and open the directory. `direct` asks for
    /// `O_DIRECT` write handles; it is dropped, with a warning, when the
    /// filesystem refuses it.
    pub fn open(dir: &Path, size: u64, direct: bool) -> io::Result<SegmentDir> {
        if size < (SEGMENT_HEADER + BLOCK) as u64 || !size.is_multiple_of(BLOCK as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("segment size {size} is not a multiple of {BLOCK} above one block"),
            ));
        }
        fs::create_dir_all(dir)?;
        let fs_kind = FsKind::of(dir).unwrap_or(FsKind::Other(0));
        if fs_kind == FsKind::Btrfs
            && let Err(e) = File::open(dir).and_then(|d| set_nocow(&d))
        {
            warn!(dir = %dir.display(), error = %e, "could not set NOCOW on the journal directory");
        }
        let mut sd = SegmentDir {
            dir: dir.to_path_buf(),
            size,
            direct,
            fs: fs_kind,
        };
        if direct && !sd.probe_direct()? {
            warn!(dir = %dir.display(), fs = ?fs_kind, "O_DIRECT is refused here; using buffered writes");
            sd.direct = false;
        }
        // Leftovers of an interrupted preparation.
        for e in fs::read_dir(dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "tmp") {
                let _ = fs::remove_file(&p);
            }
        }
        Ok(sd)
    }

    /// Whether `O_DIRECT` opens and writes a block here.
    fn probe_direct(&self) -> io::Result<bool> {
        let p = self.dir.join("direct-probe.tmp");
        let res = (|| -> io::Result<()> {
            let f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .custom_flags(o_direct())
                .open(&p)?;
            let mut b = AlignedBuf::new(BLOCK);
            b.zero_to(BLOCK);
            f.write_all_at(&b, 0)
        })();
        let _ = fs::remove_file(&p);
        match res {
            Ok(()) => Ok(true),
            Err(e) if e.raw_os_error() == Some(rustix::io::Errno::INVAL.raw_os_error()) => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    pub fn path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{seq:016x}.seg"))
    }

    fn free_path(&self, seq: u64) -> PathBuf {
        self.dir.join(format!("{seq:016x}.free"))
    }

    /// Retire a freed segment: rename it out of the replay set, durably, so
    /// a restart never replays its records. Returns the file to recycle.
    pub fn retire(&self, seq: u64) -> io::Result<PathBuf> {
        let to = self.free_path(seq);
        fs::rename(self.path(seq), &to)?;
        sync_dir(&self.dir)?;
        Ok(to)
    }

    /// Retired segment files waiting to be recycled.
    pub fn list_free(&self) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for e in fs::read_dir(&self.dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "free") {
                out.push(p);
            }
        }
        out.sort();
        Ok(out)
    }

    /// Every segment's sequence number, ascending.
    pub fn list(&self) -> io::Result<Vec<u64>> {
        let mut out = Vec::new();
        for e in fs::read_dir(&self.dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "seg")
                && let Some(seq) = p
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| u64::from_str_radix(s, 16).ok())
            {
                out.push(seq);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Open an existing segment, checking its header.
    pub fn open_segment(&self, seq: u64) -> io::Result<Arc<SegmentFile>> {
        let path = self.path(seq);
        let read = File::open(&path)?;
        let mut h = vec![0u8; SEGMENT_HEADER];
        read.read_exact_at(&mut h, 0)?;
        match format::parse_segment_header(&h) {
            Some((s, size)) if s == seq && size == self.size => {}
            Some((s, size)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{}: header names segment {s} of {size} bytes (want {seq}, {})",
                        path.display(),
                        self.size
                    ),
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: not a journal segment", path.display()),
                ));
            }
        }
        let mut o = OpenOptions::new();
        o.read(true).write(true);
        if self.direct {
            o.custom_flags(o_direct());
        }
        let write = o.open(&path)?;
        Ok(Arc::new(SegmentFile {
            seq,
            path,
            write,
            read,
            zeroed: AtomicBool::new(true),
        }))
    }

    /// Make segment `seq` ready to write: reuse the file of freed segment
    /// `recycle` if given (its old records stop validating under the new
    /// number), else create, `fallocate` and pre-zero a new one. The header
    /// goes in last; the file is `fdatasync`ed and renamed into place, and the
    /// directory synced, before this returns.
    pub fn prepare(
        &self,
        seq: u64,
        recycle: Option<&Path>,
        zero: bool,
    ) -> io::Result<Arc<SegmentFile>> {
        let tmp = self.dir.join(format!("{seq:016x}.tmp"));
        let started = std::time::Instant::now();
        let recycled = match recycle {
            Some(old) => match fs::rename(old, &tmp) {
                Ok(()) => true,
                Err(e) => {
                    warn!(old = %old.display(), error = %e, "could not recycle a segment; creating one");
                    false
                }
            },
            None => false,
        };
        let made = (|| -> io::Result<()> {
            let mut o = OpenOptions::new();
            o.read(true).write(true);
            if !recycled {
                o.create_new(true);
            }
            if self.direct {
                o.custom_flags(o_direct());
            }
            let f = o.open(&tmp)?;
            if !recycled {
                if self.fs == FsKind::Btrfs
                    && let Err(e) = set_nocow(&f)
                {
                    debug!(error = %e, "NOCOW on a segment");
                }
                if let Err(e) =
                    rustix::fs::fallocate(&f, rustix::fs::FallocateFlags::empty(), 0, self.size)
                {
                    debug!(error = %e, "fallocate refused; zero-filling only");
                }
                // Pre-zero: turns unwritten extents into written ones, so an
                // append never needs a metadata commit for the extent. Skipped
                // when the journal is ingesting fast (`zero` false): the zeros
                // would double the bytes written and make each data write an
                // overwrite, which a flash drive handles slowly. The preparer
                // zeroes such a segment later, while it is idle
                // ([`SegmentDir::zero_rest`]).
                if zero {
                    zero_range(&f, 0, self.size)?;
                }
            }
            let mut h = AlignedBuf::new(SEGMENT_HEADER);
            h.extend_from_slice(&format::segment_header(seq, self.size));
            f.write_all_at(&h, 0)?;
            f.sync_data()?;
            drop(f);
            Ok(())
        })();
        if let Err(e) = made {
            // Do not leave a half-made file behind: a recycled one goes back
            // to the free pool, a new one is deleted.
            match (recycled, recycle) {
                (true, Some(old)) => {
                    let _ = fs::rename(&tmp, old);
                }
                _ => {
                    let _ = fs::remove_file(&tmp);
                }
            }
            return Err(e);
        }
        fs::rename(&tmp, self.path(seq))?;
        sync_dir(&self.dir)?;
        debug!(
            seq,
            recycled,
            took_ms = started.elapsed().as_millis() as u64,
            "segment prepared"
        );
        let f = self.open_segment(seq)?;
        f.zeroed.store(zero || recycled, Ordering::Relaxed);
        Ok(f)
    }

    /// Pre-zero the unused body of a segment that was prepared without it.
    /// Only for a segment nothing writes to.
    pub fn zero_rest(&self, f: &SegmentFile) -> io::Result<()> {
        if f.zeroed.load(Ordering::Relaxed) {
            return Ok(());
        }
        zero_range(&f.write, SEGMENT_HEADER as u64, self.size)?;
        f.write.sync_data()?;
        f.zeroed.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Delete a retired segment file.
    pub fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)?;
        sync_dir(&self.dir)
    }
}

/// Write zeros over `[from, to)` in 1 MiB direct-I/O-safe chunks.
fn zero_range(f: &File, from: u64, to: u64) -> io::Result<()> {
    let chunk = (1usize << 20).min(to as usize);
    let mut zeros = AlignedBuf::new(chunk);
    zeros.zero_to(chunk);
    let mut at = from;
    while at < to {
        let n = chunk.min((to - at) as usize);
        f.write_all_at(&zeros[..n], at)?;
        at += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepare_recycle_and_list() {
        let d = tempfile::tempdir().unwrap();
        let sd = SegmentDir::open(d.path(), 64 << 10, false).unwrap();
        let s1 = sd.prepare(1, None, true).unwrap();
        assert_eq!(s1.read.metadata().unwrap().len(), 64 << 10);
        s1.write
            .write_all_at(&[7u8; 16], SEGMENT_HEADER as u64)
            .unwrap();
        drop(s1);
        let old = sd.retire(1).unwrap();
        assert_eq!(sd.list().unwrap(), Vec::<u64>::new());
        let s2 = sd.prepare(2, Some(&old), true).unwrap();
        assert_eq!(sd.list().unwrap(), vec![2]);
        let mut b = [0u8; 16];
        s2.read
            .read_exact_at(&mut b, SEGMENT_HEADER as u64)
            .unwrap();
        assert_eq!(b, [7u8; 16], "a recycled segment keeps its old bytes");
        assert!(sd.open_segment(2).is_ok());
    }

    #[test]
    fn a_wrong_header_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let sd = SegmentDir::open(d.path(), 64 << 10, false).unwrap();
        sd.prepare(3, None, true).unwrap();
        fs::rename(sd.path(3), sd.path(4)).unwrap();
        assert!(sd.open_segment(4).is_err());
    }

    #[test]
    fn capabilities_do_not_fail_on_any_directory() {
        let d = tempfile::tempdir().unwrap();
        let _ = DeviceCaps::of(d.path());
        assert!(FsKind::of(d.path()).is_ok());
    }
}
