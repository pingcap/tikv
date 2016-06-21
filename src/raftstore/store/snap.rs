use std::io::{self, Write, ErrorKind, Seek, SeekFrom, Read};
use std::fmt::{self, Formatter, Display};
use std::fs::{self, File, OpenOptions, Metadata};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock, Mutex, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::{Path, PathBuf};
use std::thread;

use crc::crc32::{self, Digest, Hasher32};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use protobuf::Message;

use kvproto::raftpb::Snapshot;
use kvproto::raft_serverpb::RaftSnapshotData;
use util::worker::Worker;
use raftstore::Result;
use super::worker::{SnapTask, SnapRunner};
use super::{PeerStorage, SendCh, Msg};


#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,
}

impl SnapKey {
    #[inline]
    pub fn new(region_id: u64, term: u64, idx: u64) -> SnapKey {
        SnapKey {
            region_id: region_id,
            term: term,
            idx: idx,
        }
    }

    #[inline]
    pub fn from_region_snap(region_id: u64, snap: &Snapshot) -> SnapKey {
        let index = snap.get_metadata().get_index();
        let term = snap.get_metadata().get_term();
        SnapKey::new(region_id, term, index)
    }

    pub fn from_snap(snap: &Snapshot) -> io::Result<SnapKey> {
        let mut snap_data = RaftSnapshotData::new();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            return Err(io::Error::new(ErrorKind::Other, e));
        }

        Ok(SnapKey::from_region_snap(snap_data.get_region().get_id(), snap))
    }
}

impl Display for SnapKey {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}_{}_{}", self.region_id, self.term, self.idx)
    }
}

/// Name prefix for the self-generated snapshot file.
const SNAP_GEN_PREFIX: &'static str = "gen";
/// Name prefix for the received snapshot file.
const SNAP_REV_PREFIX: &'static str = "rev";

/// A structure represents the snapshot file.
///
/// All changes to the file will be written to `tmp_file` first, and use
/// `save` method to make them persistent. When saving a crc32 checksum
/// will be appended to the file end automatically.
pub struct SnapFile {
    file: PathBuf,
    digest: Digest,
    // File is the file obj represent the tmpfile, string is the actual path to
    // tmpfile.
    tmp_file: Option<(File, String)>,
}

impl SnapFile {
    pub fn new<T: Into<PathBuf>>(snap_dir: T,
                                 is_sending: bool,
                                 key: &SnapKey)
                                 -> io::Result<SnapFile> {
        let mut file_path = snap_dir.into();
        if !file_path.exists() {
            try!(fs::create_dir_all(file_path.as_path()));
        }
        let prefix = if is_sending {
            SNAP_GEN_PREFIX
        } else {
            SNAP_REV_PREFIX
        };
        let file_name = format!("{}_{}.snap", prefix, key);
        file_path.push(&file_name);

        let mut f = SnapFile {
            file: file_path,
            digest: Digest::new(crc32::IEEE),
            tmp_file: None,
        };
        try!(f.init());
        Ok(f)
    }

    pub fn init(&mut self) -> io::Result<()> {
        if self.exists() || self.tmp_file.is_some() {
            return Ok(());
        }

        let tmp_path = format!("{}.tmp", self.path().display());
        let tmp_f = try!(OpenOptions::new().write(true).create_new(true).open(&tmp_path));
        self.tmp_file = Some((tmp_f, tmp_path));
        Ok(())
    }

    pub fn meta(&self) -> io::Result<Metadata> {
        self.file.metadata()
    }

    /// Validate whether current file is broken.
    pub fn validate(&self) -> io::Result<()> {
        let mut reader = try!(File::open(self.path()));
        let mut digest = Digest::new(crc32::IEEE);
        let len = try!(reader.metadata()).len();
        if len < 4 {
            return Err(io::Error::new(ErrorKind::InvalidInput, format!("file length {} < 4", len)));
        }
        let to_read = len as usize - 4;
        let mut total_read = 0;
        let mut buffer = vec![0; 4098];
        loop {
            let read = try!(reader.read(&mut buffer));
            if total_read + read >= to_read {
                digest.write(&buffer[..to_read - total_read]);
                try!(reader.seek(SeekFrom::End(-4)));
                break;
            }
            digest.write(&buffer);
            total_read += read;
        }
        let sum = try!(reader.read_u32::<BigEndian>());
        if sum != digest.sum32() {
            return Err(io::Error::new(ErrorKind::InvalidData,
                                      format!("crc not correct: {} != {}", sum, digest.sum32())));
        }
        Ok(())
    }

    pub fn exists(&self) -> bool {
        self.file.exists() && self.file.is_file()
    }

    pub fn delete(&self) {
        if let Err(e) = self.try_delete() {
            error!("failed to delete {}: {:?}", self.path().display(), e);
        }
    }

    pub fn try_delete(&self) -> io::Result<()> {
        debug!("deleting {}", self.path().display());
        fs::remove_file(self.path())
    }

    /// Use the content in temporary files replace the target file.
    ///
    /// Please note that this method can only be called once.
    pub fn save(&mut self) -> io::Result<()> {
        if let Some((mut f, path)) = self.tmp_file.take() {
            try!(f.write_u32::<BigEndian>(self.digest.sum32()));
            try!(f.flush());
            try!(fs::rename(path, self.file.as_path()));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        self.file.as_path()
    }
}

impl Write for SnapFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.tmp_file.is_none() {
            return Ok(0);
        }
        let written = try!(self.tmp_file.as_mut().unwrap().0.write(buf));
        self.digest.write(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.tmp_file.is_none() {
            return Ok(());
        }
        self.tmp_file.as_mut().unwrap().0.flush()
    }
}

impl Drop for SnapFile {
    fn drop(&mut self) {
        if let Some((_, path)) = self.tmp_file.take() {
            debug!("deleteing {}", path);
            if let Err(e) = fs::remove_file(&path) {
                warn!("failed to delete temporary file {}: {:?}", path, e);
            }
        }
    }
}

struct SnapEntry {
    is_sending: bool,
    ref_cnt: usize,
}

impl SnapEntry {
    fn new(is_sending: bool, ref_cnt: usize) -> SnapEntry {
        SnapEntry {
            is_sending: is_sending,
            ref_cnt: ref_cnt,
        }
    }
}

type SnapReg = HashMap<SnapKey, SnapEntry>;

fn register(base_dir: &str, registry: &mut SnapReg, key: SnapKey, is_sending: bool) {
    debug!("register [key: {}, is_sending: {}]", key, is_sending);
    match registry.entry(key) {
        Entry::Occupied(mut e) => {
            if e.get().is_sending == is_sending {
                e.get_mut().ref_cnt += 1;
            } else {
                info!("seems leadership of {} changed, cleanup old snapfiles",
                      e.key());
                if let Ok(f) = SnapFile::new(base_dir, !is_sending, e.key()) {
                    f.delete();
                }
                e.insert(SnapEntry::new(is_sending, 1));
            }
        }
        Entry::Vacant(e) => {
            e.insert(SnapEntry::new(is_sending, 1));
        }
    }
}

#[derive(Debug)]
pub struct SnapState {
    pub key: SnapKey,
    is_sending: bool,
    try_cnt: u8,
    pub snap: Mutex<Option<Snapshot>>,
    pub failed: AtomicBool,
}

impl SnapState {
    pub fn new(key: SnapKey, is_sending: bool, try_cnt: u8) -> SnapState {
        SnapState {
            key: key,
            is_sending: is_sending,
            try_cnt: try_cnt,
            snap: Mutex::new(None),
            failed: AtomicBool::new(false),
        }
    }

    fn take_snap(&self) -> Option<Snapshot> {
        let mut snap = self.snap.lock().unwrap();
        if snap.is_some() {
            Some(snap.take().unwrap())
        } else {
            None
        }
    }
}

pub trait GenericSendCh<T: Send>: Send {
    fn send(&self, t: T) -> Result<()>;
}

impl GenericSendCh<Msg> for SendCh {
    fn send(&self, m: Msg) -> Result<()> {
        SendCh::send(self, m)
    }
}

const MAX_SNAP_TRY_CNT: u8 = 5;

/// `SnapManagerCore` trace all current processing snapshots.
pub struct SnapManagerCore {
    // directory to store snapfile.
    base: String,
    registry: SnapReg,
    snaps: HashMap<u64, Arc<SnapState>>,
    snap_worker: Mutex<Worker<SnapTask>>,
}

impl SnapManagerCore {
    pub fn new<T: Into<String>>(path: T) -> SnapManagerCore {
        SnapManagerCore {
            base: path.into(),
            registry: map![],
            snaps: map![],
            snap_worker: Mutex::new(Worker::new("snapshot worker")),
        }
    }

    pub fn start<H: GenericSendCh<Msg> + 'static>(&mut self, ch: H) -> Result<()> {
        box_try!(self.snap_worker.lock().unwrap().start(SnapRunner::new(&self.base, ch)));
        try!(self.try_recover());
        Ok(())
    }

    pub fn stop(&mut self) -> thread::Result<()> {
        self.snap_worker.lock().unwrap().stop()
    }

    fn try_recover(&self) -> io::Result<()> {
        let path = Path::new(&self.base);
        if !path.exists() {
            try!(fs::create_dir_all(path));
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(ErrorKind::Other,
                                      format!("{} should be a directory", path.display())));
        }
        for path in try!(fs::read_dir(path)) {
            let p = try!(path);
            if !try!(p.file_type()).is_file() {
                continue;
            }
            debug!("deleting {}", p.path().display());
            try!(fs::remove_file(p.path()));
            // TODO: resume applying when suitable
        }
        Ok(())
    }

    pub fn gen_snap(&mut self,
                    store: &RwLockWriteGuard<PeerStorage>,
                    reschedule: bool)
                    -> Result<(SnapKey, Option<Snapshot>)> {
        let region_id = store.get_region_id();
        let mut next_try_cnt = 1;
        let mut previous_key = None;
        if let Entry::Occupied(e) = self.snaps.entry(region_id) {
            if reschedule {
                let state = e.remove();
                state.failed.store(true, Ordering::SeqCst);
                previous_key = Some(state.key.clone());
            } else if e.get().failed.load(Ordering::SeqCst) {
                next_try_cnt = e.get().try_cnt + 1;
                previous_key = Some(e.remove().key.clone());
            } else {
                let key = e.get().key.clone();
                let s = e.get().take_snap();
                if s.is_some() {
                    e.remove();
                }
                return Ok((key, s));
            }
        }

        if let Some(key) = previous_key {
            self.deregister(&key, true);
        }

        let ranges = store.region_key_ranges();
        let snap = store.raw_snapshot();
        let applied_idx = box_try!(store.load_applied_index(&snap));
        let term = box_try!(store.term(applied_idx));
        let key = SnapKey::new(region_id, term, applied_idx);
        let state = Arc::new(SnapState::new(key.clone(), true, next_try_cnt));
        let task = SnapTask::new(snap, state.clone(), ranges);
        box_try!(self.snap_worker.lock().unwrap().schedule(task));
        self.snaps.insert(region_id, state);
        self.register(key.clone(), true);
        Ok((key, None))
    }

    #[inline]
    pub fn get_snap_file(&self, key: &SnapKey, is_sending: bool) -> io::Result<SnapFile> {
        SnapFile::new(&self.base, is_sending, key)
    }

    pub fn register(&mut self, key: SnapKey, is_sending: bool) {
        register(&self.base, &mut self.registry, key, is_sending)
    }

    pub fn deregister(&mut self, key: &SnapKey, is_sending: bool) {
        debug!("deregister [key: {}, is_sending: {}]", key, is_sending);
        let mut need_cleanup = false;
        if let Some(e) = self.registry.get_mut(key) {
            if e.is_sending != is_sending {
                warn!("stale deregister key: {} {}", key, is_sending);
                return;
            }
            e.ref_cnt -= 1;
            need_cleanup = e.ref_cnt == 0;
        };
        if need_cleanup {
            self.registry.remove(key);
            if let Ok(f) = self.get_snap_file(key, is_sending) {
                f.delete();
            }
        }
    }
}

pub type SnapManager = Arc<RwLock<SnapManagerCore>>;

pub fn new_snap_mgr<T: Into<String>>(path: T) -> SnapManager {
    Arc::new(RwLock::new(SnapManagerCore::new(path)))
}
