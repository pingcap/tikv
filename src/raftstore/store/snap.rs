use std::io::{self, Write, ErrorKind, Seek, SeekFrom, Read};
use std::fmt::{self, Formatter, Display};
use std::fs::{self, File, OpenOptions, Metadata};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};

use crc::crc32::{self, Digest, Hasher32};
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use protobuf::Message;

use kvproto::eraftpb::Snapshot;
use kvproto::raft_serverpb::RaftSnapshotData;
use raftstore::store::Msg;
use util::transport::SendCh;
use util::HandyRwLock;

const TMP_FILE_SUFFIX: &'static str = ".tmp";

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
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
    size_track: Arc<RwLock<u64>>,
    // File is the file obj represent the tmpfile, string is the actual path to
    // tmpfile.
    tmp_file: Option<(File, String)>,
}

impl SnapFile {
    fn new<T: Into<PathBuf>>(snap_dir: T,
                             size_track: Arc<RwLock<u64>>,
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
            size_track: size_track,
            tmp_file: None,
        };
        try!(f.init());
        Ok(f)
    }

    pub fn init(&mut self) -> io::Result<()> {
        if self.exists() || self.tmp_file.is_some() {
            return Ok(());
        }

        let tmp_path = format!("{}{}", self.path().display(), TMP_FILE_SUFFIX);
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
        if !self.exists() {
            return Ok(());
        }
        let size = try!(self.meta()).len();
        try!(fs::remove_file(self.path()));
        let mut size_track = self.size_track.wl();
        *size_track = size_track.saturating_sub(size);
        Ok(())
    }

    /// Use the content in temporary files replace the target file.
    ///
    /// Please note that this method can only be called once.
    pub fn save(&mut self) -> io::Result<()> {
        debug!("saving to {}", self.file.as_path().display());
        if let Some((mut f, path)) = self.tmp_file.take() {
            try!(f.write_u32::<BigEndian>(self.digest.sum32()));
            try!(f.flush());
            let file_len = try!(fs::metadata(&path)).len();
            let mut size_track = self.size_track.wl();
            try!(fs::rename(path, self.file.as_path()));
            *size_track = size_track.saturating_add(file_len);
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
            debug!("deleting {}", path);
            if let Err(e) = fs::remove_file(&path) {
                warn!("failed to delete temporary file {}: {:?}", path, e);
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SnapEntry {
    Generating = 1,
    Sending = 2,
    Receiving = 3,
    Applying = 4,
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

/// `SnapManagerCore` trace all current processing snapshots.
pub struct SnapManagerCore {
    // directory to store snapfile.
    base: String,
    registry: HashMap<SnapKey, Vec<SnapEntry>>,
    ch: Option<SendCh<Msg>>,
    snap_size: Arc<RwLock<u64>>,
}

impl SnapManagerCore {
    pub fn new<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManagerCore {
        SnapManagerCore {
            base: path.into(),
            registry: map![],
            ch: ch,
            snap_size: Arc::new(RwLock::new(0)),
        }
    }

    pub fn init(&self) -> io::Result<()> {
        let path = Path::new(&self.base);
        if !path.exists() {
            try!(fs::create_dir_all(path));
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(ErrorKind::Other,
                                      format!("{} should be a directory", path.display())));
        }
        let mut size = self.snap_size.wl();
        for f in try!(fs::read_dir(path)) {
            let p = try!(f);
            if try!(p.file_type()).is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        try!(fs::remove_file(p.path()));
                    } else {
                        *size += try!(p.metadata()).len();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn list_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let path = Path::new(&self.base);
        let read_dir = try!(fs::read_dir(path));
        Ok(read_dir.filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!("failed to list content of {}: {:?}", self.base, e);
                        return None;
                    }
                    Ok(p) => p,
                };
                match p.file_type() {
                    Ok(t) if t.is_file() => {}
                    _ => return None,
                }
                let file_name = p.file_name();
                let name = match file_name.to_str() {
                    None => return None,
                    Some(n) => n,
                };
                let is_sending = name.starts_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.')
                    .next()
                    .map_or_else(|| vec![], |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    });
                if numbers.len() != 3 {
                    error!("failed to parse snapkey from {}", name);
                    return None;
                }
                Some((SnapKey::new(numbers[0], numbers[1], numbers[2]), is_sending))
            })
            .collect())
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.registry.contains_key(key)
    }

    #[inline]
    pub fn get_snap_file(&self, key: &SnapKey, is_sending: bool) -> io::Result<SnapFile> {
        SnapFile::new(&self.base, self.snap_size.clone(), is_sending, key)
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not garanteed to be accurate.
    pub fn get_total_snap_size(&self) -> u64 {
        *self.snap_size.rl()
    }

    pub fn register(&mut self, key: SnapKey, entry: SnapEntry) {
        debug!("register [key: {}, entry: {:?}]", key, entry);
        match self.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!("{} is registered more than 1 time!!!", e.key());
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }

        self.notify_stats();
    }

    pub fn deregister(&mut self, key: &SnapKey, entry: &SnapEntry) {
        debug!("deregister [key: {}, entry: {:?}]", key, entry);
        let mut need_clean = false;
        let mut handled = false;
        if let Some(e) = self.registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            self.registry.remove(key);
        }
        if handled {
            self.notify_stats();
            return;
        }
        warn!("stale deregister key: {} {:?}", key, entry);
    }

    fn notify_stats(&self) {
        if let Some(ref ch) = self.ch {
            if let Err(e) = ch.try_send(Msg::SnapshotStats) {
                error!("notify snapshot stats failed {:?}", e)
            }
        }
    }

    pub fn stats(&self) -> SnapStats {
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_cnt, mut receiving_cnt) = (0, 0);
        for v in self.registry.values() {
            let (mut is_sending, mut is_receiving) = (false, false);
            for s in v {
                match *s {
                    SnapEntry::Sending | SnapEntry::Generating => is_sending = true,
                    SnapEntry::Receiving | SnapEntry::Applying => is_receiving = true,
                }
            }
            if is_sending {
                sending_cnt += 1;
            }
            if is_receiving {
                receiving_cnt += 1;
            }
        }

        SnapStats {
            sending_count: sending_cnt,
            receiving_count: receiving_cnt,
        }
    }
}

pub type SnapManager = Arc<RwLock<SnapManagerCore>>;

pub fn new_snap_mgr<T: Into<String>>(path: T, ch: Option<SendCh<Msg>>) -> SnapManager {
    Arc::new(RwLock::new(SnapManagerCore::new(path, ch)))
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::fs::File;
    use std::io::Write;
    use std::sync::*;

    use tempdir::TempDir;

    use util::HandyRwLock;
    use super::*;

    #[test]
    fn test_snap_mgr() {
        let path = TempDir::new("test-snap-mgr").unwrap();

        // `mgr` should create the specified directory when it does not exist.
        let path1 = path.path().to_str().unwrap().to_owned() + "/snap1";
        let p = Path::new(&path1);
        assert!(!p.exists());
        let mut mgr = new_snap_mgr(path1.clone(), None);
        mgr.wl().init().unwrap();
        assert!(p.exists());

        // if target is a file, an error should be returned.
        let path2 = path.path().to_str().unwrap().to_owned() + "/snap2";
        File::create(&path2).unwrap();
        mgr = new_snap_mgr(path2, None);
        assert!(mgr.wl().init().is_err());

        // if temporary files exist, they should be deleted.
        let path3 = path.path().to_str().unwrap().to_owned() + "/snap3";
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(RwLock::new(0));
        let f1 = SnapFile::new(&path3, size_track.clone(), true, &key1).unwrap();
        let f2 = SnapFile::new(&path3, size_track.clone(), false, &key1).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SnapFile::new(&path3, size_track.clone(), true, &key2).unwrap();
        f3.save().unwrap();
        let mut f4 = SnapFile::new(&path3, size_track.clone(), false, &key2).unwrap();
        f4.save().unwrap();
        assert!(!f1.exists());
        assert!(!f2.exists());
        assert!(f3.exists());
        assert!(f4.exists());
        assert!(Path::new(&f1.tmp_file.as_ref().unwrap().1).exists());
        assert!(Path::new(&f2.tmp_file.as_ref().unwrap().1).exists());
        mgr = new_snap_mgr(path3, None);
        mgr.wl().init().unwrap();
        assert!(!Path::new(&f1.tmp_file.as_ref().unwrap().1).exists());
        assert!(!Path::new(&f2.tmp_file.as_ref().unwrap().1).exists());
        assert!(f3.exists());
        assert!(f4.exists());
    }

    #[test]
    fn test_snap_size() {
        let path = TempDir::new("test-snap-mgr").unwrap();
        let path_str = path.path().to_str().unwrap();
        let mut mgr = new_snap_mgr(path_str, None);
        mgr.wl().init().unwrap();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);

        let key1 = SnapKey::new(1, 1, 1);
        let test_data = b"test_data";
        let exp_len = test_data.len() as u64 + 4; // 4 is the crc32 sum's len
        let size_track = Arc::new(RwLock::new(0));
        let mut f1 = SnapFile::new(path_str, size_track.clone(), true, &key1).unwrap();
        let mut f2 = SnapFile::new(path_str, size_track.clone(), false, &key1).unwrap();
        f1.write_all(test_data).unwrap();
        f2.write_all(test_data).unwrap();
        let key2 = SnapKey::new(2, 1, 1);
        let mut f3 = SnapFile::new(path_str, size_track.clone(), true, &key2).unwrap();
        f3.write_all(test_data).unwrap();
        f3.save().unwrap();
        let mut f4 = SnapFile::new(path_str, size_track.clone(), false, &key2).unwrap();
        f4.write_all(test_data).unwrap();
        f4.save().unwrap();

        mgr = new_snap_mgr(path_str, None);
        mgr.wl().init().unwrap();
        // temporary file should not be count in snap size.
        assert_eq!(mgr.rl().get_total_snap_size(), exp_len * 2);

        mgr.rl().get_snap_file(&key2, true).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), exp_len);
        mgr.rl().get_snap_file(&key2, false).unwrap().delete();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);

        let mut f4 = mgr.rl().get_snap_file(&key2, true).unwrap();
        f4.write_all(test_data).unwrap();
        assert_eq!(mgr.rl().get_total_snap_size(), 0);
        f4.save().unwrap();
        assert_eq!(mgr.rl().get_total_snap_size(), exp_len);
    }
}
