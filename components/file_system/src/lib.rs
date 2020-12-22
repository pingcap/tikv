// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

#[macro_use]
extern crate lazy_static;
extern crate test;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod condvar;
mod file;
mod rate_limiter;
mod time_util;

pub use file::{File, OpenOptions};
pub use rate_limiter::{get_io_rate_limiter, set_io_rate_limiter, IORateLimiter};

pub use std::fs::{
    canonicalize, create_dir, create_dir_all, hard_link, metadata, read_dir, read_link, remove_dir,
    remove_dir_all, remove_file, rename, set_permissions, symlink_metadata, DirBuilder, DirEntry,
    FileType, Metadata, Permissions, ReadDir,
};

use std::cell::Cell;
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use openssl::error::ErrorStack;
use openssl::hash::{self, Hasher, MessageDigest};

#[derive(Debug)]
pub enum IOOp {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IOType {
    Other = 0,
    Read = 1,
    Write = 2,
    Coprocessor = 3,
    Flush = 4,
    Compaction = 5,
    Replication = 6,
    LoadBalance = 7,
    Import = 8,
    Export = 9,
}

pub const IO_TYPE_VARIANTS: usize = 10;

thread_local! {
    static IO_TYPE: Cell<IOType> = Cell::new(IOType::Other)
}

fn set_io_type(new_io_type: IOType) {
    IO_TYPE.with(|io_type| {
        io_type.set(new_io_type);
    });
}

fn get_io_type() -> IOType {
    IO_TYPE.with(|io_type| io_type.get())
}

pub struct WithIOType {
    previous_io_type: IOType,
}

impl WithIOType {
    pub fn new(new_io_type: IOType) -> WithIOType {
        let previous_io_type = get_io_type();
        set_io_type(new_io_type);
        WithIOType { previous_io_type }
    }
}

impl Drop for WithIOType {
    fn drop(&mut self) {
        set_io_type(self.previous_io_type);
    }
}

/// Indicates how large a buffer to pre-allocate before reading the entire file.
fn initial_buffer_size(file: &File) -> io::Result<usize> {
    // Allocate one extra byte so the buffer doesn't need to grow before the
    // final `read` call at the end of the file.  Don't worry about `usize`
    // overflow because reading will fail regardless in that case.
    file.metadata().map(|m| m.len() as usize + 1)
}

/// Write a slice as the entire contents of a file.
pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
    File::create(path)?.write_all(contents.as_ref())
}

/// Read the entire contents of a file into a bytes vector.
pub fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::with_capacity(initial_buffer_size(&file)?);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Read the entire contents of a file into a string.
pub fn read_to_string<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut string = String::with_capacity(initial_buffer_size(&file)?);
    file.read_to_string(&mut string)?;
    Ok(string)
}

fn copy_imp(from: &Path, to: &Path, sync: bool) -> io::Result<u64> {
    if !from.is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "the source path is not an existing regular file",
        ));
    }

    let mut reader = File::open(from)?;
    let mut writer = File::create(to)?;
    let perm = reader.metadata()?.permissions();

    let ret = io::copy(&mut reader, &mut writer)?;
    set_permissions(to, perm)?;
    if sync {
        writer.sync_all()?;
        if let Some(parent) = to.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(ret)
}

/// Copies the contents of one file to another. This function will also
/// copy the permission bits of the original file to the destination file.
pub fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
    copy_imp(from.as_ref(), to.as_ref(), false /* sync */)
}

/// Copies the contents and permission bits of one file to another, then synchronizes.
pub fn copy_and_sync<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
    copy_imp(from.as_ref(), to.as_ref(), true /* sync */)
}

pub fn get_file_size<P: AsRef<Path>>(path: P) -> io::Result<u64> {
    let meta = metadata(path)?;
    Ok(meta.len())
}

pub fn file_exists<P: AsRef<Path>>(file: P) -> bool {
    let path = file.as_ref();
    path.exists() && path.is_file()
}

/// Deletes given path from file system. Returns `true` on success, `false` if the file doesn't exist.
/// Otherwise the raw error will be returned.
pub fn delete_file_if_exist<P: AsRef<Path>>(file: P) -> io::Result<bool> {
    match remove_file(&file) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Deletes given path from file system. Returns `true` on success, `false` if the directory doesn't
/// exist. Otherwise the raw error will be returned.
pub fn delete_dir_if_exist<P: AsRef<Path>>(dir: P) -> io::Result<bool> {
    match remove_dir_all(&dir) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Creates a new, empty directory at the provided path. Returns `true` on success,
/// `false` if the directory already exists. Otherwise the raw error will be returned.
pub fn create_dir_if_not_exist<P: AsRef<Path>>(dir: P) -> io::Result<bool> {
    match create_dir(&dir) {
        Ok(_) => Ok(true),
        Err(ref e) if e.kind() == ErrorKind::AlreadyExists => Ok(false),
        Err(e) => Err(e),
    }
}

/// Call fsync on directory by its path
pub fn sync_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    // File::open will not error when opening a directory
    // because it just call libc::open and do not do the file or dir check
    File::open(path)?.sync_all()
}

const DIGEST_BUFFER_SIZE: usize = 1024 * 1024;

/// Calculates the given file's Crc32 checksum.
pub fn calc_crc32<P: AsRef<Path>>(path: P) -> io::Result<u32> {
    let mut digest = crc32fast::Hasher::new();
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut buf = vec![0; DIGEST_BUFFER_SIZE];
    loop {
        match f.read(&mut buf[..]) {
            Ok(0) => {
                return Ok(digest.finalize());
            }
            Ok(n) => {
                digest.update(&buf[..n]);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Calculates crc32 and decrypted size for a given reader.
pub fn calc_crc32_and_size<R: Read>(reader: &mut R) -> io::Result<(u32, u64)> {
    let mut digest = crc32fast::Hasher::new();
    let (mut buf, mut fsize) = (vec![0; DIGEST_BUFFER_SIZE], 0);
    loop {
        match reader.read(&mut buf[..]) {
            Ok(0) => {
                return Ok((digest.finalize(), fsize as u64));
            }
            Ok(n) => {
                digest.update(&buf[..n]);
                fsize += n;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

/// Calculates the given content's CRC32 checksum.
pub fn calc_crc32_bytes(contents: &[u8]) -> u32 {
    let mut digest = crc32fast::Hasher::new();
    digest.update(contents);
    digest.finalize()
}

pub fn sha256(input: &[u8]) -> Result<Vec<u8>, ErrorStack> {
    hash::hash(MessageDigest::sha256(), input).map(|digest| digest.to_vec())
}

/// Wrapper of a reader which computes its SHA-256 hash while reading.
pub struct Sha256Reader<R> {
    reader: R,
    hasher: Arc<Mutex<Hasher>>,
}

impl<R> Sha256Reader<R> {
    /// Creates a new `Sha256Reader`, wrapping the given reader.
    pub fn new(reader: R) -> Result<(Self, Arc<Mutex<Hasher>>), ErrorStack> {
        let hasher = Arc::new(Mutex::new(Hasher::new(MessageDigest::sha256())?));
        Ok((
            Sha256Reader {
                reader,
                hasher: hasher.clone(),
            },
            hasher,
        ))
    }
}

impl<R: Read> Read for Sha256Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.hasher.lock().unwrap().update(&buf[..len])?;
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::io::Write;
    use std::iter;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_get_file_size() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        // Ensure it works to get the size of an empty file.
        let empty_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&empty_file)
                .unwrap();
        }
        assert_eq!(get_file_size(&empty_file).unwrap(), 0);

        // Ensure it works to get the size of an non-empty file.
        let non_empty_file = dir_path.join("non_empty_file");
        let size = 5;
        let v = vec![0; size];
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&non_empty_file)
                .unwrap();
            f.write_all(&v[..]).unwrap();
        }
        assert_eq!(get_file_size(&non_empty_file).unwrap(), size as u64);

        // Ensure it works for non-existent file.
        let non_existent_file = dir_path.join("non_existent_file");
        assert!(get_file_size(&non_existent_file).is_err());
    }

    #[test]
    fn test_file_exists() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        assert_eq!(file_exists(&dir_path), false);

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);

        let non_existent_file = dir_path.join("non_existent_file");
        assert_eq!(file_exists(&non_existent_file), false);
    }

    #[test]
    fn test_delete_file_if_exist() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);
        delete_file_if_exist(&existent_file).unwrap();
        assert_eq!(file_exists(&existent_file), false);

        let non_existent_file = dir_path.join("non_existent_file");
        delete_file_if_exist(&non_existent_file).unwrap();
    }

    fn gen_rand_file<P: AsRef<Path>>(path: P, size: usize) -> u32 {
        let mut rng = thread_rng();
        let s: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(size)
            .collect();
        write(path, s.as_bytes()).unwrap();
        calc_crc32_bytes(s.as_bytes())
    }

    #[test]
    fn test_calc_crc32() {
        let tmp_dir = TempDir::new().unwrap();

        let small_file = tmp_dir.path().join("small.txt");
        let small_checksum = gen_rand_file(&small_file, 1024);
        assert_eq!(calc_crc32(&small_file).unwrap(), small_checksum);

        let large_file = tmp_dir.path().join("large.txt");
        let large_checksum = gen_rand_file(&large_file, DIGEST_BUFFER_SIZE * 4);
        assert_eq!(calc_crc32(&large_file).unwrap(), large_checksum);
    }

    #[test]
    fn test_create_delete_dir() {
        let tmp_dir = TempDir::new().unwrap();
        let subdir = tmp_dir.path().join("subdir");

        assert!(!delete_dir_if_exist(&subdir).unwrap());
        assert!(create_dir_if_not_exist(&subdir).unwrap());
        assert!(!create_dir_if_not_exist(&subdir).unwrap());
        assert!(delete_dir_if_exist(&subdir).unwrap());
    }

    #[test]
    fn test_sync_dir() {
        let tmp_dir = TempDir::new().unwrap();
        sync_dir(tmp_dir.path()).unwrap();
        let non_existent_file = tmp_dir.path().join("non_existent_file");
        sync_dir(non_existent_file).unwrap_err();
    }

    #[test]
    fn test_sha256() {
        let tmp_dir = TempDir::new().unwrap();
        let large_file = tmp_dir.path().join("large.txt");
        gen_rand_file(&large_file, DIGEST_BUFFER_SIZE * 4);

        let large_file_bytes = read(&large_file).unwrap();
        let direct_sha256 = sha256(&large_file_bytes).unwrap();

        let large_file_reader = File::open(&large_file).unwrap();
        let (mut sha256_reader, sha256_hasher) = Sha256Reader::new(large_file_reader).unwrap();
        let ret = sha256_reader.read_to_end(&mut Vec::new());

        assert_eq!(ret.unwrap(), DIGEST_BUFFER_SIZE * 4);
        assert_eq!(
            sha256_hasher.lock().unwrap().finish().unwrap().to_vec(),
            direct_sha256
        );
    }
}
