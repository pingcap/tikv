// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crc::crc32::{self, Hasher32};
use uuid::Uuid;

use kvproto::importpb::*;

use super::{Error, Result};

pub type Token = usize;

pub struct SSTImporter {
    dir: ImportDir,
    token: AtomicUsize,
    files: Mutex<HashMap<Token, ImportFile>>,
}

impl SSTImporter {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<SSTImporter> {
        let dir = ImportDir::new(root)?;
        Ok(SSTImporter {
            dir: dir,
            token: AtomicUsize::new(1),
            files: Mutex::new(HashMap::new()),
        })
    }

    pub fn token(&self) -> Token {
        self.token.fetch_add(1, Ordering::SeqCst)
    }

    pub fn create(&self, token: Token, meta: &SSTMeta) -> Result<()> {
        match self.dir.create(meta) {
            Ok(f) => {
                info!("create {}", f);
                self.insert(token, f);
                Ok(())
            }
            Err(e) => {
                error!("create {:?}: {:?}", meta, e);
                Err(e)
            }
        }
    }

    pub fn append(&self, token: Token, data: &[u8]) -> Result<()> {
        match self.remove(token) {
            Some(mut f) => match f.append(data) {
                Ok(_) => {
                    self.insert(token, f);
                    Ok(())
                }
                Err(e) => {
                    error!("append {}: {:?}", f, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    pub fn finish(&self, token: Token) -> Result<()> {
        match self.remove(token) {
            Some(mut f) => match f.finish() {
                Ok(_) => {
                    info!("finish {}", f);
                    Ok(())
                }
                Err(e) => {
                    error!("finish {}: {:?}", f, e);
                    Err(e)
                }
            },
            None => Err(Error::TokenNotFound(token)),
        }
    }

    fn insert(&self, token: Token, file: ImportFile) {
        let mut files = self.files.lock().unwrap();
        assert!(files.insert(token, file).is_none());
    }

    pub fn remove(&self, token: Token) -> Option<ImportFile> {
        let mut files = self.files.lock().unwrap();
        files.remove(&token)
    }

    pub fn locate(&self, meta: &SSTMeta) -> Result<PathBuf> {
        self.dir.locate(meta)
    }
}

pub struct ImportDir {
    root: Mutex<PathBuf>,
}

impl ImportDir {
    const TEMP_DIR: &'static str = ".temp";

    pub fn new<P: AsRef<Path>>(root: P) -> Result<ImportDir> {
        let root_dir = root.as_ref().to_owned();
        let temp_dir = root_dir.join(Self::TEMP_DIR);
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir)?;
        }
        fs::create_dir_all(&temp_dir)?;
        Ok(ImportDir {
            root: Mutex::new(root_dir),
        })
    }

    pub fn create(&self, meta: &SSTMeta) -> Result<ImportFile> {
        let root = self.root.lock().unwrap();
        let file_name = sst_meta_to_path(meta)?;
        let save_path = root.join(&file_name);
        let temp_path = root.join(Self::TEMP_DIR).join(&file_name);
        if save_path.exists() {
            return Err(Error::FileExists(save_path));
        }
        if temp_path.exists() {
            return Err(Error::FileExists(temp_path));
        }
        ImportFile::create(meta.clone(), save_path, temp_path)
    }

    pub fn locate(&self, meta: &SSTMeta) -> Result<PathBuf> {
        let root = self.root.lock().unwrap();
        let file_name = sst_meta_to_path(meta)?;
        let save_path = root.join(&file_name);
        if !save_path.exists() {
            return Err(Error::FileNotExists(save_path));
        }
        Ok(save_path)
    }
}

pub struct ImportFile {
    meta: SSTMeta,
    save_path: PathBuf,
    temp_path: PathBuf,
    temp_file: Option<File>,
    temp_digest: crc32::Digest,
}

impl ImportFile {
    fn create(meta: SSTMeta, save_path: PathBuf, temp_path: PathBuf) -> Result<ImportFile> {
        let temp_file = File::create(&temp_path)?;
        Ok(ImportFile {
            meta: meta,
            save_path: save_path,
            temp_path: temp_path,
            temp_file: Some(temp_file),
            temp_digest: crc32::Digest::new(crc32::IEEE),
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.temp_file.as_mut().unwrap().write_all(data)?;
        self.temp_digest.write(data);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.validate()?;
        self.temp_file.take();
        assert!(self.temp_path.exists());
        assert!(!self.save_path.exists());
        fs::rename(&self.temp_path, &self.save_path).map_err(Error::from)
    }

    fn remove(&mut self) -> Result<()> {
        self.temp_file.take();
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path).map_err(Error::from)
        } else {
            Ok(())
        }
    }

    fn validate(&self) -> Result<()> {
        let f = self.temp_file.as_ref().unwrap();
        if f.metadata()?.len() != self.meta.get_length() {
            return Err(Error::FileCorrupted(self.temp_path.clone()));
        }
        if self.temp_digest.sum32() != self.meta.get_crc32() {
            return Err(Error::FileCorrupted(self.temp_path.clone()));
        }
        Ok(())
    }
}

impl Drop for ImportFile {
    fn drop(&mut self) {
        if let Err(e) = self.remove() {
            warn!("remove {}: {:?}", self, e);
        }
    }
}

impl fmt::Display for ImportFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ImportFile {{meta: {{{:?}}}, save_path: {}, temp_path: {}}}",
            self.meta,
            self.save_path.to_str().unwrap(),
            self.temp_path.to_str().unwrap(),
        )
    }
}

fn sst_meta_to_path(meta: &SSTMeta) -> Result<PathBuf> {
    let uuid = Uuid::from_bytes(meta.get_uuid())?;
    Ok(PathBuf::from(format!(
        "{}_{}_{}_{}.sst",
        uuid.simple().to_string(),
        meta.get_region_id(),
        meta.get_region_epoch().get_conf_ver(),
        meta.get_region_epoch().get_version(),
    )))
}
