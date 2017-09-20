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

use std::{error, result};
use kvproto::debugpb::*;

use raftstore::store::Engines;
use raftstore::store::engine::Peekable;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};

pub type Result<T> = result::Result<T, Error>;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        InvalidArgument(msg: String) {
            description(msg)
            display("Invalid Argument {:?}", msg)
        }
        NotFound(msg: String) {
            description(msg)
            display("Not Found {:?}", msg)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

#[derive(Clone)]
pub struct Debugger {
    engines: Engines,
}

impl Debugger {
    pub fn new(engines: Engines) -> Debugger {
        Debugger { engines }
    }

    pub fn get(&self, db: DB, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        try!(validate_db_and_cf(db, cf));
        let db = match db {
            DB::KV => &self.engines.kv_engine,
            DB::RAFT => &self.engines.raft_engine,
            _ => unreachable!(),
        };
        match db.get_value_cf(cf, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(
                format!("get none value for key {:?}", key,),
            )),
            Err(e) => Err(box_err!(e)),
        }
    }
}

pub fn validate_db_and_cf(db: DB, cf: &str) -> Result<()> {
    match (db, cf) {
        (DB::KV, CF_DEFAULT) |
        (DB::KV, CF_WRITE) |
        (DB::KV, CF_LOCK) |
        (DB::KV, CF_RAFT) |
        (DB::RAFT, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(
            format!("invalid cf {:?} for db {:?}", db, cf),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use kvproto::debugpb::*;
    use tempdir::TempDir;

    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use super::*;

    #[test]
    fn test_validate_db_and_cf() {
        let valid_cases = vec![
            (DB::KV, CF_DEFAULT),
            (DB::KV, CF_WRITE),
            (DB::KV, CF_LOCK),
            (DB::KV, CF_RAFT),
            (DB::RAFT, CF_DEFAULT),
        ];
        for (db, cf) in valid_cases {
            validate_db_and_cf(db, cf).unwrap();
        }

        let invalid_cases = vec![
            (DB::RAFT, CF_WRITE),
            (DB::RAFT, CF_LOCK),
            (DB::RAFT, CF_RAFT),
            (DB::INVALID, CF_DEFAULT),
            (DB::INVALID, "BAD_CF"),
        ];
        for (db, cf) in invalid_cases {
            validate_db_and_cf(db, cf).unwrap_err();
        }
    }

    fn new_debugger() -> Debugger {
        let tmp = TempDir::new("test_debug").unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = Arc::new(
            rocksdb_util::new_engine_opt(
                path,
                DBOptions::new(),
                vec![
                    CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
                ],
            ).unwrap(),
        );

        let engines = Engines::new(engine.clone(), engine);
        Debugger::new(engines)
    }

    #[test]
    fn test_get() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv_engine;
        let (k, v) = (b"k", b"v");
        engine.put(k, v).unwrap();
        assert_eq!(&*engine.get(k).unwrap().unwrap(), v);

        assert_eq!(debugger.get(DB::KV, CF_DEFAULT, k).unwrap().as_slice(), v);
        match debugger.get(DB::KV, CF_DEFAULT, b"foo") {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }
}
