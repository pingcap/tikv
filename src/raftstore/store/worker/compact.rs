// Copyright 2016 PingCAP, Inc.
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

use util::worker::Runnable;
use util::rocksdb;
use util::escape;

use rocksdb::DB;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::fmt::{self, Formatter, Display};
use std::error;
use super::metrics::COMPACT_RANGE_CF;

pub struct Task {
    pub cf_name: String,
    pub start_key: Option<Vec<u8>>, // None means smallest key
    pub end_key: Option<Vec<u8>>, // None means largest key
}

pub struct TaskRes;

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "Compact CF[{}], range[{:?}, {:?}]",
               self.cf_name,
               self.start_key.as_ref().map(|k| escape(&k)),
               self.end_key.as_ref().map(|k| escape(&k)))
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("compact failed {:?}", err)
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
    ch: Option<Sender<TaskRes>>,
}

impl Runner {
    pub fn new(engine: Arc<DB>, ch: Option<Sender<TaskRes>>) -> Runner {
        Runner {
            engine: engine,
            ch: ch,
        }
    }

    fn compact_range_cf(&mut self,
                        cf_name: String,
                        start_key: Option<Vec<u8>>,
                        end_key: Option<Vec<u8>>)
                        -> Result<(), Error> {
        let cf_handle = box_try!(rocksdb::get_cf_handle(&self.engine, &cf_name));
        let compact_range_timer = COMPACT_RANGE_CF.with_label_values(&[&cf_name])
            .start_timer();
        self.engine.compact_range_cf(cf_handle,
                                     start_key.as_ref().map(Vec::as_slice),
                                     end_key.as_ref().map(Vec::as_slice));

        compact_range_timer.observe_duration();
        Ok(())
    }

    fn finish_task(&mut self) {
        if self.ch.is_none() {
            return;
        }
        // only used by test.
        self.ch.as_mut().unwrap().send(TaskRes).unwrap();
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        let cf = task.cf_name.clone();
        if let Err(e) = self.compact_range_cf(task.cf_name, task.start_key, task.end_key) {
            error!("execute compact range for cf {} failed, err {}", &cf, e);
        } else {
            info!("compact range for cf {} finished", &cf);
        }
        self.finish_task();
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc;
    use std::time::Duration;
    use util::rocksdb::new_engine;
    use tempdir::TempDir;
    use storage::CF_DEFAULT;
    use rocksdb::{WriteBatch, Writable};
    use super::*;

    const ROCKSDB_TOTAL_SST_FILES_SIZE: &'static str = "rocksdb.total-sst-files-size";

    #[test]
    fn test_compact_range() {
        let path = TempDir::new("compact-range-test").unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let db = Arc::new(db);

        let (tx, rx) = mpsc::channel();
        let mut runner = Runner::new(db.clone(), Some(tx));

        // generate first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put(k.as_bytes(), b"whatever content").unwrap();
        }
        db.write(wb).unwrap();
        db.flush(true).unwrap();

        // generate another sst file has the same content with first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put(k.as_bytes(), b"whatever content").unwrap();
        }
        db.write(wb).unwrap();
        db.flush(true).unwrap();

        // get total sst files size.
        let handle = rocksdb::get_cf_handle(&db, CF_DEFAULT).unwrap();
        let old_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE);

        // schedule compact range task
        runner.run(Task {
            cf_name: String::from(CF_DEFAULT),
            start_key: None,
            end_key: None,
        });
        rx.recv_timeout(Duration::from_secs(10)).unwrap();

        // get total sst files size after compact range.
        let new_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE);
        assert!(old_sst_files_size > new_sst_files_size);
    }
}
