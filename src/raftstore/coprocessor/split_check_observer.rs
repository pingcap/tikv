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

use rocksdb::{SeekKey, DB};
use kvproto::metapb::Region;

use coprocessor::codec::table as table_codec;
use storage::types::Key;
use storage::{CfName, CF_WRITE};
use util::transport::{RetryableSendCh, Sender};

use super::super::store::{keys, util, Msg};
use super::super::store::engine::IterOption;

use super::Result;
use super::metrics::*;

/// An interface for writing split checker extensions.
pub trait Observer: Send {
    // The Observer's name.
    fn name(&self) -> &str;
    /// Do some quick checks, true for skipping `check_key_value_len`.
    fn before_check(&mut self, engine: &DB, region: &Region) -> bool;
    /// Feed keys and value sizes in order to find the split key.
    fn check_key_value_len(&mut self, key: &[u8], value_len: u64) -> Option<Vec<u8>>;
}

pub use self::size::SizeCheckObserver;

mod size {
    use super::*;

    pub struct SizeCheckObserver<C> {
        ch: RetryableSendCh<Msg, C>,
        region_max_size: u64,
        split_size: u64,

        split_key: Option<Vec<u8>>,
        current_size: u64,
    }

    impl<C: Sender<Msg>> SizeCheckObserver<C> {
        pub fn new(
            ch: RetryableSendCh<Msg, C>,
            region_max_size: u64,
            split_size: u64,
        ) -> SizeCheckObserver<C> {
            SizeCheckObserver {
                ch: ch,
                region_max_size: region_max_size,
                split_size: split_size,
                split_key: None,
                current_size: 0,
            }
        }

        fn check_size(&self, engine: &DB, region: &Region) -> Option<u64> {
            let region_id = region.get_id();
            let region_size = match util::get_region_approximate_size(engine, region) {
                Ok(size) => size,
                Err(e) => {
                    error!(
                        "[region {}] failed to get approximate size: {}",
                        region_id,
                        e
                    );
                    return None;
                }
            };

            let res = Msg::ApproximateRegionSize {
                region_id: region_id,
                region_size: region_size,
            };
            if let Err(e) = self.ch.try_send(res) {
                error!(
                    "[region {}] failed to send approximate region size: {}",
                    region_id,
                    e
                );
            }

            REGION_SIZE_HISTOGRAM.observe(region_size as f64);
            Some(region_size)
        }
    }

    impl<C: Sender<Msg> + Send> Observer for SizeCheckObserver<C> {
        fn name(&self) -> &str {
            "SizeCheckObserver"
        }

        fn before_check(&mut self, engine: &DB, region: &Region) -> bool {
            self.split_key = None;
            self.current_size = 0;

            if let Some(region_size) = self.check_size(engine, region) {
                if region_size < self.region_max_size {
                    true
                } else {
                    info!(
                        "[region {}] approximate size {} >= {}, need to do split check",
                        region.get_id(),
                        region_size,
                        self.region_max_size
                    );
                    false
                }
            } else {
                false
            }
        }

        fn check_key_value_len(&mut self, key: &[u8], value_len: u64) -> Option<Vec<u8>> {
            self.current_size += key.len() as u64 + value_len;
            if self.split_key.is_none() && self.current_size > self.split_size {
                self.split_key = Some(key.to_vec());
            }
            if self.split_key.is_some() && self.current_size >= self.region_max_size {
                return self.split_key.take();
            }
            None
        }
    }
}

pub use self::table::TableCheckObserver;

mod table {
    use raftstore::store::engine::Iterable;

    use super::*;

    pub struct TableCheckObserver {
        prev_table_id: i64,
        last_table_prefix: Option<Vec<u8>>,
    }

    impl TableCheckObserver {
        pub fn new() -> TableCheckObserver {
            TableCheckObserver {
                prev_table_id: 0,
                last_table_prefix: None,
            }
        }
    }

    impl Observer for TableCheckObserver {
        fn name(&self) -> &str {
            "TableCheckObserver"
        }

        fn before_check(&mut self, engine: &DB, region: &Region) -> bool {
            self.prev_table_id = 0;
            self.last_table_prefix = None;

            let (actual_start_key, actual_end_key) =
                match bound_keys(engine, &[CF_WRITE], region) {
                    Ok(Some((actual_start_key, actual_end_key))) => {
                        (actual_start_key, actual_end_key)
                    }
                    Ok(None) => return true,
                    Err(err) => {
                        error!(
                            "[region {}] failed to get region bound: {}",
                            region.get_id(),
                            err
                        );
                        return true;
                    }
                };

            if !keys::validate_data_key(&actual_start_key) ||
                !keys::validate_data_key(&actual_end_key)
            {
                return true;
            }

            let raw_start_key =
                match Key::from_encoded(keys::origin_key(&actual_start_key).to_vec()).raw() {
                    Ok(k) => k,
                    Err(_) => return true,
                };

            let raw_end_key =
                match Key::from_encoded(keys::origin_key(&actual_end_key).to_vec()).raw() {
                    Ok(k) => k,
                    Err(_) => return true,
                };

            let start_table_id = table_codec::decode_table_id(&raw_start_key).unwrap_or(0);
            let end_table_id = table_codec::decode_table_id(&raw_end_key).unwrap_or(0);

            use std::cmp::{Ord, Ordering};
            match (
                raw_start_key[0].cmp(&table_codec::TABLE_PREFIX[0]),
                raw_end_key[0].cmp(&table_codec::TABLE_PREFIX[0]),
            ) {
                (Ordering::Less, Ordering::Less) | (Ordering::Greater, Ordering::Greater) => true,
                (Ordering::Less, Ordering::Equal) => {
                    if end_table_id != 0 {
                        self.last_table_prefix = Some(table_codec::gen_table_prefix(end_table_id));
                    }
                    false
                }
                (Ordering::Less, Ordering::Greater) => false,
                (Ordering::Equal, Ordering::Equal) => {
                    if start_table_id == end_table_id {
                        // Same table.
                        // TODO: what if start_table_id equals to 0.
                        true
                    } else {
                        self.last_table_prefix = Some(table_codec::gen_table_prefix(end_table_id));
                        false
                    }
                }
                (Ordering::Equal, Ordering::Greater) => {
                    self.prev_table_id = start_table_id;
                    false
                }
                _ => panic!(
                    "start_key {:?} and end_key {:?} out of order",
                    actual_end_key,
                    actual_end_key
                ),
            }
        }

        fn check_key_value_len(&mut self, key: &[u8], _: u64) -> Option<Vec<u8>> {
            if let Some(last_table_prefix) = self.last_table_prefix.take() {
                Some(keys::data_key(Key::from_raw(&last_table_prefix).encoded()))
            } else {
                cross_table(self.prev_table_id, key)
            }
        }
    }

    /// If `current_key` is not in the table `table_id`,
    /// it returns the `current_key`'s table prefix.
    fn cross_table(table_id: i64, current_key: &[u8]) -> Option<Vec<u8>> {
        if !keys::validate_data_key(current_key) {
            return None;
        }
        let origin_current_key = keys::origin_key(current_key);
        let raw_current_key = match Key::from_encoded(origin_current_key.to_vec()).raw() {
            Ok(k) => k,
            Err(_) => return None,
        };

        let current_table_id = match table_codec::decode_table_id(&raw_current_key) {
            Ok(id) => id,
            _ => return None,
        };

        if table_id != current_table_id {
            Some(table_codec::gen_table_prefix(current_table_id))
        } else {
            None
        }
    }

    #[allow(collapsible_if)]
    fn bound_keys(db: &DB, cfs: &[CfName], region: &Region) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        let mut first_key = None;
        let mut last_key = None;

        for cf in cfs {
            let iter_opt = IterOption::new(Some(end_key.clone()), false);
            let mut iter = box_try!(db.new_iterator_cf(cf, iter_opt));

            // the first key
            if iter.seek(start_key.as_slice().into()) {
                let key = iter.key().to_vec();
                if first_key.is_some() {
                    if &key < first_key.as_ref().unwrap() {
                        first_key = Some(key);
                    }
                } else {
                    first_key = Some(key);
                }
            } // else { No data in this CF }

            // the last key
            if iter.seek(SeekKey::End) {
                let key = iter.key().to_vec();
                if last_key.is_some() {
                    if &key < last_key.as_ref().unwrap() {
                        last_key = Some(key);
                    }
                } else {
                    last_key = Some(key);
                }
            } // else { No data in this CF }
        }

        match (first_key, last_key) {
            (Some(fk), Some(lk)) => Ok(Some((fk, lk))),
            (None, None) => Ok(None),
            (first_key, last_key) => Err(box_err!(
                "invalid bound, first key: {:?}, last key: {:?}",
                first_key,
                last_key
            )),
        }
    }

    #[cfg(test)]
    mod test {
        use std::sync::Arc;

        use tempdir::TempDir;
        use rocksdb::Writable;
        use kvproto::metapb::Peer;

        use storage::ALL_CFS;
        use storage::types::Key;
        use util::rocksdb::new_engine;

        use super::*;

        #[test]
        fn test_cross_table() {
            let t2 = keys::data_key(Key::from_raw(&table_codec::gen_table_prefix(2)).encoded());

            assert_eq!(cross_table(1, &t2).unwrap(), t2);
            assert_eq!(cross_table(2, &t2), None);
            assert_eq!(cross_table(2, b"bar"), None);
        }

        #[test]
        fn test_bound_keys() {
            let path = TempDir::new("test-split-table").unwrap();
            let engine = Arc::new(new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());

            let mut region = Region::new();
            region.set_id(1);
            region.mut_peers().push(Peer::new());
            region.mut_region_epoch().set_version(2);
            region.mut_region_epoch().set_conf_ver(5);

            assert_eq!(bound_keys(&engine, LARGE_CFS, &region).unwrap(), None);

            // arbitrary padding.
            let padding = b"_r00000005";
            // Put t1_xxx
            let mut key = table_codec::gen_table_prefix(1);
            key.extend_from_slice(padding);
            let s1 = keys::data_key(Key::from_raw(&key).encoded());
            engine.put(&s1, &s1).unwrap();

            // ["", "") => {t1_xx, t1_xx}
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s1.clone()))
            );

            // ["", "t1") => None
            region.set_start_key(vec![]);
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(bound_keys(&engine, LARGE_CFS, &region).unwrap(), None);

            // ["t1", "") => {t1_xx, t1_xx}
            region.set_start_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            region.set_end_key(vec![]);
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s1.clone()))
            );

            // ["t1", "t2") => {t1_xx, t1_xx}
            region.set_start_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(2))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s1.clone()))
            );

            // Put t2_xx
            let mut key = table_codec::gen_table_prefix(2);
            key.extend_from_slice(padding);
            let s2 = keys::data_key(Key::from_raw(&key).encoded());
            engine.put(&s2, &s2).unwrap();

            // ["t1", "") => {t1_xx, t2_xx}
            region.set_start_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            region.set_end_key(vec![]);
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s2.clone()))
            );

            // ["", "t2") => {t1_xx, t1_xx}
            region.set_start_key(vec![]);
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(2))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s1.clone()))
            );

            // ["t1", "t2") => {t1_xx, t1_xx}
            region.set_start_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(2))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s1.clone()))
            );

            // Put t3_xx
            let mut key = table_codec::gen_table_prefix(3);
            key.extend_from_slice(padding);
            let s3 = keys::data_key(Key::from_raw(&key).encoded());
            engine.put(&s3, &s3).unwrap();

            // ["", "t3") => {t1_xx, t2_xx}
            region.set_start_key(vec![]);
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(3))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s2.clone()))
            );

            // ["t1", "t3") => {t1_xx, t2_xx}
            region.set_start_key(
                Key::from_raw(&table_codec::gen_table_prefix(1))
                    .encoded()
                    .to_vec(),
            );
            region.set_end_key(
                Key::from_raw(&table_codec::gen_table_prefix(3))
                    .encoded()
                    .to_vec(),
            );
            assert_eq!(
                bound_keys(&engine, LARGE_CFS, &region).unwrap(),
                Some((s1.clone(), s2.clone()))
            );
        }
    }
}
