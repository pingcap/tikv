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

use super::{Coprocessor, RegionObserver, ObserverContext, Result as CopResult};
use util::codec::table;

use kvproto::raft_cmdpb::{SplitRequest, AdminRequest, Request, AdminResponse, Response,
                          AdminCmdType};
use protobuf::RepeatedField;
use std::result::Result as StdResult;

/// `SplitObserver` adjusts the split key so that it won't seperate
/// the data of a row into two region.
pub struct SplitObserver;

type Result<T> = StdResult<T, String>;

impl SplitObserver {
    fn on_split(&mut self, _: &mut ObserverContext, split: &mut SplitRequest) -> Result<()> {
        if !split.has_split_key() {
            return Err("split key is expected!".to_owned());
        }
        let mut key = split.get_split_key().to_vec();

        // format of a key is TABLE_PREFIX + table_id + TABLE_ROW_MARK + handle or
        // TABLE_PREFIX + table_id + TABLE_INDEX_MARK

        if !key.starts_with(table::TABLE_PREFIX) || key.len() < table::PREFIX_LEN + table::ID_LEN {
            // bypass non-data key.
            return Ok(());
        }
        let table_prefix_len = table::TABLE_PREFIX.len() + table::ID_LEN;
        if key[table_prefix_len..].starts_with(table::RECORD_PREFIX_SEP) {
            // truncate to handle
            key.truncate(table::PREFIX_LEN + table::ID_LEN);
        } else if key[table_prefix_len..].starts_with(table::INDEX_PREFIX_SEP) {
            // it's an index key, just remove the tailing version_id.
            let key_len = key.len();
            key.truncate(key_len - 8);
        } else {
            return Err(format!("key {:?} is not a valid key", key));
        }
        split.set_split_key(key);
        Ok(())
    }
}

impl Coprocessor for SplitObserver {
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

impl RegionObserver for SplitObserver {
    fn pre_admin(&mut self, ctx: &mut ObserverContext, req: &mut AdminRequest) -> CopResult<()> {
        if req.get_cmd_type() != AdminCmdType::Split {
            return Ok(());
        }
        if !req.has_split() {
            box_try!(Err("cmd_type is Split but it doesn't have split request, message maybe \
                          corrupted!"
                             .to_owned()));
        }
        if let Err(e) = self.on_split(ctx, req.mut_split()) {
            error!("failed to handle split req: {:?}", e);
            return Err(box_err!(e));
        }
        Ok(())
    }

    fn post_admin(&mut self, _: &mut ObserverContext, _: &AdminRequest, _: &mut AdminResponse) {}

    /// Hook to call before execute read/write request.
    fn pre_query(&mut self,
                 _: &mut ObserverContext,
                 _: &mut RepeatedField<Request>)
                 -> CopResult<()> {
        Ok(())
    }

    /// Hook to call after read/write request being executed.
    fn post_query(&mut self,
                  _: &mut ObserverContext,
                  _: &[Request],
                  _: &mut RepeatedField<Response>)
                  -> () {
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use tempdir::TempDir;
    use raftstore::store::engine::*;
    use raftstore::store::PeerStorage;
    use raftstore::coprocessor::ObserverContext;
    use raftstore::coprocessor::RegionObserver;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{SplitRequest, AdminRequest, AdminCmdType};
    use util::codec::{datum, table, Datum, number};
    use byteorder::{ByteOrder, BigEndian, WriteBytesExt};
    use std::io::Write;

    fn new_peer_storage(path: &TempDir) -> PeerStorage {
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        PeerStorage::new(Arc::new(engine), &Region::new()).unwrap()
    }

    fn new_split_request(key: &[u8]) -> AdminRequest {
        let mut req = AdminRequest::new();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::new();
        split_req.set_split_key(key.to_vec());
        req.set_split(split_req);
        req
    }

    fn new_row_key(table_id: i64, row_id: i64, column_id: u64, version_id: u64) -> Vec<u8> {
        let mut buf = vec![0; 8];
        number::encode_i64(&mut buf, row_id).unwrap();
        let mut key = table::encode_row_key(table_id, &buf);
        if column_id > 0 {
            key.write_u64::<BigEndian>(column_id).unwrap();
        }
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(table_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key = table::encode_index_seek_key(table_id,
                                                   idx_id,
                                                   &datum::encode_key(datums).unwrap());
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    #[test]
    fn test_split() {
        let path = TempDir::new("test-raftstore").unwrap();
        let storage = new_peer_storage(&path);
        let mut ctx = ObserverContext::new(&storage);
        let mut req = AdminRequest::new();

        let mut observer = SplitObserver;

        let resp = observer.pre_admin(&mut ctx, &mut req);
        // since no split is defined, actual coprocessor won't be invoke.
        assert!(resp.is_ok());
        assert!(!req.has_split(), "only split req should be handle.");

        req = new_split_request(b"test-1234567890abcdefg");
        let resp = observer.pre_admin(&mut ctx, &mut req);
        assert!(resp.is_err(), "invalid split should be prevented");

        let mut key = Vec::with_capacity(100);
        key.write(table::TABLE_PREFIX).unwrap();
        req = new_split_request(&key);
        assert!(observer.pre_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*key);

        key = new_row_key(1, 2, 0, 0);
        req = new_split_request(&key);
        let mut expect_key = key[..key.len() - 8].to_vec();
        assert!(observer.pre_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 0);
        req = new_split_request(&key);
        assert!(observer.pre_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_row_key(1, 2, 1, 1);
        req = new_split_request(&key);
        assert!(observer.pre_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        req = new_split_request(&key);
        expect_key = key[..key.len() - 8].to_vec();
        assert!(observer.pre_admin(&mut ctx, &mut req).is_ok());
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        key = new_index_key(1, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        req = new_split_request(&key);
        observer.pre_admin(&mut ctx, &mut req).unwrap();
        assert_eq!(req.get_split().get_split_key(), &*expect_key);

        let key = b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\
                    \x82\x7f\x80\x00\x00\x00\x00\x00\x00\xd3";
        let expect_key = b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f";
        req = new_split_request(key);
        observer.pre_admin(&mut ctx, &mut req).unwrap();
        assert_eq!(req.get_split().get_split_key(), expect_key);
    }
}
