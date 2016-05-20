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

use std::error;
use std::boxed::Box;
use std::result;
use std::io;
use std::net;
use std::vec::Vec;

use protobuf::ProtobufError;

use util::codec;
use pd;
use raft;
use kvproto::{errorpb, metapb};

use super::coprocessor::Error as CopError;
use util::escape;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        RegionNotFound(region_id: u64) {
            description("region is not found")
            display("region {} not found", region_id)
        }
        RegionNotInitialized(region_id: u64) {
            description("region has not been initialized yet.")
            display("region {} not initialized yet", region_id)
        }
        NotLeader(region_id: u64, leader: Option<metapb::Peer>) {
            description("peer is not leader")
            display("peer is not leader for region {}, leader may {:?}", region_id, leader)
        }
        KeyNotInRegion(key: Vec<u8>, region: metapb::Region) {
            description("key is not in region")
            display("key {} is not in region key range [{}, {}) for region {}",
                    escape(key),
                    escape(region.get_start_key()),
                    escape(region.get_end_key()),
                    region.get_id())
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }

        // Following is for From other errors.
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
            display("Io {}", err)
        }
        // RocksDb uses plain string as the error.
        // Maybe other libs use this too?
        RocksDb(msg: String) {
            from()
            description("RocksDb error")
            display("RocksDb {}", msg)
        }
        Protobuf(err: ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf {}", err)
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
            display("Codec {}", err)
        }
        AddrParse(err: net::AddrParseError) {
            from()
            cause(err)
            description(err.description())
            display("AddrParse {}", err)
        }
        Pd(err: pd::Error) {
            from()
            cause(err)
            description(err.description())
            display("Pd {}", err)
        }
        Raft(err: raft::Error) {
            from()
            cause(err)
            description(err.description())
            display("Raft {}", err)
        }
        Timeout(msg: String) {
            description("request timeout")
            display("Timeout {}", msg)
        }
        StaleEpoch(msg: String) {
            description("region is stale")
            display("StaleEpoch {}", msg)
        }
        Coprocessor(err: CopError) {
            from()
            cause(err)
            description(err.description())
            display("Coprocessor {}", err)
        }
    }
}


pub type Result<T> = result::Result<T, Error>;

impl Into<errorpb::Error> for Error {
    fn into(self) -> errorpb::Error {
        let mut errorpb = errorpb::Error::new();
        errorpb.set_message(error::Error::description(&self).to_owned());

        match self {
            Error::RegionNotFound(region_id) => {
                errorpb.mut_region_not_found().set_region_id(region_id);
            }
            Error::NotLeader(region_id, leader) => {
                if let Some(leader) = leader {
                    errorpb.mut_not_leader().set_leader(leader);
                }
                errorpb.mut_not_leader().set_region_id(region_id);
            }
            Error::KeyNotInRegion(key, region) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb.mut_key_not_in_region().set_region_id(region.get_id());
                errorpb.mut_key_not_in_region().set_start_key(region.get_start_key().to_vec());
                errorpb.mut_key_not_in_region().set_end_key(region.get_end_key().to_vec());
            }
            Error::StaleEpoch(_) => {
                errorpb.set_stale_epoch(errorpb::StaleEpoch::new());
            }
            _ => {}
        };

        errorpb
    }
}
