// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::error;
use std::io;
use std::net;
use std::result;

use crossbeam::TrySendError;
use protobuf::{ProtobufError, RepeatedField};

use kvproto::{errorpb, metapb};
use raft::{self, Error as RaftError, StorageError};
use tikv_util::codec;

use super::coprocessor::Error as CopError;
use super::store::SnapError;
use tikv_util::escape;
use tikv_misc::store_util;
use tikv_misc::region_snapshot::Error as SnapError2;

pub const RAFTSTORE_IS_BUSY: &str = "raftstore is busy";

/// Describes why a message is discarded.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DiscardReason {
    /// Channel is disconnected, message can't be delivered.
    Disconnected,
    /// Message is dropped due to some filter rules, usually in tests.
    Filtered,
    /// Channel runs out of capacity, message can't be delivered.
    Full,
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RaftEntryTooLarge(region_id: u64, entry_size: u64) {
            description("raft entry is too large")
            display("raft entry is too large, region {}, entry size {}", region_id, entry_size)
        }
        StoreNotMatch(to_store_id: u64, my_store_id: u64) {
            description("store is not match")
            display("to store id {}, mine {}", to_store_id, my_store_id)
        }
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
        Other(err: Box<dyn error::Error + Sync + Send>) {
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
        Engine(err: engine::Error) {
            from()
            description("Engine error")
            display("Engine {:?}", err)
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
        EpochNotMatch(msg: String, new_regions: Vec<metapb::Region>) {
            description("region epoch is not match")
            display("EpochNotMatch {}", msg)
        }
        StaleCommand {
            description("stale command")
        }
        Coprocessor(err: CopError) {
            from()
            cause(err)
            description(err.description())
            display("Coprocessor {}", err)
        }
        Transport(reason: DiscardReason) {
            description("failed to send a message")
            display("Discard due to {:?}", reason)
        }
        Snapshot(err: SnapError) {
            from()
            cause(err)
            description(err.description())
            display("Snapshot {}", err)
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
            Error::RaftEntryTooLarge(region_id, entry_size) => {
                errorpb.mut_raft_entry_too_large().set_region_id(region_id);
                errorpb
                    .mut_raft_entry_too_large()
                    .set_entry_size(entry_size);
            }
            Error::StoreNotMatch(to_store_id, my_store_id) => {
                errorpb
                    .mut_store_not_match()
                    .set_request_store_id(to_store_id);
                errorpb
                    .mut_store_not_match()
                    .set_actual_store_id(my_store_id);
            }
            Error::KeyNotInRegion(key, region) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb
                    .mut_key_not_in_region()
                    .set_region_id(region.get_id());
                errorpb
                    .mut_key_not_in_region()
                    .set_start_key(region.get_start_key().to_vec());
                errorpb
                    .mut_key_not_in_region()
                    .set_end_key(region.get_end_key().to_vec());
            }
            Error::EpochNotMatch(_, new_regions) => {
                let mut e = errorpb::EpochNotMatch::new();
                e.set_current_regions(RepeatedField::from_vec(new_regions));
                errorpb.set_epoch_not_match(e);
            }
            Error::StaleCommand => {
                errorpb.set_stale_command(errorpb::StaleCommand::new());
            }
            Error::Transport(reason) if reason == DiscardReason::Full => {
                let mut server_is_busy_err = errorpb::ServerIsBusy::new();
                server_is_busy_err.set_reason(RAFTSTORE_IS_BUSY.to_owned());
                errorpb.set_server_is_busy(server_is_busy_err);
            }
            Error::Engine(engine::Error::NotInRange(key, region_id, start_key, end_key)) => {
                errorpb.mut_key_not_in_region().set_key(key);
                errorpb.mut_key_not_in_region().set_region_id(region_id);
                errorpb
                    .mut_key_not_in_region()
                    .set_start_key(start_key.to_vec());
                errorpb
                    .mut_key_not_in_region()
                    .set_end_key(end_key.to_vec());
            }
            _ => {}
        };

        errorpb
    }
}

impl From<SnapError2> for Error {
    fn from(e: SnapError2) -> Error {
        match e {
            SnapError2::Engine(e) => {
                Error::Engine(e)
            }
            SnapError2::KeyNotInRegion(key, region) => {
                Error::KeyNotInRegion(key, region).into()
            }
            SnapError2::EpochNotMatch(msg, new_regions) => {
                Error::EpochNotMatch(msg, new_regions).into()
            }
            SnapError2::StaleCommand => {
                Error::StaleCommand.into()
            }
            SnapError2::StoreNotMatch(to_store_id, my_store_id) => {
                Error::StoreNotMatch(to_store_id, my_store_id).into()
            }
            SnapError2::Other(e) => {
                Error::Other(e).into()
            }
        }
    }
}

impl<T> From<TrySendError<T>> for Error {
    #[inline]
    fn from(e: TrySendError<T>) -> Error {
        match e {
            TrySendError::Full(_) => Error::Transport(DiscardReason::Full),
            TrySendError::Disconnected(_) => Error::Transport(DiscardReason::Disconnected),
        }
    }
}

pub fn storage_error<E>(error: E) -> raft::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

impl From<store_util::Error> for Error {
    fn from(e: store_util::Error) -> Error {
        match e {
            store_util::Error::KeyNotInRegion(key, region) => {
                Error::KeyNotInRegion(key, region)
            }
            store_util::Error::EpochNotMatch(msg, new_regions) => {
                Error::EpochNotMatch(msg, new_regions)
            }
            store_util::Error::StaleCommand => {
                Error::StaleCommand
            }
            store_util::Error::StoreNotMatch(to_store_id, my_store_id) => {
                Error::StoreNotMatch(to_store_id, my_store_id)
            }
            store_util::Error::Other(e) => {
                Error::Other(e)
            }
        }
    }
}
