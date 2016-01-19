#![crate_type = "lib"]
#![feature(test)]
#![feature(vec_push_all)]
#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate log;
extern crate test;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;
extern crate mio;

pub mod util;
pub mod raft;
pub mod proto;
pub mod storage;
pub mod raftserver;
