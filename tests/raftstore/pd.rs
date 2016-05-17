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

#![allow(dead_code)]

use std::collections::{HashMap, BTreeMap, HashSet};
use std::vec::Vec;
use std::collections::Bound::{Excluded, Unbounded};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use kvproto::metapb;
use kvproto::pdpb;
use kvproto::raftpb;
use tikv::pd::{PdClient, Result, Error, Key};
use tikv::raftstore::store::util::{find_peer, remove_peer};
use tikv::raftstore::store::keys::{enc_end_key, enc_start_key, data_key};
use tikv::util::HandyRwLock;
use super::util::new_peer;

#[derive(Default)]
struct Store {
    store: metapb::Store,
    region_ids: HashSet<u64>,
}

struct Cluster {
    meta: metapb::Cluster,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
    region_id_keys: HashMap<u64, Key>,
    base_id: AtomicUsize,
}

impl Cluster {
    fn new(cluster_id: u64) -> Cluster {
        let mut meta = metapb::Cluster::new();
        meta.set_id(cluster_id);
        meta.set_max_peer_number(5);

        Cluster {
            meta: meta,
            stores: HashMap::new(),
            regions: BTreeMap::new(),
            region_id_keys: HashMap::new(),
            base_id: AtomicUsize::new(1000),
        }
    }

    fn bootstrap(&mut self, store: metapb::Store, region: metapb::Region) {
        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(region.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = Store {
            store: store,
            region_ids: HashSet::new(),
        };


        s.region_ids.insert(region.get_id());

        self.stores.insert(store_id, s);

        self.region_id_keys.insert(region.get_id(), enc_end_key(&region));
        self.regions.insert(enc_end_key(&region), region);
    }

    // We don't care cluster id here, so any value like 0 in tests is ok.
    fn alloc_id(&self) -> Result<u64> {
        Ok(self.base_id.fetch_add(1, Ordering::Relaxed) as u64)
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<()> {
        let mut s = self.stores.entry(store.get_id()).or_insert_with(Store::default);
        s.store = store;
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        Ok(self.stores.get(&store_id).unwrap().store.clone())
    }

    fn get_region(&self, key: Vec<u8>) -> Result<metapb::Region> {
        let (_, region) = self.regions
            .range::<Key, Key>(Excluded(&key), Unbounded)
            .next()
            .unwrap();
        Ok(region.clone())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<metapb::Region> {
        let key = self.region_id_keys.get(&region_id).unwrap();
        Ok(self.regions.get(key).cloned().unwrap())
    }

    fn split_region(&mut self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        let left_end_key = enc_end_key(&left);
        let right_end_key = enc_end_key(&right);

        assert!(left.get_start_key() < left.get_end_key());
        assert_eq!(left.get_end_key(), right.get_start_key());
        assert!(right.get_end_key().is_empty() || right.get_start_key() < right.get_end_key());

        // origin pre-split region's end key is the same as right end key,
        // and must exists.
        if !self.regions.contains_key(&right_end_key) {
            return Err(box_err!("region {:?} doesn't exist", right));
        }

        if self.regions.contains_key(&left_end_key) {
            return Err(box_err!("region {:?} has already existed", left));
        }

        assert!(self.region_id_keys.insert(left.get_id(), left_end_key.clone()).is_some());
        assert!(self.region_id_keys.insert(right.get_id(), right_end_key.clone()).is_none());

        for peer in right.get_peers() {
            let store = self.stores.get_mut(&peer.get_store_id()).unwrap();
            store.region_ids.insert(right.get_id());
        }

        assert!(self.regions.insert(left_end_key, left).is_none());
        assert!(self.regions.insert(right_end_key, right).is_some());
        debug!("cluster.regions: {:?}", self.regions);

        Ok(())
    }

    fn get_stores(&self) -> Vec<metapb::Store> {
        self.stores.values().map(|s| s.store.clone()).collect()
    }

    fn handle_heartbeat_version(&mut self, region: metapb::Region) -> Result<()> {
        // For split, we should handle heartbeat carefully.
        // E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
        // after split, region 1 and 2 will do heartbeat independently.
        let end_key = enc_end_key(&region);
        let start_key = enc_start_key(&region);
        assert!(end_key > start_key);

        let version = region.get_region_epoch().get_version();
        let conf_ver = region.get_region_epoch().get_conf_ver();

        let search_key = data_key(region.get_start_key());
        let search_region = self.get_region(search_key).unwrap();
        let search_end_key = enc_end_key(&search_region);
        let search_start_key = enc_start_key(&search_region);
        let search_version = search_region.get_region_epoch().get_version();
        let search_conf_ver = search_region.get_region_epoch().get_conf_ver();

        if start_key == search_start_key && end_key == search_end_key {
            // we are the same, must check epoch here.
            try!(check_stale_region(&search_region, &region));
        } else if search_start_key > end_key {
            assert!(!region.get_end_key().is_empty());
            // No range [start, end) in region now, insert directly.
            assert!(self.regions.insert(end_key.clone(), region.clone()).is_none());
            assert!(self.region_id_keys.insert(region.get_id(), end_key.clone()).is_none());
        } else if search_start_key < end_key {
            // overlap, remove old, insert new.
            // E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
            // is overlapped with origin [a, c).
            assert!(version > search_version);
            assert!(conf_ver >= search_conf_ver);

            assert!(self.regions.remove(&search_end_key).is_some());
            assert!(self.region_id_keys.remove(&search_region.get_id()).is_some());
            assert!(self.regions.insert(end_key.clone(), region.clone()).is_none());
            assert!(self.region_id_keys.insert(region.get_id(), end_key.clone()).is_none());
        } else {
            panic!("invalid region {:?} compared searched {:?}",
                   region,
                   search_region);
        }
        Ok(())
    }

    fn handle_heartbeat_conf_ver(&mut self,
                                 mut region: metapb::Region,
                                 leader: metapb::Peer)
                                 -> Result<pdpb::RegionHeartbeatResponse> {
        let conf_ver = region.get_region_epoch().get_conf_ver();
        let end_key = enc_end_key(&region);

        let cur_region = self.get_region_by_id(region.get_id()).unwrap();

        let cur_conf_ver = cur_region.get_region_epoch().get_conf_ver();
        try!(check_stale_region(&cur_region, &region));

        let region_peer_len = region.get_peers().len();
        let cur_region_peer_len = cur_region.get_peers().len();

        let mut resp = pdpb::RegionHeartbeatResponse::new();
        let mut change_peer = pdpb::ChangePeer::new();

        if conf_ver > cur_conf_ver || cur_region_peer_len == region_peer_len {
            // We do the ConfChange already, so must check already entered the final state.
            // Must have same peers.
            must_same_peers(&region, &cur_region);

            let max_peer_number = self.meta.get_max_peer_number() as usize;
            let peer_number = region.get_peers().len();

            if peer_number < max_peer_number {
                // find the first store which the region has not covered.
                for store_id in self.stores.keys() {
                    if region.get_peers().iter().all(|x| x.get_store_id() != *store_id) {
                        let peer = new_peer(*store_id, self.alloc_id().unwrap());
                        change_peer.set_change_type(raftpb::ConfChangeType::AddNode);
                        change_peer.set_peer(peer.clone());
                        resp.set_change_peer(change_peer);
                        region.mut_peers().push(peer);
                        break;
                    }
                }
            } else if peer_number > max_peer_number {
                // find the first peer which not leader.
                let pos = region.get_peers()
                    .iter()
                    .position(|x| x.get_store_id() != leader.get_store_id())
                    .unwrap();

                let store_id = region.get_peers()[pos].get_store_id();
                change_peer.set_change_type(raftpb::ConfChangeType::RemoveNode);
                change_peer.set_peer(region.get_peers()[pos].clone());
                resp.set_change_peer(change_peer);

                assert!(remove_peer(&mut region, store_id).is_some());
            }

        } else if conf_ver == cur_conf_ver {
            // The region is not the final state in pd, we should step to it first.
            // We must change state one by one, so we can only add or remove one peer
            // at same time, and must guarantee to step to next change only after the
            // TiKV region steps to the state first.
            // E.g, peers (1), -> peers (1, 2), TiKV must enters (1, 2) first, then
            // we can step to (1, 2, 3) maybe. So for same ConfVer, we can't meet:
            // 1) pd is (1, 2, 3) but TiKV is (1)
            // 2) pd is (1) but TiKV is (1, 2, 3)
            // 3) pd is (1, 2) but TiKV is (1, 3)
            if cur_region_peer_len > region_peer_len {
                // must pd is (1, 2) but TiKV is (1)
                assert_eq!(cur_region_peer_len - region_peer_len, 1);
                let peers = different_peers(&cur_region, &region);
                assert_eq!(peers.len(), 1);
                assert!(different_peers(&region, &cur_region).is_empty());

                change_peer.set_change_type(raftpb::ConfChangeType::AddNode);
                change_peer.set_peer(peers[0].clone());
            } else {
                // must pd is (1) but TiKV is (1, 2)
                assert_eq!(region_peer_len - cur_region_peer_len, 1);
                let peers = different_peers(&region, &cur_region);
                assert_eq!(peers.len(), 1);
                assert!(different_peers(&cur_region, &region).is_empty());

                change_peer.set_change_type(raftpb::ConfChangeType::RemoveNode);
                change_peer.set_peer(peers[0].clone());
            }

            resp.set_change_peer(change_peer);
        }

        // update the region again.
        assert!(self.regions.insert(end_key, region).is_some());

        Ok(resp)

    }

    fn region_heartbeat(&mut self,
                        region: metapb::Region,
                        leader: metapb::Peer)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        try!(self.handle_heartbeat_version(region.clone()));
        self.handle_heartbeat_conf_ver(region, leader)
    }
}

fn check_stale_region(region: &metapb::Region, check_region: &metapb::Region) -> Result<()> {
    let epoch = region.get_region_epoch();
    let check_epoch = check_region.get_region_epoch();
    if epoch.get_conf_ver() >= check_epoch.get_conf_ver() &&
       epoch.get_version() >= check_epoch.get_version() {
        return Ok(());
    }

    Err(box_err!("stale epoch {:?}, we are now {:?}", check_epoch, epoch))
}

fn must_same_peers(left: &metapb::Region, right: &metapb::Region) {
    assert_eq!(left.get_peers().len(), right.get_peers().len());
    for peer in left.get_peers() {
        let p = find_peer(&right, peer.get_store_id()).unwrap();
        assert_eq!(p.get_id(), peer.get_id());
    }
}

// Left - Right, left (1, 2, 3), right (1, 2), left - right = (3)
fn different_peers(left: &metapb::Region, right: &metapb::Region) -> Vec<metapb::Peer> {
    let mut peers = vec![];
    for peer in left.get_peers() {
        if let Some(p) = find_peer(&right, peer.get_store_id()) {
            assert_eq!(p.get_id(), peer.get_id());
            continue;
        }

        peers.push(peer.clone())
    }

    peers
}

pub struct TestPdClient {
    cluster_id: u64,
    cluster: RwLock<Cluster>,
}

impl TestPdClient {
    pub fn new(cluster_id: u64) -> TestPdClient {
        TestPdClient {
            cluster_id: cluster_id,
            cluster: RwLock::new(Cluster::new(cluster_id)),
        }
    }

    pub fn get_stores(&self) -> Result<Vec<metapb::Store>> {
        Ok(self.cluster.rl().get_stores())
    }

    pub fn get_region_by_id(&self, region_id: u64) -> Result<metapb::Region> {
        self.cluster.rl().get_region_by_id(region_id)
    }

    fn check_bootstrap(&self) -> Result<()> {
        if !self.is_cluster_bootstrapped().unwrap() {
            return Err(Error::ClusterNotBootstrapped(self.cluster_id));
        }

        Ok(())
    }
}

impl PdClient for TestPdClient {
    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        if self.is_cluster_bootstrapped().unwrap() {
            return Err(Error::ClusterBootstrapped(self.cluster_id));
        }

        self.cluster.wl().bootstrap(store, region);

        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Ok(!self.cluster.rl().stores.is_empty())
    }

    fn alloc_id(&self) -> Result<u64> {
        self.cluster.rl().alloc_id()
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        try!(self.check_bootstrap());
        self.cluster.wl().put_store(store)
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        try!(self.check_bootstrap());
        self.cluster.rl().get_store(store_id)
    }


    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        try!(self.check_bootstrap());
        self.cluster.rl().get_region(data_key(key))
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        try!(self.check_bootstrap());
        Ok(self.cluster.rl().meta.clone())
    }


    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        try!(self.check_bootstrap());
        self.cluster.wl().region_heartbeat(region, leader)
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        try!(self.check_bootstrap());

        // Must ConfVer and Version be same?
        let cur_region = self.cluster.rl().get_region_by_id(region.get_id()).unwrap();
        try!(check_stale_region(&cur_region, &region));

        let mut resp = pdpb::AskSplitResponse::new();
        resp.set_new_region_id(self.alloc_id().unwrap());
        let mut peer_ids = vec![];
        for _ in region.get_peers() {
            peer_ids.push(self.alloc_id().unwrap());
        }
        resp.set_new_peer_ids(peer_ids);

        Ok(resp)
    }
}
