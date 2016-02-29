#![allow(dead_code)]

use std::collections::{HashMap, BTreeMap, HashSet};
use std::vec::Vec;
use std::collections::Bound::{Included, Unbounded};

use tikv::proto::metapb;
use tikv::pd::{Client, Result, Error, Key};

struct Store {
    store: metapb::Store,
    regions: HashSet<Key>,
}

struct Node {
    node: metapb::Node,
    stores: HashSet<u64>,
}

impl Node {
    pub fn new(node: metapb::Node, stores: &[metapb::Store]) -> Node {
        let mut ids = HashSet::new();
        for v in stores {
            let id = v.get_store_id();
            ids.insert(id);
        }

        Node {
            node: node,
            stores: ids,
        }
    }
}

struct Cluster {
    cluster_id: u64,
    nodes: HashMap<u64, Node>,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
}

impl Cluster {
    pub fn new(cluster_id: u64,
               node: metapb::Node,
               stores: Vec<metapb::Store>,
               region: metapb::Region)
               -> Cluster {
        let mut c = Cluster {
            cluster_id: cluster_id,
            nodes: HashMap::new(),
            stores: HashMap::new(),
            regions: BTreeMap::new(),
        };

        let node_id = node.get_node_id();
        c.nodes.insert(node_id, Node::new(node, &stores));

        assert_eq!(region.get_peers().len(), 1);
        let end_key = region.get_end_key().to_vec();
        let first_store_id = region.get_peers()[0].get_store_id();

        for v in stores {
            let store_id = v.get_store_id();
            let mut store = Store {
                store: v,
                regions: HashSet::new(),
            };

            if store_id == first_store_id {
                store.regions.insert(end_key.clone());
            }

            c.stores.insert(store_id, store);
        }

        c.regions.insert(end_key, region);

        c
    }

    fn put_node(&mut self, node: metapb::Node) -> Result<()> {
        let mut n = self.nodes.get_mut(&node.get_node_id()).unwrap();
        n.node = node;
        Ok(())
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<()> {
        let mut s = self.stores.get_mut(&store.get_store_id()).unwrap();
        s.store = store;
        Ok(())
    }

    fn delete_node(&mut self, node_id: u64) -> Result<()> {
        {
            let n = self.nodes.get(&node_id).unwrap();
            if !n.stores.is_empty() {
                return Err(Error::DeleteNotEmptyNode(node_id));
            }
        }

        self.nodes.remove(&node_id);
        Ok(())
    }

    fn delete_store(&mut self, store_id: u64) -> Result<()> {
        {
            let s = self.stores.get(&store_id).unwrap();
            if !s.regions.is_empty() {
                return Err(Error::DeleteNotEmptyStore(store_id));
            }

            let mut n = self.nodes.get_mut(&s.store.get_node_id()).unwrap();
            n.stores.remove(&store_id);
        }

        self.stores.remove(&store_id);

        Ok(())
    }

    fn get_node(&self, node_id: u64) -> Result<metapb::Node> {
        Ok(self.nodes.get(&node_id).unwrap().node.clone())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        Ok(self.stores.get(&store_id).unwrap().store.clone())
    }

    fn get_regions(&self, keys: Vec<Key>) -> Result<Vec<metapb::Region>> {
        let mut end_keys: HashSet<Key> = HashSet::new();
        let mut regions: Vec<metapb::Region> = vec![];
        for key in keys {
            // must exist a region contains this key.
            let (end_key, region) = self.regions
                                        .range::<Key, Key>(Included(&key), Unbounded)
                                        .next()
                                        .unwrap();
            if !end_keys.insert(end_key.clone()) {
                regions.push(region.clone());
            }
        }

        Ok(regions)
    }


    fn scan_regions(&self, start_key: Vec<u8>, limit: u32) -> Result<Vec<metapb::Region>> {
        let iter = self.regions.range::<Key, Key>(Included(&start_key), Unbounded);

        let mut regions: Vec<metapb::Region> = vec![];
        for (n, (_, region)) in iter.enumerate() {
            if n >= limit as usize {
                break;
            }

            if region.get_start_key() >= &start_key {
                regions.push(region.clone());
            }
        }

        Ok(regions)
    }
}

pub struct PdClient {
    clusters: HashMap<u64, Cluster>,

    node_id: u64,
    store_id: u64,
    region_id: u64,
    peer_id: u64,
}

impl PdClient {
    pub fn new() -> PdClient {
        PdClient {
            clusters: HashMap::new(),
            node_id: 0,
            store_id: 0,
            region_id: 0,
            peer_id: 0,
        }
    }

    fn get_cluster(&self, cluster_id: u64) -> Result<&Cluster> {
        match self.clusters.get(&cluster_id) {
            None => Err(Error::ClusterNotBootstrapped(cluster_id)),
            Some(cluster) => Ok(cluster),
        }
    }

    fn get_mut_cluster(&mut self, cluster_id: u64) -> Result<&mut Cluster> {
        match self.clusters.get_mut(&cluster_id) {
            None => Err(Error::ClusterNotBootstrapped(cluster_id)),
            Some(cluster) => Ok(cluster),
        }
    }
}

impl Client for PdClient {
    fn boostrap_cluster(&mut self,
                        cluster_id: u64,
                        node: metapb::Node,
                        stores: Vec<metapb::Store>,
                        region: metapb::Region)
                        -> Result<()> {
        if self.is_cluster_bootstrapped(cluster_id).unwrap() {
            return Err(Error::ClusterBootstrapped(cluster_id));
        }

        self.clusters.insert(cluster_id, Cluster::new(cluster_id, node, stores, region));

        Ok(())
    }

    fn is_cluster_bootstrapped(&self, cluster_id: u64) -> Result<bool> {
        Ok(self.clusters.contains_key(&cluster_id))
    }

    fn alloc_node_id(&mut self) -> Result<u64> {
        self.node_id += 1;
        Ok(self.node_id)
    }

    fn alloc_store_id(&mut self) -> Result<u64> {
        self.store_id += 1;
        Ok(self.store_id)
    }

    fn alloc_peer_id(&mut self) -> Result<u64> {
        self.peer_id += 1;
        Ok(self.peer_id)
    }

    fn alloc_region_id(&mut self) -> Result<u64> {
        self.region_id += 1;
        Ok(self.region_id)
    }

    fn put_node(&mut self, cluster_id: u64, node: metapb::Node) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.put_node(node)
    }

    fn put_store(&mut self, cluster_id: u64, store: metapb::Store) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.put_store(store)
    }

    fn delete_node(&mut self, cluster_id: u64, node_id: u64) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.delete_node(node_id)
    }

    fn delete_store(&mut self, cluster_id: u64, store_id: u64) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.delete_store(store_id)
    }

    fn get_node(&self, cluster_id: u64, node_id: u64) -> Result<metapb::Node> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_node(node_id)
    }

    fn get_store(&self, cluster_id: u64, store_id: u64) -> Result<metapb::Store> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_store(store_id)
    }


    fn get_regions(&self, cluster_id: u64, keys: Vec<Key>) -> Result<Vec<metapb::Region>> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_regions(keys)
    }


    fn scan_regions(&self,
                    cluster_id: u64,
                    start_key: Vec<u8>,
                    limit: u32)
                    -> Result<Vec<metapb::Region>> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.scan_regions(start_key, limit)
    }
}
