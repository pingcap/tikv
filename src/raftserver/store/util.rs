use std::option::Option;

use uuid::Uuid;

use proto::metapb;
use proto::raft_cmdpb::RaftCommandRequest;
use raftserver::{Result, Error};

pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    for peer in region.get_peers() {
        if peer.get_store_id() == store_id {
            return Some(&peer);
        }
    }

    None
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<metapb::Peer> {
    match region.get_peers()
                .iter()
                .position(|x| x.get_store_id() == store_id) {
        None => None,
        Some(index) => Some(region.mut_peers().remove(index)),
    }
}

// a helper function to create peer easily.
pub fn new_peer(node_id: u64, store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_node_id(node_id);
    peer.set_store_id(store_id);
    peer.set_peer_id(peer_id);

    peer
}

pub fn get_uuid_from_req(cmd: &RaftCommandRequest) -> Option<Uuid> {
    Uuid::from_bytes(cmd.get_header().get_uuid())
}

pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    // TODO: if we use column family later, the maximum end key is empty,
    // we should use another way to check it.
    if key < end_key && key >= start_key {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

 #[cfg(test)]
mod tests {
    use proto::metapb;

    use super::*;

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::new();
        region.set_region_id(1);
        region.mut_peers().push(new_peer(1, 1, 1));

        assert!(find_peer(&region, 1).is_some());
        assert!(find_peer(&region, 10).is_none());

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(find_peer(&region, 1).is_none());

    }
}
