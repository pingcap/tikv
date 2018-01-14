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

use std::ops::Deref;
use std::sync::Arc;

use kvproto::metapb::*;
use kvproto::kvrpcpb::*;
use kvproto::importpb::*;

use pd::RegionInfo;

use super::Client;

// Just used as a mark, don't use them in comparison.
pub const RANGE_MIN: &'static [u8] = &[];
pub const RANGE_MAX: &'static [u8] = &[];

pub fn new_range(start: &[u8], end: &[u8]) -> Range {
    let mut range = Range::new();
    range.set_start(start.to_owned());
    range.set_end(end.to_owned());
    range
}

pub fn before_end(key: &[u8], end: &[u8]) -> bool {
    key < end || end == RANGE_MAX
}

#[derive(Clone, Debug)]
pub struct RangeInfo {
    pub range: Range,
    pub size: usize,
}

impl RangeInfo {
    pub fn new(start: &[u8], end: &[u8], size: usize) -> RangeInfo {
        RangeInfo {
            range: new_range(start, end),
            size: size,
        }
    }
}

impl Deref for RangeInfo {
    type Target = Range;

    fn deref(&self) -> &Self::Target {
        &self.range
    }
}

pub struct RangeContext<C> {
    client: Arc<C>,
    region: Option<RegionInfo>,
    raw_size: usize,
    limit_size: usize,
}

impl<C: Client> RangeContext<C> {
    pub fn new(client: Arc<C>, limit_size: usize) -> RangeContext<C> {
        RangeContext {
            client: client,
            region: None,
            raw_size: 0,
            limit_size: limit_size,
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        self.raw_size += key.len() + value.len()
    }

    /// Reset size and region for the next key.
    pub fn reset(&mut self, key: &[u8]) {
        self.raw_size = 0;
        if let Some(ref region) = self.region {
            if before_end(key, region.get_end_key()) {
                // Still belongs in this region, no need to update.
                return;
            }
        }
        self.region = match self.client.get_region(key) {
            Ok(region) => Some(region),
            Err(e) => {
                error!("get region: {:?}", e);
                None
            }
        }
    }

    pub fn end_key(&self) -> &[u8] {
        match self.region {
            Some(ref region) => region.get_end_key(),
            None => RANGE_MAX,
        }
    }

    pub fn raw_size(&self) -> usize {
        self.raw_size
    }

    /// Check size and region range to see if we should stop before this key.
    pub fn should_stop_before(&self, key: &[u8]) -> bool {
        if !before_end(key, self.end_key()) {
            return true;
        }
        self.raw_size >= self.limit_size
    }
}

pub fn new_context(region: &RegionInfo) -> Context {
    let peer = if let Some(ref leader) = region.leader {
        leader.clone()
    } else {
        // We don't know the leader, just choose the first one.
        region.get_peers().first().unwrap().clone()
    };

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(peer.clone());
    ctx
}

pub fn find_region_peer(region: &Region, store_id: u64) -> Option<Peer> {
    region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use import::client::tests::MockClient;

    #[test]
    fn test_before_end() {
        assert!(before_end(b"ab", b"bc"));
        assert!(!before_end(b"ab", b"ab"));
        assert!(!before_end(b"cd", b"bc"));
        assert!(before_end(b"cd", RANGE_MAX));
    }

    #[test]
    fn test_range_context() {
        let mut client = MockClient::new();
        client.add_region_range(b"", b"k4");
        client.add_region_range(b"k4", b"");

        let mut ctx = RangeContext::new(Arc::new(client), 8);

        ctx.add(b"k1", b"v1");
        assert!(!ctx.should_stop_before(b"k2"));
        ctx.add(b"k2", b"v2");
        assert_eq!(ctx.raw_size(), 8);
        // Reach size limit.
        assert!(ctx.should_stop_before(b"k3"));

        ctx.reset(b"k3");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(b"k3", b"v3");
        // Reach region end.
        assert!(ctx.should_stop_before(b"k4"));

        ctx.reset(b"k4");
        assert_eq!(ctx.raw_size(), 0);
        ctx.add(b"k4", b"v4");
        assert!(!ctx.should_stop_before(b"k5"));
    }
}
