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

use super::{AdminObserver, Coprocessor, ObserverContext, QueryObserver, Result};
use coprocessor::cache::DISTSQL_CACHE;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, Request, Response};
use protobuf::RepeatedField;

pub struct DistSQLObserver;

impl DistSQLObserver {
    fn disable_cache(&self) {
        DISTSQL_CACHE.lock().unwrap().disable();
    }

    fn evict_region(&self, ctx: &mut ObserverContext) {
        let region_id = ctx.region().get_id();
        DISTSQL_CACHE
            .lock()
            .unwrap()
            .evict_region_and_enable(region_id);
    }
}

impl Coprocessor for DistSQLObserver {}

impl QueryObserver for DistSQLObserver {
    fn pre_apply_query(&self, _: &mut ObserverContext, _: &[Request]) {
        self.disable_cache();
    }

    fn post_apply_query(&self, ctx: &mut ObserverContext, _: &mut RepeatedField<Response>) {
        self.evict_region(ctx);
    }
}

impl AdminObserver for DistSQLObserver {
    fn pre_propose_admin(&self, ctx: &mut ObserverContext, req: &mut AdminRequest) -> Result<()> {
        let cmd_type = req.get_cmd_type();
        let has_split = req.has_split();
        if cmd_type == AdminCmdType::Split && has_split {
            self.evict_region(ctx);
        } else if cmd_type == AdminCmdType::TransferLeader {
            self.evict_region(ctx);
        }
        Ok(())
    }
}
