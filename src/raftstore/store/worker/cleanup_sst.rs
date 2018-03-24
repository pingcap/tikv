// Copyright 2018 PingCAP, Inc.
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

use std::fmt;
use std::sync::Arc;

use kvproto::importpb::SSTMeta;

use pd::PdClient;
use import::SSTImporter;
use raftstore::store::Msg;
use raftstore::store::util::is_epoch_stale;
use util::transport::SendCh;
use util::worker::Runnable;

pub enum Task {
    DeleteSST { ssts: Vec<SSTMeta> },
    ValidateSST { ssts: Vec<SSTMeta> },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Task::DeleteSST { ref ssts } => write!(f, "Delete {} ssts", ssts.len()),
            Task::ValidateSST { ref ssts } => write!(f, "Validate {} ssts", ssts.len()),
        }
    }
}

pub struct Runner<C> {
    store_id: u64,
    store_ch: SendCh<Msg>,
    importer: Arc<SSTImporter>,
    pd_client: Arc<C>,
}

impl<C: PdClient> Runner<C> {
    pub fn new(
        store_id: u64,
        store_ch: SendCh<Msg>,
        importer: Arc<SSTImporter>,
        pd_client: Arc<C>,
    ) -> Runner<C> {
        Runner {
            store_id: store_id,
            store_ch: store_ch,
            importer: importer,
            pd_client: pd_client,
        }
    }

    fn handle_delete_sst(&self, ssts: Vec<SSTMeta>) {
        for sst in &ssts {
            let _ = self.importer.delete(sst);
        }
    }

    fn handle_validate_sst(&self, ssts: Vec<SSTMeta>) {
        let store_id = self.store_id;
        let mut results = Vec::new();
        for sst in ssts {
            match self.pd_client.get_region(sst.get_range().get_start()) {
                Ok(region) => {
                    // The region id may or may not be the same as the
                    // SST file, but it doesn't matter, because the
                    // epoch of a range will not decrease anyway.
                    if is_epoch_stale(region.get_region_epoch(), sst.get_region_epoch()) {
                        // Region has not been updated.
                        continue;
                    }
                    if region
                        .get_peers()
                        .into_iter()
                        .any(|p| p.get_store_id() == store_id)
                    {
                        // The SST still belongs to the store.
                        continue;
                    }
                    results.push(sst);
                }
                Err(e) => {
                    error!("get region failed: {:?}", e);
                }
            }
        }

        // We need to send back the result because of a time window
        // between we check the local store and get region information
        // from PD. During this window, a region may leave a stale
        // peer on this store, which may ingest the SST file before it
        // is destroyed.
        let msg = Msg::ValidateSSTResult(results);
        if let Err(e) = self.store_ch.try_send(msg) {
            error!("send validate sst result: {:?}", e);
        }
    }
}

impl<C: PdClient> Runnable<Task> for Runner<C> {
    fn run(&mut self, task: Task) {
        match task {
            Task::DeleteSST { ssts } => {
                self.handle_delete_sst(ssts);
            }
            Task::ValidateSST { ssts } => {
                self.handle_validate_sst(ssts);
            }
        }
    }
}
