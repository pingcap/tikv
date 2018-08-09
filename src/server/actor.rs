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

use std::process;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::transport::RaftStoreRouter;
use super::Result;
use futures_cpupool::Builder;
use import::SSTImporter;
use kvproto::metapb;
use kvproto::raft_serverpb::StoreIdent;
use pd::{Error as PdError, PdClient, PdTask, INVALID_ID};
use protobuf::RepeatedField;
use raftstore::coprocessor::dispatcher::CoprocessorHost;
use raftstore::store::actor_store::Store;
use raftstore::store::router::{InternalMailboxes, InternalTransport};
use raftstore::store::{
    self, keys, AllMsg, Config as StoreConfig, Engines, Msg, Peekable, SnapManager, Transport,
};
use server::readpool::ReadPool;
use server::Config as ServerConfig;
use storage::{self, Config as StorageConfig, RaftKv, Storage};
use util::mpsc::Receiver;
use util::transport::InternalSendCh;
use util::worker::FutureWorker;

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS: u64 = 3;

pub fn create_raft_storage<S>(
    router: S,
    cfg: &StorageConfig,
    read_pool: ReadPool<storage::ReadPoolContext>,
) -> Result<Storage<RaftKv<S>>>
where
    S: RaftStoreRouter + 'static,
{
    let engine = RaftKv::new(router);
    let store = Storage::from_engine(engine, cfg, read_pool)?;
    Ok(store)
}

fn check_region_epoch(region: &metapb::Region, other: &metapb::Region) -> Result<()> {
    let epoch = region.get_region_epoch();
    let other_epoch = other.get_region_epoch();
    if epoch.get_conf_ver() != other_epoch.get_conf_ver() {
        return Err(box_err!(
            "region conf_ver inconsist: {} with {}",
            epoch.get_conf_ver(),
            other_epoch.get_conf_ver()
        ));
    }
    if epoch.get_version() != other_epoch.get_version() {
        return Err(box_err!(
            "region version inconsist: {} with {}",
            epoch.get_version(),
            other_epoch.get_version()
        ));
    }
    Ok(())
}

// Node is a wrapper for raft store.
// TODO: we will rename another better name like RaftStore later.
pub struct Node<C: PdClient + 'static> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: StoreConfig,
    mailboxes: InternalMailboxes,
    pd_client: Arc<C>,
    started: bool,
}

impl<C> Node<C>
where
    C: PdClient,
{
    pub fn new(
        cfg: &ServerConfig,
        store_cfg: &StoreConfig,
        pd_client: Arc<C>,
        mailboxes: InternalMailboxes,
    ) -> Node<C> {
        let mut store = metapb::Store::new();
        store.set_id(INVALID_ID);
        if cfg.advertise_addr.is_empty() {
            store.set_address(cfg.addr.clone());
        } else {
            store.set_address(cfg.advertise_addr.clone())
        }
        store.set_version(env!("CARGO_PKG_VERSION").to_string());

        let mut labels = Vec::new();
        for (k, v) in &cfg.labels {
            let mut label = metapb::StoreLabel::new();
            label.set_key(k.to_owned());
            label.set_value(v.to_owned());
            labels.push(label);
        }
        store.set_labels(RepeatedField::from_vec(labels));

        Node {
            cluster_id: cfg.cluster_id,
            store,
            store_cfg: store_cfg.clone(),
            mailboxes,
            pd_client,
            started: false,
        }
    }

    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    pub fn start<T>(
        &mut self,
        engines: Engines,
        trans: T,
        snap_mgr: SnapManager,
        receiver: Receiver<AllMsg>,
        pd_worker: FutureWorker<PdTask>,
        coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let bootstrapped = self.check_cluster_bootstrapped()?;
        let mut store_id = self.check_store(&engines)?;
        if store_id == INVALID_ID {
            store_id = self.bootstrap_store(&engines)?;
        } else if !bootstrapped {
            // We have saved data before, and the cluster must be bootstrapped.
            return Err(box_err!(
                "store {} is not empty, but cluster {} is not bootstrapped, \
                 maybe you connected a wrong PD or need to remove the TiKV data \
                 and start again",
                store_id,
                self.cluster_id
            ));
        }

        self.store.set_id(store_id);
        self.check_prepare_bootstrap_cluster(&engines)?;
        if !bootstrapped {
            // cluster is not bootstrapped, and we choose first store to bootstrap
            // prepare bootstrap.
            let region = self.prepare_bootstrap_cluster(&engines, store_id)?;
            self.bootstrap_cluster(&engines, region)?;
        }

        // inform pd.
        self.pd_client.put_store(self.store.clone())?;
        self.start_store(
            store_id,
            engines,
            trans,
            snap_mgr,
            receiver,
            pd_worker,
            coprocessor_host,
            importer,
        )?;
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn get_sendch(&self) -> InternalSendCh<Msg> {
        InternalSendCh::new(InternalTransport::new(self.mailboxes.clone()), "raftstore")
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, engines: &Engines) -> Result<u64> {
        let res = engines.kv.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)?;
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            error!(
                "cluster ID mismatch: local_id {} remote_id {}. \
                 you are trying to connect to another cluster, please reconnect to the correct PD",
                ident.get_cluster_id(),
                self.cluster_id
            );
            process::exit(1);
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }

        Ok(store_id)
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = self.pd_client.alloc_id()?;
        Ok(id)
    }

    fn bootstrap_store(&self, engines: &Engines) -> Result<u64> {
        let store_id = self.alloc_id()?;
        info!("alloc store id {} ", store_id);

        store::bootstrap_store(engines, self.cluster_id, store_id)?;

        Ok(store_id)
    }

    pub fn prepare_bootstrap_cluster(
        &self,
        engines: &Engines,
        store_id: u64,
    ) -> Result<metapb::Region> {
        let region_id = self.alloc_id()?;
        info!(
            "alloc first region id {} for cluster {}, store {}",
            region_id, self.cluster_id, store_id
        );
        let peer_id = self.alloc_id()?;
        info!(
            "alloc first peer id {} for first region {}",
            peer_id, region_id
        );

        let region = store::prepare_bootstrap(engines, store_id, region_id, peer_id)?;
        Ok(region)
    }

    fn check_prepare_bootstrap_cluster(&self, engines: &Engines) -> Result<()> {
        let res = engines
            .kv
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)?;
        if res.is_none() {
            return Ok(());
        }

        let first_region = res.unwrap();
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.get_region(b"") {
                Ok(region) => {
                    if region.get_id() == first_region.get_id() {
                        check_region_epoch(&region, &first_region)?;
                        store::clear_prepare_bootstrap_state(engines)?;
                    } else {
                        store::clear_prepare_bootstrap(engines, first_region.get_id())?;
                    }
                    return Ok(());
                }

                Err(e) => {
                    warn!("check cluster prepare bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster prepare bootstrapped failed"))
    }

    fn bootstrap_cluster(&mut self, engines: &Engines, region: metapb::Region) -> Result<()> {
        let region_id = region.get_id();
        match self.pd_client.bootstrap_cluster(self.store.clone(), region) {
            Err(PdError::ClusterBootstrapped(_)) => {
                error!("cluster {} is already bootstrapped", self.cluster_id);
                store::clear_prepare_bootstrap(engines, region_id)?;
                Ok(())
            }
            // TODO: should we clean region for other errors too?
            Err(e) => panic!("bootstrap cluster {} err: {:?}", self.cluster_id, e),
            Ok(_) => {
                store::clear_prepare_bootstrap_state(engines)?;
                info!("bootstrap cluster {} ok", self.cluster_id);
                Ok(())
            }
        }
    }

    fn check_cluster_bootstrapped(&self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!("check cluster bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(
                CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS,
            ));
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    fn start_store<T>(
        &mut self,
        store_id: u64,
        engines: Engines,
        trans: T,
        snap_mgr: SnapManager,
        receiver: Receiver<AllMsg>,
        pd_worker: FutureWorker<PdTask>,
        coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        info!("start raft store {}", store_id);

        if self.started {
            return Err(box_err!("{} is already started", store_id));
        }

        let cfg = self.store_cfg.clone();
        let pd_client = Arc::clone(&self.pd_client);
        let store = self.store.clone();
        let mailboxes = self.mailboxes.clone();

        let poller = Builder::new()
            .pool_size(self.store_cfg.concurrency)
            .name_prefix(thd_name!(format!("raftstore-{}.", store_id)))
            .create();
        let store = match Store::new(
            mailboxes,
            receiver,
            store,
            cfg,
            engines,
            trans,
            pd_client,
            snap_mgr,
            pd_worker,
            coprocessor_host,
            importer,
            poller,
        ) {
            Ok(s) => s,
            Err(e) => panic!("construct store {} fail: {:?}", store_id, e),
        };
        if let Err(e) = store.run() {
            panic!("store {} run fail: {:?}", store_id, e);
        }
        self.started = true;
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) -> Result<()> {
        info!("stop raft store {} thread", store_id);
        let _ = self
            .mailboxes
            .lock()
            .unwrap()
            .get_mut(&0)
            .unwrap()
            .force_send(AllMsg::Msg(Msg::Quit));
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let store_id = self.store.get_id();
        self.stop_store(store_id)
    }
}

#[cfg(test)]
mod tests {
    use super::check_region_epoch;
    use kvproto::metapb;
    use raftstore::store::keys;

    #[test]
    fn test_check_region_epoch() {
        let mut r1 = metapb::Region::new();
        r1.set_id(1);
        r1.set_start_key(keys::EMPTY_KEY.to_vec());
        r1.set_end_key(keys::EMPTY_KEY.to_vec());
        r1.mut_region_epoch().set_version(1);
        r1.mut_region_epoch().set_conf_ver(1);

        let mut r2 = metapb::Region::new();
        r2.set_id(1);
        r2.set_start_key(keys::EMPTY_KEY.to_vec());
        r2.set_end_key(keys::EMPTY_KEY.to_vec());
        r2.mut_region_epoch().set_version(2);
        r2.mut_region_epoch().set_conf_ver(1);

        let mut r3 = metapb::Region::new();
        r3.set_id(1);
        r3.set_start_key(keys::EMPTY_KEY.to_vec());
        r3.set_end_key(keys::EMPTY_KEY.to_vec());
        r3.mut_region_epoch().set_version(1);
        r3.mut_region_epoch().set_conf_ver(2);

        assert!(check_region_epoch(&r1, &r2).is_err());
        assert!(check_region_epoch(&r1, &r3).is_err());
    }
}
