// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use byteorder::{BigEndian, WriteBytesExt};
use engine_traits::{KvEngine, Snapshot};
use kvproto::metapb::Region;
use tikv_util::worker::Runnable;

use crate::coprocessor::CoprocessorHost;
use crate::store::metrics::*;
use crate::store::{CasualMessage, CasualRouter};

use super::metrics::*;

/// Consistency checking task.
pub enum Task<S> {
    ComputeHash {
        index: u64,
        context: Vec<u8>,
        region: Region,
        snap: S,
    },
}

impl<S: Snapshot> Task<S> {
    pub fn compute_hash(region: Region, index: u64, context: Vec<u8>, snap: S) -> Task<S> {
        Task::ComputeHash {
            index,
            context,
            region,
            snap,
        }
    }
}

impl<S: Snapshot> Display for Task<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::ComputeHash {
                ref region, index, ..
            } => write!(f, "Compute Hash Task for {:?} at {}", region, index),
        }
    }
}

pub struct Runner<EK: KvEngine, C: CasualRouter<EK>> {
    router: C,
    coprocessor_host: CoprocessorHost<EK>,
}

impl<EK: KvEngine, C: CasualRouter<EK>> Runner<EK, C> {
    pub fn new(router: C, cop_host: CoprocessorHost<EK>) -> Runner<EK, C> {
        Runner {
            router,
            coprocessor_host: cop_host,
        }
    }

    /// Computes the hash of the Region.
    fn compute_hash(&mut self, region: Region, index: u64, context: Vec<u8>, snap: EK::Snapshot) {
        info!("computing hash"; "region_id" => region.get_id(), "index" => index);
        REGION_HASH_COUNTER.compute.all.inc();

        let timer = REGION_HASH_HISTOGRAM.start_coarse_timer();
        let sum = match self
            .coprocessor_host
            .get_consistency_checker_host()
            .compute_hash(&region, &context, snap)
        {
            Ok(hash) => hash,
            Err(e) => {
                error!("calculate hash"; "region_id" => region.get_id(), "err" => ?e);
                REGION_HASH_COUNTER.compute.failed.inc();
                return;
            }
        };
        timer.observe_duration();

        let mut checksum = Vec::with_capacity(4);
        checksum.write_u32::<BigEndian>(sum).unwrap();
        let msg = CasualMessage::ComputeHashResult {
            index,
            context,
            hash: checksum,
        };
        if let Err(e) = self.router.send(region.get_id(), msg) {
            warn!(
                "failed to send hash compute result";
                "region_id" => region.get_id(),
                "err" => %e,
            );
        }
    }
}

impl<EK, C> Runnable<Task<EK::Snapshot>> for Runner<EK, C>
where
    EK: KvEngine,
    C: CasualRouter<EK>,
{
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ComputeHash {
                index,
                context,
                region,
                snap,
            } => self.compute_hash(region, index, context, snap),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use engine_rocks::util::new_engine;
    use engine_rocks::{RocksEngine, RocksSnapshot};
    use engine_traits::{KvEngine, SyncMutable, CF_DEFAULT, CF_RAFT};
    use kvproto::metapb::*;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::Builder;
    use tikv_util::worker::Runnable;

    #[test]
    fn test_consistency_check() {
        let path = Builder::new().prefix("tikv-store-test").tempdir().unwrap();
        let db = new_engine(
            path.path().to_str().unwrap(),
            None,
            &[CF_DEFAULT, CF_RAFT],
            None,
        )
        .unwrap();

        let mut region = Region::default();
        region.mut_peers().push(Peer::default());

        let (tx, rx) = mpsc::sync_channel(100);
        let mut runner = Runner::new(tx, CoprocessorHost::<RocksEngine>::default());
        let mut digest = crc32fast::Hasher::new();
        let kvs = vec![(b"k1", b"v1"), (b"k2", b"v2")];
        for (k, v) in kvs {
            let key = keys::data_key(k);
            db.put(&key, v).unwrap();
            // hash should contain all kvs
            digest.update(&key);
            digest.update(v);
        }

        // hash should also contains region state key.
        digest.update(&keys::region_state_key(region.get_id()));
        let sum = digest.finalize();
        runner.run(Task::<RocksSnapshot>::ComputeHash {
            index: 10,
            context: vec![],
            region: region.clone(),
            snap: db.snapshot(),
        });
        let mut checksum_bytes = vec![];
        checksum_bytes.write_u32::<BigEndian>(sum).unwrap();

        let res = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        match res {
            (
                region_id,
                CasualMessage::ComputeHashResult {
                    index,
                    hash,
                    context,
                },
            ) => {
                assert_eq!(region_id, region.get_id());
                assert_eq!(index, 10);
                assert_eq!(context, Vec::<u8>::default());
                assert_eq!(hash, checksum_bytes);
            }
            e => panic!("unexpected {:?}", e),
        }
    }
}
