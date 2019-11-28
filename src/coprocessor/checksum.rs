// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::vec::IntoIter;

use kvproto::coprocessor::{KeyRange, Response};
use protobuf::Message;
use tipb::checksum::{ChecksumAlgorithm, ChecksumRequest, ChecksumResponse, ChecksumScanOn};

use crate::storage::{Snapshot, SnapshotStore};

use crate::coprocessor::dag::executor::ExecutorMetrics;
use crate::coprocessor::dag::{ScanOn, Scanner};
use crate::coprocessor::*;

// `ChecksumContext` is used to handle `ChecksumRequest`
pub struct ChecksumContext<S: Snapshot> {
    req: ChecksumRequest,
    store: SnapshotStore<S>,
    ranges: IntoIter<KeyRange>,
    scanner: Option<Scanner<SnapshotStore<S>>>,
    metrics: ExecutorMetrics,
}

impl<S: Snapshot> ChecksumContext<S> {
    pub fn new(
        req: ChecksumRequest,
        ranges: Vec<KeyRange>,
        snap: S,
        req_ctx: &ReqContext,
    ) -> Result<Self> {
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.context.get_isolation_level(),
            !req_ctx.context.get_not_fill_cache(),
        );
        Ok(Self {
            req,
            store,
            ranges: ranges.into_iter(),
            scanner: None,
            metrics: ExecutorMetrics::default(),
        })
    }

    fn next_row(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            if let Some(scanner) = self.scanner.as_mut() {
                self.metrics.scan_counter.inc_range();
                match scanner.next_row()? {
                    Some(row) => return Ok(Some(row)),
                    None => scanner.collect_statistics_into(&mut self.metrics.cf_stats),
                }
            }

            if let Some(range) = self.ranges.next() {
                self.scanner = match self.scanner.take() {
                    Some(mut scanner) => {
                        box_try!(scanner.reset_range(range, &self.store));
                        Some(scanner)
                    }
                    None => Some(self.new_scanner(range)?),
                };
                continue;
            }

            return Ok(None);
        }
    }

    fn new_scanner(&self, range: KeyRange) -> Result<Scanner<SnapshotStore<S>>> {
        let scan_on = match self.req.get_scan_on() {
            ChecksumScanOn::Table => ScanOn::Table,
            ChecksumScanOn::Index => ScanOn::Index,
        };
        Scanner::new(&self.store, scan_on, false, false, range).map_err(Error::from)
    }
}

impl<S: Snapshot> RequestHandler for ChecksumContext<S> {
    fn handle_request(&mut self) -> Result<Response> {
        let algorithm = self.req.get_algorithm();
        if algorithm != ChecksumAlgorithm::Crc64_Xor {
            return Err(box_err!("unknown checksum algorithm {:?}", algorithm));
        }

        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let (old_prefix, new_prefix) = if self.req.has_rule() {
            let mut rule = self.req.get_rule().clone();
            (rule.take_old_prefix(), rule.take_new_prefix())
        } else {
            (vec![], vec![])
        };

        let mut prefix_digest = crc64fast::Digest::new();
        prefix_digest.write(&old_prefix);

        while let Some((k, v)) = self.next_row()? {
            if !k.starts_with(&new_prefix) {
                return Err(box_err!("Wrong prefix expect: {:?}", new_prefix));
            }
            checksum =
                checksum_crc64_xor(checksum, prefix_digest.clone(), &k[new_prefix.len()..], &v);
            total_kvs += 1;
            total_bytes += k.len() + v.len() + old_prefix.len() - new_prefix.len();
        }

        let mut resp = ChecksumResponse::new();
        resp.set_checksum(checksum);
        resp.set_total_kvs(total_kvs);
        resp.set_total_bytes(total_bytes as u64);
        let data = box_try!(resp.write_to_bytes());

        let mut resp = Response::new();
        resp.set_data(data);
        Ok(resp)
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        metrics.merge(&mut self.metrics);
    }
}

pub fn checksum_crc64_xor(
    checksum: u64,
    mut digest: crc64fast::Digest,
    k_suffix: &[u8],
    v: &[u8],
) -> u64 {
    digest.write(k_suffix);
    digest.write(v);
    checksum ^ digest.sum64()
}
