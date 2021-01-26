// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tikv_util::IntervalTask;

use crate::iosnoop::{fetch_io_bytes, flush_io_latency_metrics};
use crate::metrics::IO_BYTES_VEC;
use crate::IOBytes;
use crate::IORateLimiterStatistics;
use crate::{IOOp, IOType};

pub enum BytesFetcher {
    /// Fetch IO statistics from IO rate limiter, which records passed-through IOs in atomic counters.
    FromRateLimiter(Arc<IORateLimiterStatistics>),
    /// Fetch IO statistics from bcc snooper, which traces IO requests spawned by current process.
    FromIOSnooper(),
}

impl BytesFetcher {
    fn fetch(&self, io_type: IOType) -> IOBytes {
        match *self {
            BytesFetcher::FromRateLimiter(ref stats) => IOBytes {
                read: stats.fetch(io_type, IOOp::Read) as i64,
                write: stats.fetch(io_type, IOOp::Write) as i64,
            },
            BytesFetcher::FromIOSnooper() => fetch_io_bytes(io_type),
        }
    }
}

pub struct MetricsTask {
    fetcher: BytesFetcher,
    last_fetch: [IOBytes; IOType::VARIANT_COUNT],
}

impl MetricsTask {
    pub fn new(fetcher: BytesFetcher) -> Self {
        MetricsTask {
            fetcher,
            last_fetch: Default::default(),
        }
    }
}

macro_rules! flush_io_bytes {
    ($fetcher:expr, $metrics:ident, $io_type:expr, $last_fetch:expr) => {
        let bytes = $fetcher.fetch($io_type);
        let delta_bytes = bytes - $last_fetch;
        $last_fetch = bytes;
        IO_BYTES_VEC.$metrics.read.inc_by(delta_bytes.read);
        IO_BYTES_VEC.$metrics.write.inc_by(delta_bytes.write);
    };
}

impl IntervalTask for MetricsTask {
    fn on_tick(&mut self) {
        flush_io_latency_metrics();
        flush_io_bytes!(
            self.fetcher,
            other,
            IOType::Other,
            self.last_fetch[IOType::Other as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            foreground_read,
            IOType::ForegroundRead,
            self.last_fetch[IOType::ForegroundRead as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            foreground_write,
            IOType::ForegroundWrite,
            self.last_fetch[IOType::ForegroundWrite as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            flush,
            IOType::Flush,
            self.last_fetch[IOType::Flush as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            compaction,
            IOType::Compaction,
            self.last_fetch[IOType::Compaction as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            replication,
            IOType::Replication,
            self.last_fetch[IOType::Replication as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            load_balance,
            IOType::LoadBalance,
            self.last_fetch[IOType::LoadBalance as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            import,
            IOType::Import,
            self.last_fetch[IOType::Import as usize]
        );
        flush_io_bytes!(
            self.fetcher,
            export,
            IOType::Export,
            self.last_fetch[IOType::Export as usize]
        );
    }
}
