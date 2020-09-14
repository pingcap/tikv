// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::StorageReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter};
use crate::storage::metrics;
use std::sync::{Arc, Mutex};
use tikv_util::time::{Duration, Instant};
use tikv_util::yatp_pool::{Config, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder};

#[derive(Clone)]
pub struct FuturePoolTicker<R>
where
    R: FlowStatsReporter,
{
    reporter: R,
    last_tick: Instant,
}

impl<R: FlowStatsReporter> FuturePoolTicker<R> {
    pub fn new(reporter: R) -> Self {
        Self {
            reporter,
            last_tick: Instant::now_coarse(),
        }
    }
}

const TICK_INTERVAL: Duration = Duration::from_secs(1);

impl<R> PoolTicker for FuturePoolTicker<R>
where
    R: FlowStatsReporter,
{
    fn on_tick(&mut self) {
        let now = Instant::now_coarse();
        if now.duration_since(self.last_tick) < TICK_INTERVAL {
            return;
        }
        metrics::tls_flush(&self.reporter);
        self.last_tick = now;
    }
}

pub fn build_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let reporter2 = reporter.clone();
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(FuturePoolTicker::new(reporter))
                .name_prefix(name)
                .config(config)
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                .before_stop(move || {
                    // Safety: we call `set_` and `destroy_` with the same engine type.
                    unsafe {
                        destroy_tls_engine::<E>();
                    }
                    metrics::tls_flush(&reporter2)
                })
                .build_future_pool()
        })
        .collect()
}

pub fn build_read_pool_for_test<E: Engine>(
    config: &StorageReadPoolConfig,
    engine: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            YatpPoolBuilder::new(DefaultTicker::default())
                .config(config)
                .name_prefix(name)
                .after_start(move || set_tls_engine(engine.lock().unwrap().clone()))
                // Safety: we call `set_` and `destroy_` with the same engine type.
                .before_stop(|| unsafe { destroy_tls_engine::<E>() })
                .build_future_pool()
        })
        .collect()
}
