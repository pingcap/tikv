// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::f64::INFINITY;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::u64;

use collections::HashMap;
use engine_traits::{CFNamesExt, MiscExt};
use rand::Rng;
use tikv_util::time::{duration_to_sec, Consume, Instant, Limiter};

use crate::storage::config::Config;
use crate::storage::metrics::*;
use crate::storage::Engine;

const ADJUST_DURATION: u64 = 1000; // 1000ms
const RATIO_PRECISION: f64 = 10000000.0;
const EMA_FACTOR: f64 = 0.1;
const LIMIT_UP_PERCENT: f64 = 0.04; // 4%
const LIMIT_DOWN_PERCENT: f64 = 0.02; // 2%
const MIN_THROTTLE_SPEED: f64 = 16.0 * 1024.0; // 16KB

enum Trend {
    Increasing,
    Decreasing,
    NoTrend,
    OnlyOne,
}

pub struct FlowController {
    discard_ratio: Arc<AtomicU64>,
    limiter: Arc<Limiter>,
    tx: Sender<bool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for FlowController {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Err(e) = self.tx.send(true) {
            error!("send quit message for time monitor worker failed"; "err" => ?e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed"; "err" => ?e);
            return;
        }
    }
}

impl FlowController {
    pub fn new<E: Engine>(
        config: &Config,
        engine: E,
        flow_info_receiver: Option<Receiver<FlowInfo>>,
    ) -> Self {
        let limiter = Arc::new(Limiter::new(INFINITY));
        let discard_ratio = Arc::new(AtomicU64::new(0));
        let checker = FlowChecker::new(config, engine, discard_ratio.clone(), limiter.clone());
        let (tx, rx) = mpsc::channel();
        let handle = if config.disable_write_stall {
            Some(checker.start(rx, flow_info_receiver.unwrap()))
        } else {
            None
        };
        Self {
            discard_ratio,
            limiter,
            tx,
            handle,
        }
    }

    pub fn should_drop(&self) -> bool {
        let ratio = self.discard_ratio.load(Ordering::Relaxed) as f64 / RATIO_PRECISION;
        let mut rng = rand::thread_rng();
        rng.gen::<f64>() < ratio
    }

    pub fn consume(&self, bytes: usize) -> Consume {
        self.limiter.consume(bytes)
    }
}

struct Smoother<const CAP: usize> {
    records: VecDeque<(u64, Instant)>,
    total: u64,
    square_total: u64,
}

impl<const CAP: usize> Smoother<CAP> {
    pub fn new() -> Self {
        Self {
            records: VecDeque::with_capacity(CAP),
            total: 0,
            square_total: 0,
        }
    }

    pub fn observe(&mut self, record: u64) {
        if self.records.len() == CAP {
            let v = self.records.pop_front().unwrap().0;
            self.total -= v;
            self.square_total -= v * v;
        }

        self.total += record;
        self.square_total += record * record;

        self.records.push_back((record, Instant::now_coarse()));
        self.clean_timeout();
    }

    fn clean_timeout(&mut self) {
        while self.records.len() > 1 {
            if self.records.front().unwrap().1.elapsed_secs() > 300.0 {
                let v = self.records.pop_front().unwrap().0;
                self.total -= v;
                self.square_total -= v * v;
            } else {
                break;
            }
        }
    }

    pub fn get_recent(&self) -> u64 {
        if self.records.len() == 0 {
            return 0;
        }
        self.records.back().unwrap().0
    }

    pub fn get_avg(&self) -> f64 {
        if self.records.len() == 0 {
            return 0.0;
        }
        self.total as f64 / self.records.len() as f64
    }

    pub fn get_max(&mut self) -> u64 {
        if self.records.len() == 0 {
            return 0;
        }
        self.records
            .make_contiguous()
            .iter()
            .max_by_key(|(k, _)| k)
            .unwrap()
            .0
    }

    pub fn get_percentile_95(&mut self) -> u64 {
        let mut v = self.records.make_contiguous().to_vec();
        v.sort_by_key(|k| k.0);
        v[((self.records.len() - 1) as f64 * 0.90) as usize].0
    }

    // pub fn get_variance(&self) -> f64 {
    //     if self.size == 0 {
    //         return 0.0;
    //     }

    //     (self.square_total as f64 / self.size as f64 - self.get_avg().powi(2)).sqrt()
    // }

    // fn factorial(&self, n: u64) -> u64 {
    //     let mut res = 1;
    //     for i in 1..=n {
    //         res *= i;
    //     }
    //     res
    // }

    // fn binom_fact(&self, n: u64, k: u64) -> u64 {
    //     self.factorial(n) / self.factorial(k) / self.factorial(n - k)
    // }

    // fn binom_cdf(&self, x: u64, n: u64, p: f64) -> f64 {
    //     let mut cd = 0.0;
    //     for i in 0..=x {
    //         cd += self.binom_fact(n, i) as f64 * p.powi(i as i32) * (1.0 - p).powi((n - i) as i32);
    //     }
    //     cd
    // }

    pub fn slope(&self) -> f64 {
        if self.records.len() <= 1 {
            return 0.0;
        }

        let half = self.records.len() / 2;
        let mut left = 0.0;
        let mut right = 0.0;
        for (i, r) in self.records.iter().enumerate() {
            if i + 1 < half {
                left += r.0 as f64;
            } else if i + 1 > half {
                right += r.0 as f64;
            } else {
                if self.records.len() % 2 == 0 {
                    left += r.0 as f64;
                } else {
                    continue;
                }
            }
        }
        let elapsed = duration_to_sec(
            self.records
                .back()
                .unwrap()
                .1
                .duration_since(self.records.front().unwrap().1),
        );
        (right - left) / half as f64 / (elapsed / 2.0)
    }

    pub fn trend(&self) -> Trend {
        if self.records.len() == 0 {
            return Trend::NoTrend;
        } else if self.records.len() == 1 {
            return Trend::OnlyOne;
        }

        if self.records.back().unwrap().0 > self.records.front().unwrap().0 + 2 {
            Trend::Increasing
        } else if self.records.back().unwrap().0 < self.records.front().unwrap().0 - 2 {
            Trend::Decreasing
        } else {
            Trend::NoTrend
        }

        // follow the way of Cox-Stuart
        // let half = if self.size % 2 == 0 {
        //     self.size / 2
        // } else {
        //     (self.size - 1) / 2
        // };

        // use std::cmp::Ordering;
        // let mut num_pos = 0;
        // let mut num_neg = 0;
        // for i in 0..half {
        //     match self.records[(self.idx + i + half) % CAP].cmp(&self.records[(self.idx + i) % CAP])
        //     {
        //         Ordering::Greater => num_pos += 1,
        //         Ordering::Less => num_neg += 1,
        //         Ordering::Equal => {}
        //     }
        // }

        // let num = num_neg + num_pos;
        // let k = std::cmp::min(num_neg, num_pos);
        // let p_value = 2.0 * self.binom_cdf(k, num, 0.5);

        // if num_pos > num_neg && p_value < 0.05 {
        //     Trend::Increasing
        // } else if num_pos < num_neg && p_value < 0.05 {
        //     Trend::Decreasing
        // } else {
        //     Trend::NoTrend
        // }
    }
}

use engine_rocks::FlowInfo;

struct CFFlowChecker {
    last_num_memtables: Smoother<60>,
    last_num_l0_files: u64,
    last_num_l0_files_from_flush: u64,
    long_term_num_l0_files: Smoother<60>,
    long_term_pending_bytes: Smoother<60>,

    last_flush_bytes_time: Instant,
    last_flush_bytes: u64,
    short_term_flush_flow: Smoother<10>,
    last_l0_bytes: u64,
    last_l0_bytes_time: Instant,
    short_term_l0_flow: Smoother<3>,

    memtable_debt: f64,
    init_speed: bool,
}

struct FlowChecker<E: Engine> {
    pending_compaction_bytes_soft_limit: u64,
    pending_compaction_bytes_hard_limit: u64,
    memtables_threshold: u64,
    l0_files_threshold: u64,

    cf_checkers: HashMap<String, CFFlowChecker>,
    throttle_cf: Option<String>,
    target_flow: Option<f64>,
    last_target_file: u64,
    factor: f64,

    start_control_time: Instant,
    discard_ratio: Arc<AtomicU64>,

    engine: E,
    recorder: Smoother<30>,
    limiter: Arc<Limiter>,
    last_record_time: Instant,
}

impl<E: Engine> FlowChecker<E> {
    pub fn new(
        config: &Config,
        engine: E,
        discard_ratio: Arc<AtomicU64>,
        limiter: Arc<Limiter>,
    ) -> Self {
        let mut cf_checkers = map![];

        for cf in engine.kv_engine().cf_names() {
            cf_checkers.insert(
                cf.to_owned(),
                CFFlowChecker {
                    last_num_memtables: Smoother::new(),
                    long_term_pending_bytes: Smoother::new(),
                    long_term_num_l0_files: Smoother::new(),
                    last_num_l0_files: 0,
                    last_num_l0_files_from_flush: 0,
                    last_flush_bytes: 0,
                    last_flush_bytes_time: Instant::now_coarse(),
                    short_term_flush_flow: Smoother::new(),
                    last_l0_bytes: 0,
                    last_l0_bytes_time: Instant::now_coarse(),
                    short_term_l0_flow: Smoother::new(),
                    memtable_debt: 0.0,
                    init_speed: false,
                },
            );
        }
        Self {
            pending_compaction_bytes_soft_limit: config.pending_compaction_bytes_soft_limit,
            pending_compaction_bytes_hard_limit: config.pending_compaction_bytes_hard_limit,
            memtables_threshold: config.memtables_threshold,
            l0_files_threshold: config.l0_files_threshold,
            engine,
            factor: EMA_FACTOR,
            discard_ratio,
            start_control_time: Instant::now_coarse(),
            limiter,
            recorder: Smoother::new(),
            cf_checkers,
            throttle_cf: None,
            target_flow: None,
            last_target_file: 0,
            last_record_time: Instant::now_coarse(),
        }
    }

    fn start(self, rx: Receiver<bool>, flow_info_receiver: Receiver<FlowInfo>) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut checker = self;
                while rx.try_recv().is_err() {
                    match flow_info_receiver.recv_timeout(Duration::from_millis(ADJUST_DURATION)) {
                        Ok(FlowInfo::L0(cf, l0_bytes)) => {
                            checker.adjust_memtables(&cf);
                            checker.check_long_term_l0_files(cf, l0_bytes)
                        }
                        Ok(FlowInfo::Flush(cf, flush_bytes)) => {
                            checker.check_l0_flow(cf, flush_bytes)
                        }
                        Ok(FlowInfo::Compaction(cf)) => checker.adjust_pending_compaction_bytes(cf),
                        Err(RecvTimeoutError::Timeout) => {
                            // checker.tick_l0();
                            checker.update_statistics();
                        }
                        Err(e) => {
                            error!("failed to receive compaction info {:?}", e);
                        }
                    }
                }
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap()
    }

    fn update_statistics(&mut self) {
        if let Some(target_flow) = self.target_flow {
            SCHED_TARGET_FLOW_GAUGE.set(target_flow as i64);
        } else {
            SCHED_TARGET_FLOW_GAUGE.set(0);
        }
        let rate =
            self.limiter.total_bytes_consumed() as f64 / self.last_record_time.elapsed_secs();
        if self.limiter.total_bytes_consumed() != 0 {
            self.recorder.observe(rate as u64);
        }
        SCHED_WRITE_FLOW_GAUGE.set(rate as i64);
        self.last_record_time = Instant::now_coarse();
        self.limiter.reset_statistics();
    }

    fn adjust_pending_compaction_bytes(&mut self, cf: String) {
        let num = (self
            .engine
            .kv_engine()
            .get_cf_pending_compaction_bytes(&cf)
            .unwrap_or(None)
            .unwrap_or(0) as f64)
            .log2();
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker
            .long_term_pending_bytes
            .observe((num * RATIO_PRECISION) as u64);
        SCHED_PENDING_BYTES_GAUGE
            .with_label_values(&[&cf])
            .set(checker.long_term_pending_bytes.get_avg() as i64);
        let pending_compaction_bytes = checker.long_term_pending_bytes.get_avg() / RATIO_PRECISION;
        drop(checker);

        for (_, checker) in &self.cf_checkers {
            if num < (checker.long_term_pending_bytes.get_recent() as f64) / RATIO_PRECISION {
                return;
            }
        }
        let hard = (self.pending_compaction_bytes_hard_limit as f64).log2();
        let soft = (self.pending_compaction_bytes_soft_limit as f64).log2();

        let ratio = if pending_compaction_bytes < soft {
            0
        } else {
            let checker = self.cf_checkers.get_mut(&cf).unwrap();
            let kp = (pending_compaction_bytes - soft) / (hard - soft);
            let mut kd = -10.0 * checker.long_term_pending_bytes.slope();

            SCHED_KD_GAUGE.set(kd as i64);
            SCHED_KP_GAUGE.set((kp * RATIO_PRECISION) as i64);

            if kd > 0.1 {
                kd = 0.1;
            } else if kd < -0.1 {
                kd = -0.1;
            }

            let new_ratio = kp; // +kd
            let old_ratio = self.discard_ratio.load(Ordering::Relaxed);
            (if old_ratio != 0 {
                self.factor * (old_ratio as f64 / RATIO_PRECISION) + (1.0 - self.factor) * new_ratio
            } else {
                self.start_control_time = Instant::now_coarse();
                new_ratio
            } * RATIO_PRECISION) as u64
        };
        SCHED_DISCARD_RATIO_GAUGE.set(ratio as i64);
        self.discard_ratio.store(ratio, Ordering::Relaxed);
    }

    fn adjust_memtables(&mut self, cf: &String) {
        let num_memtables = self
            .engine
            .kv_engine()
            .get_cf_num_memtables(cf)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        SCHED_MEMTABLE_GAUGE
            .with_label_values(&[cf])
            .set(num_memtables as i64);
        let prev = checker.last_num_memtables.get_recent();
        checker.last_num_memtables.observe(num_memtables);
        drop(checker);

        for (_, c) in &self.cf_checkers {
            if num_memtables < c.last_num_memtables.get_recent() {
                return;
            }
        }

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        let is_throttled = self.limiter.speed_limit() != INFINITY;
        let should_throttle =
            checker.last_num_memtables.get_avg() > self.memtables_threshold as f64;
        let throttle = if !is_throttled {
            if should_throttle {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[cf, "memtable_init"])
                    .inc();
                checker.init_speed = true;
                self.recorder.get_percentile_95() as f64
            } else {
                INFINITY
            }
        } else if !should_throttle
            || checker.last_num_memtables.get_recent() < self.memtables_threshold
        {
            // should not throttle_memtable
            checker.memtable_debt = 0.0;
            if checker.init_speed {
                INFINITY
            } else {
                self.limiter.speed_limit() + checker.memtable_debt
            }
        } else {
            // should throttle
            let diff = if checker.last_num_memtables.get_recent() > prev {
                checker.memtable_debt += 1.0;
                -1.0
            } else if checker.last_num_memtables.get_recent() < prev {
                checker.memtable_debt -= 1.0;
                1.0
            } else {
                // keep, do nothing
                0.0
            };
            self.limiter.speed_limit() + diff
        };

        self.update_speed_limit(throttle);
    }

    fn check_long_term_l0_files(&mut self, cf: String, l0_bytes: u64) {
        let num_l0_files = self
            .engine
            .kv_engine()
            .get_cf_num_files_at_level(&cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker.last_l0_bytes += l0_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        checker.last_num_l0_files = num_l0_files;
        SCHED_L0_GAUGE
            .with_label_values(&[&cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[&cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);
        SCHED_THROTTLE_ACTION_COUNTER
            .with_label_values(&[&cf, "tick"])
            .inc();
        drop(checker);

        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            if &cf != throttle_cf {
                if num_l0_files > self.cf_checkers[throttle_cf].last_num_l0_files + 4 {
                    self.throttle_cf = Some(cf.clone());
                } else {
                    return;
                }
            }
        }

        self.adjust_l0_files(cf);
    }

    fn tick_l0(&mut self) {
        if self.limiter.speed_limit() != INFINITY {
            let cf = self.throttle_cf.as_ref().unwrap();
            let checker = self.cf_checkers.get_mut(cf).unwrap();
            if checker.last_num_l0_files <= self.l0_files_threshold {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "tick2"])
                    .inc();

                let throttle = if checker.long_term_num_l0_files.get_avg()
                    >= self.l0_files_threshold as f64 * 0.5
                {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep2"])
                        .inc();
                    self.limiter.speed_limit()
                } else if checker.long_term_num_l0_files.get_recent() as f64
                    >= self.l0_files_threshold as f64 * 0.5
                {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep3"])
                        .inc();
                    self.limiter.speed_limit()
                } else if checker.last_num_l0_files_from_flush >= self.l0_files_threshold {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep4"])
                        .inc();
                    self.limiter.speed_limit()
                } else {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "up"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 + LIMIT_UP_PERCENT)
                };

                self.update_speed_limit(throttle)
            }
        }
    }

    fn adjust_l0_files(&mut self, cf: String) {
        let checker = self.cf_checkers.get_mut(&cf).unwrap();

        let is_throttled = self.limiter.speed_limit() != INFINITY;
        let should_throttle = checker.last_num_l0_files > self.l0_files_threshold;

        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[&cf, "init"])
                .inc();
            self.throttle_cf = Some(cf.clone());
            self.recorder.get_percentile_95() as f64
        } else if is_throttled && should_throttle {
            match checker.long_term_num_l0_files.trend() {
                Trend::OnlyOne => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "down2"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
                }
                Trend::Increasing => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "down"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
                }
                Trend::Decreasing => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep_decr"])
                        .inc();
                    self.limiter.speed_limit()
                }
                Trend::NoTrend => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep"])
                        .inc();
                    self.limiter.speed_limit()
                }
            }
        } else if is_throttled && !should_throttle {
            if checker.long_term_num_l0_files.get_avg() >= self.l0_files_threshold as f64 * 0.5 {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "keep2"])
                    .inc();
                self.limiter.speed_limit()
            } else if checker.long_term_num_l0_files.get_recent() as f64
                >= self.l0_files_threshold as f64 * 0.5
            {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "keep3"])
                    .inc();
                self.limiter.speed_limit()
            } else if checker.last_num_l0_files_from_flush >= self.l0_files_threshold {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "keep4"])
                    .inc();
                self.limiter.speed_limit()
            } else {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "up"])
                    .inc();
                self.limiter.speed_limit() * (1.0 + LIMIT_UP_PERCENT)
            }
        } else {
            INFINITY
        };

        self.update_speed_limit(throttle)
    }

    fn update_speed_limit(&mut self, mut throttle: f64) {
        if throttle < MIN_THROTTLE_SPEED {
            throttle = MIN_THROTTLE_SPEED;
        }
        if throttle > 1.5 * self.recorder.get_max() as f64 {
            self.throttle_cf = None;
            self.target_flow = None;
            throttle = INFINITY;
        }
        SCHED_THROTTLE_FLOW_GAUGE.set(if throttle == INFINITY {
            0
        } else {
            throttle as i64
        });
        self.limiter.set_speed_limit(throttle)
    }

    fn check_l0_flow(&mut self, cf: String, flush_bytes: u64) {
        let num_l0_files = self
            .engine
            .kv_engine()
            .get_cf_num_files_at_level(&cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);

        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker.last_flush_bytes += flush_bytes;
        // no need to add it to long_term_num_l0_files, we want to exclude the
        checker.last_num_l0_files = num_l0_files;
        checker.last_num_l0_files_from_flush = num_l0_files;
        SCHED_FLUSH_L0_GAUGE
            .with_label_values(&[&cf])
            .set(num_l0_files as i64);

        if checker.last_flush_bytes_time.elapsed_secs() > 1.0 {
            let flush_flow =
                checker.last_flush_bytes as f64 / checker.last_flush_bytes_time.elapsed_secs();
            checker.short_term_flush_flow.observe(flush_flow as u64);
            SCHED_FLUSH_FLOW_GAUGE
                .with_label_values(&[&cf])
                .set(checker.short_term_flush_flow.get_avg() as i64);

            if checker.last_l0_bytes != 0 {
                let l0_flow =
                    checker.last_l0_bytes as f64 / checker.last_l0_bytes_time.elapsed_secs();
                checker.last_l0_bytes_time = Instant::now_coarse();
                checker.short_term_l0_flow.observe(l0_flow as u64);
                SCHED_L0_FLOW_GAUGE
                    .with_label_values(&[&cf])
                    .set(checker.short_term_l0_flow.get_avg() as i64);
            }

            checker.last_flush_bytes_time = Instant::now_coarse();
            checker.last_l0_bytes = 0;
            checker.last_flush_bytes = 0;

            if let Some(throttle_cf) = self.throttle_cf.as_ref() {
                if &cf != throttle_cf {
                    if num_l0_files > self.cf_checkers[throttle_cf].last_num_l0_files + 2 {
                        self.throttle_cf = Some(cf.clone());
                    } else {
                        return;
                    }
                }
            }

            if num_l0_files > self.l0_files_threshold {
                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_l0_flow.get_avg() > target_flow {
                        self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "refresh2_flow"])
                            .inc();
                    }
                }

                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_flush_flow.get_avg() > target_flow {
                        self.down_flow(cf);
                    } else if num_l0_files > self.last_target_file + 3 {
                        self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                        self.last_target_file = num_l0_files;
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "refresh1_flow"])
                            .inc();
                        self.down_flow(cf);
                    } else {
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "keep_flow"])
                            .inc();
                    }
                } else if self.cf_checkers[&cf].short_term_flush_flow.get_avg()
                    > self.cf_checkers[&cf].short_term_l0_flow.get_avg()
                {
                    self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                    self.last_target_file = num_l0_files;
                    self.down_flow(cf);
                } else {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "no_need_down_flow"])
                        .inc();
                }
            } else {
                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_l0_flow.get_avg() > target_flow {
                        self.target_flow = None;
                    }
                }
            }
        }
    }

    fn down_flow(&mut self, cf: String) {
        SCHED_THROTTLE_ACTION_COUNTER
            .with_label_values(&[&cf, "down_flow"])
            .inc();
        let throttle = if self.limiter.speed_limit() == INFINITY {
            self.throttle_cf = Some(cf.clone());
            self.recorder.get_percentile_95() as f64
        } else {
            self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
        };
        self.update_speed_limit(throttle)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_smoother() {
        let mut smoother = Smoother::<5>::new();
        smoother.observe(1);
        smoother.observe(6);
        smoother.observe(2);
        smoother.observe(3);
        smoother.observe(4);
        smoother.observe(5);
        smoother.observe(0);

        assert_eq!(smoother.get_avg(), 2.8);
        assert_eq!(smoother.get_recent(), 0);
        assert_eq!(smoother.get_max(), 5);
        assert_eq!(smoother.get_percentile_95(), 4);
    }
}
