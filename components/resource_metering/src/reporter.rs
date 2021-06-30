// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::cpu::recorder::CpuRecords;
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use collections::HashMap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{ReportCpuTimeRequest, ResourceUsageAgentClient};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

pub struct CpuRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl CpuRecordsCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl Collector for CpuRecordsCollector {
    fn collect(&self, records: Arc<CpuRecords>) {
        self.scheduler.schedule(Task::CpuRecords(records)).ok();
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
            Task::CpuRecords(_) => {
                write!(f, "CpuRecords")?;
            }
        }

        Ok(())
    }
}

pub struct ResourceMeteringReporter {
    config: Config,
    env: Arc<Environment>,

    scheduler: Scheduler<Task>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    reporting: Arc<AtomicBool>,
    cpu_records_collector: Option<CollectorHandle>,

    // resource_tag -> ([timestamp_secs], [cpu_time_ms])
    records: HashMap<Vec<u8>, (Vec<u64>, Vec<u32>)>,
    // timestamp_secs -> cpu_time_ms
    others: HashMap<u64, u32>,

    tmp_group_map: HashMap<Vec<u8>, u32>,
    tmp_top_vec: Vec<u32>,
}

impl ResourceMeteringReporter {
    pub fn new(config: Config, scheduler: Scheduler<Task>, env: Arc<Environment>) -> Self {
        Self {
            config,
            env,
            scheduler,
            client: None,
            reporting: Arc::new(AtomicBool::new(false)),
            cpu_records_collector: None,
            records: HashMap::default(),
            others: HashMap::default(),
            tmp_group_map: HashMap::default(),
            tmp_top_vec: Vec::default(),
        }
    }

    pub fn init_client(&mut self, addr: &str) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(addr)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
        if self.cpu_records_collector.is_none() {
            self.cpu_records_collector = Some(register_collector(Box::new(
                CpuRecordsCollector::new(self.scheduler.clone()),
            )));
        }
    }
}

impl Runnable for ResourceMeteringReporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::ConfigChange(new_config) => {
                if !new_config.should_report() {
                    self.client.take();
                    self.cpu_records_collector.take();
                } else if new_config.agent_address != self.config.agent_address
                    || new_config.enabled != self.config.enabled
                {
                    self.init_client(&new_config.agent_address);
                }

                self.config = new_config;
            }
            Task::CpuRecords(records) => {
                let max_tag_count = self.config.max_resource_groups;
                let tmp_group_map = &mut self.tmp_group_map;
                let tmp_top_vec = &mut self.tmp_top_vec;
                tmp_group_map.clear();
                tmp_top_vec.clear();

                // Group CPU time by tag
                for (tag, ms) in &records.records {
                    let tag = &tag.infos.extra_attachment;
                    if tag.is_empty() {
                        continue;
                    }

                    *tmp_group_map.entry(tag.clone()).or_insert(0) += *ms as u32;
                }
                if tmp_group_map.is_empty() {
                    return;
                }

                // CPU time less than `threshold` will be filtered out
                let threshold = (tmp_group_map.len() > max_tag_count)
                    .then(|| {
                        tmp_top_vec.extend(tmp_group_map.values());
                        pdqselect::select_by(tmp_top_vec, max_tag_count, |a, b| b.cmp(a));
                        tmp_top_vec[max_tag_count]
                    })
                    .unwrap_or(0);

                let timestamp_secs = records.begin_unix_time_secs;
                let other = self.others.entry(timestamp_secs).or_insert(0);
                for (tag, ms) in tmp_group_map.drain() {
                    if ms > threshold {
                        let (ts, cpu) = self.records.entry(tag).or_insert((vec![], vec![]));
                        ts.push(timestamp_secs);
                        cpu.push(ms);
                    } else {
                        *other += ms;
                    }
                }
                if *other == 0 {
                    self.others.remove(&timestamp_secs);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.cpu_records_collector.take();
        self.client.take();
    }
}

impl RunnableWithTimer for ResourceMeteringReporter {
    fn on_timeout(&mut self) {
        if self.records.is_empty() {
            assert!(self.others.is_empty());
            return;
        }

        let records = std::mem::take(&mut self.records);
        let others = std::mem::take(&mut self.others);

        if self.reporting.load(SeqCst) {
            return;
        }

        if let Some(client) = self.client.as_ref() {
            match client.report_cpu_time_opt(CallOption::default().timeout(Duration::from_secs(2)))
            {
                Ok((mut tx, rx)) => {
                    self.reporting.store(true, SeqCst);
                    let reporting = self.reporting.clone();
                    client.spawn(async move {
                        defer!(reporting.store(false, SeqCst));

                        for (tag, (timestamp_list, cpu_time_ms_list)) in records {
                            let mut req = ReportCpuTimeRequest::default();
                            req.set_resource_group_tag(tag);
                            req.set_record_list_timestamp_sec(timestamp_list);
                            req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                            if tx.send((req, WriteFlags::default())).await.is_err() {
                                return;
                            }
                        }

                        // others
                        if !others.is_empty() {
                            let timestamp_list = others.keys().cloned().collect::<Vec<_>>();
                            let cpu_time_ms_list = others.values().cloned().collect::<Vec<_>>();
                            let mut req = ReportCpuTimeRequest::default();
                            req.set_record_list_timestamp_sec(timestamp_list);
                            req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                            if tx.send((req, WriteFlags::default())).await.is_err() {
                                return;
                            }
                        }

                        if tx.close().await.is_err() {
                            return;
                        }
                        rx.await.ok();
                    });
                }
                Err(err) => {
                    warn!("failed to connect resource usage agent"; "error" => ?err);
                }
            }
        }
    }

    fn get_interval(&self) -> Duration {
        self.config.report_agent_interval.0
    }
}
