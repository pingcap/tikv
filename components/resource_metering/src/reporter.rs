// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::cpu::recorder::CpuRecords;
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use collections::HashMap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CollectCpuTimeRequest, ResourceUsageAgentClient};
use security::SecurityManager;
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
    security_mgr: Arc<SecurityManager>,

    scheduler: Scheduler<Task>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    cpu_records_collector: Option<CollectorHandle>,

    // resource_tag -> ([timestamp_secs], [cpu_time_ms], total_cpu_time_ms)
    records: HashMap<Vec<u8>, (Vec<u64>, Vec<u32>, u32)>,

    find_top_k: Vec<u32>,
}

impl ResourceMeteringReporter {
    pub fn new(
        config: Config,
        scheduler: Scheduler<Task>,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Self {
            config,
            env,
            security_mgr,
            scheduler,
            client: None,
            cpu_records_collector: None,
            records: HashMap::default(),
            find_top_k: Vec::default(),
        }
    }

    pub fn init_reporter(&mut self, addr: &str) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            self.security_mgr.connect(cb, addr)
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
                    self.init_reporter(&new_config.agent_address);
                }

                self.config = new_config;
            }
            Task::CpuRecords(records) => {
                let timestamp_secs = records.begin_unix_time_secs;

                for (tag, ms) in &records.records {
                    let tag = &tag.infos.extra_attachment;
                    if tag.is_empty() {
                        continue;
                    }

                    let ms = *ms as u32;
                    match self.records.get_mut(tag) {
                        Some((ts, cpu_time, total)) => {
                            if *ts.last().unwrap() == timestamp_secs {
                                *cpu_time.last_mut().unwrap() += ms;
                            } else {
                                ts.push(timestamp_secs);
                                cpu_time.push(ms);
                            }
                            *total += ms;
                        }
                        None => {
                            self.records
                                .insert(tag.clone(), (vec![timestamp_secs], vec![ms], ms));
                        }
                    }
                }

                if self.records.len() > self.config.max_resource_groups {
                    self.find_top_k.clear();
                    for (_, _, total) in self.records.values() {
                        self.find_top_k.push(*total);
                    }
                    pdqselect::select_by(
                        &mut self.find_top_k,
                        self.config.max_resource_groups,
                        |a, b| b.cmp(a),
                    );
                    let kth = self.find_top_k[self.config.max_resource_groups];
                    self.records.retain(|_, (_, _, total)| *total >= kth);
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
            return;
        }

        let records = std::mem::take(&mut self.records);
        if let Some(client) = self.client.as_ref() {
            match client.collect_cpu_time_opt(CallOption::default().timeout(Duration::from_secs(2)))
            {
                Ok((mut tx, rx)) => {
                    client.spawn(async move {
                        for (tag, (timestamp_list, cpu_time_ms_list, _)) in records {
                            let mut req = CollectCpuTimeRequest::default();
                            req.set_resource_tag(tag);
                            req.set_timestamp_list(timestamp_list);
                            req.set_cpu_time_ms_list(cpu_time_ms_list);
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
        Duration::from_secs(self.config.report_agent_interval_seconds)
    }
}
