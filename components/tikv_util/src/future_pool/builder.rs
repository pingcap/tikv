// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tokio_threadpool::Builder as TokioBuilder;

use super::metrics::*;
use crate::metrics::TokioThreadBuildWrapper;

pub struct Builder {
    inner_builder: TokioBuilder,
    name_prefix: Option<String>,
    on_tick: Option<Box<dyn Fn() + Send + Sync>>,
    // bit 0 -> `before_stop`, bit 1 -> `after_start`
    fn_set_mark: i8,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            inner_builder: TokioBuilder::new(),
            name_prefix: None,
            on_tick: None,
            fn_set_mark: 0,
        }
    }

    pub fn pool_size(&mut self, val: usize) -> &mut Self {
        self.inner_builder.pool_size(val);
        self
    }

    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.inner_builder.stack_size(val);
        self
    }

    pub fn name_prefix(&mut self, val: impl Into<String>) -> &mut Self {
        let name = val.into();
        self.name_prefix = Some(name.clone());
        self.inner_builder.name_prefix(name);
        self
    }

    pub fn on_tick<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_tick = Some(Box::new(f));
        self
    }

    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.fn_set_mark |= 1;
        self.inner_builder.before_stop_wrapper(f);
        self
    }

    pub fn after_start<F>(&mut self, f: F) -> &mut Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.fn_set_mark |= 2;
        self.inner_builder.after_start_wrapper(f);
        self
    }

    pub fn build(&mut self) -> super::FuturePool {
        let name = if let Some(name) = &self.name_prefix {
            name.as_str()
        } else {
            "future_pool"
        };
        if self.fn_set_mark & 1 == 0 {
            self.inner_builder.before_stop_wrapper(|| {});
        }
        if self.fn_set_mark & 2 == 0 {
            self.inner_builder.after_start_wrapper(|| {});
        }
        let env = Arc::new(super::Env {
            on_tick: self.on_tick.take(),
            metrics_running_task_count: FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[name]),
            metrics_handled_task_count: FUTUREPOOL_HANDLED_TASK_VEC.with_label_values(&[name]),
        });
        let pool = Arc::new(self.inner_builder.build());
        super::FuturePool { pool, env }
    }
}
