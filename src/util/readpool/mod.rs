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

mod config;
mod context;
mod priority;
mod metrics;

use std::fmt;
use std::error::Error;
use std::time::Duration;
use futures::{future, Future};

use util;
use util::futurepool::{self, FuturePool};
use util::worker;
use pd;

pub use self::config::Config;
pub use self::context::Context;
pub use self::priority::Priority;

const TICK_INTERVAL_SEC: u64 = 1;

#[derive(Clone)]
pub struct ReadPool {
    pool_high: FuturePool<Context>,
    pool_normal: FuturePool<Context>,
    pool_low: FuturePool<Context>,
    max_tasks_high: usize,
    max_tasks_normal: usize,
    max_tasks_low: usize,
}

impl util::AssertSend for ReadPool {}
impl util::AssertSync for ReadPool {}

impl ReadPool {
    pub fn new(config: &Config, pd_sender: Option<worker::FutureScheduler<pd::PdTask>>) -> Self {
        let tick_interval = Duration::from_secs(TICK_INTERVAL_SEC);
        let build_context_factory = || {
            // Take a reference of `pd_sender` instead of ownership
            // so that `build_context_factory` is `fn()`.
            let pd_sender = pd_sender.clone();

            // Closure take a reference of `pd_sender` so that it is `fn()`.
            move || Context::new(pd_sender.clone())
        };
        ReadPool {
            pool_high: FuturePool::new(
                config.high_concurrency,
                config.stack_size.0 as usize,
                "readpool-high",
                tick_interval,
                build_context_factory(),
            ),
            pool_normal: FuturePool::new(
                config.normal_concurrency,
                config.stack_size.0 as usize,
                "readpool-normal",
                tick_interval,
                build_context_factory(),
            ),
            pool_low: FuturePool::new(
                config.low_concurrency,
                config.stack_size.0 as usize,
                "readpool-low",
                tick_interval,
                build_context_factory(),
            ),
            max_tasks_high: config.max_tasks_high,
            max_tasks_normal: config.max_tasks_normal,
            max_tasks_low: config.max_tasks_low,
        }
    }

    #[inline]
    fn get_pool_by_priority(&self, priority: Priority) -> &FuturePool<Context> {
        match priority {
            Priority::High => &self.pool_high,
            Priority::Normal => &self.pool_normal,
            Priority::Low => &self.pool_low,
        }
    }

    #[inline]
    fn get_max_tasks_by_priority(&self, priority: Priority) -> usize {
        match priority {
            Priority::High => self.max_tasks_high,
            Priority::Normal => self.max_tasks_normal,
            Priority::Low => self.max_tasks_low,
        }
    }

    /// Executes a future (generated by the `future_factory`) on specified future pool,
    /// returning a success future representing the produced value, or a fail future if
    /// the future pool is full.
    pub fn future_execute<F, R>(
        &self,
        priority: Priority,
        future_factory: R,
    ) -> Box<Future<Item = Result<F::Item, F::Error>, Error = Full> + Send>
    where
        R: FnOnce(futurepool::ContextDelegators<Context>) -> F + Send + 'static,
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        let pool = self.get_pool_by_priority(priority);
        let max_tasks = self.get_max_tasks_by_priority(priority);
        if pool.get_running_task_count() >= max_tasks {
            box future::err(Full {})
        } else {
            box pool.spawn(future_factory).then(future::ok)
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Full;

impl fmt::Display for Full {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "read pool is full")
    }
}

impl Error for Full {
    fn description(&self) -> &str {
        "read pool is full"
    }
}

#[cfg(test)]
mod tests {
    use std::error;
    use std::result;
    use std::fmt;
    use std::thread;
    use std::sync::mpsc::{channel, Sender};
    use futures::{future, Future};

    pub use super::*;

    type BoxError = Box<error::Error + Send + Sync>;

    pub fn expect_val<T>(v: T, x: result::Result<T, BoxError>)
    where
        T: PartialEq + fmt::Debug + 'static,
    {
        assert!(x.is_ok());
        assert_eq!(x.unwrap(), v);
    }

    pub fn expect_err<T>(desc: &str, x: result::Result<T, BoxError>) {
        assert!(x.is_err());
        match x {
            Err(e) => assert_eq!(e.description(), desc),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_future_execute() {
        let read_pool = ReadPool::new(&Config::default_for_test(), None);

        expect_val(
            vec![1, 2, 4],
            read_pool
                .future_execute(Priority::High, |_| {
                    future::ok::<Vec<u8>, BoxError>(vec![1, 2, 4])
                })
                .wait()
                .unwrap(), // unwrap Full error
        );

        expect_err(
            "foobar",
            read_pool
                .future_execute(Priority::High, |_| {
                    future::err::<(), BoxError>(box_err!("foobar"))
                })
                .wait()
                .unwrap(), // unwrap Full error
        );
    }

    fn spawn_long_time_future(
        pool: &ReadPool,
        id: u64,
        future_duration_ms: u64,
    ) -> Box<Future<Item = Result<u64, ()>, Error = Full> + Send> {
        pool.future_execute(Priority::High, move |_| {
            thread::sleep(Duration::from_millis(future_duration_ms));
            future::ok::<u64, ()>(id)
        })
    }

    fn wait_on_new_thread<F>(sender: Sender<Result<F::Item, F::Error>>, future: F)
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        thread::spawn(move || {
            let r = future.wait();
            sender.send(r).unwrap();
        });
    }

    #[test]
    fn test_full() {
        let (tx, rx) = channel();

        let read_pool = ReadPool::new(&Config {
            high_concurrency: 2,
            max_tasks_high: 4,
            ..Config::default()
        });

        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 0, 5));
        // not full
        assert_eq!(rx.recv().unwrap().unwrap(), Ok(0));

        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 1, 100));
        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 2, 200));
        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 3, 300));
        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 4, 400));
        // no available results (running = 4)
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 5, 100));
        // full
        assert!(rx.recv().unwrap().is_err());

        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 6, 100));
        // full
        assert!(rx.recv().unwrap().is_err());

        // wait a future completes (running = 3)
        assert_eq!(rx.recv().unwrap().unwrap(), Ok(1));

        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 7, 5));
        wait_on_new_thread(tx.clone(), spawn_long_time_future(&read_pool, 8, 100));
        // full
        assert!(rx.recv().unwrap().is_err());

        assert_eq!(rx.recv().unwrap().unwrap(), Ok(2));
        assert_eq!(rx.recv().unwrap().unwrap(), Ok(3));
        assert_eq!(rx.recv().unwrap().unwrap(), Ok(7));
        assert_eq!(rx.recv().unwrap().unwrap(), Ok(4));

        // no more results
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    }
}
