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

pub mod config;
mod priority;

use std::error::Error;
use std::fmt;
use std::time::Duration;

use futures::Future;
use futures_cpupool::CpuFuture;

use util;
use util::futurepool::{self, FuturePool};

pub use self::config::Config;
pub use self::priority::Priority;

const TICK_INTERVAL_SEC: u64 = 1;

pub struct ReadPool<T: futurepool::Context + 'static> {
    pool_high: FuturePool<T>,
    pool_normal: FuturePool<T>,
    pool_low: FuturePool<T>,
    max_tasks_high: usize,
    max_tasks_normal: usize,
    max_tasks_low: usize,
}

impl<T: futurepool::Context + 'static> util::AssertSend for ReadPool<T> {}
impl<T: futurepool::Context + 'static> util::AssertSync for ReadPool<T> {}

impl<T: futurepool::Context + 'static> Clone for ReadPool<T> {
    fn clone(&self) -> Self {
        ReadPool {
            pool_high: self.pool_high.clone(),
            pool_normal: self.pool_normal.clone(),
            pool_low: self.pool_low.clone(),
            ..*self
        }
    }
}

impl<T: futurepool::Context + 'static> ReadPool<T> {
    // Rust does not support copying closures (RFC 2132) so that we need a closure builder.
    // TODO: Use a single closure once RFC 2132 is implemented.
    pub fn new<F, CF>(name_prefix: &str, config: &Config, context_factory_builder: F) -> Self
    where
        F: futurepool::Factory<CF>,
        CF: futurepool::Factory<T>,
    {
        let tick_interval = Duration::from_secs(TICK_INTERVAL_SEC);

        ReadPool {
            pool_high: FuturePool::new(
                config.high_concurrency,
                config.stack_size.0 as usize,
                &format!("{}-high", name_prefix),
                tick_interval,
                context_factory_builder.build(),
            ),
            pool_normal: FuturePool::new(
                config.normal_concurrency,
                config.stack_size.0 as usize,
                &format!("{}-normal", name_prefix),
                tick_interval,
                context_factory_builder.build(),
            ),
            pool_low: FuturePool::new(
                config.low_concurrency,
                config.stack_size.0 as usize,
                &format!("{}-low", name_prefix),
                tick_interval,
                context_factory_builder.build(),
            ),
            max_tasks_high: config.max_tasks_high,
            max_tasks_normal: config.max_tasks_normal,
            max_tasks_low: config.max_tasks_low,
        }
    }

    /// TODO: Remove pub.
    #[inline]
    pub fn get_pool_by_priority(&self, priority: Priority) -> &FuturePool<T> {
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
    ) -> Result<CpuFuture<F::Item, F::Error>, Full>
    where
        R: FnOnce(futurepool::ContextDelegators<T>) -> F + Send + 'static,
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        let pool = self.get_pool_by_priority(priority);
        let max_tasks = self.get_max_tasks_by_priority(priority);
        let current_tasks = pool.get_running_task_count();
        if current_tasks >= max_tasks {
            Err(Full {
                current_tasks,
                max_tasks,
            })
        } else {
            Ok(pool.spawn(future_factory))
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Full {
    pub current_tasks: usize,
    pub max_tasks: usize,
}

impl fmt::Display for Full {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(
            fmt,
            "read pool is full, current task count = {}, max task count = {}",
            self.current_tasks, self.max_tasks
        )
    }
}

impl Error for Full {
    fn description(&self) -> &str {
        "read pool is full"
    }
}

#[cfg(test)]
mod tests {
    use futures::{future, Future};
    use std::error;
    use std::fmt;
    use std::result;
    use std::sync::mpsc::{channel, Sender};
    use std::thread;

    use super::*;

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
            Err(e) => assert!(e.description().contains(desc), "{:?}", e),
            _ => unreachable!(),
        }
    }

    #[derive(Debug)]
    struct Context;

    impl futurepool::Context for Context {}

    #[test]
    fn test_future_execute() {
        let read_pool = ReadPool::new("readpool", &Config::default_for_test(), || || Context {});

        expect_val(
            vec![1, 2, 4],
            read_pool
                .future_execute(Priority::High, |_| {
                    future::ok::<Vec<u8>, BoxError>(vec![1, 2, 4])
                })
                .unwrap() // unwrap Full error
                .wait(),
        );

        expect_err(
            "foobar",
            read_pool
                .future_execute(Priority::High, |_| {
                    future::err::<(), BoxError>(box_err!("foobar"))
                })
                .unwrap() // unwrap Full error
                .wait(),
        );
    }

    fn spawn_long_time_future(
        pool: &ReadPool<Context>,
        id: u64,
        future_duration_ms: u64,
    ) -> Result<CpuFuture<u64, ()>, Full> {
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

        let read_pool = ReadPool::new(
            "readpool",
            &Config {
                high_concurrency: 2,
                max_tasks_high: 4,
                ..Config::default_for_test()
            },
            || || Context {},
        );

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 0, 5).unwrap(),
        );
        // not full
        assert_eq!(rx.recv().unwrap(), Ok(0));

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 1, 100).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 2, 200).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 3, 300).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 4, 400).unwrap(),
        );
        // no available results (running = 4)
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());

        // full
        assert!(spawn_long_time_future(&read_pool, 5, 100).is_err());

        // full
        assert!(spawn_long_time_future(&read_pool, 6, 100).is_err());

        // wait a future completes (running = 3)
        assert_eq!(rx.recv().unwrap(), Ok(1));

        // add new (running = 4)
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 7, 5).unwrap(),
        );

        // full
        assert!(spawn_long_time_future(&read_pool, 8, 100).is_err());

        assert_eq!(rx.recv().unwrap(), Ok(2));
        assert_eq!(rx.recv().unwrap(), Ok(3));
        assert_eq!(rx.recv().unwrap(), Ok(7));
        assert_eq!(rx.recv().unwrap(), Ok(4));

        // no more results
        assert!(rx.recv_timeout(Duration::from_millis(500)).is_err());
    }
}
