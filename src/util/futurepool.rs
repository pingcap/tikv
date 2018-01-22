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

/// This mod implemented a wrapped future pool that supports `on_tick()` which is driven by
/// tasks and is invoked no less than the specific interval. It can be extracted into the
/// `util` namespace if it is used elsewhere.

use std::fmt;
use std::cell;
use std::sync::{self, mpsc};
use std::thread;
use std::time;
use futures::Future;
use futures_cpupool as cpupool;

use util;
use util::time::Instant;
use util::collections::HashMap;

pub trait Context: fmt::Debug + Send + 'static {
    fn on_tick(&mut self) {}
}

#[derive(Debug)]
/// A wrapper to wrap context to provide `on_tick` feature.
struct ContextOuter<T: Context> {
    tick_interval: time::Duration,
    /// TODO: Can be replace by UnsafeCell to eliminate the runtime cost of borrow checking.
    inner: cell::RefCell<T>,
    last_tick: cell::Cell<Option<Instant>>,
}

impl<T: Context> ContextOuter<T> {
    fn new(context: T, tick_interval: time::Duration) -> ContextOuter<T> {
        ContextOuter {
            tick_interval,
            inner: cell::RefCell::new(context),
            last_tick: cell::Cell::new(None),
        }
    }

    fn on_task_finish(&self) {
        let now = Instant::now_coarse();
        let last_tick = self.last_tick.get();
        if last_tick.is_none() {
            // set last_tick when the first future is resolved
            self.last_tick.set(Some(now));
            return;
        }
        if now.duration_since(last_tick.unwrap()) < self.tick_interval {
            return;
        }
        self.last_tick.set(Some(now));
        self.inner.borrow_mut().on_tick();
    }
}

/// Each `ContextOuter` instance is invoked individually for each thread
/// so that we mark it as Sync.
unsafe impl<T: Context> Sync for ContextOuter<T> {}

#[derive(Debug)]
/// A future thread pool that supports `on_tick` for each thread.
pub struct FuturePool<T: Context> {
    pool: cpupool::CpuPool,
    contexts: sync::Arc<HashMap<thread::ThreadId, ContextOuter<T>>>,
}

impl<T: Context> Clone for FuturePool<T> {
    fn clone(&self) -> FuturePool<T> {
        FuturePool::<T> {
            pool: self.pool.clone(),
            contexts: sync::Arc::clone(&self.contexts),
        }
    }
}

impl<T: Context> util::AssertSend for FuturePool<T> {}
impl<T: Context> util::AssertSync for FuturePool<T> {}

impl<T: Context> FuturePool<T> {
    pub fn new<F>(
        pool_size: usize,
        stack_size: usize,
        name_prefix: &str,
        tick_interval: time::Duration,
        context_factory: F,
    ) -> FuturePool<T>
    where
        F: Send + 'static + Fn(thread::ThreadId) -> T,
    {
        let (tx, rx) = mpsc::sync_channel(pool_size);
        let pool = cpupool::Builder::new()
            .pool_size(pool_size)
            .stack_size(stack_size)
            .name_prefix(name_prefix)
            .after_start(move || {
                // We only need to know each thread's id and we can build context later
                // by invoking `context_factory` in a !Sync way.
                let thread_id = thread::current().id();
                tx.send(thread_id).unwrap();
            })
            .create();
        let contexts = rx.into_iter()
            .map(|thread_id| {
                let context = context_factory(thread_id);
                let context_outer = ContextOuter::new(context, tick_interval);
                (thread_id, context_outer)
            })
            .collect();
        FuturePool {
            pool,
            contexts: sync::Arc::new(contexts),
        }
    }

    pub fn spawn<F>(&self, f: F) -> cpupool::CpuFuture<F::Item, F::Error>
    where
        F: Future + Send + 'static,
        F::Item: Send + 'static,
        F::Error: Send + 'static,
    {
        // TODO: Support busy check
        let contexts = sync::Arc::clone(&self.contexts);
        self.pool.spawn(f.then(move |r| {
            let thread_id = thread::current().id();
            if let Some(context) = contexts.get(&thread_id) {
                context.on_task_finish();
            } else {
                unreachable!();
            }
            r
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;
    use std::sync::mpsc::{channel, Sender};
    use futures::future;

    pub use super::*;

    fn spawn_long_time_future_and_wait<T: Context>(pool: &FuturePool<T>, future_duration_ms: u64) {
        pool.spawn(future::lazy(move || {
            thread::sleep(Duration::from_millis(future_duration_ms));
            future::ok::<(), ()>(())
        })).wait()
            .unwrap();
    }

    #[test]
    fn test_tick() {
        struct MyContext {
            tx: Sender<i32>,
            sn: i32,
        }
        impl fmt::Debug for MyContext {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "MyContext")
            }
        }
        impl Context for MyContext {
            fn on_tick(&mut self) {
                self.tx.send(self.sn).unwrap();
                self.sn += 1;
            }
        }

        let (tx, rx) = channel();

        let pool = FuturePool::new(
            1,
            1024000,
            "test-pool",
            Duration::from_millis(50),
            move |_| MyContext {
                tx: tx.clone(),
                sn: 0,
            },
        );
        assert!(rx.try_recv().is_err());

        // Tick is not emitted immediately for the first future
        spawn_long_time_future_and_wait(&pool, 10);
        assert!(rx.try_recv().is_err());

        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        spawn_long_time_future_and_wait(&pool, 1);
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_long_time_future_and_wait(&pool, 100);
        assert_eq!(rx.try_recv().unwrap(), 0);
        assert!(rx.try_recv().is_err());

        // Tick is not emitted if there is no task
        thread::sleep(Duration::from_millis(100));
        assert!(rx.try_recv().is_err());

        // Tick is emitted since long enough time has passed
        spawn_long_time_future_and_wait(&pool, 1);
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert!(rx.try_recv().is_err());
    }
}
