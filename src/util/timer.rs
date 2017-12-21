// Copyright 2017 PingCAP, Inc.
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

use std::mem::replace;
use std::cmp::Ordering;
use std::time::Duration;
use std::collections::BinaryHeap;

use util::time::Instant;

#[derive(Debug)]
pub struct TimeoutTask<T> {
    next_tick: Instant,
    timeout: Duration,
    task: T,
}

impl<T> TimeoutTask<T> {
    pub fn into_inner(self) -> T {
        self.task
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl<T> AsRef<T> for TimeoutTask<T> {
    fn as_ref(&self) -> &T {
        &self.task
    }
}

impl<T> AsMut<T> for TimeoutTask<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.task
    }
}

impl<T> PartialEq for TimeoutTask<T> {
    fn eq(&self, other: &TimeoutTask<T>) -> bool {
        if self.next_tick == other.next_tick {
            return true;
        }
        false
    }
}

impl<T> Eq for TimeoutTask<T> {}

impl<T> PartialOrd for TimeoutTask<T> {
    fn partial_cmp(&self, other: &TimeoutTask<T>) -> Option<Ordering> {
        Some(if self.next_tick > other.next_tick {
            Ordering::Less
        } else if self.next_tick < other.next_tick {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }
}

impl<T> Ord for TimeoutTask<T> {
    fn cmp(&self, other: &TimeoutTask<T>) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct Timer<T> {
    pending: BinaryHeap<TimeoutTask<T>>,
}

impl<T> Timer<T> {
    pub fn new(capacity: usize) -> Self {
        Timer {
            pending: BinaryHeap::with_capacity(capacity),
        }
    }

    /// Add a periodic task into the `Timer`. The `timeout` will
    /// be aligned up to `tick` of the `Timer` if need.
    pub fn add_task(&mut self, timeout: Duration, task: T) {
        let task = TimeoutTask {
            next_tick: Instant::now() + timeout,
            timeout: timeout,
            task: task,
        };
        self.pending.push(task);
    }

    /// Remove a task from the timer. Returns the `TimeoutTask` if found.
    pub fn remove_task<F>(&mut self, f: F) -> Option<TimeoutTask<T>>
    where
        F: Fn(&TimeoutTask<T>) -> bool,
    {
        let mut res = None;
        let mut vec = replace(&mut self.pending, BinaryHeap::new()).into_vec();
        if let Some((idx, _)) = vec.iter().enumerate().find(|&(_, task)| f(task)) {
            res = Some(vec.swap_remove(idx));
        }
        self.pending = BinaryHeap::from(vec);
        res
    }

    /// Get the next `timeout` from the timer.
    pub fn next_timeout(&mut self) -> Option<Instant> {
        self.pending.peek().map(|task| task.next_tick)
    }

    pub fn tasks_before(&mut self, instant: Instant) -> TimerTaskBefore<T> {
        TimerTaskBefore {
            instant: instant,
            timer: self,
        }
    }
}

pub struct TimerTaskBefore<'a, T: 'a> {
    instant: Instant,
    timer: &'a mut Timer<T>,
}

impl<'a, T: 'a> TimerTaskBefore<'a, T> {
    pub fn as_timer(&mut self) -> &mut Timer<T> {
        self.timer
    }
}

impl<'a, T: 'a> Drop for TimerTaskBefore<'a, T> {
    fn drop(&mut self) {
        while let Some(_) = self.next() {}
    }
}

impl<'a, T: 'a> Iterator for TimerTaskBefore<'a, T> {
    type Item = TimeoutTask<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.timer.pending.pop() {
            Some(timeout_task) => if timeout_task.next_tick > self.instant {
                self.timer.pending.push(timeout_task);
                return None;
            } else {
                return Some(timeout_task);
            },
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{self, Sender};
    use util::worker::{BatchRunnableWithTimer, Builder as WorkerBuilder, Runnable};

    #[derive(Debug, PartialEq, Eq, Copy, Clone)]
    enum Task {
        A,
        B,
        C,
    }

    struct Runner {
        counter: usize,
        ch: Sender<&'static str>,
    }

    impl Runnable<&'static str> for Runner {
        fn run(&mut self, msg: &'static str) {
            self.ch.send(msg).unwrap();
        }
        fn shutdown(&mut self) {
            self.ch.send("").unwrap();
        }
    }

    impl BatchRunnableWithTimer<&'static str, Task> for Runner {
        fn on_timeout(&mut self, task: TimeoutTask<Task>, timer: &mut Timer<Task>) {
            match *task.as_ref() {
                Task::A => self.ch.send("task a").unwrap(),
                Task::B => self.ch.send("task b").unwrap(),
                _ => unreachable!(),
            };
            if self.counter < 2 {
                timer.add_task(task.timeout(), task.into_inner());
            }
            self.counter += 1;
        }
    }

    #[test]
    fn test_timer() {
        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(20), Task::A);
        timer.add_task(Duration::from_millis(150), Task::C);
        timer.add_task(Duration::from_millis(100), Task::B);
        assert_eq!(timer.pending.len(), 3);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(
            timer.tasks_before(tick_time).next().unwrap().into_inner(),
            Task::A
        );
        assert_eq!(timer.tasks_before(tick_time).next(), None);

        let tick_time = timer.next_timeout().unwrap();
        assert_eq!(
            timer.tasks_before(tick_time).next().unwrap().into_inner(),
            Task::B
        );
        assert_eq!(timer.tasks_before(tick_time).next(), None);
    }

    #[test]
    fn test_worker_with_timer() {
        let mut worker = WorkerBuilder::new("test-worker-with-timer").create();
        for _ in 0..10 {
            worker.schedule("normal msg").unwrap();
        }

        let (tx, rx) = mpsc::channel();
        let runner = Runner {
            counter: 0,
            ch: tx.clone(),
        };

        let mut timer = Timer::new(10);
        timer.add_task(Duration::from_millis(200), Task::A);
        timer.add_task(Duration::from_millis(300), Task::B);

        worker.start_with_timer(runner, timer).unwrap();

        for _ in 0..10 {
            let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(msg, "normal msg");
        }
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task a");
        let msg = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(msg, "task b");

        worker.stop().unwrap().join().unwrap();
    }
}
