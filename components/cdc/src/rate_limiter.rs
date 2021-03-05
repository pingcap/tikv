// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crossbeam::channel::TrySendError;
use std::cmp::min;
use std::sync::atomic::{AtomicBool, Ordering, AtomicUsize};
use std::sync::Arc;
use std::future::Future;
use futures::select;
use futures::SinkExt;
use crossbeam::channel::{unbounded, Sender, Receiver, TryRecvError};
use tokio::sync::mpsc::{channel as async_channel, Sender as AsyncSender, Receiver as AsyncReceiver};
use crossbeam::queue::SegQueue as Queue;
use std::task::{Waker, Context, Poll};
use futures::task::AtomicWaker;
use std::pin::Pin;

#[derive(Debug)]
pub enum RateLimiterError<E> {
    SenderError(TrySendError<E>),
    SinkClosedError(usize),
    TryAgainError,
}

#[derive(Debug)]
pub enum DrainerError<E> {
    RateLimitExceededError,
}

pub struct RateLimiter<E> {
    sink: Sender<E>,
    close_tx: AsyncSender<()>,
    state: Arc<State>,
}

pub struct Drainer<E> {
    receiver: Receiver<E>,
    close_rx: AsyncReceiver<()>,
    state: Arc<State>,
}

struct State {
    is_sink_closed: AtomicBool,
    block_scan_threshold: usize,
    close_sink_threshold: usize,
    ref_count: AtomicUsize,
    wait_queue: Queue<Waker>,
    recv_task: AtomicWaker,
    #[cfg(test)]
    has_blocked: AtomicBool,
}

impl State {
    fn yield_drain(&self, cx: &mut Context<'_>) {
        self.recv_task.register(cx.waker());
    }

    fn unyield_drain(&self) {
        let _ = self.recv_task.take();
    }

    fn wake_up_one(&self) {
        match self.wait_queue.pop() {
            Ok(waker) => waker.wake(),
            Err(_) => {},
        }
    }

    fn wake_up_all(&self) {
        loop {
            match self.wait_queue.pop() {
                Ok(waker) => waker.wake(),
                Err(_) => break
            }
        }
    }
}

pub fn new_pair<E>(block_scan_threshold: usize, close_sink_threshold: usize) -> (RateLimiter<E>, Drainer<E>) {
    let (sender, receiver) = unbounded::<E>();
    let state = Arc::new(State {
        is_sink_closed: AtomicBool::new(false),
        block_scan_threshold,
        close_sink_threshold,
        ref_count: AtomicUsize::new(0),
        wait_queue: Queue::new(),
        recv_task: AtomicWaker::new(),
        #[cfg(test)]
        has_blocked: AtomicBool::new(false),
    });
    let (close_tx, close_rx) = async_channel(1);
    let rate_limiter = RateLimiter::new(sender, state.clone(), close_tx);
    let drainer = Drainer::new(receiver, state_clone, close_rx);

    (rate_limiter, drainer)
}

impl<E> RateLimiter<E> {
    fn new(
        sink: Sender<E>,
        state: Arc<State>,
        close_tx: AsyncSender<E>,
    ) -> RateLimiter<E> {
        return RateLimiter {
            sink,
            close_tx,
            state,
        };
    }

    pub fn send_realtime_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        if self.state.is_sink_closed.load(Ordering::SeqCst) {
            return Err(RateLimiterError::SinkClosedError(0));
        }

        let queue_size = self.sink.len();
        CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);
        if queue_size >= self.state.close_sink_threshold {
            warn!("cdc send_realtime_event queue length reached threshold"; "queue_size" => queue_size);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            return Err(RateLimiterError::SinkClosedError(queue_size));
        }

        self.sink.try_send(event).map_err(|e| {
            warn!("cdc send_realtime_event error"; "err" => ?e);
            self.state.is_sink_closed.store(true, Ordering::SeqCst);
            RateLimiterError::SenderError(e)
        })?;

        Ok(())
    }

    pub async fn send_scan_event(&self, event: E) -> Result<(), RateLimiterError<E>> {
        let state_clone = self.state.clone();
        let sink_clone = self.sink.clone();
        let mut attempts: u64 = 0;

        let timer = CDC_SCAN_BLOCK_DURATION_HISTOGRAM.start_coarse_timer();
        loop {
            if state_clone.is_sink_closed.load(Ordering::SeqCst) {
                return Err(RateLimiterError::SinkClosedError(0));
            }

            let queue_size = sink_clone.len();
            CDC_SINK_QUEUE_SIZE_HISTOGRAM.observe(queue_size as f64);

            if queue_size >= state_clone.block_scan_threshold {
                // used for unit testing
                #[cfg(test)]
                    self.state.has_blocked.store(true, Ordering::SeqCst);

                info!("cdc send_scan_event backoff"; "queue_size" => queue_size, "attempts" => attempts);
                let backoff_ms: u64 = 32 << min(attempts, 10);
                tokio::time::delay_for(std::time::Duration::from_millis(backoff_ms)).await;
                attempts += 1;

                continue;
            }
            break;
        }

        timer.observe_duration();
        sink_clone.try_send(event).map_err(|e| {
            warn!("cdc send_scan_event error"; "err" => ?e);
            match e {
                crossbeam::TrySendError::Disconnected(_) => {
                    state_clone.is_sink_closed.store(true, Ordering::SeqCst);
                    info!("cdc sink closed");
                }
                _ => {}
            }
            RateLimiterError::SenderError(e)
        })
    }

    pub fn notify_close(self) {
        self.state.is_sink_closed.store(true, Ordering::SeqCst);
    }
}

impl<E> Clone for RateLimiter<E> {
    fn clone(&self) -> Self {
        self.state.ref_count.fetch_add(1, Ordering::SeqCst);
        RateLimiter {
            sink: self.sink.clone(),
            close_tx: self.close_tx.clone(),
            state: self.state.clone(),
        }
    }
}

impl<E> Drop for RateLimiter<E> {
    fn drop(&mut self) {
        if self.state.ref_count.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.close_tx.send(());
        }
    }
}

impl<E> Drainer<E> {
    fn new(receiver: Receiver<E>, state: Arc<State>, close_rx: AsyncReceiver<()>) -> Drainer<E> {
        Drainer {
            receiver,
            close_rx,
            state,
        }
    }

    pub async fn drain<F: Copy, S: SinkExt<(E, F)>>(self, mut rpc_sink: S, flag: F) -> Result<(), DrainerError<E>> {
        loop {
            select! {
                next = DrainOne::new(&self.receiver, self.state.as_ref()) => {
                    match next {
                        Some(v) => {
                            rpc_sink.send((v, flag));
                            self.state.wake_up_one();
                        },
                        None => return Ok(()),
                    }
                },
                _ = self.close_rx.next() => {
                    self.state.wake_up_all();
                    return Ok(())
                }
            }
        }
    }
}

struct DrainOne<'a, E> {
    receiver: &'a Receiver<E>,
    state: &'a State,
}

impl<'a, E> DrainOne<'a, E> {
    fn wrap(receiver: &Receiver<E>, state: state::State) -> Self {
        Self {
            receiver,
            state,
        }
    }
}

impl<'a, E> Future for DrainOne<'a, E> {
    type Output = Option<E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.try_recv() {
            Ok(v) => Poll::Ready(Some(v)),
            Err(TryRecvError::Empty) => {
                self.state.yield_drain(cx);
                if !self.receiver.is_empty() || self.state.ref_count.load(Ordering::SeqCst) == 0 {
                    self.state.unyield_drain();
                }
                Poll::Pending
            }
            Err(TryRecvError::Disconnected) => {
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_util::mpsc::batch::unbounded;

    type MockCdcEvent = u64;

    #[test]
    fn test_basic_realtime() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        for i in 0..10u64 {
            rate_limiter.send_realtime_event(i)?;
        }

        for i in 0..10u64 {
            assert_eq!(rx.recv().unwrap(), i);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_scan() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        for i in 0..10u64 {
            rate_limiter.send_scan_event(i).await?;
        }

        for i in 0..10u64 {
            assert_eq!(rx.recv().unwrap(), i);
        }

        Ok(())
    }

    #[test]
    fn test_realtime_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        drop(rx);
        match rate_limiter.send_realtime_event(4) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SenderError(e)) => {}
            _ => panic!("expected SenderError"),
        }

        match rate_limiter.send_realtime_event(5) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 1024);

        rate_limiter.send_scan_event(1).await?;
        rate_limiter.send_scan_event(2).await?;
        rate_limiter.send_scan_event(3).await?;
        drop(rx);
        match rate_limiter.send_scan_event(4).await {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SenderError(e)) => {}
            _ => panic!("expected SenderError"),
        }

        match rate_limiter.send_scan_event(5).await {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[test]
    fn test_realtime_congested() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 1024, 5);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_realtime_event(3)?;
        rate_limiter.send_realtime_event(4)?;
        rate_limiter.send_realtime_event(5)?;
        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 5),
            _ => panic!("expected SinkClosedError"),
        }

        match rate_limiter.send_realtime_event(6) {
            Ok(_) => panic!("expected error"),
            Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
            _ => panic!("expected SinkClosedError"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_backoff_normal() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 5, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_scan_event(3).await?;
        rate_limiter.send_scan_event(4).await?;
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), false);

        let rate_limiter_clone = rate_limiter.clone();
        tokio::spawn(async move {
            rate_limiter_clone.send_scan_event(6).await.unwrap();
            assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), true);
        });

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        std::thread::spawn(move || {
            assert_eq!(rx.recv().unwrap(), 1);
            assert_eq!(rx.recv().unwrap(), 2);
            assert_eq!(rx.recv().unwrap(), 3);
            assert_eq!(rx.recv().unwrap(), 4);
            assert_eq!(rx.recv().unwrap(), 5);
            assert_eq!(rx.recv().unwrap(), 6);
            finished_clone.store(true, Ordering::SeqCst);
        });

        while !finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_backoff_disconnected() -> Result<(), RateLimiterError<MockCdcEvent>> {
        let (tx, rx) = unbounded::<MockCdcEvent>(1);
        let rate_limiter = RateLimiter::new(tx, 5, 1024);

        rate_limiter.send_realtime_event(1)?;
        rate_limiter.send_realtime_event(2)?;
        rate_limiter.send_scan_event(3).await?;
        rate_limiter.send_scan_event(4).await?;
        rate_limiter.send_scan_event(5).await?;
        assert_eq!(rate_limiter.state.has_blocked.load(Ordering::SeqCst), false);

        let rate_limiter_clone = rate_limiter.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();

        tokio::spawn(async move {
            match rate_limiter_clone.send_scan_event(6).await {
                Ok(_) => panic!("expected error"),
                Err(RateLimiterError::SinkClosedError(len)) => assert_eq!(len, 0),
                _ => panic!("expected SinkClosedError"),
            }
            assert_eq!(
                rate_limiter_clone.state.has_blocked.load(Ordering::SeqCst),
                true
            );
            finished_clone.store(true, Ordering::SeqCst);
        });

        tokio::time::delay_for(std::time::Duration::from_millis(200)).await;
        drop(rx);
        rate_limiter.notify_close();

        while !finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        Ok(())
    }
}
