// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use crate::config::Config;
use crate::fsm::{Fsm, FsmScheduler};
use crate::mailbox::BasicMailbox;
use crate::router::Router;
use crossbeam::channel::{self, SendError};
use file_system::{set_io_type, IOType};
use std::borrow::Cow;
use std::ops::{Deref, DerefMut};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tikv_util::mpsc;
use tikv_util::time::Instant;
use tikv_util::{debug, error, info, safe_panic, thd_name, warn};

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    // Used as a signal that scheduler should be shutdown.
    Empty,
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident, $ty:path, Fsm = $fsm:tt) => {
        pub struct $name<N, C> {
            sender: channel::Sender<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                }
            }
        }

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: Box<Self::Fsm>) {
                match self.sender.send($ty(fsm)) {
                    Ok(()) => {}
                    // TODO: use debug instead.
                    Err(SendError($ty(fsm))) => warn!("failed to schedule fsm {:p}", fsm),
                    _ => unreachable!(),
                }
            }

            fn shutdown(&self) {
                // TODO: close it explicitly once it's supported.
                // Magic number, actually any number greater than poll pool size works.
                for _ in 0..100 {
                    let _ = self.sender.send(FsmTypes::Empty);
                }
            }
        }
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

pub trait TrackedFsm: Deref + DerefMut {
    fn offset(&self) -> usize;
}

pub struct NormalFsm<N> {
    fsm: Box<N>,
    timer: Instant,
    offset: usize,
    policy: Option<ReschedulePolicy>,
}

impl<N> NormalFsm<N> {
    #[inline]
    fn new(fsm: Box<N>) -> NormalFsm<N> {
        NormalFsm {
            fsm,
            timer: Instant::now_coarse(),
            offset: 0,
            policy: None,
        }
    }
}

impl<N> Deref for NormalFsm<N> {
    type Target = N;
    #[inline]
    fn deref(&self) -> &N {
        &self.fsm
    }
}

impl<N> DerefMut for NormalFsm<N> {
    #[inline]
    fn deref_mut(&mut self) -> &mut N {
        &mut self.fsm
    }
}

impl<N> TrackedFsm for NormalFsm<N> {
    #[inline]
    fn offset(&self) -> usize {
        self.offset
    }
}

/// A basic struct for a round of polling.
#[allow(clippy::vec_box)]
pub struct Batch<N, C> {
    normals: Vec<Option<NormalFsm<N>>>,
    control: Option<Box<C>>,
}

impl<N: Fsm, C: Fsm> Batch<N, C> {
    /// Create a a batch with given batch size.
    pub fn with_capacity(cap: usize) -> Batch<N, C> {
        Batch {
            normals: Vec::with_capacity(cap),
            control: None,
        }
    }

    fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => {
                self.normals.push(Some(NormalFsm::new(n)));
            }
            FsmTypes::Control(c) => {
                assert!(self.control.is_none());
                self.control = Some(c);
            }
            FsmTypes::Empty => return false,
        }
        true
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty() && self.control.is_none()
    }

    fn clear(&mut self) {
        self.normals.clear();
        self.control.take();
    }

    /// Put back the FSM located at index.
    ///
    /// Only when channel length is larger than `checked_len` will trigger
    /// further notification. This function may fail if channel length is
    /// larger than the given value before FSM is released.
    pub fn release(&mut self, mut fsm: NormalFsm<N>, checked_len: usize) -> Option<NormalFsm<N>> {
        let mailbox = fsm.fsm.take_mailbox().unwrap();
        mailbox.release(fsm.fsm);
        if mailbox.len() != checked_len {
            match mailbox.take_fsm() {
                None => None,
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    fsm.fsm = s;
                    Some(fsm)
                }
            }
        } else {
            None
        }
    }

    /// Remove the normal FSM located at `index`.
    ///
    /// This method should only be called when the FSM is stopped.
    /// If there are still messages in channel, the FSM is untouched and
    /// the function will return false to let caller to keep polling.
    pub fn remove(&mut self, mut fsm: NormalFsm<N>) -> Option<NormalFsm<N>> {
        let mailbox = fsm.fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm.fsm);
            None
        } else {
            fsm.fsm.set_mailbox(Cow::Owned(mailbox));
            Some(fsm)
        }
    }

    pub fn schedule(&mut self, router: &BatchRouter<N, C>, index: usize, inplace: bool) {
        let taken = if inplace {
            self.normals[index].take()
        } else {
            self.normals.swap_remove(index)
        };
        let fsm = match taken {
            Some(f) => f,
            None => return,
        };
        let released = match fsm.policy {
            Some(ReschedulePolicy::Release(l)) => self.release(fsm, l),
            Some(ReschedulePolicy::Remove) => self.remove(fsm),
            Some(ReschedulePolicy::Schedule) => {
                router.normal_scheduler.schedule(fsm.fsm);
                None
            }
            None => Some(fsm),
        };
        if let Some(mut f) = released {
            if inplace {
                f.policy.take();
                self.normals[index] = Some(f);
            } else {
                let last_index = self.normals.len();
                self.normals.push(Some(f));
                self.normals.swap(index, last_index);
            }
        }
    }

    /// Same as `release`, but working on control FSM.
    pub fn release_control(&mut self, control_box: &BasicMailbox<C>, checked_len: usize) -> bool {
        let s = self.control.take().unwrap();
        control_box.release(s);
        if control_box.len() == checked_len {
            true
        } else {
            match control_box.take_fsm() {
                None => true,
                Some(s) => {
                    self.control = Some(s);
                    false
                }
            }
        }
    }

    /// Same as `remove`, but working on control FSM.
    pub fn remove_control(&mut self, control_box: &BasicMailbox<C>) {
        if control_box.is_empty() {
            let s = self.control.take().unwrap();
            control_box.release(s);
        }
    }
}

pub enum HandleResult {
    KeepProcessing,
    StopAt { progress: usize, abort: bool },
}

impl From<Option<usize>> for HandleResult {
    #[inline]
    fn from(u: Option<usize>) -> HandleResult {
        match u {
            Some(progress) => HandleResult::StopAt {
                progress,
                abort: false,
            },
            None => HandleResult::KeepProcessing,
        }
    }
}

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
pub trait PollHandler<N, C> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, batch_size: usize);

    /// This function is called when handling readiness for control FSM.
    ///
    /// If returned value is Some, then it represents a length of channel. This
    /// function will only be called for the same fsm after channel's lengh is
    /// larger than the value. If it returns None, then this function will
    /// still be called for the same FSM in the next loop unless the FSM is
    /// stopped.
    fn handle_control(&mut self, control: &mut C) -> Option<usize>;

    /// This function is called when handling readiness for normal FSM.
    ///
    /// The returned value is handled in the same way as `handle_control`.
    fn handle_normal(&mut self, normal: &mut impl TrackedFsm<Target = N>) -> HandleResult;

    fn light_end(
        &mut self,
        _batch: &mut [Option<impl TrackedFsm<Target = N>>],
        _released: &mut Vec<usize>,
    ) {
    }

    /// This function is called at the end of every round.
    fn end(&mut self, batch: &mut [Option<impl TrackedFsm<Target = N>>]);

    /// This function is called when batch system is going to sleep.
    fn pause(&mut self) {}
}

/// Internal poller that fetches batch and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    router: Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>,
    fsm_receiver: channel::Receiver<FsmTypes<N, C>>,
    handler: Handler,
    max_batch_size: usize,
    reschedule_duration: Duration,
}

enum ReschedulePolicy {
    Release(usize),
    Remove,
    Schedule,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn fetch_fsm(&mut self, batch: &mut Batch<N, C>) -> bool {
        if batch.control.is_some() {
            return true;
        }

        if let Ok(fsm) = self.fsm_receiver.try_recv() {
            return batch.push(fsm);
        }

        if batch.is_empty() {
            self.handler.pause();
            if let Ok(fsm) = self.fsm_receiver.recv() {
                return batch.push(fsm);
            }
        }
        !batch.is_empty()
    }

    // Poll for readiness and forward to handler. Remove stale peer if necessary.
    fn poll(&mut self) {
        let mut batch = Batch::with_capacity(self.max_batch_size);
        let mut reschedule_fsms = Vec::with_capacity(self.max_batch_size);
        let mut to_released = Vec::with_capacity(self.max_batch_size);

        // Fetch batch after every round is finished. It's helpful to protect regions
        // from becoming hungry if some regions are hot points. Since we fetch new fsm every time
        // calling `poll`, we do not need to configure a large value for `self.max_batch_size`.
        let mut run = true;
        while run && self.fetch_fsm(&mut batch) {
            // If there is some region wait to be deal, we must deal with it even if it has overhead
            // max size of batch. It's helpful to protect regions from becoming hungry
            // if some regions are hot points.
            let max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());
            self.handler.begin(max_batch_size);

            if batch.control.is_some() {
                let len = self.handler.handle_control(batch.control.as_mut().unwrap());
                if batch.control.as_ref().unwrap().is_stopped() {
                    batch.remove_control(&self.router.control_box);
                } else if let Some(len) = len {
                    batch.release_control(&self.router.control_box, len);
                }
            }

            let mut hot_fsm_count = 0;
            for (i, p) in batch.normals.iter_mut().enumerate() {
                let p = p.as_mut().unwrap();
                p.offset = i;
                let len = self.handler.handle_normal(p);
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(i);
                } else {
                    if p.timer.elapsed() >= self.reschedule_duration {
                        hot_fsm_count += 1;
                        // We should only reschedule a half of the hot regions, otherwise,
                        // it's possible all the hot regions are fetched in a batch the
                        // next time.
                        if hot_fsm_count % 2 == 0 {
                            p.policy = Some(ReschedulePolicy::Schedule);
                            reschedule_fsms.push(i);
                            continue;
                        }
                    }
                    if let HandleResult::StopAt { progress, abort } = len {
                        p.policy = Some(ReschedulePolicy::Release(progress));
                        reschedule_fsms.push(i);
                        if abort {
                            to_released.push(i);
                        }
                    }
                }
            }
            let mut fsm_cnt = batch.normals.len();
            while batch.normals.len() < max_batch_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    run = batch.push(fsm);
                }
                // If we receive a ControlFsm, break this cycle and call `end`. Because ControlFsm
                // may change state of the handler, we shall deal with it immediately after
                // calling `begin` of `Handler`.
                if !run || fsm_cnt >= batch.normals.len() {
                    break;
                }
                let p = batch.normals[fsm_cnt].as_mut().unwrap();
                p.offset = fsm_cnt;
                let len = self.handler.handle_normal(p);
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(fsm_cnt);
                } else if let HandleResult::StopAt { progress, abort } = len {
                    p.policy = Some(ReschedulePolicy::Release(progress));
                    reschedule_fsms.push(fsm_cnt);
                    if abort {
                        to_released.push(fsm_cnt);
                    }
                }
                fsm_cnt += 1;
            }
            self.handler.light_end(&mut batch.normals, &mut to_released);
            for offset in &to_released {
                batch.schedule(&self.router, *offset, true);
            }
            to_released.clear();
            self.handler.end(&mut batch.normals);

            // Because release use `swap_remove` internally, so using pop here
            // to remove the correct FSM.
            while let Some(r) = reschedule_fsms.pop() {
                batch.schedule(&self.router, r, false);
            }
        }
        batch.clear();
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N, C> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

/// A system that can poll FSMs concurrently and in batch.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct BatchSystem<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    router: BatchRouter<N, C>,
    receiver: channel::Receiver<FsmTypes<N, C>>,
    pool_size: usize,
    max_batch_size: usize,
    workers: Vec<JoinHandle<()>>,
    reschedule_duration: Duration,
}

impl<N, C> BatchSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &BatchRouter<N, C> {
        &self.router
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        let props = tikv_util::thread_group::current_properties();
        for i in 0..self.pool_size {
            let props = props.clone();
            let handler = builder.build();
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.receiver.clone(),
                handler,
                max_batch_size: self.max_batch_size,
                reschedule_duration: self.reschedule_duration,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i)))
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    set_io_type(IOType::ForegroundWrite);
                    poller.poll()
                })
                .unwrap();
            self.workers.push(t);
        }
        self.name_prefix = Some(name_prefix);
    }

    /// Shutdown the batch system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.name_prefix.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown batch system {}", name_prefix);
        self.router.broadcast_shutdown();
        let mut last_error = None;
        for h in self.workers.drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join worker thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join worker thread: {:?}", e);
        }
        info!("batch system {} is stopped.", name_prefix);
    }
}

pub type BatchRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    cfg: &Config,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (BatchRouter<N, C>, BatchSystem<N, C>) {
    let control_box = BasicMailbox::new(sender, controller);
    let (tx, rx) = channel::unbounded();
    let normal_scheduler = NormalScheduler { sender: tx.clone() };
    let control_scheduler = ControlScheduler { sender: tx };
    let router = Router::new(control_box, normal_scheduler, control_scheduler);
    let system = BatchSystem {
        name_prefix: None,
        router: router.clone(),
        receiver: rx,
        pool_size: cfg.pool_size,
        max_batch_size: cfg.max_batch_size(),
        reschedule_duration: cfg.reschedule_duration.0,
        workers: vec![],
    };
    (router, system)
}
