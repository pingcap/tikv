// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use collections::HashMap;
use futures::future::{self, BoxFuture, Future, FutureExt, TryFutureExt};
use futures::sink::{Sink, SinkExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::task::{AtomicWaker, Context, Poll};
use grpcio::{
    DuplexSink, Error as GrpcError, RequestStream, Result as GrpcResult, RpcContext, RpcStatus,
    RpcStatusCode, WriteFlags,
};
use kvproto::cdcpb::{
    ChangeData, ChangeDataEvent, ChangeDataRequest, Compatibility, Event, ResolvedTs,
};
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use protobuf::Message;
use tikv_util::mpsc::batch::{self, BatchReceiver, Sender as BatchSender, VecCollector};
use tikv_util::worker::*;
use tokio::sync::mpsc::channel as tokio_bounded_channel;

use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::{Deregister, Task};
use crate::rate_limiter::{new_pair, RateLimiter};
use futures::select;
use std::cell::RefCell;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::watch::Ref;

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MSG_NOTIFY_COUNT: usize = 8;
const CDC_MAX_RESP_SIZE: u32 = 6 * 1024 * 1024; // 6MB
const CDC_MSG_MAX_BATCH_SIZE: usize = 128;
// Assume the average size of event is 1KB.
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / CDC_EVENT_MAX_BATCH_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
const CDC_EVENT_MAX_BATCH_SIZE: usize = 2;

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

#[derive(Clone)]
pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
}

impl CdcEvent {
    pub fn size(&self) -> u32 {
        match self {
            CdcEvent::ResolvedTs(ref r) => r.compute_size(),
            CdcEvent::Event(ref e) => e.compute_size(),
        }
    }

    pub fn event(&self) -> &Event {
        match self {
            CdcEvent::ResolvedTs(_) => unreachable!(),
            CdcEvent::Event(ref e) => e,
        }
    }

    pub fn resolved_ts(&self) -> &ResolvedTs {
        match self {
            CdcEvent::ResolvedTs(ref r) => r,
            CdcEvent::Event(_) => unreachable!(),
        }
    }
}

impl fmt::Debug for CdcEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CdcEvent::ResolvedTs(ref r) => {
                let mut d = f.debug_struct("ResolvedTs");
                d.field("resolved ts", &r.ts);
                d.field("region count", &r.regions.len());
                d.finish()
            }
            CdcEvent::Event(e) => {
                let mut d = f.debug_struct("Event");
                d.field("region_id", &e.region_id);
                d.field("request_id", &e.request_id);
                #[cfg(not(feature = "prost-codec"))]
                if e.has_entries() {
                    d.field("entries count", &e.get_entries().get_entries().len());
                }
                #[cfg(feature = "prost-codec")]
                if e.event.is_some() {
                    use kvproto::cdcpb::event;
                    if let Some(event::Event::Entries(ref es)) = e.event.as_ref() {
                        d.field("entries count", &es.entries.len());
                    }
                }
                d.finish()
            }
        }
    }
}

struct EventBatcher {
    buffer: Vec<ChangeDataEvent>,
    last_size: u32,
}

impl EventBatcher {
    fn with_capacity(cap: usize) -> EventBatcher {
        EventBatcher {
            buffer: Vec::with_capacity(cap),
            last_size: 0,
        }
    }

    // The size of the response should not exceed CDC_MAX_RESP_SIZE.
    // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
    fn push(&mut self, event: CdcEvent) {
        let size = event.size();
        if size >= CDC_MAX_RESP_SIZE {
            warn!("cdc event too large"; "size" => size, "event" => ?event);
        }
        match event {
            CdcEvent::Event(e) => {
                if self.buffer.is_empty() || self.last_size + size >= CDC_MAX_RESP_SIZE {
                    self.last_size = 0;
                    self.buffer.push(ChangeDataEvent::default());
                }
                self.last_size += size;
                self.buffer.last_mut().unwrap().mut_events().push(e);
            }
            CdcEvent::ResolvedTs(r) => {
                let mut change_data_event = ChangeDataEvent::default();
                change_data_event.set_resolved_ts(r);
                self.buffer.push(change_data_event);

                // Make sure the next message is not batched with ResolvedTs.
                self.last_size = CDC_MAX_RESP_SIZE;
            }
        }
    }

    fn build(self) -> Vec<ChangeDataEvent> {
        self.buffer
    }
}

struct EventBatcherSink<'a, S>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = grpcio::Error> + Send + Unpin + 'a,
{
    buf: Vec<CdcEvent>,
    inner_sink: Arc<Mutex<Option<S>>>,
    flush: Option<BoxFuture<'a, Result<(), grpcio::Error>>>,
    waker: AtomicWaker,
}

impl<'a, S> EventBatcherSink<'a, S>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = grpcio::Error> + Send + Unpin + 'a,
{
    fn new(sink: S) -> Self {
        Self {
            buf: vec![],
            inner_sink: Arc::new(Mutex::new(Some(sink))),
            flush: None,
            waker: AtomicWaker::new(),
        }
    }
}

impl<'a, S> Sink<(CdcEvent, grpcio::WriteFlags)> for EventBatcherSink<'a, S>
where
    S: Sink<(ChangeDataEvent, grpcio::WriteFlags), Error = grpcio::Error> + Send + Unpin + 'a,
{
    type Error = grpcio::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let self_mut = self.get_mut();
        if self_mut.buf.len() >= 1024 {
            self_mut.waker.register(cx.waker());
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: (CdcEvent, WriteFlags)) -> Result<(), Self::Error> {
        let self_mut = self.get_mut();
        let (event, _) = item;
        self_mut.buf.push(event);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let self_mut = self.get_mut();
        if self_mut.flush.is_none() {
            if self_mut.inner_sink.lock().unwrap().is_none() {
                return Poll::Pending;
            }
            let flush_vec = std::mem::replace(&mut self_mut.buf, vec![]);
            self_mut.waker.wake();
            let mut inner_sink = self_mut.inner_sink.clone();
            // Create a flush representing the flush task.
            let fut = async move {
                let mut batcher = EventBatcher::with_capacity(128);
                flush_vec.into_iter().for_each(|event| batcher.push(event));
                let mut st =
                    stream::iter(batcher.build()).map(|event| Ok((event, WriteFlags::default())));
                let mut inner_inner_sink = inner_sink.lock().unwrap().take().unwrap();
                inner_inner_sink.send_all(&mut st).await?;
                *inner_sink.lock().unwrap() = Some(inner_inner_sink);
                Ok(())
            }
            .boxed();
            self_mut.flush = Some(fut);
        }

        self_mut.flush.as_mut().unwrap().poll_unpin(cx).map_ok(|_| {
            self_mut.flush.take();
        })
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(inner_sink) = self.inner_sink.lock().unwrap().as_mut() {
            return inner_sink.poll_close_unpin(cx);
        }
        Poll::Pending
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;
    }
}

pub struct Conn {
    id: ConnID,
    // sink: BatchSender<CdcEvent>,
    sink: RateLimiter<CdcEvent>,
    downstreams: HashMap<u64, DownstreamID>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: RateLimiter<CdcEvent>, peer: String) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
            version: None,
            peer,
        }
    }

    // TODO refactor into Error::Version.
    pub fn check_version_and_set_feature(&mut self, ver: semver::Version) -> Option<Compatibility> {
        // Assume batch resolved ts will be release in v4.0.7
        // For easy of testing (nightly CI), we lower the gate to v4.0.6
        // TODO bump the version when cherry pick to release branch.
        let v407_bacth_resoled_ts = semver::Version::new(4, 0, 6);

        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("different version on the same connection";
                        "previous version" => ?version, "version" => ?ver,
                        "downstream" => ?self.peer, "conn_id" => ?self.id);
                    Some(Compatibility {
                        required_version: version.to_string(),
                        ..Default::default()
                    })
                }
            }
            None => {
                let mut features = FeatureGate::empty();
                if v407_bacth_resoled_ts <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                info!("cdc connection version"; "version" => ver.to_string(), "features" => ?features);
                self.version = Some((ver, features));
                None
            }
        }
        // Return Err(Compatibility) when TiKV reaches the next major release,
        // so that we can remove feature gates.
    }

    pub fn get_feature(&self) -> Option<&FeatureGate> {
        self.version.as_ref().map(|(_, f)| f)
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn take_downstreams(self) -> HashMap<u64, DownstreamID> {
        self.downstreams
    }

    pub fn get_sink(&self) -> RateLimiter<CdcEvent> {
        self.sink.clone()
    }

    pub fn subscribe(&mut self, region_id: u64, downstream_id: DownstreamID) -> bool {
        match self.downstreams.entry(region_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(downstream_id);
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, region_id: u64) {
        self.downstreams.remove(&region_id);
    }

    pub fn downstream_id(&self, region_id: u64) -> Option<DownstreamID> {
        self.downstreams.get(&region_id).copied()
    }

    pub fn flush(&self) {
        /*
        if !self.sink.is_empty() {
            if let Some(notifier) = self.sink.get_notifier() {
                notifier.notify();
            }
        }
         */
        // no-op for now
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    // We are using a tokio runtime because there are some futures that require a timer,
    // and the tokio library provides a good implementation for using timers with futures.
    runtime: Arc<tokio::runtime::Runtime>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>) -> Service {
        let tokio_runtime = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .enable_time()
            .build()
            .unwrap();
        Service {
            scheduler,
            runtime: Arc::new(tokio_runtime),
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
    ) {
        // let (tx, rx) = batch::unbounded(CDC_MSG_NOTIFY_COUNT);
        let (rate_limiter, drainer) = new_pair::<CdcEvent>(128, 8192);
        let peer = ctx.peer();
        let conn = Conn::new(rate_limiter, peer.clone());
        let conn_id = conn.get_id();

        debug!("cdc streaming request accepted"; "peer" => ?peer, "conn_id" => ?conn_id);
        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc connection failed to initialize"; "error" => ?status);
            ctx.spawn(sink.fail(status).unwrap_or_else(
                |e| error!("cdc failed to failed to initialize error"; "error" => ?e),
            ));
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = stream.try_for_each(move |request| {
            let region_epoch = request.get_region_epoch().clone();
            let req_id = request.get_request_id();
            let enable_old_value = request.get_extra_op() == TxnExtraOp::ReadOldValue;
            let version = match semver::Version::parse(request.get_header().get_ticdc_version()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("empty or invalid TiCDC version, please upgrading TiCDC";
                        "version" => request.get_header().get_ticdc_version(),
                        "error" => ?e);
                    semver::Version::new(0, 0, 0)
                }
            };
            debug!("new cdc request"; "request_id" => req_id);
            let mut downstream = Downstream::new(
                peer.clone(),
                region_epoch,
                req_id,
                conn_id,
                enable_old_value,
            );

            let ret = scheduler
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                    version,
                })
                .map_err(|e| {
                    GrpcError::RpcFailure(RpcStatus::new(
                        RpcStatusCode::INVALID_ARGUMENT,
                        Some(format!("{:?}", e)),
                    ))
                });
            debug!("cdc request ready"; "request_id" => req_id);
            future::ready(ret)
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        ctx.spawn(async move {
            let res = recv_req.await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e, "conn_id" => ?conn_id);
            }
            match res {
                Ok(()) => {
                    info!("cdc receive half closed"; "downstream" => peer, "conn_id" => ?conn_id);
                }
                Err(e) => {
                    warn!("cdc receive failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();

        // We are using a tokio runtime to drive the drainer because internally the drainer periodically
        // flushes the sink, which requires a runtime that has a timer built in.
        // The executor that comes with grpcio does not have this feature.
        let drain_handle = self.runtime.spawn(async move {
            // EventBatcherSink is used to pack CdcEvents into ChangeDataEvents.
            // Internally, EventBatcherSink composes a "inverted flat map" in front of the final sink.
            let mut batched_sink = EventBatcherSink::new(&mut sink);
            // The drainer will block asynchronously, until
            // 1) all senders have exited,
            // 2) the grpc sink has been closed,
            // 3) an error has occurred in the grpc sink,
            // or 4) the sink has been forced to close due to a congestion.
            let drain_res = drainer.drain(batched_sink, WriteFlags::default().buffer_hint(false)).await;
            match drain_res {
                Ok(_) => {
                    info!("cdc drainer exit"; "downstream" => peer.clone(), "conn_id" => ?conn_id);
                },
                Err(e) => {
                    error!("cdc drainer exit"; "downstream" => peer.clone(), "conn_id" => ?conn_id, "error" => ?e);
                }
            }
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }

            info!("cdc send half closed"; "downstream" => peer.clone(), "conn_id" => ?conn_id);
            let _ = sink.close().await;
        });

        ctx.spawn(async move {
            // await the tokio runtime here
            // TODO confirm that we do need this.
            match drain_handle.await {
                Ok(_) => {
                    debug!("cdc tokio finished");
                }
                Err(e) => {
                    debug!("cdc tokio error"; "error" => ?e);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::{
        Entries as EventEntries, Event as Event_oneof_event, Row as EventRow,
    };
    use kvproto::cdcpb::{ChangeDataEvent, Event, ResolvedTs};
    #[cfg(not(feature = "prost-codec"))]
    use kvproto::cdcpb::{EventEntries, EventRow, Event_oneof_event};

    use crate::service::{CdcEvent, EventBatcher, CDC_EVENT_MAX_BATCH_SIZE, CDC_MAX_RESP_SIZE};

    #[test]
    fn test_event_batcher() {
        let check_events = |result: Vec<ChangeDataEvent>, expected: Vec<Vec<CdcEvent>>| {
            assert_eq!(result.len(), expected.len());

            for i in 0..expected.len() {
                if !result[i].has_resolved_ts() {
                    assert_eq!(result[i].events.len(), expected[i].len());
                    for j in 0..expected[i].len() {
                        assert_eq!(&result[i].events[j], expected[i][j].event());
                    }
                } else {
                    assert_eq!(expected[i].len(), 1);
                    assert_eq!(result[i].get_resolved_ts(), expected[i][0].resolved_ts());
                }
            }
        };

        let row_small = EventRow::default();
        let event_entries = EventEntries {
            entries: vec![row_small].into(),
            ..Default::default()
        };
        let event_small = Event {
            event: Some(Event_oneof_event::Entries(event_entries)),
            ..Default::default()
        };

        let mut row_big = EventRow::default();
        row_big.set_key(vec![0_u8; CDC_MAX_RESP_SIZE as usize]);
        let event_entries = EventEntries {
            entries: vec![row_big].into(),
            ..Default::default()
        };
        let event_big = Event {
            event: Some(Event_oneof_event::Entries(event_entries)),
            ..Default::default()
        };

        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.set_ts(1);

        // None empty event should not return a zero size.
        assert_ne!(CdcEvent::ResolvedTs(resolved_ts.clone()).size(), 0);
        assert_ne!(CdcEvent::Event(event_big.clone()).size(), 0);
        assert_ne!(CdcEvent::Event(event_small.clone()).size(), 0);

        // An ReslovedTs event follows a small event, they should not be batched
        // in one message.
        let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![CdcEvent::ResolvedTs(resolved_ts.clone())],
                vec![CdcEvent::Event(event_small.clone())],
            ],
        );

        // A more complex case.
        let mut batcher = EventBatcher::with_capacity(1024);
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::ResolvedTs(resolved_ts.clone()));
        batcher.push(CdcEvent::Event(event_big.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_small.clone()));
        batcher.push(CdcEvent::Event(event_big.clone()));

        check_events(
            batcher.build(),
            vec![
                vec![CdcEvent::Event(event_small.clone())],
                vec![CdcEvent::ResolvedTs(resolved_ts.clone())],
                vec![CdcEvent::ResolvedTs(resolved_ts)],
                vec![CdcEvent::Event(event_big.clone())],
                vec![CdcEvent::Event(event_small); 2],
                vec![CdcEvent::Event(event_big)],
            ],
        );
    }
}
