// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use futures03::{
    channel::mpsc::{
        channel, unbounded, Receiver, SendError as FuturesSendError, Sender, TrySendError,
        UnboundedReceiver, UnboundedSender,
    },
    stream, SinkExt, Stream, StreamExt,
};
use grpcio::{Result as GrpcResult, WriteFlags};
use kvproto::cdcpb::ChangeDataEvent;

use crate::service::{CdcEvent, EventBatcher};

const CDC_MSG_MAX_BATCH_SIZE: usize = 128;
// Assume the average size of event is 1KB.
// 2 = (CDC_MSG_MAX_BATCH_SIZE * 1KB / CDC_EVENT_MAX_BATCH_SIZE).ceil() + 1 /* reserve for ResolvedTs */;
pub const CDC_EVENT_MAX_BATCH_SIZE: usize = 2;

pub fn canal(buffer: usize) -> (Sink, Drain) {
    let (unbounded_sender, unbounded_receiver) = unbounded();
    let (bounded_sender, bounded_receiver) = channel(buffer);

    (
        Sink {
            unbounded_sender,
            bounded_sender,
        },
        Drain {
            unbounded_receiver,
            bounded_receiver,
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    Full,
    Disconnected,
}

impl std::error::Error for SendError {}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

macro_rules! impl_from_future_send_error {
    ($($f:ty,)+) => {
        $(
            impl From<$f> for SendError {
                fn from(e: $f) -> Self {
                    if e.is_disconnected() {
                        SendError::Disconnected
                    } else if e.is_full() {
                        Self::Full
                    } else {
                        unreachable!()
                    }
                }
            }
        )+
    };
}

impl_from_future_send_error! {
    FuturesSendError,
    TrySendError<CdcEvent>,
}

#[derive(Clone)]
pub struct Sink {
    unbounded_sender: UnboundedSender<CdcEvent>,
    bounded_sender: Sender<CdcEvent>,
}

impl Sink {
    pub fn unbounded_send(&self, event: CdcEvent) -> Result<(), SendError> {
        self.unbounded_sender
            .unbounded_send(event)
            .map_err(SendError::from)
    }

    pub fn bounded_sink(&self) -> impl futures03::Sink<CdcEvent, Error = SendError> {
        self.bounded_sender.clone().sink_map_err(SendError::from)
    }
}

pub struct Drain {
    unbounded_receiver: UnboundedReceiver<CdcEvent>,
    bounded_receiver: Receiver<CdcEvent>,
}

impl Drain {
    pub fn drain(self) -> impl Stream<Item = CdcEvent> {
        stream::select(self.bounded_receiver, self.unbounded_receiver).map(|mut event| {
            if let CdcEvent::Barrier(ref mut barrier) = event {
                if let Some(barrier) = barrier.take() {
                    // Unset barrier when it is received.
                    barrier(());
                }
            }
            event
        })
    }

    pub fn drain_grpc_message(
        self,
    ) -> impl Stream<Item = GrpcResult<(ChangeDataEvent, WriteFlags)>> {
        self.drain()
            .ready_chunks(CDC_MSG_MAX_BATCH_SIZE)
            .map(|events| {
                let mut batcher = EventBatcher::with_capacity(CDC_EVENT_MAX_BATCH_SIZE);
                events.into_iter().for_each(|e| batcher.push(e));
                let resps = batcher.build();
                let last_idx = resps.len() - 1;
                stream::iter(resps.into_iter().enumerate().map(move |(i, e)| {
                    // Buffer messages and flush them at once.
                    let write_flags = WriteFlags::default().buffer_hint(i != last_idx);
                    GrpcResult::Ok((e, write_flags))
                }))
            })
            .flatten()
    }
}

#[cfg(test)]
pub fn recv_timeout<S, I>(s: &mut S, dur: std::time::Duration) -> Result<Option<I>, ()>
where
    S: Stream<Item = I> + Unpin,
{
    use futures03::FutureExt;
    let mut timeout = futures_timer::Delay::new(dur).fuse();
    let mut s = s.fuse();
    futures03::executor::block_on(async {
        futures03::select! {
            () = timeout => Err(()),
            item = s.next() => Ok(item),
        }
    })
}
