// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use futures03::channel::mpsc::{self, UnboundedSender};
use futures03::compat::{Sink01CompatExt, Stream01CompatExt};
use futures03::future::{self, BoxFuture};
use futures03::sink::SinkExt;
use futures03::stream::{StreamExt, TryStreamExt};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use kvproto::deadlock::*;
use security::SecurityManager;
use std::sync::Arc;
use std::time::Duration;

type DeadlockFuture<T> = BoxFuture<'static, Result<T>>;

pub type Callback = Box<dyn Fn(DeadlockResponse) + Send>;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "deadlock";

/// Builds the `Environment` of deadlock clients. All clients should use the same instance.
pub fn env() -> Arc<Environment> {
    Arc::new(
        EnvBuilder::new()
            .cq_count(CQ_COUNT)
            .name_prefix(thd_name!(CLIENT_PREFIX))
            .build(),
    )
}

#[derive(Clone)]
pub struct Client {
    addr: String,
    client: DeadlockClient,
    sender: Option<UnboundedSender<DeadlockRequest>>,
}

impl Client {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>, addr: &str) -> Self {
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));
        let channel = security_mgr.connect(cb, addr);
        let client = DeadlockClient::new(channel);
        Self {
            addr: addr.to_owned(),
            client,
            sender: None,
        }
    }

    pub fn register_detect_handler(
        &mut self,
        cb: Callback,
    ) -> (DeadlockFuture<()>, DeadlockFuture<()>) {
        let (tx, rx) = mpsc::unbounded();
        let (sink, receiver) = self.client.detect().unwrap();
        let send_task = Box::pin(async move {
            let mut sink = sink.sink_compat().sink_map_err(Error::Grpc);
            let result = sink
                .send_all(&mut rx.map(|r| Ok((r, WriteFlags::default()))))
                .await;
            match result {
                Ok(()) => {
                    info!("cancel detect sender");
                    sink.get_mut().get_mut().cancel();
                    Ok(())
                }
                Err(e) => Err(e),
            }
        });
        self.sender = Some(tx);

        let recv_task = Box::pin(async move {
            receiver
                .compat()
                .map_err(Error::Grpc)
                .for_each(move |resp| {
                    if let Ok(resp) = resp {
                        cb(resp);
                    }
                    future::ready(())
                })
                .await;
            Ok(())
        });

        (send_task, recv_task)
    }

    pub fn detect(&self, req: DeadlockRequest) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .unbounded_send(req)
            .map_err(|e| Error::Other(box_err!(e)))
    }
}
