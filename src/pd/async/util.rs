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

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::TryLockError;
use std::time::Instant;
use std::time::Duration;
use std::thread;

use futures::Poll;
use futures::Async;
use futures::Future;
use futures::future::ok;
use futures::future::{self, loop_fn, Loop};

use kvproto::pdpb::GetMembersResponse;
use kvproto::pdpb_grpc::PDAsync;
use kvproto::pdpb_grpc::PDAsyncClient;

use util::HandyRwLock;

use super::super::PdFuture;
use super::super::Result;
use super::super::Error;
use super::client::try_connect_leader;

struct Bundle {
    client: Arc<PDAsyncClient>,
    members: GetMembersResponse,
}

/// A leader client doing requests asynchronous.
pub struct LeaderClient {
    inner: Arc<RwLock<Bundle>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Bundle {
                client: Arc::new(client),
                members: members,
            })),
        }
    }

    pub fn client<Req, Resp, F>(&self, req: Req, f: F, retry: usize) -> Client<Req, Resp, F>
        where Req: Clone + 'static,
              F: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
    {
        Client {
            retry_count: retry,
            bundle: self.inner.clone(),
            req: req,
            resp: None,
            func: f,
        }
    }

    pub fn get_client(&self) -> Arc<PDAsyncClient> {
        self.inner.rl().client.clone()
    }

    pub fn set_client(&self, client: PDAsyncClient) {
        let mut bundle = self.inner.wl();
        bundle.client = Arc::new(client);
    }

    pub fn get_members(&self) -> GetMembersResponse {
        self.inner.rl().members.clone()
    }

    pub fn set_members(&self, members: GetMembersResponse) {
        let mut inner = self.inner.wl();
        inner.members = members;
    }
}

/// The context of using and updating a client.
pub struct Client<Req, Resp, F> {
    retry_count: usize,
    bundle: Arc<RwLock<Bundle>>,
    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

impl<Req, Resp, F> Client<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
{
    fn get(mut self) -> PdFuture<Client<Req, Resp, F>> {
        debug!("GetClient get remains: {}", self.retry_count);
        self.retry_count -= 1;

        let get_read = GetClientRead { inner: Some(self) };

        let ctx = get_read.map(|(mut this, client)| {
                let r = this.req.clone();
                let req = (this.func)(client, r);
                req.then(|resp| ok((this, resp)))
            })
            .flatten();

        ctx.map(|ctx| {
                let (mut this, resp) = ctx;
                match resp {
                    Ok(resp) => this.resp = Some(Ok(resp)),
                    Err(err) => {
                        error!("leader request failed: {:?}", err);
                    }
                };
                this
            })
            .boxed()
    }

    fn check(self) -> PdFuture<(Client<Req, Resp, F>, bool)> {
        if self.retry_count == 0 {
            return ok((self, true)).boxed();
        }

        if self.resp.as_ref().map_or(false, |ret| ret.is_ok()) {
            debug!("GetClient get Some(Ok(_))");
            return ok((self, true)).boxed();
        }

        // FIXME: should not block the core.
        warn!("updating PD client, block the tokio core");

        let start = Instant::now();
        // Go to sync world.
        let members = self.bundle.rl().members.clone();
        match try_connect_leader(&members) {
            Ok((client, members)) => {
                let mut bundle = self.bundle.wl();
                if members != bundle.members {
                    bundle.client = Arc::new(client);
                    bundle.members = members;
                }
                warn!("updating PD client done, spent {:?}", start.elapsed());
            }

            Err(err) => {
                warn!("updating PD client spent {:?}, err {:?}",
                      start.elapsed(),
                      err);
                // FIXME: use tokio-timer instead.
                thread::sleep(Duration::from_secs(1));
            }
        }

        ok((self, false)).boxed()
    }

    fn get_resp(self) -> Option<Result<Resp>> {
        self.resp
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let this = self;
        let get_client = |this: Self| {
            this.get()
                .and_then(|this| this.check())
                .and_then(|(this, done)| {
                    if done {
                        Ok(Loop::Break(this))
                    } else {
                        Ok(Loop::Continue(this))
                    }
                })
        };

        loop_fn(this, get_client)
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("GetClient fail to request")),
                }
            })
            .boxed()
    }
}

struct GetClientRead<Req, Resp, F> {
    inner: Option<Client<Req, Resp, F>>,
}

// TODO: impl Stream instead.
impl<Req, Resp, F> Future for GetClientRead<Req, Resp, F> {
    type Item = (Client<Req, Resp, F>, Arc<PDAsyncClient>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = self.inner.take().expect("GetClientRead cannot poll twice");
        let ret = match inner.bundle.try_read() {
            Ok(bundle) => Ok(Async::Ready(bundle.client.clone())),
            Err(TryLockError::WouldBlock) => Ok(Async::NotReady),
            // TODO: handle `PoisonError`.
            Err(err) => panic!("{:?}", err),
        };

        ret.map(|async| async.map(|client| (inner, client)))
    }
}

// TODO: GetClientWrite

/// The context of sending requets.
pub struct Request<C, Req, Resp, F> {
    retry_count: usize,
    client: Arc<C>,
    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

impl<C, Req, Resp, F> Request<C, Req, Resp, F>
    where C: PDAsync + Send + Sync + 'static,
          Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(&C, Req) -> PdFuture<Resp> + Send + 'static
{
    pub fn new(client: Arc<C>, req: Req, f: F, retry: usize) -> Request<C, Req, Resp, F> {
        Request {
            retry_count: retry,
            client: client,
            req: req,
            resp: None,
            func: f,
        }
    }

    fn send(mut self) -> PdFuture<Request<C, Req, Resp, F>> {
        debug!("Request retry remains: {}", self.retry_count);
        self.retry_count -= 1;
        let r = self.req.clone();
        let req = (self.func)(self.client.as_ref(), r);
        req.then(|resp| {
                match resp {
                    Ok(resp) => self.resp = Some(Ok(resp)),
                    Err(err) => {
                        error!("request failed: {:?}", err);
                    }
                };
                ok(self)
            })
            .boxed()
    }

    fn receive(self) -> PdFuture<(Request<C, Req, Resp, F>, bool)> {
        let done = self.retry_count == 0 || self.resp.is_some();
        ok((self, done)).boxed()
    }

    fn get_resp(self) -> Option<Result<Resp>> {
        self.resp
    }

    /// Returns a Future, it is resolves once a future returned by the closure
    /// is resolved successfully, otherwise it repeats `retry` times.
    pub fn execute(self) -> PdFuture<Resp> {
        let retry_req = self;
        loop_fn(retry_req, |retry_req| {
                retry_req.send()
                    .and_then(|retry_req| retry_req.receive())
                    .and_then(|(retry_req, done)| {
                        if done {
                            Ok(Loop::Break(retry_req))
                        } else {
                            Ok(Loop::Continue(retry_req))
                        }
                    })
            })
            .then(|req| {
                match req.unwrap().get_resp() {
                    Some(Ok(resp)) => future::ok(resp),
                    Some(Err(err)) => future::err(err),
                    None => future::err(box_err!("Request fail to request")),
                }
            })
            .boxed()
    }
}
