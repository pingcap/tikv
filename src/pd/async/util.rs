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
use std::collections::HashSet;

use grpc;
use url::Url;
use rand::{self, Rng};
use futures::Poll;
use futures::Async;
use futures::Future;
use futures::future::ok;
use futures::future::{self, loop_fn, Loop};

use kvproto::pdpb::{GetMembersRequest, GetMembersResponse};
use kvproto::pdpb_grpc::PDAsync;
use kvproto::pdpb_grpc::PDAsyncClient;

use util::HandyRwLock;

use super::super::PdFuture;
use super::super::Result;
use super::super::Error;

#[derive(Debug)]
struct Bundle<C, M> {
    client: C,
    members: M,
}

/// Get a leader client asynchronous.
pub struct LeaderClient {
    inner: Arc<RwLock<Bundle<Arc<PDAsyncClient>, GetMembersResponse>>>,
}

impl LeaderClient {
    pub fn new(client: PDAsyncClient, members: GetMembersResponse) -> LeaderClient {
        LeaderClient {
            inner: Arc::new(RwLock::new(Bundle {
                client: Arc::new(client),
                members: members.clone(),
            })),
        }
    }

    pub fn client<Req, Resp, G>(&self, retry: usize, req: Req, f: G) -> GetClient<Req, Resp, G>
        where Req: Clone + 'static,
              G: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
    {
        GetClient {
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

    pub fn clone_client(&self) -> Arc<PDAsyncClient> {
        self.inner.rl().client.clone()
    }

    pub fn clone_members(&self) -> GetMembersResponse {
        self.inner.rl().members.clone()
    }

    pub fn set_members(&self, members: GetMembersResponse) {
        let mut inner = self.inner.wl();
        inner.members = members;
    }
}

pub struct GetClient<Req, Resp, F> {
    retry_count: usize,
    bundle: Arc<RwLock<Bundle<Arc<PDAsyncClient>, GetMembersResponse>>>,
    req: Req,
    resp: Option<Result<Resp>>,
    func: F,
}

impl<Req, Resp, F> GetClient<Req, Resp, F>
    where Req: Clone + Send + 'static,
          Resp: Send + 'static,
          F: FnMut(Arc<PDAsyncClient>, Req) -> PdFuture<Resp> + Send + 'static
{
    fn get(mut self) -> PdFuture<GetClient<Req, Resp, F>> {
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

    fn check(self) -> PdFuture<(GetClient<Req, Resp, F>, bool)> {
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
    pub fn reconnect(self) -> PdFuture<Resp> {
        let this = self;
        loop_fn(this, |this| {
                this.get()
                    .and_then(|this| this.check())
                    .and_then(|(this, done)| {
                        if done {
                            Ok(Loop::Break(this))
                        } else {
                            Ok(Loop::Continue(this))
                        }
                    })
            })
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
    inner: Option<GetClient<Req, Resp, F>>,
}

// TODO: impl Stream instead.
impl<Req, Resp, F> Future for GetClientRead<Req, Resp, F> {
    type Item = (GetClient<Req, Resp, F>, Arc<PDAsyncClient>);
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

/// Retry a request asynchronous.
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
    pub fn new(retry: usize, client: Arc<C>, req: Req, f: F) -> Request<C, Req, Resp, F> {
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
    pub fn retry(self) -> PdFuture<Resp> {
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

pub fn validate_endpoints(endpoints: &[String]) -> Result<(PDAsyncClient, GetMembersResponse)> {
    if endpoints.is_empty() {
        return Err(box_err!("empty PD endpoints"));
    }

    let len = endpoints.len();
    let mut endpoints_set = HashSet::with_capacity(len);

    let mut members = None;
    let mut cluster_id = None;
    for ep in endpoints {
        if !endpoints_set.insert(ep) {
            return Err(box_err!("duplicate PD endpoint {}", ep));
        }

        let (_, resp) = match connect(ep) {
            Ok(resp) => resp,
            // Ignore failed PD node.
            Err(e) => {
                error!("PD endpoint {} failed to respond: {:?}", ep, e);
                continue;
            }
        };

        // Check cluster ID.
        let cid = resp.get_header().get_cluster_id();
        if let Some(sample) = cluster_id {
            if sample != cid {
                return Err(box_err!("PD response cluster_id mismatch, want {}, got {}",
                                    sample,
                                    cid));
            }
        } else {
            cluster_id = Some(cid);
        }
        // TODO: check all fields later?

        if members.is_none() {
            members = Some(resp);
        }
    }

    match members {
        Some(members) => {
            let (client, members) = try!(try_connect_leader(&members));
            info!("All PD endpoints are consistent: {:?}", endpoints);
            Ok((client, members))
        }
        _ => Err(box_err!("PD cluster failed to respond")),
    }
}

fn connect(addr: &str) -> Result<(PDAsyncClient, GetMembersResponse)> {
    debug!("connect to PD endpoint: {:?}", addr);
    let ep = box_try!(Url::parse(addr));
    let host = ep.host_str().unwrap();
    let port = ep.port().unwrap();

    let mut conf: grpc::client::GrpcClientConf = Default::default();
    conf.http.no_delay = Some(true);

    // TODO: It seems that `new` always return an Ok(_).
    PDAsyncClient::new(host, port, false, conf)
        .and_then(|client| {
            // try request.
            match Future::wait(client.GetMembers(GetMembersRequest::new())) {
                Ok(resp) => Ok((client, resp)),
                Err(e) => Err(e),
            }
        })
        .map_err(Error::Grpc)
}

pub fn try_connect_leader(previous: &GetMembersResponse)
                          -> Result<(PDAsyncClient, GetMembersResponse)> {
    // Try to connect other members.
    // Randomize endpoints.
    let members = previous.get_members();
    let mut indexes: Vec<usize> = (0..members.len()).collect();
    rand::thread_rng().shuffle(&mut indexes);

    let mut resp = None;
    'outer: for i in indexes {
        for ep in members[i].get_client_urls() {
            match connect(ep.as_str()) {
                Ok((_, r)) => {
                    resp = Some(r);
                    break 'outer;
                }
                Err(e) => {
                    error!("failed to connect to {}, {:?}", ep, e);
                    continue;
                }
            }
        }
    }

    // Then try to connect the PD cluster leader.
    if let Some(resp) = resp {
        let leader = resp.get_leader().clone();
        for ep in leader.get_client_urls() {
            if let Ok((client, _)) = connect(ep.as_str()) {
                info!("connect to PD leader {:?}", ep);
                return Ok((client, resp));
            }
        }
    }

    Err(box_err!("failed to connect to {:?}", members))
}
