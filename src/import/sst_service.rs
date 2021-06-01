// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use collections::HashSet;

use engine_traits::{name_to_cf, KvEngine, CF_DEFAULT, CF_WRITE};
use file_system::{set_io_type, IOType};
use futures::executor::{ThreadPool, ThreadPoolBuilder};
use futures::{TryFutureExt, TryStreamExt};
use grpcio::{ClientStreamingSink, RequestStream, RpcContext, UnarySink};
use kvproto::errorpb;

#[cfg(feature = "prost-codec")]
use kvproto::import_sstpb::write_request::*;
#[cfg(feature = "protobuf-codec")]
use kvproto::import_sstpb::WriteRequest_oneof_chunk as Chunk;
use kvproto::import_sstpb::*;

use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb::*;

use crate::server::CONFIG_ROCKSDB_GAUGE;
use engine_traits::{SstExt, SstWriterBuilder};
use raftstore::router::RaftStoreRouter;
use raftstore::store::Callback;
use sst_importer::send_rpc_response;
use tikv_util::future::create_stream_with_buffer;
use tikv_util::future::paired_future_callback;
use tikv_util::time::{Instant, Limiter};

use sst_importer::metrics::*;
use sst_importer::service::*;
use sst_importer::{error_inc, sst_meta_to_path, Config, Error, Result, SSTImporter};

/// ImportSSTService provides tikv-server with the ability to ingest SST files.
///
/// It saves the SST sent from client to a file and then sends a command to
/// raftstore to trigger the ingest process.
#[derive(Clone)]
pub struct ImportSSTService<E, Router>
where
    E: KvEngine,
{
    cfg: Config,
    engine: E,
    router: Router,
    threads: ThreadPool,
    importer: Arc<SSTImporter>,
    limiter: Limiter,
    task_slots: Arc<Mutex<HashSet<PathBuf>>>,
}

impl<E, Router> ImportSSTService<E, Router>
where
    E: KvEngine,
    Router: 'static + RaftStoreRouter<E>,
{
    pub fn new(
        cfg: Config,
        router: Router,
        engine: E,
        importer: Arc<SSTImporter>,
    ) -> ImportSSTService<E, Router> {
        let threads = ThreadPoolBuilder::new()
            .pool_size(cfg.num_threads)
            .name_prefix("sst-importer")
            .after_start(move |_| {
                tikv_alloc::add_thread_memory_accessor();
                set_io_type(IOType::Import);
            })
            .before_stop(move |_| tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();
        importer.start_switch_mode_check(&threads, engine.clone());
        ImportSSTService {
            cfg,
            engine,
            threads,
            router,
            importer,
            limiter: Limiter::new(f64::INFINITY),
            task_slots: Arc::new(Mutex::new(HashSet::default())),
        }
    }

    fn acquire_lock(task_slots: &Arc<Mutex<HashSet<PathBuf>>>, meta: &SstMeta) -> Result<bool> {
        let mut slots = task_slots.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.insert(p))
    }
    fn release_lock(task_slots: &Arc<Mutex<HashSet<PathBuf>>>, meta: &SstMeta) -> Result<bool> {
        let mut slots = task_slots.lock().unwrap();
        let p = sst_meta_to_path(meta)?;
        Ok(slots.remove(&p))
    }

    fn check_write_stall(&self) -> Option<errorpb::Error> {
        let write_stall = self
            .engine
            .ingest_maybe_slowdown_writes(CF_WRITE)
            .expect("cf");
        let default_stall = self
            .engine
            .ingest_maybe_slowdown_writes(CF_DEFAULT)
            .expect("cf");

        if self.importer.get_mode() == SwitchMode::Normal && (write_stall || default_stall) {
            let mut errorpb = errorpb::Error::default();
            let err = "too many sst files are ingesting";
            let mut server_is_busy_err = errorpb::ServerIsBusy::default();
            server_is_busy_err.set_reason(err.to_string());
            errorpb.set_message(err.to_string());
            errorpb.set_server_is_busy(server_is_busy_err);
            return Some(errorpb);
        }
        None
    }

    fn ingest_files(
        &self,
        mut context: Context,
        label: &'static str,
        ssts: Vec<SstMeta>,
    ) -> impl Future<Output = Result<IngestResponse>> {
        let mut header = RaftRequestHeader::default();
        let region_id = context.get_region_id();
        header.set_peer(context.take_peer());
        header.set_region_id(region_id);
        header.set_region_epoch(context.take_region_epoch());
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header.clone());
        cmd.set_requests(vec![req].into());
        let (cb, future) = paired_future_callback();

        let router = self.router.clone();
        let importer = self.importer.clone();
        async move {
            let mut resp = IngestResponse::default();
            if let Err(e) = router.send_command(cmd, Callback::Read(cb)) {
                resp.set_error(e.into());
                return Ok(resp);
            }

            let mut res = future.await.map_err(Error::from)?;
            fail_point!("import::sst_service::ingest");
            let mut resp_header = res.response.take_header();
            if resp_header.has_error() {
                pb_error_inc(label, resp_header.get_error());
                resp.set_error(resp_header.take_error());
                return Ok(resp);
            }

            // Make ingest command.
            let mut cmd = RaftCmdRequest::default();
            cmd.set_header(header);
            for sst in ssts.iter() {
                let mut ingest = Request::default();
                ingest.set_cmd_type(CmdType::IngestSst);
                ingest.mut_ingest_sst().set_sst(sst.clone());
                cmd.mut_requests().push(ingest);
            }
            cmd.mut_header().set_term(resp_header.get_current_term());

            // Here we shall check whether the file has been ingested before. This operation
            // must execute after geting a snapshot from raftstore to make sure that the
            // current leader has applied to current term.
            for sst in ssts.iter() {
                if !importer.exist(&sst) {
                    warn!(
                        "sst [{:?}] not exist. we may retry an operation that has already succeeded",
                        sst
                    );
                    let mut errorpb = errorpb::Error::default();
                    let err = "The file which would be ingested doest not exist.";
                    let stale_err = errorpb::StaleCommand::default();
                    errorpb.set_message(err.to_string());
                    errorpb.set_stale_command(stale_err);
                    resp.set_error(errorpb);
                    return Ok(resp);
                }
            }

            let (cb, future) = paired_future_callback();
            if let Err(e) = router.send_command(cmd, Callback::write(cb)) {
                resp.set_error(e.into());
                return Ok(resp);
            }

            let mut res = future.await.map_err(Error::from)?;
            let mut header = res.response.take_header();
            if header.has_error() {
                pb_error_inc(label, header.get_error());
                resp.set_error(header.take_error());
            }
            Ok(resp)
        }
    }
}

impl<E, Router> ImportSst for ImportSSTService<E, Router>
where
    E: KvEngine,
    Router: 'static + RaftStoreRouter<E>,
{
    fn switch_mode(
        &mut self,
        ctx: RpcContext<'_>,
        req: SwitchModeRequest,
        sink: UnarySink<SwitchModeResponse>,
    ) {
        let label = "switch_mode";
        let timer = Instant::now_coarse();

        let res = {
            fn mf(cf: &str, name: &str, v: f64) {
                CONFIG_ROCKSDB_GAUGE.with_label_values(&[cf, name]).set(v);
            }

            match req.get_mode() {
                SwitchMode::Normal => self.importer.enter_normal_mode(self.engine.clone(), mf),
                SwitchMode::Import => self.importer.enter_import_mode(self.engine.clone(), mf),
            }
        };
        match res {
            Ok(_) => info!("switch mode"; "mode" => ?req.get_mode()),
            Err(ref e) => error!(%*e; "switch mode failed"; "mode" => ?req.get_mode(),),
        }

        let task = async move {
            let res = Ok(SwitchModeResponse::default());
            send_rpc_response!(res, sink, label, timer);
        };
        ctx.spawn(task);
    }

    /// Receive SST from client and save the file for later ingesting.
    fn upload(
        &mut self,
        _ctx: RpcContext<'_>,
        stream: RequestStream<UploadRequest>,
        sink: ClientStreamingSink<UploadResponse>,
    ) {
        let label = "upload";
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let (rx, buf_driver) = create_stream_with_buffer(stream, self.cfg.stream_channel_window);
        let mut rx = rx.map_err(Error::from);

        let handle_task = async move {
            let res = async move {
                let first_chunk = rx.try_next().await?;
                let meta = match first_chunk {
                    Some(ref chunk) if chunk.has_meta() => chunk.get_meta(),
                    _ => return Err(Error::InvalidChunk),
                };
                let file = import.create(meta)?;
                let mut file = rx
                    .try_fold(file, |mut file, chunk| async move {
                        let start = Instant::now_coarse();
                        let data = chunk.get_data();
                        if data.is_empty() {
                            return Err(Error::InvalidChunk);
                        }
                        file.append(data)?;
                        IMPORT_UPLOAD_CHUNK_BYTES.observe(data.len() as f64);
                        IMPORT_UPLOAD_CHUNK_DURATION.observe(start.elapsed_secs());
                        Ok(file)
                    })
                    .await?;
                file.finish().map(|_| UploadResponse::default())
            }
            .await;
            send_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(buf_driver);
        self.threads.spawn_ok(handle_task);
    }

    /// Downloads the file and performs key-rewrite for later ingesting.
    fn download(
        &mut self,
        _ctx: RpcContext<'_>,
        req: DownloadRequest,
        sink: UnarySink<DownloadResponse>,
    ) {
        let label = "download";
        let timer = Instant::now_coarse();
        let importer = Arc::clone(&self.importer);
        let limiter = self.limiter.clone();
        let engine = self.engine.clone();
        let start = Instant::now();

        let handle_task = async move {
            // Records how long the download task waits to be scheduled.
            sst_importer::metrics::IMPORTER_DOWNLOAD_DURATION
                .with_label_values(&["queue"])
                .observe(start.elapsed().as_secs_f64());

            // SST writer must not be opened in gRPC threads, because it may be
            // blocked for a long time due to IO, especially, when encryption at rest
            // is enabled, and it leads to gRPC keepalive timeout.
            let sst_writer = <E as SstExt>::SstWriterBuilder::new()
                .set_db(&engine)
                .set_cf(name_to_cf(req.get_sst().get_cf_name()).unwrap())
                .build(importer.get_path(req.get_sst()).to_str().unwrap())
                .unwrap();

            // FIXME: download() should be an async fn, to allow BR to cancel
            // a download task.
            // Unfortunately, this currently can't happen because the S3Storage
            // is not Send + Sync. See the documentation of S3Storage for reason.
            let res = importer.download::<E>(
                req.get_sst(),
                req.get_storage_backend(),
                req.get_name(),
                req.get_rewrite_rule(),
                limiter,
                sst_writer,
            );
            let mut resp = DownloadResponse::default();
            match res {
                Ok(range) => match range {
                    Some(r) => resp.set_range(r),
                    None => resp.set_is_empty(true),
                },
                Err(e) => resp.set_error(e.into()),
            }
            let resp = Ok(resp);
            send_rpc_response!(resp, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    /// Ingest the file by sending a raft command to raftstore.
    ///
    /// If the ingestion fails because the region is not found or the epoch does
    /// not match, the remaining files will eventually be cleaned up by
    /// CleanupSSTWorker.
    fn ingest(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: IngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "ingest";
        let timer = Instant::now_coarse();

        let mut resp = IngestResponse::default();
        if let Some(errorpb) = self.check_write_stall() {
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let mut errorpb = errorpb::Error::default();
        if !Self::acquire_lock(&self.task_slots, req.get_sst()).unwrap_or(false) {
            errorpb.set_message(Error::FileConflict.to_string());
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let task_slots = self.task_slots.clone();
        let meta = req.take_sst();
        let f = self.ingest_files(req.take_context(), label, vec![meta.clone()]);
        let handle_task = async move {
            let res = f.await;
            Self::release_lock(&task_slots, &meta).unwrap();
            send_rpc_response!(res, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    /// Ingest multiple files by sending a raft command to raftstore.
    ///
    fn multi_ingest(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: MultiIngestRequest,
        sink: UnarySink<IngestResponse>,
    ) {
        let label = "multi-ingest";
        let timer = Instant::now_coarse();

        let mut resp = IngestResponse::default();
        if let Some(errorpb) = self.check_write_stall() {
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }

        let mut errorpb = errorpb::Error::default();
        let mut metas = vec![];
        for sst in req.get_ssts() {
            if Self::acquire_lock(&self.task_slots, sst).unwrap_or(false) {
                metas.push(sst.clone());
            }
        }
        if metas.len() < req.get_ssts().len() {
            for m in metas {
                Self::release_lock(&self.task_slots, &m).unwrap();
            }
            errorpb.set_message(Error::FileConflict.to_string());
            resp.set_error(errorpb);
            ctx.spawn(
                sink.success(resp)
                    .unwrap_or_else(|e| warn!("send rpc failed"; "err" => %e)),
            );
            return;
        }
        let task_slots = self.task_slots.clone();
        let f = self.ingest_files(req.take_context(), label, req.take_ssts().into());
        let handle_task = async move {
            let res = f.await;
            for m in metas {
                Self::release_lock(&task_slots, &m).unwrap();
            }
            send_rpc_response!(res, sink, label, timer);
        };
        self.threads.spawn_ok(handle_task);
    }

    fn compact(
        &mut self,
        _ctx: RpcContext<'_>,
        req: CompactRequest,
        sink: UnarySink<CompactResponse>,
    ) {
        let label = "compact";
        let timer = Instant::now_coarse();
        let engine = self.engine.clone();

        let handle_task = async move {
            let (start, end) = if !req.has_range() {
                (None, None)
            } else {
                (
                    Some(req.get_range().get_start()),
                    Some(req.get_range().get_end()),
                )
            };
            let output_level = if req.get_output_level() == -1 {
                None
            } else {
                Some(req.get_output_level())
            };

            let res = engine.compact_files_in_range(start, end, output_level);
            match res {
                Ok(_) => info!(
                    "compact files in range";
                    "start" => start.map(log_wrappers::Value::key),
                    "end" => end.map(log_wrappers::Value::key),
                    "output_level" => ?output_level, "takes" => ?timer.elapsed()
                ),
                Err(ref e) => error!(%*e;
                    "compact files in range failed";
                    "start" => start.map(log_wrappers::Value::key),
                    "end" => end.map(log_wrappers::Value::key),
                    "output_level" => ?output_level,
                ),
            }
            let res = res
                .map_err(|e| Error::Engine(box_err!(e)))
                .map(|_| CompactResponse::default());
            send_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(handle_task);
    }

    fn set_download_speed_limit(
        &mut self,
        ctx: RpcContext<'_>,
        req: SetDownloadSpeedLimitRequest,
        sink: UnarySink<SetDownloadSpeedLimitResponse>,
    ) {
        let label = "set_download_speed_limit";
        let timer = Instant::now_coarse();

        let speed_limit = req.get_speed_limit();
        self.limiter.set_speed_limit(if speed_limit > 0 {
            speed_limit as f64
        } else {
            f64::INFINITY
        });

        let ctx_task = async move {
            let res = Ok(SetDownloadSpeedLimitResponse::default());
            send_rpc_response!(res, sink, label, timer);
        };

        ctx.spawn(ctx_task);
    }

    fn write(
        &mut self,
        _ctx: RpcContext<'_>,
        stream: RequestStream<WriteRequest>,
        sink: ClientStreamingSink<WriteResponse>,
    ) {
        let label = "write";
        let timer = Instant::now_coarse();
        let import = self.importer.clone();
        let engine = self.engine.clone();
        let (rx, buf_driver) = create_stream_with_buffer(stream, self.cfg.stream_channel_window);
        let mut rx = rx.map_err(Error::from);

        let handle_task = async move {
            let res = async move {
                let first_req = rx.try_next().await?;
                let meta = match first_req {
                    Some(r) => match r.chunk {
                        Some(Chunk::Meta(m)) => m,
                        _ => return Err(Error::InvalidChunk),
                    },
                    _ => return Err(Error::InvalidChunk),
                };

                let writer = match import.new_writer::<E>(&engine, meta) {
                    Ok(w) => w,
                    Err(e) => {
                        error!("build writer failed {:?}", e);
                        return Err(Error::InvalidChunk);
                    }
                };
                let writer = rx
                    .try_fold(writer, |mut writer, req| async move {
                        let start = Instant::now_coarse();
                        let batch = match req.chunk {
                            Some(Chunk::Batch(b)) => b,
                            _ => return Err(Error::InvalidChunk),
                        };
                        writer.write(batch)?;
                        IMPORT_WRITE_CHUNK_DURATION.observe(start.elapsed_secs());
                        Ok(writer)
                    })
                    .await?;

                writer.finish().map(|metas| {
                    let mut resp = WriteResponse::default();
                    resp.set_metas(metas.into());
                    resp
                })
            }
            .await;
            send_rpc_response!(res, sink, label, timer);
        };

        self.threads.spawn_ok(buf_driver);
        self.threads.spawn_ok(handle_task);
    }
}

// add error statistics from pb error response
fn pb_error_inc(type_: &str, e: &errorpb::Error) {
    let label = if e.has_not_leader() {
        "not_leader"
    } else if e.has_store_not_match() {
        "store_not_match"
    } else if e.has_region_not_found() {
        "region_not_found"
    } else if e.has_key_not_in_region() {
        "key_not_in_range"
    } else if e.has_epoch_not_match() {
        "epoch_not_match"
    } else if e.has_server_is_busy() {
        "server_is_busy"
    } else if e.has_stale_command() {
        "stale_command"
    } else if e.has_raft_entry_too_large() {
        "raft_entry_too_large"
    } else {
        "unknown"
    };

    IMPORTER_ERROR_VEC.with_label_values(&[type_, label]).inc();
}
