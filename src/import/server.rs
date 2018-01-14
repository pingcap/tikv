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

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use grpc::{ChannelBuilder, EnvBuilder, Server as GrpcServer, ServerBuilder};
use kvproto::importpb_grpc::create_import_kv;

use config::TiKvConfig;
use pd::RpcClient;

use super::{ImportKVService, KVImporter};

const MAX_GRPC_MSG_LEN: usize = 32 * 1024 * 1024;

pub struct Server {
    grpc_server: GrpcServer,
}

impl Server {
    pub fn new(tikv: &TiKvConfig, client: Arc<RpcClient>) -> Server {
        let cfg = &tikv.server;
        let addr = SocketAddr::from_str(&cfg.addr).unwrap();

        let importer = KVImporter::new(&tikv.import, &tikv.rocksdb).unwrap();
        let import_service = ImportKVService::new(&tikv.import, client, Arc::new(importer));

        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(cfg.grpc_concurrency)
                .name_prefix(thd_name!("import-server"))
                .build(),
        );

        let channel_args = ChannelBuilder::new(env.clone())
            .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as usize)
            .max_concurrent_stream(cfg.grpc_concurrent_stream)
            .max_send_message_len(MAX_GRPC_MSG_LEN)
            .max_receive_message_len(MAX_GRPC_MSG_LEN)
            .build_args();

        let grpc_server = ServerBuilder::new(env.clone())
            .bind(format!("{}", addr.ip()), addr.port())
            .channel_args(channel_args)
            .register_service(create_import_kv(import_service))
            .build()
            .unwrap();

        Server { grpc_server }
    }

    pub fn start(&mut self) {
        self.grpc_server.start();
    }

    pub fn shutdown(&mut self) {
        self.grpc_server.shutdown();
    }
}
