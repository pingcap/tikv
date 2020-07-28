// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::MvccReader;
use crate::storage::txn::commands::{
    find_mvcc_infos_by_key, Command, CommandExt, ReadCommand, TypedCommand,
};
use crate::storage::txn::{ProcessResult, Result};
use crate::storage::types::MvccInfo;
use crate::storage::{ScanMode, Snapshot, Statistics};
use txn_types::{Key, TimeStamp};

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey:
        cmd_ty => MvccInfo,
        display => "kv::command::mvccbykey {:?} | {:?}", (key, ctx),
        content => {
            key: Key,
        }
}

impl CommandExt for MvccByKey {
    ctx!();
    tag!(key_mvcc);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for MvccByKey {
    fn process_read(&mut self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !self.ctx.get_not_fill_cache(),
            self.ctx.get_isolation_level(),
        );
        let result = find_mvcc_infos_by_key(&mut reader, &self.key, TimeStamp::max());
        statistics.add(reader.get_statistics());
        let (lock, writes, values) = result?;
        Ok(ProcessResult::MvccKey {
            mvcc: MvccInfo {
                lock,
                writes,
                values,
            },
        })
    }
}
