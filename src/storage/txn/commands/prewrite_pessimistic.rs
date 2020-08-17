// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{Key, Mutation, TimeStamp};

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::MvccTxn;
use crate::storage::mvcc::{Error as MvccError, ErrorInner as MvccErrorInner};
use crate::storage::txn::commands::{
    Command, CommandExt, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{latch, Error, Result};
use crate::storage::types::PrewriteResult;
use crate::storage::{Error as StorageError, ProcessResult, Snapshot};

command! {
    /// The prewrite phase of a transaction using pessimistic locking. The first phase of 2PC.
    ///
    /// This prepares the system to commit the transaction. Later a [`Commit`](Command::Commit)
    /// or a [`Rollback`](Command::Rollback) should follow.
    PrewritePessimistic:
        cmd_ty => PrewriteResult,
        display => "kv::command::prewrite_pessimistic mutations({}) @ {} | {:?}", (mutations.len, start_ts, ctx),
        content => {
            /// The set of mutations to apply; the bool = is pessimistic lock.
            mutations: Vec<(Mutation, bool)>,
            /// The primary lock. Secondary locks (from `mutations`) will refer to the primary lock.
            primary: Vec<u8>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            lock_ttl: u64,
            for_update_ts: TimeStamp,
            /// How many keys this transaction involved.
            txn_size: u64,
            min_commit_ts: TimeStamp,
            /// All secondary keys in the whole transaction (i.e., as sent to all nodes, not only
            /// this node). Only present if using async commit.
            secondary_keys: Option<Vec<Vec<u8>>>,
            pipelined: bool,
        }
}

impl CommandExt for PrewritePessimistic {
    ctx!();
    tag!(prewrite);
    ts!(start_ts);

    fn enable_pipeline(&mut self) {
        self.pipelined = true;
    }

    fn pipelined(&self) -> bool {
        false
    }

    fn write_bytes(&self) -> usize {
        let mut bytes = 0;
        for (m, _) in &self.mutations {
            match *m {
                Mutation::Put((ref key, ref value)) | Mutation::Insert((ref key, ref value)) => {
                    bytes += key.as_encoded().len();
                    bytes += value.len();
                }
                Mutation::Delete(ref key) | Mutation::Lock(ref key) => {
                    bytes += key.as_encoded().len();
                }
                Mutation::CheckNotExists(_) => (),
            }
        }
        bytes
    }

    fn gen_lock(&self, latches: &latch::Latches) -> latch::Lock {
        let mut lock = latches.gen_lock(self.mutations.iter().map(|(m, _)| m.key()));
        lock.start_ts = self.start_ts;
        if self.pipelined {
            lock.strategy = latch::LockStrategy::TakeOver;
        }
        lock
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for PrewritePessimistic {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let rows = self.mutations.len();
        let mut txn = MvccTxn::new(
            snapshot,
            self.start_ts,
            !self.ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        // Althrough pessimistic prewrite doesn't read the write record for checking conflict, we still set extra op here
        // for getting the written keys.
        txn.extra_op = context.extra_op;

        // If async commit is disabled in TiKV, set the secondary_keys in the request to None
        // so we won't do anything for async commit.
        if !context.enable_async_commit {
            self.secondary_keys = None;
        }

        let async_commit_pk: Option<Key> = self
            .secondary_keys
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map(|_| Key::from_raw(&self.primary));

        let mut locks = vec![];
        let mut async_commit_ts = TimeStamp::zero();
        for (m, is_pessimistic_lock) in self.mutations {
            let mut secondaries = &self.secondary_keys.as_ref().map(|_| vec![]);
            if Some(m.key()) == async_commit_pk.as_ref() {
                secondaries = &self.secondary_keys;
            }

            let write_through = self.pipelined
                && is_pessimistic_lock
                && context.latches.on_same_leader(m.key(), self.ctx.get_term());
            match txn.pessimistic_prewrite(
                m,
                &self.primary,
                secondaries,
                is_pessimistic_lock,
                self.lock_ttl,
                self.for_update_ts,
                self.txn_size,
                self.min_commit_ts,
                self.pipelined,
                write_through,
            ) {
                Ok(ts) => {
                    if secondaries.is_some() {
                        async_commit_ts = ts;
                    }
                }
                e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                    locks.push(
                        e.map(|_| ())
                            .map_err(Error::from)
                            .map_err(StorageError::from),
                    );
                }
                Err(e) => return Err(Error::from(e)),
            }
        }
        context.statistics.add(&txn.take_statistics());
        let (pr, to_be_write, rows, ctx, lock_info, lock_guards) = if locks.is_empty() {
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks: vec![],
                    min_commit_ts: async_commit_ts,
                },
            };
            let txn_extra = txn.take_extra();
            // Here the lock guards are taken and will be released after the write finishes.
            // If an error occurs before, these lock guards are dropped along with `txn` automatically.
            let lock_guards = txn.take_guards();
            let write_data = WriteData::new(txn.into_modifies(), txn_extra);
            (pr, write_data, rows, self.ctx, None, lock_guards)
        } else {
            // Skip write stage if some keys are locked.
            let pr = ProcessResult::PrewriteResult {
                result: PrewriteResult {
                    locks,
                    min_commit_ts: async_commit_ts,
                },
            };
            (pr, WriteData::default(), 0, self.ctx, None, vec![])
        };
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info,
            lock_guards,
        })
    }
}
