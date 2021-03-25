// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::{Modify, WriteData};
use crate::storage::lock_manager::LockManager;
use crate::storage::raw;
use crate::storage::raw::ttl::convert_to_expire_ts;
use crate::storage::txn::commands::{
    Command, CommandExt, ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::Result;
use crate::storage::{ProcessResult, Snapshot};
use engine_traits::CfName;
use txn_types::{Key, Value};

command! {
    /// RawCompareAndSwap checks whether the previous value of the key equals to the given value.
    /// If they are equal, write the new value. The bool indicates whether the comparison succeed.
    /// The previous value is always returned regardless of whether the new value is set.
    RawCompareAndSwap:
        cmd_ty => (Option<Value>, bool),
        display => "kv::command::raw_compare_and_swap {:?}", (ctx),
        content => {
            cf: CfName,
            key: Key,
            previous_value: Option<Value>,
            value: Value,
            ttl: Option<u64>,
        }
}

impl CommandExt for RawCompareAndSwap {
    ctx!();
    tag!(raw_compare_and_swap);
    gen_lock!(key);

    fn write_bytes(&self) -> usize {
        self.key.as_encoded().len() + self.value.len()
    }
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for RawCompareAndSwap {
    fn process_write(self, snapshot: S, _: WriteContext<'_, L>) -> Result<WriteResult> {
        let (cf, key, value, previous_value, ctx) =
            (self.cf, self.key, self.value, self.previous_value, self.ctx);
        let mut data = vec![];
        let expire_ts = self.ttl.map(convert_to_expire_ts);
        let old_value = if expire_ts.is_some() {
            raw::TTLSnapshot::from(snapshot).get_cf(cf, &key)?
        } else {
            snapshot.get_cf(cf, &key)?
        };

        let pr = if old_value == previous_value {
            let mut m = Modify::Put(cf, key, value);
            if let Some(ts) = expire_ts {
                m.with_ttl(ts);
            }
            data.push(m);
            ProcessResult::RawCompareAndSwapRes {
                previous_value: old_value,
                succeed: true,
            }
        } else {
            ProcessResult::RawCompareAndSwapRes {
                previous_value: old_value,
                succeed: false,
            }
        };
        fail_point!("txn_commands_compare_and_swap");
        let rows = data.len();
        let to_be_write = WriteData::from_modifies(data);
        Ok(WriteResult {
            ctx,
            to_be_write,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Engine, Statistics, TestEngineBuilder};
    use concurrency_manager::ConcurrencyManager;
    use engine_traits::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    #[test]
    fn test_cas_basic() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let cm = concurrency_manager::ConcurrencyManager::new(1.into());
        let key = b"k";

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v1".to_vec(),
            None,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert!(prev_val.is_none());
        assert!(succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            None,
            b"v2".to_vec(),
            None,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm.clone(), cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(!succeed);

        let cmd = RawCompareAndSwap::new(
            CF_DEFAULT,
            Key::from_encoded(key.to_vec()),
            Some(b"v1".to_vec()),
            b"v3".to_vec(),
            None,
            Context::default(),
        );
        let (prev_val, succeed) = sched_command(&engine, cm, cmd).unwrap();
        assert_eq!(prev_val, Some(b"v1".to_vec()));
        assert!(succeed);
    }

    pub fn sched_command<E: Engine>(
        engine: &E,
        cm: ConcurrencyManager,
        cmd: TypedCommand<(Option<Value>, bool)>,
    ) -> Result<(Option<Value>, bool)> {
        let snap = engine.snapshot(Default::default())?;
        use crate::storage::DummyLockManager;
        use kvproto::kvrpcpb::ExtraOp;
        let mut statistic = Statistics::default();
        let context = WriteContext {
            lock_mgr: &DummyLockManager {},
            concurrency_manager: cm,
            extra_op: ExtraOp::Noop,
            statistics: &mut statistic,
            async_apply_prewrite: false,
        };
        let ret = cmd.cmd.process_write(snap, context)?;
        match ret.pr {
            ProcessResult::RawCompareAndSwapRes {
                previous_value,
                succeed,
            } => {
                if succeed {
                    let ctx = Context::default();
                    engine.write(&ctx, ret.to_be_write).unwrap();
                }
                Ok((previous_value, succeed))
            }
            _ => unreachable!()
        }
    }
}
