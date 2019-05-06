// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;

use criterion::black_box;

use protobuf::RepeatedField;

use kvproto::coprocessor::KeyRange;
use tipb::executor::Executor as PbExecutor;
use tipb::executor::{ExecType, TableScan};
use tipb::schema::ColumnInfo;

use test_coprocessor::*;
use tikv::coprocessor::dag::batch::executors::BatchTableScanExecutor;
use tikv::coprocessor::dag::batch::interface::*;
use tikv::coprocessor::dag::exec_summary::ExecSummaryCollectorDisabled;
use tikv::coprocessor::dag::executor::Executor;
use tikv::coprocessor::dag::executor::TableScanExecutor;
use tikv::coprocessor::dag::expr::EvalConfig;
use tikv::storage::{RocksEngine, Store as TxnStore};

use crate::util::bencher::Bencher;
use crate::util::store::StoreDescriber;

fn create_table_scan_executor<TargetTxnStore: TxnStore>(
    columns: &[ColumnInfo],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> TableScanExecutor<ExecSummaryCollectorDisabled, TargetTxnStore> {
    let mut req = TableScan::new();
    req.set_columns(RepeatedField::from_slice(columns));

    let mut executor = TableScanExecutor::table_scan(
        ExecSummaryCollectorDisabled,
        black_box(req),
        black_box(ranges.to_vec()),
        black_box(ToTxnStore::<TargetTxnStore>::to_store(store)),
        false,
    )
    .unwrap();
    // There is a step of building scanner in the first `next()` which cost time,
    // so we next() before hand.
    executor.next().unwrap().unwrap();
    executor
}

fn create_batch_table_scan_executor<TargetTxnStore: TxnStore>(
    columns: &[ColumnInfo],
    ranges: &[KeyRange],
    store: &Store<RocksEngine>,
) -> BatchTableScanExecutor<ExecSummaryCollectorDisabled, TargetTxnStore> {
    let mut executor = BatchTableScanExecutor::new(
        ExecSummaryCollectorDisabled,
        black_box(ToTxnStore::<TargetTxnStore>::to_store(store)),
        black_box(Arc::new(EvalConfig::default())),
        black_box(columns.to_vec()),
        black_box(ranges.to_vec()),
        black_box(false),
    )
    .unwrap();
    // There is a step of building scanner in the first `next()` which cost time,
    // so we next() before hand.
    executor.next_batch(1);
    executor
}

pub trait TableScanBencher {
    fn name(&self) -> String;

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    );

    fn box_clone(&self) -> Box<dyn TableScanBencher>;
}

impl Clone for Box<dyn TableScanBencher> {
    #[inline]
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

pub struct NormalTableScanNext1Bencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> NormalTableScanNext1Bencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> TableScanBencher for NormalTableScanNext1Bencher<T> {
    fn name(&self) -> String {
        format!("{}/normal/next=1", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::NormalNext1Bencher::new(|| {
            create_table_scan_executor::<T>(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self::new())
    }
}

pub struct NormalTableScanNext1024Bencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> NormalTableScanNext1024Bencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> TableScanBencher for NormalTableScanNext1024Bencher<T> {
    fn name(&self) -> String {
        format!("{}/normal/next=1024", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::NormalNext1024Bencher::new(|| {
            create_table_scan_executor::<T>(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self::new())
    }
}

pub struct BatchTableScanNext1024Bencher<T: TxnStore + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> BatchTableScanNext1024Bencher<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> TableScanBencher for BatchTableScanNext1024Bencher<T> {
    fn name(&self) -> String {
        format!("{}/batch/next=1024", <T as StoreDescriber>::name())
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::BatchNext1024Bencher::new(|| {
            create_batch_table_scan_executor::<T>(columns, ranges, store)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self::new())
    }
}

pub struct TableScanDAGBencher<T: TxnStore + 'static> {
    batch: bool,
    display_table_rows: usize,
    _phantom: PhantomData<T>,
}

impl<T: TxnStore + 'static> TableScanDAGBencher<T> {
    pub fn new(batch: bool, display_table_rows: usize) -> Self {
        Self {
            batch,
            display_table_rows,
            _phantom: PhantomData,
        }
    }
}

impl<T: TxnStore + 'static> TableScanBencher for TableScanDAGBencher<T> {
    fn name(&self) -> String {
        let tag = if self.batch { "batch" } else { "normal" };
        format!(
            "{}/{}/with_dag/rows={}",
            <T as StoreDescriber>::name(),
            tag,
            self.display_table_rows
        )
    }

    fn bench(
        &self,
        b: &mut criterion::Bencher,
        columns: &[ColumnInfo],
        ranges: &[KeyRange],
        store: &Store<RocksEngine>,
    ) {
        crate::util::bencher::DAGHandleBencher::new(|| {
            let mut exec = PbExecutor::new();
            exec.set_tp(ExecType::TypeTableScan);
            exec.mut_tbl_scan()
                .set_columns(RepeatedField::from_slice(columns));
            crate::util::build_dag_handler::<T>(&[exec], ranges, store, self.batch)
        })
        .bench(b);
    }

    fn box_clone(&self) -> Box<dyn TableScanBencher> {
        Box::new(Self::new(self.batch, self.display_table_rows))
    }
}
