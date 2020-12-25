// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::metrics::*;
use super::IOStats;
use crate::IOType;

use collections::HashMap;
use std::collections::VecDeque;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bcc::{table::Table, Kprobe, BPF};
use crossbeam_utils::CachePadded;

/// Biosnoop leverages BCC to make use of eBPF to get disk IO of TiKV requests.
/// The BCC code is in `biosnoop.c` which is compiled and attached kernel on
/// TiKV bootstrap. The code hooks on the start and completion of blk_account_io
/// in kernel, so it's easily to get the latency and bytes of IO requests issued
/// by current PID.
///
/// The main usage of iosnoop is to get accurate disk IO of different tasks
/// separately, like compaction, coprocessor and raftstore, instead of a global
/// disk throughput. So IO-types should be tagged for different threads by
/// `set_io_type()`. And BCC code is available to get the IO-type for one thread
/// by address, then all the IO requests for that thread will be recorded in
/// corresponding type's map in BCC.
///
/// With that information, every time calling `IOContext` it get the stored stats
/// from corresponding type's map in BCC. Thus it enables TiKV to get the latency and
/// bytes of read/write request per IO-type.

const MAX_THREAD_IDX: usize = 192;

// Hold the BPF to keep it not dropped.
// The two tables are `stats_by_type` and `type_by_pid` respectively.
static mut BPF_CONTEXT: Option<BPFContext> = None;

struct BPFContext {
    bpf: BPF,
    stats_table: Table,
    type_table: Table,
}

// This array records the IO-type for every thread. The address of this array
// will be passed into eBPF, so eBPF code can get IO-type for specific thread
// without an extra syscall.
// It should be a thread local variable, but the address of thread local is not
// reliable. So define a global array and let each thread writes on a specific
// element. And the IO-type is read when blk_account_io_start is called which is
// fired in the process context. That is to say, it's called in kernel space of
// user thread rather than kernel thread. So there is no contention between user
// and kernel. Thus no need to make the elements atomic. Also use padding to
// avoid false sharing.
// Leave the last element as reserved, when there is no available index, all
// other threads will be allocated to that index with IOType::Other always.
static mut IO_TYPE_ARRAY: [CachePadded<IOType>; MAX_THREAD_IDX + 1] =
    [CachePadded::new(IOType::Other); MAX_THREAD_IDX + 1];

// The index of the element of IO_TYPE_ARRAY for this thread to access.
thread_local! {
    static IDX: IdxWrapper = unsafe {
        let idx = IDX_ALLOCATOR.allocate();
        if let Some(ctx) = BPF_CONTEXT.as_mut() {
            let tid = nix::unistd::gettid().as_raw() as u32;
            let ptr : *const *const _ = &IO_TYPE_ARRAY.as_ptr().add(idx.0);
            ctx.type_table.set(
                &mut tid.to_ne_bytes(),
                std::slice::from_raw_parts_mut(
                    ptr as *mut u8,
                    std::mem::size_of::<*const IOType>(),
                ),
            ).unwrap();
        }
        idx
    }
}

struct IdxWrapper(usize);

impl Drop for IdxWrapper {
    fn drop(&mut self) {
        unsafe { *IO_TYPE_ARRAY[self.0] = IOType::Other };
        IDX_ALLOCATOR.free(self.0);

        // drop() of static variables won't be called when program exits.
        // We need to call drop() of BPF to detach kprobe.
        if IDX_ALLOCATOR.is_all_free() {
            unsafe { BPF_CONTEXT.take() };
        }
    }
}

lazy_static! {
    static ref IDX_ALLOCATOR: IdxAllocator = IdxAllocator::new();
}

struct IdxAllocator {
    free_list: Mutex<VecDeque<usize>>,
    count: AtomicUsize,
}

impl IdxAllocator {
    fn new() -> Self {
        IdxAllocator {
            free_list: Mutex::new((0..MAX_THREAD_IDX).into_iter().collect()),
            count: AtomicUsize::new(0),
        }
    }

    fn allocate(&self) -> IdxWrapper {
        self.count.fetch_add(1, Ordering::SeqCst);
        IdxWrapper(
            if let Some(idx) = self.free_list.lock().unwrap().pop_front() {
                idx
            } else {
                MAX_THREAD_IDX
            },
        )
    }

    fn free(&self, idx: usize) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        if idx != MAX_THREAD_IDX {
            self.free_list.lock().unwrap().push_back(idx);
        }
    }

    fn is_all_free(&self) -> bool {
        self.count.load(Ordering::SeqCst) == 0
    }
}

pub fn set_io_type(new_io_type: IOType) {
    unsafe {
        IDX.with(|idx| {
            // if MAX_THREAD_IDX, keep IOType::Other always
            if idx.0 != MAX_THREAD_IDX {
                *IO_TYPE_ARRAY[idx.0] = new_io_type;
            }
        })
    };
}

pub fn get_io_type() -> IOType {
    unsafe { *IDX.with(|idx| IO_TYPE_ARRAY[idx.0]) }
}

unsafe fn get_io_stats() -> Option<HashMap<IOType, IOStats>> {
    if let Some(ctx) = BPF_CONTEXT.as_mut() {
        let mut map = HashMap::default();
        for e in ctx.stats_table.iter() {
            let io_type = ptr::read(e.key.as_ptr() as *const IOType);
            let read = std::intrinsics::atomic_load(e.value.as_ptr() as *const u64);
            let write = std::intrinsics::atomic_load((e.value.as_ptr() as *const u64).add(1));
            map.insert(io_type, IOStats { read, write });
        }
        Some(map)
    } else {
        None
    }
}

pub struct IOContext {
    io_stats_map: Option<HashMap<IOType, IOStats>>,
}

impl IOContext {
    pub fn new() -> Self {
        IOContext {
            io_stats_map: unsafe { get_io_stats() },
        }
    }

    pub fn delta_and_refresh(&mut self) -> HashMap<IOType, IOStats> {
        if self.io_stats_map.is_some() {
            if let Some(map) = unsafe { get_io_stats() } {
                for (io_type, stats) in &map {
                    self.io_stats_map
                        .as_mut()
                        .unwrap()
                        .entry(*io_type)
                        .and_modify(|e| {
                            e.read = stats.read - e.read;
                            e.write = stats.write - e.write;
                        })
                        .or_insert(stats.clone());
                }

                return self.io_stats_map.replace(map).unwrap();
            }
        }
        HashMap::default()
    }
}

pub fn init_io_snooper() -> Result<(), String> {
    unsafe {
        if BPF_CONTEXT.is_some() {
            return Ok(());
        }
    }

    let code = include_str!("biosnoop.c").replace("##TGID##", &nix::unistd::getpid().to_string());

    // TODO: When using bpf_get_ns_current_pid_tgid of newer kernel, need
    // to get the device id and inode number.
    //
    // let stat = unsafe {
    //     let mut stat: libc::stat = std::mem::zeroed();
    //     if libc::stat(
    //         CString::new("/proc/self/ns/pid").unwrap().as_ptr(),
    //         &mut stat,
    //     ) != 0
    //     {
    //         return Err(String::from("Can't get namespace stats"));
    //     }
    //     stat
    // };
    // let code = code.replace("##DEV##", &stat.st_dev.to_string())
    //   .replace("##INO##", &stat.st_ino.to_string());

    // compile the above BPF code!
    let mut bpf = BPF::new(&code).map_err(|e| e.to_string())?;
    // attach kprobes
    Kprobe::new()
        .handler("trace_req_start")
        .function("blk_account_io_start")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    Kprobe::new()
        .handler("trace_req_completion")
        .function("blk_account_io_completion")
        .attach(&mut bpf)
        .map_err(|e| e.to_string())?;
    let stats_table = bpf.table("stats_by_type").map_err(|e| e.to_string())?;
    let type_table = bpf.table("type_by_pid").map_err(|e| e.to_string())?;
    unsafe {
        BPF_CONTEXT = Some(BPFContext {
            bpf,
            stats_table,
            type_table,
        });
    }
    let _ = IO_CONTEXT.lock().unwrap(); // trigger init of io context
    Ok(())
}

lazy_static! {
    static ref IO_CONTEXT: Mutex<IOContext> = Mutex::new(IOContext::new());
}

macro_rules! flush_io_latency_and_bytes {
    ($bpf:expr, $delta:ident, $metrics:ident, $type:expr) => {
        let mut t = $bpf
            .table(concat!(stringify!($metrics), "_read_latency"))
            .unwrap();
        for mut e in t.iter() {
            let bucket = 2_u64.pow(ptr::read(e.key.as_ptr() as *const libc::c_int) as u32);
            let count = ptr::read(e.value.as_ptr() as *const u64);

            for _ in 0..count {
                IO_LATENCY_MICROS_VEC.$metrics.read.observe(bucket as f64);
            }
            let zero: u64 = 0;
            t.set(&mut e.key, &mut zero.to_ne_bytes()).unwrap();
        }

        let mut t = $bpf
            .table(concat!(stringify!($metrics), "_write_latency"))
            .unwrap();
        for mut e in t.iter() {
            let bucket = 2_u64.pow(ptr::read(e.key.as_ptr() as *const libc::c_int) as u32);
            let count = ptr::read(e.value.as_ptr() as *const u64);

            for _ in 0..count {
                IO_LATENCY_MICROS_VEC.$metrics.write.observe(bucket as f64);
            }
            let zero: u64 = 0;
            t.set(&mut e.key, &mut zero.to_ne_bytes()).unwrap();
        }
        if let Some(v) = $delta.get(&$type) {
            IO_BYTES_VEC.$metrics.read.inc_by(v.read as i64);
            IO_BYTES_VEC.$metrics.write.inc_by(v.write as i64);
        }
    };
}

pub fn flush_io_metrics() {
    unsafe {
        if let Some(ctx) = BPF_CONTEXT.as_mut() {
            let delta = IO_CONTEXT.lock().unwrap().delta_and_refresh();
            flush_io_latency_and_bytes!(ctx.bpf, delta, other, IOType::Other);
            flush_io_latency_and_bytes!(ctx.bpf, delta, read, IOType::Read);
            flush_io_latency_and_bytes!(ctx.bpf, delta, write, IOType::Write);
            flush_io_latency_and_bytes!(ctx.bpf, delta, coprocessor, IOType::Coprocessor);
            flush_io_latency_and_bytes!(ctx.bpf, delta, flush, IOType::Flush);
            flush_io_latency_and_bytes!(ctx.bpf, delta, compaction, IOType::Compaction);
            flush_io_latency_and_bytes!(ctx.bpf, delta, replication, IOType::Replication);
            flush_io_latency_and_bytes!(ctx.bpf, delta, load_balance, IOType::LoadBalance);
            flush_io_latency_and_bytes!(ctx.bpf, delta, import, IOType::Import);
            flush_io_latency_and_bytes!(ctx.bpf, delta, export, IOType::Export);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::iosnoop::imp::{BPF_CONTEXT, MAX_THREAD_IDX};
    use crate::iosnoop::metrics::*;
    use crate::{flush_io_metrics, get_io_type, init_io_snooper, set_io_type, IOContext, IOType};
    use rand::Rng;
    use std::sync::{Arc, Condvar, Mutex};
    use std::{
        fs::OpenOptions, io::Read, io::Seek, io::SeekFrom, io::Write, os::unix::fs::OpenOptionsExt,
    };
    use tempfile::TempDir;
    use test::Bencher;

    use libc::O_DIRECT;
    use maligned::A512;
    use maligned::{AsBytes, AsBytesMut};

    #[test]
    fn test_biosnoop() {
        init_io_snooper().unwrap();
        // Test cases are running in parallel, while they depend on the same global variables.
        // To make them not affect each other, run them in sequence.
        test_thread_idx_allocation();
        test_io_context();
        unsafe {
            BPF_CONTEXT.take();
        }
    }

    fn test_io_context() {
        set_io_type(IOType::Compaction);
        assert_eq!(get_io_type(), IOType::Compaction);
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("test_io_context");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut w = vec![A512::default(); 2];
        w.as_bytes_mut()[512] = 42;
        let mut ctx = IOContext::new();
        f.write(w.as_bytes()).unwrap();
        f.sync_all().unwrap();
        let delta = ctx.delta_and_refresh();
        assert_ne!(delta.get(&IOType::Compaction).unwrap().write, 0);
        assert_eq!(delta.get(&IOType::Compaction).unwrap().read, 0);
        drop(f);

        std::thread::spawn(move || {
            set_io_type(IOType::Other);
            let mut f = OpenOptions::new()
                .read(true)
                .custom_flags(O_DIRECT)
                .open(&file_path)
                .unwrap();
            let mut r = vec![A512::default(); 2];
            assert_ne!(f.read(&mut r.as_bytes_mut()).unwrap(), 0);
            drop(f);
        })
        .join()
        .unwrap();

        let delta = ctx.delta_and_refresh();
        assert_eq!(delta.get(&IOType::Compaction).unwrap().write, 0);
        assert_eq!(delta.get(&IOType::Compaction).unwrap().read, 0);
        assert_eq!(delta.get(&IOType::Other).unwrap().write, 0);
        assert_ne!(delta.get(&IOType::Other).unwrap().read, 0);

        flush_io_metrics();
        assert_ne!(IO_LATENCY_MICROS_VEC.compaction.write.get_sample_count(), 0);
        assert_ne!(IO_LATENCY_MICROS_VEC.other.read.get_sample_count(), 0);
        assert_ne!(IO_BYTES_VEC.compaction.write.get(), 0);
        assert_ne!(IO_BYTES_VEC.other.read.get(), 0);
    }

    fn test_thread_idx_allocation() {
        // the thread indexes should be recycled.
        for _ in 1..=MAX_THREAD_IDX * 2 {
            std::thread::spawn(|| {
                set_io_type(IOType::Other);
            })
            .join()
            .unwrap();
        }

        // use up all available thread index.
        let pair = Arc::new((Mutex::new(false), Condvar::new()));
        let mut handles = Vec::new();
        for _ in 1..=MAX_THREAD_IDX {
            let pair1 = pair.clone();
            let h = std::thread::spawn(move || {
                set_io_type(IOType::Compaction);
                let (lock, cvar) = &*pair1;
                let mut stop = lock.lock().unwrap();
                while !*stop {
                    stop = cvar.wait(stop).unwrap();
                }
            });
            handles.push(h);
        }

        // the reserved index is used, io type should be IOType::Other
        for _ in 1..=MAX_THREAD_IDX {
            std::thread::spawn(|| {
                set_io_type(IOType::Compaction);
                assert_eq!(get_io_type(), IOType::Other);
            })
            .join()
            .unwrap();
        }

        {
            let (lock, cvar) = &*pair;
            let mut stop = lock.lock().unwrap();
            *stop = true;
            cvar.notify_all();
        }

        for h in handles {
            h.join().unwrap();
        }

        // the thread indexes should be available again.
        for _ in 1..=MAX_THREAD_IDX {
            std::thread::spawn(|| {
                set_io_type(IOType::Compaction);
                assert_eq!(get_io_type(), IOType::Compaction);
            })
            .join()
            .unwrap();
        }
    }

    #[bench]
    #[ignore]
    fn bench_write_enable_io_snoop(b: &mut Bencher) {
        init_io_snooper().unwrap();
        bench_write(b);
    }

    #[bench]
    #[ignore]
    fn bench_write_disable_io_snoop(b: &mut Bencher) {
        unsafe { BPF_CONTEXT = None };
        bench_write(b);
    }

    #[bench]
    #[ignore]
    fn bench_read_enable_io_snoop(b: &mut Bencher) {
        init_io_snooper().unwrap();
        bench_read(b);
    }

    #[bench]
    #[ignore]
    fn bench_read_disable_io_snoop(b: &mut Bencher) {
        unsafe { BPF_CONTEXT = None };
        bench_read(b);
    }

    #[bench]
    #[ignore]
    fn bench_flush_io_metrics(b: &mut Bencher) {
        init_io_snooper().unwrap();
        set_io_type(IOType::Write);

        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_flush_io_metrics");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = vec![A512::default(); 1];
        w.as_bytes_mut()[64] = 42;
        for _ in 1..=100 {
            f.write(w.as_bytes()).unwrap();
        }
        f.sync_all().unwrap();

        b.iter(|| {
            flush_io_metrics();
        });
    }

    fn bench_write(b: &mut Bencher) {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_write_io_snoop");
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = vec![A512::default(); 1];
        w.as_bytes_mut()[64] = 42;

        b.iter(|| {
            set_io_type(IOType::Write);
            f.write(w.as_bytes()).unwrap();
            f.sync_all().unwrap();
        });
    }

    fn bench_read(b: &mut Bencher) {
        let tmp = TempDir::new().unwrap();
        let file_path = tmp.path().join("bench_read_io_snoop");
        let mut f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();

        let mut w = vec![A512::default(); 2];
        w.as_bytes_mut()[64] = 42;
        for _ in 0..100 {
            f.write(w.as_bytes()).unwrap();
        }
        f.sync_all().unwrap();
        drop(f);

        let mut rng = rand::thread_rng();
        let mut f = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECT)
            .open(&file_path)
            .unwrap();
        let mut r = vec![A512::default(); 2];
        b.iter(|| {
            set_io_type(IOType::Read);
            f.seek(SeekFrom::Start(rng.gen_range(0, 100) * 512))
                .unwrap();
            assert_ne!(f.read(&mut r.as_bytes_mut()).unwrap(), 0);
        });
    }
}
