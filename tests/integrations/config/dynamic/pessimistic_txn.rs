use std::sync::{mpsc, Arc};
use std::time::Duration;

use pd_client::PdClient;
use tikv::config::*;
use tikv::server::lock_manager::*;
use tikv::server::resolve::{Callback, StoreAddrResolver};
use tikv::server::{Error, Result};
use tikv_util::security::SecurityManager;

#[test]
fn test_config_validate() {
    let cfg = Config::default();
    cfg.validate().unwrap();

    let mut invalid_cfg = Config::default();
    invalid_cfg.wait_for_lock_timeout = 0;
    assert!(invalid_cfg.validate().is_err());
}

struct MockPdClient;
impl PdClient for MockPdClient {}

#[derive(Clone)]
struct MockResolver;
impl StoreAddrResolver for MockResolver {
    fn resolve(&self, _store_id: u64, _cb: Callback) -> Result<()> {
        Err(Error::Other(box_err!("unimplemented")))
    }
}

fn setup(
    cfg: TiKvConfig,
) -> (
    ConfigController,
    WaiterMgrScheduler,
    DetectorScheduler,
    LockManager,
) {
    let mut lock_mgr = LockManager::new();
    let pd_client = Arc::new(MockPdClient);
    let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
    lock_mgr
        .start(
            1,
            pd_client,
            MockResolver,
            security_mgr,
            &cfg.pessimistic_txn,
        )
        .unwrap();

    let mgr = lock_mgr.config_manager();
    let (w, d) = (
        mgr.waiter_mgr_scheduler.clone(),
        mgr.detector_scheduler.clone(),
    );
    let mut cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(Module::PessimisticTxn, Box::new(mgr));

    (cfg_controller, w, d, lock_mgr)
}

fn validate_waiter<F>(router: &WaiterMgrScheduler, f: F)
where
    F: FnOnce(u64, u64) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v1, v2| {
        f(v1, v2);
        tx.send(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

fn validate_dead_lock<F>(router: &DetectorScheduler, f: F)
where
    F: FnOnce(u64) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v| {
        f(v);
        tx.send(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_lock_manager_cfg_update() {
    const DEFAULT_TIMEOUT: u64 = 3000;
    const DEFAULT_DELAY: u64 = 100;
<<<<<<< HEAD
    let mut cfg = TiKvConfig::default();
    cfg.pessimistic_txn.wait_for_lock_timeout = DEFAULT_TIMEOUT;
    cfg.pessimistic_txn.wake_up_delay_duration = DEFAULT_DELAY;
=======
    let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
    cfg.pessimistic_txn.wait_for_lock_timeout = ReadableDuration::millis(DEFAULT_TIMEOUT);
    cfg.pessimistic_txn.wake_up_delay_duration = ReadableDuration::millis(DEFAULT_DELAY);
>>>>>>> d1eadfb... config: move config update interface from pd to status server (#7495)
    cfg.validate().unwrap();
    let (mut cfg_controller, waiter, deadlock, mut lock_mgr) = setup(cfg);

    // update of other module's config should not effect lock manager config
<<<<<<< HEAD
    let mut incoming = cfg.clone();
    incoming.raft_store.raft_log_gc_threshold = 2000;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert_eq!(rollback.right(), Some(true));
    validate_waiter(&waiter, move |timeout: u64, delay: u64| {
        assert_eq!(timeout, DEFAULT_TIMEOUT);
        assert_eq!(delay, DEFAULT_DELAY);
    });
=======
    cfg_controller
        .update_config("raftstore.raft-log-gc-threshold", "2000")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
            assert_eq!(delay.as_millis(), DEFAULT_DELAY);
        },
    );
>>>>>>> d1eadfb... config: move config update interface from pd to status server (#7495)
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only update wake_up_delay_duration
<<<<<<< HEAD
    let mut incoming = cfg.clone();
    incoming.pessimistic_txn.wake_up_delay_duration = 500;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert_eq!(rollback.right(), Some(true));
    validate_waiter(&waiter, move |timeout: u64, delay: u64| {
        assert_eq!(timeout, DEFAULT_TIMEOUT);
        assert_eq!(delay, 500);
    });
=======
    cfg_controller
        .update_config("pessimistic-txn.wake-up-delay-duration", "500ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
            assert_eq!(delay.as_millis(), 500);
        },
    );
>>>>>>> d1eadfb... config: move config update interface from pd to status server (#7495)
    validate_dead_lock(&deadlock, move |ttl: u64| {
        // dead lock ttl should not change
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only update wait_for_lock_timeout
<<<<<<< HEAD
    let mut incoming = cfg.clone();
    incoming.pessimistic_txn.wait_for_lock_timeout = 4000;
    // keep wake_up_delay_duration the same as last update
    incoming.pessimistic_txn.wake_up_delay_duration = 500;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert_eq!(rollback.right(), Some(true));
    validate_waiter(&waiter, move |timeout: u64, delay: u64| {
        assert_eq!(timeout, 4000);
        // wake_up_delay_duration should be the same as last update
        assert_eq!(delay, 500);
    });
=======
    cfg_controller
        .update_config("pessimistic-txn.wait-for-lock-timeout", "4000ms")
        .unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), 4000);
            // wake_up_delay_duration should be the same as last update
            assert_eq!(delay.as_millis(), 500);
        },
    );
>>>>>>> d1eadfb... config: move config update interface from pd to status server (#7495)
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4000);
    });

    // update both config
<<<<<<< HEAD
    let mut incoming = cfg;
    incoming.pessimistic_txn.wait_for_lock_timeout = 4321;
    incoming.pessimistic_txn.wake_up_delay_duration = 123;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    assert_eq!(rollback.right(), Some(true));
    validate_waiter(&waiter, move |timeout: u64, delay: u64| {
        assert_eq!(timeout, 4321);
        assert_eq!(delay, 123);
    });
=======
    let mut m = std::collections::HashMap::new();
    m.insert(
        "pessimistic-txn.wait-for-lock-timeout".to_owned(),
        "4321ms".to_owned(),
    );
    m.insert(
        "pessimistic-txn.wake-up-delay-duration".to_owned(),
        "123ms".to_owned(),
    );
    cfg_controller.update(m).unwrap();
    validate_waiter(
        &waiter,
        move |timeout: ReadableDuration, delay: ReadableDuration| {
            assert_eq!(timeout.as_millis(), 4321);
            assert_eq!(delay.as_millis(), 123);
        },
    );
>>>>>>> d1eadfb... config: move config update interface from pd to status server (#7495)
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4321);
    });

    lock_mgr.stop();
}
