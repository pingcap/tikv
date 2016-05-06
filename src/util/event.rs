use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::ops::Deref;


pub struct EventCore<T> {
    shared_cnt: AtomicUsize,
    mutex: Mutex<Option<T>>,
    cond: Condvar,
}

impl<T> Default for EventCore<T> {
    fn default() -> EventCore<T> {
        EventCore {
            shared_cnt: AtomicUsize::new(0),
            mutex: Mutex::new(None),
            cond: Condvar::new(),
        }
    }
}

/// This is a simple mechanism to synchronize between threads with timeout support.
pub struct Event<T> {
    inner: Arc<EventCore<T>>,
}

impl<T> Default for Event<T> {
    fn default() -> Event<T> {
        Event { inner: Default::default() }
    }
}

impl<T> Deref for Event<T> {
    type Target = EventCore<T>;

    fn deref(&self) -> &EventCore<T> {
        &self.inner
    }
}

impl<T> !Sync for Event<T> {}

impl<T> Event<T> {
    pub fn new() -> Event<T> {
        Default::default()
    }

    /// Set this event and wake up all other waiting-set threads.
    pub fn set(&self, t: T) {
        let mut l = self.mutex.lock().unwrap();
        *l = Some(t);
        self.cond.notify_all();
    }

    /// Apply a function to an event.
    ///
    /// If the event is not set yet, None is returned; otherwise
    /// applied result is returned.
    pub fn apply<U, F: Fn(&mut T) -> U>(&self, f: F) -> Option<U> {
        if self.mutex.lock().unwrap().is_none() {
            return None;
        }
        let mut l = self.mutex.lock().unwrap();
        let res = f(l.as_mut().unwrap());
        Some(res)
    }

    /// Query current status without block.
    pub fn is_set(&self) -> bool {
        self.mutex.lock().unwrap().is_some()
    }

    /// Take the inner value and wait up all other waiting-clear threads.
    pub fn take(&self) -> Option<T> {
        let mut l = self.mutex.lock().unwrap();
        let t = l.take();
        self.cond.notify_all();
        t
    }

    /// Wait till this event is set.
    ///
    /// If it's set before timeout, true is returned; otherwise return false.
    /// If it's for sure the value won't be set, it will return immediately.
    pub fn wait_timeout(&self, timeout: Option<Duration>) -> bool {
        self.wait(true, timeout)
    }

    // Wait for inner mutex status change.
    fn wait(&self, res: bool, timeout: Option<Duration>) -> bool {
        let start_time = Instant::now();
        let has_timeout = timeout.is_some();
        let timeout = timeout.unwrap_or_else(|| Duration::from_millis(0));
        let mut l = self.mutex.lock().unwrap();
        while l.is_some() != res {
            if self.shared_cnt.load(Ordering::Relaxed) == 0 {
                return false;
            }
            if !has_timeout {
                l = self.cond.wait(l).unwrap();
                continue;
            }
            let elapsed = start_time.elapsed();
            if timeout <= elapsed {
                return false;
            }
            let (v, timeout_res) = self.cond.wait_timeout(l, timeout - elapsed).unwrap();
            if timeout_res.timed_out() {
                return false;
            }
            l = v;
        }
        true
    }

    /// Wait for set status to be clear.
    ///
    /// If it's clear before timeout, true is returned; otherwise return false.
    /// If it's for sure the value won't be clear, it will return immediately.
    pub fn wait_clear(&self, timeout: Option<Duration>) -> bool {
        self.wait(false, timeout)
    }
}

impl<T> Clone for Event<T> {
    fn clone(&self) -> Event<T> {
        self.shared_cnt.fetch_add(1, Ordering::Relaxed);
        Event { inner: self.inner.clone() }
    }
}

impl<T> Drop for Event<T> {
    fn drop(&mut self) {
        let f = self.mutex.lock().unwrap();
        self.shared_cnt.fetch_sub(1, Ordering::Relaxed);
        // notify other clone, so that it won't hung for ever.
        self.cond.notify_all();
        drop(f);
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use std::thread;
    use std::time::{Instant, Duration};

    #[test]
    fn test_event() {
        let e = Event::new();

        assert!(!e.wait_timeout(None));
        assert!(e.wait_clear(None));

        let e2 = e.clone();

        let h = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            e2.set(4);
            let set_time = Instant::now();
            e2.wait_clear(None);
            assert!(set_time.elapsed() > Duration::from_millis(100));
        });

        assert!(!e.is_set());
        let start_time = Instant::now();
        assert!(!e.wait_timeout(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() >= Duration::from_millis(100));
        assert!(e.wait_timeout(None));
        assert!(start_time.elapsed() >= Duration::from_millis(200));
        assert!(e.is_set());

        let past_time = start_time.elapsed();
        e.wait_timeout(None);
        assert!(start_time.elapsed() - past_time < Duration::from_millis(1));

        assert!(!e.wait_clear(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() - past_time >= Duration::from_millis(100));

        let v = e.apply(|s| *s);

        assert_eq!(e.take(), v);
        assert!(!e.is_set());
        assert!(!e.wait_timeout(Some(Duration::from_millis(100))));
        assert!(start_time.elapsed() - past_time >= Duration::from_millis(100));

        h.join().unwrap();

        e.set(3);
        assert!(e.wait_timeout(None));
        assert!(!e.wait_clear(None));
    }
}
