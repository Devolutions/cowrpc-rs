use std::sync::{Condvar, Mutex};

/// ReadyEvent is a small wrapper around Condvar that provides a synchronization event.
///
/// Warning: ReadyEvent can panic and should not be used outside of tests.
pub struct ReadyEvent((Mutex<()>, Condvar));

impl ReadyEvent {
    pub fn new() -> ReadyEvent {
        ReadyEvent((Mutex::new(()), Condvar::new()))
    }

    pub fn set(&self) {
        let (ref _lock, ref cvar) = self.0;
        cvar.notify_one();
    }

    pub fn wait(&self) {
        let (ref lock, ref cvar) = self.0;
        let _ = cvar.wait(lock.lock().unwrap()).unwrap();
    }
}
