use mio::event::{Evented, Events};
use mio::{Poll, PollOpt, Ready, Registration, SetReadiness, Token};

use parking_lot::Mutex;
use std::clone::Clone;
use std::io;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub struct CancelEvent {
    set_readiness: SetReadiness,
}

impl CancelEvent {
    pub fn cancel(&self) -> io::Result<()> {
        self.set_readiness.set_readiness(Ready::readable())
    }
}

impl CancelEvent {
    pub fn new() -> (CancelEventHandle, CancelEvent) {
        let (registration, set_readiness) = Registration::new2();

        (
            CancelEventHandle::new(registration, set_readiness.clone()),
            CancelEvent { set_readiness },
        )
    }
}

pub struct CancelEventHandle {
    inner: Arc<Mutex<Inner>>,

    cancel_thread: SetReadiness,
    thread_handle: Option<JoinHandle<()>>,
}

impl Drop for CancelEventHandle {
    fn drop(&mut self) {
        // Stop the thread
        match self.cancel_thread.set_readiness(Ready::readable()) {
            Ok(_) => {
                // Wait the end of the thread...
                let _ = self.thread_handle.take().unwrap().join();
            }
            Err(e) => {
                error!("Can't stop the cancel event thread: {}", e);
            }
        }
    }
}

impl CancelEventHandle {
    fn new(registration: Registration, set_readiness: SetReadiness) -> CancelEventHandle {
        let inner = Arc::new(Mutex::new(Inner::new()));

        let inner_clone = inner.clone();
        let handle: JoinHandle<()> = thread::spawn(move || {
            CancelEventHandle::wait_cancel(registration, inner_clone);
        });

        CancelEventHandle {
            cancel_thread: set_readiness,
            thread_handle: Some(handle),
            inner,
        }
    }

    fn wait_cancel(registration: Registration, inner: Arc<Mutex<Inner>>) {
        // Create a poll instance
        let poll = Poll::new().unwrap();
        poll.register(&registration, Token(0), Ready::readable(), PollOpt::edge())
            .unwrap();

        // Create storage for events
        let mut events = Events::with_capacity(16);

        loop {
            poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    Token(0) => {
                        let mut inner = inner.lock();
                        inner.is_cancelled = true;
                        if inner.registration.is_some() {
                            // Ignore the error we can't signal the event
                            if let Err(e) = inner.registration.as_ref().unwrap().1.set_readiness(Ready::readable()) {
                                error!("Can't signal cancel event: {}", e);
                            }
                        }
                        return;
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Evented for CancelEventHandle {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        let (registration, set_readiness) = Registration::new2();

        let result = registration.register(poll, token, interest, opts);

        let mut inner = self.inner.lock();
        if inner.is_cancelled {
            set_readiness.set_readiness(Ready::readable())?;
        }
        inner.registration = Some((registration, set_readiness));

        result
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        let inner = self.inner.lock();
        if inner.registration.is_some() {
            inner
                .registration
                .as_ref()
                .unwrap()
                .0
                .reregister(poll, token, interest, opts)
        } else {
            unreachable!()
        }
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        let inner = self.inner.lock();
        if inner.registration.is_some() {
            Evented::deregister(&inner.registration.as_ref().unwrap().0, poll)
        } else {
            unreachable!()
        }
    }
}

struct Inner {
    is_cancelled: bool,
    registration: Option<(Registration, SetReadiness)>,
}

impl Inner {
    fn new() -> Inner {
        Inner {
            is_cancelled: false,
            registration: None,
        }
    }
}
