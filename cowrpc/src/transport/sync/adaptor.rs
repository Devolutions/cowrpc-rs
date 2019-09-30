use mio::event::Evented;
use mio::{Poll, PollOpt, Ready, Registration, SetReadiness, Token};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use super::*;
use crate::CowRpcMessage;

pub struct AdaptorMutInner {
    messages: VecDeque<CowRpcMessage>,
    set_readiness: SetReadiness,
}

struct AdaptorAtomicInner {
    inner: Mutex<AdaptorMutInner>,
    registration: Registration,
}

#[derive(Clone)]
pub struct Adaptor {
    arc_inner: Arc<AdaptorAtomicInner>,
}

impl Adaptor {
    pub fn new() -> Adaptor {
        let (registration, set_readiness) = Registration::new2();

        Adaptor {
            arc_inner: Arc::new(AdaptorAtomicInner {
                inner: Mutex::new(AdaptorMutInner {
                    messages: VecDeque::new(),
                    set_readiness,
                }),
                registration,
            }),
        }
    }
}

impl TransportAdapter for Adaptor {
    fn get_next_message(&self) -> Result<Option<CowRpcMessage>> {
        let mut inner = self.arc_inner.inner.lock();

        if !inner.messages.is_empty() {
            Ok(inner.messages.pop_front())
        } else {
            if !inner.set_readiness.readiness().is_empty() {
                let _ = inner.set_readiness.set_readiness(Ready::empty());
            }
            Ok(None)
        }
    }
}

impl MessageInjector for Adaptor {
    fn inject(&self, msg: CowRpcMessage) {
        {
            let mut inner = self.arc_inner.inner.lock();

            inner.messages.push_back(msg);

            if inner.set_readiness.readiness().is_empty() {
                let _ = inner.set_readiness.set_readiness(Ready::readable());
            }
        }
    }
}

impl Evented for Adaptor {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        let result = self.arc_inner.registration.register(poll, token, interest, opts);

        let inner = self.arc_inner.inner.lock();

        if !inner.messages.is_empty() {
            inner.set_readiness.set_readiness(Ready::readable())?;
        }

        result
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> io::Result<()> {
        self.arc_inner.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        Evented::deregister(&self.arc_inner.registration, poll)
    }
}
