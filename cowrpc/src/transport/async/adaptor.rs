use super::*;
use crate::error::{CowRpcError, Result};
use crate::proto::CowRpcMessage;
use futures::prelude::*;
use futures::task::Waker;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct Adaptor {
    messages: Arc<Mutex<VecDeque<CowRpcMessage>>>,
    waker: Arc<Mutex<Option<Waker>>>, //current_task: Arc<Mutex<Option<Task>>>,
}

impl Adaptor {
    pub fn new() -> Adaptor {
        Adaptor {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
        }
    }

    pub fn message_stream(&self) -> CowStream<CowRpcMessage> {
        Box::new(AdaptorStream {
            messages: self.messages.clone(),
            waker: self.waker.clone(),
        })
    }
}

impl MessageInjector for Adaptor {
    fn inject(&self, msg: CowRpcMessage) {
        {
            let mut messages = self.messages.lock();

            messages.push_back(msg);

            let waker = self.waker.lock();

            if let Some(ref waker) = &*waker {
                waker.wake_by_ref();
            }
        }
    }
}

struct AdaptorStream {
    messages: Arc<Mutex<VecDeque<CowRpcMessage>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Stream for AdaptorStream {
    type Item = Result<CowRpcMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut messages = self.messages.lock();

        if messages.is_empty() {
            let mut waker = self.waker.lock();
            if waker.is_none() {
                *waker = Some(cx.waker().clone());
            }

            return Poll::Pending;
        }

        let msg = messages.pop_front().map(|msg| Ok(msg));
        Poll::Ready(msg)
    }
}
