use super::*;
use crate::error::Result;
use crate::proto::CowRpcMessage;
use futures::task::Waker;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct Adaptor {
    messages: Arc<Mutex<VecDeque<CowRpcMessage>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Default for Adaptor {
    fn default() -> Self {
        Adaptor {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

impl Adaptor {
    pub fn message_stream(&self) -> BoxStream<CowRpcMessage> {
        Box::pin(AdaptorStream {
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

            if let Some(waker) = &*waker {
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

        let msg = messages.pop_front().map(Ok);
        Poll::Ready(msg)
    }
}
