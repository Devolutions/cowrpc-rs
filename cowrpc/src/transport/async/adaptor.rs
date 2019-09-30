use crate::error::{CowRpcError, Result};
use futures::{Async, Stream};
use parking_lot::Mutex;
use crate::proto::CowRpcMessage;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::prelude::*;
use super::*;

#[derive(Clone)]
pub struct Adaptor {
    messages: Arc<Mutex<VecDeque<CowRpcMessage>>>,
    current_task: Arc<Mutex<Option<task::Task>>>,
}

impl Adaptor {
    pub fn new() -> Adaptor {
        Adaptor {
            messages: Arc::new(Mutex::new(VecDeque::new())),
            current_task: Arc::new(Mutex::new(None)),
        }
    }

    pub fn message_stream(&self) -> CowStream<CowRpcMessage> {
        Box::new(AdaptorStream {
            messages: self.messages.clone(),
            current_task: self.current_task.clone(),
        })
    }
}

impl MessageInjector for Adaptor {
    fn inject(&self, msg: CowRpcMessage) {
        {
            let mut messages = self.messages.lock();

            messages.push_back(msg);

            let current_task = self.current_task.lock();

            if let Some(ref task) = &*current_task {
                task.notify();
            }
        }
    }
}

struct AdaptorStream {
    messages: Arc<Mutex<VecDeque<CowRpcMessage>>>,
    current_task: Arc<Mutex<Option<task::Task>>>,
}

impl Stream for AdaptorStream {
    type Item = CowRpcMessage;
    type Error = CowRpcError;

    fn poll(&mut self) -> Result<Async<Option<<Self as Stream>::Item>>> {
        let mut messages = self.messages.lock();

        if messages.is_empty() {
            let mut task = self.current_task.lock();
            if task.is_none() {
                *task = Some(task::current());
            }

            return Ok(Async::NotReady);
        }

        Ok(Async::Ready(messages.pop_front()))
    }
}
