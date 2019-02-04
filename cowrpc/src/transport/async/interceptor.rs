use error::{CowRpcError, Result};
use futures::{Async, AsyncSink, Sink};
use proto::CowRpcMessage;
use std::{net::SocketAddr, time::Duration};
use transport::uri::Uri;
use transport::{
    async::{Transport, CowFuture, CowSink, CowStream},
    MessageInterceptor, TransportError
};

pub struct InterceptorTransport {
    pub inter: Box<MessageInterceptor>,
}

impl Clone for InterceptorTransport {
    fn clone(&self) -> Self {
        InterceptorTransport {
            inter: self.inter.clone_boxed(),
        }
    }
}

impl Transport for InterceptorTransport {
    fn connect(_uri: Uri) -> CowFuture<Self>
    where
        Self: Sized,
    {
        unreachable!("Cannot call connect on the interceptor transport")
    }

    fn message_sink(&mut self) -> CowSink<CowRpcMessage> {
        Box::new(InterceptorSink {
            inter: self.inter.clone_boxed(),
        })
    }

    fn message_stream(&mut self) -> CowStream<CowRpcMessage> {
        unreachable!("Cannot call message_stream on the interceptor transport")
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.inter = cb_handler;
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn up_time(&self) -> Duration {
        Duration::from_secs(0)
    }
}

struct InterceptorSink {
    inter: Box<MessageInterceptor>,
}

impl Sink for InterceptorSink {
    type SinkItem = CowRpcMessage;
    type SinkError = CowRpcError;

    fn start_send(&mut self, item: <Self as Sink>::SinkItem) -> Result<AsyncSink<<Self as Sink>::SinkItem>> {
        if let Some(msg) = self.inter.before_send(item) {
            Err(TransportError::EndpointUnreachable(format!(
                "Unable to send msg {} trought interceptor, peer {} is inside this router",
                msg.get_msg_name(),
                msg.get_dst_id()
            )).into())
        } else {
            Ok(AsyncSink::Ready)
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>> {
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Result<Async<()>> {
        Ok(Async::Ready(()))
    }
}
