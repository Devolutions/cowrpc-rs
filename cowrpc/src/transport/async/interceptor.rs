use crate::error::{CowRpcError, Result};
use futures::{Async, AsyncSink, Sink};
use crate::proto::CowRpcMessage;
use std::{net::SocketAddr, time::Duration};
use crate::transport::uri::Uri;
use crate::transport::{
    r#async::{Transport, CowFuture, CowSink, CowStreamEx},
    MessageInterceptor, TransportError
};

pub struct InterceptorTransport {
    pub inter: Box<dyn MessageInterceptor>,
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

    fn message_stream(&mut self) -> CowStreamEx<CowRpcMessage> {
        unreachable!("Cannot call message_stream on the interceptor transport")
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.inter = cb_handler;
    }

    fn set_keep_alive_interval(&mut self, _: Option<Duration>) {
        // Not supported
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
    inter: Box<dyn MessageInterceptor>,
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
