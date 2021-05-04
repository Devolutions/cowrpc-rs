use crate::error::{CowRpcError, Result};
use crate::proto::CowRpcMessage;
use crate::transport::r#async::{CowSink, CowStreamEx, Transport};
use crate::transport::uri::Uri;
use crate::transport::{MessageInterceptor, TransportError};
use async_trait::async_trait;
use futures::prelude::*;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use crate::transport::r#async::StreamEx;

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

#[async_trait]
impl Transport for InterceptorTransport {
    async fn connect(_uri: Uri) -> Result<Self>
    where
        Self: Sized,
    {
        unreachable!("Cannot call connect on the interceptor transport")
    }

    fn message_stream_sink(self) -> (CowStreamEx<CowRpcMessage>, CowSink<CowRpcMessage>) {
        let sink = Box::pin(InterceptorSink {
            inter: self.inter.clone_boxed(),
        });

        let stream = Box::pin(InterceptorStream{});

        (stream, sink)
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

impl InterceptorSink {
    fn new(inter: Box<dyn MessageInterceptor>) -> Self {
        InterceptorSink {
            inter
        }
    }
}

impl Sink<CowRpcMessage> for InterceptorSink {
    type Error = CowRpcError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: CowRpcMessage) -> Result<()> {
        let this = self.get_mut();

        if let Some(msg) = this.inter.before_send(item) {
            Err(TransportError::EndpointUnreachable(format!(
                "Unable to send msg {} trought interceptor, peer {} is inside this router",
                msg.get_msg_name(),
                msg.get_dst_id()
            ))
            .into())
        } else {
            Ok(())
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct InterceptorStream {}

impl Stream for InterceptorStream {
    type Item = Result<CowRpcMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unreachable!()
    }
}

impl StreamEx for InterceptorStream {
    fn close_on_keep_alive_timeout(&mut self, close: bool) {
        // Not supported
    }
}
