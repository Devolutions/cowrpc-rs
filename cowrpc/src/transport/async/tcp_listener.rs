use crate::error::CowRpcError;
use crate::transport::r#async::tcp::TcpTransport;
use crate::transport::r#async::{CowFuture, CowStream, Listener};
use crate::transport::MessageInterceptor;
use async_trait::async_trait;
use futures::future;
use std::net::SocketAddr;
use tokio::net::TcpListener as TcpTokioListener;
use tokio::stream::StreamExt;
use tokio_rustls::TlsAcceptor;

pub struct TcpListener {
    listener: TcpTokioListener,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
}

#[async_trait]
impl Listener for TcpListener {
    type TransportInstance = TcpTransport;

    async fn bind(addr: &SocketAddr, _tls_acceptor: Option<TlsAcceptor>) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr).await {
            Ok(l) => Ok(TcpListener {
                listener: l,
                transport_cb_handler: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let TcpListener {
            listener,
            transport_cb_handler,
        } = self;

        Box::pin(listener.map(move |stream| {
            let tcp_stream = stream?;
            let cbh = transport_cb_handler.clone();
            Ok(Box::pin(future::ok(TcpTransport::new(tcp_stream, cbh))) as CowFuture<TcpTransport>)
        })) as CowStream<CowFuture<Self::TransportInstance>>
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}
