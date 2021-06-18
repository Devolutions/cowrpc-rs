use crate::error::CowRpcError;
use crate::transport::tcp::TcpTransport;
use crate::transport::{BoxStream, CowFuture, Listener, MessageInterceptor};
use async_trait::async_trait;
use futures::future;
use slog::Logger;
use std::net::SocketAddr;
use tokio::net::TcpListener as TcpTokioListener;
use tokio::stream::StreamExt;
use tokio_rustls::TlsAcceptor;

pub struct TcpListener {
    listener: TcpTokioListener,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
}

#[async_trait]
impl Listener for TcpListener {
    type TransportInstance = TcpTransport;

    async fn bind(addr: &SocketAddr, _tls_acceptor: Option<TlsAcceptor>, logger: Logger) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr).await {
            Ok(l) => Ok(TcpListener {
                listener: l,
                transport_cb_handler: None,
                logger,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn incoming(self) -> BoxStream<CowFuture<Self::TransportInstance>> {
        let TcpListener {
            listener,
            transport_cb_handler,
            logger,
        } = self;

        Box::pin(listener.map(move |stream| {
            let tcp_stream = stream?;
            let cbh = transport_cb_handler.clone();
            let logger_clone = logger.clone();
            Ok(Box::pin(future::ok(TcpTransport::new(tcp_stream, cbh, logger_clone))) as CowFuture<TcpTransport>)
        })) as BoxStream<CowFuture<Self::TransportInstance>>
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}
