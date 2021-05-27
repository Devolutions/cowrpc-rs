use crate::error::{CowRpcError, Result};
use crate::transport::websocket::{CowWebSocketStream, WebSocketTransport};
use crate::transport::{CowFuture, CowStream, Listener, MessageInterceptor, TransportError};
use async_trait::async_trait;
use async_tungstenite::tokio::accept_async;

use slog::Logger;
use std::net::SocketAddr;
use tokio::net::{TcpListener as TcpTokioListener, TcpStream};
use tokio_rustls::TlsAcceptor;

pub struct WebSocketListener {
    listener: TcpTokioListener,
    tls_acceptor: Option<TlsAcceptor>,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
}

#[async_trait]
impl Listener for WebSocketListener {
    type TransportInstance = WebSocketTransport;

    async fn bind(addr: &SocketAddr, tls_acceptor: Option<TlsAcceptor>, logger: Logger) -> Result<Self>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr).await {
            Ok(l) => Ok(WebSocketListener {
                listener: l,
                tls_acceptor,
                transport_cb_handler: None,
                logger,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let WebSocketListener {
            listener,
            tls_acceptor,
            transport_cb_handler,
            logger,
        } = self;

        Box::pin(tokio::stream::StreamExt::map(listener, move |stream| {
            let tcp_stream = stream?;

            let cbh = transport_cb_handler.clone();
            let logger_clone = logger.clone();
            Ok(
                Box::pin(accept_stream(tcp_stream, tls_acceptor.clone(), cbh, logger_clone))
                    as CowFuture<Self::TransportInstance>,
            )
        })) as CowStream<CowFuture<Self::TransportInstance>>
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}

async fn accept_stream(
    raw_stream: TcpStream,
    tls_acceptor: Option<TlsAcceptor>,
    cbh: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
) -> Result<WebSocketTransport> {
    if let Some(tls_acceptor) = tls_acceptor {
        let tls_stream = tls_acceptor
            .accept(raw_stream)
            .await
            .map_err(|e| CowRpcError::from(TransportError::TlsError(e.to_string())))?;
        let ws_stream = accept_async(tls_stream).await?;
        Ok(WebSocketTransport::new_server(
            CowWebSocketStream::AcceptTlsStream(ws_stream),
            cbh,
            logger,
        ))
    } else {
        let ws_stream = accept_async(raw_stream).await?;
        Ok(WebSocketTransport::new_server(
            CowWebSocketStream::AcceptStream(ws_stream),
            cbh,
            logger,
        ))
    }
}
