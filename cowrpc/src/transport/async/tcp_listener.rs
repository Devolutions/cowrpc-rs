use crate::error::CowRpcError;
use futures_01::{
    future::ok,
    Stream,
};
use std::net::SocketAddr;
use crate::transport::{
    r#async::{Listener, CowFuture, CowStream, tcp::TcpTransport},
    MessageInterceptor,
    tls::TlsOptions,
};
use tokio::net::TcpListener as TcpTokioListener;
use async_trait::async_trait;
use tokio::stream::StreamExt;
use crate::transport::r#async::Transport;
use futures::future;

pub struct TcpListener {
    listener: TcpTokioListener,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
}

#[async_trait]
impl Listener for TcpListener {
    type TransportInstance = TcpTransport;

    async fn bind(addr: &SocketAddr) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr).await {
            Ok(l) => {
                Ok(TcpListener {
                    listener: l,
                    transport_cb_handler: None,
                })
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let TcpListener {
            mut listener,
            transport_cb_handler,
        } = self;

        let cbh = if let Some(ref cbh) = transport_cb_handler {
            Some(cbh.clone_boxed())
        } else {
            None
        };

        let incoming = listener.incoming();
        Box::new(incoming.map(|stream| {
            let tcp_stream = stream?;
            Ok(Box::new(future::ok(TcpTransport::new(tcp_stream, cbh))) as CowFuture<TcpTransport>)
        })) as CowStream<CowFuture<Self::TransportInstance>>
    }

    fn set_tls_options(&mut self, _tls_opt: TlsOptions) {
        warn!("Tls is not implemented over tcp")
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}
