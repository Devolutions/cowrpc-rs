use error::CowRpcError;
use futures::{
    future::ok,
    Stream,
};
use std::net::SocketAddr;
use tokio::net::TcpListener as TcpTokioListener;
use transport::{
    async::{Listener, CowFuture, CowStream, tcp::TcpTransport},
    MessageInterceptor,
    tls::TlsOptions,
};

pub struct TcpListener {
    listener: TcpTokioListener,
    transport_cb_handler: Option<Box<MessageInterceptor>>,
}

impl Listener for TcpListener {
    type TransportInstance = TcpTransport;

    fn bind(addr: &SocketAddr) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr) {
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
            listener,
            transport_cb_handler,
        } = self;
        Box::new(listener.incoming().map_err(|e| e.into()).map(move |stream| {
            let cbh = if let Some(ref cbh) = transport_cb_handler {
                Some(cbh.clone_boxed())
            } else {
                None
            };

            let fut: CowFuture<Self::TransportInstance> =
                Box::new(ok::<Self::TransportInstance, CowRpcError>(TcpTransport::new(stream, cbh)));

            fut
        }))
    }

    fn set_tls_options(&mut self, _tls_opt: TlsOptions) {
        warn!("Tls is not implemented over tcp")
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}
