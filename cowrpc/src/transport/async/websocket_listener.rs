use std::net::SocketAddr;

use tls_api::TlsAcceptorBuilder;
use tls_api_native_tls::{TlsAcceptor as NativeTlsAcceptor, TlsAcceptorBuilder as NativeTlsAcceptorBuilder};

use crate::error::{CowRpcError, Result};
use crate::transport::r#async::websocket::{CowWebSocketStream, WebSocketTransport};
use crate::transport::r#async::{CowFuture, CowStream, Listener};
use crate::transport::tls::{Identity, TlsOptions, TlsOptionsType};
use crate::transport::{MessageInterceptor, TransportError};
use async_trait::async_trait;
use async_tungstenite::tokio::accept_async;

use tokio::net::{TcpListener as TcpTokioListener, TcpStream};

// pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

// fn wrap_stream_async(tls_acceptor: &Option<NativeTlsAcceptor>, stream: TcpStream) -> CowFuture<WebSocketStream> {
//     if let Some(ref acceptor) = tls_acceptor {
//         match acceptor.accept(stream) {
//             Ok(tls) => Box::new(ok(StreamSwitcher::Tls(tls))),
//             Err(TlsHandshakeError::Interrupted(mid_hand)) => {
//                 let tls_hand = TlsHandshake(Some(mid_hand));
//
//                 Box::new(
//                     tokio::time::timeout(::std::time::Duration::from_secs(5),
//                                          tls_hand.compat()).compat()
//                         .map_err(|_| CowRpcError::Internal("timed out".to_string()))
//                         .and_then(move |result| {
//                             match result {
//                                 Ok(tls_stream) => {
//                                     ok(StreamSwitcher::Tls(tls_stream))
//                                 }
//                                 Err(e) => {
//                                     err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
//                                 }
//                             }
//                         })
//                 )
//             }
//             Err(e) => {
//                 trace!("ERROR : Tls Handshake failed with {:?}", e);
//                 Box::new(err(TransportError::UnableToConnect.into()))
//             }
//         }
//     } else {
//         Box::new(ok(StreamSwitcher::Plain(stream)))
//     }
// }

pub struct WebSocketListener {
    listener: TcpTokioListener,
    tls_acceptor: Option<NativeTlsAcceptor>,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
}

#[async_trait]
impl Listener for WebSocketListener {
    type TransportInstance = WebSocketTransport;

    async fn bind(addr: &SocketAddr) -> Result<Self>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr).await {
            Ok(l) => Ok(WebSocketListener {
                listener: l,
                tls_acceptor: None,
                transport_cb_handler: None,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let WebSocketListener {
            listener,
            tls_acceptor: _,
            transport_cb_handler,
        } = self;

        Box::pin(tokio::stream::StreamExt::map(listener, move |stream| {
            let tcp_stream = stream?;
            let cbh = transport_cb_handler.clone();
            Ok(Box::pin(accept_stream(tcp_stream, cbh)) as CowFuture<Self::TransportInstance>)
        })) as CowStream<CowFuture<Self::TransportInstance>>
    }

    fn set_tls_options(&mut self, tls_opt: TlsOptions) {
        let TlsOptions {
            tls_version: _,
            identity,
            root_certificates: _,
            ty,
        } = tls_opt;
        assert_eq!(ty, TlsOptionsType::Acceptor);

        match identity {
            Identity::Pkcs12(pkcs12) => {
                let acceptor = match NativeTlsAcceptorBuilder::from_pkcs12(&pkcs12.der, &pkcs12.password)
                    .and_then(|a| a.build())
                {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Unable to set new tls options on listener : {:?}", e);
                        return;
                    }
                };

                self.tls_acceptor = Some(acceptor);
            }

            Identity::None => unreachable!(),
        }
    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}

async fn accept_stream(raw_stream: TcpStream, cbh: Option<Box<dyn MessageInterceptor>>) -> Result<WebSocketTransport> {
    match accept_async(raw_stream).await {
        Ok(ws_stream) => Ok(WebSocketTransport::new_server(
            CowWebSocketStream::AcceptStream(ws_stream),
            cbh,
        )),
        Err(e) => {
            error!("{:?}", e);
            Err(CowRpcError::from(TransportError::Other))
        }
    }
}
