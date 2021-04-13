use std::net::SocketAddr;

use futures::future::{err, ok};
use futures::{Future, Stream};

use tls_api::HandshakeError as TlsHandshakeError;
use tls_api::{TlsAcceptor, TlsAcceptorBuilder, TlsStream};
use tls_api_native_tls::TlsAcceptor as NativeTlsAcceptor;
use tls_api_native_tls::TlsAcceptorBuilder as NativeTlsAcceptorBuilder;
use tungstenite::HandshakeError;
use tungstenite::{
    handshake::server::{NoCallback, ServerHandshake},
    stream::Stream as StreamSwitcher,
};
use futures_03::compat::Future01CompatExt;
use futures_03::future::TryFutureExt;


use crate::error::{CowRpcError, Result};
use crate::transport::{
    r#async::{Listener, CowFuture, CowStream, websocket::{ServerWebSocketHandshake, TlsHandshake, WebSocketTransport}},
    MessageInterceptor, TransportError,
    tls::{Identity, TlsOptions, TlsOptionsType},
};
use tokio_tcp::TcpStream;
use tokio_tcp::TcpListener as TcpTokioListener;

pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

fn wrap_stream_async(tls_acceptor: &Option<NativeTlsAcceptor>, stream: TcpStream) -> CowFuture<WebSocketStream> {
    if let Some(ref acceptor) = tls_acceptor {
        match acceptor.accept(stream) {
            Ok(tls) => Box::new(ok(StreamSwitcher::Tls(tls))),
            Err(TlsHandshakeError::Interrupted(mid_hand)) => {
                let tls_hand = TlsHandshake(Some(mid_hand));

                Box::new(
                    tokio::time::timeout(::std::time::Duration::from_secs(5),
                                         tls_hand.compat()).compat()
                        .map_err(|_| CowRpcError::Internal("timed out".to_string()))
                        .and_then(move |result| {
                            match result {
                                Ok(tls_stream) => {
                                    ok(StreamSwitcher::Tls(tls_stream))
                                }
                                Err(e) => {
                                    err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                                }
                            }
                        })
                )
            }
            Err(e) => {
                trace!("ERROR : Tls Handshake failed with {:?}", e);
                Box::new(err(TransportError::UnableToConnect.into()))
            }
        }
    } else {
        Box::new(ok(StreamSwitcher::Plain(stream)))
    }
}

pub struct WebSocketListener {
    listener: TcpTokioListener,
    tls_acceptor: Option<NativeTlsAcceptor>,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
}

impl Listener for WebSocketListener {
    type TransportInstance = WebSocketTransport;

    fn bind(addr: &SocketAddr) -> Result<Self>
    where
        Self: Sized,
    {
        match TcpTokioListener::bind(addr) {
            Ok(l) => {
                Ok(WebSocketListener {
                    listener: l,
                    tls_acceptor: None,
                    transport_cb_handler: None,
                })
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let WebSocketListener { listener, tls_acceptor, transport_cb_handler } = self;

        Box::new(listener.incoming().map_err(|e| e.into()).map(move |tcp_stream| {
            let tls_acceptor_clone = match tls_acceptor {
                None => None,
                Some(ref acceptor) => Some(NativeTlsAcceptor(acceptor.0.clone()))
            };
            let transport_cb_handler_clone = transport_cb_handler.clone();

            let fut: CowFuture<WebSocketTransport> = Box::new(
                wrap_stream_async(&tls_acceptor_clone, tcp_stream).and_then(move |ws_stream| {
                    let fut: CowFuture<WebSocketTransport> =
                        match ServerHandshake::start(ws_stream, NoCallback, None).handshake() {
                            Ok(ws) => Box::new(ok(WebSocketTransport::new_server(
                                ws,
                                transport_cb_handler_clone.clone(),
                            ))),
                            Err(HandshakeError::Interrupted(m)) => Box::new(

                                tokio::time::timeout(::std::time::Duration::from_secs(5),
                                                     ServerWebSocketHandshake(Some(m)).compat()).compat()
                                    .map_err(|_| CowRpcError::Internal("timed out".to_string()))
                                    .and_then(move |result| {
                                        match result {
                                            Ok(ws) => {
                                                ok(WebSocketTransport::new_server(
                                                    ws,
                                                    transport_cb_handler_clone.clone(),
                                                ))
                                            }
                                            Err(e) => {
                                                err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                                            }
                                        }
                                    })
                            ),
                            Err(e) => {
                                trace!("ERROR : Handshake failed with {}", e);
                                Box::new(err(TransportError::UnableToConnect.into()))
                            }
                        };
                    fut
                }),
            );
            fut
        }))
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
                    .and_then(|a| a.build()) {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Unable to set new tls options on listener : {:?}", e);
                        return
                    },
                };

                self.tls_acceptor = Some(acceptor);
            }
            
            Identity::None => unreachable!()
        }

    }

    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}
