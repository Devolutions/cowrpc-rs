use std::net::SocketAddr;
use std::time::Duration;

use futures::future::{err, ok};
use futures::{Future, Stream};
use tokio::net::{TcpListener as TcpTokioListener, TcpStream};
use tokio::prelude::*;
use tokio::runtime::TaskExecutor;

use tls_api::HandshakeError as TlsHandshakeError;
use tls_api::{TlsAcceptor, TlsAcceptorBuilder, TlsStream};
use tls_api_native_tls::TlsAcceptor as NativeTlsAcceptor;
use tls_api_native_tls::TlsAcceptorBuilder as NativeTlsAcceptorBuilder;
use tungstenite::HandshakeError;
use tungstenite::{
    handshake::server::{NoCallback, ServerHandshake},
    stream::Stream as StreamSwitcher,
};

use crate::error::{CowRpcError, Result};
use crate::transport::{
    r#async::{Listener, CowFuture, CowStream, websocket::{ServerWebSocketHandshake, TlsHandshake, WebSocketTransport}},
    MessageInterceptor, TransportError,
    tls::{Identity, TlsOptions, TlsOptionsType},
};

pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

fn wrap_stream_async(tls_acceptor: &Option<NativeTlsAcceptor>, stream: TcpStream) -> CowFuture<WebSocketStream> {
    if let Some(ref acceptor) = tls_acceptor {
        match acceptor.accept(stream) {
            Ok(tls) => Box::new(ok(StreamSwitcher::Tls(tls))),
            Err(TlsHandshakeError::Interrupted(mid_hand)) => {
                let tls_hand = TlsHandshake(Some(mid_hand));

                Box::new(
                    tls_hand
                        .timeout(Duration::from_secs(5))
                        .map_err(|e| match e.into_inner() {
                            Some(e) => e,
                            None => CowRpcError::Proto("Tls handshake timed out after 5 sec".to_string()),
                        }).map(|tls_stream| StreamSwitcher::Tls(tls_stream)),
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
    executor_handle: Option<TaskExecutor>,
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
                    executor_handle: None,
                })
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>> {
        let WebSocketListener { listener, tls_acceptor, transport_cb_handler, executor_handle } = self;

        Box::new(listener.incoming().map_err(|e| e.into()).map(move |tcp_stream| {
            let tls_acceptor_clone = match tls_acceptor {
                None => None,
                Some(ref acceptor) => Some(NativeTlsAcceptor(acceptor.0.clone()))
            };
            let transport_cb_handler_clone = transport_cb_handler.clone();
            let executor_clone = executor_handle.clone();

            let fut: CowFuture<WebSocketTransport> = Box::new(
                wrap_stream_async(&tls_acceptor_clone, tcp_stream).and_then(move |ws_stream| {
                    let fut: CowFuture<WebSocketTransport> =
                        match ServerHandshake::start(ws_stream, NoCallback, None).handshake() {
                            Ok(ws) => Box::new(ok(WebSocketTransport::new_server(
                                ws,
                                transport_cb_handler_clone.clone(),
                                executor_clone.clone(),
                            ))),
                            Err(HandshakeError::Interrupted(m)) => Box::new(
                                ServerWebSocketHandshake(Some(m))
                                    .timeout(Duration::from_secs(5))
                                    .map_err(|e| match e.into_inner() {
                                        Some(e) => e,
                                        None => CowRpcError::Proto("Tls handshake timed out after 5 sec".to_string()),
                                    }).map(move |ws| {
                                        WebSocketTransport::new_server(
                                            ws,
                                            transport_cb_handler_clone.clone(),
                                            executor_clone.clone(),
                                        )
                                    }),
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

    fn set_executor_handle(&mut self, handle: TaskExecutor) {
        self.executor_handle = Some(handle)
    }
}
