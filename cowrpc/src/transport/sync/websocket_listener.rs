use std;
use std::net::SocketAddr;

use mio::net::TcpListener as MioTcpListener;
use mio::{net::TcpStream, Evented, Poll, PollOpt, Ready, Token};
use timer::Timer;
use async_tungstenite::tungstenite::{
    handshake::server::{NoCallback, ServerHandshake},
    stream::Stream as StreamSwitcher,
};
use tls_api::{TlsAcceptor, TlsAcceptorBuilder, TlsStream, MidHandshakeTlsStream};
use tls_api::HandshakeError as TlsHandshakeError;
use tls_api_native_tls::TlsAcceptor as NativeTlsAcceptor;
use tls_api_native_tls::TlsAcceptorBuilder as NativeTlsAcceptorBuilder;

use crate::error::Result;
use crate::transport::tls::{TlsOptions, TlsOptionsType, Identity};
use crate::transport::sync::websocket::WebSocketTransport;
use crate::transport::{sync::Listener, MessageInterceptor, TransportError};

const PING_INTERVAL: i64 = 120;

pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

enum WrappedStreamResult {
    Wrapped(WebSocketStream),
    TlsHandshake(MidHandshakeTlsStream<TcpStream>, TcpStream),
}

pub struct WebSocketListener {
    listener: MioTcpListener,
    tls_acceptor: Option<NativeTlsAcceptor>,
    transport_cb_handler: Option<Box<dyn MessageInterceptor>>,
    ping_timer: Timer,
}

impl WebSocketListener {
    fn wrap_stream(&self, stream: TcpStream) -> Result<WrappedStreamResult> {
        if let Some(ref acceptor) = self.tls_acceptor {
            match acceptor.accept(stream.try_clone()?) {
                Ok(tls) => Ok(WrappedStreamResult::Wrapped(StreamSwitcher::Tls(tls))),
                Err(TlsHandshakeError::Interrupted(mid_hand)) => Ok(WrappedStreamResult::TlsHandshake(mid_hand, stream)),
                Err(e) => {
                    error!("Tls Handshake failed with {:?}", e);
                    return Err(TransportError::UnableToConnect.into());
                }
            }
        } else {
            Ok(WrappedStreamResult::Wrapped(StreamSwitcher::Plain(stream)))
        }
    }
}

impl Listener for WebSocketListener {
    type Transport = WebSocketTransport;

    fn bind(addr: &SocketAddr) -> Result<Self> where Self: Sized {
        let listener = match MioTcpListener::bind(addr) {
            Ok(l) => l,
            Err(_) => return Err(TransportError::Other.into()),
        };

        Ok(WebSocketListener {
            listener,
            tls_acceptor: None,
            transport_cb_handler: None,
            ping_timer: Timer::new(),
        })
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

    fn accept(&self) -> Option<Self::Transport> {
        match self.listener.accept() {
            Ok((stream, _)) => {
                if let Ok(stream_clone) = stream.try_clone() {
                    if let Ok(wrapped_stream_res) = self.wrap_stream(stream_clone) {
                        match wrapped_stream_res {
                            WrappedStreamResult::Wrapped(ws_stream) => {
                                let handshake = ServerHandshake::start(ws_stream, NoCallback, None);
                                return Some(WebSocketTransport::new_server(
                                    handshake,
                                    stream,
                                    Some((PING_INTERVAL, &self.ping_timer)),
                                ));
                            }
                            WrappedStreamResult::TlsHandshake(mid, stream) => {
                                return Some(WebSocketTransport::new_server_secure(
                                    mid,
                                    stream,
                                    Some((PING_INTERVAL, &self.ping_timer)),
                                ));
                            }
                        }
                    }

                    error!("Failed to wrap ssl stream for new connection");
                    return None;
                }

                error!("Failed to clone stream for new connection");
                None
            }
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => None,
            Err(e) => {
                error!("Failed to accept new connection: {}", e);
                None
            }
        }
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }
}

impl Evented for WebSocketListener {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        self.listener.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        self.listener.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> std::io::Result<()> {
        self.listener.deregister(poll)
    }
}
