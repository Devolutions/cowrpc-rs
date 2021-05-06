use std::net::SocketAddr;
use std::time::Duration;

use futures::prelude::*;

use super::*;
use crate::error::CowRpcError;

use crate::transport::r#async::utils::{load_certs, load_private_key};
use crate::transport::TlsOptions;
use crate::CowRpcMessage;
use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;
use tokio_rustls::{rustls, TlsAcceptor};

pub mod adaptor;
mod interceptor;
mod tcp;
mod tcp_listener;
mod utils;
mod websocket;
mod websocket_listener;

pub type CowFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
pub type CowStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;
pub type CowSink<T> = Pin<Box<dyn Sink<T, Error = CowRpcError> + Send>>;

#[async_trait]
pub trait Listener {
    type TransportInstance: Transport;

    async fn bind(addr: &SocketAddr, tls_connector: Option<TlsAcceptor>) -> Result<Self>
    where
        Self: Sized;
    async fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>>;
    fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>);
    fn set_executor_handle(&mut self) {
        /* just drop it */
    }
}

pub struct ListenerBuilder {
    interface: Option<SocketAddr>,
    proto: Option<SupportedProto>,
    tls_options: Option<TlsOptions>,
    interceptor: Option<Box<dyn MessageInterceptor>>,
    needs_tls: bool,
}

impl ListenerBuilder {
    pub fn new() -> Self {
        ListenerBuilder {
            interface: None,
            proto: None,
            tls_options: None,
            interceptor: None,
            needs_tls: false,
        }
    }

    pub fn from_uri(uri: &str) -> Result<Self> {
        let uri: Uri = uri
            .parse()
            .map_err(|parse_error: crate::transport::uri::UriError| CowRpcError::Internal(parse_error.to_string()))?;

        let needs_tls;

        let (mut port, proto) = match uri.scheme() {
            Some("tcp") => {
                needs_tls = false;
                (80, SupportedProto::Tcp)
            }
            Some("tls") => {
                needs_tls = true;
                (443, SupportedProto::Tcp)
            }
            Some("ws") => {
                needs_tls = false;
                (80, SupportedProto::WebSocket)
            }
            Some("wss") => {
                needs_tls = true;
                (443, SupportedProto::WebSocket)
            }
            Some(scheme) => {
                return Err(CowRpcError::Transport(TransportError::InvalidProtocol(String::from(
                    scheme,
                ))));
            }
            None => {
                return Err(CowRpcError::Transport(TransportError::InvalidProtocol(String::from(
                    "No protocol specified",
                ))));
            }
        };

        let ip = {
            if let Ok(addrs) = uri.get_addrs() {
                addrs[0]
            } else {
                return Err(TransportError::InvalidUrl(String::from("No local ipAddr specified")).into());
            }
        };

        if let Some(p) = uri.port() {
            port = p
        }

        let sock_addr = SocketAddr::new(ip, port);

        Ok(ListenerBuilder {
            interface: Some(sock_addr),
            proto: Some(proto),
            tls_options: None,
            interceptor: None,
            needs_tls,
        })
    }

    pub fn protocol(mut self, proto: SupportedProto) -> Self {
        self.proto = Some(proto);
        self
    }

    pub fn listen_on(mut self, addr: SocketAddr) -> Self {
        self.interface = Some(addr);
        self
    }

    pub fn msg_interceptor(mut self, inter: Box<dyn MessageInterceptor>) -> Self {
        self.interceptor = Some(inter);
        self
    }

    pub fn with_ssl(mut self, tls_opts: TlsOptions) -> Self {
        self.tls_options = Some(tls_opts);
        self
    }

    pub async fn build(self) -> Result<CowRpcListener> {
        if self.needs_tls && self.tls_options.is_none() {
            return Err(CowRpcError::Internal(
                "Unable to build listener, the configuration loaded needed tls options but none where given"
                    .to_string(),
            ));
        }

        let interface = &self
            .interface
            .ok_or_else(|| CowRpcError::Internal("Unable to build listener, socket addr".to_string()))?;

        // Build TLS acceptor if needed

        let mut tls_acceptor = None;
        if let Some(tls_option) = self.tls_options {
            let client_no_auth = rustls::NoClientAuth::new();
            let mut server_config = rustls::ServerConfig::new(client_no_auth);
            let certs = load_certs(&tls_option.certificate_file_path)?;
            let priv_key = load_private_key(&tls_option.private_key_file_path)?;
            server_config
                .set_single_cert(certs, priv_key)
                .map_err(|e| CowRpcError::Transport(TransportError::TlsError(e.to_string())))?;
            let config_ref = Arc::new(server_config);
            tls_acceptor = Some(TlsAcceptor::from(config_ref));
        }

        // Build the listener itself

        let mut listener = match self.proto {
            Some(SupportedProto::Tcp) => tcp_listener::TcpListener::bind(interface, tls_acceptor)
                .await
                .map(|l| CowRpcListener::Tcp(l))?,
            Some(SupportedProto::WebSocket) => websocket_listener::WebSocketListener::bind(interface, tls_acceptor)
                .await
                .map(|l| CowRpcListener::WebSocket(l))?,
            _ => {
                return Err(CowRpcError::Internal(
                    "Unable to build listener, missing protocol".to_string(),
                ));
            }
        };

        if let Some(interceptor) = self.interceptor {
            listener.set_msg_interceptor(interceptor);
        }

        Ok(listener)
    }
}

pub enum CowRpcListener {
    Tcp(tcp_listener::TcpListener),
    WebSocket(websocket_listener::WebSocketListener),
}

impl CowRpcListener {
    pub fn builder() -> ListenerBuilder {
        ListenerBuilder::new()
    }

    pub async fn incoming(self) -> CowStream<CowFuture<CowRpcTransport>> {
        match self {
            CowRpcListener::Tcp(tcp) => {
                let incoming = tcp.incoming().await;
                Box::pin(futures::StreamExt::map(incoming, |result| {
                    let fut = result?;
                    let cow_fut = Box::pin(fut.and_then(|transport| future::ok(CowRpcTransport::Tcp(transport))))
                        as CowFuture<CowRpcTransport>;
                    Ok(cow_fut)
                })) as CowStream<CowFuture<CowRpcTransport>>
            }
            CowRpcListener::WebSocket(ws) => {
                let incoming = ws.incoming().await;
                Box::pin(futures::StreamExt::map(incoming, |result| {
                    let fut = result?;
                    let cow_fut = Box::pin(fut.and_then(|transport| future::ok(CowRpcTransport::WebSocket(transport))))
                        as CowFuture<CowRpcTransport>;
                    Ok(cow_fut)
                })) as CowStream<CowFuture<CowRpcTransport>>
            }
        }
    }

    pub fn set_msg_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        match self {
            CowRpcListener::Tcp(ref mut tcp) => tcp.set_msg_interceptor(cb_handler),
            CowRpcListener::WebSocket(ref mut ws) => ws.set_msg_interceptor(cb_handler),
        }
    }
}

#[async_trait]
pub trait Transport {
    async fn connect(uri: Uri) -> Result<Self>
    where
        Self: Sized;
    fn message_stream_sink(self) -> (CowStream<CowRpcMessage>, CowSink<CowRpcMessage>);
    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>);
    fn set_keep_alive_interval(&mut self, interval: Duration);
    fn local_addr(&self) -> Option<SocketAddr>;
    fn remote_addr(&self) -> Option<SocketAddr>;
    fn up_time(&self) -> Duration;
}

pub enum CowRpcTransport {
    Tcp(tcp::TcpTransport),
    WebSocket(websocket::WebSocketTransport),
    Interceptor(interceptor::InterceptorTransport),
}

impl CowRpcTransport {
    pub fn from_interceptor(inter: Box<dyn MessageInterceptor>) -> CowRpcTransport {
        CowRpcTransport::Interceptor(interceptor::InterceptorTransport { inter })
    }
}

#[async_trait]
impl Transport for CowRpcTransport {
    async fn connect(uri: Uri) -> Result<Self>
    where
        Self: Sized,
    {
        if let Some(scheme) = uri.clone().scheme() {
            match scheme {
                "tcp" => tcp::TcpTransport::connect(uri)
                    .await
                    .map(|transport| CowRpcTransport::Tcp(transport)),
                "ws" | "wss" => websocket::WebSocketTransport::connect(uri)
                    .await
                    .map(|transport| CowRpcTransport::WebSocket(transport)),
                _ => Err(TransportError::InvalidUrl("Bad scheme provided".to_string()).into()),
            }
        } else {
            Err(TransportError::InvalidUrl("No scheme provided".to_string()).into())
        }
    }

    fn message_stream_sink(self) -> (CowStream<CowRpcMessage>, CowSink<CowRpcMessage>) {
        match self {
            CowRpcTransport::Tcp(tcp) => tcp.message_stream_sink(),
            CowRpcTransport::WebSocket(ws) => ws.message_stream_sink(),
            CowRpcTransport::Interceptor(inter) => inter.message_stream_sink(),
        }
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        match self {
            CowRpcTransport::Tcp(ref mut tcp) => tcp.set_message_interceptor(cb_handler),
            CowRpcTransport::WebSocket(ref mut ws) => ws.set_message_interceptor(cb_handler),
            CowRpcTransport::Interceptor(ref mut inter) => inter.set_message_interceptor(cb_handler),
        }
    }

    fn set_keep_alive_interval(&mut self, interval: Duration) {
        match self {
            CowRpcTransport::Tcp(ref mut tcp) => tcp.set_keep_alive_interval(interval),
            CowRpcTransport::WebSocket(ref mut ws) => ws.set_keep_alive_interval(interval),
            CowRpcTransport::Interceptor(ref mut inter) => inter.set_keep_alive_interval(interval),
        }
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            CowRpcTransport::Tcp(ref tcp) => tcp.local_addr(),
            CowRpcTransport::WebSocket(ref ws) => ws.local_addr(),
            CowRpcTransport::Interceptor(ref inter) => inter.local_addr(),
        }
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        match self {
            CowRpcTransport::Tcp(ref tcp) => tcp.remote_addr(),
            CowRpcTransport::WebSocket(ref ws) => ws.remote_addr(),
            CowRpcTransport::Interceptor(ref inter) => inter.remote_addr(),
        }
    }

    fn up_time(&self) -> Duration {
        match self {
            CowRpcTransport::Tcp(ref tcp) => tcp.up_time(),
            CowRpcTransport::WebSocket(ref ws) => ws.up_time(),
            CowRpcTransport::Interceptor(ref inter) => inter.up_time(),
        }
    }
}

// impl Clone for CowRpcTransport {
//     fn clone(&self) -> Self {
//         match self {
//             CowRpcTransport::Tcp(ref tcp) => CowRpcTransport::Tcp(tcp.clone()),
//             // CowRpcTransport::WebSocket(ref ws) => CowRpcTransport::WebSocket(ws.clone()),
//             CowRpcTransport::Interceptor(ref inter) => CowRpcTransport::Interceptor(inter.clone()),
//         }
//     }
// }
