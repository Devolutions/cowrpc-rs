use std::net::SocketAddr;
use std::time::Duration;

use futures::{future::err, Future, Sink, Stream};
use tokio::runtime::TaskExecutor;

use super::*;
use error::CowRpcError;
use CowRpcMessage;
use transport::tls::TlsOptions;

mod tcp;
mod tcp_listener;
mod websocket;
mod websocket_listener;
mod interceptor;
pub mod adaptor;

pub type CowFuture<T> = Box<Future<Item = T, Error = CowRpcError> + Send>;
pub type CowStream<T> = Box<Stream<Item = T, Error = CowRpcError> + Send>;
pub type CowSink<T> = Box<Sink<SinkItem = T, SinkError = CowRpcError> + Send>;

pub trait Listener {
    type TransportInstance: Transport;

    fn bind(addr: &SocketAddr) -> Result<Self>
    where
        Self: Sized;
    fn incoming(self) -> CowStream<CowFuture<Self::TransportInstance>>;
    fn set_tls_options(&mut self, tls_opt: TlsOptions);
    fn set_msg_interceptor(&mut self, cb_handler: Box<MessageInterceptor>);
    fn set_executor_handle(&mut self, _handle: TaskExecutor) {
        /* just drop it */
    }
}

pub struct ListenerBuilder {
    interface: Option<SocketAddr>,
    proto: Option<SupportedProto>,
    tls_options: Option<TlsOptions>,
    interceptor: Option<Box<MessageInterceptor>>,
    executor: Option<TaskExecutor>,
    needs_tls: bool,
}

impl ListenerBuilder {
    pub fn new() -> Self {
        ListenerBuilder {
            interface: None,
            proto: None,
            tls_options: None,
            interceptor: None,
            executor: None,
            needs_tls: false,
        }
    }

    pub fn from_uri(uri: &str) -> Result<Self> {
        let uri: Uri = uri.parse().map_err(|parse_error: ::transport::uri::UriError| CowRpcError::Internal(parse_error.to_string()))?;

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
                return Err(CowRpcError::Transport(TransportError::InvalidProtocol(
                    String::from(scheme),
                )));
            }
            None => {
                return Err(CowRpcError::Transport(TransportError::InvalidProtocol(
                    String::from("No protocol specified"),
                )));
            }
        };

        let ip = {
            if let Ok(addrs) = uri.get_addrs() {
                addrs[0]
            } else {
                return Err(TransportError::InvalidUrl(String::from(
                    "No local ipAddr specified",
                )).into());
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
            executor: None,
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

    pub fn msg_interceptor(mut self, inter: Box<MessageInterceptor>) -> Self {
        self.interceptor = Some(inter);
        self
    }

    pub fn executor(mut self, handle: TaskExecutor) -> Self {
        self.executor = Some(handle);
        self
    }

    pub fn with_ssl(mut self, tls_opts: TlsOptions) -> Self {
        self.tls_options = Some(tls_opts);
        self
    }

    pub fn build(self) -> Result<CowRpcListener> {
        if self.needs_tls && self.tls_options.is_none() {
            return Err(CowRpcError::Internal("Unable to build listener, the configuration loaded needed tls options but none where given".to_string()));
        }

        let interface = match self.interface {
            Some(ref i) => i,
            None => return Err(CowRpcError::Internal("Unable to build listener, socket addr".to_string())),
        };

        let mut listener = match self.proto {
            Some(SupportedProto::Tcp) => {
                tcp_listener::TcpListener::bind(interface).map(|l| CowRpcListener::Tcp(l))?
            }
            Some(SupportedProto::WebSocket) => {
                websocket_listener::WebSocketListener::bind(interface).map(|l| CowRpcListener::WebSocket(l))?
            }
            _ => {
                return Err(CowRpcError::Internal("Unable to build listener, missing protocol".to_string()));
            }
        };


        if let Some(tls_options) = self.tls_options {
            listener.set_tls_options(tls_options);
        }

        if let Some(handle) = self.executor {
            listener.set_executor_handle(handle);
        }

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

    pub fn incoming(self) -> CowStream<CowFuture<CowRpcTransport>> {
        match self {
            CowRpcListener::Tcp(tcp) => {
                let fut: CowStream<CowFuture<CowRpcTransport>> = Box::new(tcp.incoming().map(|t| {
                    let fut: CowFuture<CowRpcTransport> = Box::new(t.map(|t| CowRpcTransport::Tcp(t)));
                    fut
                }));

                fut
            }
            CowRpcListener::WebSocket(ws) => {
                let fut: CowStream<CowFuture<CowRpcTransport>> = Box::new(ws.incoming().map(|t| {
                    let fut: CowFuture<CowRpcTransport> = Box::new(t.map(|t| CowRpcTransport::WebSocket(t)));
                    fut
                }));

                fut
            }
        }
    }

    pub fn set_tls_options(&mut self, tls_opt: TlsOptions) {
        match self {
            CowRpcListener::Tcp(ref mut tcp) => tcp.set_tls_options(tls_opt),
            CowRpcListener::WebSocket(ref mut ws) => ws.set_tls_options(tls_opt),
        }
    }

    pub fn set_msg_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        match self {
            CowRpcListener::Tcp(ref mut tcp) => tcp.set_msg_interceptor(cb_handler),
            CowRpcListener::WebSocket(ref mut ws) => ws.set_msg_interceptor(cb_handler),
        }
    }

    pub fn set_executor_handle(&mut self, handle: TaskExecutor) {
        match self {
            CowRpcListener::Tcp(ref mut tcp) => tcp.set_executor_handle(handle),
            CowRpcListener::WebSocket(ref mut ws) => ws.set_executor_handle(handle),
        }
    }
}

pub trait Transport {
    fn connect(uri: Uri) -> CowFuture<Self>
    where
        Self: Sized;
    fn message_sink(&mut self) -> CowSink<CowRpcMessage>;
    fn message_stream(&mut self) -> CowStream<CowRpcMessage>;
    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>);
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
    pub fn from_interceptor(inter: Box<MessageInterceptor>) -> CowRpcTransport {
        CowRpcTransport::Interceptor(interceptor::InterceptorTransport { inter })
    }
}

impl Transport for CowRpcTransport {
    fn connect(uri: Uri) -> CowFuture<Self>
    where
        Self: Sized,
    {
        if let Some(scheme) = uri.clone().scheme() {
            match scheme {
                "tcp" => {
                    Box::new(tcp::TcpTransport::connect(uri).map(|transport| CowRpcTransport::Tcp(transport)))
                }
                "ws" | "wss" => Box::new(
                    websocket::WebSocketTransport::connect(uri)
                        .map(|transport| CowRpcTransport::WebSocket(transport)),
                ),
                _ => Box::new(err(TransportError::InvalidUrl("Bad scheme provided".to_string()).into())),
            }
        } else {
            Box::new(err(TransportError::InvalidUrl("No scheme provided".to_string()).into()))
        }
    }

    fn message_sink(&mut self) -> CowSink<CowRpcMessage> {
        match self {
            CowRpcTransport::Tcp(ref mut tcp) => tcp.message_sink(),
            CowRpcTransport::WebSocket(ref mut ws) => ws.message_sink(),
            CowRpcTransport::Interceptor(ref mut inter) => inter.message_sink(),
        }
    }

    fn message_stream(&mut self) -> CowStream<CowRpcMessage> {
        match self {
            CowRpcTransport::Tcp(ref mut tcp) => tcp.message_stream(),
            CowRpcTransport::WebSocket(ref mut ws) => ws.message_stream(),
            CowRpcTransport::Interceptor(ref mut inter) => inter.message_stream(),
        }
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        match self {
            CowRpcTransport::Tcp(ref mut tcp) => tcp.set_message_interceptor(cb_handler),
            CowRpcTransport::WebSocket(ref mut ws) => ws.set_message_interceptor(cb_handler),
            CowRpcTransport::Interceptor(ref mut inter) => inter.set_message_interceptor(cb_handler),
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

impl Clone for CowRpcTransport {
    fn clone(&self) -> Self {
        match self {
            CowRpcTransport::Tcp(ref tcp) => CowRpcTransport::Tcp(tcp.clone()),
            CowRpcTransport::WebSocket(ref ws) => CowRpcTransport::WebSocket(ws.clone()),
            CowRpcTransport::Interceptor(ref inter) => CowRpcTransport::Interceptor(inter.clone()),
        }
    }
}
