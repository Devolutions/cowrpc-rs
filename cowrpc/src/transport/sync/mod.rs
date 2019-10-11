mod tcp;
mod tcp_listener;
mod websocket;
mod websocket_listener;
mod interceptor;
pub mod adaptor;

use super::*;
use crate::transport::tls::TlsOptions;

trait Listener {
    type Transport: Transport;

    fn bind(addr: &SocketAddr) -> Result<Self> where Self: Sized;
    fn set_tls_options(&mut self, tls_opt: TlsOptions);
    fn accept(&self) -> Option<Self::Transport>;
    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>);
}

trait Transport {
    fn get_interest(&self) -> Ready;
    fn send_data(&mut self) -> Result<()>;
    fn send_message(&mut self, msg: CowRpcMessage) -> Result<()>;
    fn get_next_message(&mut self) -> Result<Option<CowRpcMessage>>;
    fn read_data(&mut self) -> Result<()>;
    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>);
    fn local_addr(&self) -> Option<SocketAddr>;
    fn remote_addr(&self) -> Option<SocketAddr>;
    fn up_time(&self) -> std::time::Duration;
    fn shutdown(&mut self) -> Result<()>;
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
        let uri: Uri = uri.parse().map_err(|parse_error: crate::transport::uri::UriError| CowRpcError::Internal(parse_error.to_string()))?;

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
    pub fn accept(&self) -> Option<CowRpcTransport> {
        match *self {
            CowRpcListener::Tcp(ref l) => match l.accept() {
                Some(tcp) => Some(CowRpcTransport::Tcp(tcp)),
                _ => None,
            },
            CowRpcListener::WebSocket(ref l) => match l.accept() {
                Some(ws) => Some(CowRpcTransport::WebSocket(ws)),
                _ => None,
            },
        }
    }

    pub fn set_msg_interceptor(&mut self, interceptor: Box<dyn MessageInterceptor>) {
        match *self {
            CowRpcListener::Tcp(ref mut l) => l.set_message_interceptor(interceptor),
            CowRpcListener::WebSocket(ref mut l) => l.set_message_interceptor(interceptor),
        }
    }

    pub fn set_tls_options(&mut self, tls_opt: TlsOptions) {
        match *self {
            CowRpcListener::Tcp(ref mut l) => l.set_tls_options(tls_opt),
            CowRpcListener::WebSocket(ref mut l) => l.set_tls_options(tls_opt),
        }
    }
}

impl Evented for CowRpcListener {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        match *self {
            CowRpcListener::Tcp(ref l) => l.register(poll, token, interest, opts),
            CowRpcListener::WebSocket(ref l) => l.register(poll, token, interest, opts),
        }
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        match *self {
            CowRpcListener::Tcp(ref l) => l.reregister(poll, token, interest, opts),
            CowRpcListener::WebSocket(ref l) => l.reregister(poll, token, interest, opts),
        }
    }

    fn deregister(&self, poll: &Poll) -> std::io::Result<()> {
        match *self {
            CowRpcListener::Tcp(ref l) => l.deregister(poll),
            CowRpcListener::WebSocket(ref l) => l.deregister(poll),
        }
    }
}

pub enum CowRpcTransport {
    Tcp(tcp::TcpTransport),
    Tls,
    WebSocket(websocket::WebSocketTransport),
    SecureWebSocket(websocket::WebSocketTransport),
    Interceptor(interceptor::InterceptorTransport),
}

use self::CowRpcTransport::*;

impl CowRpcTransport {
    pub fn from_interceptor(inter: Box<dyn MessageInterceptor>) -> CowRpcTransport {
        Interceptor(interceptor::InterceptorTransport {
            inter,
        })
    }

    pub fn from_url(url: &str, tls_options: Option<TlsOptions>) -> Result<CowRpcTransport> {
        let uri: uri::Uri = String::from(url).parse().unwrap();
        match uri.scheme() {
            Some("tcp") => match tcp::TcpTransport::connect(uri.clone()) {
                Ok(t) => Ok(CowRpcTransport::Tcp(t)),
                Err(e) => Err(e)
            },
            Some("tls") => {
                Err(CowRpcError::Transport(TransportError::InvalidProtocol(String::from(
                    "tls",
                ))))
            }
            Some("ws") => match websocket::WebSocketTransport::connect(uri.clone(), None) {
                Ok(t) => Ok(CowRpcTransport::WebSocket(t)),
                Err(e) => Err(e),
            },
            Some("wss") => match websocket::WebSocketTransport::connect(uri.clone(), tls_options) {
                Ok(t) => Ok(CowRpcTransport::SecureWebSocket(t)),
                Err(e) => Err(e),
            },
            Some(scheme) => {
                Err(CowRpcError::Transport(TransportError::InvalidProtocol(String::from(
                    scheme,
                ))))
            }
            None => {
                Err(CowRpcError::Transport(TransportError::InvalidProtocol(String::from(
                    "No protocol specified",
                ))))
            }
        }
    }

    pub fn get_interest(&self) -> Ready {
        match self {
            Tcp(ref tcp) => tcp.get_interest(),
            WebSocket(ref ws) => ws.get_interest(),
            SecureWebSocket(ref wss) => wss.get_interest(),
            Interceptor(ref int) => int.get_interest(),
            _ => unreachable!(),
        }
    }

    pub fn send_data(&mut self) -> Result<()> {
        match self {
            Tcp(ref mut tcp) => tcp.send_data(),
            WebSocket(ref mut ws) => ws.send_data(),
            SecureWebSocket(ref mut wss) => wss.send_data(),
            Interceptor(ref mut int) => int.send_data(),
            _ => unreachable!(),
        }
    }

    pub fn send_message(&mut self, msg: CowRpcMessage) -> Result<()> {
        match self {
            Tcp(ref mut tcp) => tcp.send_message(msg),
            WebSocket(ref mut ws) => ws.send_message(msg),
            SecureWebSocket(ref mut wss) => wss.send_message(msg),
            Interceptor(ref mut int) => int.send_message(msg),
            _ => unreachable!(),
        }
    }

    pub fn get_next_message(&mut self) -> Result<Option<CowRpcMessage>> {
        match self {
            Tcp(ref mut tcp) => tcp.get_next_message(),
            WebSocket(ref mut ws) => ws.get_next_message(),
            SecureWebSocket(ref mut wss) => wss.get_next_message(),
            Interceptor(ref mut int) => int.get_next_message(),
            _ => unreachable!(),
        }
    }

    pub fn read_data(&mut self) -> Result<()> {
        match self {
            Tcp(ref mut tcp) => tcp.read_data(),
            WebSocket(ref mut ws) => ws.read_data(),
            SecureWebSocket(ref mut wss) => wss.read_data(),
            Interceptor(ref mut int) => int.read_data(),
            _ => unreachable!(),
        }
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            Tcp(ref tcp) => tcp.local_addr(),
            WebSocket(ref ws) => ws.local_addr(),
            SecureWebSocket(ref wss) => wss.local_addr(),
            Interceptor(ref int) => int.local_addr(),
            _ => unreachable!(),
        }
    }

    pub fn remote_addr(&self) -> Option<SocketAddr> {
        match self {
            Tcp(ref tcp) => tcp.remote_addr(),
            WebSocket(ref ws) => ws.remote_addr(),
            SecureWebSocket(ref wss) => wss.remote_addr(),
            Interceptor(ref int) => int.remote_addr(),
            _ => unreachable!(),
        }
    }

    pub fn up_time(&self) -> std::time::Duration {
        match self {
            Tcp(ref tcp) => tcp.up_time(),
            WebSocket(ref ws) => ws.up_time(),
            SecureWebSocket(ref wss) => wss.up_time(),
            Interceptor(ref int) => int.up_time(),
            _ => unreachable!(),
        }
    }

    pub fn shutdown(&mut self) -> Result<()> {
        match self {
            Tcp(ref mut tcp) => tcp.shutdown(),
            WebSocket(ref mut ws) => ws.shutdown(),
            SecureWebSocket(ref mut wss) => wss.shutdown(),
            Interceptor(ref mut int) => int.shutdown(),
            _ => unreachable!(),
        }
    }
}

impl Evented for CowRpcTransport {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        match self {
            Tcp(ref tcp) => tcp.register(poll, token, interest, opts),
            WebSocket(ref ws) => ws.register(poll, token, interest, opts),
            SecureWebSocket(ref wss) => wss.register(poll, token, interest, opts),
            _ => unreachable!(),
        }
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        match self {
            Tcp(ref tcp) => tcp.reregister(poll, token, interest, opts),
            WebSocket(ref ws) => ws.reregister(poll, token, interest, opts),
            SecureWebSocket(ref wss) => wss.reregister(poll, token, interest, opts),
            _ => unreachable!(),
        }
    }

    fn deregister(&self, poll: &Poll) -> std::io::Result<()> {
        match self {
            Tcp(ref tcp) => tcp.deregister(poll),
            WebSocket(ref ws) => ws.deregister(poll),
            SecureWebSocket(ref wss) => wss.deregister(poll),
            _ => unreachable!(),
        }
    }
}