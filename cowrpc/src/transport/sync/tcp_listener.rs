use error::Result;
use mio::net::TcpListener as MioTcpListener;
use mio::{Evented, Poll, PollOpt, Ready, Token};
use std;
use std::net::SocketAddr;
use transport::sync::tcp::TcpTransport;
use transport::{sync::Listener, MessageInterceptor, TransportError};
use transport::tls::TlsOptions;

pub struct TcpListener {
    listener: MioTcpListener,
    transport_cb_handler: Option<Box<MessageInterceptor>>,
}

impl Listener for TcpListener {
    type Transport = TcpTransport;

    fn bind(addr: &SocketAddr) -> Result<Self> where Self: Sized {
        let listener = match MioTcpListener::bind(addr) {
            Ok(l) => l,
            Err(_) => return Err(TransportError::Other.into()),
        };

        Ok(TcpListener {
            listener,
            transport_cb_handler: None,
        })
    }

    fn accept(&self) -> Option<Self::Transport> {
        match self.listener.accept() {
            Ok((stream, _)) => Some(TcpTransport {
                stream,
                data_received: Vec::new(),
                data_to_send: Vec::new(),
                callback_handler: self.transport_cb_handler.clone(),
                connected_at: std::time::Instant::now(),
            }),
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => None,
            Err(e) => {
                error!("Failed to accept new connection: {}", e);
                None
            }
        }
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.transport_cb_handler = Some(cb_handler)
    }

    fn set_tls_options(&mut self, _tls_opt: TlsOptions) {
        warn!("Tls is not implemented on the tcp transport");
    }
}

impl Evented for TcpListener {
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
