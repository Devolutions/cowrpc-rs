use std;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::sync::{Arc, Mutex};

use byteorder::{LittleEndian, ReadBytesExt};
use mio::net::TcpStream;
use mio::{Evented, Events, Poll, PollOpt, Ready, Token};
use proto::{CowRpcMessage, Message};
use time::Duration;
use timer::{Guard as TimerGuard, Timer};
use tls_api::{HandshakeError as TlsHandshakeError, MidHandshakeTlsStream, TlsConnector, TlsConnectorBuilder, TlsStream};
use tls_api_native_tls::TlsConnector as NativeTlsConnector;
use tungstenite::handshake::{
    client::{ClientHandshake, Request},
    server::{NoCallback, ServerHandshake},
    HandshakeError, MidHandshake,
};
use tungstenite::{
    stream::{Mode, Stream as StreamSwitcher},
    Message as WebSocketMessage, WebSocket,
};
use url::Url;

use error::Result;
use transport::uri::Uri;
use transport::{MessageInterceptor, sync::Transport, tls::TlsOptions, TransportError};

const WS_PING_PAYLOAD: &'static [u8] = b"";
const WS_BIN_CHUNK_SIZE: usize = 4096;

pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

pub enum WebSocketInner {
    WebSocket(WebSocket<WebSocketStream>),
    ClientHandShake(Option<(MidHandshake<ClientHandshake<WebSocketStream>>, TcpStream)>),
    ServerHandShake(Option<(MidHandshake<ServerHandshake<WebSocketStream, NoCallback>>, TcpStream)>),
    TlsHandShake(Option<(MidHandshakeTlsStream<TcpStream>, TcpStream)>),
}

pub struct WebSocketTransport {
    pub inner: Arc<Mutex<WebSocketInner>>,
    pub data_received: Vec<u8>,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<MessageInterceptor>>,
    pub connected_at: std::time::Instant,
    #[allow(dead_code)]
    ping_guard: Option<TimerGuard>,
}

pub fn wrap_stream(stream: TcpStream, domain: &str, mode: Mode, tls_options: Option<TlsOptions>) -> Result<WebSocketStream> {
    match mode {
        Mode::Plain => Ok(StreamSwitcher::Plain(stream)),
        Mode::Tls => {
            let mut connector_builder = NativeTlsConnector::builder().map_err(|e| TransportError::TlsError(e.to_string()))?;

            if let Some(tls_options) = tls_options {
                for certificate in tls_options.root_certificates {
                    trace!("adding root certificate");
                    connector_builder.add_root_certificate(certificate).map_err(|e| TransportError::TlsError(e.to_string()))?;
                }
            }
            
            let connector = connector_builder.build().map_err(|e| TransportError::TlsError(e.to_string()))?;
            match connector.connect(domain, stream.try_clone()?) {
                Ok(tls) => Ok(StreamSwitcher::Tls(tls)),
                Err(TlsHandshakeError::Interrupted(mid_hand)) => {
                    let mut handshake = mid_hand;
                    let poll = Poll::new()?;
                    let mut events = Events::with_capacity(64);
                    poll.register(&stream, Token(1), Ready::readable(), PollOpt::edge())?;

                    for i in 0..5 {
                        poll.poll(&mut events, Some(std::time::Duration::from_millis(1000)))
                            .map_err(|_| ::error::CowRpcError::from(TransportError::UnableToConnect))?;

                        match handshake.handshake() {
                            Ok(tls) => {
                                return Ok(StreamSwitcher::Tls(tls));
                            }
                            Err(TlsHandshakeError::Interrupted(m)) => {
                                handshake = m;
                            }
                            Err(e) => {
                                error!("Tls Handshake failed after {} tries with {:?}", i, e);
                                return Err(TransportError::UnableToConnect.into());
                            }
                        }
                    }
                    error!("Tls Handshake timed out");
                    return Err(TransportError::UnableToConnect.into());
                }
                Err(e) => {
                    error!("Tls Handshake failed with {:?}", e);
                    return Err(TransportError::UnableToConnect.into());
                }
            }
        }
    }
}

impl WebSocketTransport {
    pub fn new_client(
        handshake: MidHandshake<ClientHandshake<WebSocketStream>>,
        stream: TcpStream,
    ) -> WebSocketTransport {
        WebSocketTransport {
            inner: Arc::new(Mutex::new(WebSocketInner::ClientHandShake(Some((handshake, stream))))),
            data_received: Vec::new(),
            data_to_send: Vec::new(),
            callback_handler: None,
            connected_at: std::time::Instant::now(),
            ping_guard: None,
        }
    }

    pub fn new_server(
        handshake: MidHandshake<ServerHandshake<WebSocketStream, NoCallback>>,
        stream: TcpStream,
        ping: Option<(i64, &Timer)>,
    ) -> WebSocketTransport {
        let inner = Arc::new(Mutex::new(WebSocketInner::ServerHandShake(Some((handshake, stream)))));

        let ping_guard = {
            if let Some((dur, ref timer)) = ping {
                let inner_clone = inner.clone();
                Some(timer.schedule_repeating(Duration::seconds(dur), move || {
                    if let WebSocketInner::WebSocket(ref mut ws) = *inner_clone.lock().unwrap() {
                        let _ = ws.write_message(WebSocketMessage::Ping(WS_PING_PAYLOAD.to_vec()));
                        let _ = ws.write_pending();
                    }
                }))
            } else {
                None
            }
        };

        WebSocketTransport {
            inner,
            data_received: Vec::new(),
            data_to_send: Vec::new(),
            callback_handler: None,
            connected_at: std::time::Instant::now(),
            ping_guard,
        }
    }

    pub fn new_server_secure(
        handshake: MidHandshakeTlsStream<TcpStream>,
        stream: TcpStream,
        ping: Option<(i64, &Timer)>,
    ) -> WebSocketTransport {
        let inner = Arc::new(Mutex::new(WebSocketInner::TlsHandShake(Some((handshake, stream)))));

        let ping_guard = {
            if let Some((dur, ref timer)) = ping {
                let inner_clone = inner.clone();
                Some(timer.schedule_repeating(Duration::seconds(dur), move || {
                    if let WebSocketInner::WebSocket(ref mut ws) = *inner_clone.lock().unwrap() {
                        let _ = ws.write_message(WebSocketMessage::Ping(WS_PING_PAYLOAD.to_vec()));
                        let _ = ws.write_pending();
                    }
                }))
            } else {
                None
            }
        };

        WebSocketTransport {
            inner,
            data_received: Vec::new(),
            data_to_send: Vec::new(),
            callback_handler: None,
            connected_at: std::time::Instant::now(),
            ping_guard,
        }
    }

    pub fn from_ws(ws: WebSocket<WebSocketStream>) -> WebSocketTransport {
        WebSocketTransport {
            inner: Arc::new(Mutex::new(WebSocketInner::WebSocket(ws))),
            data_received: Vec::new(),
            data_to_send: Vec::new(),
            callback_handler: None,
            connected_at: std::time::Instant::now(),
            ping_guard: None,
        }
    }

    pub fn connect(uri: Uri, tls_options: Option<TlsOptions>) -> Result<WebSocketTransport> {
        let domain = uri.host().unwrap_or("");
        let url = match Url::parse(&uri.to_string()) {
            Ok(u) => u,
            Err(_) => return Err(::error::CowRpcError::Internal("Bad server url".into())),
        };

        let (mut port, mode) = match uri.scheme() {
            Some("ws") => (80, Mode::Plain),
            Some("wss") => (443, Mode::Tls),
            scheme => return Err(TransportError::InvalidUrl(format!("Unknown scheme {:?}", scheme)).into()),
        };

        if let Some(p) = uri.port() {
            port = p
        }

        if let Ok(addrs) = uri.get_addrs() {
            if let Some(addr) = addrs.iter().find(|ip| ip.is_ipv4()) {
                let sock_addr = SocketAddr::new(addr.clone(), port);

                let std_stream = StdTcpStream::connect(sock_addr)?;
                let stream = TcpStream::from_stream(std_stream)?;
                let ws_stream = wrap_stream(stream.try_clone()?, domain, mode, tls_options)?;
                let mut handshake = ClientHandshake::start(
                    ws_stream,
                    Request {
                        url,
                        extra_headers: None,
                    },
                    None,
                );

                {
                    match handshake.handshake() {
                        Ok(res) => {
                            return Ok(WebSocketTransport::from_ws(res.0));
                        }
                        Err(HandshakeError::Interrupted(m)) => {
                            handshake = m;
                        }
                        Err(e) => {
                            error!("Handshake failed with {}", e);
                            return Err(TransportError::UnableToConnect.into());
                        }
                    }

                    let poll = Poll::new()?;
                    let mut events = Events::with_capacity(64);
                    poll.register(&stream, Token(1), Ready::readable(), PollOpt::edge())?;

                    for i in 0..5 {
                        poll.poll(&mut events, Some(std::time::Duration::from_millis(1000)))
                            .map_err(|_| ::error::CowRpcError::from(TransportError::UnableToConnect))?;

                        match handshake.handshake() {
                            Ok(res) => {
                                return Ok(WebSocketTransport::from_ws(res.0));
                            }
                            Err(HandshakeError::Interrupted(m)) => {
                                handshake = m;
                            }
                            Err(e) => {
                                error!("Handshake failed after {} tries with {}", i, e);
                                return Err(TransportError::UnableToConnect.into());
                            }
                        }
                    }
                }
            }
        }

        Err(TransportError::UnableToConnect.into())
    }
}

impl Transport for WebSocketTransport {
    fn get_interest(&self) -> Ready {
        let mut ready = Ready::empty();
        ready.insert(Ready::readable());

        if !self.data_to_send.is_empty() {
            ready.insert(Ready::writable());
        }

        return ready;
    }

    fn send_data(&mut self) -> Result<()> {
        let mut inner_variant_changed = false;

        {
            let mut inner = self.inner.lock().unwrap();
            {
                if let WebSocketInner::WebSocket(ref mut ws) = *inner {
                    assert_ne!(self.data_to_send.len(), 0);

                    let mut data_to_send = self.data_to_send.clone();

                    if data_to_send.len() < WS_BIN_CHUNK_SIZE {
                        // Send all the data since it fits inside one chunk
                        match ws.write_message(WebSocketMessage::Binary(data_to_send)) {
                            Err(::tungstenite::Error::SendQueueFull(ws_msg)) => {
                                self.data_to_send.clear();
                                self.data_to_send = ws_msg.into_data();
                                return Ok(());
                            }
                            Err(e) => return Err(TransportError::from(e).into()),
                            Ok(_) => {
                                self.data_to_send.clear();
                                return Ok(());
                            }
                        }
                    } else {
                        // fragment the buffer
                        use std::io::{Cursor, Read};
                        let mut cursor = Cursor::new(data_to_send);
                        loop {
                            let mut chunk = Vec::with_capacity(WS_BIN_CHUNK_SIZE);
                            match cursor.read(&mut chunk) {
                                Ok(0) => {
                                    self.data_to_send.clear();
                                    return Ok(());
                                }
                                Ok(_n) => match ws.write_message(WebSocketMessage::Binary(chunk)) {
                                    Err(::tungstenite::Error::SendQueueFull(ws_msg)) => {
                                        let mut remains = ws_msg.into_data();
                                        remains.extend_from_slice(
                                            &self.data_to_send.split_off(cursor.position() as usize),
                                        );
                                        self.data_to_send = remains;
                                        return Ok(());
                                    }
                                    Err(e) => return Err(TransportError::from(e).into()),
                                    Ok(_) => {
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                }
            }

            let new_inner = match *inner {
                WebSocketInner::ServerHandShake(ref mut hand_opt) => match hand_opt.take() {
                    Some((hand, stream)) => match hand.handshake() {
                        Ok(res) => {
                            inner_variant_changed = true;
                            WebSocketInner::WebSocket(res)
                        }
                        Err(HandshakeError::Interrupted(m)) => WebSocketInner::ServerHandShake(Some((m, stream))),
                        _ => {
                            return Err(TransportError::UnableToConnect.into());
                        }
                    },
                    None => {
                        return Err(TransportError::UnableToConnect.into());
                    }
                },
                WebSocketInner::TlsHandShake(ref mut hand_opt) => match hand_opt.take() {
                    Some((hand, stream)) => match hand.handshake() {
                        Ok(tls) => {
                            inner_variant_changed = true;
                            let ws_stream = StreamSwitcher::Tls(tls);
                            let handshake = ServerHandshake::start(ws_stream, NoCallback, None);
                            WebSocketInner::ServerHandShake(Some((handshake, stream)))
                        }
                        Err(TlsHandshakeError::Interrupted(mid_hand)) => {
                            WebSocketInner::TlsHandShake(Some((mid_hand, stream)))
                        }
                        Err(e) => {
                            error!("Tls Handshake failed with {:?}", e);
                            return Err(TransportError::UnableToConnect.into());
                        }
                    },
                    None => {
                        return Err(TransportError::UnableToConnect.into());
                    }
                },
                WebSocketInner::ClientHandShake(_) => {
                    error!("Send_Data Should not be called during client handshake");
                    return Err(TransportError::Other.into());
                }
                WebSocketInner::WebSocket(_) => {
                    error!("Send_Data failed for websocket, This should never happen");
                    return Err(TransportError::Other.into());
                }
            };

            *inner = new_inner;
        }

        if inner_variant_changed {
            self.send_data()?;
        }

        Ok(())
    }

    fn send_message(&mut self, msg: CowRpcMessage) -> Result<()> {
        let mut msg = msg;

        if let Some(ref mut interceptor) = self.callback_handler {
            msg = match interceptor.before_send(msg) {
                Some(msg) => msg,
                None => return Ok(()),
            };
        }

        info!(">> {}", msg.get_msg_info());
        msg.write_to(&mut self.data_to_send)?;
        Ok(())
    }

    fn get_next_message(&mut self) -> Result<Option<CowRpcMessage>> {
        loop {
            let data_len = self.data_received.len();

            if data_len > 4 {
                let msg_len = self.data_received.as_slice().read_u32::<LittleEndian>()? as usize;
                if data_len >= msg_len {
                    let msg;
                    let v: Vec<u8>;
                    {
                        let mut slice: &[u8] = &self.data_received;
                        msg = CowRpcMessage::read_from(&mut slice)?;
                        v = slice.to_vec();
                    }
                    self.data_received = v;

                    if let Some(ref mut interceptor) = self.callback_handler {
                        match interceptor.before_recv(msg) {
                            Some(msg) => {
                                info!("<< {}", msg.get_msg_info());
                                return Ok(Some(msg));
                            }
                            None => {
                                continue;
                            }
                        };
                    } else {
                        info!("<< {}", msg.get_msg_info());
                        return Ok(Some(msg));
                    }
                }
            }
            return Ok(None);
        }
    }

    fn read_data(&mut self) -> Result<()> {
        let mut inner_variant_changed = false;

        {
            let mut inner = self.inner.lock().unwrap();
            {
                if let WebSocketInner::WebSocket(ref mut ws) = *inner {
                    loop {
                        match ws.read_message() {
                            Err(::tungstenite::Error::Io(e)) => {
                                if let ::std::io::ErrorKind::WouldBlock = e.kind() {
                                    return Ok(());
                                } else {
                                    return Err(e.into());
                                }
                            }
                            Ok(msg) => match msg {
                                WebSocketMessage::Binary(mut data) => self.data_received.append(&mut data),
                                WebSocketMessage::Pong(_) | WebSocketMessage::Ping(_) => {
                                    trace!("Websocket received control message");
                                    continue;
                                }
                                _ => {
                                    return Err(::error::CowRpcError::Proto("Received malformed data on socket".into()))
                                }
                            },
                            Err(::tungstenite::Error::ConnectionClosed(_)) => {
                                return Err(TransportError::ConnectionReset.into());
                            }
                            e => {
                                trace!("ERROR Connection error {:?}", e);
                                return Err(TransportError::ConnectionReset.into());
                            }
                        }
                    }
                }
            }

            let new_inner = match *inner {
                WebSocketInner::ServerHandShake(ref mut hand_opt) => match hand_opt.take() {
                    Some((hand, stream)) => match hand.handshake() {
                        Ok(res) => {
                            inner_variant_changed = true;
                            WebSocketInner::WebSocket(res)
                        }
                        Err(HandshakeError::Interrupted(m)) => WebSocketInner::ServerHandShake(Some((m, stream))),
                        _ => {
                            return Err(TransportError::ConnectionReset.into());
                        }
                    },
                    None => {
                        return Err(TransportError::ConnectionReset.into());
                    }
                },
                WebSocketInner::TlsHandShake(ref mut hand_opt) => match hand_opt.take() {
                    Some((hand, stream)) => match hand.handshake() {
                        Ok(tls) => {
                            inner_variant_changed = true;
                            let ws_stream = StreamSwitcher::Tls(tls);
                            let handshake = ServerHandshake::start(ws_stream, NoCallback, None);
                            WebSocketInner::ServerHandShake(Some((handshake, stream)))
                        }
                        Err(TlsHandshakeError::Interrupted(mid_hand)) => {
                            WebSocketInner::TlsHandShake(Some((mid_hand, stream)))
                        }
                        Err(e) => {
                            trace!("ERROR Tls Handshake failed with {:?}", e);
                            return Err(TransportError::UnableToConnect.into());
                        }
                    },
                    None => {
                        return Err(TransportError::UnableToConnect.into());
                    }
                },
                WebSocketInner::ClientHandShake(_) => {
                    error!("read_data should not read on Client Handshake");
                    return Err(TransportError::Other.into());
                }
                WebSocketInner::WebSocket(_) => {
                    error!("Read_Data failed for websocket, This should never happen");
                    return Err(TransportError::Other.into());
                }
            };

            *inner = new_inner;
        }

        if inner_variant_changed {
            self.read_data()?;
        }

        Ok(())
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.callback_handler = Some(cb_handler)
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref ws) => match ws.get_ref() {
                StreamSwitcher::Plain(tcp_stream) => tcp_stream.local_addr().ok(),
                StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().local_addr().ok(),
            },
            _ => None,
        }
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref ws) => match ws.get_ref() {
                StreamSwitcher::Plain(tcp_stream) => tcp_stream.peer_addr().ok(),
                StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().peer_addr().ok(),
            },
            _ => None,
        }
    }

    fn up_time(&self) -> std::time::Duration {
        std::time::Instant::now().duration_since(self.connected_at)
    }

    fn shutdown(&mut self) -> Result<()> {
        match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref mut ws) => ws.close(None).map_err(|_| TransportError::ConnectionReset.into()),
            _ => Ok(()),
        }
    }
}

impl Evented for WebSocketTransport {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref ws) => match ws.get_ref() {
                StreamSwitcher::Plain(tcp_stream) => tcp_stream.register(poll, token, interest, opts),
                StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().register(poll, token, interest, opts),
            },
            WebSocketInner::ClientHandShake(Some((_, ref stream))) => stream.register(poll, token, interest, opts),
            WebSocketInner::ServerHandShake(Some((_, ref stream))) => stream.register(poll, token, interest, opts),
            WebSocketInner::TlsHandShake(Some((_, ref stream))) => stream.register(poll, token, interest, opts),
            _ => Err(IoError::new(
                IoErrorKind::Other,
                "Unable to register ws stream, encountered None",
            )),
        }
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        let res = match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref ws) => match ws.get_ref() {
                StreamSwitcher::Plain(tcp_stream) => tcp_stream.reregister(poll, token, interest, opts),
                StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().reregister(poll, token, interest, opts),
            },
            WebSocketInner::ClientHandShake(Some((_, ref stream))) => stream.reregister(poll, token, interest, opts),
            WebSocketInner::ServerHandShake(Some((_, ref stream))) => stream.reregister(poll, token, interest, opts),
            WebSocketInner::TlsHandShake(Some((_, ref stream))) => stream.reregister(poll, token, interest, opts),
            _ => Err(IoError::new(
                IoErrorKind::Other,
                "Unable to reregister ws stream, encountered None",
            )),
        };

        // On Linux it is not possible to register the clone of a stream and then reregister the original stream.
        // This will trigger a NotFound error, and is easily fixed by registering the original stream
        if let Err(ref e) = res {
            if let ::std::io::ErrorKind::NotFound = e.kind() {
                return self.register(poll, token, interest, opts);
            }
        }

        res
    }

    fn deregister(&self, poll: &Poll) -> std::io::Result<()> {
        match *self.inner.lock().unwrap() {
            WebSocketInner::WebSocket(ref ws) => match ws.get_ref() {
                StreamSwitcher::Plain(tcp_stream) => tcp_stream.deregister(poll),
                StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().deregister(poll),
            },
            WebSocketInner::ClientHandShake(Some((_, ref stream))) => stream.deregister(poll),
            WebSocketInner::ServerHandShake(Some((_, ref stream))) => stream.deregister(poll),
            WebSocketInner::TlsHandShake(Some((_, ref stream))) => stream.deregister(poll),
            _ => Err(IoError::new(
                IoErrorKind::Other,
                "Unable to deregister ws stream, encountered None",
            )),
        }
    }
}
