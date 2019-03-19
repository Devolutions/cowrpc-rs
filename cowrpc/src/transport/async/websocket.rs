use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use byteorder::{LittleEndian, ReadBytesExt};
use futures::{self, Async, AsyncSink, Future, Sink, Stream};
use parking_lot::Mutex;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::TaskExecutor;
use tokio::timer::Delay;
use tokio::util::FutureExt;
use tls_api::{HandshakeError as TlsHandshakeError, MidHandshakeTlsStream, TlsConnector, TlsConnectorBuilder, TlsStream};
use tls_api_native_tls::TlsConnector as NativeTlsConnector;
use transport::{
    async::{Transport, CowFuture, CowSink, CowStream},
    uri::Uri,
    MessageInterceptor, TransportError, tls::TlsOptions,
};
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

use error::{CowRpcError, Result};
use proto::{CowRpcMessage, Message};

const PING_INTERVAL: u64 = 30;
const WS_PING_PAYLOAD: &'static [u8] = b"";
const WS_BIN_CHUNK_SIZE: usize = 4096;

pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

pub fn wrap_stream_async(stream: TcpStream, domain: &str, mode: Mode, tls_options: Option<TlsOptions>) -> CowFuture<WebSocketStream> {
    match mode {
        Mode::Plain => Box::new(futures::finished(StreamSwitcher::Plain(stream))),
        Mode::Tls => {
            let mut connector_builder = match NativeTlsConnector::builder().map_err(|e| TransportError::from(e)) {
                Ok(c) => c,
                Err(e) => return Box::new(futures::failed(e.into()))
            };

            if let Some(tls_options) = tls_options {
                for certificate in tls_options.root_certificates {
                    trace!("adding root certificate");
                    if let Err(e) = connector_builder.add_root_certificate(certificate).map_err(|e| TransportError::from(e)) {
                        return Box::new(futures::failed(e.into()));
                    }
                }
            }

            let connector = match connector_builder.build().map_err(|e| TransportError::from(e))
            {
                Ok(c) => c,
                Err(e) => return Box::new(futures::failed(e.into())),
            };
            match connector.connect(domain, stream) {
                Ok(tls) => Box::new(futures::finished(StreamSwitcher::Tls(tls))),
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
                    Box::new(futures::failed(TransportError::UnableToConnect.into()))
                }
            }
        }
    }
}

pub struct ServerWebSocketHandshake(pub Option<MidHandshake<ServerHandshake<WebSocketStream, NoCallback>>>);

impl Future for ServerWebSocketHandshake {
    type Item = WebSocket<WebSocketStream>;
    type Error = CowRpcError;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>> {
        let handshake = self.0.take().expect("This should never happen");
        match handshake.handshake() {
            Ok(ws) => Ok(Async::Ready(ws)),
            Err(HandshakeError::Interrupted(m)) => {
                self.0 = Some(m);
                Ok(Async::NotReady)
            }
            Err(e) => {
                trace!("ERROR : Handshake failed with {}", e);
                Err(TransportError::UnableToConnect.into())
            }
        }
    }
}

struct ClientWebSocketHandshake(Option<MidHandshake<ClientHandshake<WebSocketStream>>>);

impl Future for ClientWebSocketHandshake {
    type Item = WebSocket<WebSocketStream>;
    type Error = CowRpcError;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>> {
        let handshake = self.0.take().expect("This should never happen");
        match handshake.handshake() {
            Ok(res) => Ok(Async::Ready(res.0)),
            Err(HandshakeError::Interrupted(m)) => {
                self.0 = Some(m);
                Ok(Async::NotReady)
            }
            Err(e) => {
                trace!("ERROR : Handshake failed with {}", e);
                Err(TransportError::UnableToConnect.into())
            }
        }
    }
}

pub struct TlsHandshake(pub Option<MidHandshakeTlsStream<TcpStream>>);

impl Future for TlsHandshake {
    type Item = TlsStream<TcpStream>;
    type Error = CowRpcError;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>> {
        let handshake = self.0.take().expect("This should never happen");
        match handshake.handshake() {
            Ok(tls) => Ok(Async::Ready(tls)),
            Err(TlsHandshakeError::Interrupted(m)) => {
                self.0 = Some(m);
                Ok(Async::NotReady)
            }
            Err(e) => {
                trace!("ERROR : Tls Handshake failed with {:?}", e);
                Err(TransportError::UnableToConnect.into())
            }
        }
    }
}

#[derive(Clone)]
struct ServerWebSocketPingUtils {
    ping_sent: u64,
    ping_expired: Arc<Mutex<bool>>,
    executor_handle: TaskExecutor,
}

pub struct WebSocketTransport {
    stream: Arc<Mutex<WebSocket<WebSocketStream>>>,
    callback_handler: Option<Box<MessageInterceptor>>,
    connected_at: Instant,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

impl Clone for WebSocketTransport {
    fn clone(&self) -> Self {
        WebSocketTransport {
            stream: self.stream.clone(),
            callback_handler: self.callback_handler.clone(),
            connected_at: self.connected_at.clone(),
            ping_utils: self.ping_utils.clone(),
        }
    }
}

impl WebSocketTransport {
    pub fn new(stream: WebSocket<WebSocketStream>, callback_handler: Option<Box<MessageInterceptor>>) -> Self {
        WebSocketTransport {
            stream: Arc::new(Mutex::new(stream)),
            callback_handler,
            connected_at: Instant::now(),
            ping_utils: None,
        }
    }

    pub fn new_server(
        stream: WebSocket<WebSocketStream>,
        callback_handler: Option<Box<MessageInterceptor>>,
        handle: Option<TaskExecutor>,
    ) -> Self {
        if let Some(handle) = handle {
            WebSocketTransport {
                stream: Arc::new(Mutex::new(stream)),
                callback_handler,
                connected_at: Instant::now(),
                ping_utils: Some(ServerWebSocketPingUtils {
                    ping_sent: 0,
                    ping_expired: Arc::new(Mutex::new(true)),
                    executor_handle: handle,
                }),
            }
        } else {
            Self::new(stream, callback_handler)
        }
    }
}

impl Transport for WebSocketTransport {
    fn connect(uri: Uri) -> CowFuture<Self>
    where
        Self: Sized,
    {
        let domain = uri.host().unwrap_or("").to_string();
        let url = match Url::parse(&uri.to_string()) {
            Ok(u) => u,
            Err(_) => return Box::new(futures::failed(::error::CowRpcError::Internal("Bad server url".into()))),
        };

        let (mut port, mode) = match uri.scheme() {
            Some("ws") => (80, Mode::Plain),
            Some("wss") => (443, Mode::Tls),
            scheme => {
                return Box::new(futures::failed(
                    TransportError::InvalidUrl(format!("Unknown scheme {:?}", scheme)).into(),
                ))
            }
        };

        if let Some(p) = uri.port() {
            port = p
        }

        if let Ok(addrs) = uri.get_addrs() {
            if let Some(addr) = addrs.iter().find(|ip| ip.is_ipv4()) {
                let sock_addr = SocketAddr::new(addr.clone(), port);

                return Box::new(
                    TcpStream::connect(&sock_addr)
                        .map_err(|e| e.into())
                        .and_then(move |stream| {
                            wrap_stream_async(stream, domain.as_ref(), mode, None).and_then(|ws_stream| {
                                let fut: CowFuture<WebSocketTransport> = match ClientHandshake::start(
                                    ws_stream,
                                    Request {
                                        url,
                                        extra_headers: None,
                                    },
                                    None,
                                ).handshake()
                                {
                                    Ok(ws) => Box::new(futures::finished(WebSocketTransport::new(ws.0, None))),
                                    Err(HandshakeError::Interrupted(m)) => Box::new(
                                        ClientWebSocketHandshake(Some(m))
                                            .timeout(Duration::from_secs(5))
                                            .map_err(|e| match e.into_inner() {
                                                Some(e) => e,
                                                None => CowRpcError::Proto(
                                                    "Tls handshake timed out after 10 sec".to_string(),
                                                ),
                                            }).map(|ws| WebSocketTransport::new(ws, None)),
                                    ),
                                    Err(e) => {
                                        trace!("ERROR : Handshake failed with {}", e);
                                        Box::new(futures::failed(TransportError::UnableToConnect.into()))
                                    }
                                };
                                fut
                            })
                        }).map_err(|err| {
                            trace!("ERROR : {:?}", err);
                            CowRpcError::from(TransportError::UnableToConnect)
                        }),
                );
            }
        }

        Box::new(futures::failed(TransportError::UnableToConnect.into()))
    }

    fn message_sink(&mut self) -> CowSink<CowRpcMessage> {
        Box::new(CowMessageSink {
            stream: self.stream.clone(),
            data_to_send: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        })
    }

    fn message_stream(&mut self) -> CowStream<CowRpcMessage> {
        Box::new(CowMessageStream {
            stream: self.stream.clone(),
            data_received: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            ping_utils: self.ping_utils.clone(),
        })
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.callback_handler = Some(cb_handler);
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        match self.stream.lock().get_ref() {
            StreamSwitcher::Plain(tcp_stream) => tcp_stream.local_addr().ok(),
            StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().local_addr().ok(),
        }
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        match self.stream.lock().get_ref() {
            StreamSwitcher::Plain(tcp_stream) => tcp_stream.peer_addr().ok(),
            StreamSwitcher::Tls(tls_stream) => tls_stream.get_ref().peer_addr().ok(),
        }
    }

    fn up_time(&self) -> Duration {
        Instant::now().duration_since(self.connected_at)
    }
}

pub struct CowMessageStream {
    pub stream: Arc<Mutex<WebSocket<WebSocketStream>>>,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<MessageInterceptor>>,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

impl CowMessageStream {
    fn check_ping(&mut self) -> Result<()> {
        if let Some(ref mut ping_utils) = self.ping_utils {
            if ping_utils.ping_sent >= 3 {
                warn!("WS_PING sent {} times and no response received.", ping_utils.ping_sent);
                return Err(TransportError::ConnectionReset.into());
            }

            let mut expired_guard = ping_utils.ping_expired.lock();
            if *expired_guard {
                let expired_clone = ping_utils.ping_expired.clone();
                let mut stream_clone = self.stream.clone();

                let task = task::current();
                let timeout = Delay::new(Instant::now() + Duration::from_secs(PING_INTERVAL));

                ping_utils.executor_handle.spawn(timeout.then(move |_| {
                    *expired_clone.lock() = true;
                    if let Err(e) = stream_clone
                        .lock()
                        .write_message(WebSocketMessage::Ping(WS_PING_PAYLOAD.to_vec())) {

                        error!("WS_PING can't be sent: {}", e);
                    }
                    task.notify();
                    futures::finished::<(), ()>(())
                }));

                *expired_guard = false;
                ping_utils.ping_sent += 1;
            }
        }

        Ok(())
    }
}

impl Stream for CowMessageStream {
    type Item = CowRpcMessage;
    type Error = CowRpcError;

    fn poll(&mut self) -> ::std::result::Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
        self.check_ping()?;

        let mut ws = self.stream.lock();

        {
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
                                return Ok(Async::Ready(Some(msg)));
                            }
                            None => {}
                        };
                    } else {
                        info!("<< {}", msg.get_msg_info());
                        return Ok(Async::Ready(Some(msg)));
                    }
                }
            }
        }

        loop {
            match ws.read_message() {
                Err(::tungstenite::Error::Io(e)) => {
                    if let ::std::io::ErrorKind::WouldBlock = e.kind() {
                        return Ok(Async::NotReady);
                    } else {
                        return Err(e.into());
                    }
                }
                Ok(msg) => match msg {
                    WebSocketMessage::Binary(mut data) => {
                        self.data_received.append(&mut data);
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
                                            return Ok(Async::Ready(Some(msg)));
                                        }
                                        None => {}
                                    };
                                } else {
                                    info!("<< {}", msg.get_msg_info());
                                    return Ok(Async::Ready(Some(msg)));
                                }
                            }
                        }
                        continue;
                    }
                    WebSocketMessage::Pong(_) | WebSocketMessage::Ping(_) => {
                        if let Some(ref mut ping_utils) = self.ping_utils {
                            ping_utils.ping_sent = 0;
                        }
                        continue;
                    }
                    _ => return Err(::error::CowRpcError::Proto("Received malformed data on socket".into())),
                },
                Err(e) => {
                    error!("ws.read_message returned error: {}", e);
                    return Err(TransportError::ConnectionReset.into());
                }
            }
        }
    }
}

pub struct CowMessageSink {
    pub stream: Arc<Mutex<WebSocket<WebSocketStream>>>,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<MessageInterceptor>>,
}

impl Sink for CowMessageSink {
    type SinkItem = CowRpcMessage;
    type SinkError = CowRpcError;

    fn start_send(
        &mut self,
        item: <Self as Sink>::SinkItem,
    ) -> ::std::result::Result<AsyncSink<<Self as Sink>::SinkItem>, <Self as Sink>::SinkError> {
        let mut msg = item;

        if let Some(ref mut interceptor) = self.callback_handler {
            msg = match interceptor.before_send(msg) {
                Some(msg) => msg,
                None => return Ok(AsyncSink::Ready),
            };
        }

        info!(">> {}", msg.get_msg_info());
        msg.write_to(&mut self.data_to_send)?;
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> ::std::result::Result<Async<()>, <Self as Sink>::SinkError> {
        if !self.data_to_send.is_empty() {
            let mut ws = self.stream.lock();
            let data_to_send = self.data_to_send.clone();

            if data_to_send.len() < WS_BIN_CHUNK_SIZE {
                // Send all the data since it fits inside one chunk
                match ws.write_message(WebSocketMessage::Binary(data_to_send)) {
                    Err(::tungstenite::Error::SendQueueFull(ws_msg)) => {
                        self.data_to_send.clear();
                        self.data_to_send = ws_msg.into_data();
                        return Ok(Async::NotReady);
                    }
                    Err(e) => return Err(TransportError::from(e).into()),
                    Ok(_) => {
                        self.data_to_send.clear();
                        return Ok(Async::Ready(()));
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
                            return Ok(Async::Ready(()));
                        }
                        Ok(_n) => match ws.write_message(WebSocketMessage::Binary(chunk)) {
                            Err(::tungstenite::Error::SendQueueFull(ws_msg)) => {
                                let mut remains = ws_msg.into_data();
                                remains.extend_from_slice(&self.data_to_send.split_off(cursor.position() as usize));
                                self.data_to_send = remains;
                                return Ok(Async::NotReady);
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
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> ::std::result::Result<Async<()>, <Self as Sink>::SinkError> {
        Ok(Async::Ready(()))
    }
}
