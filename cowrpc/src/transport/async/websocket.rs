use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use byteorder::{LittleEndian};
use futures::{self, Async, AsyncSink, Future, Sink, Stream, task};
use parking_lot::Mutex;
use tls_api::{HandshakeError as TlsHandshakeError, MidHandshakeTlsStream, TlsConnector, TlsConnectorBuilder, TlsStream};
use tls_api_native_tls::TlsConnector as NativeTlsConnector;
use crate::transport::{
    r#async::{Transport, CowFuture, CowSink, CowStream},
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

use crate::error::{CowRpcError, Result};
use crate::proto::{CowRpcMessage, Message};
use tokio_tcp::TcpStream;
use futures::future::{ok, err};
use futures_03::compat::Future01CompatExt;
use futures_03::future::TryFutureExt;

const PING_INTERVAL: u64 = 60;
const PING_TIMEOUT: u64 = 15;
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
    send_ping: Arc<Mutex<bool>>,
    ping_expired: Arc<Mutex<bool>>,
    waiting_pong: Arc<Mutex<bool>>,
    send_ping_error: Arc<Mutex<Option<TransportError>>>,
    ping_interval: Option<Duration>,
}

pub struct WebSocketTransport {
    stream: Arc<Mutex<WebSocket<WebSocketStream>>>,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
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
    pub fn new(stream: WebSocket<WebSocketStream>, callback_handler: Option<Box<dyn MessageInterceptor>>) -> Self {
        WebSocketTransport {
            stream: Arc::new(Mutex::new(stream)),
            callback_handler,
            connected_at: Instant::now(),
            ping_utils: None,
        }
    }

    pub fn new_server(
        stream: WebSocket<WebSocketStream>,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
    ) -> Self {
        WebSocketTransport {
            stream: Arc::new(Mutex::new(stream)),
            callback_handler,
            connected_at: Instant::now(),
            ping_utils: Some(ServerWebSocketPingUtils {
                ping_expired: Arc::new(Mutex::new(false)),
                send_ping: Arc::new(Mutex::new(true)),
                waiting_pong: Arc::new(Mutex::new(false)),
                send_ping_error: Arc::new(Mutex::new(None)),
                ping_interval: None,
            }),
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
            Err(_) => return Box::new(futures::failed(crate::error::CowRpcError::Internal("Bad server url".into()))),
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
                                        tokio::time::timeout(::std::time::Duration::from_secs(5),
                                                             ClientWebSocketHandshake(Some(m)).compat()).compat()
                                            .map_err(|_| CowRpcError::Internal("timed out".to_string()))
                                            .and_then(move |result| {
                                                match result {
                                                    Ok(ws) => {
                                                        ok(WebSocketTransport::new(ws, None))
                                                    }
                                                    Err(e) => {
                                                        err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                                                    }
                                                }
                                            })
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

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.callback_handler = Some(cb_handler);
    }

    fn set_keep_alive_interval(&mut self, interval: Option<Duration>) {
        if let Some(ping_utils) = &mut self.ping_utils {
            ping_utils.ping_interval = interval;
        }
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
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

impl CowMessageStream {
    fn check_ping(&mut self) -> Result<()> {
        if let Some(ref mut ping_utils) = self.ping_utils {

            // Error if ping has expired
            if *ping_utils.ping_expired.lock() {
                return Err(CowRpcError::Timeout);
            }

            // Error if last ping failed to be sent
            let error = ping_utils.send_ping_error.lock().take();
            if let Some(error) = error {
                return Err(error.into());
            }

            let mut send_ping = ping_utils.send_ping.lock();
            if *send_ping {
                *send_ping = false;

                let send_ping_clone = ping_utils.send_ping.clone();
                let ping_interval = ping_utils.ping_interval.clone().unwrap_or(Duration::from_secs(PING_INTERVAL));
                let task = task::current();

                if *ping_utils.waiting_pong.lock() {
                    trace!("Pong not received yet. Scheduling a new ping...");
                    tokio::spawn(async move {
                        let ping_delay = tokio::time::delay_until(tokio::time::Instant::now() + ping_interval);
                        ping_delay.await;
                        *send_ping_clone.lock() = true;
                        task.notify();
                        futures::finished::<(), ()>(())
                    });
                } else {
                    let ping_expired = ping_utils.ping_expired.clone();
                    let waiting_pong = ping_utils.waiting_pong.clone();
                    let send_ping_error = ping_utils.send_ping_error.clone();
                    let stream_clone = self.stream.clone();

                    tokio::spawn(async move {
                        let result = stream_clone
                            .lock()
                            .write_message(WebSocketMessage::Ping(WS_PING_PAYLOAD.to_vec()));
                        match result {
                            Ok(_) => {
                                trace!("WS_PING sent.");
                                *waiting_pong.lock() = true;

                                // Start another task to be wake up in 15 seconds and validate that we received the pong response.
                                let task_clone = task.clone();
                                tokio::spawn(async move {
                                    let timeout = tokio::time::delay_until(tokio::time::Instant::now() + Duration::from_secs(PING_TIMEOUT));
                                    timeout.await;
                                    if *waiting_pong.lock() {
                                        *ping_expired.lock() = true;
                                        task_clone.notify();
                                    }
                                    futures::finished::<(), ()>(())
                                });

                                // Wait the interval and raise the flag to send another ping. If we still wait a pong response, we wait another interval
                                let ping_delay = tokio::time::delay_until(tokio::time::Instant::now() + ping_interval);
                                ping_delay.await;
                                *send_ping_clone.lock() = true;
                            }
                            Err(e) => {
                                debug!("Sending WS_PING failed: {}", e);

                                match e {
                                    ::tungstenite::Error::SendQueueFull(_) => {
                                        *send_ping_clone.lock() = true;
                                        warn!("WS_PING can't be send, queue is full.")
                                    }
                                    e => {
                                        // Ping can't be sent. Keep the error to send this error back later.
                                        *send_ping_error.lock() = Some(e.into());
                                    }
                                }
                            }
                        }

                        task.notify();
                        futures::finished::<(), ()>(())
                    });
                }
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
                let msg_len = byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut self.data_received.as_slice())? as usize;
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
                                debug!("<< {}", msg.get_msg_info());
                                return Ok(Async::Ready(Some(msg)));
                            }
                            None => {}
                        };
                    } else {
                        debug!("<< {}", msg.get_msg_info());
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
                            let msg_len = byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut self.data_received.as_slice())? as usize;
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
                                            debug!("<< {}", msg.get_msg_info());
                                            return Ok(Async::Ready(Some(msg)));
                                        }
                                        None => {}
                                    };
                                } else {
                                    debug!("<< {}", msg.get_msg_info());
                                    return Ok(Async::Ready(Some(msg)));
                                }
                            }
                        }
                        continue;
                    }
                    WebSocketMessage::Pong(_) | WebSocketMessage::Ping(_) => {
                        if let Some(ref mut ping_utils) = self.ping_utils {
                            trace!("WS_PONG received.");
                            *ping_utils.waiting_pong.lock() = false;
                        }
                        continue;
                    }
                    _ => return Err(crate::error::CowRpcError::Proto("Received malformed data on socket".into())),
                },
                Err(e) => {
                    match e {
                        tungstenite::error::Error::ConnectionClosed(_) => {
                            // WebSocket connection closed normally. The stream is terminated
                            return Ok(Async::Ready(None));
                        },
                        _ => {
                            error!("ws.read_message returned error: {}", e);
                            return Err(TransportError::ConnectionReset.into());
                        }
                    }
                }
            }
        }
    }
}

pub struct CowMessageSink {
    pub stream: Arc<Mutex<WebSocket<WebSocketStream>>>,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
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

        debug!(">> {}", msg.get_msg_info());
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
                let mut cursor = std::io::Cursor::new(data_to_send);
                loop {
                    let mut chunk = Vec::with_capacity(WS_BIN_CHUNK_SIZE);
                    match std::io::Read::read(&mut cursor, &mut chunk) {
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
