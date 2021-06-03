use crate::error::{CowRpcError, Result};
use crate::proto::{CowRpcMessage, Message};
use crate::transport::{CowRpcTransportError, CowSink, CowStream, MessageInterceptor, Transport, TransportError};
use async_trait::async_trait;
use async_tungstenite::tokio::{ConnectStream, TokioAdapter};
use async_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use async_tungstenite::WebSocketStream;
use byteorder::LittleEndian;
use futures::prelude::*;
use futures::{ready, FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use slog::{debug, error, trace, Logger};
use std::cmp::min;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;
use tokio_rustls::rustls;
use url::Url;

type WsStream = Pin<Box<dyn Stream<Item = std::result::Result<WsMessage, WsError>> + Send + Sync>>;
type WsSink = Pin<Box<dyn Sink<WsMessage, Error = WsError> + Send>>;

const PING_INTERVAL: u64 = 60;
const PING_TIMEOUT: u64 = 15;
const WS_PING_PAYLOAD: &[u8] = b"";
const WS_BIN_CHUNK_SIZE: usize = 4096;

struct WsPingConfig {
    ping_interval: Duration,
}

impl WsPingConfig {
    fn new(ping_interval: Option<Duration>) -> Self {
        WsPingConfig {
            ping_interval: ping_interval.unwrap_or_else(|| Duration::from_secs(PING_INTERVAL)),
        }
    }
}
#[derive(Clone)]
struct ServerWebSocketPingUtils {
    send_ping: Arc<Mutex<bool>>,
    ping_expired: Arc<Mutex<bool>>,
    waiting_pong: Arc<Mutex<bool>>,
    send_ping_error: Arc<Mutex<Option<TransportError>>>,
    ping_interval: Duration,
    ws_sink: Arc<AsyncMutex<WsSink>>,
    logger: Logger,
}

impl ServerWebSocketPingUtils {
    fn new(config: &WsPingConfig, ws_sink: Arc<AsyncMutex<WsSink>>, logger: Logger) -> Self {
        ServerWebSocketPingUtils {
            send_ping: Arc::new(Mutex::new(true)),
            ping_expired: Arc::new(Mutex::new(false)),
            waiting_pong: Arc::new(Mutex::new(false)),
            send_ping_error: Arc::new(Mutex::new(None)),
            ping_interval: config.ping_interval,
            ws_sink,
            logger,
        }
    }

    fn check_ping(&self, waker: Waker) -> Result<()> {
        // Error if ping has expired
        if *self.ping_expired.lock() {
            return Err(CowRpcError::Timeout);
        }

        // Error if last ping failed to be sent
        let error = self.send_ping_error.lock().take();
        if let Some(error) = error {
            return Err(error.into());
        }

        let mut send_ping = self.send_ping.lock();
        if *send_ping {
            self.send_ping(waker);
            *send_ping = false;
        }

        Ok(())
    }

    fn send_ping(&self, waker: Waker) {
        let ping_utils = self.clone();
        let logger = self.logger.clone();

        tokio::spawn(async move {
            let result = ping_utils
                .ws_sink
                .lock()
                .await
                .send(WsMessage::Ping(WS_PING_PAYLOAD.to_vec()))
                .await;
            match result {
                Ok(_) => {
                    trace!(logger, "WS_PING sent");

                    let now = tokio::time::Instant::now();
                    let ping_expired_instant = now + Duration::from_secs(PING_TIMEOUT);
                    let next_ping = now + ping_utils.ping_interval;

                    *ping_utils.waiting_pong.lock() = true;

                    tokio::time::delay_until(ping_expired_instant).await;

                    if *ping_utils.waiting_pong.lock() {
                        *ping_utils.ping_expired.lock() = true;
                        waker.clone().wake();
                        return;
                    }

                    tokio::time::delay_until(next_ping).await;

                    *ping_utils.send_ping.lock() = true;

                    waker.wake();
                }
                Err(e) => {
                    debug!(logger, "Sending WS_PING failed: {}", e);
                    *ping_utils.send_ping_error.lock() = Some(TransportError::from(e));
                }
            }
        });
    }

    fn pong_received(&self) {
        trace!(self.logger, "WS_PONG received");
        *self.waiting_pong.lock() = false;
    }
}

pub enum CowWebSocketStream {
    ConnectStream(WebSocketStream<ConnectStream>),
    AcceptStream(WebSocketStream<TokioAdapter<TcpStream>>),
    AcceptTlsStream(WebSocketStream<TokioAdapter<tokio_rustls::server::TlsStream<TcpStream>>>),
}

pub struct WebSocketTransport {
    stream: CowWebSocketStream,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    connected_at: Instant,
    ping_config: Option<WsPingConfig>,
    logger: Logger,
}

impl WebSocketTransport {
    pub fn new(
        stream: CowWebSocketStream,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
        logger: Logger,
    ) -> Self {
        WebSocketTransport {
            stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_config: None,
            logger,
        }
    }

    pub fn new_server(
        stream: CowWebSocketStream,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
        logger: Logger,
    ) -> Self {
        WebSocketTransport {
            stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_config: Some(WsPingConfig::new(None)),
            logger,
        }
    }
}

#[async_trait()]
impl Transport for WebSocketTransport {
    async fn connect(url: Url, logger: Logger) -> Result<Self>
    where
        Self: Sized,
    {
        let client_config = Arc::new(rustls::ClientConfig::default());
        let tls_connector = tokio_rustls::TlsConnector::from(client_config);

        match async_tungstenite::tokio::connect_async_with_tls_connector(url.as_str(), Some(tls_connector)).await {
            Ok((stream, _)) => Ok(WebSocketTransport::new(
                CowWebSocketStream::ConnectStream(stream),
                None,
                logger,
            )),
            Err(e) => {
                error!(logger, "{:?}", e);
                Err(CowRpcError::from(TransportError::UnableToConnect))
            }
        }
    }

    fn message_stream_sink(self) -> (CowStream<CowRpcMessage>, CowSink<CowRpcMessage>) {
        let (sink, stream) = match self.stream {
            CowWebSocketStream::ConnectStream(stream) => {
                let (sink, stream) = stream.split();
                (
                    Arc::new(AsyncMutex::new(Box::pin(sink) as WsSink)),
                    Box::pin(stream) as WsStream,
                )
            }
            CowWebSocketStream::AcceptStream(stream) => {
                let (sink, stream) = stream.split();
                (
                    Arc::new(AsyncMutex::new(Box::pin(sink) as WsSink)),
                    Box::pin(stream) as WsStream,
                )
            }
            CowWebSocketStream::AcceptTlsStream(stream) => {
                let (sink, stream) = stream.split();
                (
                    Arc::new(AsyncMutex::new(Box::pin(sink) as WsSink)),
                    Box::pin(stream) as WsStream,
                )
            }
        };

        let mut ping_util = None;
        if let Some(ping_config) = &self.ping_config {
            ping_util = Some(ServerWebSocketPingUtils::new(
                ping_config,
                sink.clone(),
                self.logger.clone(),
            ))
        }

        let cow_stream = CowMessageStream::new(
            stream,
            self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            ping_util,
            self.logger.clone(),
        );
        let cow_sink = CowMessageSink::new(
            sink,
            self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            self.logger.clone(),
        );
        (Box::pin(cow_stream), Box::pin(cow_sink))
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.callback_handler = Some(cb_handler);
    }

    fn set_keep_alive_interval(&mut self, interval: Duration) {
        if let Some(ping_config) = &mut self.ping_config {
            ping_config.ping_interval = interval;
        }
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        // TODO : We can't get the TcpStream for now, but the latest version of async-tungstenite allows that (>0.13)
        None
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        // TODO : We can't get the TcpStream for now, but the latest version of async-tungstenite allows that (>0.13)
        None
    }

    fn up_time(&self) -> Duration {
        Instant::now().duration_since(self.connected_at)
    }

    fn set_logger(&mut self, logger: Logger) {
        self.logger = logger;
    }
}

pub struct CowMessageStream {
    pub stream: WsStream,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    ping_utils: Option<ServerWebSocketPingUtils>,
    logger: Logger,
}

impl CowMessageStream {
    fn new(
        stream: WsStream,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
        ping_utils: Option<ServerWebSocketPingUtils>,
        logger: Logger,
    ) -> Self {
        CowMessageStream {
            stream,
            data_received: Vec::new(),
            callback_handler,
            ping_utils,
            logger,
        }
    }
}
impl Stream for CowMessageStream {
    type Item = Result<CowRpcMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(ping_utils) = &this.ping_utils {
            ping_utils.check_ping(cx.waker().clone())?;
        }

        {
            let data_len = this.data_received.len();
            if data_len > 4 {
                let msg_len =
                    byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize;
                if data_len >= msg_len {
                    let msg;
                    let v: Vec<u8>;
                    {
                        let mut slice: &[u8] = &this.data_received;
                        msg = CowRpcMessage::read_from(&mut slice)?;
                        v = slice.to_vec();
                    }
                    this.data_received = v;

                    if let Some(ref mut interceptor) = this.callback_handler {
                        if let Some(msg) = interceptor.before_recv(msg) {
                            debug!(this.logger, "<< {}", msg.get_msg_info());
                            return Poll::Ready(Some(Ok(msg)));
                        }
                    } else {
                        debug!(this.logger, "<< {}", msg.get_msg_info());
                        return Poll::Ready(Some(Ok(msg)));
                    }
                }
            }
        }

        loop {
            match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(msg)) => match msg {
                    WsMessage::Binary(mut data) => {
                        this.data_received.append(&mut data);
                        let data_len = this.data_received.len();
                        if data_len > 4 {
                            let msg_len =
                                byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())?
                                    as usize;
                            if data_len >= msg_len {
                                let msg;
                                let v: Vec<u8>;
                                {
                                    let mut slice: &[u8] = &this.data_received;
                                    msg = CowRpcMessage::read_from(&mut slice)?;
                                    v = slice.to_vec();
                                }
                                this.data_received = v;

                                if let Some(ref mut interceptor) = this.callback_handler {
                                    if let Some(msg) = interceptor.before_recv(msg) {
                                        debug!(this.logger, "<< {}", msg.get_msg_info());
                                        return Poll::Ready(Some(Ok(msg)));
                                    }
                                } else {
                                    debug!(this.logger, "<< {}", msg.get_msg_info());
                                    return Poll::Ready(Some(Ok(msg)));
                                }
                            }
                        }
                        continue;
                    }
                    WsMessage::Pong(_) | WsMessage::Ping(_) => {
                        if let Some(ping_utils) = &this.ping_utils {
                            ping_utils.pong_received();
                        }
                    }
                    WsMessage::Close(_) => return Poll::Ready(None),
                    WsMessage::Text(_) => {
                        return Poll::Ready(Some(Err(crate::error::CowRpcError::Proto(
                            "Text webSocket messages are not supported.".into(),
                        ))))
                    }
                },
                Some(Err(WsError::Io(e))) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    return Poll::Pending;
                }
                Some(Err(e)) => {
                    match e {
                        WsError::ConnectionClosed => {
                            // WebSocket connection closed normally. The stream is terminated
                            return Poll::Ready(None);
                        }
                        e => {
                            error!(this.logger, "ws.read_message returned error: {}", e);
                            return Poll::Ready(Some(Err(CowRpcTransportError::from(e).into())));
                        }
                    }
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

struct CowMessageSink {
    sink: Arc<AsyncMutex<WsSink>>,
    data_to_send: Vec<u8>,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
}

impl CowMessageSink {
    fn new(
        sink: Arc<AsyncMutex<WsSink>>,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
        logger: Logger,
    ) -> Self {
        CowMessageSink {
            sink,
            data_to_send: Vec::new(),
            callback_handler,
            logger,
        }
    }
}

impl Sink<CowRpcMessage> for CowMessageSink {
    type Error = CowRpcError;

    fn poll_ready(self: Pin<&mut CowMessageSink>, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: CowRpcMessage) -> std::result::Result<(), Self::Error> {
        let this = self.get_mut();
        let mut msg = item;

        if let Some(ref mut interceptor) = this.callback_handler {
            msg = match interceptor.before_send(msg) {
                Some(msg) => msg,
                None => return Ok(()),
            };
        }

        debug!(this.logger, ">> {}", msg.get_msg_info());
        msg.write_to(&mut this.data_to_send)?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut CowMessageSink>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut stream = ready!(Box::pin(this.sink.lock()).poll_unpin(cx));
        loop {
            if !(this.data_to_send.is_empty()) {
                match ready!(stream.poll_ready_unpin(cx)) {
                    Ok(_) => {
                        let size = min(this.data_to_send.len(), WS_BIN_CHUNK_SIZE);
                        let data_to_send: Vec<u8> = this.data_to_send.drain(0..size).collect();
                        match stream.start_send_unpin(WsMessage::Binary(data_to_send)) {
                            Ok(_) => {}
                            Err(e) => {
                                return Poll::Ready(Err(e.into()));
                            }
                        }
                    }
                    Err(e) => {
                        return Poll::Ready(Err(e.into()));
                    }
                }
            } else {
                break;
            }
        }
        stream.poll_flush_unpin(cx).map_err(|e| TransportError::from(e).into())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let mut stream = ready!(Box::pin(self.sink.lock()).poll_unpin(cx));
        stream
            .poll_close_unpin(cx)
            .map_err(|e| CowRpcTransportError::from(e).into())
    }
}
