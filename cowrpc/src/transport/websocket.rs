use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::transport::{CowSink, Transport};

use crate::transport::uri::Uri;
use crate::transport::{CowRpcTransportError, MessageInterceptor, TransportError};

use byteorder::LittleEndian;
use futures::prelude::*;
use futures::{ready, FutureExt, SinkExt};
use parking_lot::Mutex;

use tokio::sync::Mutex as AsyncMutex;

use crate::error::{CowRpcError, Result};
use crate::proto::{CowRpcMessage, Message};
use crate::transport::CowStream;
use async_trait::async_trait;
use async_tungstenite::tokio::{ConnectStream, TokioAdapter};

use async_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use async_tungstenite::WebSocketStream;

use futures::StreamExt;
use std::cmp::min;

use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use tokio::net::TcpStream;
use tokio_rustls::rustls;

type WsStream = Pin<Box<dyn Stream<Item = std::result::Result<WsMessage, WsError>> + Send + Sync>>;
type WsSink = Pin<Box<dyn Sink<WsMessage, Error = WsError> + Send>>;

const PING_INTERVAL: u64 = 60;
const PING_TIMEOUT: u64 = 15;
const WS_PING_PAYLOAD: &'static [u8] = b"";
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
}

impl ServerWebSocketPingUtils {
    fn new(config: &WsPingConfig, ws_sink: Arc<AsyncMutex<WsSink>>) -> Self {
        ServerWebSocketPingUtils {
            send_ping: Arc::new(Mutex::new(true)),
            ping_expired: Arc::new(Mutex::new(false)),
            waiting_pong: Arc::new(Mutex::new(false)),
            send_ping_error: Arc::new(Mutex::new(None)),
            ping_interval: config.ping_interval,
            ws_sink,
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
            if let Ok(_) = self.send_ping(waker) {
                *send_ping = false;
            }
        }

        Ok(())
    }

    fn send_ping(&self, waker: Waker) -> Result<()> {
        let ping_utils = self.clone();

        tokio::spawn(async move {
            let result = ping_utils
                .ws_sink
                .lock()
                .await
                .send(WsMessage::Ping(WS_PING_PAYLOAD.to_vec()))
                .await;
            match result {
                Ok(_) => {
                    trace!("WS_PING sent");

                    let now = tokio::time::Instant::now();
                    let ping_expired_instant = now + Duration::from_secs(PING_TIMEOUT);
                    let next_ping = now + ping_utils.ping_interval;

                    *ping_utils.waiting_pong.lock() = true;

                    tokio::time::delay_until(ping_expired_instant).await;

                    if *ping_utils.waiting_pong.lock() {
                        *ping_utils.ping_expired.lock() = true;
                        waker.clone().wake();
                        return ();
                    }

                    tokio::time::delay_until(next_ping).await;

                    *ping_utils.send_ping.lock() = true;

                    waker.wake();
                }
                Err(e) => {
                    debug!("Sending WS_PING failed: {}", e);
                    *ping_utils.send_ping_error.lock() = Some(TransportError::from(e));
                }
            }
        });

        Ok(())
    }

    fn pong_received(&self) {
        trace!("WS_PONG received");
        *self.waiting_pong.lock() = false;
    }
}

// TODO: Keep only dyn Stream/Sink
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
}

impl WebSocketTransport {
    pub fn new(stream: CowWebSocketStream, callback_handler: Option<Box<dyn MessageInterceptor>>) -> Self {
        WebSocketTransport {
            stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_config: None,
        }
    }

    pub fn new_server(stream: CowWebSocketStream, callback_handler: Option<Box<dyn MessageInterceptor>>) -> Self {
        WebSocketTransport {
            stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_config: Some(WsPingConfig::new(None)),
        }
    }
}

#[async_trait()]
impl Transport for WebSocketTransport {
    async fn connect(uri: Uri) -> Result<Self>
    where
        Self: Sized,
    {
        // TODO : Remove if not needed
        let client_config = Arc::new(rustls::ClientConfig::default());
        let tls_connector = tokio_rustls::TlsConnector::from(client_config);

        match async_tungstenite::tokio::connect_async_with_tls_connector(uri.to_string(), Some(tls_connector)).await {
            Ok((stream, _)) => Ok(WebSocketTransport::new(CowWebSocketStream::ConnectStream(stream), None)),
            Err(e) => {
                error!("{:?}", e);
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
            ping_util = Some(ServerWebSocketPingUtils::new(ping_config, sink.clone()))
        }

        let cow_stream = CowMessageStream::new(
            stream,
            self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            ping_util,
        );
        let cow_sink = CowMessageSink::new(sink, self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()));
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
}

pub struct CowMessageStream {
    pub stream: WsStream,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

impl CowMessageStream {
    fn new(
        stream: WsStream,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
        ping_utils: Option<ServerWebSocketPingUtils>,
    ) -> Self {
        CowMessageStream {
            stream,
            data_received: Vec::new(),
            callback_handler,
            ping_utils,
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
                        match interceptor.before_recv(msg) {
                            Some(msg) => {
                                debug!("<< {}", msg.get_msg_info());
                                return Poll::Ready(Some(Ok(msg)));
                            }
                            None => {}
                        };
                    } else {
                        debug!("<< {}", msg.get_msg_info());
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
                                    match interceptor.before_recv(msg) {
                                        Some(msg) => {
                                            debug!("<< {}", msg.get_msg_info());
                                            return Poll::Ready(Some(Ok(msg)));
                                        }
                                        None => {}
                                    };
                                } else {
                                    debug!("<< {}", msg.get_msg_info());
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
                            error!("ws.read_message returned error: {}", e);
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
}

impl CowMessageSink {
    fn new(sink: Arc<AsyncMutex<WsSink>>, callback_handler: Option<Box<dyn MessageInterceptor>>) -> Self {
        CowMessageSink {
            sink: sink,
            data_to_send: Vec::new(),
            callback_handler,
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

        debug!(">> {}", msg.get_msg_info());
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
