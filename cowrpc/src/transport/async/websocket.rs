use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use byteorder::{LittleEndian};
use futures::prelude::*;
use parking_lot::{Mutex, RwLock, RawMutex};
use tls_api::{HandshakeError as TlsHandshakeError, MidHandshakeTlsStream, TlsConnector, TlsConnectorBuilder, TlsStream};
use tls_api_native_tls::TlsConnector as NativeTlsConnector;
use crate::transport::{r#async::{Transport, CowFuture, CowSink, CowStreamEx, StreamEx}, uri::Uri, MessageInterceptor, TransportError, tls::TlsOptions, CowRpcTransportError};
use async_tungstenite::tungstenite::handshake::{
    client::{ClientHandshake, Request},
    server::{NoCallback, ServerHandshake},
    HandshakeError, MidHandshake,
};
use async_tungstenite::tungstenite::{stream::{Mode}, Message as WebSocketMessage, WebSocket, Error};
use url::Url;
use futures::{ready, SinkExt, FutureExt};

use crate::error::{CowRpcError, Result};
use crate::proto::{CowRpcMessage, Message};
use futures::future::TryFutureExt;
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::net::TcpStream;
use async_trait::async_trait;
use async_tungstenite::tungstenite::http::Response;
use async_tungstenite::tokio::{TokioAdapter, ConnectStream};
use futures::StreamExt;
use async_tungstenite::WebSocketStream;
use async_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};
use futures::stream::{SplitSink, SplitStream};
use url::idna::domain_to_ascii;
use std::cmp::min;

const PING_INTERVAL: u64 = 60;
const PING_TIMEOUT: u64 = 15;
const WS_PING_PAYLOAD: &'static [u8] = b"";
const WS_BIN_CHUNK_SIZE: usize = 4096;

//pub type WebSocketStream = StreamSwitcher<TcpStream, TlsStream<TcpStream>>;

// pub fn wrap_stream_async(stream: TcpStream, domain: &str, mode: Mode, tls_options: Option<TlsOptions>) -> CowFuture<WebSocketStream> {
//     match mode {
//         Mode::Plain => Box::new(future::ok(StreamSwitcher::Plain(stream))),
//         Mode::Tls => {
//             let mut connector_builder = match NativeTlsConnector::builder().map_err(|e| TransportError::from(e)) {
//                 Ok(c) => c,
//                 Err(e) => return Box::new(futures_01::failed(e.into()))
//             };
//
//             if let Some(tls_options) = tls_options {
//                 for certificate in tls_options.root_certificates {
//                     trace!("adding root certificate");
//                     if let Err(e) = connector_builder.add_root_certificate(certificate).map_err(|e| TransportError::from(e)) {
//                         return Box::new(futures_01::failed(e.into()));
//                     }
//                 }
//             }
//
//             let connector = match connector_builder.build().map_err(|e| TransportError::from(e))
//             {
//                 Ok(c) => c,
//                 Err(e) => return Box::new(futures_01::failed(e.into())),
//             };
//             match connector.connect(domain, stream) {
//                 Ok(tls) => Box::new(futures_01::finished(StreamSwitcher::Tls(tls))),
//                 Err(TlsHandshakeError::Interrupted(mid_hand)) => {
//                     let tls_hand = TlsHandshake(Some(mid_hand));
//
//                     Box::new(
//                     tokio::time::timeout(::std::time::Duration::from_secs(5),
//                                          tls_hand.compat()).compat()
//                         .map_err(|_| CowRpcError::Internal("timed out".to_string()))
//                         .and_then(move |result| {
//                             match result {
//                                 Ok(tls_stream) => {
//                                     ok(StreamSwitcher::Tls(tls_stream))
//                                 }
//                                 Err(e) => {
//                                     err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
//                                 }
//                             }
//                         })
//                     )
//                 }
//                 Err(e) => {
//                     trace!("ERROR : Tls Handshake failed with {:?}", e);
//                     Box::new(futures_01::failed(TransportError::UnableToConnect.into()))
//                 }
//             }
//         }
//     }
// }

// pub struct ServerWebSocketHandshake(pub Option<MidHandshake<ServerHandshake<WebSocketStream, NoCallback>>>);
//
// impl Future for ServerWebSocketHandshake {
//     type Output = Result<WebSocket<WebSocketStream>>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let handshake = self.0.take().expect("This should never happen");
//         match handshake.handshake() {
//             Ok(ws) => Poll::Ready(Ok(ws)),
//             Err(HandshakeError::Interrupted(m)) => {
//                 self.0 = Some(m);
//                 Poll::Pending
//             }
//             Err(e) => {
//                 trace!("ERROR : Handshake failed with {}", e);
//                 Poll::Ready(Err(TransportError::UnableToConnect.into()))
//             }
//         }
//     }
// }

// struct ClientWebSocketHandshake(Option<MidHandshake<ClientHandshake<WebSocketStream>>>);
//
// impl Future for ClientWebSocketHandshake {
//     type Output = Result<WebSocket<WebSocketStream>>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let handshake = self.0.take().expect("This should never happen");
//         match handshake.handshake() {
//             Ok(res) => Poll::Ready(Ok(res.0)),
//             Err(HandshakeError::Interrupted(m)) => {
//                 self.0 = Some(m);
//                 Poll::Pending
//             }
//             Err(e) => {
//                 trace!("ERROR : Handshake failed with {}", e);
//                 Poll::Ready(Err(TransportError::UnableToConnect.into()))
//             }
//         }
//     }
// }

// pub struct TlsHandshake(pub Option<MidHandshakeTlsStream<TcpStream>>);
//
// impl Future for TlsHandshake {
//     type Output = Result<TlsStream<TcpStream>>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let handshake = self.0.take().expect("This should never happen");
//         match handshake.handshake() {
//             Ok(tls) => Poll::Ready(Ok(tls)),
//             Err(TlsHandshakeError::Interrupted(m)) => {
//                 self.0 = Some(m);
//                 Poll::Pending
//             }
//             Err(e) => {
//                 trace!("ERROR : Tls Handshake failed with {:?}", e);
//                 Poll::Ready(Err(TransportError::UnableToConnect.into()))
//             }
//         }
//     }
// }

#[derive(Clone)]
struct ServerWebSocketPingUtils {
    send_ping: Arc<Mutex<bool>>,
    ping_expired: Arc<Mutex<bool>>,
    waiting_pong: Arc<Mutex<bool>>,
    send_ping_error: Arc<Mutex<Option<TransportError>>>,
    ping_interval: Option<Duration>,
    close_on_ping_expired: Arc<RwLock<bool>>,
}

pub enum CowWebSocketStream {
    ConnectStream(WebSocketStream<ConnectStream>),
    AcceptStream(WebSocketStream<TokioAdapter<TcpStream>>)
}

pub struct WebSocketTransport {
    stream: CowWebSocketStream,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    connected_at: Instant,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

// impl Clone for WebSocketTransport {
//     fn clone(&self) -> Self {
//         WebSocketTransport {
//             stream: self.stream.clone(),
//             callback_handler: self.callback_handler.clone(),
//             connected_at: self.connected_at.clone(),
//             ping_utils: self.ping_utils.clone(),
//         }
//     }
// }

impl WebSocketTransport {
    pub fn new(stream: CowWebSocketStream, callback_handler: Option<Box<dyn MessageInterceptor>>) -> Self {
        WebSocketTransport {
            stream: stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_utils: None,
        }
    }

    pub fn new_server(
        stream: CowWebSocketStream,
        callback_handler: Option<Box<dyn MessageInterceptor>>,
    ) -> Self {
        WebSocketTransport {
            stream: stream,
            callback_handler,
            connected_at: Instant::now(),
            ping_utils: Some(ServerWebSocketPingUtils {
                ping_expired: Arc::new(Mutex::new(false)),
                send_ping: Arc::new(Mutex::new(true)),
                waiting_pong: Arc::new(Mutex::new(false)),
                send_ping_error: Arc::new(Mutex::new(None)),
                ping_interval: None,
                close_on_ping_expired: Arc::new(RwLock::new(true)),
            }),
        }
    }
}

#[async_trait()]
impl Transport for WebSocketTransport {
    async fn connect(uri: Uri) -> Result<Self>
    where
        Self: Sized,
    {
        let domain = uri.host().unwrap_or("").to_string();
        let url = Url::parse(&uri.to_string()).map_err(|_| crate::error::CowRpcError::Internal("Bad server url".into()))?;

        let (mut port, mode) = match uri.scheme() {
            Some("ws") => (80, Mode::Plain),
            Some("wss") => (443, Mode::Tls),
            scheme => {
                return Err(TransportError::InvalidUrl(format!("Unknown scheme {:?}", scheme)).into());
            }
        };

        if let Some(p) = uri.port() {
            port = p
        }

        if let Ok(addrs) = uri.get_addrs() {
            if let Some(addr) = addrs.iter().find(|ip| ip.is_ipv4()) {
                let sock_addr = SocketAddr::new(addr.clone(), port);

                match async_tungstenite::tokio::connect_async(uri.to_string()).await {
                    Ok((stream, _)) => {
                        return Ok(WebSocketTransport::new(CowWebSocketStream::ConnectStream(stream), None));
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        return Err(CowRpcError::from(TransportError::UnableToConnect));
                    }
                }
            }
        }

        Err(TransportError::UnableToConnect.into())
    }

    fn message_stream_sink(self) -> (CowStreamEx<CowRpcMessage>, CowSink<CowRpcMessage>) {
        macro_rules! boxify {
            ($reader:ident, $writer:ident) => {{
                 (Box::pin(CowMessageStream {
                    stream: $reader,
                    data_received: Vec::new(),
                    callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
                    ping_utils: self.ping_utils.clone(),
                }),
                 Box::pin(CowMessageSink {
                     stream: Some($writer),
                     data_to_send: Vec::new(),
                     callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
                     send_ws_message: None,
                 }))
            }}
        }
        match self.stream {
            CowWebSocketStream::ConnectStream(stream) => {
                let (writer, reader) = stream.split();
                boxify!(reader, writer)
            }
            CowWebSocketStream::AcceptStream(stream) => {
                let (writer, reader) = stream.split();
                boxify!(reader, writer)
            }
        }
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

pub struct CowMessageStream<S: Stream<Item = std::result::Result<WsMessage, WsError>> + Send> {
    pub stream: SplitStream<S>,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    ping_utils: Option<ServerWebSocketPingUtils>,
}

impl<S: Stream<Item = std::result::Result<WsMessage, WsError>> + Send> CowMessageStream<S> {
    fn check_ping(&mut self) -> Result<()> {
        // TODO
        Ok(())
        // if let Some(ref mut ping_utils) = self.ping_utils {
        //
        //     // Error if ping has expired
        //     if *ping_utils.ping_expired.lock() {
        //         if *ping_utils.close_on_ping_expired.read() {
        //             return Err(CowRpcError::Timeout);
        //         } else {
        //             warn!("Pong not received, but keep the connection open");
        //         }
        //     }
        //
        //     // Error if last ping failed to be sent
        //     let error = ping_utils.send_ping_error.lock().take();
        //     if let Some(error) = error {
        //         return Err(error.into());
        //     }
        //
        //     let mut send_ping = ping_utils.send_ping.lock();
        //     if *send_ping {
        //         *send_ping = false;
        //
        //         let send_ping_clone = ping_utils.send_ping.clone();
        //         let ping_interval = ping_utils.ping_interval.clone().unwrap_or(Duration::from_secs(PING_INTERVAL));
        //         let task = task::current();
        //
        //         if *ping_utils.waiting_pong.lock() {
        //             trace!("Pong not received yet. Scheduling a new ping...");
        //             tokio::spawn(async move {
        //                 let ping_delay = tokio::time::delay_until(tokio::time::Instant::now() + ping_interval);
        //                 ping_delay.await;
        //                 *send_ping_clone.lock() = true;
        //                 task.notify();
        //                 futures_01::finished::<(), ()>(())
        //             });
        //         } else {
        //             let ping_expired = ping_utils.ping_expired.clone();
        //             let waiting_pong = ping_utils.waiting_pong.clone();
        //             let send_ping_error = ping_utils.send_ping_error.clone();
        //             let stream_clone = self.stream.clone();
        //
        //             tokio::spawn(async move {
        //                 let result = stream_clone
        //                     .lock()
        //                     .write_message(WebSocketMessage::Ping(WS_PING_PAYLOAD.to_vec()));
        //                 match result {
        //                     Ok(_) => {
        //                         trace!("WS_PING sent.");
        //                         *waiting_pong.lock() = true;
        //
        //                         // Start another task to be wake up in 15 seconds and validate that we received the pong response.
        //                         let task_clone = task.clone();
        //                         tokio::spawn(async move {
        //                             let timeout = tokio::time::delay_until(tokio::time::Instant::now() + Duration::from_secs(PING_TIMEOUT));
        //                             timeout.await;
        //                             if *waiting_pong.lock() {
        //                                 *ping_expired.lock() = true;
        //                                 task_clone.notify();
        //                             }
        //                             futures_01::finished::<(), ()>(())
        //                         });
        //
        //                         // Wait the interval and raise the flag to send another ping. If we still wait a pong response, we wait another interval
        //                         let ping_delay = tokio::time::delay_until(tokio::time::Instant::now() + ping_interval);
        //                         ping_delay.await;
        //                         *send_ping_clone.lock() = true;
        //                     }
        //                     Err(e) => {
        //                         debug!("Sending WS_PING failed: {}", e);
        //
        //                         match e {
        //                             ::async_tungstenite::tungstenite::Error::SendQueueFull(_) => {
        //                                 *send_ping_clone.lock() = true;
        //                                 warn!("WS_PING can't be send, queue is full.")
        //                             }
        //                             e => {
        //                                 // Ping can't be sent. Keep the error to send this error back later.
        //                                 *send_ping_error.lock() = Some(e.into());
        //                             }
        //                         }
        //                     }
        //                 }
        //
        //                 task.notify();
        //                 futures_01::finished::<(), ()>(())
        //             });
        //         }
        //     }
        // }
        //
        // Ok(())
    }
}
impl<S: Stream<Item = std::result::Result<WsMessage, WsError>> + Send> Stream for CowMessageStream<S> {
    type Item = Result<CowRpcMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.check_ping()?;

        {
            let data_len = this.data_received.len();
            if data_len > 4 {
                let msg_len = byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize;
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
            let x = this.stream.poll_next_unpin(cx);
            match ready!(x) {
                Some(Ok(msg)) => {
                    match msg {
                        WsMessage::Binary(mut data) => {
                            this.data_received.append(&mut data);
                            let data_len = this.data_received.len();
                            if data_len > 4 {
                                let msg_len = byteorder::ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize;
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
                        WsMessage::Close(_) => {
                            return Poll::Ready(None)
                        }
                        _ => {
                            return Poll::Ready(Some(Err(crate::error::CowRpcError::Proto("Received malformed data on socket".into()))))
                        },
                        // TODO
                        // Message::Ping(_) => {}
                        // Message::Pong(_) => {}

                    }
                }
                Some(Err(WsError::Io(e))) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    return Poll::Pending;
                }
                Some(Err(e)) => {
                    match e {
                        WsError::ConnectionClosed => {
                            // WebSocket connection closed normally. The stream is terminated
                            return Poll::Ready(None);
                        },
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

impl<S: Stream<Item = std::result::Result<WsMessage, WsError>> + Send> StreamEx for CowMessageStream<S> {
    fn close_on_keep_alive_timeout(&mut self, close: bool) {
        if let Some(ping_util) = &self.ping_utils {
            *ping_util.close_on_ping_expired.write() = close
        }
    }
}

struct CowMessageSink<S: Sink<WsMessage, Error = WsError> + Send> {
    stream: Option<SplitSink<S, WsMessage>>,
    data_to_send: Vec<u8>,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    send_ws_message: Option<SendWsMessage<S>>,
}

impl<S: Sink<WsMessage, Error = WsError> + Send> Sink<CowRpcMessage> for CowMessageSink<S> {
    type Error = CowRpcError;

    fn poll_ready(mut self: Pin<&mut CowMessageSink<S>>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
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

    fn poll_flush(mut self: Pin<&mut CowMessageSink<S>>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let this: &mut CowMessageSink<S> = self.get_mut();

        loop {
            if let Some(mut send_ws_message) = this.send_ws_message.take() {
                match send_ws_message.poll_unpin(cx) {
                    Poll::Ready(Ok(sink)) => {
                        this.stream = Some(sink);
                    },
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        this.send_ws_message = Some(send_ws_message);
                        return Poll::Pending;
                    }
                }
            }

            if !this.data_to_send.is_empty() {
                let size = min(this.data_to_send.len(), WS_BIN_CHUNK_SIZE);
                let data_to_send: Vec<u8> = this.data_to_send.drain(0..size).collect();

                if let Some(sink) = this.stream.take() {
                    this.send_ws_message = Some(SendWsMessage::new(sink, WsMessage::Binary(data_to_send)));
                } else {
                    // Should never happen
                    return Poll::Ready(Err(CowRpcError::Internal("Sink is none, we can't send a websocket message".to_string())));
                }
            } else {
                break;
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(mut sink) = this.stream.as_mut() {
            sink.poll_close_unpin(cx).map_err(|e|CowRpcTransportError::from(e).into())
        } else {
            Poll::Ready(Err(CowRpcError::Internal("Sink is none, can't be closed.".to_string())))
        }
    }
}

struct SendWsMessage<S: Sink<WsMessage, Error = WsError> + Send> {
    sink: Option<SplitSink<S, WsMessage>>,
    item: Option<WsMessage>,
}

impl<S: Sink<WsMessage, Error = WsError> + Send> SendWsMessage<S> {
    fn new(sink: SplitSink<S, WsMessage>, item: WsMessage) -> Self {
        SendWsMessage {
            sink: Some(sink),
            item: Some(item),
        }
    }
}

impl<S: Sink<WsMessage, Error = WsError> + Send> Future for SendWsMessage<S> {
    type Output = Result<SplitSink<S, WsMessage>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(item) = this.item.take() {
            if let Some(sink) = this.sink.as_mut() {
                match sink.poll_ready_unpin(cx) {
                    Poll::Ready(Ok(_)) => {
                        sink.start_send_unpin(item).map_err(|e| TransportError::from(e))?;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(TransportError::from(e).into()));
                    }
                    Poll::Pending => {
                        this.item = Some(item);
                        return Poll::Pending;
                    }
                }
            }
        }

        if let Some(mut sink) = this.sink.take() {
            match sink.poll_flush_unpin(cx) {
                Poll::Pending => {
                    this.sink = Some(sink);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(_)) => {
                    return Poll::Ready(Ok(sink));
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(TransportError::from(e).into()));
                }
            }
        }

        // Should never happen
        Poll::Ready(Err(CowRpcError::Internal("Invalid use of SendWsMessage future".to_string())))
    }
}
