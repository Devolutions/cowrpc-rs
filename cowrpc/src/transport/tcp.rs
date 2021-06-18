use crate::error::CowRpcError;
use crate::proto::{CowRpcMessage, Message};
use crate::transport::{
    CowSink, CowStream, LoggerObject, MessageInterceptor, SinkAndLog, StreamAndLog, Transport, TransportError,
};
use async_trait::async_trait;
use byteorder::{LittleEndian, ReadBytesExt};
use futures::prelude::*;
use futures::{self, ready};
use slog::{debug, error, Logger};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use url::Url;

pub struct TcpTransport {
    stream: TcpStream,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    connected_at: Instant,
    logger: Logger,
}

impl TcpTransport {
    pub fn new(stream: TcpStream, msg_inter: Option<Box<dyn MessageInterceptor>>, logger: Logger) -> Self {
        TcpTransport {
            stream,
            callback_handler: msg_inter,
            connected_at: Instant::now(),
            logger,
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(url: Url, logger: Logger) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        match TcpStream::connect(&url.to_string()).await {
            Ok(stream) => Ok(TcpTransport {
                stream,
                callback_handler: None,
                connected_at: Instant::now(),
                logger: logger.clone(),
            }),
            Err(e) => {
                error!(logger, "{:?}", e);
                Err(CowRpcError::from(TransportError::UnableToConnect))
            }
        }
    }

    fn message_stream_sink(self) -> (CowStream<CowRpcMessage>, CowSink<CowRpcMessage>) {
        let (reader, writer) = self.stream.into_split();

        let sink = Box::pin(CowMessageSink {
            stream: writer,
            data_to_send: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            logger: self.logger.clone(),
        });

        let stream = Box::pin(CowMessageStream {
            stream: reader,
            data_received: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            logger: self.logger,
        });

        (stream, sink)
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.callback_handler = Some(cb_handler)
    }

    fn set_keep_alive_interval(&mut self, _: Duration) {
        // Not supported
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.local_addr().ok()
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    fn up_time(&self) -> Duration {
        Instant::now().duration_since(self.connected_at)
    }

    fn set_logger(&mut self, logger: Logger) {
        self.logger = logger;
    }
}

pub struct CowMessageStream {
    pub stream: OwnedReadHalf,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
}

impl Stream for CowMessageStream {
    type Item = Result<CowRpcMessage, CowRpcError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        {
            let data_len = this.data_received.len();
            if data_len > 4 {
                let msg_len = ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize; //.read_u32::<LittleEndian>()? as usize;
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
            let mut buff = [0u8; 4096];
            let result = ready!(this.stream.read(&mut buff).poll_unpin(cx));
            match result {
                Ok(0) => {
                    return Poll::Ready(None);
                }
                Ok(n) => {
                    let data_len = n;
                    let mut buff = buff.to_vec();
                    buff.truncate(data_len);
                    this.data_received.append(&mut buff);

                    if data_len > 4 {
                        let msg_len =
                            ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize;
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
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    return Poll::Pending;
                }
                Err(e) => {
                    return Poll::Ready(Some(Err(e.into())));
                }
            }
        }
    }
}

impl LoggerObject for CowMessageStream {
    fn get_logger(&self) -> Logger {
        self.logger.clone()
    }

    fn set_logger(&mut self, logger: Logger) {
        self.logger = logger;
    }
}

impl StreamAndLog for CowMessageStream {}

pub struct CowMessageSink {
    pub stream: OwnedWriteHalf,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
    logger: Logger,
}

impl Sink<CowRpcMessage> for CowMessageSink {
    type Error = CowRpcError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: CowRpcMessage) -> Result<(), Self::Error> {
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

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        if !this.data_to_send.is_empty() {
            let res = ready!(this.stream.write_all(&this.data_to_send).poll_unpin(cx));
            if let Err(e) = res {
                if let ErrorKind::WouldBlock = e.kind() {
                    return Poll::Pending;
                }

                return Poll::Ready(Err(e.into()));
            }
            this.data_to_send.clear();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl LoggerObject for CowMessageSink {
    fn get_logger(&self) -> Logger {
        self.logger.clone()
    }

    fn set_logger(&mut self, logger: Logger) {
        self.logger = logger;
    }
}

impl SinkAndLog<CowRpcMessage> for CowMessageSink {}
