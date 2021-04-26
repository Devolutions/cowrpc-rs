use byteorder::{LittleEndian, ReadBytesExt};
use crate::error::CowRpcError;
use futures::prelude::*;
use futures::{self, Future};
use crate::proto::{CowRpcMessage, Message};
use std::{
    io::{ErrorKind, Read, Write},
    net::SocketAddr,
    time::{Duration, Instant},
};
use crate::transport::{
    r#async::{Transport, CowFuture, CowSink, CowStreamEx, StreamEx},
    uri::Uri,
    MessageInterceptor, TransportError,
};
use std::task::{Context, Poll};
use std::pin::Pin;
use tokio::net::TcpStream;
use std::io::Error;
use async_trait::async_trait;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use crate::tokio::io::{AsyncReadExt, AsyncWriteExt};
use futures::ready;

pub struct TcpTransport {
    stream: TcpStream,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    connected_at: Instant,
}

// impl Clone for TcpTransport {
//     fn clone(&self) -> Self {
//         let stream = self
//             .stream
//             .try_clone()
//             .expect("Async router implementation rely on tcpstream being cloned, this is fatal");
//
//         TcpTransport {
//             stream,
//             callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
//             connected_at: self.connected_at,
//         }
//     }
// }

impl TcpTransport {
    pub fn new(stream: TcpStream, msg_inter: Option<Box<dyn MessageInterceptor>>) -> Self {
        TcpTransport {
            stream,
            callback_handler: msg_inter,
            connected_at: Instant::now(),
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(uri: Uri) -> Result<Self, CowRpcError>
    where
        Self: Sized,
    {
        let mut port: u16 = 80;
        if let Some(p) = uri.port() {
            port = p
        }

        if let Ok(addrs) = uri.get_addrs() {
            for addr in addrs {
                let sock_addr = SocketAddr::new(addr, port);

                match TcpStream::connect(&sock_addr).await {
                    Ok(stream) => {
                        return Ok(TcpTransport {
                            stream,
                            callback_handler: None,
                            connected_at: Instant::now(),
                        });
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        return Err(CowRpcError::from(TransportError::UnableToConnect));
                    }
                }
            }
        }

        Err(TransportError::InvalidUrl("Unable to resolve hostname".to_string()).into())
    }

    fn message_sink(&mut self) -> CowSink<CowRpcMessage> {
        todo!()
        // let stream = self
        //     .stream
        //     .try_clone()
        //     .expect("Async router implementation rely on tcpstream being cloned, this is fatal");
        //
        // Box::new(CowMessageSink {
        //     stream,
        //     data_to_send: Vec::new(),
        //     callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        // })
    }

    fn message_stream(&mut self) -> CowStreamEx<CowRpcMessage> {
        todo!()
        // let stream = self
        //     .stream
        //     .try_clone()
        //     .expect("Async router implementation rely on tcpstream being cloned, this is fatal");
        //
        // Box::new(CowMessageStream {
        //     stream,
        //     data_received: Vec::new(),
        //     callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        // })
    }

    fn message_stream_sink(self) -> (CowStreamEx<CowRpcMessage>, CowSink<CowRpcMessage>) {
        let (reader, writer) = self.stream.into_split();

        let sink = Box::new(CowMessageSink {
            stream: writer,
            data_to_send: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        });

        let stream = Box::new(CowMessageStream {
            stream: reader,
            data_received: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        });

        (stream, sink)
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.callback_handler = Some(cb_handler)
    }

    fn set_keep_alive_interval(&mut self, _: Option<Duration>) {
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
}

pub struct CowMessageStream {
    pub stream: OwnedReadHalf,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
}

impl Stream for CowMessageStream {
    type Item = Result<CowRpcMessage, CowRpcError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        {
            let data_len = this.data_received.len();
            if data_len > 4 {
                let msg_len = ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize ;//.read_u32::<LittleEndian>()? as usize;
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
                        let msg_len = ReadBytesExt::read_u32::<LittleEndian>(&mut this.data_received.as_slice())? as usize;
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

impl StreamEx for CowMessageStream {
    fn close_on_keep_alive_timeout(&mut self, _close: bool) {
        // Nothing to do
    }
}

pub struct CowMessageSink {
    pub stream: OwnedWriteHalf,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
}

impl Sink<CowRpcMessage> for CowMessageSink {
    type Error = CowRpcError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
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

        debug!(">> {}", msg.get_msg_info());
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

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
