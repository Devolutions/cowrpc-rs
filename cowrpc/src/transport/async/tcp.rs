use byteorder::{LittleEndian, ReadBytesExt};
use crate::error::CowRpcError;
use futures::{self, Async, AsyncSink, Future, Sink, Stream};
use crate::proto::{CowRpcMessage, Message};
use std::{
    io::{ErrorKind, Read, Write},
    net::SocketAddr,
    time::{Duration, Instant},
};
use crate::transport::{
    r#async::{Transport, CowFuture, CowSink, CowStream},
    uri::Uri,
    MessageInterceptor, TransportError,
};
use tokio_tcp::TcpStream;

pub struct TcpTransport {
    stream: TcpStream,
    callback_handler: Option<Box<dyn MessageInterceptor>>,
    connected_at: Instant,
}

impl Clone for TcpTransport {
    fn clone(&self) -> Self {
        let stream = self
            .stream
            .try_clone()
            .expect("Async router implementation rely on tcpstream being cloned, this is fatal");

        TcpTransport {
            stream,
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
            connected_at: self.connected_at,
        }
    }
}

impl TcpTransport {
    pub fn new(stream: TcpStream, msg_inter: Option<Box<dyn MessageInterceptor>>) -> Self {
        TcpTransport {
            stream,
            callback_handler: msg_inter,
            connected_at: Instant::now(),
        }
    }
}

impl Transport for TcpTransport {
    fn connect(uri: Uri) -> CowFuture<Self>
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

                return Box::new(
                    TcpStream::connect(&sock_addr)
                        .map(|stream| TcpTransport {
                            stream,
                            callback_handler: None,
                            connected_at: Instant::now(),
                        }).map_err(|err| {
                            error!("{:?}", err);
                            CowRpcError::from(TransportError::UnableToConnect)
                        }),
                );
            }
        }

        Box::new(futures::failed(
            TransportError::InvalidUrl("Unable to resolve hostname".to_string()).into(),
        ))
    }

    fn message_sink(&mut self) -> CowSink<CowRpcMessage> {
        let stream = self
            .stream
            .try_clone()
            .expect("Async router implementation rely on tcpstream being cloned, this is fatal");

        Box::new(CowMessageSink {
            stream,
            data_to_send: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        })
    }

    fn message_stream(&mut self) -> CowStream<CowRpcMessage> {
        let stream = self
            .stream
            .try_clone()
            .expect("Async router implementation rely on tcpstream being cloned, this is fatal");

        Box::new(CowMessageStream {
            stream,
            data_received: Vec::new(),
            callback_handler: self.callback_handler.as_ref().map(|cbh| cbh.clone_boxed()),
        })
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<dyn MessageInterceptor>) {
        self.callback_handler = Some(cb_handler)
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
    pub stream: TcpStream,
    pub data_received: Vec<u8>,
    pub callback_handler: Option<Box<dyn MessageInterceptor>>,
}

impl Stream for CowMessageStream {
    type Item = CowRpcMessage;
    type Error = CowRpcError;

    fn poll(&mut self) -> ::std::result::Result<Async<Option<<Self as Stream>::Item>>, <Self as Stream>::Error> {
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
            let mut buff = [0u8; 4096];
            let result = self.stream.read(&mut buff);
            match result {
                Ok(0) => {
                    return Ok(Async::Ready(None));
                }
                Ok(n) => {
                    let data_len = n;
                    let mut buff = buff.to_vec();
                    buff.truncate(data_len);
                    self.data_received.append(&mut buff);

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
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    return Ok(Async::NotReady);
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }
}

pub struct CowMessageSink {
    pub stream: TcpStream,
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

        info!(">> {}", msg.get_msg_info());
        msg.write_to(&mut self.data_to_send)?;
        Ok(AsyncSink::Ready)
    }

    fn poll_complete(&mut self) -> ::std::result::Result<Async<()>, <Self as Sink>::SinkError> {
        if !self.data_to_send.is_empty() {
            let res = self.stream.write_all(&self.data_to_send);
            if let Err(e) = res {
                if let ErrorKind::WouldBlock = e.kind() {
                    return Ok(Async::NotReady);
                }

                return Err(e.into());
            }
            self.data_to_send.clear();
        }
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> ::std::result::Result<Async<()>, <Self as Sink>::SinkError> {
        Ok(Async::Ready(()))
    }
}
