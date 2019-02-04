use byteorder::{LittleEndian, ReadBytesExt};
use error::Result;
use mio::net::TcpStream;
use mio::{Evented, Poll, PollOpt, Ready, Token};
use proto::{CowRpcMessage, Message};
use std;
use std::io::{ErrorKind, Read, Write};
use std::net::Shutdown;
use std::net::SocketAddr;
use transport::uri::Uri;
use transport::{MessageInterceptor, sync::Transport, TransportError};

pub struct TcpTransport {
    pub stream: TcpStream,
    pub data_received: Vec<u8>,
    pub data_to_send: Vec<u8>,
    pub callback_handler: Option<Box<MessageInterceptor>>,
    pub connected_at: std::time::Instant,
}

impl TcpTransport {
    pub fn new(stream: TcpStream) -> TcpTransport {
        TcpTransport {
            stream,
            data_received: Vec::new(),
            data_to_send: Vec::new(),
            callback_handler: None,
            connected_at: std::time::Instant::now(),
        }
    }

    pub fn connect(uri: Uri) -> Result<TcpTransport> {
        let mut port: u16 = 80;
        if let Some(p) = uri.port() {
            port = p
        }

        if let Ok(addrs) = uri.get_addrs() {
            for addr in addrs {
                let sock_addr = SocketAddr::new(addr, port);

                if let Ok(stream) = TcpStream::connect(&sock_addr) {
                    return Ok(TcpTransport::new(stream));
                }
            }
        }

        Err(TransportError::UnableToConnect.into())
    }
}

impl Transport for TcpTransport {
    fn get_interest(&self) -> Ready {
        let mut ready = Ready::empty();
        ready.insert(Ready::readable());

        if !self.data_to_send.is_empty() {
            ready.insert(Ready::writable());
        }

        ready
    }

    fn send_data(&mut self) -> Result<()> {
        assert_ne!(self.data_to_send.len(), 0);

        self.stream.write_all(&self.data_to_send)?;
        self.data_to_send.clear();
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
                            None => {}
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
        loop {
            let result = self.stream.read_to_end(&mut self.data_received);
            match result {
                Ok(0) => {
                    return Ok(());
                }
                Ok(_n) => {}
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }

    fn set_message_interceptor(&mut self, cb_handler: Box<MessageInterceptor>) {
        self.callback_handler = Some(cb_handler)
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        self.stream.local_addr().ok()
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        self.stream.peer_addr().ok()
    }

    fn up_time(&self) -> std::time::Duration {
        std::time::Instant::now().duration_since(self.connected_at)
    }

    fn shutdown(&mut self) -> Result<()> {
        self.stream
            .shutdown(Shutdown::Both)
            .map_err(|_| TransportError::ConnectionReset.into())
    }
}

impl Evented for TcpTransport {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        self.stream.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt) -> std::io::Result<()> {
        self.stream.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> std::io::Result<()> {
        self.stream.deregister(poll)
    }
}
