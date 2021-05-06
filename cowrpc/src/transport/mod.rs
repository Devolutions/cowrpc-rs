use std;
use std::any::Any;
use std::fmt;

//use mio::{Evented, Poll, PollOpt, Ready, Token};

use crate::error::Result;
use crate::proto::CowRpcMessage;

pub mod r#async;
mod uri;

pub use crate::transport::uri::{Uri, UriError};

pub enum SupportedProto {
    Tcp,
    WebSocket,
}

/// A set of options for a TLS connection.
pub struct TlsOptions {
    pub certificate_file_path: String,
    pub private_key_file_path: String,
}

pub trait MessageInjector: Sync {
    fn inject(&self, msg: CowRpcMessage);
}

pub trait MessageInterceptor: Send + Sync {
    fn before_send(&mut self, msg: CowRpcMessage) -> Option<CowRpcMessage>;
    fn before_recv(&mut self, msg: CowRpcMessage) -> Option<CowRpcMessage>;
    fn as_any(&self) -> &dyn Any;
    fn clone_boxed(&self) -> Box<dyn MessageInterceptor>;
}

impl Clone for Box<dyn MessageInterceptor> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

pub struct CowRpcMessageInterceptor<T>
where
    T: Sized + Send + Sync + Clone,
{
    pub cb_param: Box<T>,
    pub before_send: Option<fn(&Box<T>, CowRpcMessage) -> Option<CowRpcMessage>>,
    pub before_recv: Option<fn(&Box<T>, CowRpcMessage) -> Option<CowRpcMessage>>,
}

impl<T: Send + Sync> MessageInterceptor for CowRpcMessageInterceptor<T>
where
    T: 'static + Sized + Send + Clone,
{
    fn before_send(&mut self, msg: CowRpcMessage) -> Option<CowRpcMessage> {
        if let Some(func) = self.before_send {
            func(&self.cb_param, msg)
        } else {
            None
        }
    }

    fn before_recv(&mut self, msg: CowRpcMessage) -> Option<CowRpcMessage> {
        if let Some(func) = self.before_recv {
            func(&self.cb_param, msg)
        } else {
            None
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_boxed(&self) -> Box<dyn MessageInterceptor> {
        Box::new(CowRpcMessageInterceptor {
            cb_param: self.cb_param.clone(),
            before_send: self.before_send,
            before_recv: self.before_recv,
        })
    }
}

#[derive(Debug)]
pub enum TransportError {
    InvalidUri(uri::UriError),
    InvalidUrl(String),
    InvalidProtocol(String),
    PortAlreadyInUse(String),
    EndpointUnreachable(String),
    TlsError(String),
    WsError(String),
    UnableToConnect,
    ConnectionReset,
    Other,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransportError::InvalidUri(ref e) => e.fmt(f),
            TransportError::InvalidUrl(ref desc) => write!(f, "Invalid url: {}", desc),
            TransportError::InvalidProtocol(ref desc) => write!(f, "Invalid protocol: {}", desc),
            TransportError::PortAlreadyInUse(ref desc) => write!(f, "Port already in use: {}", desc),
            TransportError::EndpointUnreachable(ref desc) => write!(f, "Endpoint unreachable: {}", desc),
            TransportError::UnableToConnect => write!(f, "Unable to connect"),
            TransportError::ConnectionReset => write!(f, "Connection reset"),
            TransportError::Other => write!(f, "Unknown"),
            TransportError::TlsError(ref desc) => write!(f, "Tls error: {}", desc),
            TransportError::WsError(ref desc) => write!(f, "WebSocket error: {}", desc),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<::async_tungstenite::tungstenite::Error> for TransportError {
    fn from(e: ::async_tungstenite::tungstenite::Error) -> Self {
        TransportError::WsError(e.to_string())
    }
}

pub type CowRpcTransportError = TransportError;
