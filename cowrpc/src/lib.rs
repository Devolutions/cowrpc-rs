extern crate bufstream;
extern crate byteorder;
extern crate bytes;
extern crate dns_lookup;
//TODO FD REMOVE
//extern crate mio;
//extern crate mio_extras;
extern crate rmp;
extern crate url;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate mouscache;
extern crate rand;
#[macro_use]
extern crate mouscache_derive;
extern crate futures;
extern crate parking_lot;
extern crate time;
extern crate timer;
extern crate tls_api;
extern crate tls_api_native_tls;
extern crate tokio;
//todo fd remove
//extern crate tokio_tcp;
//extern crate tungstenite;

use futures::channel::oneshot::Sender as AsyncSender;
//TODO FD REMOVE
//use mio::{Events, Poll, PollOpt, Ready, Token};
//use mio_extras::channel::Sender;
pub use crate::proto::{CowRpcMessage, Message};
pub use crate::transport::r#async::CowFuture;
pub use crate::transport::tls::{TlsOptions, TlsOptionsBuilder};
pub use crate::transport::{CowRpcMessageInterceptor, MessageInjector as CowRpcMessageInjector};

use crate::error::{CowRpcError, CowRpcErrorCode, Result};

pub mod async_peer;
pub mod async_router;
pub mod error;
mod proto;
pub mod transport;

// const NEW_CONNECTION: Token = Token(1);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcRole {
    PEER,
    ROUTER,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcMode {
    ROUTED,
    DIRECT,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CowRpcState {
    INITIAL,
    HANDSHAKE,
    ACTIVE,
    TERMINATE,
}

pub struct CowRpcAsyncResolveReq {
    id: usize,
    node_id: Option<u32>,
    name: Option<String>,
    reverse: bool,
    tx: Option<AsyncSender<CowRpcAsyncResolveRsp>>,
}

#[derive(Debug)]
struct CowRpcAsyncResolveRsp {
    error: CowRpcErrorCode,
    node_id: Option<u32>,
    name: Option<String>,
}

impl CowRpcAsyncResolveRsp {
    fn get_result(&self) -> Result<u32> {
        if self.error == CowRpcErrorCode::Success {
            return Ok(self.node_id.unwrap());
        } else {
            return Err(CowRpcError::CowRpcFailure(self.error.clone()));
        }
    }

    fn get_reverse_result(&self) -> Result<String> {
        if self.error == CowRpcErrorCode::Success {
            return Ok(self.name.as_ref().unwrap().clone());
        } else {
            return Err(CowRpcError::CowRpcFailure(self.error.clone()));
        }
    }
}

pub struct CowRpcAsyncVerifyReq {
    id: usize,
    call_id: u32,
    tx: Option<AsyncSender<CowRpcAsyncVerifyRsp>>,
}

pub struct CowRpcAsyncVerifyRsp {
    _error: CowRpcErrorCode,
    pub payload: Vec<u8>,
}

impl CowRpcAsyncVerifyRsp {
    fn _is_success(&self) -> bool {
        self._error == CowRpcErrorCode::Success
    }

    fn _get_error(&self) -> CowRpcErrorCode {
        self._error.clone()
    }
}

pub struct CowRpcAsyncHttpReq {
    id: usize,
    call_id: u32,
    tx: Option<AsyncSender<CowRpcAsyncHttpRsp>>,
}

pub struct CowRpcAsyncHttpRsp {
    _error: CowRpcErrorCode,
    pub http_rsp: Vec<u8>,
}

impl CowRpcAsyncHttpRsp {
    fn _is_success(&self) -> bool {
        self._error == CowRpcErrorCode::Success
    }

    fn _get_error(&self) -> CowRpcErrorCode {
        self._error.clone()
    }
}

pub enum CowRpcAsyncReq {
    Resolve(CowRpcAsyncResolveReq),
    Verify(CowRpcAsyncVerifyReq),
    Http(CowRpcAsyncHttpReq),
}

impl CowRpcAsyncReq {
    fn get_id(&self) -> usize {
        match self {
            CowRpcAsyncReq::Resolve(ref req) => req.id,
            CowRpcAsyncReq::Verify(ref req) => req.id,
            CowRpcAsyncReq::Http(ref req) => req.id,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CowRpcIdentityType {
    /// Anonymous identity
    NONE,
    /// User Principal Name
    UPN,
    /// Server Principal Name
    SPN,
}

impl std::string::ToString for CowRpcIdentityType {
    fn to_string(&self) -> String {
        use crate::CowRpcIdentityType::*;

        match *self {
            NONE => "NONE".to_string(),
            UPN => "UPN".to_string(),
            SPN => "SPN".to_string(),
        }
    }
}

impl std::str::FromStr for CowRpcIdentityType {
    type Err = CowRpcError;

    fn from_str(s: &str) -> Result<Self> {
        use crate::CowRpcIdentityType::*;

        Ok(match s {
            "UPN" => UPN,
            "SPN" => SPN,
            _ => NONE,
        })
    }
}

impl CowRpcIdentityType {
    pub fn try_from(typ: u8) -> Result<Self> {
        match typ {
            0 => Ok(CowRpcIdentityType::NONE),
            1 => Ok(CowRpcIdentityType::UPN),
            2 => Ok(CowRpcIdentityType::SPN),
            _ => Err(error::CowRpcError::Proto(format!("Unknown identity type - ({})", typ))),
        }
    }

    pub fn into(&self) -> u8 {
        match *self {
            CowRpcIdentityType::NONE => 0,
            CowRpcIdentityType::UPN => 1,
            CowRpcIdentityType::SPN => 2,
        }
    }
}

/// Provides information about the remote peer when processing a call.
pub struct CowRpcCallContext {
    caller_id: u32,
}

impl CowRpcCallContext {
    pub fn new(caller_id: u32) -> Self {
        CowRpcCallContext { caller_id }
    }

    /// Returns the id of peer sending the call.
    pub fn get_caller_id(&self) -> u32 {
        self.caller_id
    }
}
