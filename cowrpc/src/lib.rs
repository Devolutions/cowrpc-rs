extern crate bufstream;
extern crate byteorder;
extern crate bytes;
extern crate mouscache;
#[macro_use]
extern crate mouscache_derive;
extern crate futures;
extern crate parking_lot;
extern crate rand;
extern crate rmp;
extern crate time;
extern crate timer;
extern crate tokio;
extern crate url;

pub use crate::proto::{CowRpcMessage, Message};
pub use crate::transport::{CowFuture, CowRpcMessageInterceptor, MessageInjector as CowRpcMessageInjector};
use futures::channel::oneshot::Sender as AsyncSender;

use crate::error::{CowRpcError, CowRpcErrorCode, Result};

pub mod error;
pub mod peer;
mod proto;
pub mod router;
pub mod transport;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcRole {
    Peer,
    Router,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcMode {
    Routed,
    Direct,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CowRpcState {
    Initial,
    Handshake,
    Active,
    Terminate,
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
            Ok(self.node_id.unwrap())
        } else {
            Err(CowRpcError::CowRpcFailure(self.error.clone()))
        }
    }

    fn get_reverse_result(&self) -> Result<String> {
        if self.error == CowRpcErrorCode::Success {
            Ok(self.name.as_ref().unwrap().clone())
        } else {
            Err(CowRpcError::CowRpcFailure(self.error.clone()))
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
    None,
    /// User Principal Name
    Upn,
    /// Server Principal Name
    Spn,
}

impl std::string::ToString for CowRpcIdentityType {
    fn to_string(&self) -> String {
        use crate::CowRpcIdentityType::*;

        match *self {
            None => "NONE".to_string(),
            Upn => "UPN".to_string(),
            Spn => "SPN".to_string(),
        }
    }
}

impl std::str::FromStr for CowRpcIdentityType {
    type Err = CowRpcError;

    fn from_str(s: &str) -> Result<Self> {
        use crate::CowRpcIdentityType::*;

        Ok(match s {
            "UPN" => Upn,
            "SPN" => Spn,
            _ => None,
        })
    }
}

impl CowRpcIdentityType {
    pub fn try_from(typ: u8) -> Result<Self> {
        match typ {
            0 => Ok(CowRpcIdentityType::None),
            1 => Ok(CowRpcIdentityType::Upn),
            2 => Ok(CowRpcIdentityType::Spn),
            _ => Err(error::CowRpcError::Proto(format!("Unknown identity type - ({})", typ))),
        }
    }

    pub fn into(&self) -> u8 {
        match *self {
            CowRpcIdentityType::None => 0,
            CowRpcIdentityType::Upn => 1,
            CowRpcIdentityType::Spn => 2,
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
