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

use async_trait::async_trait;
use futures::channel::oneshot::Sender as AsyncSender;
use futures::Future;
use std::io::prelude::*;
use std::sync::Arc;
//TODO FD REMOVE
//use mio::{Events, Poll, PollOpt, Ready, Token};
//use mio_extras::channel::Sender;
pub use crate::proto::{CowRpcMessage, Message};
pub use crate::transport::r#async::CowFuture;
pub use crate::transport::tls::{TlsOptions, TlsOptionsBuilder};
pub use crate::transport::{CowRpcMessageInterceptor, MessageInjector as CowRpcMessageInjector};
use tokio::sync::RwLock;

use crate::error::{CowRpcError, CowRpcErrorCode, Result};
//use crate::peer::*;
// use crate::proto::*;
//use crate::transport::sync::{CowRpcListener, CowRpcTransport};
//use crate::cancel_event::CancelEventHandle;

pub mod async_peer;
pub mod async_router;
//pub mod cancel_event;
pub mod error;
pub mod msgpack;
//pub mod peer;
mod proto;
//pub mod router;
pub mod transport;

// const NEW_CONNECTION: Token = Token(1);

pub type CallFuture<T> = Box<dyn Future<Output = std::result::Result<T, ()>> + Unpin + Send>;

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

/// An RPC interface.
///
/// In comparison with the `CowRpcIfaceReg` type, this type contains the mapping of local and network ids.
pub struct CowRpcIface {
    pub name: String,
    pub lid: u16,
    pub rid: u16,
    pub procs: Vec<CowRpcProc>,
    pub server: Option<Box<dyn Server>>,
}

impl CowRpcIface {
    pub fn get_proc(&self, proc_id: u16, is_local_id: bool) -> Option<&CowRpcProc> {
        for procedure in &self.procs {
            if (is_local_id && proc_id == procedure.lid) || (!is_local_id && proc_id == procedure.rid) {
                return Some(&procedure);
            }
        }
        None
    }

    pub fn set_server(&mut self, server: Option<Box<dyn Server>>) {
        self.server = server
    }
}

pub struct CowRpcAsyncIface {
    pub name: String,
    pub lid: u16,
    pub rid: u16,
    pub procs: Vec<CowRpcProc>,
    pub server: Option<Box<dyn AsyncServer>>,
}

impl CowRpcAsyncIface {
    pub fn get_proc(&self, proc_id: u16, is_local_id: bool) -> Option<&CowRpcProc> {
        for procedure in &self.procs {
            if (is_local_id && proc_id == procedure.lid) || (!is_local_id && proc_id == procedure.rid) {
                return Some(&procedure);
            }
        }
        None
    }

    pub fn set_server(&mut self, server: Option<Box<dyn AsyncServer>>) {
        self.server = server
    }
}

#[async_trait]
trait Iface {
    async fn get_remote_id(&self) -> u16;
    async fn get_local_id(&self) -> u16;
}

#[async_trait]
impl Iface for Arc<RwLock<CowRpcIface>> {
    async fn get_remote_id(&self) -> u16 {
        let iface = self.read().await;
        iface.rid
    }

    async fn get_local_id(&self) -> u16 {
        let iface = self.read().await;
        iface.lid
    }
}

#[async_trait]
impl Iface for Arc<RwLock<CowRpcAsyncIface>> {
    async fn get_remote_id(&self) -> u16 {
        let iface = self.read().await;
        return iface.rid;
    }

    async fn get_local_id(&self) -> u16 {
        let iface = self.read().await;
        return iface.lid;
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcProc {
    pub name: String,
    pub lid: u16,
    pub rid: u16,
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

pub struct CowRpcAsyncBindContext {
    pub is_server: bool,
    pub remote_id: u32,
    pub iface: Arc<RwLock<CowRpcAsyncIface>>,
}

impl CowRpcAsyncBindContext {
    pub fn new(is_server: bool, remote_id: u32, iface: &Arc<RwLock<CowRpcAsyncIface>>) -> Arc<CowRpcAsyncBindContext> {
        Arc::new(CowRpcAsyncBindContext {
            is_server,
            remote_id,
            iface: iface.clone(),
        })
    }

    pub async fn get_iface_remote_id(&self) -> u16 {
        return self.iface.read().await.rid;
    }

    pub async fn get_proc_remote_id(&self, local_proc_id: u16) -> Option<u16> {
        let iface = self.iface.read().await;
        let p_opt = iface.get_proc(local_proc_id, true);
        if let Some(p) = p_opt {
            return Some(p.rid);
        }
        None
    }
}

/// A bind context.
///
/// A bind context is needed to send calls to a peer on a specific interface.
pub struct CowRpcBindContext {
    /// The local peer acts as the server
    pub is_server: bool,
    /// The id of the remote peer
    pub remote_id: u32,
    /// The interface
    pub iface: Arc<RwLock<CowRpcIface>>,
}

impl CowRpcBindContext {
    pub fn new(is_server: bool, remote_id: u32, iface: &Arc<RwLock<CowRpcIface>>) -> Arc<CowRpcBindContext> {
        Arc::new(CowRpcBindContext {
            is_server,
            remote_id,
            iface: iface.clone(),
        })
    }

    pub async fn get_iface_remote_id(&self) -> u16 {
        self.iface.read().await.rid
    }

    pub async fn get_proc_remote_id(&self, local_proc_id: u16) -> Option<u16> {
        let iface = self.iface.read().await;
        let p_opt = iface.get_proc(local_proc_id, true);
        if let Some(p) = p_opt {
            Some(p.rid)
        } else {
            None
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

/// An RPC interface registration.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CowRpcIfaceReg {
    name: &'static str,
    procs: Vec<CowRpcProcReg>,
}

impl CowRpcIfaceReg {
    pub fn new(name: &'static str, procs: Vec<CowRpcProcReg>) -> CowRpcIfaceReg {
        CowRpcIfaceReg { name, procs }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct CowRpcProcReg {
    pub name: &'static str,
    pub id: u16,
}

impl CowRpcProcReg {
    pub fn new(name: &'static str, id: u16) -> CowRpcProcReg {
        CowRpcProcReg { name, id }
    }
}

pub trait CowRpcParams: Send + Sync {
    fn read_from<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized;

    fn write_to(&self, writer: &mut dyn Write) -> Result<()>;

    fn get_size(&self) -> Result<u32> {
        let mut buffer = Vec::new();
        self.write_to(&mut buffer)?;
        Ok(buffer.len() as u32)
    }
}

pub trait Server: Send + Sync {
    fn dispatch_call(&self, caller_id: u32, proc_id: u16, payload: &mut Vec<u8>) -> Result<Box<dyn CowRpcParams>>;
}

pub trait AsyncServer: Send + Sync {
    fn dispatch_call(&self, caller_id: u32, proc_id: u16, payload: &mut Vec<u8>) -> CowFuture<Box<dyn CowRpcParams>>;
}
