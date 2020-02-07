extern crate bufstream;
extern crate byteorder;
extern crate bytes;
extern crate dns_lookup;
extern crate mio;
extern crate mio_extras;
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
extern crate tokio_tcp;
extern crate tungstenite;

use std::io::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use futures::Future;
use futures::sync::oneshot::Sender as AsyncSender;
use mio::{Events, Poll, PollOpt, Ready, Token};
use mio_extras::channel::Sender;
use parking_lot::{Mutex, RwLock};

pub use crate::transport::r#async::CowFuture;
pub use crate::transport::tls::{TlsOptions, TlsOptionsBuilder};
pub use crate::proto::CowRpcMessage;
pub use crate::proto::Message;
pub use crate::transport::CowRpcMessageInterceptor;
pub use crate::transport::MessageInjector as CowRpcMessageInjector;

use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::peer::*;
use crate::proto::*;
use crate::transport::sync::{CowRpcListener, CowRpcTransport};
use crate::cancel_event::CancelEventHandle;

pub mod async_peer;
pub mod async_router;
pub mod cancel_event;
pub mod error;
pub mod msgpack;
pub mod peer;
mod proto;
pub mod router;
pub mod transport;

const NEW_CONNECTION: Token = Token(1);

pub type CallFuture<T> = Box<dyn Future<Item = T, Error = ()> + Send>;

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

/// An RPC context.
pub struct CowRpc {
    role: CowRpcRole,
    mode: CowRpcMode,
    listener: Mutex<Option<CowRpcListener>>,
    server_poll: Poll,
    ifaces: Mutex<Vec<Arc<RwLock<CowRpcIface>>>>,
}

impl CowRpc {
    /// Creates an RPC context with the provided role and mode.
    pub fn new(role: CowRpcRole, mode: CowRpcMode) -> CowRpc {
        CowRpc {
            role,
            mode,
            listener: Mutex::new(None),
            server_poll: Poll::new().unwrap(),
            ifaces: Mutex::new(Vec::new()),
        }
    }

    /// Registers an interface.
    ///
    /// If the interface is registered to make calls to it, `server` should be `None`.
    ///
    /// If calls to this interface are implemented, `server` should point to an object that implements the `Server`
    /// trait. This object is obtained by calling `ServerDipatch::new` on the generated interface.
    ///
    /// ```ignore
    /// rpc.register_iface(
    ///     cow_nowsession_iface_get_def(),
    ///     Some(Box::new(now_session_cow::ServerDispatch::new(now_session_iface))));
    /// ```
    ///
    /// On success, this method returns the id of the registered interface.
    pub fn register_iface(&self, iface_reg: CowRpcIfaceReg, server: Option<Box<dyn Server>>) -> Result<u16> {
        let mut ifaces = self.ifaces.lock();

        let iface_id = ifaces.len() as u16;

        let mut iface = CowRpcIface {
            name: String::from(iface_reg.name),
            lid: iface_id,
            rid: 0,
            procs: Vec::new(),
            server,
        };

        for procedure in iface_reg.procs {
            iface.procs.push(CowRpcProc {
                lid: procedure.id,
                rid: 0,
                name: String::from(procedure.name),
            })
        }

        ifaces.push(Arc::new(RwLock::new(iface)));
        Ok(iface_id)
    }

    pub fn get_iface(&self, iface_id: u16, is_local_id: bool) -> Option<Arc<RwLock<CowRpcIface>>> {
        let ifaces = self.ifaces.lock();

        let ifaces = ifaces.deref();
        for iface_mutex in ifaces.iter() {
            let iface = iface_mutex.read();
            if (is_local_id && iface.lid == iface_id) || (!is_local_id && iface.rid == iface_id) {
                return Some(iface_mutex.clone());
            }
        }
        None
    }

    pub fn set_iface_server(&self, iface_id: u16, server: Option<Box<dyn Server>>) {
        let ifaces = self.ifaces.lock();

        let ifaces = ifaces.deref();
        for iface_mutex in ifaces.iter() {
            let mut iface = iface_mutex.write();
            if iface.lid == iface_id {
                iface.set_server(server);
                break;
            }
        }
    }

    fn register_iface_def(&self, iface_def: &mut CowRpcIfaceDef, server: bool) -> Result<()> {
        let ifaces = self.ifaces.lock();
        let mut iface_found = false;

        let ifaces = ifaces.deref();
        for iface in ifaces.iter() {
            let mut iface = iface.write();
            if iface_def.name.eq(&iface.name) {
                iface_found = true;

                if server {
                    iface.rid = iface.lid;
                    iface_def.id = iface.rid;
                } else {
                    iface.rid = iface_def.id;
                }

                for proc_def in &mut iface_def.procs {
                    let mut proc_found = false;

                    for procedure in &mut iface.procs {
                        if proc_def.name.eq(&procedure.name) {
                            proc_found = true;

                            if server {
                                procedure.rid = procedure.lid;
                                proc_def.id = procedure.rid;
                            } else {
                                procedure.rid = proc_def.id;
                            }
                        }
                    }

                    if !proc_found {
                        return Err(error::CowRpcError::Proto(format!(
                            "Proc name not found - ({})",
                            proc_def.name
                        )));
                    }
                }
            }
        }

        if !iface_found {
            return Err(error::CowRpcError::Proto(format!(
                "IFace name not found - ({})",
                iface_def.name
            )));
        }

        Ok(())
    }

    /// Listen for RPC clients on `url`.
    pub fn server_listen(&self, url: &str, _cert_path: Option<&str>, _pkey_path: Option<&str>) -> Result<()> {
        let listener = transport::sync::ListenerBuilder::from_uri(url)?.build()?;
        self.server_poll
            .register(&listener, NEW_CONNECTION, Ready::readable(), PollOpt::edge())?;
        *self.listener.lock() = Some(listener);

        Ok(())
    }
}

/// A client is used to connect to a remote peer.
/// 
/// Client is an alternative to the `client_connect` function.
pub struct Client<'a> {
    rpc: Arc<CowRpc>,
    url: String,
    timeout: Option<Duration>,
    cancel_handle: Option<&'a CancelEventHandle>,
    tls_options: Option<TlsOptions>,
}

impl<'a> Client<'a> {
    /// Get an instance of a client.
    pub fn new(rpc: &Arc<CowRpc>, url: &str) -> Client<'a> {
        Client{
            rpc: rpc.clone(),
            url: url.to_string(),
            timeout: None,
            cancel_handle: None,
            tls_options: None,
        }
    }

    /// Set the default timeout.
    pub fn timeout(mut self, timeout: Duration)  -> Client<'a> {
        self.timeout = Some(timeout);
        self
    }

    /// Set a cancel handle on the client.
    pub fn cancel_handle(mut self, cancel_handle: &'a CancelEventHandle) -> Client<'a> {
        self.cancel_handle = Some(cancel_handle);
        self
    }

    /// Set TLS options.
    pub fn tls_options(mut self, tls_options: TlsOptions) -> Client<'a> {
        self.tls_options = Some(tls_options);
        self
    }

    /// Connects to a remote peer.
    /// 
    /// On success, this function returns a `CowRpcPeer` that represents the remote peer.
    pub fn connect(self) -> Result<Arc<CowRpcPeer>> {
        let transport = CowRpcTransport::from_url(&self.url, self.tls_options)?;
        let (msg_to_send_tx, msg_to_send_rx) = mio_extras::channel::channel();
        let peer = CowRpcPeer::new(transport, msg_to_send_rx, msg_to_send_tx, &self.rpc)?;
        peer.init_client(self.timeout, self.cancel_handle)?;
        Ok(Arc::new(peer))
    }
}

/// Connects to a remote peer.
///
/// On success, this function returns a `CowRpcPeer` that represents the remote peer.
pub fn client_connect(
    rpc: &Arc<CowRpc>,
    url: &str,
    timeout: Option<Duration>,
    cancel_handle: Option<&CancelEventHandle>,
) -> Result<Arc<CowRpcPeer>> {
    let transport = CowRpcTransport::from_url(url, None)?;
    let (msg_to_send_tx, msg_to_send_rx) = mio_extras::channel::channel();
    let peer = CowRpcPeer::new(transport, msg_to_send_rx, msg_to_send_tx, &rpc)?;
    peer.init_client(timeout, cancel_handle)?;
    Ok(Arc::new(peer))
}

/// Wait for a remote peer to connect to the server.
///
/// Returns the connected `CowRpcPeer` on success.
pub fn server_connect(
    rpc: &Arc<CowRpc>,
    timeout: Option<Duration>,
    cancel_handle: Option<&CancelEventHandle>,
) -> Result<Option<Arc<CowRpcPeer>>> {
    let listener_guard = rpc.listener.lock();
    assert_eq!(listener_guard.is_some(), true);
    let listener = listener_guard.as_ref().unwrap();

    // Create storage for events
    let mut events = Events::with_capacity(1024);

    //    info!("Waiting clients on port {}...", listener.local_addr().unwrap().port());
    loop {
        rpc.server_poll.poll(&mut events, None).unwrap();

        for event in events.iter() {
            match event.token() {
                NEW_CONNECTION => {
                    let transport = listener.accept().unwrap();
                    info!("New client");

                    let (msg_to_send_tx, msg_to_send_rx) = mio_extras::channel::channel();
                    let peer = CowRpcPeer::new(transport, msg_to_send_rx, msg_to_send_tx, &rpc)?;
                    peer.init_server(timeout, cancel_handle)?;

                    return Ok(Some(Arc::new(peer)));
                }
                _ => {}
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CowRpcState {
    INITIAL,
    HANDSHAKE,
    REGISTER,
    ACTIVE,
    TERMINATE,
}

pub struct CowRpcIdentifyReq {
    id: usize,
    name: String,
    tx: Sender<CowRpcIdentifyRsp>,
}

struct CowRpcIdentifyRsp {
    error: CowRpcErrorCode,
}

impl CowRpcIdentifyRsp {
    fn get_result(&self) -> Result<()> {
        if self.error == CowRpcErrorCode::Success {
            Ok(())
        } else {
            Err(CowRpcError::CowRpcFailure(self.error.clone()))
        }
    }
}

pub struct CowRpcResolveReq {
    id: usize,
    node_id: Option<u32>,
    name: Option<String>,
    reverse: bool,
    tx: Sender<CowRpcResolveRsp>,
}

struct CowRpcResolveRsp {
    error: CowRpcErrorCode,
    node_id: Option<u32>,
    name: Option<String>,
}

impl CowRpcResolveRsp {
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

pub struct CowRpcBindReq {
    id: usize,
    server_id: u32,
    iface_id: u16,
    tx: Sender<CowRpcBindRsp>,
}

struct CowRpcBindRsp {
    error: CowRpcErrorCode,
}

impl CowRpcBindRsp {
    fn is_success(&self) -> bool {
        self.error == CowRpcErrorCode::Success
    }

    fn get_error(&self) -> CowRpcErrorCode {
        self.error.clone()
    }
}

pub struct CowRpcUnbindReq {
    id: usize,
    from_client: bool,
    remote_id: u32,
    iface_id: u16,
    tx: Sender<CowRpcUnbindRsp>,
}

struct CowRpcUnbindRsp {
    error: CowRpcErrorCode,
}

impl CowRpcUnbindRsp {
    fn is_success(&self) -> bool {
        self.error == CowRpcErrorCode::Success
    }

    fn get_error(&self) -> CowRpcErrorCode {
        self.error.clone()
    }
}

pub struct CowRpcCallReq {
    id: usize,
    call_id: u32,
    iface_id: u16,
    proc_id: u16,
    tx: Sender<CowRpcCallRsp>,
}

pub struct CowRpcCallRsp {
    error: CowRpcErrorCode,
    pub msg_pack: Vec<u8>,
}

impl CowRpcCallRsp {
    fn is_success(&self) -> bool {
        self.error == CowRpcErrorCode::Success
    }

    fn get_error(&self) -> CowRpcErrorCode {
        self.error.clone()
    }
}

pub struct CowRpcVerifyReq {
    id: usize,
    call_id: u32,
    tx: Sender<CowRpcVerifyRsp>,
}

pub struct CowRpcVerifyRsp {
    _error: CowRpcErrorCode,
    pub payload: Vec<u8>,
}

impl CowRpcVerifyRsp {
    fn _is_success(&self) -> bool {
        self._error == CowRpcErrorCode::Success
    }

    fn _get_error(&self) -> CowRpcErrorCode {
        self._error.clone()
    }
}

pub struct CowRpcHttpReq {
    id: usize,
    call_id: u32,
    tx: Sender<CowRpcHttpRsp>,
}

pub struct CowRpcHttpRsp {
    _error: CowRpcErrorCode,
    pub http_rsp: Vec<u8>,
}

impl CowRpcHttpRsp {
    fn _is_success(&self) -> bool {
        self._error == CowRpcErrorCode::Success
    }

    fn _get_error(&self) -> CowRpcErrorCode {
        self._error.clone()
    }
}

/// Type of Cow Rpc Request a client can send.
pub enum CowRpcReq {
    Identify(CowRpcIdentifyReq),
    Resolve(CowRpcResolveReq),
    Bind(CowRpcBindReq),
    Unbind(CowRpcUnbindReq),
    Call(CowRpcCallReq),
    Verify(CowRpcVerifyReq),
    Http(CowRpcHttpReq),
}

impl CowRpcReq {
    fn get_id(&self) -> usize {
        match self {
            CowRpcReq::Identify(ref req) => req.id,
            CowRpcReq::Resolve(ref req) => req.id,
            CowRpcReq::Bind(ref req) => req.id,
            CowRpcReq::Unbind(ref req) => req.id,
            CowRpcReq::Call(ref req) => req.id,
            CowRpcReq::Verify(ref req) => req.id,
            CowRpcReq::Http(ref req) => req.id,
        }
    }
}

pub struct CowRpcAsyncIdentifyReq {
    id: usize,
    name: String,
    tx: Option<AsyncSender<CowRpcAsyncIdentifyRsp>>,
}

#[derive(Debug)]
struct CowRpcAsyncIdentifyRsp {
    error: CowRpcErrorCode,
}

impl CowRpcAsyncIdentifyRsp {
    fn get_result(&self) -> Result<()> {
        if self.error == CowRpcErrorCode::Success {
            return Ok(());
        } else {
            return Err(CowRpcError::CowRpcFailure(self.error.clone()));
        }
    }
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

pub struct CowRpcAsyncBindReq {
    id: usize,
    server_id: u32,
    iface_id: u16,
    tx: Option<AsyncSender<CowRpcAsyncBindRsp>>,
}

#[derive(Debug)]
struct CowRpcAsyncBindRsp {
    error: CowRpcErrorCode,
}

impl CowRpcAsyncBindRsp {
    fn is_success(&self) -> bool {
        return self.error == CowRpcErrorCode::Success;
    }

    fn get_error(&self) -> CowRpcErrorCode {
        return self.error.clone();
    }
}

pub struct CowRpcAsyncUnbindReq {
    id: usize,
    from_client: bool,
    remote_id: u32,
    iface_id: u16,
    tx: Option<AsyncSender<CowRpcAsyncUnbindRsp>>,
}

#[derive(Debug)]
struct CowRpcAsyncUnbindRsp {
    error: CowRpcErrorCode,
}

impl CowRpcAsyncUnbindRsp {
    fn is_success(&self) -> bool {
        return self.error == CowRpcErrorCode::Success;
    }

    fn get_error(&self) -> CowRpcErrorCode {
        return self.error.clone();
    }
}

pub struct CowRpcAsyncCallReq {
    id: usize,
    call_id: u32,
    iface_id: u16,
    proc_id: u16,
    tx: Option<AsyncSender<CowRpcAsyncCallRsp>>,
}

#[derive(Debug)]
pub struct CowRpcAsyncCallRsp {
    error: CowRpcErrorCode,
    pub msg_pack: Vec<u8>,
}

impl CowRpcAsyncCallRsp {
    fn is_success(&self) -> bool {
        return self.error == CowRpcErrorCode::Success;
    }

    fn get_error(&self) -> CowRpcErrorCode {
        return self.error.clone();
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
    Identify(CowRpcAsyncIdentifyReq),
    Resolve(CowRpcAsyncResolveReq),
    Bind(CowRpcAsyncBindReq),
    Unbind(CowRpcAsyncUnbindReq),
    Call(CowRpcAsyncCallReq),
    Verify(CowRpcAsyncVerifyReq),
    Http(CowRpcAsyncHttpReq),
}

impl CowRpcAsyncReq {
    fn get_id(&self) -> usize {
        match self {
            CowRpcAsyncReq::Identify(ref req) => req.id,
            CowRpcAsyncReq::Resolve(ref req) => req.id,
            CowRpcAsyncReq::Bind(ref req) => req.id,
            CowRpcAsyncReq::Unbind(ref req) => req.id,
            CowRpcAsyncReq::Call(ref req) => req.id,
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

trait Iface {
    fn get_remote_id(&self) -> u16;
    fn get_local_id(&self) -> u16;
}

impl Iface for Arc<RwLock<CowRpcIface>> {
    fn get_remote_id(&self) -> u16 {
        let iface = self.read();
        iface.rid
    }

    fn get_local_id(&self) -> u16 {
        let iface = self.read();
        iface.lid
    }
}

impl Iface for Arc<RwLock<CowRpcAsyncIface>> {
    fn get_remote_id(&self) -> u16 {
        let iface = self.read();
        return iface.rid;
    }

    fn get_local_id(&self) -> u16 {
        let iface = self.read();
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

    pub fn get_iface_remote_id(&self) -> u16 {
        return self.iface.read().rid;
    }

    pub fn get_proc_remote_id(&self, local_proc_id: u16) -> Option<u16> {
        let iface = self.iface.read();
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

    pub fn get_iface_remote_id(&self) -> u16 {
        self.iface.read().rid
    }

    pub fn get_proc_remote_id(&self, local_proc_id: u16) -> Option<u16> {
        let iface = self.iface.read();
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
