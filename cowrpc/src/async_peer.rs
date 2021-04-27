use crate::error::CowRpcError;
use crate::error::CowRpcErrorCode;
use futures::future::Future;
use futures::prelude::*;
use futures::channel::oneshot::{channel, Receiver, Sender};
use tokio::sync::{RwLock, Mutex};
use crate::proto::*;
use crate::proto::{CowRpcIfaceDef, Message};
use std::ops::Deref;
use std::sync::{
    atomic::{self, AtomicUsize},
    Arc,
};
use std::time::Duration;
use crate::transport::{
    r#async::{CowRpcTransport, Transport, CowFuture, CowSink, CowStreamEx},
    Uri,
};
use std::task::{Context, Poll};
use std::pin::Pin;
use crate::{CowRpcAsyncReq, CowRpcState, proto, CowRpcIdentityType, transport, CowRpcMode, CowRpcCallContext, CallFuture, CowRpcAsyncBindContext, CowRpcAsyncIface, CowRpcAsyncIdentifyRsp, CowRpcAsyncVerifyRsp, CowRpcAsyncHttpRsp, CowRpcAsyncResolveRsp, CowRpcAsyncBindRsp, Iface, CowRpcAsyncUnbindRsp, CowRpcAsyncCallRsp, CowRpcParams, CowRpcIfaceReg, AsyncServer, CowRpcProc, CowRpcAsyncIdentifyReq, CowRpcAsyncVerifyReq, CowRpcAsyncHttpReq, CowRpcAsyncResolveReq, CowRpcAsyncBindReq, CowRpcAsyncUnbindReq, CowRpcAsyncCallReq};
use crate::error::Result;
use tokio::stream::StreamExt;
use pin_utils::pin_mut;
use crate::transport::r#async::StreamEx;
use futures::ready;

type HandleMonitor = Arc<Mutex<Option<PeerHandle>>>;
type HandleMsgProcessor = Arc<RwLock<Option<CowRpcPeerAsyncMsgProcessor>>>;
type HandleThreadHandle = Arc<Mutex<Option<std::thread::JoinHandle<Result<()>>>>>;
type HttpMsgCallback = dyn Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>> + Send + Sync;
type UnbindCallback = dyn Fn(Arc<CowRpcAsyncBindContext>) + Send + Sync;
pub type PeerMonitor = Receiver<()>;

pub struct PeerHandle {
    sender: Sender<()>,
}

impl PeerHandle {
    pub fn exit(self) {
        let _ = self.sender.send(());
    }
}

static COWRPC_REQ_ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
struct CowRpcPeerSharedInner {
    id: Arc<RwLock<u32>>,
    router_id: Arc<RwLock<u32>>,
    mode: CowRpcMode,
    ifaces: Arc<RwLock<Vec<Arc<RwLock<CowRpcAsyncIface>>>>>,
    bind_contexts: Arc<RwLock<Vec<Arc<CowRpcAsyncBindContext>>>>,
    state: Arc<RwLock<CowRpcState>>,
    requests: Arc<Mutex<Vec<CowRpcAsyncReq>>>,
    writer_sink: Arc<Mutex<CowSink<CowRpcMessage>>>,
    on_unbind_callback: Arc<Option<Box<UnbindCallback>>>,
    on_http_msg_callback: Arc<Option<Box<HttpMsgCallback>>>,
}

impl CowRpcPeerSharedInner {
    async fn get_id(&self) -> u32 {
        *self.id.read().await
    }

    async fn get_router_id(&self) -> u32 {
        *self.router_id.read().await
    }

    async fn set_id(&self, id: u32) {
        *self.id.write().await = id;
    }

    async fn set_router_id(&self, router_id: u32) {
        *self.router_id.write().await = router_id;
    }

    async fn get_state(&self) -> CowRpcState {
        *self.state.read().await
    }

    async fn transition_to_state(&self, new_state: CowRpcState) {
        *self.state.write().await = new_state;
    }

    pub async fn get_iface(&self, iface_id: u16, is_local_id: bool) -> Option<Arc<RwLock<CowRpcAsyncIface>>> {
        let ifaces = self.ifaces.read().await;

        let ifaces = ifaces.deref();
        for iface_mutex in ifaces.iter() {
            let iface = iface_mutex.read().await;
            if (is_local_id && iface.lid == iface_id) || (!is_local_id && iface.rid == iface_id) {
                return Some(iface_mutex.clone());
            }
        }

        None
    }

    async fn register_iface_def(&self, iface_def: &mut CowRpcIfaceDef, server: bool) -> Result<()> {
        let ifaces = self.ifaces.write().await;
        let mut iface_found = false;

        let ifaces = ifaces.deref();
        for iface in ifaces.iter() {
            let mut iface = iface.write().await;
            if iface_def.name.eq(&iface.name) {
                iface_found = true;

                if server {
                    iface.rid = iface.lid;
                    iface_def.id = iface.rid;
                } else {
                    iface.rid = iface_def.id;
                }

                for proc_def in iface_def.procs.iter_mut() {
                    let mut proc_found = false;

                    for procedure in iface.procs.iter_mut() {
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
                        return Err(crate::error::CowRpcError::Proto(format!(
                            "Proc name not found - ({})",
                            proc_def.name
                        )));
                    }
                }
            }
        }

        if !iface_found {
            return Err(crate::error::CowRpcError::Proto(format!(
                "IFace name not found - ({})",
                iface_def.name
            )));
        }

        Ok(())
    }
}

// struct CowRpcPeerAsyncMsgProcessorSender {
//     pub writer_sink: Arc<Mutex<CowSink<CowRpcMessage>>>,
//     pub msg_to_send: Option<CowRpcMessage>,
// }

// impl Future for CowRpcPeerAsyncMsgProcessorSender {
//     type Output = Result<()>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let mut writer = self.writer_sink.lock().await;
//         let pin_writer = Pin::new(&mut**writer);
//         if let Some(msg) = self.msg_to_send.take() {
//             pin_writer.start_send(msg)?;
//         }
//         pin_writer.poll_flush(cx)
//     }
// }

#[derive(Clone)]
struct CowRpcPeerAsyncMsgProcessor {
    pub inner: CowRpcPeerSharedInner,
}

impl CowRpcPeerAsyncMsgProcessor {
    pub async fn process_message(&self, msg: CowRpcMessage) -> Result<()> {
        let is_response = msg.is_response();

        match msg {
            CowRpcMessage::Identity(header, msg) => {
                if is_response {
                    self.process_identify_rsp(header, msg).await
                } else {
                    self.process_identify_req(header, msg).await
                }
            }

            CowRpcMessage::Resolve(header, msg) => {
                if is_response {
                    self.process_resolve_rsp(header, msg).await
                } else {
                    self.process_resolve_req(header, msg).await
                }
            }

            CowRpcMessage::Bind(header, msg) => {
                if is_response {
                    self.process_bind_rsp(header, msg).await
                } else {
                    self.process_bind_req(header, msg).await
                }
            }

            CowRpcMessage::Unbind(header, msg) => {
                if is_response {
                    self.process_unbind_rsp(header, msg).await
                } else {
                    self.process_unbind_req(header, msg).await
                }
            }

            CowRpcMessage::Call(header, msg, mut payload) => {
                if is_response {
                    Err(CowRpcError::Proto(
                        "Received unexpected call msg on which the response flag has been set".to_string(),
                    ))
                } else {
                    self.process_call_req(header, msg, &mut payload).await
                }
            }

            CowRpcMessage::Result(header, msg, payload) => {
                if is_response {
                    self.process_result_rsp(header, msg, payload).await
                } else {
                    Err(CowRpcError::Proto(
                        "Received unexpected result msg on which the response flag has not been set".to_string(),
                    ))
                }
            }

            CowRpcMessage::Terminate(header) => {
                if is_response {
                    self.process_terminate_rsp(header).await
                } else {
                    self.process_terminate_req(header).await
                }
            }

            CowRpcMessage::Verify(header, msg, payload) => {
                if is_response {
                    self.process_verify_rsp(header, msg, payload).await
                } else {
                    self.process_verify_req(header, msg, &payload).await
                }
            }

            CowRpcMessage::Http(header, msg, mut payload) => {
                if is_response {
                    self.process_http_rsp(header, msg, payload).await
                } else {
                    self.process_http_req(header, msg, &mut payload).await
                }
            }

            unexpected_msg => Err(CowRpcError::Proto(format!(
                "Received unexpected msg : {:?}",
                unexpected_msg
            ))),
        }
    }

    async fn process_identify_rsp(&self, header: CowRpcHdr, msg: CowRpcIdentityMsg) -> Result<()> {
        let msg_clone = msg.clone();

        let req_opt = self.remove_request(move |req| {
            match *req {
                CowRpcAsyncReq::Identify(ref identify_req) => identify_req.name.eq(&msg_clone.identity),
                _ => false /* Wrong request type, we move to the next one */
            }
        }).await?;

        if let Some(mut req) = req_opt {
            match req {
                CowRpcAsyncReq::Identify(ref mut identify_req) => {
                    if let Err(e) = identify_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncIdentifyRsp {
                            error: CowRpcErrorCode::from(header.flags),
                        }) {
                        return Err(CowRpcError::Internal(format!(
                            "Unable to send response through futures oneshot channel: {:?}",
                            e
                        )));
                    }
                }
                _ => {} /* Wrong request type, we move to the next one */
            }

            Ok(())
        } else {
            Err(CowRpcError::Internal(format!("Unable to find the matching request")))
        }
    }

    async fn process_identify_req(&self, _: CowRpcHdr, msg: CowRpcIdentityMsg) -> Result<()> {
        Err(CowRpcError::Proto(format!(
            "Received unexpected identify request : {}",
            msg.identity
        )))
    }

    async fn process_verify_rsp(&self, header: CowRpcHdr, msg: CowRpcVerifyMsg, payload: Vec<u8>) -> Result<()> {
        let msg_clone = msg.clone();

        let req_opt = self.remove_request(move |req| {
            match *req {
                CowRpcAsyncReq::Verify(ref verify_req) => verify_req.call_id.eq(&msg_clone.call_id),
                _ => false /* Wrong request type, we move to the next one */
            }
        }).await?;

        if let Some(mut req) = req_opt {
            match req {
                CowRpcAsyncReq::Verify(ref mut verify_req) => {
                    if let Err(_e) = verify_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncVerifyRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            payload
                        }) {
                        return Err(CowRpcError::Internal(
                            "Unable to send verify response through futures oneshot channel".to_string()
                        ));
                    }
                }
                _ => {} /* Wrong request type, we move to the next one */
            }

            Ok(())
        } else {
            Err(CowRpcError::Internal(format!("Unable to find the matching request")))
        }
    }

    async fn process_verify_req(&self, _: CowRpcHdr, _msg: CowRpcVerifyMsg, _payload: &[u8]) -> Result<()> {
        Err(CowRpcError::Proto("Received unexpected verify request".to_string()))
    }

    async fn process_http_rsp(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, http_rsp: Vec<u8>) -> Result<()> {
        let msg_clone = msg.clone();

        let req_opt = self.remove_request(move |req| {
            match *req {
                CowRpcAsyncReq::Http(ref http_req) => http_req.call_id.eq(&msg_clone.call_id),
                _ => false /* Wrong request type, we move to the next one */
            }
        }).await?;

        if let Some(mut req) = req_opt {
            match req {
                CowRpcAsyncReq::Http(ref mut http_req) => {
                    if let Err(_e) = http_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncHttpRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            http_rsp
                        }) {
                        return Err(CowRpcError::Internal("Unable to send http response through futures oneshot channel".to_string()));
                    }
                }
                _ => {} /* Wrong request type, we move to the next one */
            }

            Ok(())
        } else {
            Err(CowRpcError::Internal(format!("Unable to find the matching request")))
        }
    }

    async fn process_http_req(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, payload: &mut [u8]) -> Result<()> {
        let res_fut = if let Some(ref cb) = *self.inner.on_http_msg_callback {
            (**cb)(CowRpcCallContext::new(header.src_id), payload)
        } else {
            Box::new(future::ok::<Vec<u8>, ()>(b"HTTP/1.1 501 Not Implemented\r\n\r\n".to_vec())) as CallFuture<Vec<u8>>
        };

        let self_clone = self.clone();

        match res_fut.await {
            Ok(rsp) => {
                self_clone.send_http_rsp(header.src_id, msg.call_id, rsp, header.flags).await
            }
            Err(_) => {
                self_clone.send_http_rsp(header.src_id, msg.call_id, b"HTTP/1.1 500 Internal Server Error\r\n\r\n".to_vec(), header.flags | COW_RPC_FLAG_FAILURE).await
            }
        }
    }

    async fn process_resolve_rsp(&self, header: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
        let msg_clone = msg.clone();

        let resolve_req_opt = self.remove_request(move |req| {
            if let CowRpcAsyncReq::Resolve(ref resolve_req) = req {
                if header.is_reverse() && resolve_req.reverse {
                    if let Some(requested_node_id) = resolve_req.node_id {
                        return requested_node_id == msg_clone.node_id;
                    }
                } else if !header.is_reverse() && !resolve_req.reverse {
                    if let Some(ref req_name) = resolve_req.name {
                        if let Some(ref msg_identity) = msg_clone.identity {
                            return req_name.eq(&msg_identity.identity);
                        }
                    }
                }
            }
            false
        }).await?;

        if let Some(mut matching_req) = resolve_req_opt {
            match matching_req {
                CowRpcAsyncReq::Resolve(ref mut resolve_req) => {
                    if header.is_reverse() {
                        if let Err(e) = resolve_req
                            .tx
                            .take()
                            .expect("Cannot Send twice on request oneshot channel")
                            .send(CowRpcAsyncResolveRsp {
                                node_id: None,
                                name: msg.identity.as_ref().and_then(|cow_id| Some(cow_id.identity.clone())),
                                error: CowRpcErrorCode::from(header.flags),
                            }) {
                            return Err(CowRpcError::Internal(format!(
                                "Unable to send response through futures oneshot channel: {:?}",
                                e
                            )));
                        }
                    } else if let Err(e) = resolve_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncResolveRsp {
                            node_id: Some(msg.node_id),
                            name: None,
                            error: CowRpcErrorCode::from(header.flags),
                        })
                    {
                        return Err(CowRpcError::Internal(format!(
                            "Unable to send response through futures oneshot channel: {:?}",
                            e
                        )));
                    }
                }
                _ => unreachable!(),
            }
        } else {
            return Err(CowRpcError::Internal(format!("Unable to find the matching request")));
        }

        Ok(())
    }

    async fn process_resolve_req(&self, _: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
        Err(CowRpcError::Proto(format!(
            "Received unexpected resolve request : {:?}",
            msg
        )))
    }

    async fn process_bind_rsp(&self, header: CowRpcHdr, msg: CowRpcBindMsg) -> Result<()> {
        let msg_clone = msg.clone();
        let req_opt = self.remove_request(move |req| {
            if let CowRpcAsyncReq::Bind(ref bind_req) = req {
                bind_req.server_id == header.src_id && bind_req.iface_id == msg_clone.ifaces[0].id
            } else {
                false
            }
        }).await?;

        if let Some(mut req) = req_opt {
            if let CowRpcAsyncReq::Bind(ref mut bind_req) = req {
                if bind_req.server_id == header.src_id && bind_req.iface_id == msg.ifaces[0].id {
                    let mut error = CowRpcErrorCode::from(msg.ifaces[0].flags);
                    if error == CowRpcErrorCode::Success {
                        error = CowRpcErrorCode::from(header.flags);
                    }

                    if let Err(e) = bind_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncBindRsp { error })
                    {
                        return Err(CowRpcError::Internal(format!(
                            "Unable to send response through futures oneshot channel: {:?}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_bind_req(&self, header: CowRpcHdr, msg: CowRpcBindMsg) -> Result<()> {
        for msg_iface in msg.ifaces {
            // Clone the iface_def to update the flags
            let mut iface_def = msg_iface.clone();
            let flag_result;

            let self_clone = self.clone();

            if let Some(iface) = self.inner.get_iface(msg_iface.id, false).await {
                {
                    let mut bind_contexts = self.inner.bind_contexts.write().await;
                    let mut bind_context_found = false;
                    for bind_context in &*bind_contexts {
                        if !bind_context.is_server && bind_context.remote_id == header.src_id && bind_context.iface.get_remote_id().await == iface.get_remote_id().await {
                            bind_context_found = true;
                            break;
                        }
                    }

                    if !bind_context_found {
                        // Success : New bind context
                        let new_bind_context = CowRpcAsyncBindContext::new(true, header.src_id, &iface);
                        bind_contexts.push(new_bind_context);
                        flag_result = CowRpcErrorCode::Success;
                    } else {
                        // Already bound
                        iface_def.flags = CowRpcErrorCode::AlreadyBound.into();
                        flag_result = CowRpcErrorCode::AlreadyBound;
                    }
                }

                // TODO : Should we return error ?
                let _ = self_clone.send_bind_rsp(header.src_id, iface_def, flag_result.into()).await;
            } else {
                // Interface doesn't exist
                iface_def.flags = CowRpcErrorCode::IfaceId.into();
                flag_result = CowRpcErrorCode::IfaceId;

                // TODO : Should we return error ?
                let _ = self.send_bind_rsp(header.src_id, iface_def, flag_result.into()).await;
            }
        }

        Ok(())

    }

    async fn process_unbind_rsp(&self, header: CowRpcHdr, msg: CowRpcUnbindMsg) -> Result<()> {
        let msg_clone = msg.clone();
        let req_opt = self.remove_request(move |req| {
            if let CowRpcAsyncReq::Unbind(ref unbind_req) = req {
                let from_client = header.flags & COW_RPC_FLAG_SERVER == 0;
                if unbind_req.from_client != from_client {
                    return false;
                }

                header.src_id == unbind_req.remote_id && msg_clone.ifaces[0].id == unbind_req.iface_id
            } else {
                false
            }
        }).await?;

        if let Some(mut req) = req_opt {
            if let CowRpcAsyncReq::Unbind(ref mut unbind_req) = req {
                if let Err(e) = unbind_req
                    .tx
                    .take()
                    .expect("Cannot Send twice on request oneshot channel")
                    .send(CowRpcAsyncUnbindRsp {
                        error: CowRpcErrorCode::from(header.flags),
                    }) {
                    return Err(CowRpcError::Internal(format!(
                        "Unable to send response through futures oneshot channel: {:?}",
                        e
                    )));
                }
            }
        }

        Ok(())
    }

    async fn process_unbind_req(&self, header: CowRpcHdr, msg: CowRpcUnbindMsg) -> Result<()> {
        let from_client = header.flags & COW_RPC_FLAG_SERVER == 0;

        for msg_iface in msg.ifaces {
            // Clone the iface_def to update the flags
            let mut iface_def = msg_iface.clone();
            let mut flag_result = CowRpcErrorCode::Success;

            // Try to get the iface with the iface_id
            let iface = self.inner.get_iface(msg_iface.id, false).await;

            let self_clone = self.clone();

            if iface.is_some() {

                let bind_context_removed_opt = self.remove_bind_context(from_client, header.src_id, msg_iface.id).await?;
                if let Some(bind_context_removed) = bind_context_removed_opt {
                    flag_result = CowRpcErrorCode::Success;

                    let clone = self_clone.clone();
                    if let Some(ref callback) = *clone.inner.on_unbind_callback {
                        callback(bind_context_removed);
                    }
                } else {
                    iface_def.flags = CowRpcErrorCode::NotBound.into();
                    flag_result = CowRpcErrorCode::NotBound;
                }

                // TODO Should we return error?
                let _ = self_clone.send_unbind_rsp(header.src_id, iface_def, flag_result.into()).await;
            } else {
                iface_def.flags = CowRpcErrorCode::IfaceId.into();
                flag_result = CowRpcErrorCode::IfaceId;

                // TODO : Should we return error ?
                let _ = self.send_unbind_rsp(header.src_id, iface_def, flag_result.into()).await;
            }
        }

        Ok(())
    }

    async fn process_call_req(&self, header: CowRpcHdr, msg: CowRpcCallMsg, mut payload: &mut Vec<u8>) -> Result<()> {
        // Try to get the iface with the iface_id
        let iface = self.inner.get_iface(msg.iface_id, false).await;

        let flag;
        let mut output_param = None;

        match iface {
            Some(iface) => {
                let iface = iface.read().await;
                if let Some(procedure) = iface.get_proc(msg.proc_id, false) {
                    match &iface.server {
                        Some(ref server) => {
                            let self_clone = self.clone();
                            match server.dispatch_call(header.src_id, procedure.lid, &mut payload).await {
                                Ok(call_result) => {
                                    output_param = Some(call_result);
                                    flag = CowRpcErrorCode::Success;
                                }
                                Err(e) => match e {
                                    crate::error::CowRpcError::CowRpcFailure(error_code) => flag = error_code,
                                    _ => flag = CowRpcErrorCode::Internal,
                                }
                            }
                            return self_clone.send_result_rsp(header.src_id, msg, output_param, flag.into()).await;
                        }
                        None => {
                            flag = CowRpcErrorCode::Internal;
                        }
                    }
                } else {
                    flag = CowRpcErrorCode::ProcId
                }
            }
            None => {
                flag = CowRpcErrorCode::IfaceId;
            }
        }

        self.send_result_rsp(header.src_id, msg, output_param, flag.into()).await
    }

    async fn process_result_rsp(&self, header: CowRpcHdr, msg: CowRpcResultMsg, payload: Vec<u8>) -> Result<()> {
        let msg_clone = msg.clone();

        let req_opt = self.remove_request(move |req| {
            if let CowRpcAsyncReq::Call(ref call_req) = req {
                call_req.call_id == msg.call_id
                    && call_req.iface_id == msg_clone.iface_id
                    && call_req.proc_id == msg.proc_id
            } else {
                false
            }
        }).await?;

        if let Some(mut req) = req_opt {
            if let CowRpcAsyncReq::Call(ref mut call_req) = req {
                if let Err(e) = call_req
                    .tx
                    .take()
                    .expect("Cannot Send twice on request oneshot channel")
                    .send(CowRpcAsyncCallRsp {
                        error: CowRpcErrorCode::from(header.flags),
                        msg_pack: payload.to_owned(),
                    }) {
                    return Err(CowRpcError::Internal(format!(
                        "Unable to send response through futures oneshot channel: {:?}",
                        e
                    )));
                }
            }
        }

        Ok(())
    }

    async fn process_terminate_rsp(&self, _: CowRpcHdr) -> Result<()> {
        self.inner.transition_to_state(CowRpcState::TERMINATE).await;
        Ok(())
    }

    async fn process_terminate_req(&self, header: CowRpcHdr) -> Result<()> {
        self.send_terminate_rsp(header.src_id).await
    }

    async fn send_bind_rsp(&self, dst_id: u32, iface_def: CowRpcIfaceDef, flags: u16) -> Result<()> {
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_BIND_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcBindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Bind(header, msg)).await
    }

    async fn send_unbind_rsp(&self, dst_id: u32, iface_def: CowRpcIfaceDef, flags: u16) -> Result<()> {
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.inner.get_id().await,
            dst_id: dst_id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Unbind(header, msg)).await
    }

    async fn send_result_rsp(
        &self,
        dst_id: u32,
        call_msg: CowRpcCallMsg,
        output_param: Option<Box<dyn CowRpcParams>>,
        flags: u16,
    ) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESULT_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcResultMsg {
            call_id: call_msg.call_id,
            iface_id: call_msg.iface_id,
            proc_id: call_msg.proc_id,
        };

        if let Some(call_result) = output_param {
            let result_size = match call_result.get_size() {
                Ok(size) => size,
                Err(e) => return Err(e.into()),
            };
            header.size = header.get_size() + msg.get_size() + result_size;
            header.offset = (header.get_size() + msg.get_size()) as u8;

            let mut payload = Vec::new();
            if let Err(e) = call_result.write_to(&mut payload) {
                return Err(e.into());
            }
            self.send_message(CowRpcMessage::Result(header, msg, payload)).await
        } else {
            header.size = header.get_size() + msg.get_size();
            header.offset = header.size as u8;

            self.send_message(CowRpcMessage::Result(header, msg, Vec::new())).await
        }
    }

    async fn send_http_req(&self, dst_id: u32, call_id: u32, http_msg: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HTTP_MSG_ID,
            flags: COW_RPC_FLAG_FINAL,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcHttpMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + http_msg.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_message(CowRpcMessage::Http(header, msg, http_msg)).await
    }

    async fn send_http_rsp(&self, dst_id: u32, call_id: u32, http_msg: Vec<u8>, flags: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HTTP_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | COW_RPC_FLAG_FINAL | flags,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcHttpMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + http_msg.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_message(CowRpcMessage::Http(header, msg, http_msg)).await
    }

    async fn send_terminate_rsp(&self, dst_id: u32) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_message(CowRpcMessage::Terminate(header)).await
    }

    async fn remove_bind_context(&self, is_server: bool, remote_id: u32, iface_rid: u16) -> Result<Option<Arc<CowRpcAsyncBindContext>>> {
        let mut bind_context_removed = None;
        let mut index = 0;

        let mut bind_contexts= self.inner.bind_contexts.write().await;

        // TODO Can we do better ?
        for bind_context in &*bind_contexts {
            if (bind_context.is_server == is_server) && (bind_context.remote_id == remote_id) && bind_context.iface.get_remote_id().await == iface_rid {
                bind_context_removed = Some(bind_contexts.remove(index));
                break;
            }
            index += 1;
        }
        // let bind_context_removed = bind_contexts
        //     .iter()
        //     .position(|bind_context| {
        //         (bind_context.is_server == is_server)
        //             && (bind_context.remote_id == remote_id)
        //             && bind_context.iface.get_remote_id().await == iface_rid
        //     })
        //     .map(|position| bind_contexts.remove(position));

        Ok(bind_context_removed)
    }

    async fn send_identify_req(&self, name: &str, typ: CowRpcIdentityType) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_IDENTIFY_MSG_ID,
            src_id: self.inner.get_id().await,
            dst_id: self.inner.get_router_id().await,
            ..Default::default()
        };

        let identity = CowRpcIdentityMsg {
            typ: typ,
            flags: 0,
            identity: String::from(name),
        };

        header.size = header.get_size() + identity.get_size();
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Identity(header, identity)).await
    }

    async fn send_verify_req(&self, call_id: u32, payload: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            src_id: self.inner.get_id().await,
            dst_id: self.inner.get_router_id().await,
            ..Default::default()
        };

        let msg = CowRpcVerifyMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + payload.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_message(CowRpcMessage::Verify(header, msg, payload)).await
    }

    async fn send_resolve_req(&self, id: Option<u32>, name: Option<&str>, reverse: bool) -> Result<()> {
        if (reverse && id.is_none()) || (!reverse && name.is_none()) {
            return Err(CowRpcError::Internal("Wrong parameters".to_string()));
        }

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESOLVE_MSG_ID,
            flags: if reverse { proto::COW_RPC_FLAG_REVERSE } else { 0 },
            src_id: self.inner.get_id().await,
            dst_id: self.inner.get_router_id().await,
            ..Default::default()
        };

        let node_id;
        let identity;
        if reverse {
            node_id = id.expect("This check has already been done");
            identity = None;
        } else {
            node_id = 0;
            identity = Some(CowRpcIdentityMsg {
                typ: CowRpcIdentityType::UPN,
                flags: 0,
                identity: String::from(name.expect("This check has already been done")),
            });
        }

        let msg = CowRpcResolveMsg { node_id, identity };

        header.size = header.get_size() + msg.get_size(header.flags);
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Resolve(header, msg)).await
    }

    async fn send_bind_req(&self, server_id: u32, iface: &Arc<RwLock<CowRpcAsyncIface>>) -> Result<()> {
        let iface = iface.read().await;
        let iface_def = build_ifacedef(&(*iface), false, true);
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_BIND_MSG_ID,
            src_id: self.inner.get_id().await,
            dst_id: server_id,
            ..Default::default()
        };

        let msg = CowRpcBindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Bind(header, msg)).await
    }

    async fn send_unbind_req(
        &self,
        remote_id: u32,
        remote_is_server: bool,
        iface: &Arc<RwLock<CowRpcAsyncIface>>,
    ) -> Result<()> {
        let iface = iface.read().await;
        let iface_def = build_ifacedef(&iface, false, false);
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: if !remote_is_server { COW_RPC_FLAG_SERVER } else { 0 },
            src_id: self.inner.get_id().await,
            dst_id: remote_id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_message(CowRpcMessage::Unbind(header, msg)).await
    }

    async fn send_call_req<T: CowRpcParams>(
        &self,
        bind_context: Arc<CowRpcAsyncBindContext>,
        proc_id: u16,
        call_id: u32,
        params: T,
    ) -> Result<()> {
        let iface = &bind_context.iface;
        let iface = iface.read().await;

        if let Some(procedure) = iface.get_proc(proc_id, true) {
            let mut header = CowRpcHdr {
                msg_type: proto::COW_RPC_CALL_MSG_ID,
                src_id: self.inner.get_id().await,
                dst_id: bind_context.remote_id,
                ..Default::default()
            };

            let msg = CowRpcCallMsg {
                call_id,
                iface_id: iface.rid,
                proc_id: procedure.rid,
            };

            let param_size = match params.get_size() {
                Ok(size) => size,
                Err(e) => return Err(e.into()),
            };

            header.size = header.get_size() + msg.get_size() + param_size;
            header.offset = (header.get_size() + msg.get_size()) as u8;

            let mut payload = Vec::new();
            if let Err(e) = params.write_to(&mut payload) {
                return Err(e.into());
            }

            self.send_message(CowRpcMessage::Call(header, msg, payload)).await
        } else {
            Err(CowRpcError::Proto("Proc not found".to_string()))
        }
    }

    async fn send_terminate_req(&self) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            src_id: self.inner.get_id().await,
            dst_id: self.inner.get_router_id().await,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_message(CowRpcMessage::Terminate(header)).await
    }

    async fn send_message(&self, msg: CowRpcMessage) -> Result<()> {
        // TODO CowRpcPeerAsyncMsgProcessorSender could be removed ?
        let mut writer = self.inner.writer_sink.lock().await;
        writer.send(msg).await?;
        Ok(())

        // Box::new(CowRpcPeerAsyncMsgProcessorSender {
        //     writer_sink: self.inner.writer_sink.clone(),
        //     msg_to_send: Some(msg),
        // }).await
    }

    async fn add_request(&self, req: CowRpcAsyncReq) -> Result<()> {
        self.inner.requests.lock().await.push(req);

        Ok(())
    }

    async fn remove_request<P>(&self, predicate: P) -> Result<Option<CowRpcAsyncReq>>
        where
            P: Fn(&CowRpcAsyncReq) -> bool + Send + 'static,
    {
        let mut requests = self.inner.requests.lock().await;

        let request_id = {
            if let Some(request) = requests.iter().find(move |r| predicate(r)) {
                request.get_id()
            } else {
                return Ok(None);
            }
        };

        Ok(requests
            .iter()
            .position(|r| r.get_id() == request_id)
            .map(move |position| requests.remove(position)))
    }
}

// TODO REMOVE
// struct CowRpcAsyncPeerSend(pub Option<CowRpcAsyncPeer>, pub Option<CowRpcMessage>);
//
// impl Future for CowRpcAsyncPeerSend {
//     type Output = Result<CowRpcAsyncPeer>;
//
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let res = {
//             let peer = self
//                 .0
//                 .as_mut()
//                 .take()
//                 .expect("An async peer send was created without an asyncPeer ctx");
//             let mut writer = peer.inner.writer_sink.lock();
//             let pin_writer = Pin::new(&mut **writer);
//             if let Some(msg) = self.1.take() {
//                 pin_writer.start_send(msg)?;
//             }
//             pin_writer.poll_flush(cx)?
//         };
//
//         if let Poll::Ready(_) = res {
//             let peer = self
//                 .0
//                 .take()
//                 .expect("An async peer send was created without an asyncPeer ctx");
//             Poll::Ready(Ok(peer))
//         } else {
//             Poll::Pending
//         }
//     }
// }

struct CowRpcAsyncPeer {
    inner: CowRpcPeerSharedInner,
    reader_stream: CowStreamEx<CowRpcMessage>,
}

impl CowRpcAsyncPeer {
    pub fn new(transport: CowRpcTransport, mode: CowRpcMode, ifaces: Vec<Arc<RwLock<CowRpcAsyncIface>>>, on_unbind: Option<Box<UnbindCallback>>, on_http: Option<Box<HttpMsgCallback>>) -> Self {
        let mut transport = transport;
        let (reader_stream, writer_sink) = transport.message_stream_sink();

        CowRpcAsyncPeer {
            inner: CowRpcPeerSharedInner {
                id: Arc::new(RwLock::new(0)),
                router_id: Arc::new(RwLock::new(0)),
                mode,
                ifaces: Arc::new(RwLock::new(ifaces)),
                bind_contexts: Arc::new(RwLock::new(Vec::new())),
                state: Arc::new(RwLock::new(CowRpcState::INITIAL)),
                requests: Arc::new(Mutex::new(Vec::new())),
                writer_sink: Arc::new(Mutex::new(writer_sink)),
                on_unbind_callback: Arc::new(on_unbind),
                on_http_msg_callback: Arc::new(on_http),
            },
            reader_stream,
        }
    }

    fn message_processor(&self) -> CowRpcPeerAsyncMsgProcessor {
        CowRpcPeerAsyncMsgProcessor {
            inner: self.inner.clone(),
        }
    }

    async fn send_msg(self, msg: CowRpcMessage) -> Result<CowRpcAsyncPeer> {
        {
            let mut writer = self.inner.writer_sink.lock().await;
            writer.send(msg).await?;
        }

        Ok(self)
    }

    async fn handshake(self) -> Result<Self> {
        self.inner.transition_to_state(CowRpcState::HANDSHAKE).await;
        client_handshake(self).await
    }

    async fn process_handshake_rsp(&self, header: CowRpcHdr, _: CowRpcHandshakeMsg) -> Result<()> {
        if header.is_failure() {
            return Err(crate::error::CowRpcError::CowRpcFailure(CowRpcErrorCode::from(header.flags)));
        }

        if self.inner.get_state().await != CowRpcState::HANDSHAKE {
            return Err(crate::error::CowRpcError::Internal(format!(
                "Handshake response received and state machine has wrong state ({:?})",
                self.inner.get_state().await
            )));
        }

        self.inner.set_id(header.dst_id).await;
        self.inner.set_router_id(header.src_id).await;
        Ok(())
    }
}

impl Stream for CowRpcAsyncPeer {
    type Item = Result<CowRpcMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = ready!(futures::StreamExt::next(&mut self.reader_stream).poll_unpin(cx));
        match result {
            Some(Err(CowRpcError::Transport(transport::TransportError::ConnectionReset))) => {
                // We want to check if the error is a disconnection
                return Poll::Ready(None);
            }
            other => return Poll::Ready(other),
        }
    }
}

async fn client_handshake(mut async_peer: CowRpcAsyncPeer) -> Result<CowRpcAsyncPeer> {
    let mut header = CowRpcHdr {
        msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
        flags: if async_peer.inner.mode == CowRpcMode::DIRECT {
            proto::COW_RPC_FLAG_DIRECT
        } else {
            0
        },
        src_id: 0,
        dst_id: 0,
        ..Default::default()
    };

    let msg = CowRpcHandshakeMsg::default();

    header.size = header.get_size() + msg.get_size();
    header.offset = header.size as u8;

    let mut peer = async_peer.send_msg(CowRpcMessage::Handshake(header, msg)).await?;

    let msg: CowRpcMessage = match futures::StreamExt::next(&mut peer.reader_stream).await {
        Some(msg_result) => msg_result?,
        None => return Err(CowRpcError::Proto(
            "Connection was reset before handshake response".to_string(),
        )),
    };

    match msg {
        CowRpcMessage::Handshake(header, msg) => {
            if header.is_response() {
                peer.process_handshake_rsp(header, msg).await?;
            } else {
                return Err(CowRpcError::Proto(
                    "Expected Handshake Response, Handshake request".to_string(),
                ));
            }
        }
        msg => {
            return Err(CowRpcError::Proto(format!(
                "Expected Handshake Response, got {:?}",
                msg
            )));
        }
    }

    Ok(peer)
}

pub struct CowRpcPeer {
    url: String,
    connection_timeout: Option<Duration>,
    is_server: bool,
    mode: CowRpcMode,
    ifaces: Vec<Arc<RwLock<CowRpcAsyncIface>>>,
    #[allow(dead_code)]
    monitor: PeerMonitor,
    peer_handle_inner: HandleMsgProcessor,
    #[allow(dead_code)]
    thread_handle: HandleThreadHandle,
    on_unbind_callback: Option<Box<UnbindCallback>>,
    on_http_msg_callback: Option<Box<HttpMsgCallback>>,
}

impl CowRpcPeer {
    pub fn new_client(
        url: &str,
        connection_timeout: Option<Duration>,
        mode: CowRpcMode,
    ) -> (CowRpcPeer, CowRpcPeerHandle) {
        let (handle, monitor) = channel();
        let peer_handle_inner = Arc::new(RwLock::new(None));
        let thread_handle = Arc::new(Mutex::new(None));

        let peer_handle = PeerHandle { sender: handle };

        (
            CowRpcPeer {
                url: url.to_string(),
                connection_timeout,
                is_server: false,
                mode,
                ifaces: Vec::new(),
                monitor,
                peer_handle_inner: peer_handle_inner.clone(),
                thread_handle: thread_handle.clone(),
                on_unbind_callback: None,
                on_http_msg_callback: None
            },
            CowRpcPeerHandle {
                monitor: Arc::new(Mutex::new(Some(peer_handle))),
                peer: peer_handle_inner,
                thread_handle,
            },
        )
    }

    pub fn new_server(listener_url: &str) -> (CowRpcPeer, CowRpcPeerHandle) {
        let (handle, monitor) = channel();
        let peer_handle_inner = Arc::new(RwLock::new(None));
        let thread_handle = Arc::new(Mutex::new(None));

        let peer_handle = PeerHandle { sender: handle };

        (
            CowRpcPeer {
                url: listener_url.to_string(),
                connection_timeout: None,
                is_server: true,
                mode: CowRpcMode::DIRECT,
                ifaces: Vec::new(),
                monitor,
                peer_handle_inner: peer_handle_inner.clone(),
                thread_handle: thread_handle.clone(),
                on_unbind_callback: None,
                on_http_msg_callback: None
            },
            CowRpcPeerHandle {
                monitor: Arc::new(Mutex::new(Some(peer_handle))),
                peer: peer_handle_inner,
                thread_handle,
            },
        )
    }

    pub async fn spawn_server(self) -> Result<()> {
        unimplemented!()
    }

    pub async fn spawn_client(self) -> Result<()> {
        use std::str::FromStr;

        let CowRpcPeer {
            url,
            connection_timeout,
            is_server: _,
            mode,
            ifaces,
            monitor: _,
            peer_handle_inner,
            thread_handle: _,
            on_unbind_callback,
            on_http_msg_callback,
        } = self;

        let connection_timeout = connection_timeout.unwrap_or_else(|| Duration::from_secs(30));

        let uri = match Uri::from_str(&url).map_err(|e| CowRpcError::Internal(e.to_string())) {
            Ok(u) => u,
            Err(e) => return Err(e.into()),
        };

        let mut peer = match tokio::time::timeout(connection_timeout, CowRpcTransport::connect(uri)).await {
            Ok(Ok(transport)) => {
                let mut peer = CowRpcAsyncPeer::new(transport, mode, ifaces, on_unbind_callback, on_http_msg_callback);
                peer.handshake().await?
            }
            Ok(Err(e)) => {
                return Err(e);
            }
            Err(e) => {
                return Err(CowRpcError::Proto(format!("Connection attempt timed out")));
            }
        };

        let msg_processor = peer.message_processor();

        *peer_handle_inner.write().await = Some(msg_processor.clone());

        tokio::spawn(async move {
                loop {
                    match futures::StreamExt::next(&mut peer).await {
                        Some(Ok(msg)) => {
                            let msg_processor_clone = msg_processor.clone();
                            tokio::spawn(async move {
                                match msg_processor_clone.process_message(msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("Msg processor got an error : {:?}", e);
                                    }
                                }
                                future::ready(())
                            });
                        }
                        Some(Err(e)) => {
                            error!("Peer msg stream failed with error : {:?}", e);
                            return future::ready(());
                        }
                        None => {
                            // clean disconnection
                            return future::ready(())
                        }
                    }
                }
            }
        );

        Ok(())
    }

    pub async fn spawn(self) -> Result<()> {
        if self.is_server {
            self.spawn_server().await
        } else {
            self.spawn_client().await
        }
    }

    // TODO REMOVE
    // pub fn register_iface(&mut self, iface_reg: CowRpcIfaceReg, server: Option<Box<dyn AsyncServer>>) -> Result<u16> {
    //     let iface_id = self.ifaces.len() as u16;
    //
    //     let mut iface = CowRpcAsyncIface {
    //         name: String::from(iface_reg.name),
    //         lid: iface_id,
    //         rid: 0,
    //         procs: Vec::new(),
    //         server,
    //     };
    //
    //     for procedure in iface_reg.procs {
    //         iface.procs.push(CowRpcProc {
    //             lid: procedure.id,
    //             rid: 0,
    //             name: String::from(procedure.name),
    //         })
    //     }
    //
    //     self.ifaces.push(Arc::new(RwLock::new(iface)));
    //     Ok(iface_id)
    // }
    //
    // pub async fn set_iface_server(&mut self, iface_id: u16, server: Option<Box<dyn AsyncServer>>) {
    //     for iface_mutex in self.ifaces.iter() {
    //         let mut iface = iface_mutex.write().await;
    //         if iface.lid == iface_id {
    //             iface.set_server(server);
    //             break;
    //         }
    //     }
    // }

    pub fn on_unbind_callback<F: 'static + Send + Sync + Fn(Arc<CowRpcAsyncBindContext>)>(&mut self, callback: F) {
        self.on_unbind_callback = Some(Box::new(callback));
    }

    pub fn on_http_msg_callback<F: 'static + Send + Sync +  Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>>>(&mut self, callback: F) {
        self.on_http_msg_callback = Some(Box::new(callback));
    }
}

#[derive(Clone)]
pub struct CowRpcPeerHandle {
    monitor: HandleMonitor,
    peer: HandleMsgProcessor,
    thread_handle: HandleThreadHandle,
}

impl CowRpcPeerHandle {
    async fn inner(&self) -> Option<CowRpcPeerAsyncMsgProcessor> {
        self.peer.read().await.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.monitor
            .lock()
            .await
            .take()
            .expect("A CowRpcPeerHandle cannot exist without a monitor, can't exit twice")
            .exit();

        if let Some(handle) = self.thread_handle.lock().await.take() {
            return handle
                .join()
                .map_err(|_| CowRpcError::Internal("Unable to stop CowRpcPeer thread".to_string()))?;
        }

        Ok(())
    }

    pub async fn exit(&self) -> Result<()> {
        let inner = self.inner().await;

        let self_clone = self.clone();

        if let Some(inner) = inner {
            inner.send_terminate_req().await?;
            self_clone
                .monitor
                .lock()
                .await
                .take()
                .expect("A CowRpcPeerHandle cannot exist without a monitor, can't exit twice")
                .exit();

            let mut t_handle = self_clone.thread_handle.lock().await;

            if let Some(handle) = t_handle.take() {
                if let Err(e) = handle
                    .join()
                    .map_err(|_| CowRpcError::Internal("Unable to stop CowRpcPeer thread".to_string()))
                    {
                        Err(e.into())
                    } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        } else {
            self.monitor
                .lock()
                .await
                .take()
                .expect("A CowRpcPeerHandle cannot exist without a monitor, can't exit twice")
                .exit();

            if let Some(handle) = self.thread_handle.lock().await.take() {
                if let Err(e) = handle
                    .join()
                    .map_err(|_| CowRpcError::Internal("Unable to stop CowRpcPeer thread".to_string()))
                    {
                        Err(e.into())
                    } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        }
    }

    pub async fn identify_async(&self, name: &str, identity_type: CowRpcIdentityType, timeout: Duration) -> Result<()> {
        if let Some(inner) = self.inner().await {
            let (tx, rx) = channel();
            let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            let req = CowRpcAsyncReq::Identify(CowRpcAsyncIdentifyReq {
                id,
                name: name.to_string(),
                tx: Some(tx),
            });

            let name = name.to_string();

            let inner_clone = inner.clone();

            inner.add_request(req).await?;
            inner_clone.send_identify_req(&name, identity_type).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    match res.get_result() {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::identify_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn verify_async(&self, payload: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        if let Some(inner) = self.inner().await {
            let (tx, rx) = channel();
            let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            let req = CowRpcAsyncReq::Verify(CowRpcAsyncVerifyReq {
                id,
                call_id: id as u32,
                tx: Some(tx),
            });

            let inner_clone = inner.clone();

            inner.add_request(req).await?;
            inner_clone.send_verify_req(id as u32, payload).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    Ok(res.payload)
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::identify_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn call_http_async_v2(&self, remote_id: u32, http_req: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        if let Some(inner) = self.inner().await {
            let (tx, rx) = channel();
            let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            let req = CowRpcAsyncReq::Http(CowRpcAsyncHttpReq {
                id,
                call_id: id as u32,
                tx: Some(tx),
            });

            let inner_clone = inner.clone();

            inner.add_request(req).await?;
            inner_clone.send_http_req(remote_id, id as u32, http_req).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    if res._error == CowRpcErrorCode::Success {
                        Ok(res.http_rsp)
                    } else {
                        Err(CowRpcError::CowRpcFailure(res._error))
                    }
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::identify_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn call_http_async(&self, bind_context: Arc<CowRpcAsyncBindContext>, http_req: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        self.call_http_async_v2(bind_context.remote_id, http_req, timeout).await
    }

    pub async fn resolve_async(&self, name: &str, timeout: Duration) -> Result<u32> {
        if let Some(inner) = self.inner().await {
            let (tx, rx) = channel();
            let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            let req = CowRpcAsyncReq::Resolve(CowRpcAsyncResolveReq {
                id,
                node_id: None,
                name: Some(name.to_string()),
                reverse: false,
                tx: Some(tx),
            });

            let name = name.to_string();

            let inner_clone = inner.clone();

            inner.add_request(req).await?;
            inner_clone.send_resolve_req(None, Some(&name), false).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    match res.get_result() {
                        Ok(r) => Ok(r),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::resolve_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn resolve_reverse_async(&self, node_id: u32, timeout: Duration) -> Result<String> {
        if let Some(inner) = self.inner().await {
            let (tx, rx) = channel();
            let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            let req = CowRpcAsyncReq::Resolve(CowRpcAsyncResolveReq {
                id,
                node_id: Some(node_id),
                name: None,
                reverse: true,
                tx: Some(tx),
            });

            let inner_clone = inner.clone();

            inner.add_request(req).await?;
            inner_clone.send_resolve_req(Some(node_id), None, true).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    match res.get_reverse_result() {
                        Ok(r) => Ok(r),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::resolve_reverse_async before CowRpcPeer::run will have no effect"
                    .to_string(),
            ))
        }
    }

    pub async fn bind_async(
        &self,
        server_id: u32,
        iface_id: u16,
        timeout: Duration,
    ) -> Result<Arc<CowRpcAsyncBindContext>> {
        if let Some(processor) = self.inner().await {
            if let Some(iface) = processor.inner.get_iface(iface_id, true).await {
                let mut bc_opt = None;
                for bc in &*processor.inner.bind_contexts.read().await {
                    if bc.remote_id == server_id && bc.iface.read().await.lid == iface_id {
                        bc_opt = Some(bc.clone());
                    }
                }

                if let Some(bc) = bc_opt {
                    trace!("bind context already existing locally");
                    Ok(bc)
                } else {
                    let (tx, rx) = channel();

                    let req;
                    let id;
                    {
                        let iface = iface.read().await;
                        id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
                        req = CowRpcAsyncReq::Bind(CowRpcAsyncBindReq {
                            id,
                            tx: Some(tx),
                            server_id,
                            iface_id: iface.rid,
                        });
                    }

                    let processor_clone = processor.clone();

                    processor.add_request(req).await?;
                    processor_clone.send_bind_req(server_id, &iface).await?;
                    let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
                    match result {
                        Ok(res) => {
                            let bind_context = if res.is_success() {
                                CowRpcAsyncBindContext::new(false, server_id, &iface)
                            } else {
                                match res.get_error() {
                                    CowRpcErrorCode::AlreadyBound => {
                                        trace!("bind context already existing remotely, creating one locally");
                                        CowRpcAsyncBindContext::new(false, server_id, &iface)
                                    }
                                    _ => {
                                        return Err(CowRpcError::CowRpcFailure(res.get_error()));
                                    }
                                }
                            };

                            {
                                processor.inner.bind_contexts.write().await.push(bind_context.clone());
                            }

                            Ok(bind_context)
                        }
                        Err(e) => {
                            Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                        }
                    }
                }
            } else {
                Err(CowRpcError::Proto("unregistered interface".to_string()))
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::bind_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn unbind_async(&self, bind_context: Arc<CowRpcAsyncBindContext>, timeout: Duration) -> Result<()> {
        if let Some(processor) = self.inner().await {
            let (tx, rx) = channel();
            let req;
            let id;
            {
                let iface = bind_context.iface.read().await;
                id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
                req = CowRpcAsyncReq::Unbind(CowRpcAsyncUnbindReq {
                    id,
                    tx: Some(tx),
                    from_client: true,
                    remote_id: bind_context.remote_id,
                    iface_id: iface.rid,
                });
            }

            let processor_clone = processor.clone();

            processor.add_request(req).await?;
            processor_clone.send_unbind_req(bind_context.remote_id, true, &bind_context.iface).await?;
            let result = tokio::time::timeout(timeout, rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    let processor_clone = processor.clone();
                    if res.is_success() {
                            processor_clone
                                .remove_bind_context(
                                    bind_context.is_server,
                                    bind_context.remote_id,
                                    bind_context.get_iface_remote_id().await,
                                ).await?;
                        Ok(())
                    } else {
                        Err(CowRpcError::CowRpcFailure(res.get_error()))
                    }
                }
                Err(e) => {
                    Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
                }
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::unbind_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn call_async<P: 'static + CowRpcParams + Send + Sync, T: CowRpcParams + Send + 'static>(
        &self,
        bind_context: Arc<CowRpcAsyncBindContext>,
        proc_id: u16,
        params: P,
    ) -> Result<T> {
        //TODO
        todo!()
        // if let Some(processor) = self.inner().await {
        //     if let Some(proc_remote_id) = bind_context.get_proc_remote_id(proc_id).await {
        //         let (tx, rx) = channel();
        //
        //         let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        //         let req = CowRpcAsyncReq::Call(CowRpcAsyncCallReq {
        //             id,
        //             call_id: id as u32,
        //             iface_id: bind_context.get_iface_remote_id(),
        //             proc_id: proc_remote_id,
        //             tx: Some(tx),
        //         });
        //
        //         let processor_clone = processor.clone();
        //
        //         processor.add_request(req).await?;
        //         processor_clone.send_call_req(bind_context.clone(), proc_id, id as u32, params).await?;
        //         let result = tokio::time::timeout(Duration::from_secs(10), rx).await.map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
        //         match result {
        //             Ok(res) => {
        //                 let processor_clone = processor.clone();
        //                 if res.is_success() {
        //                     let mut msg_pack = &res.msg_pack[..];
        //                     match T::read_from(&mut msg_pack) {
        //                         Ok(output_param) => Ok(output_param),
        //                         Err(e) => Err(e),
        //                     });
        //                     fut
        //                 } else {
        //                     let (remote_id, iface_rid) = {
        //                         (
        //                             bind_context.get_iface_remote_id() as u32,
        //                             bind_context.iface.read().rid.clone(),
        //                         )
        //                     };
        //                     let fut: CowFuture<T> = Box::new(match res.get_error() {
        //                         CowRpcErrorCode::NotBound => {
        //                             trace!("bind context existing locally but not remotely, deleting it");
        //                             let fut: CowFuture<T> = Box::new(
        //                                 processor_clone
        //                                     .remove_bind_context(false, remote_id, iface_rid)
        //                                     .and_then(move |bind_context_removed| {
        //                                         if bind_context_removed.is_none() {
        //                                             warn!(
        //                                                 "Unable to remove bind context remote {}  iface {}",
        //                                                 remote_id, iface_rid
        //                                             );
        //                                         }
        //
        //                                         err(CowRpcError::CowRpcFailure(res.get_error()))
        //                                     }),
        //                             );
        //                             fut
        //                         }
        //                         _ => {
        //                             Err(CowRpcError::CowRpcFailure(res.get_error()))
        //                         }
        //                     });
        //                     fut
        //                 }
        //             }
        //             Err(e) => {
        //                 Err(CowRpcError::Internal(format!("The receiver has been cancelled, {:?}", e)))
        //             }
        //         }
        //     } else {
        //         Err(CowRpcError::Internal(format!(
        //             "Remote proc_id can't be found for local proc_id={}",
        //             proc_id
        //         )))
        //     }
        // } else {
        //     Err(CowRpcError::Internal(
        //         "Calling CowRpcPeerHandle::call_async before CowRpcPeer::run will have no effect".to_string(),
        //     ))
        // }
    }
}

async fn build_ifacedef_list(
    ifaces: &Vec<Arc<RwLock<CowRpcAsyncIface>>>,
    with_names: bool,
    with_procs: bool,
) -> Vec<CowRpcIfaceDef> {
    let mut iface_def_list = Vec::new();

    for iface in ifaces {
        let iface = iface.write().await;
        let iface_def = build_ifacedef(&(*iface), with_names, with_procs);
        iface_def_list.push(iface_def);
    }

    return iface_def_list;
}

fn build_ifacedef(iface: &CowRpcAsyncIface, with_names: bool, with_procs: bool) -> CowRpcIfaceDef {
    let mut iface_def = CowRpcIfaceDef::default();
    iface_def.id = iface.rid;

    if with_names {
        iface_def.flags |= COW_RPC_DEF_FLAG_NAMED;
        iface_def.name = iface.name.clone();
    }

    if with_procs {
        iface_def.procs = build_proc_def_list(&iface.procs, with_names);
    } else {
        iface_def.flags |= COW_RPC_DEF_FLAG_EMPTY;
    }

    return iface_def;
}

fn build_proc_def_list(procs: &Vec<CowRpcProc>, with_name: bool) -> Vec<CowRpcProcDef> {
    let mut proc_def_list = Vec::new();

    for procedure in procs {
        let proc_def = build_proc_def(procedure, with_name);
        proc_def_list.push(proc_def);
    }

    return proc_def_list;
}

fn build_proc_def(procedure: &CowRpcProc, with_name: bool) -> CowRpcProcDef {
    let mut proc_def = CowRpcProcDef::default();
    proc_def.id = procedure.lid;

    if with_name {
        proc_def.flags |= COW_RPC_DEF_FLAG_NAMED;
        proc_def.name = procedure.name.clone();
    }

    return proc_def;
}
