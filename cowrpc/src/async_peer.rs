use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto::{Message, *};
use crate::transport::r#async::{CowRpcTransport, CowSink, CowStreamEx, Transport};
use crate::transport::Uri;
use crate::{
    proto, transport, CowRpcAsyncHttpReq, CowRpcAsyncHttpRsp, CowRpcAsyncReq, CowRpcAsyncResolveReq,
    CowRpcAsyncResolveRsp, CowRpcAsyncVerifyReq, CowRpcAsyncVerifyRsp, CowRpcCallContext, CowRpcIdentityType,
    CowRpcState,
};
use futures::channel::oneshot::channel;
use futures::prelude::*;
use futures::ready;

use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

pub type CallFuture<T> = Box<dyn Future<Output = std::result::Result<T, ()>> + Unpin + Send>;
type HttpMsgCallback = dyn Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>> + Send + Sync;

static COWRPC_REQ_ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

pub struct CowRpcPeer {
    url: String,
    connection_timeout: Option<Duration>,
    msg_processor: Option<CowRpcPeerAsyncMsgProcessor>,
    on_http_msg_callback: Option<Box<HttpMsgCallback>>,
    msg_processing_task: Option<JoinHandle<Result<()>>>,
}

impl CowRpcPeer {
    pub fn new(url: &str, connection_timeout: Option<Duration>) -> Self {
        CowRpcPeer {
            url: url.to_string(),
            connection_timeout,
            msg_processor: None,
            on_http_msg_callback: None,
            msg_processing_task: None,
        }
    }

    pub fn on_http_msg_callback<F: 'static + Send + Sync + Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>>>(
        &mut self,
        callback: F,
    ) {
        self.on_http_msg_callback = Some(Box::new(callback));
    }

    pub async fn start(&mut self) -> Result<()> {
        let connection_timeout = self.connection_timeout.unwrap_or_else(|| Duration::from_secs(30));

        let uri = Uri::from_str(&self.url).map_err(|e| CowRpcError::Internal(e.to_string()))?;

        let mut peer = match tokio::time::timeout(connection_timeout, CowRpcTransport::connect(uri)).await {
            Ok(connect_result) => {
                let transport = connect_result?;
                let mut peer = CowRpcAsyncPeer::new(transport, self.on_http_msg_callback.take());
                peer.handshake().await?;
                peer
            }
            Err(_) => {
                return Err(CowRpcError::Proto(format!("Connection attempt timed out")));
            }
        };

        let msg_processor = peer.message_processor();
        self.msg_processor = Some(msg_processor.clone());

        let msg_processing_task = tokio::spawn(async move {
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
                        return Err(e);
                    }
                    None => {
                        // clean disconnection
                        return Ok(());
                    }
                }
            }
        });

        self.msg_processing_task = Some(msg_processing_task);

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(inner) = &self.msg_processor {
            inner.send_terminate_req().await?;
        }

        if let Some(handle) = self.msg_processing_task.take() {
            if let Ok(res) = handle.await {
                res?;
            };
        }

        Ok(())
    }

    pub async fn verify_async(&self, payload: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        if let Some(inner) = &self.msg_processor {
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
            let result = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => Ok(res.payload),
                Err(e) => Err(CowRpcError::Internal(format!(
                    "The receiver has been cancelled, {:?}",
                    e
                ))),
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::identify_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn call_http_async_v2(&self, remote_id: u32, http_req: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        if let Some(inner) = &self.msg_processor {
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
            let result = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => {
                    if res._error == CowRpcErrorCode::Success {
                        Ok(res.http_rsp)
                    } else {
                        Err(CowRpcError::CowRpcFailure(res._error))
                    }
                }
                Err(e) => Err(CowRpcError::Internal(format!(
                    "The receiver has been cancelled, {:?}",
                    e
                ))),
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::identify_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn resolve_async(&self, name: &str, timeout: Duration) -> Result<u32> {
        if let Some(inner) = &self.msg_processor {
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
            let result = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => match res.get_result() {
                    Ok(r) => Ok(r),
                    Err(e) => Err(e),
                },
                Err(e) => Err(CowRpcError::Internal(format!(
                    "The receiver has been cancelled, {:?}",
                    e
                ))),
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::resolve_async before CowRpcPeer::run will have no effect".to_string(),
            ))
        }
    }

    pub async fn resolve_reverse_async(&self, node_id: u32, timeout: Duration) -> Result<String> {
        if let Some(inner) = &self.msg_processor {
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
            let result = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| CowRpcError::Internal("timed out".to_string()))?;
            match result {
                Ok(res) => match res.get_reverse_result() {
                    Ok(r) => Ok(r),
                    Err(e) => Err(e),
                },
                Err(e) => Err(CowRpcError::Internal(format!(
                    "The receiver has been cancelled, {:?}",
                    e
                ))),
            }
        } else {
            Err(CowRpcError::Internal(
                "Calling CowRpcPeerHandle::resolve_reverse_async before CowRpcPeer::run will have no effect"
                    .to_string(),
            ))
        }
    }
}

#[derive(Clone)]
struct CowRpcPeerSharedInner {
    id: Arc<RwLock<u32>>,
    router_id: Arc<RwLock<u32>>,
    state: Arc<RwLock<CowRpcState>>,
    requests: Arc<Mutex<Vec<CowRpcAsyncReq>>>,
    writer_sink: Arc<Mutex<CowSink<CowRpcMessage>>>,
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
}

#[derive(Clone)]
struct CowRpcPeerAsyncMsgProcessor {
    pub inner: CowRpcPeerSharedInner,
}

impl CowRpcPeerAsyncMsgProcessor {
    async fn process_message(&self, msg: CowRpcMessage) -> Result<()> {
        let is_response = msg.is_response();

        match msg {
            CowRpcMessage::Resolve(header, msg) => {
                if is_response {
                    self.process_resolve_rsp(header, msg).await
                } else {
                    Err(CowRpcError::Proto(format!(
                        "Received unexpected resolve request : {:?}",
                        msg
                    )))
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

            CowRpcMessage::Terminate(header) => {
                if is_response {
                    self.process_terminate_rsp(header).await
                } else {
                    self.process_terminate_req(header).await
                }
            }

            mut msg if !is_response && matches!(msg, CowRpcMessage::Bind(..) | CowRpcMessage::Unbind(..)) => {
                msg.swap_src_dst();
                msg.add_flag(CowRpcErrorCode::NotImplemented.into());
                self.send_message(msg).await
            }

            CowRpcMessage::Call(header, msg, _payload) if !is_response => {
                // Call are deprecated, we send back an error
                self.send_failure_result_rsp(header.src_id, msg, CowRpcErrorCode::NotImplemented.into())
                    .await
            }

            unexpected_msg => Err(CowRpcError::Proto(format!(
                "Received unexpected msg : {:?}",
                unexpected_msg
            ))),
        }
    }

    async fn process_verify_rsp(&self, header: CowRpcHdr, msg: CowRpcVerifyMsg, payload: Vec<u8>) -> Result<()> {
        let msg_clone = msg.clone();

        let req_opt = self
            .remove_request(move |req| {
                match *req {
                    CowRpcAsyncReq::Verify(ref verify_req) => verify_req.call_id.eq(&msg_clone.call_id),
                    _ => false, /* Wrong request type, we move to the next one */
                }
            })
            .await?;

        if let Some(mut req) = req_opt {
            match req {
                CowRpcAsyncReq::Verify(ref mut verify_req) => {
                    if let Err(_e) = verify_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncVerifyRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            payload,
                        })
                    {
                        return Err(CowRpcError::Internal(
                            "Unable to send verify response through futures oneshot channel".to_string(),
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

        let req_opt = self
            .remove_request(move |req| {
                match *req {
                    CowRpcAsyncReq::Http(ref http_req) => http_req.call_id.eq(&msg_clone.call_id),
                    _ => false, /* Wrong request type, we move to the next one */
                }
            })
            .await?;

        if let Some(mut req) = req_opt {
            match req {
                CowRpcAsyncReq::Http(ref mut http_req) => {
                    if let Err(_e) = http_req
                        .tx
                        .take()
                        .expect("Cannot Send twice on request oneshot channel")
                        .send(CowRpcAsyncHttpRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            http_rsp,
                        })
                    {
                        return Err(CowRpcError::Internal(
                            "Unable to send http response through futures oneshot channel".to_string(),
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

    async fn process_http_req(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, payload: &mut [u8]) -> Result<()> {
        let res_fut = if let Some(ref cb) = *self.inner.on_http_msg_callback {
            (**cb)(CowRpcCallContext::new(header.src_id), payload)
        } else {
            Box::new(future::ok::<Vec<u8>, ()>(
                b"HTTP/1.1 501 Not Implemented\r\n\r\n".to_vec(),
            )) as CallFuture<Vec<u8>>
        };

        let self_clone = self.clone();

        match res_fut.await {
            Ok(rsp) => {
                self_clone
                    .send_http_rsp(header.src_id, msg.call_id, rsp, header.flags)
                    .await
            }
            Err(_) => {
                self_clone
                    .send_http_rsp(
                        header.src_id,
                        msg.call_id,
                        b"HTTP/1.1 500 Internal Server Error\r\n\r\n".to_vec(),
                        header.flags | COW_RPC_FLAG_FAILURE,
                    )
                    .await
            }
        }
    }

    async fn process_resolve_rsp(&self, header: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
        let msg_clone = msg.clone();

        let resolve_req_opt = self
            .remove_request(move |req| {
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
            })
            .await?;

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
                            })
                        {
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

    async fn process_terminate_rsp(&self, _: CowRpcHdr) -> Result<()> {
        self.inner.transition_to_state(CowRpcState::TERMINATE).await;
        Ok(())
    }

    async fn process_terminate_req(&self, header: CowRpcHdr) -> Result<()> {
        self.send_terminate_rsp(header.src_id).await
    }

    async fn send_failure_result_rsp(&self, dst_id: u32, call_msg: CowRpcCallMsg, flags: u16) -> Result<()> {
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

        header.size = header.get_size() + msg.get_size();
        header.offset = header.size as u8;

        self.send_message(CowRpcMessage::Result(header, msg, Vec::new())).await
    }

    async fn send_http_req(&self, dst_id: u32, call_id: u32, http_msg: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HTTP_MSG_ID,
            flags: COW_RPC_FLAG_FINAL,
            src_id: self.inner.get_id().await,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcHttpMsg { call_id };

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

        let msg = CowRpcHttpMsg { call_id };

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

    async fn send_verify_req(&self, call_id: u32, payload: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            src_id: self.inner.get_id().await,
            dst_id: self.inner.get_router_id().await,
            ..Default::default()
        };

        let msg = CowRpcVerifyMsg { call_id };

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
        let mut writer = self.inner.writer_sink.lock().await;
        writer.send(msg).await?;
        Ok(())
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

struct CowRpcAsyncPeer {
    inner: CowRpcPeerSharedInner,
    reader_stream: CowStreamEx<CowRpcMessage>,
}

impl CowRpcAsyncPeer {
    pub fn new(transport: CowRpcTransport, on_http: Option<Box<HttpMsgCallback>>) -> Self {
        let transport = transport;
        let (reader_stream, writer_sink) = transport.message_stream_sink();

        CowRpcAsyncPeer {
            inner: CowRpcPeerSharedInner {
                id: Arc::new(RwLock::new(0)),
                router_id: Arc::new(RwLock::new(0)),
                state: Arc::new(RwLock::new(CowRpcState::INITIAL)),
                requests: Arc::new(Mutex::new(Vec::new())),
                writer_sink: Arc::new(Mutex::new(writer_sink)),
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

    async fn send_msg(&mut self, msg: CowRpcMessage) -> Result<()> {
        {
            let mut writer = self.inner.writer_sink.lock().await;
            writer.send(msg).await?;
        }

        Ok(())
    }

    async fn handshake(&mut self) -> Result<()> {
        self.inner.transition_to_state(CowRpcState::HANDSHAKE).await;

        // Send handshake

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
            flags: 0,
            src_id: 0,
            dst_id: 0,
            ..Default::default()
        };

        let msg = CowRpcHandshakeMsg::default();

        header.size = header.get_size() + msg.get_size();
        header.offset = header.size as u8;

        self.send_msg(CowRpcMessage::Handshake(header, msg)).await?;

        // Receive handshake response

        let msg: CowRpcMessage = match futures::StreamExt::next(&mut self.reader_stream).await {
            Some(msg_result) => msg_result?,
            None => {
                return Err(CowRpcError::Proto(
                    "Connection was reset before handshake response".to_string(),
                ))
            }
        };

        match msg {
            CowRpcMessage::Handshake(header, msg) => {
                if header.is_response() {
                    self.process_handshake_rsp(header, msg).await?;
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

        Ok(())
    }

    async fn process_handshake_rsp(&self, header: CowRpcHdr, _: CowRpcHandshakeMsg) -> Result<()> {
        if header.is_failure() {
            return Err(crate::error::CowRpcError::CowRpcFailure(CowRpcErrorCode::from(
                header.flags,
            )));
        }

        if self.inner.get_state().await != CowRpcState::HANDSHAKE {
            return Err(crate::error::CowRpcError::Internal(format!(
                "Handshake response received and state machine has wrong state ({:?})",
                self.inner.get_state().await
            )));
        }

        self.inner.set_id(header.dst_id).await;
        self.inner.set_router_id(header.src_id).await;

        self.inner.transition_to_state(CowRpcState::ACTIVE).await;

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
