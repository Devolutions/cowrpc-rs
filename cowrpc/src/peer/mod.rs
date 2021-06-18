use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto::{Message, *};
use crate::transport::{CowRpcTransport, CowRpcTransportError, CowSink, CowStream, Transport};
use crate::{proto, CowRpcCallContext, CowRpcIdentityType};
use futures::channel::oneshot::{channel, Sender as AsyncSender};
use futures::future::BoxFuture;
use futures::prelude::*;
use slog::{error, o, Drain, Logger};
use std::str::FromStr;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use url::Url;

mod handshake;

const DEFAULT_CONNECTION_TIMEOUT: u64 = 30;

pub type CallFuture<'a, T> = BoxFuture<'a, std::result::Result<T, ()>>;
type HttpMsgCallback = dyn Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>> + Send + Sync;

static COWRPC_REQ_ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
enum CowRpcState {
    Active,
    Terminate,
}

pub struct CowRpcPeerConfig {
    url: String,
    connection_timeout: Duration,
    on_http_msg_callback: Option<Box<HttpMsgCallback>>,
    logger: Option<Logger>,
}

impl CowRpcPeerConfig {
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Self {
        self.connection_timeout = connection_timeout;
        self
    }

    pub fn on_http_msg_callback<F: 'static + Send + Sync + Fn(CowRpcCallContext, &mut [u8]) -> CallFuture<Vec<u8>>>(
        mut self,
        callback: F,
    ) -> Self {
        self.on_http_msg_callback = Some(Box::new(callback));
        self
    }

    pub fn logger(mut self, logger: Logger) -> Self {
        self.logger = Some(logger);
        self
    }
}

#[derive(Clone)]
pub struct CowRpcPeer {
    config: Arc<CowRpcPeerConfig>,
    id: u32,
    router_id: u32,
    state: Arc<RwLock<CowRpcState>>,
    pending_requests: Arc<Mutex<Vec<CowRpcAsyncReq>>>,
    writer: Arc<RwLock<CowRpcPeerWriter>>,
    msg_task_handle: Arc<Mutex<Option<JoinHandle<Result<()>>>>>,
    logger: Logger,
}

pub(crate) struct CowRpcPeerInner {
    peer: CowRpcPeer,
    reader: CowStream<CowRpcMessage>,
}

struct CowRpcPeerWriter {
    writer_sink: CowSink<CowRpcMessage>,
}

impl CowRpcPeer {
    pub fn config(url: &str) -> CowRpcPeerConfig {
        CowRpcPeerConfig {
            url: url.to_string(),
            connection_timeout: Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT),
            on_http_msg_callback: None,
            logger: None,
        }
    }

    pub async fn connect(config: CowRpcPeerConfig) -> Result<Self> {
        let url =
            Url::from_str(&config.url).map_err(|e| CowRpcTransportError::InvalidUrl(format!("Invalid URL: {}", e)))?;

        let logger = config
            .logger
            .clone()
            .unwrap_or_else(|| slog::Logger::root(slog_stdlog::StdLog.fuse(), o!("type" => "peer")));

        let peer_inner = match tokio::time::timeout(
            config.connection_timeout,
            CowRpcTransport::connect(url, logger.clone()),
        )
        .await
        {
            Ok(connect_result) => {
                let transport = connect_result?;
                let (reader_stream, writer_sink) = transport.message_stream_sink();
                let peer = handshake::handshake(config, reader_stream, writer_sink, logger).await?;
                peer
            }
            Err(_) => {
                return Err(CowRpcError::Timeout);
            }
        };

        let peer = peer_inner.peer.clone();

        *peer.msg_task_handle.lock().await = Some(tokio::spawn(process_msg_task(peer_inner.peer, peer_inner.reader)));

        Ok(peer)
    }

    pub async fn disconnect(self) -> Result<()> {
        self.send_terminate_req().await?;

        if let Some(handle) = &mut *self.msg_task_handle.lock().await {
            let _ = handle.await;
        }
        Ok(())
    }

    pub async fn verify(&self, payload: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcAsyncReq::Verify(CowRpcAsyncVerifyReq {
            id,
            call_id: id as u32,
            tx: Some(tx),
        });

        self.add_request(req).await?;
        self.send_verify_req(id as u32, payload).await?;
        let result = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| CowRpcError::Timeout)?;

        match result {
            Ok(res) => Ok(res.payload),
            Err(_) => Err(CowRpcError::Cancel),
        }
    }

    pub async fn call_http(&self, remote_id: u32, http_req: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcAsyncReq::Http(CowRpcAsyncHttpReq {
            id,
            call_id: id as u32,
            tx: Some(tx),
        });

        self.add_request(req).await?;
        self.send_http_req(remote_id, id as u32, http_req).await?;
        let result = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| CowRpcError::Timeout)?;
        match result {
            Ok(res) => {
                if res._error == CowRpcErrorCode::Success {
                    Ok(res.http_rsp)
                } else {
                    Err(CowRpcError::CowRpcFailure(res._error))
                }
            }
            Err(_) => Err(CowRpcError::Cancel),
        }
    }

    pub async fn resolve(&self, name: &str, timeout: Duration) -> Result<u32> {
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

        self.add_request(req).await?;
        self.send_resolve_req(None, Some(&name), false).await?;
        let result = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| CowRpcError::Timeout)?;
        match result {
            Ok(res) => match res.get_result() {
                Ok(r) => Ok(r),
                Err(e) => Err(e),
            },
            Err(_) => Err(CowRpcError::Cancel),
        }
    }

    pub async fn resolve_reverse(&self, node_id: u32, timeout: Duration) -> Result<String> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcAsyncReq::Resolve(CowRpcAsyncResolveReq {
            id,
            node_id: Some(node_id),
            name: None,
            reverse: true,
            tx: Some(tx),
        });

        self.add_request(req).await?;
        self.send_resolve_req(Some(node_id), None, true).await?;
        let result = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| CowRpcError::Timeout)?;
        match result {
            Ok(res) => match res.get_reverse_result() {
                Ok(r) => Ok(r),
                Err(e) => Err(e),
            },
            Err(_) => Err(CowRpcError::Cancel),
        }
    }

    async fn send_verify_req(&self, call_id: u32, payload: Vec<u8>) -> Result<()> {
        let src_id = self.id;
        let dst_id = self.router_id;

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            src_id,
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcVerifyMsg { call_id };

        header.size = header.get_size() + msg.get_size() + payload.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_message(CowRpcMessage::Verify(header, msg, payload)).await
    }

    async fn send_message(&self, msg: CowRpcMessage) -> Result<()> {
        let mut writer = self.writer.write().await;
        writer.writer_sink.send(msg).await?;
        Ok(())
    }

    async fn process_message(self, msg: CowRpcMessage) -> Result<()> {
        let is_response = msg.is_response();

        match msg {
            CowRpcMessage::Resolve(header, msg) => {
                if is_response {
                    self.process_resolve_rsp(header, msg).await
                } else {
                    Err(CowRpcError::Proto("Received unexpected resolve request".to_string()))
                }
            }

            CowRpcMessage::Verify(header, msg, payload) => {
                if is_response {
                    self.process_verify_rsp(header, msg, payload).await
                } else {
                    Err(CowRpcError::Proto("Received unexpected verify request".to_string()))
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
            if let CowRpcAsyncReq::Verify(ref mut verify_req) = req {
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

            Ok(())
        } else {
            Err(CowRpcError::Internal("Unable to find the matching request".to_string()))
        }
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
            if let CowRpcAsyncReq::Http(ref mut http_req) = req {
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

            Ok(())
        } else {
            Err(CowRpcError::Internal("Unable to find the matching request".to_string()))
        }
    }

    async fn process_http_req(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, payload: &mut [u8]) -> Result<()> {
        let res_fut = if let Some(ref cb) = self.config.on_http_msg_callback {
            (**cb)(CowRpcCallContext::new(header.src_id), payload)
        } else {
            Box::pin(future::ok::<Vec<u8>, ()>(
                b"HTTP/1.1 501 Not Implemented\r\n\r\n".to_vec(),
            ))
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
                                name: msg.identity.as_ref().map(|identity_msg| identity_msg.identity.clone()),
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
            return Err(CowRpcError::Internal("Unable to find the matching request".to_string()));
        }

        Ok(())
    }

    async fn process_terminate_rsp(&self, _: CowRpcHdr) -> Result<()> {
        self.transition_to_state(CowRpcState::Terminate).await;
        Ok(())
    }

    async fn transition_to_state(&self, new_state: CowRpcState) {
        *self.state.write().await = new_state;
    }

    async fn process_terminate_req(&self, header: CowRpcHdr) -> Result<()> {
        self.send_terminate_rsp(header.src_id).await?;

        // Close the sink, it will send the close message on websocket
        self.writer.write().await.writer_sink.close().await?;
        Ok(())
    }

    async fn send_failure_result_rsp(&self, dst_id: u32, call_msg: CowRpcCallMsg, flags: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESULT_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.id,
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
            src_id: self.id,
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
            src_id: self.id,
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
            src_id: self.id,
            dst_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_message(CowRpcMessage::Terminate(header)).await
    }

    async fn send_resolve_req(&self, id: Option<u32>, name: Option<&str>, reverse: bool) -> Result<()> {
        if (reverse && id.is_none()) || (!reverse && name.is_none()) {
            return Err(CowRpcError::Internal("Wrong parameters".to_string()));
        }

        let src_id = self.id;
        let dst_id = self.router_id;

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESOLVE_MSG_ID,
            flags: if reverse { proto::COW_RPC_FLAG_REVERSE } else { 0 },
            src_id,
            dst_id,
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
                typ: CowRpcIdentityType::Upn,
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
        let src_id = self.id;
        let dst_id = self.router_id;

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            src_id,
            dst_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_message(CowRpcMessage::Terminate(header)).await
    }

    async fn add_request(&self, req: CowRpcAsyncReq) -> Result<()> {
        self.pending_requests.lock().await.push(req);

        Ok(())
    }

    async fn remove_request<P>(&self, predicate: P) -> Result<Option<CowRpcAsyncReq>>
    where
        P: Fn(&CowRpcAsyncReq) -> bool + Send + 'static,
    {
        let mut requests = self.pending_requests.lock().await;

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

async fn process_msg_task(peer: CowRpcPeer, mut stream: CowStream<CowRpcMessage>) -> Result<()> {
    loop {
        let logger_clone = peer.logger.clone();

        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(msg)) => {
                let peer_clone = peer.clone();
                tokio::spawn(async move {
                    if let Err(e) = peer_clone.process_message(msg).await {
                        error!(logger_clone, "Error during peer message processing: {:?}", e);
                    }
                });
            }
            Some(Err(e)) => {
                error!(logger_clone, "Peer msg stream failed with error : {:?}", e);
                return Err(e);
            }
            None => {
                // clean disconnection
                return Ok(());
            }
        }
    }
}

struct CowRpcAsyncResolveReq {
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

struct CowRpcAsyncVerifyReq {
    id: usize,
    call_id: u32,
    tx: Option<AsyncSender<CowRpcAsyncVerifyRsp>>,
}

struct CowRpcAsyncVerifyRsp {
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

struct CowRpcAsyncHttpReq {
    id: usize,
    call_id: u32,
    tx: Option<AsyncSender<CowRpcAsyncHttpRsp>>,
}

struct CowRpcAsyncHttpRsp {
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

enum CowRpcAsyncReq {
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
