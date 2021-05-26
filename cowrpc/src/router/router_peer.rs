use super::{CowRpcIdentityType, CowRpcMessage};
use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto;
use crate::proto::*;
use crate::transport::{CowRpcTransport, CowSink, Transport};
use futures::prelude::*;
use futures::stream::StreamExt;
use mouscache::CacheFunc;
use parking_lot::RwLock as SyncRwLock;
use std::collections::HashMap;

use std::sync::Arc;

use crate::router::{RouterShared, ALLOCATED_COW_ID_SET};
use crate::transport::CowStream;
use std::ops::Deref;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use {mouscache, rand, std};

pub(crate) struct CowRpcRouterPeer {
    inner: Arc<CowRpcRouterPeerSharedInner>,
    identity: Arc<SyncRwLock<Option<CowRpcIdentity>>>,
    reader_stream: CowStream<CowRpcMessage>,
    router: RouterShared,
}

impl CowRpcRouterPeer {
    pub fn get_cow_id(&self) -> u32 {
        self.inner.cow_id
    }

    pub async fn run(
        mut self,
    ) -> std::result::Result<(u32, Option<CowRpcIdentity>), (u32, Option<CowRpcIdentity>, CowRpcError)> {
        loop {
            match self.reader_stream.next().await {
                Some(Ok(msg)) => {
                    if let Err(e) = self.process_msg(msg).await {
                        debug!("Msg failed to be processed: {}", e);
                    }
                }
                Some(Err(e)) => {
                    return Err((self.inner.cow_id, self.identity.read().clone(), e));
                }
                None => {
                    return Ok((self.inner.cow_id, self.identity.read().clone()));
                }
            }

            match &*self.inner.state.read().await {
                CowRpcRouterPeerState::Error => {
                    return Err((
                        self.inner.cow_id,
                        self.identity.read().clone(),
                        CowRpcError::Internal("An error occured while polling the peer connection".to_string()),
                    ));
                }
                CowRpcRouterPeerState::Terminated => {
                    return Ok((self.inner.cow_id, self.identity.read().clone()));
                }
                _ => {}
            }
        }
    }

    pub(crate) async fn handshake(
        transport: CowRpcTransport,
        router: RouterShared,
    ) -> Result<(CowRpcRouterPeer, CowRpcRouterPeerSender)> {
        let transport = transport;
        let remote_addr = transport.remote_addr();
        let (mut reader_stream, writer_sink) = transport.message_stream_sink();
        let (peer, peer_sender) = match reader_stream.next().await {
            Some(msg) => match msg? {
                CowRpcMessage::Handshake(hdr, msg) => {
                    if !hdr.is_response() {
                        let flag: u16 = CowRpcErrorCode::Success.into();

                        if hdr.flags & COW_RPC_FLAG_DIRECT != 0 {
                            return Err(CowRpcError::Internal("Direct mode is not implemented.".to_string()));
                        } else {
                            trace!("Client connected from {:?}", remote_addr);

                            let (mut peer, peer_sender) = {
                                let router = router.clone();
                                let peer_id = generate_peer_id(&router);
                                let inner = Arc::new(CowRpcRouterPeerSharedInner {
                                    cow_id: peer_id,
                                    writer_sink: Mutex::new(writer_sink),
                                    state: RwLock::new(CowRpcRouterPeerState::Connected),
                                });

                                (
                                    CowRpcRouterPeer {
                                        inner: inner.clone(),
                                        identity: Arc::new(SyncRwLock::new(None)),
                                        reader_stream,
                                        router,
                                    },
                                    CowRpcRouterPeerSender { inner },
                                )
                            };

                            peer.send_handshake_rsp(flag).await?;

                            (peer, peer_sender)
                        }
                    } else {
                        return Err(CowRpcError::Proto(format!(
                            "Router can't process a response: hdr={:?} - msg={:?}",
                            hdr, msg
                        )));
                    }
                }
                _ => {
                    return Err(CowRpcError::Proto(
                        "First message was not a handshake message, shutting down the connection".to_string(),
                    ))
                }
            },
            None => return Err(CowRpcError::Proto("Connection was closed before handshake".to_string())),
        };

        Ok((peer, peer_sender))
    }

    async fn process_msg(&mut self, msg: CowRpcMessage) -> Result<()> {
        match msg {
            CowRpcMessage::Handshake(hdr, msg) => {
                error!(
                    "CowRpc Protocol Error: Handshake should have been processed at the beginning: hdr={:?} - msg={:?}",
                    hdr, msg
                );
            }
            CowRpcMessage::Register(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_register_req(hdr, msg).await?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Identity(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_identify_req(hdr, msg).await?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Resolve(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_resolve_req(hdr, msg).await?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Terminate(hdr) => {
                if !hdr.is_response() {
                    self.process_terminate_req(hdr).await?;
                } else {
                    self.process_terminate_rsp(hdr).await?;
                }
            }

            CowRpcMessage::Verify(hdr, msg, payload) => {
                if !hdr.is_response() {
                    self.process_verify_req(hdr, msg, &payload).await?;
                } else {
                    error!("CowRpc Protocol Error: Router can't process a response: hdr={:?}", hdr);
                }
            }

            msg => {
                self.router.process_msg(msg).await;
            }
        }

        Ok(())
    }

    async fn process_register_req(&mut self, _: CowRpcHdr, _msg: CowRpcRegisterMsg) -> Result<()> {
        // Register is not supported, verify has to be used.
        let flag = CowRpcErrorCode::NotImplemented;
        self.send_register_rsp(flag.into(), Vec::new()).await?;
        Ok(())
    }

    async fn process_identify_req(&mut self, _: CowRpcHdr, msg: CowRpcIdentityMsg) -> Result<()> {
        // Identify is not supported, verify has to be used.
        let flag = CowRpcErrorCode::Unauthorized;
        self.send_identify_rsp(flag.into(), msg).await?;
        Ok(())
    }

    async fn process_verify_req(&mut self, _: CowRpcHdr, msg: CowRpcVerifyMsg, payload: &[u8]) -> Result<()> {
        let (rsp, identity_opt) = if let Some(ref cb) = *self.router.inner.verify_identity_cb.read().await {
            (**cb)(self.inner.cow_id, payload).await
        } else {
            (b"HTTP/1.1 501 NOT IMPLEMENTED\r\n\r\n".to_vec(), None)
        };

        let mut flag = CowRpcErrorCode::Success;
        {
            if let Some(mut identity) = identity_opt {
                // den is a special case. Only one peer should be identified with den. Nobody should be able
                // to request the den identity (except the den itself of course) since a pop-token has been validated.
                if identity.eq("den") {
                    identity = format!("den{}", self.router.inner.id);
                }

                let identity = CowRpcIdentity {
                    typ: CowRpcIdentityType::UPN,
                    name: identity.clone(),
                };

                let cache = &self.router.inner.cache;
                let cow_id = self.inner.cow_id;
                match cache.add_cow_identity(&identity, cow_id) {
                    Ok(_) => {
                        *self.identity.write() = Some(identity);
                    }
                    Err(e) => {
                        warn!(
                            "Unable to add Identity {} the the router cache : {:?}",
                            identity.name, e
                        );
                        flag = CowRpcErrorCode::Unavailable;
                    }
                }
            } else {
                flag = CowRpcErrorCode::Unauthorized;
            }
        }

        self.send_verify_rsp(flag.into(), msg, rsp).await?;
        Ok(())
    }

    async fn process_resolve_req(&mut self, header: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
        let mut flag: u16 = CowRpcErrorCode::NotFound.into();
        let mut msg_clone = msg.clone();

        {
            let cache = &self.router.inner.cache;
            if header.is_reverse() {
                match cache.get_cow_identity(msg.node_id) {
                    Ok(opt) => {
                        if let Some(identity) = opt {
                            let iden = CowRpcIdentity {
                                typ: CowRpcIdentityType::UPN,
                                name: identity,
                            };
                            msg_clone.identity = Some(CowRpcIdentityMsg::from(iden));
                            flag = CowRpcErrorCode::Success.into();
                        } else {
                            flag = CowRpcErrorCode::NotFound.into();
                        }
                    }
                    Err(e) => {
                        error!("Cache returned an error: {:?}", e);
                        flag = CowRpcErrorCode::NotFound.into();
                    }
                }

                flag |= COW_RPC_FLAG_REVERSE;
            } else {
                if let Some(identity_to_resolve) = msg.identity {
                    let mut identity = identity_to_resolve.identity;

                    {
                        // FIXME: This is a temporary fix until group identity are implemented, as discussed with fdubois
                        if identity.eq("den") {
                            identity = format!("den{}", self.router.inner.id);
                        }
                        // FIXME: End
                    }

                    match cache.get_cow_identity_peer_addr(&CowRpcIdentity {
                        typ: CowRpcIdentityType::NONE,
                        name: identity.clone(),
                    }) {
                        Ok(Some(node_id)) => {
                            msg_clone.node_id = node_id;
                            flag = CowRpcErrorCode::Success.into();
                        }
                        Err(e) => {
                            error!("Cache returned an error: {:?}", e);
                            flag = CowRpcErrorCode::NotFound.into();
                        }
                        _ => {
                            flag = CowRpcErrorCode::NotFound.into();
                        }
                    }
                }
            }
        }

        self.send_resolve_rsp(flag, msg_clone).await?;
        Ok(())
    }

    async fn process_terminate_req(&mut self, _: CowRpcHdr) -> Result<()> {
        *self.inner.state.write().await = CowRpcRouterPeerState::Terminated;
        self.send_terminate_rsp().await?;

        // Close the sink, it will send the close message on websocket
        self.inner.writer_sink.lock().await.close().await?;
        Ok(())
    }

    async fn process_terminate_rsp(&mut self, _: CowRpcHdr) -> Result<()> {
        *self.inner.state.write().await = CowRpcRouterPeerState::Terminated;

        // Close the sink, it will send the close message on websocket
        self.inner.writer_sink.lock().await.close().await?;
        Ok(())
    }

    async fn send_handshake_rsp(&mut self, flag: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        let msg = CowRpcHandshakeMsg::default();

        header.size = header.get_size() + msg.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Handshake(header, msg)).await?;
        Ok(())
    }

    async fn send_register_rsp(&mut self, flag: u16, ifaces: Vec<CowRpcIfaceDef>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_REGISTER_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        let msg = CowRpcRegisterMsg { ifaces };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Register(header, msg)).await?;
        Ok(())
    }

    async fn send_identify_rsp(&mut self, flag: u16, msg: CowRpcIdentityMsg) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_IDENTIFY_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Identity(header, msg)).await?;
        Ok(())
    }

    async fn send_verify_rsp(&mut self, flag: u16, msg: CowRpcVerifyMsg, payload: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size() + payload.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_messages(CowRpcMessage::Verify(header, msg, payload)).await?;
        Ok(())
    }

    async fn send_resolve_rsp(&mut self, flag: u16, msg: CowRpcResolveMsg) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESOLVE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size(header.flags);
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Resolve(header, msg)).await?;
        Ok(())
    }

    async fn send_terminate_rsp(&mut self) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Terminate(header)).await?;
        Ok(())
    }

    async fn send_messages(&mut self, msg: CowRpcMessage) -> Result<()> {
        self.inner.writer_sink.lock().await.send(msg).await
    }
}

pub struct CowRpcRouterPeerSharedInner {
    cow_id: u32,
    state: RwLock<CowRpcRouterPeerState>,
    writer_sink: Mutex<CowSink<CowRpcMessage>>,
}

pub(crate) struct CowRpcRouterPeerSender {
    inner: Arc<CowRpcRouterPeerSharedInner>,
}

impl CowRpcRouterPeerSender {
    pub(crate) fn new(cow_id: u32, state: CowRpcRouterPeerState, writer_sink: CowSink<CowRpcMessage>) -> Self {
        CowRpcRouterPeerSender {
            inner: Arc::new(CowRpcRouterPeerSharedInner {
                cow_id,
                state: RwLock::new(state),
                writer_sink: Mutex::new(writer_sink),
            }),
        }
    }

    pub(crate) async fn set_connection_error(&self) {
        *self.inner.state.write().await = CowRpcRouterPeerState::Error;
    }

    pub(crate) async fn send_messages(&self, msg: CowRpcMessage) -> Result<()> {
        self.inner.writer_sink.lock().await.send(msg).await
    }
}

pub(crate) struct CowRpcRouterPeerSenderGuard<'a> {
    guard: RwLockReadGuard<'a, HashMap<u32, CowRpcRouterPeerSender>>,
    key: u32,
}

impl<'a> CowRpcRouterPeerSenderGuard<'a> {
    pub fn new(guard: RwLockReadGuard<'a, HashMap<u32, CowRpcRouterPeerSender>>, key: u32) -> Option<Self> {
        if guard.get(&key).is_some() {
            Some(CowRpcRouterPeerSenderGuard { guard, key })
        } else {
            None
        }
    }
}

impl<'a> Deref for CowRpcRouterPeerSenderGuard<'a> {
    type Target = CowRpcRouterPeerSender;

    fn deref(&self) -> &Self::Target {
        self.guard
            .get(&self.key)
            .expect("Sender should exist, we validated at guard creation")
    }
}

pub(crate) enum CowRpcRouterPeerState {
    Connected,
    Terminated,
    Error,
}

fn generate_peer_id(router: &RouterShared) -> u32 {
    loop {
        let id = rand::random::<u16>();

        // 0 is not accepted as peer_id
        if id != 0 {
            let peer_id = router.inner.id | u32::from(id);
            if let Ok(false) = router
                .inner
                .cache
                .get_raw_cache()
                .set_ismember(ALLOCATED_COW_ID_SET, peer_id)
            {
                if router
                    .inner
                    .cache
                    .get_raw_cache()
                    .set_add(ALLOCATED_COW_ID_SET, &[peer_id])
                    .is_ok()
                {
                    break peer_id;
                }
            }
        }
    }
}
