use super::{CowRpcIdentityType, CowRpcMessage};
use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto::*;
use crate::transport::r#async::adaptor::Adaptor;
use crate::transport::r#async::{CowRpcTransport, CowSink, ListenerBuilder, Transport};
use crate::transport::MessageInterceptor;
use crate::{proto, CowRpcMessageInterceptor};
use futures::channel::oneshot::{channel, Receiver, Sender};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::StreamExt;

use mouscache::{Cache, CacheFunc};
use parking_lot::RwLock as SyncRwLock;
use std::collections::HashMap;

use std::sync::Arc;

use crate::transport::r#async::CowStream;
use crate::transport::TlsOptions;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use {mouscache, rand, std};

pub type RouterMonitor = Receiver<()>;

pub struct RouterHandle {
    sender: Sender<()>,
}

impl RouterHandle {
    fn new(s: Sender<()>) -> RouterHandle {
        RouterHandle { sender: s }
    }

    pub fn exit(self) {
        let _ = self.sender.send(());
    }
}

const PEER_CONNECTION_GRACE_PERIOD: u64 = 10;

const ALLOCATED_COW_ID_SET: &str = "allocated_cow_id";
const COW_ID_RECORDS: &str = "cow_address_records";
const IDENTITY_RECORDS: &str = "identities_records";

type IdentityVerificationCallback = dyn Fn(u32, &[u8]) -> BoxFuture<'_, (Vec<u8>, Option<String>)> + Send + Sync;
type PeerConnectionCallback = dyn Fn(u32) -> () + Send + Sync;
type PeerDisconnectionCallback = dyn Fn(u32, Option<CowRpcIdentity>) -> BoxFuture<'static, ()> + Send + Sync;
pub type PeersAreAliveCallback = dyn Fn(&[u32]) -> BoxFuture<'_, ()> + Send + Sync;

pub struct CowRpcRouter {
    listener_url: String,
    tls_options: Option<TlsOptions>,
    monitor: RouterMonitor,
    shared: RouterShared,
    adaptor: Adaptor,
    msg_interceptor: Option<Box<dyn MessageInterceptor>>,
    peers_are_alive_task_info: Option<PeersAreAliveTaskInfo>,
    keep_alive_interval: Option<Duration>,
}

struct PeersAreAliveTaskInfo {
    callback: Box<PeersAreAliveCallback>,
    interval: Duration,
}

impl CowRpcRouter {
    pub async fn new(url: &str, tls_options: Option<TlsOptions>) -> Result<(CowRpcRouter, RouterHandle)> {
        let id: u32 = 0;
        let (handle, router_monitor) = channel();
        let router = CowRpcRouter {
            listener_url: url.to_string(),
            tls_options,
            monitor: router_monitor,
            shared: RouterShared::new(id).await,
            adaptor: Adaptor::new(),
            msg_interceptor: None,
            peers_are_alive_task_info: None,
            keep_alive_interval: None,
        };

        let router_handle = RouterHandle::new(handle);

        Ok((router, router_handle))
    }

    pub async fn new2(
        id: u16,
        cache: Cache,
        url: &str,
        tls_options: Option<TlsOptions>,
    ) -> Result<(CowRpcRouter, RouterHandle)> {
        let router_id = u32::from(id) << 16;
        let (handle, router_monitor) = channel();
        let router = CowRpcRouter {
            listener_url: url.to_string(),
            tls_options,
            monitor: router_monitor,
            shared: RouterShared::new2(router_id, cache).await,
            adaptor: Adaptor::new(),
            msg_interceptor: None,
            peers_are_alive_task_info: None,
            keep_alive_interval: None,
        };

        let router_handle = RouterHandle::new(handle);

        Ok((router, router_handle))
    }

    pub async fn on_peer_connection_callback<F: 'static + Fn(u32) + Send + Sync>(&mut self, callback: F) {
        let mut cb = self.shared.inner.on_peer_connection_callback.write().await;
        *cb = Some(Box::new(callback));
    }

    pub async fn on_peer_disconnection_callback<
        F: 'static + Fn(u32, Option<CowRpcIdentity>) -> BoxFuture<'static, ()> + Send + Sync,
    >(
        &mut self,
        callback: F,
    ) {
        let mut cb = self.shared.inner.on_peer_disconnection_callback.write().await;
        *cb = Some(Box::new(callback));
    }

    pub async fn verify_identity_callback<
        F: 'static + Fn(u32, &[u8]) -> BoxFuture<'_, (Vec<u8>, Option<String>)> + Send + Sync,
    >(
        &mut self,
        callback: F,
    ) {
        let mut cb = self.shared.inner.verify_identity_cb.write().await;
        *cb = Some(Box::new(callback));
    }

    pub fn set_keep_alive_interval(&mut self, interval: Duration) {
        self.keep_alive_interval = Some(interval);
    }

    pub async fn set_msg_interceptor<T: 'static + Send + Sync + Clone>(
        &mut self,
        interceptor: CowRpcMessageInterceptor<T>,
    ) {
        let (_, sink) = CowRpcTransport::from_interceptor(interceptor.clone_boxed()).message_stream_sink();

        let peer = CowRpcRouterPeerSender {
            inner: Arc::new(CowRpcRouterPeerSharedInner {
                cow_id: 0,
                state: RwLock::new(CowRpcRouterPeerState::Connected),
                writer_sink: Mutex::new(sink),
            }),
        };
        *self.shared.inner.multi_router_peer.write().await = Some(peer);
        self.msg_interceptor = Some(Box::new(interceptor));
    }

    pub fn set_peers_are_alive_callback<F: 'static + Fn(&[u32]) -> BoxFuture<'_, ()> + Send + Sync>(
        &mut self,
        interval: Duration,
        callback: F,
    ) {
        self.peers_are_alive_task_info = Some(PeersAreAliveTaskInfo {
            callback: Box::new(callback),
            interval,
        });
    }

    pub fn get_msg_injector(&self) -> Adaptor {
        self.adaptor.clone()
    }

    pub fn get_id(&self) -> u32 {
        self.shared.inner.id
    }

    pub async fn run(self) -> Result<RouterMonitor> {
        let CowRpcRouter {
            listener_url,
            tls_options,
            monitor,
            shared,
            adaptor,
            msg_interceptor,
            peers_are_alive_task_info,
            keep_alive_interval,
        } = self;

        let mut listener_builder = ListenerBuilder::from_uri(&listener_url)?;

        if let Some(interceptor) = msg_interceptor {
            listener_builder = listener_builder.msg_interceptor(interceptor);
        }

        if let Some(tls) = tls_options {
            listener_builder = listener_builder.with_ssl(tls);
        }

        let listener = listener_builder.build().await?;

        let router_shared_clone = shared.clone();
        tokio::spawn(adaptor.message_stream().for_each(move |msg| {
            let mut router = router_shared_clone.clone();
            async move {
                if let Ok(msg) = msg {
                    router.process_msg(msg).await;
                }
            }
        }));

        if let Some(task_info) = peers_are_alive_task_info {
            tokio::spawn(peers_are_alive_task(shared.inner.peer_senders.clone(), task_info));
        }

        let router_shared_clone = shared.clone();
        let incoming = listener.incoming().await;
        incoming
            .for_each(move |transport| {
                let router = router_shared_clone.clone();
                tokio::spawn(async move {
                    match transport {
                        Ok(transport) => {
                            if let Ok(mut transport) = transport.await {
                                if let Some(keep_alive_interval) = keep_alive_interval.clone() {
                                    transport.set_keep_alive_interval(keep_alive_interval);
                                }

                                if let Err(e) = handle_connection(transport, router).await {
                                    error!("Peer finished with error: {:?}", e);
                                }
                            };
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                });
                future::ready(())
            })
            .await;

        Ok(monitor)
    }
}

async fn handle_connection(transport: CowRpcTransport, router: RouterShared) -> Result<()> {
    let (peer, peer_sender) = tokio::time::timeout(
        std::time::Duration::from_secs(PEER_CONNECTION_GRACE_PERIOD),
        CowRpcRouterPeer::handshake(transport, router.clone()),
    )
    .await
    .map_err(|_| CowRpcError::Internal("timed out".to_string()))??;

    router
        .clone()
        .inner
        .peer_senders
        .write()
        .await
        .insert(peer.inner.cow_id, peer_sender);

    if let Some(ref callback) = &*router.inner.on_peer_connection_callback.read().await {
        callback(peer.inner.cow_id);
    }

    let (peer_id, identity) = peer.run().await.map_err(|(_, _, error)| error)?;
    router.clone().clean_up_connection(peer_id, identity).await;

    Ok(())
}

async fn peers_are_alive_task(
    peers: Arc<RwLock<HashMap<u32, CowRpcRouterPeerSender>>>,
    task_info: PeersAreAliveTaskInfo,
) {
    let interval = tokio::time::interval(task_info.interval);
    interval
        .for_each(|_| async {
            let peers: Vec<u32> = peers.read().await.keys().map(|x| x.clone()).collect();
            (task_info.callback)(&peers).await;
        })
        .await;
}

struct Inner {
    id: u32,
    cache: RouterCache,
    peer_senders: Arc<RwLock<HashMap<u32, CowRpcRouterPeerSender>>>,
    multi_router_peer: RwLock<Option<CowRpcRouterPeerSender>>,
    verify_identity_cb: RwLock<Option<Box<IdentityVerificationCallback>>>,
    on_peer_connection_callback: RwLock<Option<Box<PeerConnectionCallback>>>,
    on_peer_disconnection_callback: RwLock<Option<Box<PeerDisconnectionCallback>>>,
}

impl Inner {
    async fn new(id: u32) -> Inner {
        let cache = mouscache::memory();
        Inner {
            id,
            cache: RouterCache::new(cache.clone()),
            peer_senders: Arc::new(RwLock::new(HashMap::new())),
            verify_identity_cb: RwLock::new(None),
            on_peer_connection_callback: RwLock::new(None),
            multi_router_peer: RwLock::new(None),
            on_peer_disconnection_callback: RwLock::new(None),
        }
    }

    async fn new2(id: u32, cache: Cache) -> Inner {
        Inner {
            id,
            cache: RouterCache::new(cache.clone()),
            peer_senders: Arc::new(RwLock::new(HashMap::new())),
            verify_identity_cb: RwLock::new(None),
            on_peer_connection_callback: RwLock::new(None),
            multi_router_peer: RwLock::new(None),
            on_peer_disconnection_callback: RwLock::new(None),
        }
    }
}

#[derive(Clone)]
struct RouterShared {
    pub inner: Arc<Inner>,
}

impl RouterShared {
    async fn new(id: u32) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new(id).await),
        }
    }

    async fn new2(id: u32, cache: Cache) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new2(id, cache).await),
        }
    }

    #[inline]
    async fn find_sender_and_then<F, U>(&mut self, cow_id: u32, and_then: F) -> U
    where
        F: FnOnce(Option<&CowRpcRouterPeerSender>) -> BoxFuture<'_, U>,
    {
        // TODO: We could wrap the guard in a struct and use it.
        let reader = self.inner.peer_senders.read().await;
        let sender_opt = reader
            .iter()
            .find(|p| p.1.inner.cow_id == cow_id)
            .map(|(_, peer_s)| peer_s);

        let res = and_then(sender_opt).await;

        res
    }

    async fn clean_up_connection(self, peer_id: u32, peer_identity: Option<CowRpcIdentity>) {
        let peer = {
            let mut peers = self.inner.peer_senders.write().await;
            peers.remove(&peer_id)
        };

        match peer {
            Some(_p) => {
                if let Some(ref callback) = &*self.inner.on_peer_disconnection_callback.read().await {
                    callback(peer_id, peer_identity.clone()).await;
                }

                self.clean_identity(peer_id, peer_identity);

                {
                    if let Err(e) = self.inner.cache.get_raw_cache().set_rem(ALLOCATED_COW_ID_SET, peer_id) {
                        error!(
                            "Unable to remove allocated cow id {:#010X}, got error: {:?}",
                            peer_id, e
                        );
                    }
                }

                trace!("Peer {:#010X} removed", peer_id);
            }
            None => {
                warn!("Unable to remove peer {:#010X}", peer_id);
            }
        }
    }

    async fn process_msg(&mut self, msg: CowRpcMessage) {
        // Forward message to the right peer
        self.forward_msg(msg).await;
    }

    async fn forward_msg(&mut self, msg: CowRpcMessage) {
        let dst_id = msg.get_dst_id();

        if (dst_id & 0xFFFF_0000) != self.inner.id {
            if let Some(ref router_sender) = &*self.inner.multi_router_peer.read().await {
                match router_sender.send_messages(msg.clone()).await {
                    Ok(_) => {
                        return;
                    }
                    Err(e) => {
                        error!("Message can't be sent via multi_router_peer (nats): {}", e);
                    }
                }
            } else {
                error!("can't send message to the other router: multi_router_peer is None (nats is probably not configured)");
            }
        } else {
            let msg_clone = msg.clone();
            if self
                .find_sender_and_then(dst_id, |sender_opt| {
                    Box::pin(async move {
                        if let Some(sender_ref) = sender_opt {
                            if let Err(e) = sender_ref.send_messages(msg_clone).await {
                                warn!("Send message to peer ID {} failed: {}", dst_id, e);
                                sender_ref.set_connection_error().await;
                            } else {
                                return true;
                            }
                        }
                        false
                    })
                })
                .await
            {
                return;
            }
        }

        warn!(
            "Host unreachable, message can't be forwarded. (msgType={}, srcId={}, dstId={})",
            msg.get_msg_name(),
            msg.get_src_id(),
            dst_id
        );

        if !msg.is_response() {
            match msg {
                CowRpcMessage::Call(header, msg, _) => {
                    // To answer a call, we have to send a result message
                    self.send_call_result_failure(&header, &msg, CowRpcErrorCode::Unreachable.into())
                        .await;
                }
                _ => {
                    // To answer other messages, we just swap src-dst and we set the flag as response + failure.
                    let src_id = msg.get_src_id();

                    if (src_id & 0xFFFF_0000) != self.inner.id {
                        if let Some(ref router_sender) = &*self.inner.multi_router_peer.read().await {
                            let mut msg_clone = msg.clone();
                            msg_clone.swap_src_dst();
                            let flag: u16 = CowRpcErrorCode::Unreachable.into();
                            msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                            let _ = router_sender.send_messages(msg_clone);
                        }
                    } else {
                        self.find_sender_and_then(src_id, |sender_opt| {
                            Box::pin(async move {
                                if let Some(sender_ref) = sender_opt {
                                    let mut msg_clone = msg.clone();
                                    msg_clone.swap_src_dst();
                                    let flag: u16 = CowRpcErrorCode::Unreachable.into();
                                    msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                                    if let Err(e) = sender_ref.send_messages(msg_clone).await {
                                        warn!("Send message to peer ID {} failed: {}", src_id, e);
                                        sender_ref.set_connection_error().await;
                                    }
                                }
                            })
                        })
                        .await;
                    }
                }
            }
        }
    }

    async fn send_call_result_failure(&mut self, header_received: &CowRpcHdr, msg_received: &CowRpcCallMsg, flag: u16) {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESULT_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: header_received.dst_id,
            dst_id: header_received.src_id,
            ..Default::default()
        };

        let msg = CowRpcResultMsg {
            call_id: msg_received.call_id,
            iface_id: msg_received.iface_id,
            proc_id: msg_received.proc_id,
        };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.size as u8;

        let dst_id = header_received.src_id;

        if (dst_id & 0xFFFF_0000) != self.inner.id {
            if let Some(ref router_sender) = &*self.inner.multi_router_peer.read().await {
                let _ = router_sender.send_messages(CowRpcMessage::Result(header, msg, Vec::new()));
            }
        } else {
            self.find_sender_and_then(dst_id, |sender_opt| {
                Box::pin(async move {
                    if let Some(sender_ref) = sender_opt {
                        if let Err(e) = sender_ref
                            .send_messages(CowRpcMessage::Result(header, msg, Vec::new()))
                            .await
                        {
                            warn!("Send message to peer ID {} failed: {}", header.src_id, e);
                            sender_ref.set_connection_error().await;
                        }
                    }
                })
            })
            .await;
        }
    }

    fn clean_identity(&self, peer_id: u32, identity: Option<CowRpcIdentity>) {
        if let Some(ref identity) = identity {
            let res = match self.inner.cache.get_cow_identity_peer_addr(identity) {
                Ok(opt_cow_id) => {
                    if let Some(cow_id) = opt_cow_id {
                        if cow_id == peer_id {
                            self.inner.cache.remove_cow_identity(identity, peer_id)
                        } else {
                            Err(CowRpcError::Internal(format!(
                                "Identity {} already belongs to another peer {}",
                                identity.name, cow_id
                            )))
                        }
                    } else {
                        self.inner.cache.remove_cow_identity(identity, peer_id)
                    }
                }
                Err(e) => Err(e),
            };

            match res {
                Ok(_) => {
                    debug!("Identity {} removed", identity.name);
                }
                Err(e) => {
                    warn!(
                        "Unable to remove identity record {}, got error: {:?}",
                        &identity.name, e
                    );
                }
            }
        }
    }
}

pub enum CowRpcRouterPeerState {
    Initial,
    Connected,
    Terminated,
    Error,
}

impl CowRpcRouterPeerState {
    pub fn is_initial(&self) -> bool {
        if let CowRpcRouterPeerState::Initial = self {
            return true;
        }
        false
    }

    pub fn is_connected(&self) -> bool {
        if let CowRpcRouterPeerState::Connected = self {
            return true;
        }
        false
    }

    pub fn is_terminated(&self) -> bool {
        if let CowRpcRouterPeerState::Terminated = self {
            return true;
        }
        false
    }

    pub fn is_error(&self) -> bool {
        if let CowRpcRouterPeerState::Error = self {
            return true;
        }
        false
    }
}

pub struct CowRpcRouterPeerSharedInner {
    cow_id: u32,
    state: RwLock<CowRpcRouterPeerState>,
    writer_sink: Mutex<CowSink<CowRpcMessage>>,
}

pub struct CowRpcRouterPeer {
    inner: Arc<CowRpcRouterPeerSharedInner>,
    identity: Arc<SyncRwLock<Option<CowRpcIdentity>>>,
    reader_stream: CowStream<CowRpcMessage>,
    router: RouterShared,
}

pub struct CowRpcRouterPeerSender {
    inner: Arc<CowRpcRouterPeerSharedInner>,
}

impl CowRpcRouterPeerSender {
    async fn set_connection_error(&self) {
        *self.inner.state.write().await = CowRpcRouterPeerState::Error;
    }

    async fn send_messages(&self, msg: CowRpcMessage) -> Result<()> {
        self.inner.writer_sink.lock().await.send(msg).await
    }
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

impl CowRpcRouterPeer {
    async fn run(
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

    async fn handshake(
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
                    "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
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
                    error!("CowRpc Protocol Error: Router can't process a response: hdr={:?}", hdr);
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
        // self.inner.writer_sink.lock().start_send(msg).map(|_| ())
    }
}

#[derive(Clone)]
struct RouterProc {
    id: u16,
    name: String,
}

#[derive(Clone)]
struct RouterCache {
    inner: Cache,
}

impl RouterCache {
    fn new(cache: Cache) -> Self {
        RouterCache { inner: cache }
    }

    fn get_raw_cache(&self) -> &Cache {
        &self.inner
    }

    fn get_cow_identity(&self, peer_id: u32) -> Result<Option<String>> {
        self.inner
            .hash_get(COW_ID_RECORDS, &peer_id.to_string())
            .map_err(|e| CowRpcError::Internal(format!("got error while doing identity lookup {:?}", e)))
    }

    fn get_cow_identity_peer_addr(&self, identity: &CowRpcIdentity) -> Result<Option<u32>> {
        self.inner
            .hash_get::<u32>(IDENTITY_RECORDS, identity.name.as_ref())
            .map_err(|e| CowRpcError::Internal(format!("got error while doing identity lookup {:?}", e)))
    }

    fn add_cow_identity(&self, identity: &CowRpcIdentity, peer_id: u32) -> Result<()> {
        match self
            .inner
            .hash_set(COW_ID_RECORDS, &peer_id.to_string(), identity.name.clone())
        {
            Ok(true) => {}
            Ok(false) => warn!(
                "Cow addr {:#010X} was updated and now has identity {}",
                peer_id, identity.name
            ),
            _ => {
                return Err(CowRpcError::Internal(format!(
                    "Unable to add identity {} to peer {:#010X}",
                    identity.name, peer_id
                )));
            }
        }

        match self.inner.hash_set(IDENTITY_RECORDS, identity.name.as_ref(), peer_id) {
            Ok(true) => info!(
                "Identity added in router cache: den_id: {}, cow_id: {:#010X}",
                identity.name, peer_id
            ),
            Ok(false) => warn!(
                "Identity {} was updated and now belongs to peer {:#010X}",
                identity.name, peer_id
            ),
            Err(redis_err) => {
                if let Err(e) = self.inner.hash_delete(COW_ID_RECORDS, &[peer_id.to_string().as_ref()]) {
                    error!("Unable to clean cow id record {:#010X}, got error {:?}", peer_id, e);
                }
                return Err(CowRpcError::Internal(format!(
                    "Unable to add record of identity {} to peer {:#010X} with error {:?}",
                    identity.name, peer_id, redis_err
                )));
            }
        }

        Ok(())
    }

    fn remove_cow_identity(&self, identity: &CowRpcIdentity, peer_id: u32) -> Result<()> {
        let mut got_error = false;
        match self.inner.hash_delete(IDENTITY_RECORDS, &[identity.name.as_ref()]) {
            Ok(_) => info!(
                "Identity removed from router cache: den_id: {}, cow_id: {:#010X}",
                identity.name, peer_id
            ),
            _ => {
                got_error = true;
                error!("Unable to clean cow id record {:#010X}", peer_id);
            }
        }

        match self.inner.hash_delete(COW_ID_RECORDS, &[peer_id.to_string().as_ref()]) {
            Ok(_) => {}
            _ => {
                got_error = true;
                error!("Unable to clean cow id record {:#010X}", peer_id);
            }
        }

        if got_error {
            return Err(CowRpcError::Internal(format!(
                "Unable to cleam record of identity {} to peer {:#010X}",
                identity.name, peer_id
            )));
        }

        Ok(())
    }
}
