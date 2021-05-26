use super::{CowRpcIdentityType, CowRpcMessage};
use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto::*;
use crate::router::router_peer::CowRpcRouterPeerSenderGuard;
use crate::transport::adaptor::Adaptor;
use crate::transport::{CowRpcTransport, ListenerBuilder, MessageInterceptor, Transport};
use crate::{proto, CowRpcMessageInterceptor};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::StreamExt;
use mouscache::{Cache, CacheFunc};
use parking_lot::{Mutex as SyncMutex, RwLock as SyncRwLock};
use std::collections::HashMap;

use std::sync::Arc;

use crate::router::router_peer::{CowRpcRouterPeer, CowRpcRouterPeerSender, CowRpcRouterPeerState};
use crate::transport::{CowRpcListener, TlsOptions};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::sync::RwLock;
use {mouscache, std};

mod router_peer;

const ALLOCATED_COW_ID_SET: &str = "allocated_cow_id";
const COW_ID_RECORDS: &str = "cow_address_records";
const IDENTITY_RECORDS: &str = "identities_records";

const PEER_CONNECTION_GRACE_PERIOD: u64 = 10;

const ROUTER_DEFAULT_PORT: u16 = 10261;

type IdentityVerificationCallback = dyn Fn(u32, &[u8]) -> BoxFuture<'_, (Vec<u8>, Option<String>)> + Send + Sync;
type PeerConnectionCallback = dyn Fn(u32) -> () + Send + Sync;
type PeerDisconnectionCallback = dyn Fn(u32, Option<CowRpcIdentity>) -> BoxFuture<'static, ()> + Send + Sync;
pub type PeersAreAliveCallback = dyn Fn(&[u32]) -> BoxFuture<'_, ()> + Send + Sync;

#[derive(Clone, PartialEq)]
pub enum RouterState {
    Init,
    Running,
    Stopping,
    Stopped,
}

#[derive(Clone)]
pub struct RouterHandle {
    state: Arc<SyncRwLock<RouterState>>,
    wakers: Arc<SyncMutex<Vec<(RouterState, Waker)>>>,
}

impl RouterHandle {
    fn new() -> Self {
        RouterHandle {
            state: Arc::new(SyncRwLock::new(RouterState::Init)),
            wakers: Arc::new(SyncMutex::new(Vec::new())),
        }
    }

    pub async fn stop(&self) {
        self.update_state(RouterState::Stopping);
        self.wait_state(RouterState::Stopped).await;
    }

    pub async fn wait(&self) {
        self.wait_state(RouterState::Stopped).await;
    }

    fn wait_state(&self, waiting_state: RouterState) -> WaitRouterState {
        WaitRouterState {
            registered: false,
            state: self.clone(),
            waiting_state: waiting_state,
        }
    }

    fn get_state(&self) -> RouterState {
        self.state.read().clone()
    }

    fn update_state(&self, new_state: RouterState) {
        let mut state_writer = self.state.write();
        let mut wakers = self.wakers.lock();

        let mut i = 0;
        while i != wakers.len() {
            let (state, _) = &wakers[i];
            if *state == new_state {
                let (_, waker) = wakers.remove(i);
                waker.wake();
            } else {
                i += 1;
            }
        }

        *state_writer = new_state;
    }

    fn register(&mut self, waiting_state: RouterState, waker: Waker) {
        self.wakers.lock().push((waiting_state, waker));
    }
}

pub struct WaitRouterState {
    state: RouterHandle,
    waiting_state: RouterState,
    registered: bool,
}

impl Future for WaitRouterState {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if !this.registered {
            this.state.register(this.waiting_state.clone(), cx.waker().clone());
            this.registered = true;
        }

        if this.state.get_state() == this.waiting_state {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct RouterTask<T, S> {
    task: Pin<Box<T>>,
    shutdown: Pin<Box<S>>,
}

impl<T, S> RouterTask<T, S> {
    pub fn new(task: T, shutdown: S) -> Self {
        RouterTask {
            task: Box::pin(task),
            shutdown: Box::pin(shutdown),
        }
    }
}

impl<T, S> Future for RouterTask<T, S>
where
    T: Future<Output = ()>,
    S: Future<Output = ()>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::as_mut(&mut self.shutdown).poll(cx) {
            Poll::Ready(()) => Poll::Ready(()),
            Poll::Pending => match Pin::as_mut(&mut self.task).poll(cx) {
                Poll::Ready(()) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[derive(Default)]
pub struct CowRpcRouterBuilder {
    id: Option<u16>,
    cache: Option<Cache>,
    listener_url: Option<String>,
    tls_options: Option<TlsOptions>,
    peers_are_alive_task_info: Option<PeersAreAliveTaskInfo>,
    keep_alive_interval: Option<Duration>,
}

impl CowRpcRouterBuilder {
    pub fn new() -> Self {
        CowRpcRouterBuilder::default()
    }

    pub fn id(mut self, id: u16) -> Self {
        self.id = Some(id);
        self
    }

    pub fn cache(mut self, cache: Cache) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn listener_url(mut self, listener_url: &str) -> Self {
        self.listener_url = Some(listener_url.to_owned());
        self
    }

    pub fn tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }

    pub fn peers_are_alive_callback<F: 'static + Fn(&[u32]) -> BoxFuture<'_, ()> + Send + Sync>(
        &mut self,
        interval: Duration,
        callback: F,
    ) {
        self.peers_are_alive_task_info = Some(PeersAreAliveTaskInfo {
            callback: Box::new(callback),
            interval,
        });
    }

    pub fn keep_alive_interval(mut self, keep_alive_interval: Duration) -> Self {
        self.keep_alive_interval = Some(keep_alive_interval);
        self
    }

    pub async fn build(self) -> CowRpcRouter {
        let listener_url = self
            .listener_url
            .unwrap_or_else(|| format!("ws://0.0.0.0:{}", ROUTER_DEFAULT_PORT));
        let router_id = u32::from(self.id.unwrap_or(0)) << 16;
        let cache = self.cache.unwrap_or(mouscache::memory());

        CowRpcRouter {
            listener_url,
            tls_options: self.tls_options,
            shared: RouterShared::new(router_id, cache).await,
            adaptor: Adaptor::new(),
            msg_interceptor: None,
            peers_are_alive_task_info: self.peers_are_alive_task_info,
            keep_alive_interval: self.keep_alive_interval,
        }
    }
}
pub struct CowRpcRouter {
    listener_url: String,
    tls_options: Option<TlsOptions>,
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

    pub async fn set_msg_interceptor<T: 'static + Send + Sync + Clone>(
        &mut self,
        interceptor: CowRpcMessageInterceptor<T>,
    ) {
        let (_, sink) = CowRpcTransport::from_interceptor(interceptor.clone_boxed()).message_stream_sink();

        let peer = CowRpcRouterPeerSender::new(0, CowRpcRouterPeerState::Connected, sink);
        *self.shared.inner.multi_router_peer.write().await = Some(peer);
        self.msg_interceptor = Some(Box::new(interceptor));
    }

    pub fn get_msg_injector(&self) -> Adaptor {
        self.adaptor.clone()
    }

    pub fn get_id(&self) -> u32 {
        self.shared.inner.id
    }

    pub async fn start(self) -> Result<RouterHandle> {
        let CowRpcRouter {
            listener_url,
            tls_options,
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

        let router_handle = RouterHandle::new();

        tokio::spawn(RouterTask::new(
            msg_injection_task(adaptor, shared.clone()),
            router_handle.wait_state(RouterState::Stopping),
        ));

        if let Some(task_info) = peers_are_alive_task_info {
            tokio::spawn(RouterTask::new(
                peers_are_alive_task(shared.inner.peer_senders.clone(), task_info),
                router_handle.wait_state(RouterState::Stopping),
            ));
        }

        let router_handle_clone = router_handle.clone();
        tokio::spawn(
            RouterTask::new(
                incoming_task(listener_builder.build().await?, shared.clone(), keep_alive_interval),
                router_handle.wait_state(RouterState::Stopping),
            )
            .then(|_| async move {
                shared.terminate_all_connections().await;
                router_handle_clone.update_state(RouterState::Stopped);
            }),
        );

        router_handle.update_state(RouterState::Running);

        Ok(router_handle)
    }
}

async fn incoming_task(listener: CowRpcListener, router: RouterShared, keep_alive_interval: Option<Duration>) {
    let router_shared_clone = router.clone();
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
}

async fn msg_injection_task(adaptor: Adaptor, router: RouterShared) {
    adaptor
        .message_stream()
        .for_each(move |msg| {
            let mut router_clone = router.clone();
            async move {
                if let Ok(msg) = msg {
                    router_clone.process_msg(msg).await;
                }
            }
        })
        .await;
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
        .insert(peer.get_cow_id(), peer_sender);

    if let Some(ref callback) = &*router.inner.on_peer_connection_callback.read().await {
        callback(peer.get_cow_id());
    }

    let (peer_id, identity, error) = match peer.run().await {
        Ok((peer_id, identity)) => (peer_id, identity, None),
        Err((peer_id, identity, error)) => (peer_id, identity, Some(error)),
    };

    router.clone().clean_up_connection(peer_id, identity).await;

    if let Some(err) = error {
        Err(err)
    } else {
        Ok(())
    }
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

pub(crate) struct Inner {
    id: u32,
    cache: RouterCache,
    peer_senders: Arc<RwLock<HashMap<u32, CowRpcRouterPeerSender>>>,
    multi_router_peer: RwLock<Option<CowRpcRouterPeerSender>>,
    verify_identity_cb: RwLock<Option<Box<IdentityVerificationCallback>>>,
    on_peer_connection_callback: RwLock<Option<Box<PeerConnectionCallback>>>,
    on_peer_disconnection_callback: RwLock<Option<Box<PeerDisconnectionCallback>>>,
}

impl Inner {
    async fn new(id: u32, cache: Cache) -> Inner {
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
pub(crate) struct RouterShared {
    pub inner: Arc<Inner>,
}

impl RouterShared {
    async fn new(id: u32, cache: Cache) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new(id, cache).await),
        }
    }

    async fn find_sender(&mut self, cow_id: u32) -> Option<CowRpcRouterPeerSenderGuard<'_>> {
        let peer_senders = self.inner.peer_senders.read().await;
        CowRpcRouterPeerSenderGuard::new(peer_senders, cow_id)
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

                if let Err(e) = self.inner.cache.get_raw_cache().set_rem(ALLOCATED_COW_ID_SET, peer_id) {
                    error!(
                        "Unable to remove allocated cow id {:#010X}, got error: {:?}",
                        peer_id, e
                    );
                }

                trace!("Peer {:#010X} removed", peer_id);
            }
            None => {
                warn!("Peer {:#010X} not found, it can't be removed", peer_id);
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
            if let Some(sender) = self.find_sender(dst_id).await {
                if let Err(e) = sender.send_messages(msg_clone).await {
                    warn!("Send message to peer ID {} failed: {}", dst_id, e);
                    sender.set_connection_error().await;
                } else {
                    return;
                }
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
                        if let Some(sender) = self.find_sender(src_id).await {
                            let mut msg_clone = msg.clone();
                            msg_clone.swap_src_dst();
                            let flag: u16 = CowRpcErrorCode::Unreachable.into();
                            msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                            if let Err(e) = sender.send_messages(msg_clone).await {
                                warn!("Send message to peer ID {} failed: {}", src_id, e);
                                sender.set_connection_error().await;
                            }
                        }
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
            if let Some(sender) = self.find_sender(dst_id).await {
                if let Err(e) = sender
                    .send_messages(CowRpcMessage::Result(header, msg, Vec::new()))
                    .await
                {
                    warn!("Send message to peer ID {} failed: {}", header.src_id, e);
                    sender.set_connection_error().await;
                }
            }
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

    async fn terminate_all_connections(&self) {
        let mut peer_senders = self.inner.peer_senders.write().await;

        for (id, peer_sender) in peer_senders.drain() {
            let mut header = CowRpcHdr {
                msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
                src_id: self.inner.id,
                dst_id: id,
                ..Default::default()
            };

            header.size = header.get_size();
            header.offset = header.size as u8;

            let _ = peer_sender.send_messages(CowRpcMessage::Terminate(header)).await;
        }
    }
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
