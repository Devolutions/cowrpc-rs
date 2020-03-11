use super::{CowRpcIdentityType, CowRpcMessage};
use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use futures::{
    future::ok,
    sync::oneshot::{channel, Receiver, Sender},
    Async, Future, Stream,
};
use mouscache;
use mouscache::Cache;
use mouscache::CacheFunc;
use parking_lot::{Mutex, RwLock};
use crate::proto;
use crate::proto::*;
use rand;
use crate::router::CowRpcIdentity;
use std;
use std::{collections::HashMap, fmt, sync::Arc};
use tokio::prelude::*;
use tokio::util::FutureExt;
use tokio::runtime::{Runtime, TaskExecutor};
use crate::transport::{
    r#async::{ListenerBuilder, CowRpcTransport, Transport, CowSink, CowStream, adaptor::Adaptor},
    MessageInterceptor,
    tls::TlsOptions,
};
use crate::CowRpcMessageInterceptor;

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

pub const ALLOCATED_COW_ID_SET: &str = "allocated_cow_id";
pub const COW_ID_RECORDS: &str = "cow_address_records";
pub const IDENTITY_RECORDS: &str = "identities_records";

type IdentityVerificationCallback = dyn Fn(&[u8]) -> (Vec<u8>, Option<String>) + Send + Sync;
type PeerConnectionCallback = dyn Fn(u32) -> () + Send + Sync;
type PeerDisconnectionCallback = dyn Fn(u32, Option<CowRpcIdentity>) -> () + Send + Sync;

pub struct CowRpcRouter {
    listener_url: String,
    listener_tls_options: Option<TlsOptions>,
    monitor: RouterMonitor,
    shared: RouterShared,
    adaptor: Adaptor,
    msg_interceptor: Option<Box<dyn MessageInterceptor>>,
}

impl CowRpcRouter {
    pub fn new(url: &str, listener_tls_options: Option<TlsOptions>) -> Result<(CowRpcRouter, RouterHandle)> {
        let id: u32 = 0;
        let (handle, router_monitor) = channel();
        let router = CowRpcRouter {
            listener_url: url.to_string(),
            listener_tls_options,
            monitor: router_monitor,
            shared: RouterShared::new(id),
            adaptor: Adaptor::new(),
            msg_interceptor: None,
        };

        let router_handle = RouterHandle::new(handle);

        Ok((router, router_handle))
    }

    pub fn new2(
        id: u16,
        cache: Cache,
        url: &str,
        listener_tls_options: Option<TlsOptions>,
    ) -> Result<(CowRpcRouter, RouterHandle)> {
        let router_id = u32::from(id) << 16;
        let (handle, router_monitor) = channel();
        let router = CowRpcRouter {
            listener_url: url.to_string(),
            listener_tls_options,
            monitor: router_monitor,
            shared: RouterShared::new2(router_id, cache),
            adaptor: Adaptor::new(),
            msg_interceptor: None,
        };

        let router_handle = RouterHandle::new(handle);

        Ok((router, router_handle))
    }

    pub fn on_peer_connection_callback<F: 'static + Fn(u32) + Send + Sync>(&mut self, callback: F) {
        let mut cb = self.shared.inner.on_peer_connection_callback.write();
        *cb = Some(Box::new(callback));
    }

    pub fn on_peer_disconnection_callback<F: 'static + Fn(u32, Option<CowRpcIdentity>) + Send + Sync>(
        &mut self,
        callback: F,
    ) {
        let mut cb = self.shared.inner.on_peer_disconnection_callback.write();
        *cb = Some(Box::new(callback));
    }

    pub fn verify_identity_callback<F: 'static + Fn(&[u8]) -> (Vec<u8>, Option<String>) + Send + Sync>(&mut self, callback: F) {
        let mut cb = self.shared.inner.verify_identity_cb.write();
        *cb = Some(Box::new(callback));
    }

    pub fn set_msg_interceptor<T: 'static + Send + Sync + Clone>(&mut self, interceptor: CowRpcMessageInterceptor<T>) {
        let peer = CowRpcRouterPeerSender {
            inner: Arc::new(CowRpcRouterPeerSharedInner {
                cow_id: 0,
                state: RwLock::new(CowRpcRouterPeerState::Connected),
                writer_sink: Mutex::new(CowRpcTransport::from_interceptor(interceptor.clone_boxed()).message_sink()),
                binds: RouterBindCollection::new(0, self.shared.inner.cache.get_raw_cache().clone()),
            }),
        };
        *self.shared.inner.multi_router_peer.write() = Some(peer);
        self.msg_interceptor = Some(Box::new(interceptor));
    }

    pub fn get_msg_injector(&self) -> Adaptor {
        self.adaptor.clone()
    }

    pub fn get_id(&self) -> u32 {
        self.shared.inner.id
    }

    pub fn spawn(self, executor_handle: TaskExecutor) -> Result<RouterMonitor> {
        let CowRpcRouter {
            listener_url,
            listener_tls_options,
            monitor,
            shared,
            adaptor,
            msg_interceptor,
        } = self;

        let router_shared_clone = shared.clone();

        let mut listener_builder = ListenerBuilder::from_uri(&listener_url)?.executor(executor_handle.clone());

        if let Some(interceptor) = msg_interceptor {
            listener_builder = listener_builder.msg_interceptor(interceptor);
        }

        if let Some(tls) = listener_tls_options {
            listener_builder = listener_builder.with_ssl(tls);
        }

        let listener = listener_builder.build()?;

        let executor_handle_clone = executor_handle.clone();
        let router_peer_stream = listener.incoming().for_each(move |stream_fut| {
            let router_shared_hand = router_shared_clone.clone();
            let peer_handshake = stream_fut.and_then(move |stream| {
                CowRpcRouterPeer::new(stream, router_shared_hand.clone())
                    .timeout(::std::time::Duration::from_secs(PEER_CONNECTION_GRACE_PERIOD))
                    .map_err(|e| match e.into_inner() {
                        Some(e) => e,
                        None => CowRpcError::Proto(format!(
                            "handshake timed out after {}",
                            PEER_CONNECTION_GRACE_PERIOD
                        )),
                    })
            });

            let peer_router_shared = router_shared_clone.clone();
            let router_cleanup_clone = router_shared_clone.clone();
            let router_error_clone = router_shared_clone.clone();
            let peer_fut = peer_handshake
                .and_then(move |(peer, peer_sender)| {
                    {
                        peer_router_shared
                            .inner
                            .peer_senders
                            .write()
                            .insert(peer.inner.cow_id, peer_sender);

                        if let Some(ref callback) = &*peer_router_shared.inner.on_peer_connection_callback.read() {
                            callback(peer.inner.cow_id);
                        }
                    }

                    peer.map(move |(peer_id, identity)| {
                        router_cleanup_clone.clean_up_connection(peer_id, identity);
                    }).map_err(move |(peer_id, identity, error)| {
                        error!("Got an error while polling peer {:#010X} : {:?}", peer_id, error);
                        router_error_clone.clean_up_connection(peer_id, identity);
                        error
                    })
                }).map_err(|e| {
                trace!("{:?}", e);
            });

            executor_handle_clone.spawn(peer_fut);

            ok(())
        });

        executor_handle.spawn(router_peer_stream.map_err(|_cow_error| ()));
        let mut router_shared_clone = shared.clone();
        executor_handle.spawn(
            adaptor
                .message_stream()
                .for_each(move |msg| {
                    router_shared_clone.process_msg(msg);
                    ok::<(), CowRpcError>(())
                }).map(|_| ())
                .map_err(|_| ()),
        );

        Ok(monitor)
    }

    pub fn run(self) -> Result<()> {
        let mut runtime =
            Runtime::new().expect("This should never fails, a runtime is needed by the entire implementation");

        let executor_handle = runtime.executor();

        let monitor = self.spawn(executor_handle)?;
        runtime.block_on(monitor.map_err(|_| CowRpcError::Internal("The runtime stopped unexpectedly".to_string())))
    }
}

struct Inner {
    id: u32,
    cache: RouterCache,
    ifaces: RouterIfaceCollection,
    peer_senders: RwLock<HashMap<u32, CowRpcRouterPeerSender>>,
    multi_router_peer: RwLock<Option<CowRpcRouterPeerSender>>,
    verify_identity_cb: RwLock<Option<Box<IdentityVerificationCallback>>>,
    on_peer_connection_callback: RwLock<Option<Box<PeerConnectionCallback>>>,
    on_peer_disconnection_callback: RwLock<Option<Box<PeerDisconnectionCallback>>>,
}

impl Inner {
    fn new(id: u32) -> Inner {
        let cache = mouscache::memory();
        Inner {
            id,
            cache: RouterCache::new(cache.clone()),
            peer_senders: RwLock::new(HashMap::new()),
            ifaces: RouterIfaceCollection::new(cache),
            verify_identity_cb: RwLock::new(None),
            on_peer_connection_callback: RwLock::new(None),
            multi_router_peer: RwLock::new(None),
            on_peer_disconnection_callback: RwLock::new(None),
        }
    }

    fn new2(id: u32, cache: Cache) -> Inner {
        Inner {
            id,
            cache: RouterCache::new(cache.clone()),
            peer_senders: RwLock::new(HashMap::new()),
            ifaces: RouterIfaceCollection::new(cache),
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
    fn new(id: u32) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new(id)),
        }
    }

    fn new2(id: u32, cache: Cache) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new2(id, cache)),
        }
    }

    #[inline]
    fn find_sender_and_then<F, U>(&mut self, cow_id: u32, and_then: F) -> U
        where
            F: FnOnce(Option<&CowRpcRouterPeerSender>) -> U,
    {
        let reader = self.inner.peer_senders.read();
        let sender_opt = reader
            .iter()
            .find(|p| p.1.inner.cow_id == cow_id)
            .map(|(_, peer_s)| peer_s);

        let res = and_then(sender_opt);

        sender_opt.and_then(|sender| {
            let _ = sender.inner.writer_sink.lock().poll_complete();
            Some(())
        });

        res
    }

    fn register_iface_def(&self, iface_def: &mut CowRpcIfaceDef) -> Result<()> {
        self.inner.ifaces.add(iface_def)
    }

    fn clean_up_connection(&self, peer_id: u32, peer_identity: Option<CowRpcIdentity>) {
        let mut peers = self.inner.peer_senders.write();

        //Remove the peer and unbind all bind context between this peer and others
        match peers.remove(&peer_id) {
            Some(ref peer_ref) => {
                if let Some(ref callback) = &*self.inner.on_peer_disconnection_callback.read() {
                    callback(peer_id, peer_identity.clone());
                }

                self.clean_identity(peer_id, peer_identity);

                if let Ok(binds) = peer_ref.inner.binds.get_all() {
                    for (client_id, server_id, iface_id) in binds {
                        if let Ok(true) =
                        peer_ref
                            .inner
                            .binds
                            .update(client_id, server_id, iface_id, CowRpcBindState::Unbinding)
                            {
                                let remote_id = if client_id == peer_ref.inner.cow_id {
                                    server_id
                                } else {
                                    client_id
                                };
                                if let Some(remote_ref) = peers.get_mut(&remote_id) {
                                    remote_ref.send_unbind_req(client_id, server_id, iface_id);
                                    {
                                        let _ = remote_ref.inner.writer_sink.lock().poll_complete();
                                    }
                                } else {
                                    if let Some(ref router_sender) = &*self.inner.multi_router_peer.read() {
                                        // Send Unbind req
                                        let iface_def = CowRpcIfaceDef {
                                            id: iface_id,
                                            flags: COW_RPC_DEF_FLAG_EMPTY,
                                            ..Default::default()
                                        };

                                        let iface_defs = vec![iface_def];

                                        let remote_is_server = server_id == remote_id;
                                        let src_id = if remote_is_server { client_id } else { server_id };

                                        let mut header = CowRpcHdr {
                                            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
                                            flags: if !remote_is_server { COW_RPC_FLAG_SERVER } else { 0 },
                                            src_id,
                                            dst_id: remote_id,
                                            ..Default::default()
                                        };

                                        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

                                        header.size = header.get_size() + msg.get_size();
                                        header.offset = header.get_size() as u8;

                                        let _ = router_sender.send_messages(CowRpcMessage::Unbind(header, msg));
                                    } else {
                                        warn!("No peer found with cow id {}", remote_id);
                                    }
                                }
                            }
                    }
                }

                peer_ref.clear_bind_contexts();

                {
                    if let Err(e) = self.inner.cache.get_raw_cache().set_rem(ALLOCATED_COW_ID_SET, peer_id) {
                        error!("Unable to remove allocated cow id {:#010X}, got error: {:?}", peer_id, e);
                    }
                }

                trace!("Peer {:#010X} removed", peer_id);
            }
            None => {
                warn!("Unable to remove peer {:#010X}", peer_id);
            }
        }
    }

    fn process_msg(&mut self, msg: CowRpcMessage) {
        match msg.clone() {
            CowRpcMessage::Bind(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_bind_req(hdr, msg);
                } else {
                    self.process_bind_rsp(hdr, msg);
                }
            }
            CowRpcMessage::Unbind(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_unbind_req(hdr, msg);
                } else {
                    self.process_unbind_rsp(hdr, msg);
                }
            }
            _ => {}
        }

        // Forward message to the right peer
        self.forward_msg(msg);
    }

    fn process_bind_req(&mut self, _header: CowRpcHdr, _msg: CowRpcBindMsg) {
        trace!("received bind request")
    }

    fn process_bind_rsp(&mut self, header: CowRpcHdr, msg: CowRpcBindMsg) {
        for iface in msg.ifaces {
            let client_id = header.dst_id;
            let server_id = header.src_id;
            let iface_id = iface.id;

            let local_peer_id = header.dst_id;

            let mut success = false;

            if header.is_failure() || iface.is_failure() {
                if let CowRpcErrorCode::AlreadyBound = CowRpcErrorCode::from(iface.flags) {
                    success = true;
                } else {
                    self.update_bind(local_peer_id, client_id, server_id, iface_id, CowRpcBindState::Failure);
                    self.remove_bind(local_peer_id, client_id, server_id, iface_id);
                }
            } else {
                success = true;
            }

            if success {
                self.find_sender_and_then(header.dst_id, |peer_sender_opt| {
                    if let Some(sender) = peer_sender_opt {
                        sender.add_bind(client_id, server_id, iface_id);
                        sender.add_remote_bind(header.src_id, client_id, server_id, iface_id);
                    }
                });
            }
        }
    }

    fn process_unbind_req(&mut self, header: CowRpcHdr, msg: CowRpcUnbindMsg) {
        for iface in msg.ifaces {
            let client_id;
            let server_id;
            let iface_id = iface.id;

            let local_peer_id = header.dst_id;

            if header.from_server() {
                client_id = header.dst_id;
                server_id = header.src_id;
            } else {
                client_id = header.src_id;
                server_id = header.dst_id;
            }

            if header.is_failure() || iface.is_failure() {
                self.remove_bind(local_peer_id, client_id, server_id, iface_id);
            }
        }
    }

    fn process_unbind_rsp(&mut self, header: CowRpcHdr, msg: CowRpcUnbindMsg) {
        for iface in msg.ifaces {
            let client_id;
            let server_id;
            let iface_id = iface.id;

            let local_peer_id = header.dst_id;

            if header.from_server() {
                client_id = header.src_id;
                server_id = header.dst_id;
            } else {
                client_id = header.dst_id;
                server_id = header.src_id;
            }

            if iface.is_failure() || header.is_failure() {
                self.update_bind(local_peer_id, client_id, server_id, iface_id, CowRpcBindState::Failure);
            } else {
                self.update_bind(local_peer_id, client_id, server_id, iface_id, CowRpcBindState::Unbound);
            }

            self.remove_bind(local_peer_id, client_id, server_id, iface_id);
        }
    }

    fn update_bind(
        &mut self,
        local_peer_id: u32,
        client_id: u32,
        server_id: u32,
        iface_id: u16,
        new_state: CowRpcBindState,
    ) {
        // Update the bind state
        self.find_sender_and_then(local_peer_id, |sender_opt| {
            if let Some(sender) = sender_opt {
                sender.update_bind_state(client_id, server_id, iface_id, new_state);
                let remote_id = if local_peer_id == client_id {
                    server_id
                } else {
                    client_id
                };
                sender.update_remote_bind_state(remote_id, client_id, server_id, iface_id, new_state);
            }
        });
    }

    fn remove_bind(&mut self, local_peer_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        // Remove the bind
        self.find_sender_and_then(local_peer_id, |sender_opt| {
            if let Some(sender) = sender_opt {
                sender.remove_bind(client_id, server_id, iface_id);
                let remote_id = if local_peer_id == client_id {
                    server_id
                } else {
                    client_id
                };
                sender.remove_remote_bind(remote_id, client_id, server_id, iface_id);
            }
        });
    }

    fn forward_msg(&mut self, msg: CowRpcMessage) {
        let dst_id = msg.get_dst_id();

        if (dst_id & 0xFFFF_0000) != self.inner.id {
            if let Some(ref router_sender) = &*self.inner.multi_router_peer.read() {
                match router_sender.send_messages(msg.clone()) {
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
            if self.find_sender_and_then(dst_id, |sender_opt| {
                if let Some(sender_ref) = sender_opt {
                    if let Err(e) = sender_ref.send_messages(msg.clone()) {
                        warn!("Send message to peer ID {} failed: {}", dst_id, e);
                        sender_ref.set_connection_error();
                    } else {
                        return true;
                    }
                }
                false
            }) {
                return;
            } else if msg.is_unbind() {
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
                    self.send_call_result_failure(&header, &msg, CowRpcErrorCode::Unreachable.into());
                }
                _ => {
                    // To answer other messages, we just swap src-dst and we set the flag as response + failure.
                    let src_id = msg.get_src_id();

                    if (src_id & 0xFFFF_0000) != self.inner.id {
                        if let Some(ref router_sender) = &*self.inner.multi_router_peer.read() {
                            let mut msg_clone = msg.clone();
                            msg_clone.swap_src_dst();
                            let flag: u16 = CowRpcErrorCode::Unreachable.into();
                            msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                            let _ = router_sender.send_messages(msg_clone);
                        }
                    } else {
                        self.find_sender_and_then(src_id, |sender_opt| {
                            if let Some(sender_ref) = sender_opt {
                                let mut msg_clone = msg.clone();
                                msg_clone.swap_src_dst();
                                let flag: u16 = CowRpcErrorCode::Unreachable.into();
                                msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                                sender_ref.send_messages(msg_clone).unwrap_or_else(|e| {
                                    warn!("Send message to peer ID {} failed: {}", src_id, e);
                                    sender_ref.set_connection_error();
                                });
                            }
                        });
                    }
                }
            }
        } else {
            match msg.clone() {
                CowRpcMessage::Bind(hdr, msg) => {
                    let src_id = hdr.src_id;

                    for iface in msg.ifaces {
                        if (src_id & 0xFFFF_0000) != self.inner.id {
                            if let Some(ref router_sender) = &*self.inner.multi_router_peer.read() {
                                // Send Unbind req
                                let iface_defs = vec![iface];

                                let mut header = CowRpcHdr {
                                    msg_type: proto::COW_RPC_UNBIND_MSG_ID,
                                    flags: 0,
                                    src_id: hdr.dst_id,
                                    dst_id: hdr.src_id,
                                    ..Default::default()
                                };

                                let msg = CowRpcUnbindMsg { ifaces: iface_defs };

                                header.size = header.get_size() + msg.get_size();
                                header.offset = header.get_size() as u8;

                                let _ = router_sender.send_messages(CowRpcMessage::Unbind(header, msg));
                            }
                        } else {
                            self.find_sender_and_then(dst_id, |sender_opt| {
                                if let Some(sender_ref) = sender_opt {
                                    sender_ref.send_unbind_req(hdr.dst_id, hdr.src_id, iface.id);
                                }
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn send_call_result_failure(&mut self, header_received: &CowRpcHdr, msg_received: &CowRpcCallMsg, flag: u16) {
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
            if let Some(ref router_sender) = &*self.inner.multi_router_peer.read() {
                let _ = router_sender.send_messages(CowRpcMessage::Result(header, msg, Vec::new()));
            }
        } else {
            self.find_sender_and_then(dst_id, |sender_opt| {
                if let Some(sender_ref) = sender_opt {
                    sender_ref
                        .send_messages(CowRpcMessage::Result(header, msg, Vec::new()))
                        .unwrap_or_else(|e| {
                            warn!("Send message to peer ID {} failed: {}", header.src_id, e);
                            sender_ref.set_connection_error();
                        });
                }
            });
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

pub struct CowRpcRouterPeerHandshake {
    transport: CowRpcTransport,
    reader_stream: CowStream<CowRpcMessage>,
    router: RouterShared,
}

impl CowRpcRouterPeerHandshake {
    fn generate_peer_id(&self) -> u32 {
        loop {
            let id = rand::random::<u16>();

            //0 is not accepted as peer_id
            if id != 0 {
                let peer_id = self.router.inner.id | u32::from(id);
                if let Ok(false) = self
                    .router
                    .inner
                    .cache
                    .get_raw_cache()
                    .set_ismember(ALLOCATED_COW_ID_SET, peer_id)
                    {
                        if self
                            .router
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
}

impl Future for CowRpcRouterPeerHandshake {
    type Item = (CowRpcRouterPeer, CowRpcRouterPeerSender);
    type Error = CowRpcError;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>> {
        match self.reader_stream.poll()? {
            Async::Ready(Some(msg)) => match msg {
                CowRpcMessage::Handshake(hdr, msg) => {
                    if !hdr.is_response() {
                        let mut flag: u16 = CowRpcErrorCode::Success.into();

                        if hdr.flags & COW_RPC_FLAG_DIRECT != 0 {
                            flag = CowRpcErrorCode::Proto.into();

                            let router = self.router.clone();
                            let cache_clone = self.router.inner.cache.get_raw_cache().clone();
                            let mut transport = self.transport.clone();
                            let reader_stream = transport.message_stream();
                            let writer_sink = transport.message_sink();
                            let inner = Arc::new(CowRpcRouterPeerSharedInner {
                                cow_id: 0,
                                writer_sink: Mutex::new(writer_sink),
                                binds: RouterBindCollection::new(0, cache_clone),
                                state: RwLock::new(CowRpcRouterPeerState::Error),
                            });

                            let mut peer = CowRpcRouterPeer {
                                inner: inner.clone(),
                                identity: None,
                                reader_stream,
                                router,
                            };

                            peer.send_handshake_rsp(flag)?;
                            peer.inner.writer_sink.lock().poll_complete()?;

                            Err(CowRpcError::Proto(
                                "Handshake used the direct connection flag, shutting down the connection".to_string(),
                            ))
                        } else {
                            trace!("Client connected from {:?}", self.transport.remote_addr());

                            let (mut peer, peer_sender) = {
                                let router = self.router.clone();
                                let cache_clone = self.router.inner.cache.get_raw_cache().clone();
                                let mut transport = self.transport.clone();
                                let reader_stream = transport.message_stream();
                                let writer_sink = transport.message_sink();
                                let peer_id = self.generate_peer_id();
                                let inner = Arc::new(CowRpcRouterPeerSharedInner {
                                    cow_id: peer_id,
                                    writer_sink: Mutex::new(writer_sink),
                                    binds: RouterBindCollection::new(peer_id, cache_clone),
                                    state: RwLock::new(CowRpcRouterPeerState::Connected),
                                });

                                (
                                    CowRpcRouterPeer {
                                        inner: inner.clone(),
                                        identity: None,
                                        reader_stream,
                                        router,
                                    },
                                    CowRpcRouterPeerSender { inner },
                                )
                            };

                            peer.send_handshake_rsp(flag)?;
                            peer.inner.writer_sink.lock().poll_complete()?;

                            Ok(Async::Ready((peer, peer_sender)))
                        }
                    } else {
                        Err(CowRpcError::Proto(format!(
                            "Router can't process a response: hdr={:?} - msg={:?}",
                            hdr, msg
                        )))
                    }
                }
                _ => Err(CowRpcError::Proto(
                    "First message was not a handshake message, shutting down the connection".to_string(),
                )),
            },
            Async::Ready(None) => Err(CowRpcError::Proto("Connection was closed before handshake".to_string())),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub struct CowRpcRouterPeerSharedInner {
    cow_id: u32,
    state: RwLock<CowRpcRouterPeerState>,
    writer_sink: Mutex<CowSink<CowRpcMessage>>,
    binds: RouterBindCollection,
}

pub struct CowRpcRouterPeer {
    inner: Arc<CowRpcRouterPeerSharedInner>,
    identity: Option<CowRpcIdentity>,
    reader_stream: CowStream<CowRpcMessage>,
    router: RouterShared,
}

pub struct CowRpcRouterPeerSender {
    inner: Arc<CowRpcRouterPeerSharedInner>,
}

impl CowRpcRouterPeerSender {
    fn set_connection_error(&self) {
        *self.inner.state.write() = CowRpcRouterPeerState::Error;
    }

    fn send_unbind_req(&self, client_id: u32, server_id: u32, iface_id: u16) {
        let iface_def = CowRpcIfaceDef {
            id: iface_id,
            flags: COW_RPC_DEF_FLAG_EMPTY,
            ..Default::default()
        };

        let iface_defs = vec![iface_def];
        let cow_id = self.inner.cow_id;

        let remote_is_server = server_id == cow_id;
        let src_id = if remote_is_server { client_id } else { server_id };

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: if !remote_is_server { COW_RPC_FLAG_SERVER } else { 0 },
            src_id,
            dst_id: cow_id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        let _ = self.send_messages(CowRpcMessage::Unbind(header, msg));
    }

    fn send_messages(&self, msg: CowRpcMessage) -> Result<()> {
        self.inner.writer_sink.lock().start_send(msg).map(|_| ())
    }

    fn add_remote_bind(&self, remote_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        if self
            .inner
            .binds
            .contains_peer_bind(remote_id, client_id, server_id, iface_id)
            {
                return;
            }

        if let Err(e) = self
            .inner
            .binds
            .add_peer_bind(remote_id, client_id, server_id, iface_id)
            {
                error!("BindCollection returned error: {:?}", e);
            }
    }

    fn remove_remote_bind(&self, remote_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        if let Err(e) = self
            .inner
            .binds
            .remove_peer_bind(remote_id, client_id, server_id, iface_id)
            {
                error!("BindCollection returned error: {:?}", e);
            }
    }

    fn update_remote_bind_state(
        &self,
        remote_id: u32,
        client_id: u32,
        server_id: u32,
        iface_id: u16,
        new_state: CowRpcBindState,
    ) {
        if let Err(e) = self
            .inner
            .binds
            .update_peer_bind(remote_id, client_id, server_id, iface_id, new_state)
            {
                match new_state {
                    CowRpcBindState::Unbinding | CowRpcBindState::Unbound => {}
                    _ => error!("BindCollection returned error: {:?}", e),
                };
            }
    }

    fn add_bind(&self, client_id: u32, server_id: u32, iface_id: u16) {
        if self.inner.binds.contains(client_id, server_id, iface_id) {
            return;
        }

        if let Err(e) = self.inner.binds.add(client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn remove_bind(&self, client_id: u32, server_id: u32, iface_id: u16) {
        if let Err(e) = self.inner.binds.remove(client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn update_bind_state(&self, client_id: u32, server_id: u32, iface_id: u16, new_state: CowRpcBindState) {
        if let Err(e) = self.inner.binds.update(client_id, server_id, iface_id, new_state) {
            match new_state {
                CowRpcBindState::Unbinding | CowRpcBindState::Unbound => {}
                _ => error!("BindCollection returned error: {:?}", e),
            };
        }
    }

    fn clear_bind_contexts(&self) {
        let cow_id = self.inner.cow_id;

        match self.inner.binds.get_all() {
            Ok(binds) => {
                for (client_id, server_id, iface_id) in binds {
                    self.remove_bind(client_id, server_id, iface_id);
                    let remote_id = if cow_id == client_id { server_id } else { client_id };
                    self.remove_remote_bind(remote_id, client_id, server_id, iface_id);
                }
            }
            Err(e) => {
                error!("Unable to clear bind contexts. Got error: {:?}", e);
            }
        }
    }
}

impl Future for CowRpcRouterPeer {
    type Item = (u32, Option<CowRpcIdentity>);
    type Error = (u32, Option<CowRpcIdentity>, CowRpcError);

    fn poll(&mut self) -> std::result::Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        loop {
            match self.reader_stream.poll() {
                Ok(Async::Ready(Some(msg))) => {
                    self.process_msg(msg)
                        .map_err(|e| (self.inner.cow_id, self.identity.clone(), e))?;
                }
                Ok(Async::NotReady) => {
                    break; // nothing to do with that
                }
                Ok(Async::Ready(None)) => {
                    return Ok(Async::Ready((self.inner.cow_id, self.identity.clone()))); // means the transport is disconnected
                }
                Err(e) => {
                    return Err((self.inner.cow_id, self.identity.clone(), e));
                }
            }
        }
        {
            self.inner
                .writer_sink
                .lock()
                .poll_complete()
                .map_err(|e| (self.inner.cow_id, self.identity.clone(), e))?;
        }
        {
            match &*self.inner.state.read() {
                CowRpcRouterPeerState::Error => {
                    return Err((
                        self.inner.cow_id,
                        self.identity.clone(),
                        CowRpcError::Internal("An error occured while polling the peer connection".to_string()),
                    ));
                }
                CowRpcRouterPeerState::Terminated => {
                    return Ok(Async::Ready((self.inner.cow_id, self.identity.clone())));
                }
                _ => {}
            }
        }

        Ok(Async::NotReady)
    }
}

impl CowRpcRouterPeer {
    fn new(transport: CowRpcTransport, router: RouterShared) -> CowRpcRouterPeerHandshake {
        let mut transport = transport;
        let reader_stream = transport.message_stream();
        CowRpcRouterPeerHandshake {
            transport,
            reader_stream,
            router,
        }
    }

    fn process_msg(&mut self, msg: CowRpcMessage) -> Result<()> {
        match msg {
            CowRpcMessage::Handshake(hdr, msg) => {
                error!(
                    "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                    hdr, msg
                );
            }
            CowRpcMessage::Register(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_register_req(hdr, msg)?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Identity(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_identify_req(hdr, msg)?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Resolve(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_resolve_req(hdr, msg)?;
                } else {
                    error!(
                        "CowRpc Protocol Error: Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    );
                }
            }

            CowRpcMessage::Terminate(hdr) => {
                if !hdr.is_response() {
                    self.process_terminate_req(hdr)?;
                } else {
                    error!("CowRpc Protocol Error: Router can't process a response: hdr={:?}", hdr);
                }
            }

            CowRpcMessage::Verify(hdr, msg, payload) => {
                if !hdr.is_response() {
                    self.process_verify_req(hdr, msg, &payload)?;
                } else {
                    error!("CowRpc Protocol Error: Router can't process a response: hdr={:?}", hdr);
                }
            }

            msg => {
                self.router.process_msg(msg);
            }
        }

        Ok(())
    }

    fn process_register_req(&mut self, _: CowRpcHdr, msg: CowRpcRegisterMsg) -> Result<()> {
        let mut msg_clone = msg.clone();

        for mut iface in &mut msg_clone.ifaces {
            if let Err(e) = self.router.register_iface_def(&mut iface) {
                error!("Registering iface failed, {:?}", e);
                iface.flags = CowRpcErrorCode::Internal.into();
            }
        }

        self.send_register_rsp(msg_clone.ifaces)?;
        Ok(())
    }

    fn process_identify_req(&mut self, _: CowRpcHdr, msg: CowRpcIdentityMsg) -> Result<()> {
        let mut flag = CowRpcErrorCode::Success;
        {
            if self.identity.is_none() {
                let identity = {
                    // FIXME: This is a temporary fix until group identity are implemented, as discussed with fdubois
                    if msg.identity.eq("den") {
                        format!("den{}", self.router.inner.id)
                    } else {
                        msg.identity.clone()
                    }
                    // FIXME: End
                };

                let identity = CowRpcIdentity {
                    typ: msg.typ.clone(),
                    name: identity.clone(),
                };
                let cache = &self.router.inner.cache;
                let cow_id = self.inner.cow_id;
                match cache.add_cow_identity(&identity, cow_id) {
                    Ok(_) => {
                        self.identity = Some(identity);
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
                flag = CowRpcErrorCode::Proto;
            }
        }

        self.send_identify_rsp(flag.into(), msg)?;
        Ok(())
    }

    fn process_verify_req(&mut self, _: CowRpcHdr, msg: CowRpcVerifyMsg, payload: &[u8]) -> Result<()> {

        let (rsp, identity_opt) = if let Some(ref cb) = *self.router.inner.verify_identity_cb.read() {
            (**cb)(payload)
        } else {
            (b"HTTP/1.1 501 NOT IMPLEMENTED\r\n\r\n".to_vec(), None)
        };

        let mut flag = CowRpcErrorCode::Success;
        {
            if let Some(identity) = identity_opt {
                let identity = CowRpcIdentity {
                    typ: CowRpcIdentityType::UPN,
                    name: identity.clone(),
                };

                let cache = &self.router.inner.cache;
                let cow_id = self.inner.cow_id;
                match cache.add_cow_identity(&identity, cow_id) {
                    Ok(_) => {
                        self.identity = Some(identity);
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

        self.send_verify_rsp(flag.into(), msg, rsp)?;
        Ok(())
    }

    fn process_resolve_req(&mut self, header: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
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

        self.send_resolve_rsp(flag, msg_clone)?;
        Ok(())
    }

    fn process_terminate_req(&mut self, _: CowRpcHdr) -> Result<()> {
        *self.inner.state.write() = CowRpcRouterPeerState::Terminated;
        self.send_terminate_rsp()?;
        Ok(())
    }

    fn send_handshake_rsp(&mut self, flag: u16) -> Result<()> {
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

        self.send_messages(CowRpcMessage::Handshake(header, msg))?;
        Ok(())
    }

    fn send_register_rsp(&mut self, ifaces: Vec<CowRpcIfaceDef>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_REGISTER_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        let msg = CowRpcRegisterMsg { ifaces };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Register(header, msg))?;
        Ok(())
    }

    fn send_identify_rsp(&mut self, flag: u16, msg: CowRpcIdentityMsg) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_IDENTIFY_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Identity(header, msg))?;
        Ok(())
    }

    fn send_verify_rsp(&mut self, flag: u16, msg: CowRpcVerifyMsg, payload: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size() + payload.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_messages(CowRpcMessage::Verify(header, msg, payload))?;
        Ok(())
    }

    fn send_resolve_rsp(&mut self, flag: u16, msg: CowRpcResolveMsg) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESOLVE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size() + msg.get_size(header.flags);
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Resolve(header, msg))?;
        Ok(())
    }

    fn send_terminate_rsp(&mut self) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.router.inner.id,
            dst_id: self.inner.cow_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Terminate(header))?;
        Ok(())
    }

    fn send_messages(&mut self, msg: CowRpcMessage) -> Result<()> {
        self.inner.writer_sink.lock().start_send(msg).map(|_| ())
    }

    pub fn clean_identity(&mut self) {
        let peer_cow_id = self.inner.cow_id;

        if let Some(ref identity) = self.identity {
            let res = match self.router.inner.cache.get_cow_identity_peer_addr(identity) {
                Ok(opt_cow_id) => {
                    if let Some(cow_id) = opt_cow_id {
                        if cow_id == peer_cow_id {
                            self.router.inner.cache.remove_cow_identity(identity, peer_cow_id)
                        } else {
                            Err(CowRpcError::Internal(format!(
                                "Identity {} already belongs to another peer {}",
                                identity.name, cow_id
                            )))
                        }
                    } else {
                        self.router.inner.cache.remove_cow_identity(identity, peer_cow_id)
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

        self.identity = None;
    }
}

struct RouterBindCollection {
    pub peer_id: u32,
    cache: Cache,
}

impl RouterBindCollection {
    const COW_RPC_BIND_CONTEXT_SET: &'static str = "rpc_bind_context";
    const COW_RPC_BIND_STATE_HASHSET: &'static str = "rpc_bind_state";

    pub fn new(id: u32, cache: Cache) -> Self {
        RouterBindCollection {
            peer_id: id,
            cache: cache.clone(),
        }
    }

    pub fn get_all(&self) -> Result<Vec<(u32, u32, u16)>> {
        self.get_all_peer_binds(self.peer_id)
    }

    pub fn contains(&self, client_id: u32, server_id: u32, iface_id: u16) -> bool {
        self.contains_peer_bind(self.peer_id, client_id, server_id, iface_id)
    }

    pub fn add(&self, client_id: u32, server_id: u32, iface_id: u16) -> Result<()> {
        self.add_peer_bind(self.peer_id, client_id, server_id, iface_id)
    }

    pub fn update(&self, client_id: u32, server_id: u32, iface_id: u16, new_state: CowRpcBindState) -> Result<bool> {
        self.update_peer_bind(self.peer_id, client_id, server_id, iface_id, new_state)
    }

    pub fn remove(&self, client_id: u32, server_id: u32, iface_id: u16) -> Result<()> {
        self.remove_peer_bind(self.peer_id, client_id, server_id, iface_id)
    }

    pub fn get_all_peer_binds(&self, peer_id: u32) -> Result<Vec<(u32, u32, u16)>> {
        let vec = self
            .cache
            .set_members(&Self::cow_rpc_bind_context_set_key(peer_id))?
            .iter()
            .filter_map(|s| Self::read_key(&s))
            .collect::<Vec<_>>();
        Ok(vec)
    }

    pub fn contains_peer_bind(&self, peer_id: u32, client_id: u32, server_id: u32, iface_id: u16) -> bool {
        let bind_key = Self::create_key(client_id, server_id, iface_id);
        if let Ok(ismember) = self
            .cache
            .set_ismember(&Self::cow_rpc_bind_context_set_key(peer_id), bind_key)
            {
                ismember
            } else {
            false
        }
    }

    pub fn add_peer_bind(&self, peer_id: u32, client_id: u32, server_id: u32, iface_id: u16) -> Result<()> {
        let bind_key = Self::create_key(client_id, server_id, iface_id);
        if self
            .cache
            .set_ismember(&Self::cow_rpc_bind_context_set_key(peer_id), bind_key.clone())?
            {
                return Err(CowRpcError::CowRpcFailure(CowRpcErrorCode::AlreadyBound));
            }

        self.cache
            .set_add(&Self::cow_rpc_bind_context_set_key(peer_id), &[bind_key.clone()])?;
        self.cache.hash_set(
            &Self::cow_rpc_bind_state_hashset_key(peer_id),
            &bind_key,
            CowRpcBindState::Bound.get_name(),
        )?;

        Ok(())
    }

    pub fn update_peer_bind(
        &self,
        peer_id: u32,
        client_id: u32,
        server_id: u32,
        iface_id: u16,
        new_state: CowRpcBindState,
    ) -> Result<bool> {
        let bind_key = Self::create_key(client_id, server_id, iface_id);
        if let Some(current_state) = self
            .cache
            .hash_get::<String>(&Self::cow_rpc_bind_state_hashset_key(peer_id), &bind_key)?
            {
                let current_state = CowRpcBindState::from_name(&current_state);
                if current_state == new_state {
                    return Ok(true);
                }

                let mut success = false;
                match new_state {
                    CowRpcBindState::Initial => {
                        if current_state == CowRpcBindState::Initial {
                            success = true;
                        }
                    }
                    CowRpcBindState::Binding => {
                        if current_state == CowRpcBindState::Initial || current_state == CowRpcBindState::Binding {
                            success = true;
                        }
                    }
                    CowRpcBindState::Bound => {
                        if current_state == CowRpcBindState::Binding || current_state == CowRpcBindState::Bound {
                            success = true;
                        }
                    }
                    CowRpcBindState::Unbinding => {
                        if current_state == CowRpcBindState::Binding
                            || current_state == CowRpcBindState::Bound
                            || current_state == CowRpcBindState::Unbinding
                            {
                                success = true;
                            }
                    }
                    CowRpcBindState::Unbound => {
                        if current_state == CowRpcBindState::Bound
                            || current_state == CowRpcBindState::Unbinding
                            || current_state == CowRpcBindState::Unbound
                            {
                                success = true;
                            }
                    }
                    CowRpcBindState::Failure => {
                        success = true;
                    }
                }

                if success {
                    debug!(
                        "BindCtx Transition: {} -> {} (clientId={} - serverId={} - ifaceId={})",
                        current_state.get_name(),
                        new_state.get_name(),
                        client_id,
                        server_id,
                        iface_id
                    );
                    self.cache.hash_set(
                        &Self::cow_rpc_bind_state_hashset_key(peer_id),
                        &bind_key,
                        new_state.get_name(),
                    )?;
                    return Ok(true);
                } else {
                    debug!("BindCtx Transition failed. State has not been changed: {} -> {} (clientId={} - serverId={} - ifaceId={})", current_state.get_name(), new_state.get_name(), client_id, server_id, iface_id);
                }
            }

        Ok(false)
    }

    pub fn remove_peer_bind(&self, peer_id: u32, client_id: u32, server_id: u32, iface_id: u16) -> Result<()> {
        let bind_key = Self::create_key(client_id, server_id, iface_id);

        if !self
            .cache
            .set_ismember(&Self::cow_rpc_bind_context_set_key(peer_id), bind_key.clone())?
            {
                return Ok(());
            }

        self.cache
            .set_rem(&Self::cow_rpc_bind_context_set_key(peer_id), bind_key.clone())?;
        self.cache
            .hash_delete(&Self::cow_rpc_bind_state_hashset_key(peer_id), &[bind_key.as_ref()])?;

        Ok(())
    }

    fn create_key(client_id: u32, server_id: u32, iface_id: u16) -> String {
        format!("{}:{}:{}", client_id, server_id, iface_id)
    }

    fn read_key(key: &str) -> Option<(u32, u32, u16)> {
        let bind_info = key.split(':').collect::<Vec<_>>();
        if bind_info.len() == 3 {
            let src_id: u32 = bind_info[0].parse().unwrap_or(0);
            let dst_id: u32 = bind_info[1].parse().unwrap_or(0);
            let iface_id: u16 = bind_info[2].parse().unwrap_or(0);

            if src_id != 0 && dst_id != 0 && iface_id != 0 {
                return Some((src_id, dst_id, iface_id));
            }
        }

        None
    }

    #[inline]
    fn cow_rpc_bind_context_set_key(peer_id: u32) -> String {
        format!("{}{}", Self::COW_RPC_BIND_CONTEXT_SET, peer_id)
    }

    #[inline]
    fn cow_rpc_bind_state_hashset_key(peer_id: u32) -> String {
        format!("{}{}", Self::COW_RPC_BIND_STATE_HASHSET, peer_id)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CowRpcBindState {
    Initial,
    Binding,
    Bound,
    Unbinding,
    Unbound,
    Failure,
}

impl CowRpcBindState {
    fn get_name(&self) -> &str {
        match self {
            CowRpcBindState::Initial => "Initial",
            CowRpcBindState::Binding => "Binding",
            CowRpcBindState::Bound => "Bound",
            CowRpcBindState::Unbinding => "Unbinding",
            CowRpcBindState::Unbound => "Unbound",
            CowRpcBindState::Failure => "Failure",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "Initial" => CowRpcBindState::Initial,
            "Binding" => CowRpcBindState::Binding,
            "Bound" => CowRpcBindState::Bound,
            "Unbinding" => CowRpcBindState::Unbinding,
            "Unbound" => CowRpcBindState::Unbound,
            "Failure" => CowRpcBindState::Failure,
            _ => CowRpcBindState::Failure,
        }
    }
}

#[derive(Clone)]
struct RouterProc {
    id: u16,
    name: String,
}

#[derive(Clone)]
struct RouterIface {
    id: u16,
    name: String,
    procs: Arc<RwLock<HashMap<String, RouterProc>>>,
}

impl std::fmt::Display for RouterIface {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Iface \"{}\" id: {}", self.name, self.id)?;
        writeln!(f, "|")?;
        for procedure in self.procs.read().iter() {
            writeln!(f, "    Proc \"{}\" id: {}", procedure.1.name, procedure.1.id)?;
        }
        writeln!(f, "|")?;
        Ok(())
    }
}

impl RouterIface {
    pub fn contains_proc(&self, name: &str) -> bool {
        self.procs.read().contains_key(name)
    }
}

struct RouterIfaceCollection {
    cache: Cache,
    iface_list: RwLock<HashMap<String, RouterIface>>,
}

impl RouterIfaceCollection {
    const COW_IFACE_HASH_SET: &'static str = "cow_iface";
    const COW_IFACE_PROC_FIELD: &'static str = "cow_proc";
    const COW_IFACES_SET: &'static str = "cow_ifaces";
    const COW_PROCS_SET: &'static str = "cow_procs";

    fn new(cache: Cache) -> Self {
        let coll = RouterIfaceCollection {
            cache,
            iface_list: RwLock::new(HashMap::new()),
        };

        if let Err(e) = coll.load_from_cache() {
            error!("Unable to load CowRpcIfaces from cache: {}", e);
        }

        coll
    }

    fn load_from_cache(&self) -> Result<()> {
        let mut list_mut = self.iface_list.write();

        let members = self.cache.set_members(Self::COW_IFACES_SET)?;

        for member in members {
            let id = self
                .cache
                .hash_get(&Self::iface_hash_set_key(&member), "id")?
                .unwrap_or(0);
            let name = member.clone();
            let mut procs = HashMap::new();
            let procs_name = self.cache.set_members(&Self::proc_set_key(&member))?;

            for pname in procs_name {
                let proc_id: u16 = self
                    .cache
                    .hash_get(&Self::iface_hash_set_key(&name), &Self::iface_proc_field_key(&pname))?
                    .unwrap_or(0);
                procs.insert(
                    pname.clone(),
                    RouterProc {
                        id: proc_id,
                        name: pname.clone(),
                    },
                );
            }

            let procs = Arc::new(RwLock::new(procs));

            let iface = RouterIface { id, name, procs };

            info!("Loaded interface from cache : \n {}", iface);

            list_mut.insert(member.clone(), iface);
        }

        Ok(())
    }

    pub fn add(&self, iface_def: &mut CowRpcIfaceDef) -> Result<()> {
        let mut new_iface = false;
        self.add_to_cache(iface_def)?;

        let CowRpcIfaceDef {
            ref id,
            flags: _iflags,
            name: ref iface_name,
            ref procs,
        } = iface_def;

        let mut list_mut = self.iface_list.write();

        if !list_mut.contains_key(iface_name) {
            new_iface = true;
            list_mut.insert(
                iface_name.clone(),
                RouterIface {
                    id: *id,
                    name: iface_name.clone(),
                    procs: Arc::new(RwLock::new(HashMap::new())),
                },
            );
        }

        let router_iface = list_mut.get_mut(iface_name)
            .expect("This should never happend since there is a check to add the iface to the hashmap if it does not already exists");

        for p in procs {
            let CowRpcProcDef {
                id: ref proc_id,
                flags: _pflags,
                name: ref proc_name,
            } = p;

            if router_iface.contains_proc(proc_name) {
                continue;
            } else {
                let mut proc_list = router_iface.procs.write();
                proc_list.insert(
                    proc_name.clone(),
                    RouterProc {
                        id: *proc_id,
                        name: proc_name.clone(),
                    },
                );
            }
        }

        if new_iface {
            info!("Added interface : \n {}", router_iface);
        }

        Ok(())
    }

    fn add_to_cache(&self, iface_def: &mut CowRpcIfaceDef) -> Result<()> {
        let CowRpcIfaceDef {
            ref mut id,
            flags: _iflags,
            name: ref iface_name,
            ref mut procs,
        } = iface_def;

        if self.cache.set_ismember(Self::COW_IFACES_SET, iface_name)? {
            *id = self
                .cache
                .hash_get(&Self::iface_hash_set_key(iface_name), "id")?
                .unwrap_or(0);
            for p in procs {
                let CowRpcProcDef {
                    id: ref mut proc_id,
                    flags: _pflags,
                    name: ref proc_name,
                } = p;

                if self.cache.set_ismember(&Self::proc_set_key(iface_name), proc_name)? {
                    *proc_id = self
                        .cache
                        .hash_get(
                            &Self::iface_hash_set_key(iface_name),
                            &Self::iface_proc_field_key(proc_name),
                        )?.unwrap_or(0);
                } else {
                    let _ = self.cache.set_add(&Self::proc_set_key(iface_name), &[proc_name])?;
                    *proc_id = self.cache.set_card(&Self::proc_set_key(iface_name))? as u16;
                    let _ = self.cache.hash_set(
                        &Self::iface_hash_set_key(iface_name),
                        &Self::iface_proc_field_key(proc_name),
                        proc_id.clone(),
                    )?;
                }
            }
        } else {
            // Add name to set in redis
            let _ = self.cache.set_add(Self::COW_IFACES_SET, &[iface_name])?;
            *id = self.cache.set_card(Self::COW_IFACES_SET)? as u16;

            // Add values to hashset in redis
            let _ = self
                .cache
                .hash_set(&Self::iface_hash_set_key(iface_name), "id", id.clone())?;

            for p in procs {
                let CowRpcProcDef {
                    id: ref mut proc_id,
                    flags: _pflags,
                    name: ref proc_name,
                } = p;

                let _ = self.cache.set_add(&Self::proc_set_key(iface_name), &[proc_name])?;
                *proc_id = self.cache.set_card(&Self::proc_set_key(iface_name))? as u16;
                let _ = self.cache.hash_set(
                    &Self::iface_hash_set_key(iface_name),
                    &Self::iface_proc_field_key(proc_name),
                    proc_id.clone(),
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn iface_hash_set_key(iface_name: &str) -> String {
        format!("{}:{}", Self::COW_IFACE_HASH_SET, iface_name)
    }

    #[inline]
    fn iface_proc_field_key(proc_name: &str) -> String {
        format!("{}:{}", Self::COW_IFACE_PROC_FIELD, proc_name)
    }

    #[inline]
    fn proc_set_key(iface_name: &str) -> String {
        format!("{}:{}", Self::COW_PROCS_SET, iface_name)
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
            Ok(true) => info!("Identity added in router cache: den_id: {}, cow_id: {:#010X}", identity.name, peer_id),
            Ok(false) => warn!("Identity {} was updated and now belongs to peer {:#010X}", identity.name, peer_id),
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
            Ok(_) => info!("Identity removed from router cache: den_id: {}, cow_id: {:#010X}", identity.name, peer_id),
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
