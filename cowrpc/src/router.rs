use std;
use std::{cell::RefCell, collections::HashMap, fmt, ops::Deref, rc::Rc, sync::Arc};

use mio::{Events, Poll, PollOpt, Ready, Token};
use mouscache;
use mouscache::Cache;
use mouscache::CacheFunc;
use mouscache_derive::Cacheable;
use parking_lot::{RwLock, RwLockReadGuard};
use rand;

use crate::transport::MessageInterceptor;
use crate::transport::{sync::{ListenerBuilder, CowRpcListener, CowRpcTransport, adaptor::Adaptor}, TransportAdapter};
use crate::CowRpcMessageInterceptor;
use crate::TlsOptions;
use super::{CowRpcIdentityType, CowRpcMessage};
use crate::cancel_event::CancelEventHandle;
use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::proto;
use crate::proto::*;

const CANCEL_EVENT: Token = Token(std::usize::MAX - 1);
const ADAPTER_EVENT: Token = Token(std::usize::MAX - 2);

const PEER_CONNECTION_GRACE_PERIOD: u64 = 10;

pub const ALLOCATED_COW_ID_SET: &str = "allocated_cow_id";
pub const COW_ID_RECORDS: &str = "cow_address_records";
pub const IDENTITY_RECORDS: &str = "identities_records";

type IdentityVerificationCallback = Fn(&[u8]) -> (Vec<u8>, Option<String>);

pub struct CowRpcRouter {
    id: u32,
    listener: CowRpcListener,
    multi_router_peer: Option<RefCell<CowRpcRouterPeer>>,
    shared: RouterShared,
    adaptor: Adaptor,
    on_peer_connection_callback: Option<Box<Fn(&CowRpcRouterPeer)>>,
    on_peer_disconnection_callback: Option<Box<Fn(&CowRpcRouterPeer)>>,
}

impl CowRpcRouter {
    pub fn new(url: &str, listener_tls_options: Option<TlsOptions>) -> Result<CowRpcRouter> {
        let id: u32 = 0;
        let mut l_builder = ListenerBuilder::from_uri(url)?;

        if let Some(tls) = listener_tls_options {
            l_builder = l_builder.with_ssl(tls);
        }

        let listener = l_builder.build()?;

        let router = CowRpcRouter {
            id,
            listener,
            multi_router_peer: None,
            shared: RouterShared::new(id),
            adaptor: Adaptor::new(),
            on_peer_connection_callback: None,
            on_peer_disconnection_callback: None,
        };

        Ok(router)
    }

    pub fn new2(
        id: u16,
        cache: Cache,
        url: &str,
        listener_tls_options: Option<TlsOptions>,
    ) -> Result<CowRpcRouter> {
        let mut l_builder = ListenerBuilder::from_uri(url)?;

        if let Some(tls) = listener_tls_options {
            l_builder = l_builder.with_ssl(tls);
        }

        let listener = l_builder.build()?;

        let router_id = u32::from(id) << 16;
        let router = CowRpcRouter {
            id: router_id,
            listener,
            multi_router_peer: None,
            shared: RouterShared::new2(router_id, cache),
            adaptor: Adaptor::new(),
            on_peer_connection_callback: None,
            on_peer_disconnection_callback: None,
        };

        Ok(router)
    }

    pub fn on_peer_connection_callback<F: 'static + Fn(&CowRpcRouterPeer)>(&mut self, callback: F) {
        self.on_peer_connection_callback = Some(Box::new(callback));
    }

    pub fn on_peer_disconnection_callback<F: 'static + Fn(&CowRpcRouterPeer)>(&mut self, callback: F) {
        self.on_peer_disconnection_callback = Some(Box::new(callback));
    }

    pub fn verify_identity_callback<F: 'static + Fn(&[u8]) -> (Vec<u8>, Option<String>)>(&mut self, callback: F) {
        self.shared.verify_identity_cb = Some(Arc::new(Box::new(callback)));
    }

    pub fn set_msg_interceptor<T: 'static + Send + Sync + Clone>(&mut self, interceptor: CowRpcMessageInterceptor<T>) {
        let peer = CowRpcRouterPeer {
            id: 0,
            identity: None,
            token: Token(0),
            transport: CowRpcTransport::from_interceptor(interceptor.clone_boxed()),
            event_register_needed: false,
            state: CowRpcRouterPeerState::Connected,
            router: self.shared.clone(),
            binds: RouterBindCollection::new(0, self.shared.inner.cache.get_raw_cache().clone()),
        };
        self.multi_router_peer = Some(RefCell::new(peer));
        self.listener.set_msg_interceptor(Box::new(interceptor));
    }

    pub fn get_msg_injector(&self) -> Adaptor {
        self.adaptor.clone()
    }

    pub fn get_id(&self) -> u32 {
        self.shared.inner.id
    }

    pub fn run(&mut self, cancel_handle: Option<&CancelEventHandle>) -> Result<()> {
        let mut poll = Poll::new()?;

        poll.register(
            &self.listener,
            Token(self.id as usize),
            Ready::readable(),
            PollOpt::edge(),
        )?;

        if cancel_handle.is_some() {
            poll.register(cancel_handle.unwrap(), CANCEL_EVENT, Ready::readable(), PollOpt::edge())?;
        }

        {
            poll.register(&self.adaptor, ADAPTER_EVENT, Ready::readable(), PollOpt::edge())?;
        }

        let mut events = Events::with_capacity(1024);

        info!("Listening for clients");

        loop {
            poll.poll(&mut events, Some(std::time::Duration::from_secs(2)))
                .map_err(|e| CowRpcError::Internal(e.to_string()))?;
            for event in events.iter() {
                match event.token() {
                    CANCEL_EVENT => {
                        let peer_ids = {
                            let peers = self.shared.inner.peers.read();
                            peers.keys().cloned().collect::<Vec<_>>()
                        };
                        self.clean_up_connection(peer_ids);
                        return Ok(());
                    }
                    ADAPTER_EVENT => loop {
                        let opt_msg = {
                            match self.adaptor.get_next_message() {
                                Ok(opt) => opt,
                                Err(_e) => {
                                    warn!("something went wrong with the adapter");
                                    None
                                }
                            }
                        };

                        match opt_msg {
                            Some(msg) => {
                                debug!("Received injected message via adapter {}", msg.get_msg_info());
                                self.process_msg(msg.get_src_id(), msg);
                            }
                            None => break,
                        }
                    },
                    _ => {
                        self.process_event(&mut poll, event.token(), event.readiness());
                    }
                }
            }

            self.reregister_events(&mut poll);
        }
    }

    fn reregister_events(&mut self, poll: &mut Poll) {
        let mut peers_to_remove = Vec::new();

        {
            let peers = self.shared.inner.peers.read();
            for peer_id in peers.keys() {
                if let Some(r_peer) = peers.get(&peer_id) {
                    let mut peer = r_peer.write();

                    if !peer.is_valid() {
                        peers_to_remove.push(peer.id);
                    } else if peer.event_register_needed() {
                        if let Err(e) = peer.reregister_events(poll) {
                            warn!("Reregister failed: {}", e);
                            peers_to_remove.push(peer.id);
                        }
                    }
                }
            }
        }

        if !peers_to_remove.is_empty() {
            trace!("Removing {} connection.", peers_to_remove.len());
            self.clean_up_connection(peers_to_remove);
        }
    }

    fn clean_up_connection(&mut self, peers_to_remove: Vec<u32>) {
        let mut peers = self.shared.inner.peers.write();

        //Remove the peer and unbind all bind context between this peer and others
        for peer_id in peers_to_remove {
            match peers.remove(&peer_id) {
                Some(ref peer_ref) => {
                    let mut peer = peer_ref.write();

                    if let Some(ref callback) = self.on_peer_disconnection_callback {
                        callback(&peer);
                    }

                    peer.clean_identity();

                    if let Ok(binds) = peer.binds.get_all() {
                        for (client_id, server_id, iface_id) in binds {
                            if let Ok(true) =
                            peer.binds
                                .update(client_id, server_id, iface_id, CowRpcBindState::Unbinding)
                                {
                                    let remote_id = if client_id == peer.id { server_id } else { client_id };
                                    if let Some(remote_ref) = peers.get_mut(&remote_id) {
                                        let mut remote = remote_ref.write();
                                        remote.send_unbind_req(client_id, server_id, iface_id);
                                    } else {
                                        if let Some(ref multi_peer) = self.multi_router_peer {
                                            let mut peer = multi_peer.borrow_mut();
                                            peer.id = remote_id;
                                            peer.binds.peer_id = remote_id;
                                            peer.send_unbind_req(client_id, server_id, iface_id);
                                        } else {
                                            warn!("No peer found with cow id {}", remote_id);
                                        }
                                    }
                                }
                        }
                    }

                    peer.clear_bind_contexts();

                    {
                        if let Err(e) = self
                            .shared
                            .inner
                            .cache
                            .get_raw_cache()
                            .set_rem(ALLOCATED_COW_ID_SET, peer_id)
                            {
                                error!("Unable to remove allocated cow id {}, got error: {:?}", peer_id, e);
                            }
                    }

                    trace!("Peer {} removed", peer_id);

                    // since there is only one peer per connection, ensure the connection close gracefully
                    let _ = peer.transport.shutdown();
                }
                None => {
                    warn!("Unable to remove peer {}", peer_id);
                }
            }
        }
    }

    fn process_event(&mut self, poll: &mut Poll, token: Token, event: Ready) {
        let peer_id = token.0 as u32;

        // WRITE event
        if event.is_writable() {
            self.process_write_event(peer_id);
        }

        // READ event
        if event.is_readable() {
            self.process_read_event(poll, peer_id);
        }

        if peer_id != self.id {
            if let Some(peer) = self.find_peer_by_id(peer_id) {
                peer.write().set_event_register_needed();
            }
        }
    }

    fn process_write_event(&mut self, peer_id: u32) {
        assert!(peer_id != self.get_id(), "Received writable event for Server");

        match self.find_peer_by_id(peer_id) {
            Some(peer) => {
                let mut peer_writer = peer.write();
                peer_writer.process_write_event().unwrap_or_else(|e| {
                    warn!("Write event failed for peer ID {}: {}", peer_id, e);
                    peer_writer.set_connection_error();
                });
            }
            None => {
                warn!("Write event failed, no peer ID {}", peer_id);
            }
        }
    }

    fn process_read_event(&mut self, poll: &mut Poll, peer_id: u32) {
        if peer_id == self.id {
            self.accept_new_connection(poll);
        } else {
            {
                match self.find_peer_by_id(peer_id) {
                    Some(peer_ref) => {
                        let mut peer = peer_ref.write();
                        peer.process_read_event().unwrap_or_else(|e| {
                            if peer.state.is_connected() {
                                warn!("Read event failed for peer ID {}: {}", peer_id, e);
                            } else {
                                trace!("Read event failed for peer {} before handshake : {}", peer_id, e);
                            }
                            peer.set_connection_error();
                        });
                    }
                    None => {
                        warn!("Read event failed, no peer ID {}", peer_id);
                    }
                }
            }

            // Process all messages if full messages have been received
            loop {
                let msg = self.find_peer_by_id(peer_id).and_then(|peer_ref| {
                    let mut peer = peer_ref.write();
                    peer.get_next_message().unwrap_or_else(|e| {
                        warn!("Get next message failed for peer ID {}: {}", peer_id, e);
                        peer.set_connection_error();
                        None
                    })
                });

                if let Some(m) = msg {
                    self.process_msg(peer_id, m);
                } else {
                    // No more message to process
                    break;
                }
            }
        }
    }

    fn process_msg(&mut self, peer_id: u32, msg: CowRpcMessage) {
        if msg.is_handled_by_router() {
            if let Some(peer_guard) = self.find_peer_by_id(peer_id) {
                let mut peer = peer_guard.write();
                peer.process_msg(msg).unwrap_or_else(|e| {
                    warn!("Process message failed for peer ID {}: {}", peer_id, e);
                    peer.set_connection_error();
                });
            }
        } else {
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
                match self.find_peer_by_id(header.dst_id) {
                    Some(peer_ref) => {
                        let mut peer = peer_ref.write();
                        peer.add_bind(client_id, server_id, iface_id);
                        peer.add_remote_bind(header.src_id, client_id, server_id, iface_id);
                    }
                    None => {}
                }
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
        match self.find_peer_by_id(local_peer_id) {
            Some(peer_ref) => {
                let mut peer = peer_ref.write();
                peer.update_bind_state(client_id, server_id, iface_id, new_state);
                let remote_id = if local_peer_id == client_id {
                    server_id
                } else {
                    client_id
                };
                peer.update_remote_bind_state(remote_id, client_id, server_id, iface_id, new_state);
            }
            None => {}
        }
    }

    fn remove_bind(&mut self, local_peer_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        // Remove the bind
        match self.find_peer_by_id(local_peer_id) {
            Some(peer_ref) => {
                let mut peer = peer_ref.write();
                peer.remove_bind(client_id, server_id, iface_id);
                let remote_id = if local_peer_id == client_id {
                    server_id
                } else {
                    client_id
                };
                peer.remove_remote_bind(remote_id, client_id, server_id, iface_id);
            }
            None => {}
        }
    }

    fn accept_new_connection(&mut self, poll: &mut Poll) {
        loop {
            let transport = match self.listener.accept() {
                Some(t) => t,
                None => return,
            };

            let id: u32 = self.generate_peer_id();
            let mut peer = CowRpcRouterPeer::new(id, transport, self.shared.clone());

            if let Err(e) = peer.register_events(poll) {
                error!("Failed to register peer ID {} with poller: {}", id, e);
                return;
            }

            if let Some(ref callback) = self.on_peer_connection_callback {
                callback(&peer);
            }

            self.shared.inner.peers.write().insert(id, RwLock::new(peer));
        }
    }

    fn generate_peer_id(&self) -> u32 {
        loop {
            let id = rand::random::<u16>();

            //0 is not accepted as peer_id
            if id != 0 {
                let peer_id = self.id | u32::from(id);
                if let Ok(false) = self
                    .shared
                    .inner
                    .cache
                    .get_raw_cache()
                    .set_ismember(ALLOCATED_COW_ID_SET, peer_id)
                    {
                        if self
                            .shared
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

    fn find_peer_by_id(&mut self, id: u32) -> Option<PeerGuard> {
        let peers = self.shared.inner.peers.read();
        if peers.get(&id).is_some() {
            Some(PeerGuard { id, lock_guard: peers })
        } else {
            None
        }
    }

    fn forward_msg(&mut self, msg: CowRpcMessage) {
        let dst_id = msg.get_dst_id();

        {
            if let Some(peer_ref) = self.find_peer_by_id(dst_id) {
                let mut peer = peer_ref.write();
                if let Err(e) = peer.send_messages(msg.clone()) {
                    warn!("Send message to peer ID {} failed: {}", dst_id, e);
                    peer.set_connection_error();
                } else {
                    return;
                }
            }
        }

        if (dst_id & 0xFFFF_0000) != self.id {
            if let Some(ref mut multi_peer) = self.multi_router_peer {
                let mut peer = multi_peer.borrow_mut();
                peer.id = dst_id;
                peer.binds.peer_id = dst_id;

                if peer.send_messages(msg.clone()).is_ok() {
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
                    self.send_call_result_failure(&header, &msg, CowRpcErrorCode::Unreachable.into());
                }
                _ => {
                    // To answer other messages, we just swap src-dst and we set the flag as response + failure.
                    let src_id = msg.get_src_id();

                    {
                        if let Some(peer_ref) = self.find_peer_by_id(src_id) {
                            let mut peer = peer_ref.write();

                            let mut msg_clone = msg.clone();
                            msg_clone.swap_src_dst();
                            let flag: u16 = CowRpcErrorCode::Unreachable.into();
                            msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                            peer.send_messages(msg_clone).unwrap_or_else(|e| {
                                warn!("Send message to peer ID {} failed: {}", src_id, e);
                                peer.set_connection_error();
                            });

                            return;
                        }
                    }

                    if let Some(ref mut multi_peer) = self.multi_router_peer {
                        let mut peer = multi_peer.borrow_mut();
                        peer.id = src_id;
                        peer.binds.peer_id = src_id;

                        let mut msg_clone = msg.clone();
                        msg_clone.swap_src_dst();
                        let flag: u16 = CowRpcErrorCode::Unreachable.into();
                        msg_clone.add_flag(COW_RPC_FLAG_RESPONSE | flag);

                        let _ = peer.send_messages(msg_clone);
                    }
                }
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

        {
            if let Some(peer_ref) = self.find_peer_by_id(dst_id) {
                let mut peer = peer_ref.write();

                peer.send_messages(CowRpcMessage::Result(header, msg, Vec::new()))
                    .unwrap_or_else(|e| {
                        warn!("Send message to peer ID {} failed: {}", header.src_id, e);
                        peer.set_connection_error();
                    });

                return;
            }
        }

        if let Some(ref mut multi_peer) = self.multi_router_peer {
            let mut peer = multi_peer.borrow_mut();
            peer.id = dst_id;
            peer.binds.peer_id = dst_id;

            let _ = peer.send_messages(CowRpcMessage::Result(header, msg, Vec::new()));
        }
    }
}

#[derive(Clone)]
struct RouterShared {
    pub inner: Arc<Inner>,
    pub verify_identity_cb: Option<Arc<Box<IdentityVerificationCallback>>>,
}

impl RouterShared {
    fn new(id: u32) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new(id)),
            verify_identity_cb: None,
        }
    }

    fn new2(id: u32, cache: Cache) -> RouterShared {
        RouterShared {
            inner: Arc::new(Inner::new2(id, cache)),
            verify_identity_cb: None,
        }
    }

    fn register_iface_def(&self, iface_def: &mut CowRpcIfaceDef) -> Result<()> {
        self.inner.ifaces.add(iface_def)
    }
}

struct Inner {
    id: u32,
    peers: RwLock<HashMap<u32, RwLock<CowRpcRouterPeer>>>,
    ifaces: RouterIfaceCollection,
    cache: RouterCache,
}

impl Inner {
    fn new(id: u32) -> Inner {
        let cache = mouscache::memory();
        Inner {
            id,
            peers: RwLock::new(HashMap::new()),
            ifaces: RouterIfaceCollection::new(cache.clone()),
            cache: RouterCache::new(cache),
        }
    }

    fn new2(id: u32, cache: Cache) -> Inner {
        Inner {
            id,
            peers: RwLock::new(HashMap::new()),
            ifaces: RouterIfaceCollection::new(cache.clone()),
            cache: RouterCache::new(cache),
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

pub struct CowRpcRouterPeer {
    id: u32,
    identity: Option<CowRpcIdentity>,
    token: Token,
    transport: CowRpcTransport,
    event_register_needed: bool,
    state: CowRpcRouterPeerState,

    router: RouterShared,

    binds: RouterBindCollection,
}

impl CowRpcRouterPeer {
    fn new(id: u32, transport: CowRpcTransport, router: RouterShared) -> CowRpcRouterPeer {
        let cache_clone = router.inner.cache.get_raw_cache().clone();
        CowRpcRouterPeer {
            id,
            identity: None,
            token: Token(id as usize),
            transport,
            event_register_needed: true,
            router,
            binds: RouterBindCollection::new(id, cache_clone),
            state: CowRpcRouterPeerState::Initial,
        }
    }

    fn register_events(&mut self, poll: &mut Poll) -> Result<()> {
        poll.register(
            &self.transport,
            Token(self.id as usize),
            self.transport.get_interest(),
            PollOpt::edge(),
        )?;
        self.event_register_needed = false;
        Ok(())
    }

    fn reregister_events(&mut self, poll: &mut Poll) -> Result<()> {
        poll.reregister(
            &self.transport,
            self.token,
            self.transport.get_interest(),
            PollOpt::edge(),
        )?;
        self.event_register_needed = false;
        Ok(())
    }

    fn process_read_event(&mut self) -> Result<()> {
        self.transport.read_data()?;
        Ok(())
    }

    fn get_next_message(&mut self) -> Result<Option<CowRpcMessage>> {
        self.transport.get_next_message()
    }

    fn process_msg(&mut self, msg: CowRpcMessage) -> Result<()> {
        match msg {
            CowRpcMessage::Handshake(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_handshake_req(hdr, msg)?;
                } else {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    )));
                }
            }
            CowRpcMessage::Register(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_register_req(hdr, msg)?;
                } else {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    )));
                }
            }

            CowRpcMessage::Identity(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_identify_req(hdr, msg)?;
                } else {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    )));
                }
            }

            CowRpcMessage::Resolve(hdr, msg) => {
                if !hdr.is_response() {
                    self.process_resolve_req(hdr, msg)?;
                } else {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    )));
                }
            }

            CowRpcMessage::Terminate(hdr) => {
                if !hdr.is_response() {
                    self.process_terminate_req(hdr)?;
                } else {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, msg
                    )));
                }
            }

            CowRpcMessage::Verify(hdr, msg, payload) => {
                if !hdr.is_response() {
                    return Err(CowRpcError::Proto(format!(
                        "Router can't process a response: hdr={:?} - msg={:?}",
                        hdr, payload
                    )));
                } else {
                    self.process_verify_req(hdr, msg, &payload)?;
                }
            }

            // These requests are forwarded by the router. We should not have to process them here
            CowRpcMessage::Http(_, _, _) => unreachable!(),
            CowRpcMessage::Bind(_, _) => unreachable!(),
            CowRpcMessage::Unbind(_, _) => unreachable!(),
            CowRpcMessage::Call(_, _, _) => unreachable!(),
            CowRpcMessage::Result(_, _, _) => unreachable!(),
        }

        Ok(())
    }

    fn process_handshake_req(&mut self, header: CowRpcHdr, _: CowRpcHandshakeMsg) -> Result<()> {
        // The peer must use the same RPC mode
        let mut flag: u16 = CowRpcErrorCode::Success.into();

        if header.flags & COW_RPC_FLAG_DIRECT != 0 {
            flag = CowRpcErrorCode::Proto.into();
        } else {
            trace!("Client connected from {:?}", self.transport.remote_addr());
            self.state = CowRpcRouterPeerState::Connected;
        }
        self.send_handshake_rsp(flag)?;
        Ok(())
    }

    fn process_register_req(&mut self, _: CowRpcHdr, msg: CowRpcRegisterMsg) -> Result<()> {
        let mut msg_clone = msg.clone();

        for mut iface in &mut msg_clone.ifaces {
            self.router.register_iface_def(&mut iface)?;
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
                match cache.add_cow_identity(&identity, self.id) {
                    Ok(_) => {
                        debug!("new identity: id={} - name={}", self.id, identity.name);
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

        let (rsp, identity_opt) = if let Some(ref cb) = self.router.verify_identity_cb {
            (***cb)(payload)
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
                match cache.add_cow_identity(&identity, self.id) {
                    Ok(_) => {
                        debug!("new identity: id={} - name={}", self.id, identity.name);
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
            } else if let Some(identity_to_resolve) = msg.identity {
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

        self.send_resolve_rsp(flag, msg_clone)?;
        Ok(())
    }

    fn process_terminate_req(&mut self, _: CowRpcHdr) -> Result<()> {
        self.state = CowRpcRouterPeerState::Terminated;
        self.send_terminate_rsp()?;
        Ok(())
    }

    fn process_write_event(&mut self) -> Result<()> {
        self.transport.send_data()?;
        Ok(())
    }

    fn send_handshake_rsp(&mut self, flag: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: self.router.inner.id,
            dst_id: self.id,
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
            dst_id: self.id,
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
            dst_id: self.id,
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
            dst_id: self.id,
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
            dst_id: self.id,
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
            dst_id: self.id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Terminate(header))?;
        Ok(())
    }

    fn send_messages(&mut self, msg: CowRpcMessage) -> Result<()> {
        // Data to send, we set the flag to register the event again with the writable intention
        self.set_event_register_needed();
        self.transport.send_message(msg)
    }

    fn event_register_needed(&self) -> bool {
        self.event_register_needed
    }

    fn set_event_register_needed(&mut self) {
        self.event_register_needed = true;
    }

    fn set_connection_error(&mut self) {
        self.state = CowRpcRouterPeerState::Error;
    }

    fn is_valid(&self) -> bool {
        (self.state.is_initial()
            && self.transport.up_time() < std::time::Duration::from_secs(PEER_CONNECTION_GRACE_PERIOD))
            || self.state.is_connected()
            || (self.state.is_terminated() && self.transport.get_interest().is_writable())
    }

    fn add_remote_bind(&mut self, remote_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        if self.binds.contains_peer_bind(remote_id, client_id, server_id, iface_id) {
            return;
        }

        if let Err(e) = self.binds.add_peer_bind(remote_id, client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn remove_remote_bind(&mut self, remote_id: u32, client_id: u32, server_id: u32, iface_id: u16) {
        if let Err(e) = self.binds.remove_peer_bind(remote_id, client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn update_remote_bind_state(
        &mut self,
        remote_id: u32,
        client_id: u32,
        server_id: u32,
        iface_id: u16,
        new_state: CowRpcBindState,
    ) {
        if let Err(e) = self
            .binds
            .update_peer_bind(remote_id, client_id, server_id, iface_id, new_state)
            {
                match new_state {
                    CowRpcBindState::Unbinding | CowRpcBindState::Unbound => {}
                    _ => error!("BindCollection returned error: {:?}", e),
                };
            }
    }

    fn add_bind(&mut self, client_id: u32, server_id: u32, iface_id: u16) {
        if self.binds.contains(client_id, server_id, iface_id) {
            return;
        }

        if let Err(e) = self.binds.add(client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn remove_bind(&mut self, client_id: u32, server_id: u32, iface_id: u16) {
        if let Err(e) = self.binds.remove(client_id, server_id, iface_id) {
            error!("BindCollection returned error: {:?}", e);
        }
    }

    fn update_bind_state(&mut self, client_id: u32, server_id: u32, iface_id: u16, new_state: CowRpcBindState) {
        if let Err(e) = self.binds.update(client_id, server_id, iface_id, new_state) {
            match new_state {
                CowRpcBindState::Unbinding | CowRpcBindState::Unbound => {}
                _ => error!("BindCollection returned error: {:?}", e),
            };
        }
    }

    fn clear_bind_contexts(&mut self) {
        match self.binds.get_all() {
            Ok(binds) => {
                for (client_id, server_id, iface_id) in binds {
                    self.remove_bind(client_id, server_id, iface_id);
                    let remote_id = if self.id == client_id { server_id } else { client_id };
                    self.remove_remote_bind(remote_id, client_id, server_id, iface_id);
                }
            }
            Err(e) => {
                error!("Unable to clear bind contexts. Got error: {:?}", e);
            }
        }
    }

    fn send_unbind_req(&mut self, client_id: u32, server_id: u32, iface_id: u16) {
        let iface_def = CowRpcIfaceDef {
            id: iface_id,
            flags: COW_RPC_DEF_FLAG_EMPTY,
            ..Default::default()
        };

        let iface_defs = vec![iface_def];

        let remote_is_server = server_id == self.id;
        let src_id = if remote_is_server { client_id } else { server_id };

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: if !remote_is_server { COW_RPC_FLAG_SERVER } else { 0 },
            src_id,
            dst_id: self.id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        let _ = self.send_messages(CowRpcMessage::Unbind(header, msg));
    }

    fn _get_identity(&self) -> Option<CowRpcIdentity> {
        self.identity.clone()
    }

    pub fn get_identity_name(&self) -> Option<String> {
        if self.identity.is_some() {
            Some(self.identity.as_ref().unwrap().name.clone())
        } else {
            None
        }
    }

    pub fn clean_identity(&mut self) {
        if let Some(ref identity) = self.identity {
            let res = match self.router.inner.cache.get_cow_identity_peer_addr(identity) {
                Ok(opt_cow_id) => {
                    if let Some(cow_id) = opt_cow_id {
                        if cow_id == self.id {
                            self.router.inner.cache.remove_cow_identity(identity, self.id)
                        } else {
                            Err(CowRpcError::Internal(format!(
                                "Identity {} already belongs to another peer {}",
                                identity.name, cow_id
                            )))
                        }
                    } else {
                        self.router.inner.cache.remove_cow_identity(identity, self.id)
                    }
                }
                Err(e) => Err(e),
            };

            match res {
                Ok(_) => {
                    debug!("Identity {} removed", identity.name);
                }
                Err(e) => {
                    error!(
                        "Unable to remove identity record {}, got error: {:?}",
                        &identity.name, e
                    );
                }
            }
        }

        self.identity = None;
    }
}

struct PeerGuard<'a> {
    id: u32,
    lock_guard: RwLockReadGuard<'a, HashMap<u32, RwLock<CowRpcRouterPeer>>>,
}

impl<'b> Deref for PeerGuard<'b> {
    type Target = RwLock<CowRpcRouterPeer>;

    fn deref(&self) -> &RwLock<CowRpcRouterPeer> {
        &self.lock_guard.get(&self.id).unwrap()
    }
}

pub struct RouterBindCollection {
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

    pub fn create_key(client_id: u32, server_id: u32, iface_id: u16) -> String {
        format!("{}:{}:{}", client_id, server_id, iface_id)
    }

    pub fn read_key(key: &str) -> Option<(u32, u32, u16)> {
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
    pub fn cow_rpc_bind_context_set_key(peer_id: u32) -> String {
        format!("{}{}", Self::COW_RPC_BIND_CONTEXT_SET, peer_id)
    }

    #[inline]
    pub fn cow_rpc_bind_state_hashset_key(peer_id: u32) -> String {
        format!("{}{}", Self::COW_RPC_BIND_STATE_HASHSET, peer_id)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CowRpcBindState {
    Initial,
    Binding,
    Bound,
    Unbinding,
    Unbound,
    Failure,
}

impl CowRpcBindState {
    pub fn get_name(&self) -> &str {
        match self {
            CowRpcBindState::Initial => "Initial",
            CowRpcBindState::Binding => "Binding",
            CowRpcBindState::Bound => "Bound",
            CowRpcBindState::Unbinding => "Unbinding",
            CowRpcBindState::Unbound => "Unbound",
            CowRpcBindState::Failure => "Failure",
        }
    }

    pub fn from_name(name: &str) -> Self {
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

#[derive(Cacheable, Debug, PartialEq, Eq, Clone)]
pub struct CowRpcIdentity {
    pub typ: CowRpcIdentityType,
    pub name: String,
}

#[derive(Clone)]
pub struct RouterProc {
    id: u16,
    name: String,
}

#[derive(Clone)]
pub struct RouterIface {
    id: u16,
    name: String,
    procs: Rc<RefCell<HashMap<String, RouterProc>>>,
}

impl std::fmt::Display for RouterIface {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "Iface \"{}\" id: {}", self.name, self.id)?;
        writeln!(f, "|")?;
        for procedure in self.procs.borrow().iter() {
            writeln!(f, "    Proc \"{}\" id: {}", procedure.1.name, procedure.1.id)?;
        }
        writeln!(f, "|")?;
        Ok(())
    }
}

impl RouterIface {
    pub fn contains_proc(&self, name: &str) -> bool {
        self.procs.borrow().contains_key(name)
    }
}

pub struct RouterIfaceCollection {
    cache: Cache,
    iface_list: RwLock<HashMap<String, RouterIface>>,
}

impl RouterIfaceCollection {
    const COW_IFACE_HASH_SET: &'static str = "cow_iface";
    const COW_IFACE_PROC_FIELD: &'static str = "cow_proc";
    const COW_IFACES_SET: &'static str = "cow_ifaces";
    const COW_PROCS_SET: &'static str = "cow_procs";

    pub fn new(cache: Cache) -> Self {
        let coll = RouterIfaceCollection {
            cache,
            iface_list: RwLock::new(HashMap::new()),
        };

        if let Err(e) = coll.load_from_cache() {
            error!("Unable to load CowRpcIfaces from cache: {}", e);
        }

        coll
    }

    pub fn load_from_cache(&self) -> Result<()> {
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

            let procs = Rc::new(RefCell::new(procs));

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
                    procs: Rc::new(RefCell::new(HashMap::new())),
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
                let mut proc_list = router_iface.procs.borrow_mut();
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

    pub fn add_to_cache(&self, iface_def: &mut CowRpcIfaceDef) -> Result<()> {
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
    pub fn iface_hash_set_key(iface_name: &str) -> String {
        format!("{}:{}", Self::COW_IFACE_HASH_SET, iface_name)
    }

    #[inline]
    pub fn iface_proc_field_key(proc_name: &str) -> String {
        format!("{}:{}", Self::COW_IFACE_PROC_FIELD, proc_name)
    }

    #[inline]
    pub fn proc_set_key(iface_name: &str) -> String {
        format!("{}:{}", Self::COW_PROCS_SET, iface_name)
    }
}

#[derive(Clone)]
pub struct RouterCache {
    inner: Cache,
}

impl RouterCache {
    pub fn new(cache: Cache) -> Self {
        RouterCache { inner: cache }
    }

    pub fn get_raw_cache(&self) -> &Cache {
        &self.inner
    }

    pub fn get_cow_identity(&self, peer_id: u32) -> Result<Option<String>> {
        self.inner
            .hash_get(COW_ID_RECORDS, &peer_id.to_string())
            .map_err(|e| CowRpcError::Internal(format!("got error while doing identity lookup {:?}", e)))
    }

    pub fn get_cow_identity_peer_addr(&self, identity: &CowRpcIdentity) -> Result<Option<u32>> {
        self.inner
            .hash_get::<u32>(IDENTITY_RECORDS, identity.name.as_ref())
            .map_err(|e| CowRpcError::Internal(format!("got error while doing identity lookup {:?}", e)))
    }

    pub fn add_cow_identity(&self, identity: &CowRpcIdentity, peer_id: u32) -> Result<()> {
        match self
            .inner
            .hash_set(COW_ID_RECORDS, &peer_id.to_string(), identity.name.clone())
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    "Cow addr {} was updated and now has identity {}",
                    peer_id, identity.name
                ),
                _ => {
                    return Err(CowRpcError::Internal(format!(
                        "Unable to add identity {} to peer {}",
                        identity.name, peer_id
                    )));
                }
            }

        match self.inner.hash_set(IDENTITY_RECORDS, identity.name.as_ref(), peer_id) {
            Ok(true) => {}
            Ok(false) => warn!(
                "Identity {} was updated and now belongs to peer {}",
                identity.name, peer_id
            ),
            Err(redis_err) => {
                if let Err(e) = self.inner.hash_delete(COW_ID_RECORDS, &[peer_id.to_string().as_ref()]) {
                    error!("Unable to clean cow id record {}, got error {:?}", peer_id, e);
                }
                return Err(CowRpcError::Internal(format!(
                    "Unable to add record of identity {} to peer {} with error {:?}",
                    identity.name, peer_id, redis_err
                )));
            }
        }

        Ok(())
    }

    pub fn remove_cow_identity(&self, identity: &CowRpcIdentity, peer_id: u32) -> Result<()> {
        let mut got_error = false;
        match self.inner.hash_delete(IDENTITY_RECORDS, &[identity.name.as_ref()]) {
            Ok(_) => {}
            _ => {
                got_error = true;
                error!("Unable to clean cow id record {}", peer_id);
            }
        }

        match self.inner.hash_delete(COW_ID_RECORDS, &[peer_id.to_string().as_ref()]) {
            Ok(_) => {}
            _ => {
                got_error = true;
                error!("Unable to clean cow id record {}", peer_id);
            }
        }

        if got_error {
            return Err(CowRpcError::Internal(format!(
                "Unable to cleam record of identity {} to peer {}",
                identity.name, peer_id
            )));
        }

        Ok(())
    }
}
