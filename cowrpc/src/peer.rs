use super::*;

use parking_lot::{Mutex, RwLock};
use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mio_extras::channel::channel;
use mio_extras::channel::{Receiver, Sender};
use mio_extras::timer::Timer;

use crate::cancel_event::{CancelEvent, CancelEventHandle};
use crate::error::CowRpcError;
use crate::error::CowRpcErrorCode;
use crate::proto::Message;
use crate::proto::*;

static COWRPC_REQ_ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

const CHANNEL_EVENT: Token = Token(1);
const TRANSPORT_EVENT: Token = Token(2);
const CANCEL_EVENT: Token = Token(3);
const TIMER_EVENT: Token = Token(4);
const PROCESS_EVENT: Token = Token(5);

type UnbindCallback = dyn Fn(Arc<CowRpcBindContext>) + Send;
type HttpMsgCallback = dyn Fn(&mut [u8]) -> Vec<u8> + Send;

/// An RPC peer (synchronous)
pub struct CowRpcPeer {
    id: Mutex<u32>,
    router_id: Mutex<u32>,
    rpc: Arc<CowRpc>,
    bind_contexts: Mutex<Vec<Arc<CowRpcBindContext>>>,
    state: Mutex<CowRpcState>,
    requests: Arc<Mutex<Vec<CowRpcReq>>>,

    socket_poll: Poll,
    transport: Mutex<CowRpcTransport>,
    msg_receiver: Mutex<Receiver<CowRpcMessage>>,
    msg_sender: Mutex<Sender<CowRpcMessage>>,

    call_sender: Mutex<Sender<CowRpcMessage>>,
    call_receiver: Mutex<Receiver<CowRpcMessage>>,
    call_processor_thread_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    call_stop_event: Mutex<Option<CancelEvent>>,
    thread_handle: Mutex<Option<JoinHandle<Result<()>>>>,
    stop_event: Mutex<Option<CancelEvent>>,
    on_unbind_callback: Mutex<Option<Box<UnbindCallback>>>,
    on_http_msg_callback: Mutex<Option<Box<HttpMsgCallback>>>,
}

impl CowRpcPeer {
    pub fn new(
        transport: CowRpcTransport,
        msg_receiver: Receiver<CowRpcMessage>,
        msg_sender: Sender<CowRpcMessage>,
        rpc: &Arc<CowRpc>,
    ) -> Result<CowRpcPeer> {
        // We keep poll in our struct because an evented can't be registered to different poll object (even if we call deregister)
        let poll = Poll::new()?;
        poll.register(&msg_receiver, CHANNEL_EVENT, Ready::readable(), PollOpt::edge())?;
        poll.register(&transport, TRANSPORT_EVENT, transport.get_interest(), PollOpt::edge())?;

        let (tx, rx) = channel();

        let peer = CowRpcPeer {
            id: Mutex::new(0),
            router_id: Mutex::new(0),

            rpc: rpc.clone(),
            bind_contexts: Mutex::new(Vec::new()),
            state: Mutex::new(CowRpcState::INITIAL),
            requests: Arc::new(Mutex::new(Vec::new())),

            socket_poll: poll,
            transport: Mutex::new(transport),
            msg_receiver: Mutex::new(msg_receiver),
            msg_sender: Mutex::new(msg_sender),

            call_sender: Mutex::new(tx),
            call_receiver: Mutex::new(rx),
            call_processor_thread_handle: Mutex::new(None),
            call_stop_event: Mutex::new(None),
            thread_handle: Mutex::new(None),
            stop_event: Mutex::new(None),
            on_unbind_callback: Mutex::new(None),
            on_http_msg_callback: Mutex::new(None),
        };

        Ok(peer)
    }

    pub fn on_unbind_callback<F: 'static + Send + Fn(Arc<CowRpcBindContext>)>(&self, callback: F) {
        *self.on_unbind_callback.lock() = Some(Box::new(callback));
    }

    pub fn on_http_msg_callback<F: 'static + Send + Fn(&mut [u8]) -> Vec<u8>>(&self, callback: F) {
        *self.on_http_msg_callback.lock() = Some(Box::new(callback));
    }

    fn get_id(&self) -> u32 {
        *self.id.lock()
    }

    fn get_router_id(&self) -> u32 {
        *self.router_id.lock()
    }

    fn set_id(&self, id: u32) {
        *self.id.lock() = id;
    }

    fn set_router_id(&self, router_id: u32) {
        *self.router_id.lock() = router_id;
    }

    fn get_state(&self) -> CowRpcState {
        *self.state.lock()
    }

    fn transition_to_state(&self, new_state: CowRpcState) {
        *self.state.lock() = new_state;
    }

    pub(crate) fn init_client(
        &self,
        timeout: Option<Duration>,
        cancel_handle: Option<&CancelEventHandle>,
    ) -> Result<()> {
        // Allow a short time to be sure the socket is connected
        thread::sleep(Duration::from_millis(500));

        self.send_handshake_req()?;
        self.transition_to_state(CowRpcState::HANDSHAKE);
        self.run_to_state(CowRpcState::ACTIVE, timeout, cancel_handle)?;

        Ok(())
    }

    pub(crate) fn init_server(
        &self,
        timeout: Option<Duration>,
        cancel_handle: Option<&CancelEventHandle>,
    ) -> Result<()> {
        self.transition_to_state(CowRpcState::HANDSHAKE);
        self.run_to_state(CowRpcState::ACTIVE, timeout, cancel_handle)?;
        Ok(())
    }

    fn send_handshake_req(&self) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
            flags: if self.rpc.mode == CowRpcMode::DIRECT {
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

        self.send_messages(CowRpcMessage::Handshake(header, msg))?;
        Ok(())
    }

    fn send_handshake_rsp(&self, flag: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            size: 0,
            msg_type: proto::COW_RPC_HANDSHAKE_MSG_ID,
            offset: 0,
            flags: COW_RPC_FLAG_RESPONSE | flag,
            src_id: 0,
            dst_id: 0,
        };

        let msg = CowRpcHandshakeMsg {
            version_major: 1,
            version_minor: 0,
            arch_flags: if std::mem::size_of::<usize>() == 8 {
                proto::COW_RPC_ARCH_FLAG_64_BIT
            } else {
                0
            },
            role_flags: 0,
            capabilities: 0,
            reserved: 0,
        };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Handshake(header, msg))?;
        Ok(())
    }

    fn send_register_req(&self) -> Result<()> {
        let ifacedefs;
        {
            let ifaces = self.rpc.ifaces.lock();
            ifacedefs = CowRpcPeer::build_ifacedef_list(&ifaces, true, true);
            drop(ifaces);
        }
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_REGISTER_MSG_ID,
            src_id: self.get_id(),
            dst_id: self.get_router_id(),
            ..Default::default()
        };

        let msg = CowRpcRegisterMsg { ifaces: ifacedefs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Register(header, msg))?;
        Ok(())
    }

    fn send_register_rsp(&self, dst_id: u32, ifaces: Vec<CowRpcIfaceDef>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_REGISTER_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcRegisterMsg { ifaces };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Register(header, msg))?;
        Ok(())
    }

    fn send_identify_req(&self, name: &str, typ: CowRpcIdentityType) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_IDENTIFY_MSG_ID,
            src_id: self.get_id(),
            dst_id: self.get_router_id(),
            ..Default::default()
        };

        let identity = CowRpcIdentityMsg {
            typ,
            flags: 0,
            identity: String::from(name),
        };

        header.size = header.get_size() + identity.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Identity(header, identity))?;
        Ok(())
    }

    fn send_verify_req(&self, call_id: u32, payload: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_VERIFY_MSG_ID,
            src_id: self.get_id(),
            dst_id: self.get_router_id(),
            ..Default::default()
        };

        let msg = CowRpcVerifyMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + payload.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_messages(CowRpcMessage::Verify(header, msg, payload))?;
        Ok(())
    }

    fn send_resolve_req(&self, id: Option<u32>, name: Option<&str>, reverse: bool) -> Result<()> {
        if (reverse && id.is_none()) || (!reverse && name.is_none()) {
            return Err(CowRpcError::Internal("Wrong parameters".to_string()));
        }

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESOLVE_MSG_ID,
            flags: if reverse { proto::COW_RPC_FLAG_REVERSE } else { 0 },
            src_id: self.get_id(),
            dst_id: self.get_router_id(),
            ..Default::default()
        };

        let node_id;
        let identity;
        if reverse {
            node_id = id.unwrap();
            identity = None;
        } else {
            node_id = 0;
            identity = Some(CowRpcIdentityMsg {
                typ: CowRpcIdentityType::UPN,
                flags: 0,
                identity: String::from(name.unwrap()),
            });
        }

        let msg = CowRpcResolveMsg { node_id, identity };

        header.size = header.get_size() + msg.get_size(header.flags);
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Resolve(header, msg))?;
        Ok(())
    }

    fn send_bind_req(&self, server_id: u32, iface: &Arc<RwLock<CowRpcIface>>) -> Result<()> {
        let iface = iface.read();
        let iface_def = CowRpcPeer::build_ifacedef(&(*iface), false, true);
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_BIND_MSG_ID,
            src_id: self.get_id(),
            dst_id: server_id,
            ..Default::default()
        };

        let msg = CowRpcBindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Bind(header, msg))?;
        Ok(())
    }

    fn send_bind_rsp(&self, dst_id: u32, iface_def: CowRpcIfaceDef, flags: u16) -> Result<()> {
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_BIND_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcBindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Bind(header, msg))?;
        Ok(())
    }

    fn send_unbind_rsp(&self, dst_id: u32, iface_def: CowRpcIfaceDef, flags: u16) -> Result<()> {
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Unbind(header, msg))?;
        Ok(())
    }

    fn send_unbind_req(&self, remote_id: u32, remote_is_server: bool, iface: &Arc<RwLock<CowRpcIface>>) -> Result<()> {
        let iface = iface.read();
        let iface_def = CowRpcPeer::build_ifacedef(&iface, false, false);
        let iface_defs = vec![iface_def];

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_UNBIND_MSG_ID,
            flags: if !remote_is_server { COW_RPC_FLAG_SERVER } else { 0 },
            src_id: self.get_id(),
            dst_id: remote_id,
            ..Default::default()
        };

        let msg = CowRpcUnbindMsg { ifaces: iface_defs };

        header.size = header.get_size() + msg.get_size();
        header.offset = header.get_size() as u8;

        self.send_messages(CowRpcMessage::Unbind(header, msg))?;
        Ok(())
    }

    fn send_call_req<T: CowRpcParams>(
        &self,
        bind_context: Arc<CowRpcBindContext>,
        proc_id: u16,
        call_id: u32,
        params: &T,
    ) -> Result<()> {
        let iface = &bind_context.iface;
        let iface = iface.read();
        let procedure = iface.get_proc(proc_id, true).unwrap();

        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_CALL_MSG_ID,
            src_id: self.get_id(),
            dst_id: bind_context.remote_id,
            ..Default::default()
        };

        let msg = CowRpcCallMsg {
            call_id,
            iface_id: iface.rid,
            proc_id: procedure.rid,
        };

        header.size = header.get_size() + msg.get_size() + params.get_size()?;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        let mut payload = Vec::new();
        params.write_to(&mut payload)?;
        self.send_messages(CowRpcMessage::Call(header, msg, payload))?;
        Ok(())
    }

    fn send_result_rsp(
        &self,
        dst_id: u32,
        call_msg: CowRpcCallMsg,
        output_param: Option<&dyn CowRpcParams>,
        flags: u16,
    ) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_RESULT_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | flags,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcResultMsg {
            call_id: call_msg.call_id,
            iface_id: call_msg.iface_id,
            proc_id: call_msg.proc_id,
        };

        if output_param.is_some() {
            let call_result = output_param.unwrap();
            header.size = header.get_size() + msg.get_size() + call_result.get_size()?;
            header.offset = (header.get_size() + msg.get_size()) as u8;

            let mut payload = Vec::new();
            call_result.write_to(&mut payload)?;
            self.send_messages(CowRpcMessage::Result(header, msg, payload))?;
        } else {
            header.size = header.get_size() + msg.get_size();
            header.offset = header.size as u8;

            self.send_messages(CowRpcMessage::Result(header, msg, Vec::new()))?;
        }

        Ok(())
    }

    fn send_http_req(&self, dst_id: u32, call_id: u32, http_msg: Vec<u8>) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HTTP_MSG_ID,
            flags: COW_RPC_FLAG_FINAL,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcHttpMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + http_msg.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_messages(CowRpcMessage::Http(header, msg, http_msg))?;
        Ok(())
    }

    fn send_http_rsp(&self, dst_id: u32, call_id: u32, http_msg: Vec<u8>, flags: u16) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_HTTP_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE | COW_RPC_FLAG_FINAL | flags,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        let msg = CowRpcHttpMsg {
            call_id
        };

        header.size = header.get_size() + msg.get_size() + http_msg.len() as u32;
        header.offset = (header.get_size() + msg.get_size()) as u8;

        self.send_messages(CowRpcMessage::Http(header, msg, http_msg))?;
        Ok(())
    }

    fn send_terminate_req(&self) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            src_id: self.get_id(),
            dst_id: self.get_router_id(),
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Terminate(header))?;
        Ok(())
    }

    fn send_terminate_rsp(&self, dst_id: u32) -> Result<()> {
        let mut header = CowRpcHdr {
            msg_type: proto::COW_RPC_TERMINATE_MSG_ID,
            flags: COW_RPC_FLAG_RESPONSE,
            src_id: self.get_id(),
            dst_id,
            ..Default::default()
        };

        header.size = header.get_size();
        header.offset = header.size as u8;

        self.send_messages(CowRpcMessage::Terminate(header))?;
        Ok(())
    }

    fn run_to_state(
        &self,
        state: CowRpcState,
        timeout: Option<Duration>,
        cancel_handle: Option<&CancelEventHandle>,
    ) -> Result<()> {
        let msg_receiver = self.msg_receiver.lock();
        let mut transport = self.transport.lock();

        self.socket_poll.reregister(
            transport.deref(),
            TRANSPORT_EVENT,
            transport.get_interest(),
            PollOpt::edge(),
        )?;

        if cancel_handle.is_some() {
            self.socket_poll
                .register(cancel_handle.unwrap(), CANCEL_EVENT, Ready::readable(), PollOpt::edge())?;
        }

        let mut timer = Timer::<()>::default();
        if timeout.is_some() {
            timer.set_timeout(timeout.unwrap(), ());
            self.socket_poll
                .register(&timer, TIMER_EVENT, Ready::readable(), PollOpt::edge())?;
        }

        // Create storage for events
        let mut events = Events::with_capacity(1024);

        loop {
            self.socket_poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    TRANSPORT_EVENT => {
                        if event.readiness().is_writable() {
                            transport.send_data()?;
                            self.socket_poll.reregister(
                                transport.deref(),
                                TRANSPORT_EVENT,
                                transport.get_interest(),
                                PollOpt::edge(),
                            )?;
                        }

                        if event.readiness().is_readable() {
                            transport.read_data()?;
                            while let Some(msg) = transport.get_next_message()? {
                                // Handshake req and register req has to be processed in the current thread since the
                                // msg processor thread is not started yet when we init a server (direct mode)
                                if msg.is_response() || msg.is_handshake() || msg.is_register() {
                                    self.process_msg(msg)?;
                                } else {
                                    match self.call_sender.lock().send(msg) {
                                        Ok(_) => {}
                                        Err(e) => debug!("call sending to the processor thread failed : {}", e),
                                    };
                                }

                                if *self.state.lock() == state {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    CHANNEL_EVENT => {
                        loop {
                            let result = msg_receiver.try_recv();
                            match result {
                                Ok(msg) => {
                                    transport.send_message(msg)?;
                                }
                                Err(e) => match e {
                                    std::sync::mpsc::TryRecvError::Empty => {
                                        break;
                                    }
                                    std::sync::mpsc::TryRecvError::Disconnected => {
                                        return Err(e.into());
                                    }
                                },
                            }
                        }

                        self.socket_poll.reregister(
                            transport.deref(),
                            TRANSPORT_EVENT,
                            transport.get_interest(),
                            PollOpt::edge(),
                        )?;
                    }

                    CANCEL_EVENT => {
                        return Err(CowRpcError::Cancel);
                    }

                    TIMER_EVENT => {
                        return Err(CowRpcError::Timeout);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub(crate) fn process_msg(&self, msg: CowRpcMessage) -> Result<()> {
        let is_response = msg.is_response();

        match msg {
            CowRpcMessage::Handshake(header, msg) => {
                if is_response {
                    self.process_handshake_rsp(header, msg)?
                } else {
                    self.process_handshake_req(header, msg)?
                }
            }

            CowRpcMessage::Register(header, msg) => {
                if is_response {
                    self.process_register_rsp(header, msg)?
                } else {
                    self.process_register_req(header, msg)?
                }
            }

            CowRpcMessage::Identity(header, msg) => {
                if is_response {
                    self.process_identity_rsp(header, msg)?
                } else {
                    self.process_identity_req(header, msg)?
                }
            }

            CowRpcMessage::Resolve(header, msg) => {
                if is_response {
                    self.process_resolve_rsp(header, msg)?
                } else {
                    self.process_resolve_req(header, msg)?
                }
            }

            CowRpcMessage::Bind(header, msg) => {
                if is_response {
                    self.process_bind_rsp(header, msg)?
                } else {
                    self.process_bind_req(header, msg)?
                }
            }

            CowRpcMessage::Unbind(header, msg) => {
                if is_response {
                    self.process_unbind_rsp(header, msg)?
                } else {
                    self.process_unbind_req(header, msg)?
                }
            }

            CowRpcMessage::Call(header, msg, mut payload) => {
                if is_response {
                    unreachable!()
                } else {
                    self.process_call_req(header, msg, &mut payload)?
                }
            }

            CowRpcMessage::Result(header, msg, payload) => {
                if is_response {
                    self.process_result_rsp(header, msg, &payload)?
                } else {
                    unreachable!()
                }
            }

            CowRpcMessage::Terminate(header) => {
                if is_response {
                    self.process_terminate_rsp(header)?
                } else {
                    self.process_terminate_req(header)?
                }
            }

            CowRpcMessage::Verify(header, msg, payload) => {
                if is_response {
                    self.process_verify_rsp(header, msg, &payload)?
                } else {
                    unreachable!()
                }
            }

            CowRpcMessage::Http(header, msg, mut payload) => {
                if is_response {
                    self.process_http_rsp(header, msg, &payload)?
                } else {
                    self.process_http_req(header, msg, &mut payload)?
                }
            }
        }

        Ok(())
    }

    fn process_handshake_rsp(&self, header: CowRpcHdr, _: CowRpcHandshakeMsg) -> Result<()> {
        if header.is_failure() {
            return Err(error::CowRpcError::CowRpcFailure(CowRpcErrorCode::from(header.flags)));
        }

        if self.get_state() != CowRpcState::HANDSHAKE {
            return Err(error::CowRpcError::Internal(format!(
                "Handshake response received and state machine has wrong state ({:?})",
                self.get_state()
            )));
        }

        self.set_id(header.dst_id);
        self.set_router_id(header.src_id);

        self.send_register_req()?;
        self.transition_to_state(CowRpcState::REGISTER);
        Ok(())
    }

    fn process_handshake_req(&self, header: CowRpcHdr, _: CowRpcHandshakeMsg) -> Result<()> {
        if self.get_state() != CowRpcState::HANDSHAKE {
            return Err(error::CowRpcError::Internal(format!(
                "Handshake response received and state machine has wrong state ({:?})",
                self.get_state()
            )));
        }

        let mut flag: u16 = CowRpcErrorCode::Success.into();

        // The peer must use the same RPC mode
        if header.flags & COW_RPC_FLAG_DIRECT != 0 {
            if self.rpc.mode != CowRpcMode::DIRECT {
                flag = CowRpcErrorCode::Proto.into();
            }
        } else {
            if self.rpc.mode != CowRpcMode::ROUTED || self.rpc.role != CowRpcRole::ROUTER {
                flag = CowRpcErrorCode::Proto.into();
            }
        }

        self.send_handshake_rsp(flag)?;
        self.transition_to_state(CowRpcState::REGISTER);

        Ok(())
    }

    fn process_register_rsp(&self, header: CowRpcHdr, msg: CowRpcRegisterMsg) -> Result<()> {
        if header.is_failure() {
            return Err(error::CowRpcError::CowRpcFailure(CowRpcErrorCode::from(header.flags)));
        }

        if self.get_state() != CowRpcState::REGISTER {
            return Err(error::CowRpcError::Internal(format!(
                "Register response received and state machine has wrong state ({:?})",
                self.get_state()
            )));
        }

        for iface in &msg.ifaces {
            let mut iface_clone = iface.clone();
            self.rpc.register_iface_def(&mut iface_clone, false)?;
        }

        self.transition_to_state(CowRpcState::ACTIVE);
        Ok(())
    }

    fn process_register_req(&self, header: CowRpcHdr, msg: CowRpcRegisterMsg) -> Result<()> {
        if self.get_state() != CowRpcState::REGISTER {
            return Err(error::CowRpcError::Internal(format!(
                "Register response received and state machine has wrong state ({:?})",
                self.get_state()
            )));
        }

        let mut msg_clone = msg.clone();

        for mut iface in &mut msg_clone.ifaces {
            self.rpc.register_iface_def(&mut iface, true)?;
        }

        self.send_register_rsp(header.src_id, msg_clone.ifaces)?;
        self.transition_to_state(CowRpcState::ACTIVE);
        Ok(())
    }

    fn process_identity_rsp(&self, header: CowRpcHdr, msg: CowRpcIdentityMsg) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Identify(ref identify_req) => {
                    if identify_req.name.eq(&msg.identity) {
                        return identify_req
                            .tx
                            .send(CowRpcIdentifyRsp {
                                error: CowRpcErrorCode::from(header.flags),
                            }).map_err(|e| CowRpcError::Internal(e.to_string()));
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_identity_req(&self, _: CowRpcHdr, _: CowRpcIdentityMsg) -> Result<()> {
        unimplemented!()
    }

    fn process_resolve_rsp(&self, header: CowRpcHdr, msg: CowRpcResolveMsg) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Resolve(ref resolve_req) => {
                    if header.is_reverse() && resolve_req.reverse {
                        if resolve_req.node_id.unwrap() == msg.node_id {
                            return resolve_req
                                .tx
                                .send(CowRpcResolveRsp {
                                    node_id: None,
                                    name: if msg.identity.is_some() {
                                        Some(msg.identity.as_ref().unwrap().identity.clone())
                                    } else {
                                        None
                                    },
                                    error: CowRpcErrorCode::from(header.flags),
                                }).map_err(|e| CowRpcError::Internal(e.to_string()));
                        }
                    } else if !header.is_reverse() && !resolve_req.reverse {
                        if resolve_req
                            .name
                            .as_ref()
                            .unwrap()
                            .eq(&msg.identity.as_ref().unwrap().identity)
                            {
                                return resolve_req
                                    .tx
                                    .send(CowRpcResolveRsp {
                                        node_id: Some(msg.node_id),
                                        name: None,
                                        error: CowRpcErrorCode::from(header.flags),
                                    }).map_err(|e| CowRpcError::Internal(e.to_string()));
                            }
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_resolve_req(&self, _: CowRpcHdr, _: CowRpcResolveMsg) -> Result<()> {
        unimplemented!()
    }

    fn process_bind_rsp(&self, header: CowRpcHdr, msg: CowRpcBindMsg) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Bind(ref bind_req) => {
                    // TODO support many ifaces
                    if bind_req.server_id == header.src_id && bind_req.iface_id == msg.ifaces[0].id {
                        let mut error = CowRpcErrorCode::from(msg.ifaces[0].flags);
                        if error == CowRpcErrorCode::Success {
                            error = CowRpcErrorCode::from(header.flags);
                        }
                        return bind_req.tx.send(CowRpcBindRsp { error }).map_err(|e| CowRpcError::Internal(e.to_string()));
                        ;
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_bind_req(&self, header: CowRpcHdr, msg: CowRpcBindMsg) -> Result<()> {
        for msg_iface in msg.ifaces {
            // Clone the iface_def to update the flags
            let mut iface_def = msg_iface.clone();
            let flag_result;

            // Try to get the iface with the iface_id
            let iface = self.rpc.get_iface(msg_iface.id, false);
            if iface.is_some() {
                let iface = iface.unwrap();
                let mut bind_contexts = self.bind_contexts.lock();

                let bind_context_found = bind_contexts.deref().iter().any(|bind_context| {
                    (!bind_context.is_server)
                        && bind_context.remote_id == header.src_id
                        && bind_context.iface.get_remote_id() == iface.get_remote_id()
                });

                if !bind_context_found {
                    // Success : New bind context
                    let new_bind_context = CowRpcBindContext::new(true, header.src_id, &iface);
                    bind_contexts.push(new_bind_context);
                    flag_result = CowRpcErrorCode::Success;
                } else {
                    // Already bound
                    iface_def.flags = CowRpcErrorCode::AlreadyBound.into();
                    flag_result = CowRpcErrorCode::AlreadyBound;
                }
            } else {
                // Interface doesn't exist
                iface_def.flags = CowRpcErrorCode::IfaceId.into();
                flag_result = CowRpcErrorCode::IfaceId;
            }

            self.send_bind_rsp(header.src_id, iface_def, flag_result.into())?;
        }
        Ok(())
    }

    fn process_unbind_rsp(&self, header: CowRpcHdr, msg: CowRpcUnbindMsg) -> Result<()> {
        let req_list = self.requests.lock();
        for req in req_list.iter() {
            match *req {
                CowRpcReq::Unbind(ref unbind_req) => {
                    // The request has to be in the same direction to match
                    let from_client = header.flags & COW_RPC_FLAG_SERVER == 0;
                    if unbind_req.from_client != from_client {
                        continue;
                    }
                    //TODO support many ifaces
                    if header.src_id == unbind_req.remote_id && msg.ifaces[0].id == unbind_req.iface_id {
                        return unbind_req
                            .tx
                            .send(CowRpcUnbindRsp {
                                error: CowRpcErrorCode::from(header.flags),
                            }).map_err(|e| CowRpcError::Internal(e.to_string()));
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_unbind_req(&self, header: CowRpcHdr, msg: CowRpcUnbindMsg) -> Result<()> {
        let from_client = header.flags & COW_RPC_FLAG_SERVER == 0;

        for msg_iface in msg.ifaces {
            // Clone the iface_def to update the flags
            let mut iface_def = msg_iface.clone();
            let mut removed_bind_context = None;
            let flag_result;

            // Try to get the iface with the iface_id
            let iface = self.rpc.get_iface(msg_iface.id, false);

            if iface.is_some() {
                removed_bind_context = self.remove_bind_context(from_client, header.src_id, msg_iface.id);
                if removed_bind_context.is_some() {
                    flag_result = CowRpcErrorCode::Success;
                } else {
                    iface_def.flags = CowRpcErrorCode::NotBound.into();
                    flag_result = CowRpcErrorCode::NotBound;
                }
            } else {
                iface_def.flags = CowRpcErrorCode::IfaceId.into();
                flag_result = CowRpcErrorCode::IfaceId;
            }

            self.send_unbind_rsp(header.src_id, iface_def, flag_result.into())?;

            if let Some(bind_context) = removed_bind_context {
                let callback_opt = self.on_unbind_callback.lock();
                if let Some(ref callback) = *callback_opt {
                    callback(bind_context);
                }
            }
        }
        Ok(())
    }

    fn process_call_req(&self, header: CowRpcHdr, msg: CowRpcCallMsg, mut payload: &mut Vec<u8>) -> Result<()> {
        // Try to get the iface with the iface_id
        let iface = self.rpc.get_iface(msg.iface_id, false);

        let flag;
        let output_result;
        let mut output_param = None;

        match iface {
            Some(iface) => {
                let iface = iface.read();
                let procedure = iface.get_proc(msg.proc_id, false).unwrap();
                match &iface.server {
                    Some(ref server) => {
                        let result = server.dispatch_call(header.src_id, procedure.lid, &mut payload);
                        match result {
                            Ok(call_result) => {
                                output_result = call_result;
                                output_param = Some(&*output_result);
                                flag = CowRpcErrorCode::Success;
                            }
                            Err(e) => match e {
                                error::CowRpcError::CowRpcFailure(error_code) => flag = error_code,
                                _ => flag = CowRpcErrorCode::Internal,
                            },
                        }
                    }
                    None => {
                        flag = CowRpcErrorCode::Internal;
                    }
                }
            }
            None => {
                flag = CowRpcErrorCode::IfaceId;
            }
        }

        self.send_result_rsp(header.src_id, msg, output_param, flag.into())?;
        Ok(())
    }

    fn process_result_rsp(&self, header: CowRpcHdr, msg: CowRpcResultMsg, payload: &[u8]) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Call(ref call_req) => {
                    if call_req.call_id == msg.call_id
                        && call_req.iface_id == msg.iface_id
                        && call_req.proc_id == msg.proc_id
                        {
                            return call_req
                                .tx
                                .send(CowRpcCallRsp {
                                    error: CowRpcErrorCode::from(header.flags),
                                    msg_pack: payload.to_owned(),
                                }).map_err(|e| CowRpcError::Internal(e.to_string()));
                        }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_verify_rsp(&self, header: CowRpcHdr, msg: CowRpcVerifyMsg, payload: &[u8]) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Verify(ref verify_req) => {
                    if verify_req.call_id == msg.call_id {
                        return verify_req.tx.send(CowRpcVerifyRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            payload: payload.to_vec(),
                        }).map_err(|e| CowRpcError::Internal(e.to_string()));
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_http_rsp(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, payload: &[u8]) -> Result<()> {
        let req_list = self.requests.lock();

        for req in req_list.iter() {
            match *req {
                CowRpcReq::Http(ref http_req) => {
                    if http_req.call_id == msg.call_id {
                        return http_req.tx.send(CowRpcHttpRsp {
                            _error: CowRpcErrorCode::from(header.flags),
                            http_rsp: payload.to_vec(),
                        }).map_err(|e| CowRpcError::Internal(e.to_string()));
                    }
                }
                _ => continue, /* Wrong request type, we move to the next one */
            }
        }

        Ok(())
    }

    fn process_http_req(&self, header: CowRpcHdr, msg: CowRpcHttpMsg, payload: &mut [u8]) -> Result<()> {
        let res = if let Some(ref cb) = *self.on_http_msg_callback.lock() {
            (**cb)(payload)
        } else {
            b"HTTP/1.1 501 Not Implemented\r\n\r\n".to_vec()
        };

        self.send_http_rsp(header.src_id, msg.call_id, res, header.flags)
    }

    fn process_terminate_rsp(&self, _: CowRpcHdr) -> Result<()> {
        self.transition_to_state(CowRpcState::TERMINATE);
        Ok(())
    }

    fn process_terminate_req(&self, header: CowRpcHdr) -> Result<()> {
        self.send_terminate_rsp(header.src_id)?;
        Ok(())
    }

    /// Sends an Identify request to set this peer's `name`.
    pub fn identify_sync(
        &self,
        name: &str,
        identity_type: CowRpcIdentityType,
        timeout: Duration,
        cancel_event: Option<&CancelEventHandle>,
    ) -> Result<()> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Identify(CowRpcIdentifyReq {
            id,
            name: String::from(name),
            tx,
        });

        self.add_request(req);

        self.send_identify_req(name, identity_type)?;
        let result = self.wait_response::<CowRpcIdentifyRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        let rsp = result?;
        rsp.get_result()
    }

    /// Sends a verify request to set this peer's `name` with complete trust of the router
    pub fn verify_sync(
        &self,
        http_req: &[u8],
        timeout: Duration,
        cancel_event: Option<&CancelEventHandle>,
    ) -> Result<Vec<u8>> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Verify(CowRpcVerifyReq {
            id,
            call_id: id as u32,
            tx,
        });

        self.add_request(req);

        self.send_verify_req(id as u32, http_req.to_vec())?;
        let result = self.wait_response::<CowRpcVerifyRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        result.map(|r| r.payload)
    }

    /// Sends a verify request to set this peer's `name` with complete trust of the router
    pub fn call_http_sync(
        &self,
        bind_context: Arc<CowRpcBindContext>,
        http_req: &[u8],
        timeout: Duration,
        cancel_event: Option<&CancelEventHandle>,
    ) -> Result<Vec<u8>> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Http(CowRpcHttpReq {
            id,
            call_id: id as u32,
            tx,
        });

        self.add_request(req);

        self.send_http_req(bind_context.remote_id, id as u32, http_req.to_vec())?;
        let result = self.wait_response::<CowRpcHttpRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        result.map(|r| r.http_rsp)
    }

    /// Requests the id of the peer with the specified `name`.
    pub fn resolve_sync(&self, name: &str, timeout: Duration, cancel_event: Option<&CancelEventHandle>) -> Result<u32> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Resolve(CowRpcResolveReq {
            id,
            node_id: None,
            name: Some(String::from(name)),
            reverse: false,
            tx,
        });

        self.add_request(req);
        self.send_resolve_req(None, Some(name), false)?;
        let result = self.wait_response::<CowRpcResolveRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        let rsp = result?;
        rsp.get_result()
    }

    pub fn resolve_reverse_sync(
        &self,
        node_id: u32,
        timeout: Duration,
        cancel_event: Option<&CancelEventHandle>,
    ) -> Result<String> {
        let (tx, rx) = channel();
        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Resolve(CowRpcResolveReq {
            id,
            node_id: Some(node_id),
            name: None,
            reverse: true,
            tx,
        });

        self.add_request(req);
        self.send_resolve_req(Some(node_id), None, true)?;
        let result = self.wait_response::<CowRpcResolveRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        let rsp = result?;
        rsp.get_reverse_result()
    }

    /// Binds to an interface on a remote peer. This is required before sending calls to that interface.
    pub fn bind_sync(
        &self,
        server_id: u32,
        iface_id: u16,
        timeout: Duration,
        cancel_handle: Option<&CancelEventHandle>,
    ) -> Result<Arc<CowRpcBindContext>> {
        if let Some(iface) = self.rpc.get_iface(iface_id, true) {
            {
                if let Some(bc) = self
                    .bind_contexts
                    .lock()
                    .iter()
                    .find(|bc| bc.remote_id == server_id && bc.iface.read().lid == iface_id)
                    {
                        trace!("bind context already existing locally");
                        return Ok(bc.clone());
                    }
            }
            {
                let (tx, rx) = channel();

                let req;
                let id;
                {
                    let iface = iface.read();
                    id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
                    req = CowRpcReq::Bind(CowRpcBindReq {
                        id,
                        tx,
                        server_id,
                        iface_id: iface.rid,
                    });
                }
                self.add_request(req);
                self.send_bind_req(server_id, &iface)?;

                let result = self.wait_response::<CowRpcBindRsp>(&rx, timeout, cancel_handle);
                self.remove_request(id);

                let rsp = result?;

                let mut bind_contexts = self.bind_contexts.lock();

                if rsp.is_success() {
                    let bind_context = CowRpcBindContext::new(false, server_id, &iface);
                    bind_contexts.push(bind_context.clone());
                    Ok(bind_context)
                } else {
                    match rsp.get_error() {
                        CowRpcErrorCode::AlreadyBound => {
                            let bind_context = CowRpcBindContext::new(false, server_id, &iface);
                            bind_contexts.push(bind_context.clone());
                            trace!("bind context already existing remotely, creating one locally");
                            Ok(bind_context)
                        }
                        _ => Err(CowRpcError::CowRpcFailure(rsp.get_error())),
                    }
                }
            }
        } else {
            Err(CowRpcError::Proto("unregistered interface".to_string()))
        }
    }

    /// Unbinds from an interface on a remote peer.
    pub fn unbind_sync(
        &self,
        bind_context: Arc<CowRpcBindContext>,
        timeout: Duration,
        cancel_event: Option<&CancelEventHandle>,
    ) -> Result<()> {
        let (tx, rx) = channel();

        let req;
        let id;
        {
            let iface = bind_context.iface.read();
            id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
            req = CowRpcReq::Unbind(CowRpcUnbindReq {
                id,
                tx,
                from_client: true,
                remote_id: bind_context.remote_id,
                iface_id: iface.rid,
            });
        }

        self.add_request(req);
        self.send_unbind_req(bind_context.remote_id, true, &bind_context.iface)?;

        let result = self.wait_response::<CowRpcUnbindRsp>(&rx, timeout, cancel_event);
        self.remove_request(id);

        let rsp = result?;

        if rsp.is_success() {
            self.remove_bind_context(
                bind_context.is_server,
                bind_context.remote_id,
                bind_context.get_iface_remote_id(),
            );
            Ok(())
        } else {
            Err(CowRpcError::CowRpcFailure(rsp.get_error()))
        }
    }

    pub fn call_sync<P: 'static + CowRpcParams, T: CowRpcParams>(
        &self,
        bind_context: Arc<CowRpcBindContext>,
        proc_id: u16,
        params: &P,
    ) -> Result<T> {
        let proc_remote_id = bind_context.get_proc_remote_id(proc_id).ok_or_else(|| {
            CowRpcError::Internal(format!("Remote proc_id can't be found for local proc_id={}", proc_id))
        })?;
        let (tx, rx) = channel();

        let id = COWRPC_REQ_ID_GENERATOR.fetch_add(1, atomic::Ordering::SeqCst);
        let req = CowRpcReq::Call(CowRpcCallReq {
            id,
            call_id: id as u32,
            iface_id: bind_context.get_iface_remote_id(),
            proc_id: proc_remote_id,
            tx,
        });

        self.add_request(req);
        self.send_call_req(bind_context.clone(), proc_id, id as u32, params)?;

        let result = self.wait_response::<CowRpcCallRsp>(&rx, Duration::from_secs(10), None); //TODO add timeout
        self.remove_request(id);

        let rsp = result?;

        if rsp.is_success() {
            let mut msg_pack = &rsp.msg_pack[..];
            let output_param = T::read_from(&mut msg_pack)?;
            Ok(output_param)
        } else {
            let (remote_id, iface_rid) = {
                (
                    u32::from(bind_context.get_iface_remote_id()),
                    bind_context.iface.read().rid,
                )
            };
            match rsp.get_error() {
                CowRpcErrorCode::NotBound => {
                    trace!("bind context existing locally but not remotely, deleting it");
                    if self.remove_bind_context(false, remote_id, iface_rid).is_none() {
                        warn!(
                            "Unable to remove bind context remote {}  iface {}",
                            remote_id, iface_rid
                        );
                    }
                }
                _ => {}
            }
            Err(CowRpcError::CowRpcFailure(rsp.get_error()))
        }
    }

    fn wait_response<T>(
        &self,
        receiver: &Receiver<T>,
        timeout: Duration,
        cancel_handle: Option<&CancelEventHandle>,
    ) -> Result<T> {
        // Create a poll instance
        let poll = Poll::new()?;

        let mut timer = Timer::<()>::default();
        timer.set_timeout(timeout, ());

        poll.register(receiver, CHANNEL_EVENT, Ready::readable(), PollOpt::edge())?;
        poll.register(&timer, TIMER_EVENT, Ready::readable(), PollOpt::edge())?;

        if cancel_handle.is_some() {
            poll.register(cancel_handle.unwrap(), CANCEL_EVENT, Ready::readable(), PollOpt::edge())?;
        }

        // Create storage for events
        let mut events = Events::with_capacity(1024);

        loop {
            poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    CHANNEL_EVENT => loop {
                        let result = receiver.try_recv();
                        match result {
                            Ok(rsp) => {
                                return Ok(rsp);
                            }
                            Err(e) => match e {
                                std::sync::mpsc::TryRecvError::Empty => {
                                    break;
                                }
                                std::sync::mpsc::TryRecvError::Disconnected => {
                                    return Err(e.into());
                                }
                            },
                        }
                    },
                    TIMER_EVENT => {
                        return Err(CowRpcError::Timeout);
                    }
                    CANCEL_EVENT => {
                        return Err(CowRpcError::Cancel);
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn build_ifacedef_list(
        ifaces: &Vec<Arc<RwLock<CowRpcIface>>>,
        with_names: bool,
        with_procs: bool,
    ) -> Vec<CowRpcIfaceDef> {
        let mut iface_def_list = Vec::new();

        for iface in ifaces {
            let iface = iface.write();
            let iface_def = CowRpcPeer::build_ifacedef(&(*iface), with_names, with_procs);
            iface_def_list.push(iface_def);
        }

        iface_def_list
    }

    fn build_ifacedef(iface: &CowRpcIface, with_names: bool, with_procs: bool) -> CowRpcIfaceDef {
        let mut iface_def = CowRpcIfaceDef::default();
        iface_def.id = iface.rid;

        if with_names {
            iface_def.flags |= COW_RPC_DEF_FLAG_NAMED;
            iface_def.name = iface.name.clone();
        }

        if with_procs {
            iface_def.procs = CowRpcPeer::build_proc_def_list(&iface.procs, with_names);
        } else {
            iface_def.flags |= COW_RPC_DEF_FLAG_EMPTY;
        }

        iface_def
    }

    fn build_proc_def_list(procs: &[CowRpcProc], with_name: bool) -> Vec<CowRpcProcDef> {
        let mut proc_def_list = Vec::new();

        for procedure in procs {
            let proc_def = CowRpcPeer::build_proc_def(procedure, with_name);
            proc_def_list.push(proc_def);
        }

        proc_def_list
    }

    fn build_proc_def(procedure: &CowRpcProc, with_name: bool) -> CowRpcProcDef {
        let mut proc_def = CowRpcProcDef::default();
        proc_def.id = procedure.lid;

        if with_name {
            proc_def.flags |= COW_RPC_DEF_FLAG_NAMED;
            proc_def.name = procedure.name.clone();
        }

        proc_def
    }

    fn send_messages(&self, msg: CowRpcMessage) -> Result<()> {
        let msg_sender = self.msg_sender.lock();
        msg_sender.send(msg)?;
        Ok(())
    }

    fn remove_bind_context(&self, is_server: bool, remote_id: u32, iface_rid: u16) -> Option<Arc<CowRpcBindContext>> {
        let mut bind_contexts = self.bind_contexts.lock();

        bind_contexts
            .deref()
            .iter()
            .position(|bind_context| {
                (bind_context.is_server == is_server)
                    && (bind_context.remote_id == remote_id)
                    && bind_context.iface.get_remote_id() == iface_rid
            }).map(|position| bind_contexts.remove(position))
    }

    fn add_request(&self, req: CowRpcReq) {
        self.requests.lock().push(req);
    }

    fn remove_request(&self, id: usize) -> bool {
        let mut requests = self.requests.lock();

        requests
            .deref()
            .iter()
            .position(|request| request.get_id() == id)
            .map(|position| requests.remove(position))
            .is_some()
    }

    fn msg_processor_thread(&self, cancel_handle: &CancelEventHandle) -> Result<()> {
        let call_receiver = self.call_receiver.lock();
        let poll = Poll::new().unwrap();

        let _ = poll.register(call_receiver.deref(), PROCESS_EVENT, Ready::readable(), PollOpt::edge());
        let _ = poll.register(cancel_handle, CANCEL_EVENT, Ready::readable(), PollOpt::edge());

        let mut events = Events::with_capacity(1024);

        loop {
            poll.poll(&mut events, None).unwrap();

            let mut cancelled = false;

            for event in events.iter() {
                match event.token() {
                    PROCESS_EVENT => loop {
                        let result = call_receiver.try_recv();
                        match result {
                            Ok(msg) => {
                                self.process_msg(msg)?;
                            }
                            Err(e) => match e {
                                std::sync::mpsc::TryRecvError::Empty => {
                                    break;
                                }
                                std::sync::mpsc::TryRecvError::Disconnected => {
                                    return Err(e.into());
                                }
                            },
                        }
                    },
                    CANCEL_EVENT => {
                        cancelled = true;
                        break;
                    }
                    _ => unreachable!(),
                }
            }
            if cancelled {
                break;
            }
        }

        Ok(())
    }

    /// Starts the thread for this peer.
    pub fn start(peer: &Arc<CowRpcPeer>) {
        let peer_clone = peer.clone();

        let (cancel_handle, cancel_event) = CancelEvent::new();

        let (cancel_handle2, cancel_event2) = CancelEvent::new();

        let call_processor_handle: JoinHandle<Result<()>> =
            thread::spawn(move || peer_clone.msg_processor_thread(&cancel_handle2));

        *peer.call_processor_thread_handle.lock() = Some(call_processor_handle);
        *peer.call_stop_event.lock() = Some(cancel_event2);

        let peer_clone = peer.clone();

        let handle: JoinHandle<Result<()>> =
            thread::spawn(move || peer_clone.run_to_state(CowRpcState::TERMINATE, None, Some(&cancel_handle)));

        *peer.thread_handle.lock() = Some(handle);
        *peer.stop_event.lock() = Some(cancel_event);
    }

    /// Terminates the peer connection and stops the threads for this peer.
    pub fn stop(&self, timeout: Option<Duration>) -> Result<()> {
        // Stop msg thread if started
        let mut stop = self.stop_event.lock();
        if stop.is_some() {
            stop.take().unwrap().cancel()?;
            self.wait_thread_to_finish()?;
        }

        self.send_terminate_req()?;

        self.run_to_state(CowRpcState::TERMINATE, timeout, None)?;
        Ok(())
    }

    /// Wait for the peer thread to reach the Terminate state.
    pub fn wait_thread_to_finish(&self) -> Result<()> {
        let mut handle = self.thread_handle.lock();
        if handle.is_some() {
            let handle = handle.take().unwrap();
            if let Err(e) = handle.join().unwrap() {
                match e {
                    CowRpcError::Cancel => {}
                    _ => {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}
