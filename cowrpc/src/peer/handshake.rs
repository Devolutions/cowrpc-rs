use crate::error::{CowRpcError, CowRpcErrorCode, Result};
use crate::peer::{CowRpcPeer, CowRpcPeerConfig, CowRpcPeerInner, CowRpcPeerWriter, CowRpcState};
use crate::proto::{CowRpcHandshakeMsg, CowRpcHdr};
use crate::transport::{CowSink, CowStream};
use crate::{proto, CowRpcMessage, Message};
use futures::SinkExt;
use slog::{o, Logger};
use std::sync::Arc;
use tokio::stream::StreamExt;
use tokio::sync::{Mutex, RwLock};

pub(crate) async fn handshake(
    config: CowRpcPeerConfig,
    mut stream: CowStream<CowRpcMessage>,
    mut sink: CowSink<CowRpcMessage>,
    logger: Logger,
) -> Result<CowRpcPeerInner> {
    send_handshake_req(&mut sink).await?;
    let (router_id, peer_id) = wait_handshake_rsp(&mut stream).await?;

    let peer_logger =
        logger.new(o!("router_id" => format!("{:#010X}", router_id), "peer_id" => format!("{:#010X}", peer_id)));
    sink.set_logger(peer_logger.clone());
    stream.set_logger(peer_logger.clone());

    let writer = CowRpcPeerWriter { writer_sink: sink };

    let peer = CowRpcPeer {
        config: Arc::new(config),
        id: peer_id,
        router_id,
        state: Arc::new(RwLock::new(CowRpcState::Active)),
        pending_requests: Arc::new(Mutex::new(Vec::new())),
        writer: Arc::new(RwLock::new(writer)),
        msg_task_handle: Arc::new(Mutex::new(None)),
        logger: peer_logger,
    };

    Ok(CowRpcPeerInner { peer, reader: stream })
}

async fn send_handshake_req(sink: &mut CowSink<CowRpcMessage>) -> Result<()> {
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

    sink.send(CowRpcMessage::Handshake(header, msg)).await?;

    Ok(())
}

async fn wait_handshake_rsp(stream: &mut CowStream<CowRpcMessage>) -> Result<(u32, u32)> {
    let msg: CowRpcMessage = match stream.next().await {
        Some(msg_result) => msg_result?,
        None => {
            return Err(CowRpcError::Proto(
                "Connection was reset before handshake response".to_string(),
            ))
        }
    };

    match msg {
        CowRpcMessage::Handshake(header, _msg) => {
            if !header.is_response() {
                return Err(CowRpcError::Proto(
                    "Expected Handshake Response, Handshake request received".to_string(),
                ));
            }

            if header.is_failure() {
                return Err(crate::error::CowRpcError::CowRpcFailure(CowRpcErrorCode::from(
                    header.flags,
                )));
            }

            Ok((header.src_id, header.dst_id))
        }
        msg => Err(CowRpcError::Proto(format!(
            "Expected Handshake Response, got {}",
            msg.get_msg_info()
        ))),
    }
}
