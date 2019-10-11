use crate::error::Result;
use mio::Ready;
use std::net::SocketAddr;
use std::time::Duration;
use crate::transport::MessageInterceptor;
use crate::transport::sync::Transport;
use crate::transport::TransportError;
use crate::CowRpcMessage;

pub struct InterceptorTransport {
    pub inter: Box<dyn MessageInterceptor>,
}

impl Transport for InterceptorTransport {
    fn get_interest(&self) -> Ready {
        Ready::empty()
    }

    fn send_data(&mut self) -> Result<()> {
        Ok(())
    }

    fn send_message(&mut self, msg: CowRpcMessage) -> Result<()> {
        if let Some(msg) = self.inter.before_send(msg) {
            Err(TransportError::EndpointUnreachable(format!(
                "Unable to send msg {} trought interceptor, peer {} is inside this router",
                msg.get_msg_name(),
                msg.get_dst_id()
            )).into())
        } else {
            Ok(())
        }
    }

    fn get_next_message(&mut self) -> Result<Option<CowRpcMessage>> {
        Ok(None)
    }

    fn read_data(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_message_interceptor(&mut self, _cb_handler: Box<dyn MessageInterceptor>) {}

    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    fn up_time(&self) -> Duration {
        Duration::new(0, 0)
    }

    fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
