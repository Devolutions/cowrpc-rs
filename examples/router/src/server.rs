extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use std::time::Duration;
use log::{info, error};
use cowrpc::async_peer::CowRpcPeer;
use futures::future::err;
use cowrpc::{CowRpcCallContext, CallFuture};

#[tokio::main]
async fn main() {
    env_logger::init();

    let (mut peer, peer_handle) = CowRpcPeer::new_client("tcp://127.0.0.1:12346", None, cowrpc::CowRpcMode::ROUTED);

    peer.on_http_msg_callback(on_http_call);
    let task_handle = tokio::spawn(peer.run());

    // TODO : REMOVE THAT
    std::thread::sleep(Duration::from_secs(2));

    let mut verify_req = format!("GET {} HTTP/1.1 \r\n", "/");
    verify_req.push_str(&format!("Den_ID: {} \r\n", "server"));
    verify_req.push_str(&format!("Den-Pop-Token: {}", "pop_token"));
    verify_req.push_str("\r\n");

    let result = peer_handle.verify_async(verify_req.into_bytes(), Duration::from_secs(10)).await.expect("verify failed");
    info!("Verify returned: {}", std::str::from_utf8(&result).unwrap());

    if let Ok(Err(e)) = task_handle.await {
        error!("Server stopped with error: {}", e);
    }
}

fn on_http_call(ctx: CowRpcCallContext, request: &mut [u8]) -> CallFuture<Vec<u8>> {
    let req_string = String::from_utf8_lossy(request).to_string();
    info!("HTTP call received from {}: \r\n {}", ctx.get_caller_id(), req_string);

    Box::new(
        futures::future::ok(b"HTTP/1.1 200 OK\r\n\r\n".to_vec())
    )
}
