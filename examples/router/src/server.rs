extern crate cowrpc;
extern crate env_logger;
extern crate log;
extern crate tls_api;

use cowrpc::peer::{CallFuture, CowRpcPeer};
use cowrpc::CowRpcCallContext;
use futures::FutureExt;
use log::info;
use std::time::Duration;

#[tokio::main]
async fn main() {
    env_logger::init();

    let peer_config = CowRpcPeer::config("ws://127.0.0.1:12346").on_http_msg_callback(on_http_call);
    let peer = CowRpcPeer::connect(peer_config).await.expect("Connection failed");

    let mut verify_req = format!("GET {} HTTP/1.1 \r\n", "/");
    verify_req.push_str(&format!("Den_ID: {} \r\n", "server"));
    verify_req.push_str(&format!("Den-Pop-Token: {}", "pop_token"));
    verify_req.push_str("\r\n");

    let result = peer
        .verify(verify_req.into_bytes(), Duration::from_secs(10))
        .await
        .expect("verify failed");
    info!("Verify returned: {}", std::str::from_utf8(&result).unwrap());

    futures::future::pending::<()>().await;

    peer.disconnect().await.expect("Peer stop failed");
}

fn on_http_call(ctx: CowRpcCallContext, request: &mut [u8]) -> CallFuture<Vec<u8>> {
    let req_string = String::from_utf8_lossy(request).to_string();
    info!("HTTP call received from {}: \r\n {}", ctx.get_caller_id(), req_string);

    futures::future::ok(b"HTTP/1.1 200 OK\r\n\r\n".to_vec()).boxed()
}
